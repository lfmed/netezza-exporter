"""Teste ponta a ponta do importador (Job real, Spark serverless, tabela Delta real, .dat real num volume).

Monta um ambiente isolado e o remove no fim:
  * branch temporário do Lakebase (o app de produção não é tocado);
  * schema/volume/tabela temporários no Unity Catalog;
  * subblocos EXPORTED (o que a SP_CARGA_EVENTUAL deixaria) com os .dat no formato da SP.
Esperado:
  * Data (FATO_TESTE): jan e fev carregados por append e .dat apagados; mar com linha ilegível -> período limpo no
    destino, .dat movido para erro/ e subbloco IMPORT_FAILED (período liberado);
  * Número (FATO_NUM, faixa 1-300 em 3): 1-100 e 101-200 carregados; 201-300 com linha ilegível -> BETWEEN 201 AND 300
    limpo no destino (a linha 999, fora da faixa, fica);
  * Sem filtro (DIM_TOTAL) em reprocesso: a tabela destino é apagada inteira e recebe o .dat <TABELA>_1_1.dat;
  * dispatcher real num subbloco numérico sem Netezza configurado: a chamada é montada com mínimo e máximo e o
    subbloco termina FAILED por falta de conexão (confere a fila e as transições sem chamar o Netezza).

Uso: .venv/bin/python scripts/teste_importacao.py [--manter]
"""
import argparse
import io
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.sql import StatementState  # noqa: E402

from nzexport import migrate, repo  # noqa: E402
from nzexport.db import Database, LakebaseConfig  # noqa: E402

PROFILE = "fe-vm-stable-classic-pg4xe1"
PROJECT = "projects/netezza-exporter"
BRANCH = "teste-importacao"
WAREHOUSE = "9981e1dca61efe86"
CAT, SCH, VOL, TAB = "stable_classic_pg4xe1_catalog", "nzexport_teste_importacao", "landing", "fato_teste"
DEST = f"{CAT}.{SCH}.{TAB}"
LANDING = f"/Volumes/{CAT}/{SCH}/{VOL}"
IMPORTER_JOB = 1045038738828699
DISPATCHER_JOB = 65950468575247
DEST_NUM, DEST_TOTAL = f"{CAT}.{SCH}.fato_num", f"{CAT}.{SCH}.dim_total"

# .dat no formato da SP: DELIM '|', IncludeHeader, escapechar '\', textos com quote_literal (aspas simples), LATIN9
DAT = {
    "FATO_TESTE_20250101_20250131.dat": "SK_DATA|NOME|VALOR\n20250105|'João Ação'|10.50\n20250120|'Ma\\|ria'|20.00\n",
    "FATO_TESTE_20250201_20250228.dat": "SK_DATA|NOME|VALOR\n20250201|'Fev 1'|1.00\n20250214|'Fev \\null'|2.00\n"
                                         "20250228|'Fev 3'|3.00\n",
    "FATO_TESTE_20250301_20250331.dat": "SK_DATA|NOME|VALOR\n20250301|'Mar ok'|5.00\n20250315|'Mar ruim'|abc\n",
    "FATO_NUM_1_100.dat": "NR|NOME\n5|'cinco'\n50|'cinquenta'\n",
    "FATO_NUM_101_200.dat": "NR|NOME\n150|'cento e cinquenta'\n",
    "FATO_NUM_201_300.dat": "NR|NOME\n250|'ok'\nxyz|'ruim'\n",
    "DIM_TOTAL_1_1.dat": "ID|NOME\n10|'nova'\n11|'nova 2'\n",
}


def sql(w, stmt):
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=WAREHOUSE, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{stmt[:80]}: {r.status.error.message if r.status.error else r.status.state}")
    return r.result.data_array if r.result else []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manter", action="store_true", help="não remove o ambiente no fim")
    a = ap.parse_args()
    w = WorkspaceClient(profile=PROFILE)

    print("1) Unity Catalog: schema, volume e tabela destino temporários")
    sql(w, f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
    sql(w, f"CREATE VOLUME IF NOT EXISTS {CAT}.{SCH}.{VOL}")
    sql(w, f"CREATE OR REPLACE TABLE {DEST} (SK_DATA INT, NOME STRING, VALOR DECIMAL(18,2))")
    sql(w, f"INSERT INTO {DEST} VALUES (20250310, 'linha antiga de março', 9.99), (20241215, 'dezembro (fora)', 1.00)")
    sql(w, f"CREATE OR REPLACE TABLE {DEST_NUM} (NR BIGINT, NOME STRING)")
    sql(w, f"INSERT INTO {DEST_NUM} VALUES (260, 'antiga na faixa 201-300'), (999, 'fora do escopo')")
    sql(w, f"CREATE OR REPLACE TABLE {DEST_TOTAL} (ID INT, NOME STRING)")
    sql(w, f"INSERT INTO {DEST_TOTAL} VALUES (1, 'carga anterior'), (2, 'carga anterior 2')")
    for name, text in DAT.items():
        w.files.upload(f"{LANDING}/tmp/{name}", io.BytesIO(text.encode("iso-8859-15")), overwrite=True)

    print("2) Lakebase: branch temporário + cadastro + 3 subblocos EXPORTED")
    try:
        op = w.postgres.delete_branch(name=f"{PROJECT}/branches/{BRANCH}")
        op.wait() if hasattr(op, "wait") else None
    except Exception:  # noqa: BLE001
        pass
    import subprocess
    subprocess.run(["databricks", "postgres", "create-branch", PROJECT, BRANCH, "--json",
                    '{"spec": {"source_branch": "%s/branches/production", "ttl": "86400s"}}' % PROJECT,
                    "--profile", PROFILE], check=True, capture_output=True)
    endpoint = f"{PROJECT}/branches/{BRANCH}/endpoints/primary"
    cfg = LakebaseConfig(endpoint=endpoint, w=w)
    migrate.run(cfg)
    db = Database(cfg, max_size=4)
    with db.tx() as c:
        c.execute("""UPDATE source_connection SET database = 'SYSTEM',
                     default_source_database = 'DWA', default_ext_database = 'STAGE', default_ext_schema = 'ADMIN',
                     landing_root = %s, landing_dat_folder = 'tmp', landing_error_folder = 'erro',
                     landing_work_folder = 'limpeza', max_imports = 2, file_wait_min = 1 WHERE source_id = 'netezza'""",
                  (LANDING,))
    comum = dict(call_profile="SP_CARGA_EVENTUAL", procedure_name="SP_CARGA_EVENTUAL", date_format="YYYYMMDD",
                 dest_catalog=CAT, dest_schema=SCH, file_options={}, default_subblock_unit="MONTH",
                 default_subblock_size=1, default_subblock_count=3, max_parallel=2, call_timeout_min=60,
                 max_retries=0, retry_unknown=False)
    repo.save_table(db, "teste", "FATO_TESTE", "netezza", True, comum | dict(
        source_table="FATO_TESTE", time_column="SK_DATA", dest_table=TAB))
    repo.save_table(db, "teste", "FATO_NUM", "netezza", True, comum | dict(
        filter_type="NUMBER", source_table="FATO_NUM", time_column="NR", dest_table="fato_num",
        scope_num_min=1, scope_num_max=400))
    repo.save_table(db, "teste", "DIM_TOTAL", "netezza", True, comum | dict(
        filter_type="NONE", source_table="DIM_TOTAL", time_column=None, dest_table="dim_total"))
    TR = repo.TableRequest
    rid = repo.create_request(db, "teste", "teste importação", 2, None, None, [
        TR("FATO_TESTE", date(2025, 1, 1), date(2025, 3, 31), "MONTH", 1),
        TR("FATO_NUM", 1, 300, "COUNT", 3, filter_type="NUMBER"),
        TR("DIM_TOTAL", 0, 0, "WHOLE", 1, is_reprocess=True, reprocess_reason="recarga total", filter_type="NONE")])
    with db.tx() as c:  # estado que o dispatcher deixa depois da SP: EXPORTED + nome do .dat (como a SP gera)
        c.execute("""UPDATE export_subblock s SET status = 'EXPORTED', attempts = 1, exported_at = now(),
                     file_name = s.table_config_id || '_' || CASE v.filter_type
                       WHEN 'DATE' THEN to_char(DATE '1970-01-01' + lower(s.key_range)::int, 'YYYYMMDD') || '_'
                                     || to_char(DATE '1970-01-01' + (upper(s.key_range) - 1)::int, 'YYYYMMDD')
                       WHEN 'NUMBER' THEN lower(s.key_range) || '_' || (upper(s.key_range) - 1)
                       ELSE '1_1' END || '.dat'
                     FROM export_request_table rt JOIN table_config_version v
                       ON v.table_config_id = rt.table_config_id AND v.config_version = rt.config_version
                     WHERE rt.request_table_id = s.request_table_id AND s.request_id = %s""", (rid,))
        c.execute("UPDATE export_request SET status = 'RUNNING', started_at = now() WHERE request_id = %s", (rid,))

    print("3) Job nzexport-importer (real) apontando para o branch")
    run = w.jobs.run_now(job_id=IMPORTER_JOB, job_parameters={"lakebase_endpoint": endpoint, "idle_exit_s": "15"}).result()
    print("   run", run.run_id, run.state.result_state)

    print("4) Conferência")
    ok = True
    todos = repo.request_subblocks(db, rid)
    sbs = {str(s["data_ini"]): s for s in todos if s["table_config_id"] == "FATO_TESTE"}
    num = {s["data_ini"]: s for s in todos if s["table_config_id"] == "FATO_NUM"}
    total = [s for s in todos if s["table_config_id"] == "DIM_TOTAL"]
    for s in sorted(todos, key=lambda x: (x["table_config_id"], str(x["data_ini"]))):
        print(f"   {s['table_config_id']:10} {str(s['data_ini']):10} {s['status']:14} linhas={s['rows_loaded']} "
              f"arquivo={s['file_name']} erro={(s['last_error'] or '')[:100]}")
    ok &= sbs["2025-01-01"]["status"] == "DONE" and sbs["2025-01-01"]["rows_loaded"] == 2
    ok &= sbs["2025-02-01"]["status"] == "DONE" and sbs["2025-02-01"]["rows_loaded"] == 3
    ok &= sbs["2025-03-01"]["status"] == "IMPORT_FAILED"
    linhas = sql(w, f"SELECT SK_DATA, NOME, CAST(VALOR AS STRING) FROM {DEST} ORDER BY SK_DATA")
    for r in linhas:
        print("   destino:", r)
    nomes = {r[1] for r in linhas}
    ok &= {"João Ação", "Ma|ria"} <= nomes                      # acento LATIN9 e \| tratados pela limpeza
    ok &= any(r[0] == "20250214" and r[1] == "Fev " for r in linhas)   # \null -> vazio
    ok &= not any(r[0].startswith("202503") for r in linhas)     # março limpo (inclusive a linha antiga)
    ok &= any(r[0] == "20241215" for r in linhas)                 # fora do período: intocado
    restantes = [f.name for f in w.files.list_directory_contents(f"{LANDING}/tmp")]
    erro = [f.name for f in w.files.list_directory_contents(f"{LANDING}/erro")]
    print("   tmp/:", restantes, "| erro/:", erro)
    ok &= restantes == [] and sorted(erro) == ["FATO_NUM_201_300.dat", "FATO_TESTE_20250301_20250331.dat"]
    print("   -- Número")
    ok &= num[1]["status"] == "DONE" and num[1]["rows_loaded"] == 2
    ok &= num[101]["status"] == "DONE" and num[101]["rows_loaded"] == 1
    ok &= num[201]["status"] == "IMPORT_FAILED"
    linhas_num = sql(w, f"SELECT NR, NOME FROM {DEST_NUM} ORDER BY NR")
    print("   destino num:", linhas_num)
    ok &= [r[0] for r in linhas_num] == ["5", "50", "150", "999"]   # 201-300 limpo (260 antiga saiu); 999 fica
    print("   -- Sem filtro (reprocesso)")
    ok &= len(total) == 1 and total[0]["status"] == "DONE" and total[0]["rows_loaded"] == 2
    linhas_total = sql(w, f"SELECT ID, NOME FROM {DEST_TOTAL} ORDER BY ID")
    print("   destino total:", linhas_total)
    ok &= [r[0] for r in linhas_total] == ["10", "11"]                # carga anterior apagada inteira
    att = db.one("""SELECT rows_cleaned, detail FROM export_import_attempt WHERE subblock_id = %s
                    ORDER BY attempt_no DESC LIMIT 1""", (total[0]["subblock_id"],))
    print("   limpeza total:", att)
    ok &= att["rows_cleaned"] == 2

    print("5) Dispatcher real (sem Netezza configurado) num subbloco numérico")
    rid2 = repo.create_request(db, "teste", "teste dispatcher", 1, None, None,
                               [TR("FATO_NUM", 301, 400, "COUNT", 1, filter_type="NUMBER")])
    run = w.jobs.run_now(job_id=DISPATCHER_JOB, job_parameters={"lakebase_endpoint": endpoint, "idle_exit_s": "15"}).result()
    print("   run", run.run_id, run.state.result_state)
    sb2 = repo.request_subblocks(db, rid2)[0]
    a2 = db.one("SELECT status, call_statement, error_class, error_message FROM export_attempt WHERE subblock_id = %s",
                (sb2["subblock_id"],))
    print("   subbloco:", sb2["status"], "| tentativa:", a2)
    ok &= sb2["status"] == "FAILED" and a2 is not None
    ok &= "'301', '400', 'NR'" in (a2["call_statement"] or "")
    ok &= a2["error_class"] == "CONNECT"
    rev = repo.review(db, [repo.TableRequest("FATO_TESTE", date(2025, 3, 1), date(2025, 3, 31), "MONTH", 1)])[0]
    print("   março liberado para novo pedido:", [(str(b.start), str(b.end)) for b in rev.blocks])
    ok &= len(rev.blocks) == 1
    req = repo.get_request(db, rid)
    print("   pedido:", req["status"], "|", req["status_reason"])
    db.close()
    print("RESULTADO:", "OK" if ok else "FALHOU")

    if not a.manter:
        print("6) Limpeza do ambiente de teste")
        sql(w, f"DROP SCHEMA IF EXISTS {CAT}.{SCH} CASCADE")
        subprocess.run(["databricks", "postgres", "delete-branch", f"{PROJECT}/branches/{BRANCH}", "--profile", PROFILE],
                       capture_output=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
