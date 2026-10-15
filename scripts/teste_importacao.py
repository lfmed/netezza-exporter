"""Teste ponta a ponta do importador (Job real, Spark serverless, tabela Delta real, .dat real num volume).

Monta um ambiente isolado e o remove no fim:
  * branch temporário do Lakebase (o app de produção não é tocado);
  * schema/volume/tabela temporários no Unity Catalog;
  * 3 subblocos EXPORTED (o que a SP_CARGA_EVENTUAL deixaria) com os .dat no formato da SP.
Esperado: jan e fev carregados por append e .dat apagados; mar com linha ilegível -> período limpo no destino,
.dat movido para erro/ e subbloco IMPORT_FAILED (período liberado).

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

# .dat no formato da SP: DELIM '|', IncludeHeader, escapechar '\', textos com quote_literal (aspas simples), LATIN9
DAT = {
    "FATO_TESTE_20250101_20250131.dat": "SK_DATA|NOME|VALOR\n20250105|'João Ação'|10.50\n20250120|'Ma\\|ria'|20.00\n",
    "FATO_TESTE_20250201_20250228.dat": "SK_DATA|NOME|VALOR\n20250201|'Fev 1'|1.00\n20250214|'Fev \\null'|2.00\n"
                                         "20250228|'Fev 3'|3.00\n",
    "FATO_TESTE_20250301_20250331.dat": "SK_DATA|NOME|VALOR\n20250301|'Mar ok'|5.00\n20250315|'Mar ruim'|abc\n",
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
    for name, text in DAT.items():
        w.files.upload(f"{LANDING}/tmp/{name}", io.BytesIO(text.encode("iso-8859-15")), overwrite=True)

    print("2) Lakebase: branch temporário + cadastro + 3 subblocos EXPORTED")
    try:
        w.postgres.delete_branch(name=f"{PROJECT}/branches/{BRANCH}").result()
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
    repo.save_table(db, "teste", "FATO_TESTE", "netezza", True, dict(
        call_profile="SP_CARGA_EVENTUAL", procedure_name="SP_CARGA_EVENTUAL", source_table="FATO_TESTE",
        time_column="SK_DATA", date_format="YYYYMMDD", dest_catalog=CAT, dest_schema=SCH, dest_table=TAB,
        file_options={}, default_subblock_unit="MONTH", default_subblock_size=1, max_parallel=2,
        call_timeout_min=60, max_retries=0, retry_unknown=False))
    rid = repo.create_request(db, "teste", "teste importação", 2, None, None,
                              [repo.TableRequest("FATO_TESTE", date(2025, 1, 1), date(2025, 3, 31), "MONTH", 1)])
    with db.tx() as c:  # estado que o dispatcher deixa depois da SP: EXPORTED + nome do .dat
        c.execute("""UPDATE export_subblock SET status = 'EXPORTED', attempts = 1, exported_at = now(),
                     file_name = 'FATO_TESTE_' || to_char(lower(data_range), 'YYYYMMDD') || '_'
                                 || to_char(upper(data_range) - 1, 'YYYYMMDD') || '.dat'
                     WHERE request_id = %s""", (rid,))
        c.execute("UPDATE export_request SET status = 'RUNNING', started_at = now() WHERE request_id = %s", (rid,))

    print("3) Job nzexport-importer (real) apontando para o branch")
    run = w.jobs.run_now(job_id=IMPORTER_JOB, job_parameters={"lakebase_endpoint": endpoint, "idle_exit_s": "15"}).result()
    print("   run", run.run_id, run.state.result_state)

    print("4) Conferência")
    ok = True
    sbs = {str(s["data_ini"]): s for s in repo.request_subblocks(db, rid)}
    for k, s in sorted(sbs.items()):
        print(f"   {k} {s['status']:14} linhas={s['rows_loaded']} versão={s['delta_version']} erro={(s['last_error'] or '')[:110]}")
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
    ok &= restantes == [] and erro == ["FATO_TESTE_20250301_20250331.dat"]
    rev = repo.review(db, [repo.TableRequest("FATO_TESTE", date(2025, 3, 1), date(2025, 3, 31), "MONTH", 1)])[0]
    print("   março liberado para novo pedido:", [(str(b.start), str(b.end)) for b in rev.blocks])
    ok &= len(rev.blocks) == 1
    req = repo.get_request(db, rid)
    print("   pedido:", req["status"], "|", req["status_reason"])
    db.close()
    print("RESULTADO:", "OK" if ok else "FALHOU")

    if not a.manter:
        print("5) Limpeza do ambiente de teste")
        sql(w, f"DROP SCHEMA IF EXISTS {CAT}.{SCH} CASCADE")
        subprocess.run(["databricks", "postgres", "delete-branch", f"{PROJECT}/branches/{BRANCH}", "--profile", PROFILE],
                       capture_output=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
