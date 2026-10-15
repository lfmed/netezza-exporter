"""Teste do tipo de filtro (Data, Número, Sem filtro) num branch temporário do Lakebase.

Não toca o branch production: cria o branch `teste-filtro` (ttl 1 dia) a partir dele e o apaga no fim.
  1. grava um subbloco no modelo ANTIGO (data_range) e migra: confere a conversão para key_range;
  2. cadastra uma tabela de cada tipo e cria pedidos (revisão, exclusão do que já foi executado, EXCLUDE no banco,
     reprocesso do sem filtro, trava da troca de tipo);
  3. confere a cobertura (unidades e status) e as lacunas;
  4. abre as telas e envia os formulários pelo app Flask local (sem scheduler: nada dispara jobs).

Uso: .venv/bin/python scripts/teste_filtro_branch.py [--manter]
"""
import argparse
import html
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import psycopg  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

from nzexport import migrate, repo  # noqa: E402
from nzexport.db import Database, LakebaseConfig  # noqa: E402

PROFILE = "fe-vm-stable-classic-pg4xe1"
PROJECT = "projects/netezza-exporter"
BRANCH = "teste-filtro"
ENDPOINT = f"{PROJECT}/branches/{BRANCH}/endpoints/primary"
FALHAS: list[str] = []


def check(cond, msg):
    print(("  ok   " if cond else "  FALHA ") + msg)
    if not cond:
        FALHAS.append(msg)


def base_fields(**kw):
    return {"call_profile": "SP_CARGA_EVENTUAL", "procedure_name": "SP_CARGA_EVENTUAL", "source_database": "DWA",
            "ext_database": "STAGE", "ext_schema": "ADMIN", "date_format": "YYYYMMDD", "file_options": {},
            "default_subblock_unit": "MONTH", "default_subblock_size": 1, "default_subblock_count": 10,
            "max_parallel": 2, "call_timeout_min": 60, "max_retries": 0, "retry_unknown": False} | kw


def fase_migracao(w):
    print("1. migração data_range -> key_range")
    cfg = LakebaseConfig(endpoint=ENDPOINT, w=w)
    with cfg.connect() as c:
        tem_antiga = c.execute("""SELECT 1 FROM information_schema.columns WHERE table_schema = 'nzexport'
                                  AND table_name = 'export_subblock' AND column_name = 'data_range'""").fetchone()
        check(bool(tem_antiga), "branch começa no modelo antigo (data_range)")
        c.execute("""INSERT INTO table_config (table_config_id, source_id) VALUES ('LEGADO', 'netezza')""")
        c.execute("""INSERT INTO table_config_version (table_config_id, config_version, source_table, procedure_name,
                     call_profile, time_column, migration_start, migration_end)
                     VALUES ('LEGADO', 1, 'LEGADO', 'SP_CARGA_EVENTUAL', 'SP_CARGA_EVENTUAL', 'SK_DATA',
                             '2016-01-01', '2016-03-31')""")
        rid = c.execute("""INSERT INTO export_request (request_name, requested_by, status, max_parallel)
                           VALUES ('legado', 'teste', 'COMPLETED', 1) RETURNING request_id""").fetchone()["request_id"]
        rt = c.execute("""INSERT INTO export_request_table (request_id, ordinal, table_config_id, config_version,
                          period_start, period_end, subblock_unit, subblock_size)
                          VALUES (%s, 1, 'LEGADO', 1, '2016-01-01', '2016-01-31', 'MONTH', 1)
                          RETURNING request_table_id""", (rid,)).fetchone()["request_table_id"]
        c.execute("""INSERT INTO export_subblock (request_table_id, request_id, table_config_id, seq, data_range, status)
                     VALUES (%s, %s, 'LEGADO', 1, '[2016-01-01,2016-02-01)', 'DONE')""", (rt, rid))
        c.commit()
    migrate.run(cfg)
    migrate.run(cfg)  # idempotente
    with cfg.connect() as c:
        sb = c.execute("SELECT key_range::text AS k FROM export_subblock WHERE table_config_id = 'LEGADO'").fetchone()
        ini = (date(2016, 1, 1) - date(1970, 1, 1)).days
        check(sb["k"] == f"[{ini},{ini + 31})", f"subbloco convertido para key_range {sb['k']}")
        rtr = c.execute("SELECT key_lo, key_hi FROM export_request_table WHERE table_config_id = 'LEGADO'").fetchone()
        check((rtr["key_lo"], rtr["key_hi"]) == (ini, ini + 30), "pedido antigo ganhou key_lo/key_hi")
        cov = c.execute("SELECT * FROM v_migration_coverage WHERE table_config_id = 'LEGADO'").fetchone()
        check((cov["scope_units"], cov["done_units"], cov["missing_units"], cov["gap_count"]) == (91, 31, 60, 1),
              f"cobertura do legado em dias: escopo {cov['scope_units']}, migrado {cov['done_units']}, "
              f"lacuna {cov['missing_units']} em {cov['gap_count']}")
    return cfg


def fase_pedidos(db):
    print("2. cadastro e pedidos por tipo")
    repo.save_table(db, "teste", "T_DATA", "netezza", True, base_fields(
        filter_type="DATE", source_table="T_DATA", time_column="SK_DATA",
        migration_start=date(2024, 1, 1), migration_end=date(2024, 12, 31)))
    repo.save_table(db, "teste", "T_NUM", "netezza", True, base_fields(
        filter_type="NUMBER", source_table="T_NUM", time_column="NR_CONTRATO",
        scope_num_min=1, scope_num_max=100, default_subblock_count=4))
    repo.save_table(db, "teste", "T_TOTAL", "netezza", True, base_fields(
        filter_type="NONE", source_table="T_TOTAL", time_column=None))

    TR = repo.TableRequest
    r1 = repo.create_request(db, "teste", "num 30-60", 2, None, None,
                             [TR("T_NUM", 30, 60, "COUNT", 1, filter_type="NUMBER")])
    rev = repo.review(db, [TR("T_NUM", 1, 100, "COUNT", 4, filter_type="NUMBER")])[0]
    check(rev.pieces == [(1, 29), (61, 100)], f"revisão exclui 30–60: pedaços {rev.pieces}")
    check([(b.start, b.end) for b in rev.blocks] == [(1, 25), (26, 29), (61, 75), (76, 100)],
          f"cortes da quantidade não se deslocam: {[(b.start, b.end) for b in rev.blocks]}")
    check(rev.excluded_units == 31 and rev.excluded[0]["status"] == "PENDING", "31 valores excluídos (pendentes)")
    r2 = repo.create_request(db, "teste", "num resto", 2, None, None,
                             [TR("T_NUM", 1, 100, "COUNT", 4, filter_type="NUMBER")])
    with db.tx() as c:
        n = c.execute("SELECT count(*) AS n FROM export_subblock WHERE request_id = %s", (r2,)).fetchone()["n"]
        lo = c.execute("""SELECT lower(key_range) AS lo FROM export_subblock WHERE request_id = %s
                          ORDER BY lower(key_range) DESC LIMIT 1""", (r2,)).fetchone()["lo"]
    check(n == 4 and lo == 76, "pedido numérico gravou 4 subblocos; o primeiro na fila é o de maior valor (76)")
    try:
        with db.tx() as c:
            rt = c.execute("SELECT request_table_id FROM export_request_table WHERE request_id = %s", (r2,)).fetchone()
            c.execute("""INSERT INTO export_subblock (request_table_id, request_id, table_config_id, seq, key_range, status)
                         VALUES (%s, %s, 'T_NUM', 99, int8range(50, 70, '[]'), 'PENDING')""",
                      (rt["request_table_id"], r2))
        check(False, "EXCLUDE bloqueia sobreposição numérica")
    except psycopg.errors.ExclusionViolation:
        check(True, "EXCLUDE bloqueia sobreposição numérica")

    r3 = repo.create_request(db, "teste", "total", 1, None, None, [TR("T_TOTAL", 0, 0, "WHOLE", 1, filter_type="NONE")])
    sbs = repo.request_subblocks(db, r3)
    check(len(sbs) == 1 and sbs[0]["key_range"].lower == 0, "sem filtro: um subbloco [0, 1)")
    try:
        repo.create_request(db, "teste", "total de novo", 1, None, None,
                            [TR("T_TOTAL", 0, 0, "WHOLE", 1, filter_type="NONE")])
        check(False, "sem filtro em execução não entra em outro pedido")
    except repo.NothingToRun:
        check(True, "sem filtro em execução não entra em outro pedido")
    with db.tx() as c:
        c.execute("UPDATE export_subblock SET status = 'DONE' WHERE request_id = %s", (r3,))
    rev = repo.review(db, [TR("T_TOTAL", 0, 0, "WHOLE", 1, is_reprocess=True, reprocess_reason="recarga",
                              filter_type="NONE")])[0]
    check(len(rev.blocks) == 1 and rev.superseded, "reprocesso do sem filtro substitui o concluído")
    r4 = repo.create_request(db, "teste", "total reproc", 1, None, None,
                             [TR("T_TOTAL", 0, 0, "WHOLE", 1, is_reprocess=True, reprocess_reason="recarga",
                                 filter_type="NONE")])

    rev = repo.review(db, [TR("T_NUM", 1, 10, "COUNT", 2, filter_type="DATE")])[0]
    check(bool(rev.conflicts), "linha com tipo diferente do cadastro vira conflito")
    try:
        repo.save_table(db, "teste", "T_NUM", "netezza", True, base_fields(
            filter_type="DATE", source_table="T_NUM", time_column="NR_CONTRATO"))
        check(False, "tipo de filtro não muda com subblocos")
    except ValueError as e:
        check("não pode mudar" in str(e), "tipo de filtro não muda com subblocos")
    r5 = repo.create_request(db, "teste", "data", 2, None, None,
                             [TR("T_DATA", date(2024, 1, 1), date(2024, 3, 31), "MONTH", 1)])
    return {"num": r2, "total": r4, "data": r5}


def fase_cobertura(db):
    print("3. cobertura e lacunas")
    with db.tx() as c:  # conclui metade da faixa numérica
        c.execute("UPDATE export_subblock SET status = 'DONE' WHERE table_config_id = 'T_NUM' AND lower(key_range) >= 61")
    cov = {t["table_config_id"]: t for t in repo.migration_coverage(db)}
    n, t, d = cov["T_NUM"], cov["T_TOTAL"], cov["T_DATA"]
    check((n["scope_units"], n["done_units"], n["inflight_units"]) == (100, 40, 60),
          f"número: escopo {n['scope_units']}, migrado {n['done_units']}, em execução {n['inflight_units']}")
    check(n["coverage_status"] == "EM_ANDAMENTO", f"número: {n['coverage_status']}")
    check((t["scope_units"], t["coverage_status"]) == (1, "EM_ANDAMENTO"), f"sem filtro: {t['coverage_status']}")
    check((d["scope_units"], d["missing_units"], d["gap_count"]) == (366, 275, 1),
          f"data: escopo {d['scope_units']} dias, lacuna {d['missing_units']} em {d['gap_count']}")
    with db.tx() as c:  # cancela 1..25 (vira lacuna)
        c.execute("UPDATE export_subblock SET status = 'CANCELED' WHERE table_config_id = 'T_NUM' AND lower(key_range) = 1")
    gaps = repo.gap_rows(db, ["T_NUM", "T_DATA"])
    num = [g for g in gaps if g["table"] == "T_NUM"]
    check(num == [{"table": "T_NUM", "aligned": "1", "reprocess": "0", "reason": "", "start": "1", "end": "25",
                   "unit": "COUNT", "size": "1"}], f"lacuna numérica: {num}")
    rng = repo.table_ranges(db, "T_NUM")["missing"]
    check(rng and (rng[0]["data_ini"], rng[0]["data_fim"], rng[0]["units"]) == (1, 25, 25), "faixas da lacuna em valores")


def fase_telas(reqs):
    print("4. telas e formulários (Flask local)")
    os.environ["LAKEBASE_ENDPOINT"] = ENDPOINT
    os.environ["DATABRICKS_CONFIG_PROFILE"] = PROFILE
    os.environ.pop("DISPATCHER_JOB_ID", None)
    from webapp import create_app, services
    services._db = None
    app = create_app()
    cl = app.test_client()
    db = services.db()
    sb = {k: str(repo.request_subblocks(db, v)[0]["subblock_id"]) for k, v in reqs.items()}
    pages = ["/", "/requests", "/requests/new", "/admin", "/admin/tables/new", "/admin/tables/template.csv",
             "/admin/tables/export.csv"] + [f"/coverage/{t}" for t in ("T_DATA", "T_NUM", "T_TOTAL", "LEGADO")] + \
            [f"/admin/tables/{t}" for t in ("T_DATA", "T_NUM", "T_TOTAL")] + \
            [f"/requests/{r}" for r in reqs.values()] + [f"/subblocks/{s}" for s in sb.values()]
    for p in pages:
        r = cl.get(p)
        check(r.status_code == 200, f"GET {p} -> {r.status_code}")
    txt = cl.get("/coverage/T_NUM").get_data(as_text=True)
    check("<td>1</td><td>25</td>" in txt and "valores" in txt, "cobertura numérica mostra a lacuna 1–25 em valores")
    txt = cl.get(f"/requests/{reqs['total']}").get_data(as_text=True)
    check("tabela inteira" in txt, "pedido sem filtro mostra 'tabela inteira'")
    txt = cl.get("/admin/tables/T_NUM").get_data(as_text=True)
    check('name="filter_type" id="filter_type" disabled' in txt, "tipo de filtro travado na tela (tem subblocos)")
    form = {"name": "rev", "max_parallel": "2", "table": ["T_NUM", "T_TOTAL", "T_DATA"],
            "start": ["1.000", "", "2025-01-01"], "end": ["2.000", "", "2025-02-28"], "size": ["5", "1", "1"],
            "unit": ["MONTH", "MONTH", "MONTH"], "aligned": ["1", "1", "1"], "reprocess": ["0", "0", "0"],
            "reason": ["", "", ""]}
    r = cl.post("/requests/review", data=form)
    txt = html.unescape(r.get_data(as_text=True))
    check(r.status_code == 200 and "1.000 → 2.000" in txt and "'1000', '1200'" in txt,
          f"revisão: faixa numérica e chamada com mín./máx. ({r.status_code})")
    check("Nada a executar" in txt, "revisão: sem filtro já reprocessado fica fora do lote")
    r = cl.post("/requests/gaps", data={"table_ids": ["T_NUM"]})
    check(r.status_code == 200 and '"start": "1"' in r.get_data(as_text=True), "pedido das lacunas pré-preenchido")
    r = cl.post("/admin/tables", data={
        "table_config_id": "T_NUM2", "source_id": "netezza", "active": "1", "call_profile": "SP_CARGA_EVENTUAL",
        "procedure_name": "SP_CARGA_EVENTUAL", "filter_type": "NUMBER", "source_table": "T_NUM2", "time_column": "nr",
        "source_database": "DWA", "ext_database": "STAGE", "ext_schema": "ADMIN", "scope_num_min": "-1.000", "scope_num_max": "5000",
        "default_subblock_count": "8", "default_subblock_unit": "MONTH", "default_subblock_size": "1",
        "max_parallel": "2", "call_timeout_min": "60", "max_retries": "1", "retry_unknown": "0"})
    t2 = repo.get_table(db, "T_NUM2")
    check(r.status_code == 302 and t2 and (t2["scope_num_min"], t2["scope_num_max"], t2["default_subblock_count"],
          t2["migration_start"]) == (-1000, 5000, 8, None), "cadastro numérico pelo formulário")


def apagar_branch(w):
    op = w.postgres.delete_branch(name=f"{PROJECT}/branches/{BRANCH}")
    if hasattr(op, "wait"):
        op.wait()


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--manter", action="store_true", help="não apaga o branch no fim")
    args = a.parse_args()
    w = WorkspaceClient(profile=PROFILE)
    try:
        apagar_branch(w)
    except Exception:  # noqa: BLE001
        pass
    subprocess.run(["databricks", "postgres", "create-branch", PROJECT, BRANCH, "--json",
                    '{"spec": {"source_branch": "%s/branches/production", "ttl": "86400s"}}' % PROJECT,
                    "--profile", PROFILE], check=True, capture_output=True)
    try:
        cfg = fase_migracao(w)
        db = Database(cfg, max_size=4)
        reqs = fase_pedidos(db)
        fase_cobertura(db)
        fase_telas(reqs)
    finally:
        if not args.manter:
            apagar_branch(w)
            print(f"branch {BRANCH} apagado")
    print(f"\n{'TUDO OK' if not FALHAS else f'{len(FALHAS)} FALHA(S)'}")
    sys.exit(1 if FALHAS else 0)


if __name__ == "__main__":
    main()
