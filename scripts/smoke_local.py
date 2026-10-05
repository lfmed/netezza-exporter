"""Teste ponta a ponta local com a procedure simulada (driver fake).

Cadastra tabelas de exemplo, cria um pedido, roda o dispatcher localmente e valida:
limites de concorrência, retries, bloqueio de sobreposição e retomada.
"""
import os
import sys
import threading
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.sdk import WorkspaceClient  # noqa: E402

from nzexport import repo  # noqa: E402
from nzexport.db import Database, LakebaseConfig  # noqa: E402
from nzexport.dispatcher import Dispatcher  # noqa: E402

PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "fe-vm-stable-classic-pg4xe1")
ENDPOINT = "projects/netezza-exporter/branches/production/endpoints/primary"
ACTOR = "smoke-test"

db = Database(LakebaseConfig(endpoint=ENDPOINT, w=WorkspaceClient(profile=PROFILE)), max_size=12)

repo.save_source(db, ACTOR, "netezza", "Netezza (simulado)", "fake", None, 3, 30,
                 {"min_s": 1, "max_s": 3, "fail_pct": 15})
for tid, tbl in (("FATO_VENDAS", "DW.FATO_VENDAS"), ("FATO_ESTOQUE", "DW.FATO_ESTOQUE")):
    if repo.get_table(db, tid) is None:
        repo.save_table(db, ACTOR, tid, "netezza", True, dict(
            source_table=tbl, procedure_name="ADMIN.SP_EXPORTA_TABELA", default_subblock_unit="MONTH",
            default_subblock_size=1, max_parallel=2, call_timeout_min=60, max_retries=1, retry_unknown=False,
            migration_start=date(2000, 1, 1), migration_end=date(2025, 12, 31),
            description="tabela de exemplo"))

tag = int(time.time())
year = 2000 + tag % 20  # ano diferente a cada execução para não colidir com execuções anteriores
tables = [repo.TableRequest("FATO_VENDAS", date(year, 1, 1), date(year, 6, 30), "MONTH", 1),
          repo.TableRequest("FATO_ESTOQUE", date(year, 1, 1), date(year, 1, 20), "DAY", 5)]
rid = repo.create_request(db, ACTOR, f"smoke {tag}", 3, None, "teste local", tables)
print("pedido", rid)

# período totalmente coberto por outro pedido: nada a executar
try:
    repo.create_request(db, ACTOR, "duplicado", 1, None, None,
                        [repo.TableRequest("FATO_VENDAS", date(year, 3, 15), date(year, 4, 15), "MONTH", 1)])
    raise SystemExit("ERRO: período já pedido não foi excluído")
except repo.NothingToRun:
    print("período já pedido excluído: nada a executar")

# sobreposição parcial: só o restante vira subbloco
partial = repo.review(db, [repo.TableRequest("FATO_VENDAS", date(year, 5, 1), date(year, 8, 31), "MONTH", 1)])[0]
assert [(b.start, b.end) for b in partial.blocks] == [(date(year, 7, 1), date(year, 7, 31)),
                                                      (date(year, 8, 1), date(year, 8, 31))], partial.blocks
print("sobreposição parcial: excluídos", partial.excluded_days, "dias; a executar", len(partial.blocks), "subblocos")

peak = {"n": 0}
stop = threading.Event()


def watch():
    while not stop.is_set():
        n = db.one("SELECT count(*) n FROM export_subblock WHERE status='RUNNING'")["n"]
        peak["n"] = max(peak["n"], n)
        time.sleep(0.5)


threading.Thread(target=watch, daemon=True).start()
stats = Dispatcher(db, run_id=-tag, idle_exit_s=5, poll_s=1).run()
stop.set()
print("stats", stats, "pico de concorrência", peak["n"])
assert peak["n"] <= 3, "limite global excedido"

req = repo.get_request(db, rid)
print("status", req["status"], req["status_reason"])
for t in repo.request_tables(db, rid):
    print(f"  {t['table_config_id']}: total={t['total']} done={t['done']} failed={t['failed']}")

if req["status"] == "FAILED":
    repo.resume_request(db, rid, ACTOR, retry_failed=True)
    repo.save_source(db, ACTOR, "netezza", "Netezza (simulado)", "fake", None, 3, 30, {"min_s": 1, "max_s": 2})
    Dispatcher(db, run_id=-tag - 1, idle_exit_s=5, poll_s=1).run()
    req = repo.get_request(db, rid)
    print("após retomada:", req["status"])
assert req["status"] == "COMPLETED", req["status"]
repo.save_source(db, ACTOR, "netezza", "Netezza (simulado)", "fake", None, 4, 30,
                 {"min_s": 3, "max_s": 12, "fail_pct": 0})
print("OK")
db.close()
