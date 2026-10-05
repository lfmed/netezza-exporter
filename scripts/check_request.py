"""Mostra status, eventos e subblocos de um pedido. Uso: check_request.py <request_id>"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from databricks.sdk import WorkspaceClient  # noqa: E402

from nzexport import repo  # noqa: E402
from nzexport.db import Database, LakebaseConfig  # noqa: E402

db = Database(LakebaseConfig(endpoint="projects/netezza-exporter/branches/production/endpoints/primary",
                             w=WorkspaceClient(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "fe-vm-stable-classic-pg4xe1"))))
rid = sys.argv[1]
r = repo.get_request(db, rid)
print("status:", r["status"], "| agendado:", r["scheduled_start_at"], "| início:", r["started_at"], "| fim:", r["finished_at"])
for s in repo.request_subblocks(db, rid):
    print("  ", s["data_ini"], s["status"], s["started_at"])
for e in db.query("""SELECT event_time, event_type, actor, details FROM export_event
                     WHERE request_id = %s OR (event_type = 'DISPATCHER_AUTO_TRIGGERED' AND event_time > %s)
                     ORDER BY event_time""", (rid, r["submitted_at"])):
    print("  ev", e["event_time"], e["event_type"], e["actor"], e["details"] or "")
db.close()
