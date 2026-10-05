"""Aplica o schema no Lakebase a partir da máquina local (como dono do projeto).

Uso: .venv/bin/python scripts/migrate_local.py [--grant-role <sp_client_id>]
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.sdk import WorkspaceClient  # noqa: E402

from nzexport import migrate  # noqa: E402
from nzexport.db import LakebaseConfig  # noqa: E402

PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "fe-vm-stable-classic-pg4xe1")
ENDPOINT = "projects/netezza-exporter/branches/production/endpoints/primary"

p = argparse.ArgumentParser()
p.add_argument("--grant-role", action="append", default=[])
a = p.parse_args()
cfg = LakebaseConfig(endpoint=ENDPOINT, w=WorkspaceClient(profile=PROFILE))
migrate.run(cfg, grant_to=a.grant_role)
with cfg.connect() as conn:
    print([r["table_name"] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='nzexport' ORDER BY 1").fetchall()])
