"""Aplica o schema e concede acesso a roles adicionais (ex.: service principal do app)."""
from pathlib import Path

import psycopg

from .db import LakebaseConfig, SCHEMA

SCHEMA_SQL = Path(__file__).with_name("schema.sql")

SEED_SQL = """
INSERT INTO source_connection (source_id, display_name, driver, max_sessions, driver_options)
VALUES ('netezza', 'Netezza (simulado)', 'fake', 4, '{"min_s": 3, "max_s": 12, "fail_pct": 0}')
ON CONFLICT DO NOTHING;
"""


def apply_schema(conn: psycopg.Connection, seed: bool = True):
    conn.execute(SCHEMA_SQL.read_text())
    if seed:
        conn.execute(SEED_SQL)


def grant_role(conn: psycopg.Connection, role: str):
    r = psycopg.sql.Identifier(role)
    s = psycopg.sql.Identifier(SCHEMA)
    for stmt in (
        "GRANT USAGE ON SCHEMA {s} TO {r}",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {s} TO {r}",
        "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {s} TO {r}",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {r}",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT USAGE, SELECT ON SEQUENCES TO {r}",
    ):
        conn.execute(psycopg.sql.SQL(stmt).format(s=s, r=r))


def run(cfg: LakebaseConfig, grant_to: list[str] | None = None, seed: bool = True):
    with cfg.connect(autocommit=False) as conn:
        apply_schema(conn, seed=seed)
        for role in grant_to or []:
            exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
            if exists:
                grant_role(conn, role)
            else:
                print(f"[migrate] role {role} ainda não existe no Postgres; grant ignorado")
        conn.commit()
