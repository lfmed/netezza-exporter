"""Operações no modelo de controle, compartilhadas entre o app e o dispatcher."""
import json
from dataclasses import dataclass, field
from datetime import date

import psycopg
from psycopg.types.range import Range

from .db import Database
from .planner import plan

LIVE = ("PENDING", "RUNNING", "DONE", "UNKNOWN")
ACTIVE_REQUEST = ("SUBMITTED", "RUNNING")
TERMINAL_REQUEST = ("STOPPED", "FAILED", "COMPLETED", "CANCELED")


class ConflictError(Exception):
    def __init__(self, conflicts: list[dict]):
        super().__init__(f"{len(conflicts)} conflito(s) de período")
        self.conflicts = conflicts


class StateError(Exception):
    pass


def log_event(conn, request_id, event_type: str, actor: str, details: dict | None = None, subblock_id=None):
    conn.execute(
        "INSERT INTO export_event (request_id, subblock_id, event_type, actor, details) VALUES (%s,%s,%s,%s,%s)",
        (request_id, subblock_id, event_type, actor, json.dumps(details, default=str) if details else None),
    )


# ---------------------------------------------------------------- catálogo

def list_sources(db: Database):
    return db.query("SELECT * FROM source_connection ORDER BY source_id")


def save_source(db: Database, actor: str, source_id: str, display_name: str, driver: str,
                secret_scope: str | None, max_sessions: int, connect_timeout_s: int, driver_options: dict):
    with db.tx() as conn:
        conn.execute("""
            INSERT INTO source_connection (source_id, display_name, driver, secret_scope, max_sessions,
                                           connect_timeout_s, driver_options, updated_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_id) DO UPDATE SET display_name = EXCLUDED.display_name, driver = EXCLUDED.driver,
              secret_scope = EXCLUDED.secret_scope, max_sessions = EXCLUDED.max_sessions,
              connect_timeout_s = EXCLUDED.connect_timeout_s, driver_options = EXCLUDED.driver_options,
              updated_by = EXCLUDED.updated_by, updated_at = now()""",
            (source_id, display_name, driver, secret_scope or None, max_sessions, connect_timeout_s,
             json.dumps(driver_options), actor))
        log_event(conn, None, "SOURCE_SAVED", actor, {"source_id": source_id, "driver": driver,
                                                     "max_sessions": max_sessions})


def list_tables(db: Database, only_active: bool = False):
    return db.query(f"""
        SELECT tc.table_config_id, tc.source_id, tc.active, tc.current_version, v.*
        FROM table_config tc JOIN table_config_version v
          ON v.table_config_id = tc.table_config_id AND v.config_version = tc.current_version
        {"WHERE tc.active" if only_active else ""}
        ORDER BY tc.table_config_id""")


def get_table(db: Database, table_config_id: str):
    return db.one("""
        SELECT tc.table_config_id, tc.source_id, tc.active, tc.current_version, v.*
        FROM table_config tc JOIN table_config_version v
          ON v.table_config_id = tc.table_config_id AND v.config_version = tc.current_version
        WHERE tc.table_config_id = %s""", (table_config_id,))


TABLE_VERSION_FIELDS = ("source_table", "procedure_name", "default_subblock_unit", "default_subblock_size",
                        "max_parallel", "call_timeout_min", "max_retries", "retry_unknown",
                        "migration_start", "migration_end", "description")


def save_table(db: Database, actor: str, table_config_id: str, source_id: str, active: bool, fields: dict):
    """Cria a tabela ou grava uma nova versão imutável da configuração."""
    vals = [fields.get(f) for f in TABLE_VERSION_FIELDS]
    with db.tx() as conn:
        cur = conn.execute("SELECT current_version FROM table_config WHERE table_config_id = %s FOR UPDATE",
                           (table_config_id,)).fetchone()
        if cur is None:
            version = 1
            conn.execute("INSERT INTO table_config (table_config_id, source_id, active, current_version) "
                         "VALUES (%s,%s,%s,1)", (table_config_id, source_id, active))
        else:
            version = cur["current_version"] + 1
        conn.execute(
            f"INSERT INTO table_config_version (table_config_id, config_version, {', '.join(TABLE_VERSION_FIELDS)}, created_by) "
            f"VALUES (%s, %s, {', '.join(['%s'] * len(TABLE_VERSION_FIELDS))}, %s)",
            (table_config_id, version, *vals, actor))
        conn.execute("UPDATE table_config SET current_version = %s, source_id = %s, active = %s "
                     "WHERE table_config_id = %s", (version, source_id, active, table_config_id))
        log_event(conn, None, "TABLE_CONFIG_SAVED", actor, {"table_config_id": table_config_id, "version": version})
    return version


def list_hooks(db: Database):
    return db.query("SELECT * FROM post_export_hook ORDER BY hook_id")


def save_hook(db: Database, actor: str, hook_id: str, description: str, table_config_id: str | None,
              trigger_level: str, job_id: int, enabled: bool):
    with db.tx() as conn:
        conn.execute("""
            INSERT INTO post_export_hook (hook_id, description, table_config_id, trigger_level, job_id, enabled)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (hook_id) DO UPDATE SET description = EXCLUDED.description,
              table_config_id = EXCLUDED.table_config_id, trigger_level = EXCLUDED.trigger_level,
              job_id = EXCLUDED.job_id, enabled = EXCLUDED.enabled""",
            (hook_id, description, table_config_id or None, trigger_level, job_id, enabled))
        log_event(conn, None, "HOOK_SAVED", actor, {"hook_id": hook_id, "job_id": job_id, "enabled": enabled})


# ---------------------------------------------------------------- pedidos

@dataclass
class TableRequest:
    table_config_id: str
    period_start: date
    period_end: date
    subblock_unit: str
    subblock_size: int
    calendar_aligned: bool = True
    is_reprocess: bool = False
    reprocess_reason: str | None = None
    # preenchidos na revisão
    config_version: int | None = None
    blocks: list = field(default_factory=list)
    conflicts: list = field(default_factory=list)
    superseded: list = field(default_factory=list)


def _daterange(start: date, end_inclusive: date) -> Range:
    return Range(start, end_inclusive, bounds="[]")


def find_overlaps(conn, table_config_id: str, start: date, end: date, statuses=LIVE) -> list[dict]:
    return conn.execute("""
        SELECT s.subblock_id, s.request_id, r.request_name, s.status,
               lower(s.data_range) AS data_ini, upper(s.data_range) - 1 AS data_fim
        FROM export_subblock s JOIN export_request r USING (request_id)
        WHERE s.table_config_id = %s AND s.data_range && %s AND s.status = ANY(%s)
        ORDER BY lower(s.data_range)""", (table_config_id, _daterange(start, end), list(statuses))).fetchall()


def review(db: Database, tables: list[TableRequest]) -> list[TableRequest]:
    """Planeja os subblocos e identifica conflitos sem gravar nada."""
    with db.pool.connection() as conn:
        for i, t in enumerate(tables):
            cfg = conn.execute("""
                SELECT tc.current_version, tc.active FROM table_config tc WHERE tc.table_config_id = %s""",
                (t.table_config_id,)).fetchone()
            if not cfg or not cfg["active"]:
                t.conflicts = [{"reason": "tabela não cadastrada ou inativa"}]
                continue
            t.config_version = cfg["current_version"]
            t.blocks = plan(t.period_start, t.period_end, t.subblock_unit, t.subblock_size, t.calendar_aligned)
            overlaps = find_overlaps(conn, t.table_config_id, t.period_start, t.period_end)
            if t.is_reprocess:
                t.superseded = [o for o in overlaps if o["status"] == "DONE"]
                overlaps = [o for o in overlaps if o["status"] != "DONE"]
            t.conflicts = [{**o, "reason": f"já existe subbloco {o['status']}"} for o in overlaps]
            for j, other in enumerate(tables[:i]):
                if (other.table_config_id == t.table_config_id
                        and other.period_start <= t.period_end and t.period_start <= other.period_end):
                    t.conflicts.append({"reason": f"sobrepõe a linha {j + 1} deste pedido"})
    return tables


def create_request(db: Database, actor: str, name: str, max_parallel: int, deadline_at, notes: str | None,
                   tables: list[TableRequest]) -> str:
    tables = review(db, tables)
    conflicts = [c | {"table_config_id": t.table_config_id} for t in tables for c in t.conflicts]
    if conflicts:
        raise ConflictError(conflicts)
    try:
        with db.tx() as conn:
            req = conn.execute("""
                INSERT INTO export_request (request_name, requested_by, status, max_parallel, deadline_at, notes)
                VALUES (%s,%s,'SUBMITTED',%s,%s,%s) RETURNING request_id""",
                (name, actor, max_parallel, deadline_at, notes)).fetchone()
            rid = req["request_id"]
            for ordinal, t in enumerate(tables, start=1):
                if t.is_reprocess:
                    sup = conn.execute("""
                        UPDATE export_subblock SET status = 'SUPERSEDED', updated_at = now()
                        WHERE table_config_id = %s AND data_range && %s AND status = 'DONE'
                        RETURNING subblock_id""", (t.table_config_id, _daterange(t.period_start, t.period_end))).fetchall()
                    log_event(conn, rid, "REPROCESS_SUPERSEDED", actor,
                              {"table_config_id": t.table_config_id, "subblocks": len(sup),
                               "reason": t.reprocess_reason})
                rt = conn.execute("""
                    INSERT INTO export_request_table (request_id, ordinal, table_config_id, config_version,
                      period_start, period_end, subblock_unit, subblock_size, calendar_aligned,
                      is_reprocess, reprocess_reason)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING request_table_id""",
                    (rid, ordinal, t.table_config_id, t.config_version, t.period_start, t.period_end,
                     t.subblock_unit, t.subblock_size, t.calendar_aligned, t.is_reprocess,
                     t.reprocess_reason)).fetchone()
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO export_subblock (request_table_id, request_id, table_config_id, seq,
                                                     data_range, status)
                        VALUES (%s,%s,%s,%s,%s,'PENDING')""",
                        [(rt["request_table_id"], rid, t.table_config_id, b.seq, _daterange(b.start, b.end))
                         for b in t.blocks])
            log_event(conn, rid, "REQUEST_SUBMITTED", actor,
                      {"tables": [t.table_config_id for t in tables],
                       "subblocks": sum(len(t.blocks) for t in tables), "max_parallel": max_parallel})
            return str(rid)
    except psycopg.errors.ExclusionViolation:
        # corrida com outro pedido gravado entre a revisão e o insert
        tables = review(db, tables)
        raise ConflictError([c | {"table_config_id": t.table_config_id} for t in tables for c in t.conflicts]
                            or [{"reason": "sobreposição detectada pelo banco"}])


def list_requests(db: Database, status: str | None = None, limit: int = 200):
    where = "WHERE r.status = %s" if status else ""
    params = (status, limit) if status else (limit,)
    return db.query(f"""
        SELECT r.*, p.total, p.done, p.running, p.pending, p.failed, p.unknown, p.tables
        FROM export_request r
        LEFT JOIN (SELECT request_id, sum(total) total, sum(done) done, sum(running) running,
                          sum(pending) pending, sum(failed) failed, sum(unknown) unknown,
                          string_agg(table_config_id, ', ' ORDER BY ordinal) tables
                   FROM v_request_table_progress GROUP BY request_id) p USING (request_id)
        {where}
        ORDER BY r.submitted_at DESC LIMIT %s""", params)


def get_request(db: Database, request_id: str):
    return db.one("SELECT * FROM export_request WHERE request_id = %s", (request_id,))


def request_tables(db: Database, request_id: str):
    return db.query("SELECT * FROM v_request_table_progress WHERE request_id = %s ORDER BY ordinal", (request_id,))


def request_subblocks(db: Database, request_id: str, status: str | None = None, limit: int = 2000):
    extra = "AND s.status = %s" if status else ""
    params = (request_id, status, limit) if status else (request_id, limit)
    return db.query(f"""
        SELECT s.*, lower(s.data_range) AS data_ini, upper(s.data_range) - 1 AS data_fim, rt.ordinal,
               a.databricks_run_id, a.error_class
        FROM export_subblock s
        JOIN export_request_table rt USING (request_table_id)
        LEFT JOIN LATERAL (SELECT databricks_run_id, error_class FROM export_attempt
                           WHERE subblock_id = s.subblock_id ORDER BY attempt_no DESC LIMIT 1) a ON true
        WHERE s.request_id = %s {extra}
        ORDER BY CASE s.status WHEN 'RUNNING' THEN 0 WHEN 'UNKNOWN' THEN 1 WHEN 'FAILED' THEN 2
                 WHEN 'PENDING' THEN 3 ELSE 4 END, rt.ordinal, s.seq
        LIMIT %s""", params)


def subblock_attempts(db: Database, subblock_id: str):
    return db.query("SELECT * FROM export_attempt WHERE subblock_id = %s ORDER BY attempt_no DESC", (subblock_id,))


def request_events(db: Database, request_id: str, limit: int = 200):
    return db.query("SELECT * FROM export_event WHERE request_id = %s ORDER BY event_time DESC LIMIT %s",
                    (request_id, limit))


def _set_request_status(conn, request_id, new_status: str, actor: str, allowed_from: tuple,
                        reason: str | None = None, extra_sql: str = "", extra_params: tuple = ()):
    row = conn.execute(f"""
        UPDATE export_request SET status = %s, status_reason = %s, updated_at = now() {extra_sql}
        WHERE request_id = %s AND status = ANY(%s) RETURNING status""",
        (new_status, reason, *extra_params, request_id, list(allowed_from))).fetchone()
    if row is None:
        cur = conn.execute("SELECT status FROM export_request WHERE request_id = %s", (request_id,)).fetchone()
        raise StateError(f"ação não permitida no estado {cur['status'] if cur else 'inexistente'}")
    log_event(conn, request_id, f"REQUEST_{new_status}", actor, {"reason": reason} if reason else None)


def stop_request(db: Database, request_id: str, actor: str):
    with db.tx() as conn:
        _set_request_status(conn, request_id, "STOPPING", actor, ACTIVE_REQUEST,
                            extra_sql=", stop_requested_at = now(), stop_requested_by = %s", extra_params=(actor,))


def resume_request(db: Database, request_id: str, actor: str, retry_failed: bool = True,
                   retry_unknown: bool = False):
    statuses = (["FAILED"] if retry_failed else []) + (["UNKNOWN"] if retry_unknown else [])
    with db.tx() as conn:
        _set_request_status(conn, request_id, "SUBMITTED", actor, ("STOPPED", "STOPPING", "FAILED"),
                            extra_sql=", finished_at = NULL, stop_requested_at = NULL")
        n = 0
        if statuses:
            n = len(conn.execute("""
                UPDATE export_subblock s SET status = 'PENDING', next_attempt_at = NULL, updated_at = now()
                WHERE s.request_id = %s AND s.status = ANY(%s)
                  AND NOT EXISTS (SELECT 1 FROM export_subblock o
                                  WHERE o.table_config_id = s.table_config_id AND o.subblock_id <> s.subblock_id
                                    AND o.status = ANY(%s) AND o.data_range && s.data_range)
                RETURNING s.subblock_id""", (request_id, statuses, list(LIVE))).fetchall())
        log_event(conn, request_id, "REQUEST_RESUMED", actor, {"requeued_subblocks": n, "statuses": statuses})


def cancel_request(db: Database, request_id: str, actor: str, reason: str | None = None):
    with db.tx() as conn:
        _set_request_status(conn, request_id, "CANCELED", actor,
                            ("SUBMITTED", "RUNNING", "STOPPING", "STOPPED", "FAILED"), reason=reason,
                            extra_sql=", finished_at = now()")
        n = conn.execute("""UPDATE export_subblock SET status = 'CANCELED', updated_at = now()
                            WHERE request_id = %s AND status = 'PENDING'""", (request_id,)).rowcount
        log_event(conn, request_id, "SUBBLOCKS_CANCELED", actor, {"count": n})


def set_parallelism(db: Database, request_id: str, actor: str, max_parallel: int):
    with db.tx() as conn:
        old = conn.execute("UPDATE export_request r SET max_parallel = %s, updated_at = now() "
                           "FROM (SELECT max_parallel FROM export_request WHERE request_id = %s) o "
                           "WHERE r.request_id = %s RETURNING o.max_parallel", (max_parallel, request_id, request_id)).fetchone()
        log_event(conn, request_id, "PARALLELISM_CHANGED", actor,
                  {"from": old["max_parallel"] if old else None, "to": max_parallel})


def retry_subblock(db: Database, subblock_id: str, actor: str):
    with db.tx() as conn:
        sb = conn.execute("""
            UPDATE export_subblock SET status = 'PENDING', next_attempt_at = NULL, updated_at = now()
            WHERE subblock_id = %s AND status IN ('FAILED','UNKNOWN','CANCELED') RETURNING request_id""",
            (subblock_id,)).fetchone()
        if sb is None:
            raise StateError("subbloco não está em FAILED/UNKNOWN/CANCELED")
        conn.execute("""UPDATE export_request SET status = 'SUBMITTED', finished_at = NULL, updated_at = now()
                        WHERE request_id = %s AND status IN ('FAILED','STOPPED','COMPLETED','CANCELED')""",
                     (sb["request_id"],))
        log_event(conn, sb["request_id"], "SUBBLOCK_RETRY", actor, subblock_id=subblock_id)


def resolve_unknown(db: Database, subblock_id: str, actor: str, done: bool, reason: str):
    """Reconciliação manual: o operador confirma no Netezza se a procedure concluiu."""
    new = "DONE" if done else "FAILED"
    with db.tx() as conn:
        sb = conn.execute("""
            UPDATE export_subblock SET status = %s, finished_at = coalesce(finished_at, now()),
                   last_error = CASE WHEN %s THEN last_error ELSE %s END, updated_at = now()
            WHERE subblock_id = %s AND status = 'UNKNOWN' RETURNING request_id""",
            (new, done, reason, subblock_id)).fetchone()
        if sb is None:
            raise StateError("subbloco não está em UNKNOWN")
        log_event(conn, sb["request_id"], f"SUBBLOCK_MARKED_{new}", actor, {"reason": reason}, subblock_id)


def migration_coverage(db: Database):
    return db.query("SELECT * FROM v_migration_coverage ORDER BY table_config_id")


def table_ranges(db: Database, table_config_id: str) -> dict[str, list[dict]]:
    """Intervalos (datas inclusivas) por situação dentro do escopo: lacunas, falhas e desconhecidos."""
    out = {}
    for kind, col in (("missing", "missing_ranges"), ("failed", "failed_ranges"), ("unknown", "unknown_ranges")):
        out[kind] = db.query(f"""
            SELECT lower(r) AS data_ini, upper(r) - 1 AS data_fim, upper(r) - lower(r) AS days
            FROM v_migration_coverage c, unnest(c.{col}::datemultirange) r
            WHERE c.table_config_id = %s ORDER BY lower(r)""", (table_config_id,))
    return out


def gap_rows(db: Database, table_ids: list[str]) -> list[dict]:
    """Linhas de pedido que cobrem exatamente as lacunas do escopo das tabelas escolhidas."""
    rows = []
    for tid in table_ids:
        cfg = get_table(db, tid)
        if not cfg:
            continue
        for g in table_ranges(db, tid)["missing"]:
            rows.append({"table": tid, "start": g["data_ini"].isoformat(), "end": g["data_fim"].isoformat(),
                         "size": str(cfg["default_subblock_size"]), "unit": cfg["default_subblock_unit"],
                         "aligned": "1", "reprocess": "0", "reason": ""})
    return rows


def live_dispatcher(db: Database):
    return db.one("""SELECT * FROM dispatcher_run WHERE finished_at IS NULL
                     AND heartbeat_at > now() - interval '3 minutes' ORDER BY started_at DESC LIMIT 1""")


def recent_dispatcher_runs(db: Database, limit: int = 10):
    return db.query("SELECT * FROM dispatcher_run ORDER BY started_at DESC LIMIT %s", (limit,))


def work_pending(db: Database) -> bool:
    return db.one("""SELECT EXISTS (SELECT 1 FROM export_subblock s JOIN export_request r USING (request_id)
                     WHERE s.status = 'PENDING' AND r.status IN ('SUBMITTED','RUNNING')) AS x""")["x"]
