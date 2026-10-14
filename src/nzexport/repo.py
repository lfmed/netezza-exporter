"""Operações no modelo de controle, compartilhadas entre o app e o dispatcher."""
import json
from dataclasses import dataclass, field
from datetime import date

import psycopg
from psycopg.types.range import Range

from .db import Database
from .planner import merge_contiguous, plan_pieces, subtract_ranges

# Status que reservam o período da tabela. FAILED reserva até o usuário repetir ou cancelar;
# CANCELED, SUPERSEDED e IMPORT_FAILED liberam o período para um novo pedido.
LIVE = ("PENDING", "RUNNING", "EXPORTED", "IMPORTING", "DONE", "UNKNOWN", "FAILED")
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


CONNECTION_FIELDS = ("display_name", "driver", "host", "port", "database", "security_level", "connect_timeout_s",
                     "max_sessions", "default_source_database", "default_ext_database", "default_ext_schema",
                     "landing_root", "landing_dat_folder", "landing_error_folder", "landing_work_folder",
                     "max_imports", "file_wait_min")


def get_source(db: Database, source_id: str):
    return db.one("SELECT * FROM source_connection WHERE source_id = %s", (source_id,))


def save_connection(db: Database, actor: str, source_id: str, fields: dict, secret_scope: str | None = None,
                    credentials_changed: bool = False):
    """Grava os parâmetros da conexão (sem credenciais; usuário/senha ficam no secret scope)."""
    vals = [fields.get(f) for f in CONNECTION_FIELDS]
    with db.tx() as conn:
        conn.execute(f"""
            INSERT INTO source_connection (source_id, {', '.join(CONNECTION_FIELDS)}, secret_scope, updated_by)
            VALUES (%s, {', '.join(['%s'] * len(CONNECTION_FIELDS))}, %s, %s)
            ON CONFLICT (source_id) DO UPDATE SET {', '.join(f'{f} = EXCLUDED.{f}' for f in CONNECTION_FIELDS)},
              secret_scope = coalesce(EXCLUDED.secret_scope, source_connection.secret_scope),
              updated_by = EXCLUDED.updated_by, updated_at = now()""",
            (source_id, *vals, secret_scope, actor))
        if credentials_changed:
            conn.execute("""UPDATE source_connection SET credentials_set_at = now(), credentials_set_by = %s
                            WHERE source_id = %s""", (actor, source_id))
        log_event(conn, None, "CONNECTION_SAVED", actor,
                  {"source_id": source_id, "driver": fields.get("driver"), "host": fields.get("host"),
                   "database": fields.get("database"), "max_sessions": fields.get("max_sessions"),
                   "credentials_changed": credentials_changed})


def create_connection_test(db: Database, source_id: str, actor: str) -> str:
    with db.tx() as conn:
        return str(conn.execute("INSERT INTO connection_test (source_id, requested_by) VALUES (%s, %s) RETURNING test_id",
                                (source_id, actor)).fetchone()["test_id"])


def update_connection_test(db: Database, test_id: str, status: str, details=None, job_run_id: int | None = None):
    with db.tx() as conn:
        conn.execute("""UPDATE connection_test SET status = %s, details = coalesce(%s, details),
                        job_run_id = coalesce(%s, job_run_id),
                        finished_at = CASE WHEN %s IN ('OK','FAILED') THEN now() ELSE finished_at END
                        WHERE test_id = %s""",
                     (status, json.dumps(details) if details is not None else None, job_run_id, status, test_id))


def connection_tests(db: Database, source_id: str, limit: int = 10):
    return db.query("SELECT * FROM connection_test WHERE source_id = %s ORDER BY requested_at DESC LIMIT %s",
                    (source_id, limit))


def procedure_checks(db: Database, source_id: str) -> list[tuple[str, str]]:
    """(banco, procedure) a verificar no teste: onde as tabelas desta conexão vão chamar a procedure."""
    src = get_source(db, source_id) or {}
    rows = db.query("""SELECT DISTINCT v.call_profile, v.ext_database, v.procedure_name
                       FROM table_config tc JOIN table_config_version v
                         ON v.table_config_id = tc.table_config_id AND v.config_version = tc.current_version
                       WHERE tc.source_id = %s AND tc.active""", (source_id,))
    out = set()
    for r in rows:
        db_name = (r["ext_database"] or src.get("default_ext_database")) if r["call_profile"] == "SP_CARGA_EVENTUAL" \
            else src.get("database")
        if db_name and r["procedure_name"]:
            out.add((db_name, r["procedure_name"]))
    return sorted(out)


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


TABLE_VERSION_FIELDS = ("call_profile", "source_database", "source_table", "ext_database", "ext_schema",
                        "time_column", "date_format", "procedure_name", "file_options",
                        "dest_catalog", "dest_schema", "dest_table",
                        "default_subblock_unit",
                        "default_subblock_size", "max_parallel", "call_timeout_min", "max_retries", "retry_unknown",
                        "migration_start", "migration_end", "description")


def save_table(db: Database, actor: str, table_config_id: str, source_id: str, active: bool, fields: dict):
    """Cria a tabela ou grava uma nova versão imutável da configuração."""
    with db.tx() as conn:
        return _save_table(conn, actor, table_config_id, source_id, active, fields)


def _save_table(conn, actor: str, table_config_id: str, source_id: str, active: bool, fields: dict,
                origin: str = "form") -> int:
    cur = conn.execute("SELECT current_version FROM table_config WHERE table_config_id = %s FOR UPDATE",
                       (table_config_id,)).fetchone()
    if fields.get("file_options") is None:
        # ex.: importação por CSV: mantém o formato do arquivo da versão atual
        prev = conn.execute("""SELECT v.file_options FROM table_config tc JOIN table_config_version v
                               ON v.table_config_id = tc.table_config_id AND v.config_version = tc.current_version
                               WHERE tc.table_config_id = %s""", (table_config_id,)).fetchone()
        fields = {**fields, "file_options": prev["file_options"] if prev else {}}
    vals = [json.dumps(fields.get(f) or {}) if f == "file_options" else fields.get(f) for f in TABLE_VERSION_FIELDS]
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
    log_event(conn, None, "TABLE_CONFIG_SAVED", actor,
              {"table_config_id": table_config_id, "version": version, "origin": origin})
    return version


def import_tables(db: Database, actor: str, rows: list, filename: str) -> dict:
    """Aplica as linhas válidas de um CSV numa única transação (tudo ou nada)."""
    counts = {"NOVA": 0, "NOVA_VERSAO": 0, "SEM_ALTERACAO": 0}
    with db.tx() as conn:
        for r in rows:
            counts[r.action] += 1
            if r.action in ("NOVA", "NOVA_VERSAO"):
                v = r.values
                _save_table(conn, actor, v["table_config_id"], v["source_id"], v["active"],
                            {f: v.get(f) for f in TABLE_VERSION_FIELDS}, origin=f"csv:{filename}")
        log_event(conn, None, "TABLES_CSV_IMPORTED", actor, {"file": filename, **counts})
    return counts


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
    pieces: list = field(default_factory=list)      # intervalos que serão executados
    blocks: list = field(default_factory=list)
    excluded: list = field(default_factory=list)    # períodos já executados/em execução, fora do lote
    conflicts: list = field(default_factory=list)
    superseded: list = field(default_factory=list)

    @property
    def excluded_days(self) -> int:
        return sum(e["days"] for e in self.excluded)


class NothingToRun(ValueError):
    pass


def _daterange(start: date, end_inclusive: date) -> Range:
    return Range(start, end_inclusive, bounds="[]")


def find_overlaps(conn, table_config_id: str, start: date, end: date, statuses=LIVE) -> list[dict]:
    return conn.execute("""
        SELECT s.subblock_id, s.request_id, r.request_name, s.status,
               lower(s.data_range) AS data_ini, upper(s.data_range) - 1 AS data_fim
        FROM export_subblock s JOIN export_request r USING (request_id)
        WHERE s.table_config_id = %s AND s.data_range && %s AND s.status = ANY(%s)
        ORDER BY lower(s.data_range)""", (table_config_id, _daterange(start, end), list(statuses))).fetchall()


def _group_excluded(overlaps: list[dict], start: date, end: date) -> list[dict]:
    """Agrupa subblocos sobrepostos por pedido/status em intervalos contíguos, recortados ao período pedido."""
    groups: dict[tuple, list] = {}
    for o in overlaps:
        key = (str(o["request_id"]), o["request_name"], o["status"])
        groups.setdefault(key, []).append((max(o["data_ini"], start), min(o["data_fim"], end)))
    out = []
    for (rid, name, status), ranges in groups.items():
        for a, b in merge_contiguous(ranges):
            out.append({"request_id": rid, "request_name": name, "status": status,
                        "data_ini": a, "data_fim": b, "days": (b - a).days + 1})
    return sorted(out, key=lambda e: e["data_ini"])


def review(db: Database, tables: list[TableRequest]) -> list[TableRequest]:
    """Planeja os subblocos sem gravar nada.

    Períodos da tabela já concluídos, em execução ou com resultado desconhecido (em qualquer pedido)
    são mostrados e excluídos do lote; só o restante vira subbloco. Em reprocessamento, os concluídos
    entram no lote e serão substituídos; os em execução continuam excluídos.
    """
    with db.pool.connection() as conn:
        for i, t in enumerate(tables):
            t.pieces, t.blocks, t.excluded, t.conflicts, t.superseded = [], [], [], [], []
            cfg = conn.execute("SELECT current_version, active FROM table_config WHERE table_config_id = %s",
                               (t.table_config_id,)).fetchone()
            if not cfg or not cfg["active"]:
                t.conflicts = [{"reason": "tabela não cadastrada ou inativa"}]
                continue
            t.config_version = cfg["current_version"]
            overlaps = find_overlaps(conn, t.table_config_id, t.period_start, t.period_end)
            if t.is_reprocess:
                t.superseded = _group_excluded([o for o in overlaps if o["status"] == "DONE"],
                                               t.period_start, t.period_end)
                overlaps = [o for o in overlaps if o["status"] != "DONE"]
            t.excluded = _group_excluded(overlaps, t.period_start, t.period_end)
            # linhas anteriores do mesmo pedido para a mesma tabela também ficam de fora
            for j, other in enumerate(tables[:i]):
                if other.table_config_id != t.table_config_id:
                    continue
                for a, b in other.pieces:
                    a, b = max(a, t.period_start), min(b, t.period_end)
                    if a <= b:
                        t.excluded.append({"request_id": None, "request_name": f"linha {j + 1} deste pedido",
                                           "status": "THIS_REQUEST", "data_ini": a, "data_fim": b,
                                           "days": (b - a).days + 1})
            t.excluded.sort(key=lambda e: e["data_ini"])
            t.pieces = subtract_ranges(t.period_start, t.period_end,
                                       [(e["data_ini"], e["data_fim"]) for e in t.excluded])
            t.blocks = plan_pieces(t.pieces, t.subblock_unit, t.subblock_size, t.calendar_aligned)
    return tables


def create_request(db: Database, actor: str, name: str, max_parallel: int, deadline_at, notes: str | None,
                   tables: list[TableRequest], scheduled_start_at=None) -> str:
    if scheduled_start_at and deadline_at and deadline_at <= scheduled_start_at:
        raise ValueError("o prazo/janela precisa ser depois do início agendado")
    for attempt in range(2):
        tables = review(db, tables)
        conflicts = [c | {"table_config_id": t.table_config_id} for t in tables for c in t.conflicts]
        if conflicts:
            raise ConflictError(conflicts)
        if not any(t.blocks for t in tables):
            raise NothingToRun("todos os períodos solicitados já foram executados ou estão em execução")
        try:
            return _insert_request(db, actor, name, max_parallel, deadline_at, notes, tables, scheduled_start_at)
        except psycopg.errors.ExclusionViolation:
            # outro pedido gravou um período entre a revisão e o insert: revisa de novo uma vez
            if attempt:
                raise ConflictError([{"reason": "sobreposição detectada pelo banco; revise o pedido"}])
    raise AssertionError("inalcançável")


def _insert_request(db, actor, name, max_parallel, deadline_at, notes, tables: list[TableRequest],
                    scheduled_start_at=None) -> str:
    with db.tx() as conn:
        req = conn.execute("""
            INSERT INTO export_request (request_name, requested_by, status, max_parallel, deadline_at, notes,
                                        scheduled_start_at)
            VALUES (%s,%s,'SUBMITTED',%s,%s,%s,%s) RETURNING request_id""",
            (name, actor, max_parallel, deadline_at, notes, scheduled_start_at)).fetchone()
        rid = req["request_id"]
        ordinal = 0
        for t in tables:
            if not t.blocks:
                continue
            ordinal += 1
            if t.is_reprocess:
                sup = conn.execute("""
                    UPDATE export_subblock SET status = 'SUPERSEDED', updated_at = now()
                    WHERE table_config_id = %s AND data_range && %s AND status = 'DONE'
                    RETURNING subblock_id""", (t.table_config_id, _daterange(t.period_start, t.period_end))).fetchall()
                log_event(conn, rid, "REPROCESS_SUPERSEDED", actor,
                          {"table_config_id": t.table_config_id, "subblocks": len(sup), "reason": t.reprocess_reason})
            rt = conn.execute("""
                INSERT INTO export_request_table (request_id, ordinal, table_config_id, config_version,
                  period_start, period_end, subblock_unit, subblock_size, calendar_aligned,
                  is_reprocess, reprocess_reason, excluded_ranges)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING request_table_id""",
                (rid, ordinal, t.table_config_id, t.config_version, t.period_start, t.period_end,
                 t.subblock_unit, t.subblock_size, t.calendar_aligned, t.is_reprocess, t.reprocess_reason,
                 json.dumps(t.excluded, default=str))).fetchone()
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO export_subblock (request_table_id, request_id, table_config_id, seq, data_range, status)
                    VALUES (%s,%s,%s,%s,%s,'PENDING')""",
                    [(rt["request_table_id"], rid, t.table_config_id, b.seq, _daterange(b.start, b.end))
                     for b in t.blocks])
        log_event(conn, rid, "REQUEST_SUBMITTED", actor,
                  {"tables": [t.table_config_id for t in tables if t.blocks],
                   "skipped_tables": [t.table_config_id for t in tables if not t.blocks],
                   "subblocks": sum(len(t.blocks) for t in tables),
                   "excluded_days": sum(t.excluded_days for t in tables), "max_parallel": max_parallel,
                   "scheduled_start_at": scheduled_start_at})
        return str(rid)


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
                 WHEN 'PENDING' THEN 3 ELSE 4 END, rt.ordinal, lower(s.data_range) DESC
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
                            WHERE request_id = %s AND status IN ('PENDING','FAILED')""", (request_id,)).rowcount
        log_event(conn, request_id, "SUBBLOCKS_CANCELED", actor, {"count": n})


def reschedule_request(db: Database, request_id: str, actor: str, when):
    """Altera (ou remove, com when=None) o início agendado de um pedido que ainda não começou."""
    with db.tx() as conn:
        row = conn.execute("""
            UPDATE export_request r SET scheduled_start_at = %s, updated_at = now()
            FROM (SELECT scheduled_start_at FROM export_request WHERE request_id = %s) o
            WHERE r.request_id = %s AND r.status = 'SUBMITTED' AND r.started_at IS NULL
              AND (r.deadline_at IS NULL OR %s::timestamptz IS NULL OR r.deadline_at > %s::timestamptz)
            RETURNING o.scheduled_start_at AS old""", (when, request_id, request_id, when, when)).fetchone()
        if row is None:
            raise StateError("só é possível agendar pedidos que ainda não começaram, com início antes do prazo")
        log_event(conn, request_id, "REQUEST_RESCHEDULED", actor, {"from": row["old"], "to": when})


def set_parallelism(db: Database, request_id: str, actor: str, max_parallel: int):
    with db.tx() as conn:
        old = conn.execute("UPDATE export_request r SET max_parallel = %s, updated_at = now() "
                           "FROM (SELECT max_parallel FROM export_request WHERE request_id = %s) o "
                           "WHERE r.request_id = %s RETURNING o.max_parallel", (max_parallel, request_id, request_id)).fetchone()
        log_event(conn, request_id, "PARALLELISM_CHANGED", actor,
                  {"from": old["max_parallel"] if old else None, "to": max_parallel})


def _refresh_request_status(conn, request_id, actor: str):
    """Após decisões manuais em subblocos, ajusta o status de um pedido já encerrado pelo dispatcher."""
    c = conn.execute("""
        SELECT r.status,
               count(*) FILTER (WHERE s.status IN ('PENDING','RUNNING')) AS active,
               count(*) FILTER (WHERE s.status IN ('FAILED','UNKNOWN')) AS bad,
               count(*) FILTER (WHERE s.status = 'CANCELED') AS canceled
        FROM export_request r LEFT JOIN export_subblock s USING (request_id)
        WHERE r.request_id = %s GROUP BY r.status""", (request_id,)).fetchone()
    if not c or c["status"] not in ("FAILED", "COMPLETED") or c["active"]:
        return
    new = "FAILED" if c["bad"] else "COMPLETED"
    reason = (f"{c['bad']} subbloco(s) com falha ou estado desconhecido" if c["bad"] else
              f"{c['canceled']} subbloco(s) cancelado(s): período liberado para novo pedido" if c["canceled"] else None)
    conn.execute("UPDATE export_request SET status = %s, status_reason = %s, updated_at = now() WHERE request_id = %s",
                 (new, reason, request_id))
    if new != c["status"]:
        log_event(conn, request_id, f"REQUEST_{new}", actor, {"reason": reason})


def retry_subblock(db: Database, subblock_id: str, actor: str):
    """Repete um subbloco com falha ou desconhecido. Cancelado é definitivo no pedido: o período só volta
    a ser executado em um novo pedido."""
    try:
        with db.tx() as conn:
            sb = conn.execute("""
                UPDATE export_subblock s SET status = 'PENDING', next_attempt_at = NULL, updated_at = now()
                FROM export_request r
                WHERE s.subblock_id = %s AND s.status IN ('FAILED','UNKNOWN')
                  AND r.request_id = s.request_id AND r.status <> 'CANCELED'
                RETURNING s.request_id""", (subblock_id,)).fetchone()
            if sb is None:
                cur = conn.execute("SELECT status FROM export_subblock WHERE subblock_id = %s", (subblock_id,)).fetchone()
                if cur and cur["status"] == "CANCELED":
                    raise StateError("subbloco cancelado não pode ser repetido neste pedido; "
                                     "o período está livre para um novo pedido")
                raise StateError("só é possível repetir subblocos com falha ou estado desconhecido "
                                 "de pedidos não encerrados")
            conn.execute("""UPDATE export_request SET status = 'SUBMITTED', finished_at = NULL, updated_at = now()
                            WHERE request_id = %s AND status IN ('FAILED','STOPPED','COMPLETED')""",
                         (sb["request_id"],))
            log_event(conn, sb["request_id"], "SUBBLOCK_RETRY", actor, subblock_id=subblock_id)
    except psycopg.errors.ExclusionViolation:
        raise StateError("o período deste subbloco já faz parte de outro pedido; não é possível repeti-lo aqui") from None


def retry_failed(db: Database, request_id: str, actor: str) -> int:
    """Recoloca na fila todos os subblocos com falha do pedido."""
    with db.tx() as conn:
        n = len(conn.execute("""
            UPDATE export_subblock SET status = 'PENDING', next_attempt_at = NULL, updated_at = now()
            WHERE request_id = %s AND status = 'FAILED' RETURNING subblock_id""", (request_id,)).fetchall())
        if n:
            conn.execute("""UPDATE export_request SET status = 'SUBMITTED', finished_at = NULL, updated_at = now()
                            WHERE request_id = %s AND status IN ('FAILED','STOPPED','COMPLETED')""", (request_id,))
            log_event(conn, request_id, "FAILED_SUBBLOCKS_RETRY", actor, {"count": n})
        return n


def cancel_failed(db: Database, request_id: str, actor: str, reason: str, subblock_id: str | None = None) -> int:
    """Cancela subblocos com falha (um ou todos do pedido): o período fica livre para um novo pedido."""
    with db.tx() as conn:
        extra = "AND subblock_id = %s" if subblock_id else ""
        params = (request_id, subblock_id) if subblock_id else (request_id,)
        rows = conn.execute(f"""
            UPDATE export_subblock SET status = 'CANCELED', next_attempt_at = NULL, updated_at = now(),
                   last_error = coalesce(last_error, '') || ' | cancelado: ' || %s
            WHERE request_id = %s AND status = 'FAILED' {extra}
            RETURNING subblock_id, lower(data_range) AS data_ini, upper(data_range) - 1 AS data_fim""",
            (reason, *params)).fetchall()
        if not rows:
            raise StateError("nenhum subbloco com falha para cancelar")
        log_event(conn, request_id, "FAILED_SUBBLOCKS_CANCELED", actor,
                  {"count": len(rows), "reason": reason,
                   "periods": [f"{r['data_ini']}..{r['data_fim']}" for r in rows][:50]},
                  subblock_id=subblock_id)
        _refresh_request_status(conn, request_id, actor)
        return len(rows)


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


# Condição de subbloco pronto para executar agora (usada pelo dispatcher e pelo agendador do app)
DUE_NOW_SQL = """s.status = 'PENDING' AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= now())
                  AND r.status IN ('SUBMITTED', 'RUNNING')
                  AND (r.scheduled_start_at IS NULL OR r.scheduled_start_at <= now())
                  AND (r.deadline_at IS NULL OR r.deadline_at > now())"""


def work_due(conn) -> bool:
    return conn.execute(f"""SELECT EXISTS (SELECT 1 FROM export_subblock s JOIN export_request r USING (request_id)
                            JOIN table_config tc ON tc.table_config_id = s.table_config_id
                            WHERE {DUE_NOW_SQL} AND tc.active) AS x""").fetchone()["x"]
