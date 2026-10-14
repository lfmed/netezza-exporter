"""Fechamento de pedidos e hooks pós-exportação: usados pelo dispatcher e pelo importador."""
from databricks.sdk import WorkspaceClient

from .db import Database
from .repo import log_event

ACTOR = "dispatcher"
# subblocos ainda em andamento: chamada da procedure ou importação do .dat
ACTIVE = ("RUNNING", "EXPORTED", "IMPORTING")


def finalize_requests(db: Database):
    with db.tx() as conn:
        rows = conn.execute("""
            SELECT r.request_id, r.status, (r.deadline_at IS NOT NULL AND r.deadline_at <= now()) AS expired,
                   c.running, c.pending, c.bad, c.done, c.canceled, c.import_failed
            FROM export_request r
            CROSS JOIN LATERAL (
              SELECT count(*) FILTER (WHERE s.status IN ('RUNNING','EXPORTED','IMPORTING')) AS running,
                     count(*) FILTER (WHERE s.status = 'IMPORT_FAILED') AS import_failed,
                     count(*) FILTER (WHERE s.status = 'PENDING') AS pending,
                     count(*) FILTER (WHERE s.status IN ('FAILED','UNKNOWN')) AS bad,
                     count(*) FILTER (WHERE s.status = 'DONE') AS done,
                     count(*) FILTER (WHERE s.status = 'CANCELED') AS canceled
              FROM export_subblock s WHERE s.request_id = r.request_id) c
            WHERE r.status IN ('SUBMITTED','RUNNING','STOPPING')
            FOR UPDATE OF r SKIP LOCKED""").fetchall()
        for r in rows:
            new, reason = None, None
            if r["running"] > 0:
                continue
            if r["status"] == "STOPPING":
                new, reason = "STOPPED", "parado pelo usuário"
            elif r["pending"] == 0:
                new = "COMPLETED" if r["bad"] == 0 and r["import_failed"] == 0 else "FAILED"
                parts = []
                if r["bad"]:
                    parts.append(f"{r['bad']} subbloco(s) com falha ou estado desconhecido")
                if r["import_failed"]:
                    parts.append(f"{r['import_failed']} subbloco(s) com falha na importação "
                                 "(período limpo no destino e liberado para novo pedido)")
                if r["canceled"] and new == "COMPLETED":
                    parts.append(f"{r['canceled']} subbloco(s) cancelado(s)")
                reason = "; ".join(parts) or None
            elif r["expired"]:
                new, reason = "STOPPED", "janela operacional encerrada"
            if new:
                conn.execute("""UPDATE export_request SET status = %s, status_reason = %s, finished_at = now(),
                                updated_at = now() WHERE request_id = %s""", (new, reason, r["request_id"]))
                log_event(conn, r["request_id"], f"REQUEST_{new}", ACTOR,
                          {"done": r["done"], "bad": r["bad"], "pending": r["pending"], "reason": reason})


def fire_hooks(db: Database, w: WorkspaceClient | None):
    if w is None:
        return
    due = db.query("""
        WITH rt_done AS (
          SELECT p.request_id, p.request_table_id, p.table_config_id, p.period_start, p.period_end,
                 p.last_finished_at
          FROM v_request_table_progress p
          -- só dispara com a tabela inteira concluída (subbloco cancelado = período não exportado)
          WHERE p.total > p.canceled AND p.done = p.total - p.canceled AND p.canceled_only = 0
            AND p.import_failed = 0)
        SELECT h.hook_id, h.job_id, h.trigger_level, d.request_id, d.request_table_id, d.table_config_id,
               d.period_start, d.period_end
        FROM post_export_hook h JOIN rt_done d
          ON h.trigger_level = 'REQUEST_TABLE' AND (h.table_config_id IS NULL OR h.table_config_id = d.table_config_id)
        -- só cargas concluídas depois da criação do hook (não dispara retroativamente)
        WHERE h.enabled AND d.last_finished_at >= h.created_at
        UNION ALL
        SELECT h.hook_id, h.job_id, h.trigger_level, r.request_id, NULL, NULL, NULL, NULL
        FROM post_export_hook h JOIN export_request r ON r.status = 'COMPLETED'
        WHERE h.enabled AND h.trigger_level = 'REQUEST' AND r.finished_at >= h.created_at
          AND (h.table_config_id IS NULL OR EXISTS (SELECT 1 FROM export_request_table rt
               WHERE rt.request_id = r.request_id AND rt.table_config_id = h.table_config_id))
    """)
    for d in due:
        with db.tx() as conn:
            inv = conn.execute("""
                INSERT INTO hook_invocation (hook_id, request_id, request_table_id, status)
                VALUES (%s,%s,%s,'TRIGGERED') ON CONFLICT DO NOTHING RETURNING invocation_id""",
                (d["hook_id"], d["request_id"], d["request_table_id"])).fetchone()
        if inv is None:
            continue
        params = {"request_id": str(d["request_id"]), "trigger_level": d["trigger_level"],
                  "request_table_id": str(d["request_table_id"] or ""),
                  "table_config_id": d["table_config_id"] or "",
                  "data_ini": str(d["period_start"] or ""), "data_fim": str(d["period_end"] or "")}
        try:
            run = w.jobs.run_now(job_id=d["job_id"], job_parameters=params)
            with db.tx() as conn:
                conn.execute("UPDATE hook_invocation SET job_run_id = %s WHERE invocation_id = %s",
                             (run.run_id, inv["invocation_id"]))
                log_event(conn, d["request_id"], "HOOK_TRIGGERED", ACTOR,
                          {"hook_id": d["hook_id"], "job_id": d["job_id"], "run_id": run.run_id, **params})
        except Exception as e:  # noqa: BLE001 - falha no hook não afeta a exportação
            with db.tx() as conn:
                conn.execute("UPDATE hook_invocation SET status = 'FAILED', error_message = %s "
                             "WHERE invocation_id = %s", (str(e)[:2000], inv["invocation_id"]))
                log_event(conn, d["request_id"], "HOOK_FAILED", ACTOR, {"hook_id": d["hook_id"], "error": str(e)[:500]})


def live_run(db: Database, kind: str):
    return db.one("""SELECT * FROM dispatcher_run WHERE kind = %s AND finished_at IS NULL
                     AND heartbeat_at > now() - interval '3 minutes' ORDER BY started_at DESC LIMIT 1""", (kind,))


def import_due(conn) -> bool:
    return conn.execute("""SELECT EXISTS (SELECT 1 FROM export_subblock s
                           WHERE s.status = 'EXPORTED' OR (s.status = 'IMPORTING' AND s.lease_expires_at < now()))
                           AS x""").fetchone()["x"]


def ensure_importer(db: Database, w: WorkspaceClient, job_id: int) -> int | None:
    """Dispara o importador se não houver um ativo. Retorna o run_id disparado."""
    if live_run(db, "importer") is not None:
        return None
    if list(w.jobs.list_runs(job_id=job_id, active_only=True)):
        return None
    return w.jobs.run_now(job_id=job_id).run_id
