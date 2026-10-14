"""Importador: carrega o .dat de cada subbloco exportado na tabela destino (Delta) e apaga o arquivo.

Regras (definidas com o cliente):
  * APPEND puro, sem validação de período ou conteúdo: o app garante que os subblocos não se sobrepõem;
  * só a estrutura é conferida (header × colunas da tabela; linha ilegível aborta, FAILFAST);
  * sucesso: subbloco DONE e o .dat é apagado;
  * erro: o período do subbloco é LIMPO no destino, o .dat vai para a pasta de erro e o subbloco fica
    IMPORT_FAILED, o que libera o período para um novo pedido;
  * no máximo uma importação por tabela destino ao mesmo tempo (commits Delta na mesma tabela em paralelo
    conflitam); tabelas diferentes em paralelo, até o limite de importações da conexão.
"""
import logging
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from . import file_format, lifecycle
from . import motor_dat as m
from .db import Database
from .repo import log_event

log = logging.getLogger("nzexport.importer")
ACTOR = "importador"
MAX_WORKERS = 32
LEASE_S = 300
RENEW_EVERY_S = 30


@dataclass
class ImportWork:
    subblock_id: str
    request_id: str
    table_config_id: str
    source_id: str
    attempt_no: int
    import_attempt_id: str
    dest: str
    time_column: str | None
    file_options: dict
    is_reprocess: bool
    ini: str
    fim: str
    file_path: str
    error_folder: str
    work_folder: str
    file_wait_min: int


class Importer:
    def __init__(self, db: Database, run_id: int, spark, dbutils, w: WorkspaceClient | None = None,
                 idle_exit_s: int = 120, max_runtime_s: int = 12 * 3600, poll_s: float = 5.0):
        self.db, self.run_id, self.w = db, run_id, w
        self.owner = f"imp-{run_id}"
        self.idle_exit_s, self.max_runtime_s, self.poll_s = idle_exit_s, max_runtime_s, poll_s
        self.pool = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="import")
        self.in_flight: dict[str, ImportWork] = {}
        self.lock = threading.Lock()
        m.iniciar(spark, dbutils, pasta_limpeza="/tmp/nzexport-limpeza")

    # ------------------------------------------------------------ ciclo
    def run(self) -> dict:
        started, idle_since, last_renew, stats = time.time(), None, 0.0, Counter()
        self._register()
        try:
            while True:
                self._reap_expired(stats)
                draining = time.time() - started > self.max_runtime_s
                if not draining:
                    for wk in self._claim(MAX_WORKERS - len(self.in_flight)):
                        with self.lock:
                            self.in_flight[wk.subblock_id] = wk
                        stats["claimed"] += 1
                        self.pool.submit(self._process, wk, stats)
                if time.time() - last_renew > RENEW_EVERY_S:
                    self._renew()
                    last_renew = time.time()
                    lifecycle.finalize_requests(self.db)
                    lifecycle.fire_hooks(self.db, self.w)
                if self.in_flight or (not draining and self._has_work()):
                    idle_since = None
                else:
                    idle_since = idle_since or time.time()
                    if draining or time.time() - idle_since >= self.idle_exit_s:
                        break
                time.sleep(self.poll_s)
        finally:
            self.pool.shutdown(wait=True)
            lifecycle.finalize_requests(self.db)
            lifecycle.fire_hooks(self.db, self.w)
            with self.db.tx() as conn:
                conn.execute("""UPDATE dispatcher_run SET finished_at = now(), status = 'FINISHED', in_flight = 0
                                WHERE run_id = %s""", (self.run_id,))
        log.info("importador finalizado: %s", dict(stats))
        return dict(stats)

    def _register(self):
        with self.db.tx() as conn:
            conn.execute("""INSERT INTO dispatcher_run (run_id, kind) VALUES (%s, 'importer')
                            ON CONFLICT (run_id) DO UPDATE SET heartbeat_at = now(), finished_at = NULL,
                            status = 'RUNNING'""", (self.run_id,))

    def _renew(self):
        with self.lock:
            ids = list(self.in_flight)
        with self.db.tx() as conn:
            conn.execute("UPDATE dispatcher_run SET heartbeat_at = now(), in_flight = %s WHERE run_id = %s",
                         (len(ids), self.run_id))
            if ids:
                conn.execute(f"""UPDATE export_subblock SET lease_expires_at = now() + interval '{LEASE_S} seconds'
                                 WHERE subblock_id = ANY(%s) AND status = 'IMPORTING' AND lease_owner = %s""",
                             (ids, self.owner))

    def _has_work(self) -> bool:
        with self.db.pool.connection() as conn:
            return lifecycle.import_due(conn)

    def _reap_expired(self, stats: Counter):
        """Importador anterior morreu no meio: volta para EXPORTED (a retomada pelo userMetadata evita duplicar)."""
        with self.db.tx() as conn:
            rows = conn.execute("""
                UPDATE export_subblock SET status = 'EXPORTED', lease_owner = NULL, lease_expires_at = NULL,
                       updated_at = now()
                WHERE status = 'IMPORTING' AND lease_expires_at < now()
                  AND NOT (lease_owner = %s AND subblock_id = ANY(%s))
                RETURNING subblock_id, request_id, import_attempts""", (self.owner, list(self.in_flight))).fetchall()
            for r in rows:
                conn.execute("""UPDATE export_import_attempt SET status = 'ABANDONED', finished_at = now()
                                WHERE subblock_id = %s AND attempt_no = %s AND status = 'RUNNING'""",
                             (r["subblock_id"], r["import_attempts"]))
                log_event(conn, r["request_id"], "IMPORT_LEASE_EXPIRED", ACTOR, None, r["subblock_id"])
                stats["reaped"] += 1

    # ------------------------------------------------------------ claim
    def _claim(self, free: int) -> list[ImportWork]:
        if free <= 0:
            return []
        with self.db.tx() as conn:
            sources = {s["source_id"]: s for s in conn.execute("SELECT * FROM source_connection FOR UPDATE").fetchall()}
            running = conn.execute("""
                SELECT tc.source_id, v.dest_catalog || '.' || v.dest_schema || '.' || v.dest_table AS dest, count(*) n
                FROM export_subblock s JOIN export_request_table rt USING (request_table_id)
                JOIN table_config tc ON tc.table_config_id = s.table_config_id
                JOIN table_config_version v ON v.table_config_id = rt.table_config_id AND v.config_version = rt.config_version
                WHERE s.status = 'IMPORTING' GROUP BY 1, 2""").fetchall()
            by_source, by_dest = Counter(), Counter()
            for r in running:
                by_source[r["source_id"]] += r["n"]
                by_dest[r["dest"]] += r["n"]
            cands = conn.execute("""
                SELECT s.subblock_id, s.request_id, s.table_config_id, s.import_attempts, s.file_name,
                       to_char(lower(s.data_range), 'YYYYMMDD') AS ini, to_char(upper(s.data_range) - 1, 'YYYYMMDD') AS fim,
                       tc.source_id, rt.is_reprocess, v.time_column, v.file_options,
                       v.dest_catalog || '.' || v.dest_schema || '.' || v.dest_table AS dest
                FROM export_subblock s
                JOIN export_request r USING (request_id)
                JOIN export_request_table rt USING (request_table_id)
                JOIN table_config tc ON tc.table_config_id = s.table_config_id
                JOIN table_config_version v ON v.table_config_id = rt.table_config_id AND v.config_version = rt.config_version
                WHERE s.status = 'EXPORTED'
                ORDER BY r.submitted_at, rt.ordinal, lower(s.data_range) DESC
                LIMIT 500
                FOR UPDATE OF s SKIP LOCKED""").fetchall()
            works = []
            for c in cands:
                if len(works) >= free:
                    break
                src = sources.get(c["source_id"])
                if (src is None or by_source[c["source_id"]] >= (src["max_imports"] or 1) or by_dest[c["dest"]] >= 1
                        or not src.get("landing_root") or not c["file_name"]):
                    continue
                by_source[c["source_id"]] += 1
                by_dest[c["dest"]] += 1
                n = c["import_attempts"] + 1
                root = src["landing_root"].rstrip("/")
                path = f"{root}/{src['landing_dat_folder'].strip('/')}/{c['file_name']}"
                conn.execute(f"""UPDATE export_subblock SET status = 'IMPORTING', import_attempts = %s, lease_owner = %s,
                                 lease_expires_at = now() + interval '{LEASE_S} seconds', updated_at = now()
                                 WHERE subblock_id = %s""", (n, self.owner, c["subblock_id"]))
                att = conn.execute("""INSERT INTO export_import_attempt (subblock_id, attempt_no, databricks_run_id,
                                      status, file_path, dest_table) VALUES (%s,%s,%s,'RUNNING',%s,%s)
                                      RETURNING import_attempt_id""",
                                   (c["subblock_id"], n, self.run_id, path, c["dest"])).fetchone()
                works.append(ImportWork(
                    str(c["subblock_id"]), str(c["request_id"]), c["table_config_id"], c["source_id"], n,
                    str(att["import_attempt_id"]), c["dest"], c["time_column"],
                    file_format.effective(c["file_options"]), bool(c["is_reprocess"]), c["ini"], c["fim"], path,
                    f"{root}/{src['landing_error_folder'].strip('/')}", f"{root}/{src['landing_work_folder'].strip('/')}",
                    int(src["file_wait_min"] or 0)))
            return works

    # ------------------------------------------------------------ processamento
    def _process(self, wk: ImportWork, stats: Counter):
        name = wk.file_path.rsplit("/", 1)[-1]
        arq = {"dat": wk.file_path, "base": name.rsplit(".", 1)[0], "nome": name, "pasta_limpeza": wk.work_folder}
        tag = f"nzexport:{wk.subblock_id}"
        meta, inicio, detalhe, size, cleaned = None, time.time(), [], None, None
        try:
            meta = m.metadados(wk.dest, wk.file_options, wk.time_column)
            feito = m.ja_carregado(meta, tag)
            if feito:
                versao, linhas = feito
                detalhe.append("retomada: o commit deste subbloco já estava no destino")
            else:
                size = m.esperar_arquivo(wk.file_path, wk.file_wait_min)
                if wk.is_reprocess:
                    cleaned = m.limpar_periodo(meta, wk.ini, wk.fim)
                    detalhe.append(f"reprocesso: {cleaned} linha(s) do período removidas antes da carga")
                if m.limpeza_ativa(meta):
                    modo, seg, d = m.preparar_dat(arq, meta)
                    detalhe.append(f"limpeza do DAT em {seg:.1f} s (modo {modo}{'; ' + d if d else ''})")
                m.conferir_header(arq, meta)
                versao, linhas = m.append(arq, meta, tag)
            m.apagar(wk.file_path)
            self._finish_ok(wk, versao, linhas, size, cleaned, "; ".join(detalhe) or None, time.time() - inicio)
            stats["loaded"] += 1
        except Exception as e:  # noqa: BLE001 - qualquer erro: limpa o período e libera o subbloco
            tipo, msg = (e.tipo, e.mensagem) if isinstance(e, m.ErroCarga) else m.classificar(e)
            limpeza_msg = None
            if meta and meta.get("col_tempo"):
                try:
                    cleaned = m.limpar_periodo(meta, wk.ini, wk.fim)
                    limpeza_msg = f"{cleaned} linha(s) do período removidas do destino"
                except Exception as e2:  # noqa: BLE001
                    limpeza_msg = f"falha ao limpar o período no destino: {str(e2)[:300]}"
            moved = m.mover_para_erro(wk.file_path, wk.error_folder) if m.tamanho(wk.file_path) is not None else None
            self._finish_error(wk, tipo, msg, size, cleaned, moved, limpeza_msg)
            stats["failed"] += 1
        finally:
            m.apagar_tmp(arq)
            with self.lock:
                self.in_flight.pop(wk.subblock_id, None)

    def _finish_ok(self, wk, versao, linhas, size, cleaned, detalhe, dur):
        with self.db.tx() as conn:
            conn.execute("""UPDATE export_import_attempt SET status = 'SUCCEEDED', finished_at = now(), file_size = %s,
                            rows_inserted = %s, rows_cleaned = %s, delta_version = %s, detail = %s
                            WHERE import_attempt_id = %s""",
                         (size, linhas, cleaned, versao, detalhe, wk.import_attempt_id))
            conn.execute("""UPDATE export_subblock SET status = 'DONE', rows_loaded = %s, delta_version = %s,
                            loaded_at = now(), finished_at = now(), last_error = NULL, lease_owner = NULL,
                            lease_expires_at = NULL, updated_at = now()
                            WHERE subblock_id = %s AND status = 'IMPORTING' AND import_attempts = %s""",
                         (linhas, versao, wk.subblock_id, wk.attempt_no))
            log_event(conn, wk.request_id, "SUBBLOCK_LOADED", ACTOR,
                      {"dest": wk.dest, "rows": linhas, "version": versao, "file": wk.file_path,
                       "seconds": round(dur, 1), "detail": detalhe}, wk.subblock_id)

    def _finish_error(self, wk, tipo, msg, size, cleaned, moved, limpeza_msg):
        texto = f"[{tipo}] {msg}" + (f" | {limpeza_msg}" if limpeza_msg else "")
        with self.db.tx() as conn:
            conn.execute("""UPDATE export_import_attempt SET status = 'FAILED', finished_at = now(), file_size = %s,
                            rows_cleaned = %s, moved_to = %s, detail = %s, error_class = %s, error_message = %s
                            WHERE import_attempt_id = %s""",
                         (size, cleaned, moved, limpeza_msg, tipo, msg[:3000], wk.import_attempt_id))
            conn.execute("""UPDATE export_subblock SET status = 'IMPORT_FAILED', last_error = %s, finished_at = now(),
                            lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                            WHERE subblock_id = %s AND status = 'IMPORTING' AND import_attempts = %s""",
                         (texto[:3000], wk.subblock_id, wk.attempt_no))
            log_event(conn, wk.request_id, "SUBBLOCK_IMPORT_FAILED", ACTOR,
                      {"dest": wk.dest, "error_class": tipo, "error": msg[:500], "moved_to": moved,
                       "cleanup": limpeza_msg}, wk.subblock_id)
