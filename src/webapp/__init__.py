"""App Flask: plano de controle do exportador Netezza."""
import logging
import os
from pathlib import Path
from datetime import date, datetime

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, url_for

from nzexport import repo
from nzexport.planner import UNITS

from . import services

log = logging.getLogger("nzexport.app")
APP_VERSION = (Path(__file__).resolve().parents[1] / "VERSION").read_text().strip()

STATUS_LABEL = {
    "SUBMITTED": "Na fila", "RUNNING": "Executando", "STOPPING": "Parando", "STOPPED": "Parado",
    "FAILED": "Falhou", "COMPLETED": "Concluído", "CANCELED": "Encerrado",
    "PENDING": "Pendente", "DONE": "Concluído", "UNKNOWN": "Desconhecido", "SUPERSEDED": "Substituído",
    "SUCCEEDED": "Sucesso", "ABANDONED": "Abandonado",
    "COMPLETA": "Completa", "EM_ANDAMENTO": "Em andamento", "INCOMPLETA": "Incompleta (lacunas)",
    "NAO_INICIADA": "Não iniciada", "COM_FALHAS": "Com falhas", "SEM_ESCOPO": "Sem escopo",
    "ESTADO_DESCONHECIDO": "Estado desconhecido", "INATIVA": "Inativa", "THIS_REQUEST": "Neste pedido",
}
UNIT_LABEL = {"DAY": "dia(s)", "WEEK": "semana(s)", "MONTH": "mês(es)", "YEAR": "ano(s)"}


def _user() -> str:
    return (request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-Preferred-Username")
            or os.environ.get("LOCAL_USER", "local"))


def _date(v: str) -> date:
    return datetime.strptime(v, "%Y-%m-%d").date()


def _int(v, default=None, lo=None, hi=None):
    try:
        n = int(v)
    except (TypeError, ValueError):
        if default is None:
            raise ValueError(f"número inválido: {v!r}")
        return default
    if (lo is not None and n < lo) or (hi is not None and n > hi):
        raise ValueError(f"valor fora do intervalo [{lo}, {hi}]: {n}")
    return n


def _parse_table_rows(form) -> list[repo.TableRequest]:
    rows = []
    tables = form.getlist("table")
    for i, tid in enumerate(tables):
        if not tid:
            continue
        reprocess = form.getlist("reprocess")[i] == "1"
        rows.append(repo.TableRequest(
            table_config_id=tid,
            period_start=_date(form.getlist("start")[i]),
            period_end=_date(form.getlist("end")[i]),
            subblock_unit=form.getlist("unit")[i] if form.getlist("unit")[i] in UNITS else "MONTH",
            subblock_size=_int(form.getlist("size")[i], lo=1, hi=1000),
            calendar_aligned=form.getlist("aligned")[i] == "1",
            is_reprocess=reprocess,
            reprocess_reason=(form.getlist("reason")[i] or "").strip() or None,
        ))
    if not rows:
        raise ValueError("selecione pelo menos uma tabela")
    for r in rows:
        if r.period_start > r.period_end:
            raise ValueError(f"{r.table_config_id}: data inicial maior que a final")
        if r.is_reprocess and not r.reprocess_reason:
            raise ValueError(f"{r.table_config_id}: reprocessamento exige justificativa")
    return rows


FORM_ROW_FIELDS = ("table", "start", "end", "size", "unit", "aligned", "reprocess", "reason")


def _form_rows(form) -> list[dict]:
    """Linhas do formulário para repopular a tela após erro."""
    if not form:
        return []
    cols = {k: form.getlist(k) for k in FORM_ROW_FIELDS}
    return [{k: (cols[k][i] if i < len(cols[k]) else "") for k in FORM_ROW_FIELDS}
            for i in range(len(cols["table"]))]


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(24)
    app.jinja_env.globals.update(STATUS_LABEL=STATUS_LABEL, UNIT_LABEL=UNIT_LABEL, UNITS=UNITS,
                                  APP_VERSION=APP_VERSION)

    @app.template_filter("dt")
    def _fmt_dt(v):
        if not v:
            return "—"
        if isinstance(v, datetime):
            return v.astimezone().strftime("%d/%m/%Y %H:%M:%S")
        return v.strftime("%d/%m/%Y") if isinstance(v, date) else str(v)

    @app.template_filter("pct")
    def _pct(done, total):
        return 0 if not total else int(round(100 * (done or 0) / total))

    @app.template_filter("dur")
    def _dur(start, end=None):
        if not start:
            return "—"
        end = end or datetime.now(start.tzinfo)
        s = int((end - start).total_seconds())
        return f"{s // 3600}h{(s % 3600) // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"

    @app.context_processor
    def _ctx():
        return {"current_user": _user()}

    @app.errorhandler(repo.StateError)
    def _state_error(e):
        flash(str(e), "error")
        return redirect(request.referrer or url_for("dashboard"))

    # ------------------------------------------------------------ dashboard
    @app.get("/")
    def dashboard():
        db = services.db()
        tables = repo.migration_coverage(db)
        scoped = [t for t in tables if t["scope_days"]]
        overall = {k: sum(t[k] for t in scoped) for k in ("scope_days", "done_days", "inflight_days",
                                                          "failed_days", "unknown_days", "missing_days")}
        overall["complete_tables"] = sum(1 for t in tables if t["coverage_status"] == "COMPLETA")
        overall["tables"] = len(tables)
        return render_template("dashboard.html", tables=tables, overall=overall,
                               active=repo.list_requests(db, limit=20),
                               dispatcher=repo.live_dispatcher(db),
                               runs=repo.recent_dispatcher_runs(db, 5))

    @app.post("/dispatcher/start")
    def dispatcher_start():
        run_id = services.ensure_dispatcher(force=True)
        flash(f"Dispatcher disparado (run {run_id})." if run_id else "Dispatcher já está em execução.", "info")
        return redirect(url_for("dashboard"))

    @app.post("/dispatcher/cancel")
    def dispatcher_cancel():
        n = services.cancel_dispatcher(_user())
        flash(f"Cancelamento solicitado para {n} run(s). Chamadas em curso no Netezza não são interrompidas; "
              "os subblocos ficarão como 'Desconhecido' quando o lease expirar.", "warn")
        return redirect(url_for("dashboard"))

    # ------------------------------------------------------------ pedidos
    @app.get("/requests")
    def requests_list():
        status = request.args.get("status") or None
        return render_template("requests.html", requests=repo.list_requests(services.db(), status=status),
                               status=status)

    @app.get("/requests/new")
    def request_new():
        tables = repo.list_tables(services.db(), only_active=True)
        return render_template("request_new.html", tables=tables, form=None, rows=[])

    @app.post("/requests/gaps")
    def request_from_gaps():
        """Pré-preenche um pedido com as lacunas de migração das tabelas selecionadas."""
        db = services.db()
        ids = request.form.getlist("table_ids")
        rows = repo.gap_rows(db, ids)
        if not rows:
            flash("As tabelas selecionadas não têm lacunas no escopo de migração.", "info")
            return redirect(url_for("dashboard"))
        form = {"name": "Lacunas de migração — " + ", ".join(ids), "max_parallel": "2", "deadline_at": "", "notes":
                "Pedido gerado a partir das lacunas do painel de completude."}
        return render_template("request_new.html", tables=repo.list_tables(db, only_active=True),
                               form=form, rows=rows)

    @app.get("/coverage/<table_config_id>")
    def coverage_detail(table_config_id):
        db = services.db()
        cov = next((t for t in repo.migration_coverage(db) if t["table_config_id"] == table_config_id), None)
        if not cov:
            abort(404)
        reqs = db.query("""SELECT DISTINCT r.request_id, r.request_name, r.status, r.submitted_at, rt.period_start,
                                  rt.period_end, rt.is_reprocess
                           FROM export_request_table rt JOIN export_request r USING (request_id)
                           WHERE rt.table_config_id = %s ORDER BY r.submitted_at DESC""", (table_config_id,))
        return render_template("coverage.html", t=cov, ranges=repo.table_ranges(db, table_config_id), reqs=reqs)

    @app.post("/requests/review")
    def request_review():
        db = services.db()
        try:
            rows = _parse_table_rows(request.form)
            max_parallel = _int(request.form.get("max_parallel"), lo=1, hi=200)
            reviewed = repo.review(db, rows)
        except ValueError as e:
            flash(str(e), "error")
            return render_template("request_new.html", tables=repo.list_tables(db, only_active=True),
                                   form=request.form, rows=_form_rows(request.form)), 400
        sources = {s["source_id"]: s for s in repo.list_sources(db)}
        cfg = {t["table_config_id"]: t for t in repo.list_tables(db)}
        return render_template("request_review.html", rows=reviewed, form=request.form,
                               max_parallel=max_parallel, cfg=cfg, sources=sources,
                               has_conflicts=any(r.conflicts for r in reviewed),
                               nothing_to_run=not any(r.blocks for r in reviewed))

    @app.post("/requests")
    def request_create():
        db = services.db()
        try:
            rows = _parse_table_rows(request.form)
            deadline = request.form.get("deadline_at") or None
            rid = repo.create_request(
                db, _user(), request.form.get("name", "").strip() or "pedido sem nome",
                _int(request.form.get("max_parallel"), lo=1, hi=200),
                datetime.fromisoformat(deadline).astimezone() if deadline else None,
                request.form.get("notes") or None, rows)
        except repo.ConflictError as e:
            flash(f"Pedido não criado: {len(e.conflicts)} conflito(s) de período. Revise.", "error")
            return request_review()
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("request_new"))
        services.ensure_dispatcher()
        flash("Pedido criado e enviado para execução.", "info")
        return redirect(url_for("request_detail", request_id=rid))

    def _load_request(request_id):
        req = repo.get_request(services.db(), request_id)
        if not req:
            abort(404)
        return req

    @app.get("/requests/<request_id>")
    def request_detail(request_id):
        db = services.db()
        req = _load_request(request_id)
        return render_template("request_detail.html", req=req, **_detail_ctx(db, request_id),
                               events=repo.request_events(db, request_id, 100))

    @app.get("/requests/<request_id>/partial")
    def request_partial(request_id):
        db = services.db()
        req = _load_request(request_id)
        return render_template("_request_progress.html", req=req, **_detail_ctx(db, request_id))

    def _detail_ctx(db, request_id):
        status = request.args.get("sb_status") or None
        tables = repo.request_tables(db, request_id)
        totals = {k: sum(t[k] or 0 for t in tables) for k in ("total", "done", "running", "pending", "failed",
                                                             "unknown", "canceled")}
        return {"tables": tables, "totals": totals, "sb_status": status,
                "subblocks": repo.request_subblocks(db, request_id, status=status, limit=1000),
                "dispatcher": repo.live_dispatcher(db)}

    @app.post("/requests/<request_id>/stop")
    def request_stop(request_id):
        repo.stop_request(services.db(), request_id, _user())
        flash("Parada solicitada: nenhum subbloco novo será iniciado; os que estão em execução vão terminar.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/resume")
    def request_resume(request_id):
        repo.resume_request(services.db(), request_id, _user(),
                            retry_failed=request.form.get("retry_failed") == "1",
                            retry_unknown=request.form.get("retry_unknown") == "1")
        services.ensure_dispatcher()
        flash("Pedido retomado.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/cancel")
    def request_cancel(request_id):
        repo.cancel_request(services.db(), request_id, _user(), request.form.get("reason") or None)
        flash("Pedido encerrado. Subblocos pendentes foram cancelados e os períodos liberados.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/parallelism")
    def request_parallelism(request_id):
        try:
            n = _int(request.form.get("max_parallel"), lo=1, hi=200)
        except ValueError as e:
            flash(str(e), "error")
        else:
            repo.set_parallelism(services.db(), request_id, _user(), n)
            flash(f"Paralelismo do pedido alterado para {n}. Vale a partir do próximo ciclo do dispatcher.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.get("/subblocks/<subblock_id>")
    def subblock_detail(subblock_id):
        db = services.db()
        sb = db.one("""SELECT s.*, lower(s.data_range) data_ini, upper(s.data_range) - 1 data_fim, r.request_name
                       FROM export_subblock s JOIN export_request r USING (request_id)
                       WHERE subblock_id = %s""", (subblock_id,))
        if not sb:
            abort(404)
        return render_template("subblock.html", sb=sb, attempts=repo.subblock_attempts(db, subblock_id))

    @app.post("/subblocks/<subblock_id>/retry")
    def subblock_retry(subblock_id):
        repo.retry_subblock(services.db(), subblock_id, _user())
        services.ensure_dispatcher()
        flash("Subbloco recolocado na fila.", "info")
        return redirect(request.referrer or url_for("dashboard"))

    @app.post("/subblocks/<subblock_id>/resolve")
    def subblock_resolve(subblock_id):
        reason = (request.form.get("reason") or "").strip()
        if not reason:
            flash("Informe a evidência/justificativa da reconciliação.", "error")
        else:
            repo.resolve_unknown(services.db(), subblock_id, _user(), request.form.get("done") == "1", reason)
            flash("Subbloco reconciliado.", "info")
        return redirect(request.referrer or url_for("dashboard"))

    # ------------------------------------------------------------ administração
    @app.get("/admin")
    def admin():
        db = services.db()
        return render_template("admin.html", sources=repo.list_sources(db), tables=repo.list_tables(db),
                               hooks=repo.list_hooks(db))

    @app.post("/admin/sources")
    def admin_source_save():
        f = request.form
        try:
            opts = {}
            for k in ("min_s", "max_s", "fail_pct", "unknown_pct", "security_level"):
                if f.get(k):
                    opts[k] = float(f[k]) if k != "security_level" else int(f[k])
            repo.save_source(services.db(), _user(), f["source_id"].strip(), f["display_name"].strip(),
                             f["driver"], f.get("secret_scope", "").strip(),
                             _int(f["max_sessions"], lo=1, hi=200), _int(f.get("connect_timeout_s"), 30, 1, 600),
                             opts)
            flash("Origem salva. O novo limite de sessões vale no próximo ciclo do dispatcher.", "info")
        except (ValueError, KeyError) as e:
            flash(f"Erro: {e}", "error")
        return redirect(url_for("admin"))

    @app.get("/admin/tables/new")
    @app.get("/admin/tables/<table_config_id>")
    def admin_table_form(table_config_id=None):
        db = services.db()
        t = repo.get_table(db, table_config_id) if table_config_id else None
        versions = db.query("SELECT * FROM table_config_version WHERE table_config_id = %s ORDER BY config_version DESC",
                            (table_config_id,)) if table_config_id else []
        return render_template("admin_table_form.html", t=t, sources=repo.list_sources(db), versions=versions)

    @app.post("/admin/tables")
    def admin_table_save():
        f = request.form
        try:
            tid = f["table_config_id"].strip().upper()
            if not tid:
                raise ValueError("identificador obrigatório")
            mig_start = _date(f["migration_start"]) if f.get("migration_start") else None
            mig_end = _date(f["migration_end"]) if f.get("migration_end") else None
            if mig_start and mig_end and mig_start > mig_end:
                raise ValueError("início do escopo maior que o fim")
            fields = dict(
                source_table=f["source_table"].strip(), procedure_name=f["procedure_name"].strip(),
                default_subblock_unit=f["default_subblock_unit"],
                default_subblock_size=_int(f["default_subblock_size"], lo=1),
                max_parallel=_int(f["max_parallel"], lo=1, hi=200),
                call_timeout_min=_int(f["call_timeout_min"], lo=1),
                max_retries=_int(f["max_retries"], lo=0, hi=10),
                retry_unknown=f.get("retry_unknown") == "1",
                migration_start=mig_start, migration_end=mig_end,
                description=f.get("description") or None)
            v = repo.save_table(services.db(), _user(), tid, f["source_id"], f.get("active") == "1", fields)
            flash(f"Tabela {tid} salva (versão {v}).", "info")
        except (ValueError, KeyError) as e:
            flash(f"Erro: {e}", "error")
        return redirect(url_for("admin"))

    @app.post("/admin/hooks")
    def admin_hook_save():
        f = request.form
        try:
            repo.save_hook(services.db(), _user(), f["hook_id"].strip(), f.get("description") or "",
                           f.get("table_config_id") or None, f["trigger_level"], _int(f["job_id"], lo=1),
                           f.get("enabled") == "1")
            flash("Hook salvo. Ele só dispara para cargas concluídas a partir de agora.", "info")
        except (ValueError, KeyError) as e:
            flash(f"Erro: {e}", "error")
        return redirect(url_for("admin"))

    # ------------------------------------------------------------ API mínima
    @app.get("/api/requests/<request_id>")
    def api_request(request_id):
        db = services.db()
        req = _load_request(request_id)
        return jsonify({"request": req, "tables": repo.request_tables(db, request_id)})

    @app.get("/healthz")
    def healthz():
        services.db().one("SELECT 1 AS ok")
        return {"ok": True, "version": APP_VERSION}

    return app
