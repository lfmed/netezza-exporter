"""Entrada do Databricks App.

O bundle publica o app com `gunicorn app:app ...` e as variáveis de ambiente (config.command/env no databricks.yml,
aplicados por `bundle deploy` + `bundle run nzexport_app`). Se o app subir com o comando padrão da plataforma
(`python app.py`), a configuração do bundle não foi aplicada: em vez de sair na hora ("app exited unexpectedly"),
este arquivo sobe o mesmo gunicorn e registra no log o que falta.

Log: stdout, nível em NZEXPORT_LOG_LEVEL (padrão INFO; DEBUG para mais detalhes).
"""
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=os.environ.get("NZEXPORT_LOG_LEVEL", "INFO").upper(), stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("nzexport.app")

# variáveis que o bundle injeta no app (databricks.yml: apps.nzexport_app.config.env)
BUNDLE_ENV = ("LAKEBASE_ENDPOINT", "NETEZZA_SECRET_SCOPE", "DISPATCHER_JOB_ID", "CONN_TEST_JOB_ID", "IMPORTER_JOB_ID")
GUNICORN = ["-w", "2", "--threads", "8", "--timeout", "120"]   # o mesmo do config.command do bundle


def diagnostico():
    versao = (Path(__file__).resolve().parent / "VERSION").read_text().strip()
    log.info("nzexport %s iniciando (pid %s): %s", versao, os.getpid(), " ".join(sys.argv))
    log.info("variáveis do bundle: %s", ", ".join(f"{v}={'ok' if os.environ.get(v) else 'AUSENTE'}" for v in BUNDLE_ENV))
    faltam = [v for v in BUNDLE_ENV if not os.environ.get(v)]
    if faltam:
        log.error("variáveis ausentes: %s. A configuração do app do databricks.yml (comando e variáveis) não foi "
                  "aplicada. Publique com 'databricks bundle deploy -t <alvo>' e depois 'databricks bundle run "
                  "nzexport_app -t <alvo>' (não pelo botão Deploy da tela nem por 'databricks apps deploy').",
                  ", ".join(faltam))


diagnostico()

from webapp import create_app  # noqa: E402

app = create_app()

if __name__ == "__main__":
    # comando padrão da plataforma: sobe o mesmo servidor do bundle na porta do Databricks Apps
    from gunicorn.app.wsgiapp import run

    port = os.environ.get("DATABRICKS_APP_PORT", "8000")
    log.warning("iniciado com 'python app.py' (comando padrão): subindo gunicorn em 0.0.0.0:%s. O comando esperado "
                "é o do bundle; confira se o app foi publicado com 'databricks bundle run nzexport_app'.", port)
    sys.argv = ["gunicorn", "app:app", "-b", f"0.0.0.0:{port}", *GUNICORN]
    sys.exit(run())
