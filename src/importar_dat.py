# Databricks notebook source
# MAGIC %md
# MAGIC # nzexport · Importador do .dat
# MAGIC Carrega na tabela destino (Delta) o `.dat` que a `SP_CARGA_EVENTUAL` gerou para cada subbloco exportado, e apaga
# MAGIC o arquivo. Disparado pelo dispatcher e pelo app; drena a fila de subblocos `EXPORTED` e encerra sozinho.
# MAGIC Leitura e limpeza do DAT reaproveitadas do motor PLHDWA (`nzexport/motor_dat.py`).
# MAGIC Regras: append sem validação de período/conteúdo; erro = período limpo no destino, `.dat` para a pasta de erro e
# MAGIC subbloco liberado para novo pedido.

# COMMAND ----------

import logging
import os
import sys

sys.path.insert(0, os.getcwd())  # pacote nzexport ao lado do notebook (arquivos do bundle)

from databricks.sdk import WorkspaceClient  # noqa: E402

from nzexport.db import Database, LakebaseConfig  # noqa: E402
from nzexport.importer import Importer  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
for n in ("lakebase_endpoint", "run_id", "idle_exit_s", "max_runtime_min"):
    dbutils.widgets.text(n, "")  # noqa: F821
P = {n: dbutils.widgets.get(n) for n in ("lakebase_endpoint", "run_id", "idle_exit_s", "max_runtime_min")}  # noqa: F821

# COMMAND ----------

w = WorkspaceClient()
db = Database(LakebaseConfig(endpoint=P["lakebase_endpoint"], w=w), min_size=2, max_size=12)
try:
    stats = Importer(db, run_id=int(P["run_id"]), spark=spark, dbutils=dbutils, w=w,  # noqa: F821
                     idle_exit_s=int(P["idle_exit_s"] or 120),
                     max_runtime_s=int(P["max_runtime_min"] or 720) * 60).run()
finally:
    db.close()
print(stats)
