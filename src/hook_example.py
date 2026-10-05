"""Job de exemplo para o hook pós-exportação: só registra os parâmetros recebidos.

Substituir pelo job de importação para Delta quando ele existir.
"""
import argparse

p = argparse.ArgumentParser()
for name in ("request_id", "trigger_level", "request_table_id", "table_config_id", "data_ini", "data_fim"):
    p.add_argument(f"--{name}", default="")
args = p.parse_args()
print("Hook pós-exportação recebido:")
for k, v in vars(args).items():
    print(f"  {k} = {v}")
