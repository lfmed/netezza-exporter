import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nzexport.procedure import ConfigError, build_call, validate_table_config  # noqa: E402

SOURCE = {"default_source_database": "DWA", "default_ext_database": "STAGE", "default_ext_schema": "ADMIN"}


def cfg(**kw):
    base = {"call_profile": "SP_CARGA_EVENTUAL", "procedure_name": "SP_CARGA_EVENTUAL", "source_table": "FATO_VENDAS",
            "time_column": "SK_DATA", "date_format": "YYYYMMDD", "call_timeout_min": 60}
    base.update(kw)
    return base


def test_sp_carga_eventual_seven_params_in_order():
    s = build_call(cfg(source_database="DWPROD"), SOURCE, date(2016, 1, 1), date(2016, 1, 31))
    assert s.statement() == ("CALL SP_CARGA_EVENTUAL('DWPROD', 'FATO_VENDAS', 'STAGE', 'ADMIN', "
                             "'20160101', '20160131', 'SK_DATA')")
    assert s.arg_names == ["p_banco", "p_tabela", "p_banco_ext_table", "p_esquema_ext_table", "p_dtini",
                           "p_dtfim", "p_NomeColunaTempo"]
    # a procedure faz INSERT INTO esquema.tabela sem o banco: sessão no banco da external table
    assert s.session_database == "STAGE"
    assert s.extra["data_object"] == "/tmp/FATO_VENDAS_20160101_20160131.dat"
    assert s.timeout_s == 3600


def test_table_values_override_connection_defaults():
    s = build_call(cfg(ext_database="EXTDB", ext_schema="EXTSCH"), SOURCE, date(2020, 2, 1), date(2020, 2, 29))
    assert s.args[0] == "DWA" and s.args[2:4] == ["EXTDB", "EXTSCH"] and s.args[4:6] == ["20200201", "20200229"]


def test_missing_and_invalid_values_are_config_errors():
    errs = validate_table_config(cfg(time_column="", source_table="FATO;DROP"), {})
    assert any("p_NomeColunaTempo" in e for e in errs)
    assert any("p_tabela inválido" in e for e in errs)
    assert any("p_banco não informado" in e for e in errs)
    with pytest.raises(ConfigError):
        build_call(cfg(ext_schema="ADM'IN"), SOURCE, date(2020, 1, 1), date(2020, 1, 2))
    with pytest.raises(ConfigError):
        build_call(cfg(procedure_name="SP; DROP"), SOURCE, date(2020, 1, 1), date(2020, 1, 2))


def test_legacy_three_param_profile_and_iso_dates():
    s = build_call({"call_profile": "TABELA_INI_FIM", "procedure_name": "ADMIN.SP_EXPORTA_TABELA",
                    "source_table": "DW.FATO_X", "date_format": "YYYY-MM-DD"}, SOURCE, date(2024, 1, 1), date(2024, 1, 31))
    assert s.statement() == "CALL ADMIN.SP_EXPORTA_TABELA('DW.FATO_X', '2024-01-01', '2024-01-31')"
    assert s.session_database is None
