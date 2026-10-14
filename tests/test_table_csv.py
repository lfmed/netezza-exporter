import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nzexport import table_csv  # noqa: E402


DEFAULTS = {"netezza": {"default_source_database": "DWA", "default_ext_database": "STAGE", "default_ext_schema": "ADMIN"}}


def test_template_parses_cleanly():
    rows = table_csv.parse(table_csv.template_csv(), DEFAULTS)
    assert [r.errors for r in rows] == [[], [], []]
    saude = rows[1].values
    assert saude["table_config_id"] == "FATO_SAUDE" and saude["default_subblock_size"] == 3
    assert saude["migration_start"] == date(2018, 1, 1) and saude["migration_end"] == date(2025, 12, 31)
    assert rows[2].values["default_subblock_unit"] == "DAY"


def test_comma_delimiter_defaults_and_aliases():
    text = "table_config_id,source_database,source_table,ext_database,ext_schema,time_column,default_subblock_unit,retry_unknown\n" \
           "fato_x,dwa,fato_x,stage,admin,sk_data,semana,sim\n"
    r = table_csv.parse(text, ["netezza"])[0]
    assert r.values["call_profile"] == "SP_CARGA_EVENTUAL" and r.values["procedure_name"] == "SP_CARGA_EVENTUAL"
    assert r.values["source_table"] == "FATO_X" and r.values["time_column"] == "SK_DATA"
    assert not r.errors
    assert r.values["table_config_id"] == "FATO_X" and r.values["source_id"] == "netezza"
    assert r.values["default_subblock_unit"] == "WEEK" and r.values["retry_unknown"] is True
    assert r.values["max_parallel"] == 2 and r.values["active"] is True


def test_row_errors_are_reported_per_line():
    text = "table_config_id;source_id;call_profile;source_table;procedure_name;max_parallel;migration_start;migration_end\n" \
           "FATO_A;outra;TABELA_INI_FIM;DW.A;ADMIN.SP;0;2025-02-01;2025-01-01\n" \
           "FATO_A;netezza;TABELA_INI_FIM;;bad name;2;31/02/2025;\n"
    rows = table_csv.parse(text, ["netezza"])
    e1, e2 = rows[0].errors, rows[1].errors
    assert any("source_id" in e for e in e1) and any("max_parallel" in e for e in e1)
    assert any("maior que" in e for e in e1)
    assert any("repetido" in e for e in e2) and any("source_table" in e for e in e2)
    assert any("migration_start" in e for e in e2)


def test_sp_carga_eventual_requires_params_with_connection_defaults():
    head = "table_config_id;source_table;time_column\n"
    r = table_csv.parse(head + "FATO_B;FATO_B;\n", {"netezza": {}})[0]
    assert any("p_NomeColunaTempo" in e for e in r.errors) and any("p_banco não informado" in e for e in r.errors)
    defaults = {"default_source_database": "DWA", "default_ext_database": "STAGE", "default_ext_schema": "ADMIN"}
    r = table_csv.parse(head + "FATO_B;FATO_B;SK_DATA\n", {"netezza": defaults})[0]
    assert not r.errors
    r = table_csv.parse("table_config_id;procedure_name;source_table\nFATO_C;bad name;X\n", {"netezza": defaults})[0]
    assert any("procedure" in e for e in r.errors)


def test_header_validation():
    with pytest.raises(ValueError, match="obrigatórias"):
        table_csv.parse("table_config_id;procedure_name\nA;B\n", ["netezza"])
    with pytest.raises(ValueError, match="desconhecidas"):
        table_csv.parse("table_config_id;source_table;procedure_name;foo\nA;B;C;D\n", ["netezza"])


def test_classify_new_version_and_unchanged():
    rows = table_csv.parse(table_csv.template_csv(), DEFAULTS)
    v = rows[0].values
    current = {"FATO_VENDAS": {**v}, "FATO_SAUDE": {**rows[1].values, "max_parallel": 5}}
    table_csv.classify(rows, current)
    assert [r.action for r in rows] == ["SEM_ALTERACAO", "NOVA_VERSAO", "NOVA"]
    assert rows[1].changes == [{"field": "max_parallel", "old": 5, "new": 1}]


def test_export_roundtrip():
    t = {"table_config_id": "FATO_Z", "source_id": "netezza", "active": False, "source_table": "DW.Z",
         "call_profile": "TABELA_INI_FIM", "source_database": None, "ext_database": None, "ext_schema": None,
         "time_column": None, "date_format": "YYYYMMDD", "procedure_name": "ADMIN.SP",
         "dest_catalog": "cat", "dest_schema": "sch", "dest_table": "fato_z", "default_subblock_unit": "YEAR", "default_subblock_size": 1, "max_parallel": 3,
         "call_timeout_min": 60, "max_retries": 0, "retry_unknown": False, "migration_start": date(2020, 1, 1),
         "migration_end": date(2020, 12, 31), "description": None}
    r = table_csv.parse(table_csv.export_csv([t]), ["netezza"])[0]
    assert not r.errors
    assert table_csv.classify([r], {"FATO_Z": t})[0].action == "SEM_ALTERACAO"


def test_destination_must_be_complete_and_simple():
    head = "table_config_id;source_table;time_column;dest_catalog;dest_schema;dest_table\n"
    r = table_csv.parse(head + "FATO_D;FATO_D;SK;cat;;tab-x\n", DEFAULTS)[0]
    assert any("dest_schema: não informado" in e for e in r.errors) and any("dest_table: nome inválido" in e for e in r.errors)
    r = table_csv.parse(head + "FATO_D;FATO_D;SK;migracao;bronze;fato_d\n", DEFAULTS)[0]
    assert not r.errors and r.values["dest_table"] == "fato_d"
