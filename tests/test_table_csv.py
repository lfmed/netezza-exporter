import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nzexport import table_csv  # noqa: E402


def test_template_parses_cleanly():
    rows = table_csv.parse(table_csv.template_csv(), ["netezza"])
    assert [r.errors for r in rows] == [[], [], []]
    saude = rows[1].values
    assert saude["table_config_id"] == "FATO_SAUDE" and saude["default_subblock_size"] == 3
    assert saude["migration_start"] == date(2018, 1, 1) and saude["migration_end"] == date(2025, 12, 31)
    assert rows[2].values["default_subblock_unit"] == "DAY"


def test_comma_delimiter_defaults_and_aliases():
    text = "table_config_id,source_table,procedure_name,default_subblock_unit,retry_unknown\n" \
           "fato_x,DW.X,ADMIN.SP,semana,sim\n"
    r = table_csv.parse(text, ["netezza"])[0]
    assert not r.errors
    assert r.values["table_config_id"] == "FATO_X" and r.values["source_id"] == "netezza"
    assert r.values["default_subblock_unit"] == "WEEK" and r.values["retry_unknown"] is True
    assert r.values["max_parallel"] == 2 and r.values["active"] is True


def test_row_errors_are_reported_per_line():
    text = "table_config_id;source_id;source_table;procedure_name;max_parallel;migration_start;migration_end\n" \
           "FATO_A;outra;DW.A;ADMIN.SP;0;2025-02-01;2025-01-01\n" \
           "FATO_A;netezza;;bad name;2;31/02/2025;\n"
    rows = table_csv.parse(text, ["netezza"])
    e1, e2 = rows[0].errors, rows[1].errors
    assert any("source_id" in e for e in e1) and any("max_parallel" in e for e in e1)
    assert any("maior que" in e for e in e1)
    assert any("repetido" in e for e in e2) and any("source_table" in e for e in e2)
    assert any("procedure_name" in e for e in e2) and any("migration_start" in e for e in e2)


def test_header_validation():
    with pytest.raises(ValueError, match="obrigatórias"):
        table_csv.parse("table_config_id;source_table\nA;B\n", ["netezza"])
    with pytest.raises(ValueError, match="desconhecidas"):
        table_csv.parse("table_config_id;source_table;procedure_name;foo\nA;B;C;D\n", ["netezza"])


def test_classify_new_version_and_unchanged():
    rows = table_csv.parse(table_csv.template_csv(), ["netezza"])
    v = rows[0].values
    current = {"FATO_VENDAS": {**v}, "FATO_SAUDE": {**rows[1].values, "max_parallel": 5}}
    table_csv.classify(rows, current)
    assert [r.action for r in rows] == ["SEM_ALTERACAO", "NOVA_VERSAO", "NOVA"]
    assert rows[1].changes == [{"field": "max_parallel", "old": 5, "new": 1}]


def test_export_roundtrip():
    t = {"table_config_id": "FATO_Z", "source_id": "netezza", "active": False, "source_table": "DW.Z",
         "procedure_name": "ADMIN.SP", "default_subblock_unit": "YEAR", "default_subblock_size": 1, "max_parallel": 3,
         "call_timeout_min": 60, "max_retries": 0, "retry_unknown": False, "migration_start": date(2020, 1, 1),
         "migration_end": date(2020, 12, 31), "description": None}
    r = table_csv.parse(table_csv.export_csv([t]), ["netezza"])[0]
    assert not r.errors
    assert table_csv.classify([r], {"FATO_Z": t})[0].action == "SEM_ALTERACAO"
