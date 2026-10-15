"""Tipo de filtro: faixa genérica, planner por quantidade, chamada da SP e limpeza no destino."""
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nzexport import faixa  # noqa: E402
from nzexport.file_format import predicado_periodo  # noqa: E402
from nzexport.planner import merge_contiguous, plan_count, plan_count_pieces, subtract_ranges  # noqa: E402
from nzexport.procedure import ConfigError, build_call, validate_table_config  # noqa: E402

SOURCE = {"default_source_database": "DWA", "default_ext_database": "STAGE", "default_ext_schema": "ADMIN"}


def cfg(**kw):
    base = {"call_profile": "SP_CARGA_EVENTUAL", "procedure_name": "SP_CARGA_EVENTUAL", "source_table": "FATO_X",
            "time_column": "NR_CONTRATO", "filter_type": "NUMBER"}
    return base | kw


def test_date_key_is_contiguous_and_roundtrips():
    assert faixa.to_key("DATE", date(1970, 1, 1)) == 0
    jan31, feb1 = faixa.to_key("DATE", date(2016, 1, 31)), faixa.to_key("DATE", date(2016, 2, 1))
    assert feb1 == jan31 + 1
    assert faixa.from_key("DATE", jan31) == date(2016, 1, 31)
    assert faixa.units("DATE", date(2016, 1, 1), date(2016, 12, 31)) == 366
    assert faixa.units("NUMBER", -5, 5) == 11 and faixa.units("NONE", 0, 0) == 1


def test_parse_int_and_format():
    assert faixa.parse_int("1.000.000") == 1_000_000 and faixa.parse_int(" -42 ") == -42
    with pytest.raises(ValueError):
        faixa.parse_int("1,5")
    with pytest.raises(ValueError):
        faixa.parse_int(str(2 ** 63))
    assert faixa.fmt_range("NUMBER", 1000, 2_500_000) == "1.000 → 2.500.000"
    assert faixa.fmt_range("NONE", 0, 0) == "tabela inteira"
    assert faixa.fmt_range("DATE", date(2016, 1, 1), date(2016, 1, 31)) == "01/01/2016 → 31/01/2016"


def test_plan_count_splits_evenly_and_truncates_last():
    b = plan_count(1, 100, 3)
    assert [(x.start, x.end) for x in b] == [(1, 34), (35, 68), (69, 100)]
    assert [x.seq for x in b] == [1, 2, 3]
    # quantidade maior que a faixa: um subbloco por valor
    assert [(x.start, x.end) for x in plan_count(-2, 1, 10)] == [(-2, -2), (-1, -1), (0, 0), (1, 1)]
    with pytest.raises(ValueError):
        plan_count(10, 1, 2)
    with pytest.raises(ValueError, match="5000"):
        plan_count(1, 6000, 6000)   # passo 1 -> 6000 subblocos


def test_plan_count_pieces_keeps_cuts_when_ranges_are_excluded():
    # faixa 1..100 em 4 (passo 25); 30..60 já executado: os cortes não se deslocam
    pieces = subtract_ranges(1, 100, [(30, 60)])
    assert pieces == [(1, 29), (61, 100)]
    b = plan_count_pieces(1, 100, 4, pieces)
    assert [(x.start, x.end) for x in b] == [(1, 25), (26, 29), (61, 75), (76, 100)]
    assert [x.seq for x in b] == [1, 2, 3, 4]


def test_interval_helpers_work_for_ints_and_dates():
    assert merge_contiguous([(5, 9), (1, 4), (11, 12)]) == [(1, 9), (11, 12)]
    assert subtract_ranges(0, 0, [(0, 0)]) == []
    d = subtract_ranges(date(2016, 1, 1), date(2016, 1, 31), [(date(2016, 1, 10), date(2016, 1, 20))])
    assert d == [(date(2016, 1, 1), date(2016, 1, 9)), (date(2016, 1, 21), date(2016, 1, 31))]


def test_build_call_number_passes_min_and_max():
    s = build_call(cfg(), SOURCE, -100, 2_000_000)
    assert s.statement() == "CALL SP_CARGA_EVENTUAL('DWA', 'FATO_X', 'STAGE', 'ADMIN', '-100', '2000000', 'NR_CONTRATO')"
    assert s.extra["data_object"] == "/tmp/FATO_X_-100_2000000.dat"


def test_build_call_no_filter_sends_always_true():
    s = build_call(cfg(filter_type="NONE", time_column=None), SOURCE, 0, 0)
    assert s.statement() == "CALL SP_CARGA_EVENTUAL('DWA', 'FATO_X', 'STAGE', 'ADMIN', '1', '1', '1')"
    assert s.extra["data_object"] == "/tmp/FATO_X_1_1.dat"
    assert validate_table_config(cfg(filter_type="NONE", time_column=None), SOURCE) == []


def test_build_call_rules_by_type():
    assert validate_table_config(cfg(time_column=None), SOURCE)  # Número exige a coluna
    assert any("SP_CARGA_EVENTUAL" in e for e in validate_table_config(
        cfg(call_profile="TABELA_INI_FIM", procedure_name="ADMIN.SP", source_table="DW.X"), SOURCE))
    assert validate_table_config(cfg(filter_type="XPTO"), SOURCE)
    with pytest.raises(ConfigError):
        build_call(cfg(filter_type="NONE", call_profile="TABELA_INI_FIM", procedure_name="ADMIN.SP"), SOURCE, 0, 0)
    # Data continua igual (AAAAMMDD)
    s = build_call(cfg(filter_type="DATE", time_column="SK_DATA"), SOURCE, date(2016, 1, 1), date(2016, 1, 31))
    assert "'20160101', '20160131', 'SK_DATA'" in s.statement()


def test_cleanup_predicate_by_filter_type():
    assert predicado_periodo("NR", "bigint", "-10", "20", "NUMBER") == "`NR` BETWEEN -10 AND 20"
    assert predicado_periodo("NR", "decimal(18,0)", "1", "2", "NUMBER") == "`NR` BETWEEN 1 AND 2"
    with pytest.raises(ValueError, match="inteira"):
        predicado_periodo("NR", "string", "1", "2", "NUMBER")
    with pytest.raises(ValueError):
        predicado_periodo("NR", "bigint", "1; DROP", "2", "NUMBER")
    assert predicado_periodo(None, None, "1", "1", "NONE") == "TRUE"
    assert predicado_periodo("SK", "int", "20160101", "20160131") == "`SK` BETWEEN 20160101 AND 20160131"
