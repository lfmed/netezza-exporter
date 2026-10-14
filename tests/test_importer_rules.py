import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nzexport.file_format import predicado_periodo  # noqa: E402


def test_period_predicate_by_column_type():
    assert predicado_periodo("SK_DATA", "int", "20250101", "20250131") == "`SK_DATA` BETWEEN 20250101 AND 20250131"
    assert predicado_periodo("SK_DATA", "bigint", "20250101", "20250131").endswith("20250131")
    assert predicado_periodo("DT", "date", "20250101", "20250131") == "`DT` BETWEEN DATE'2025-01-01' AND DATE'2025-01-31'"
    assert predicado_periodo("DT", "string", "20250101", "20250131") == "`DT` BETWEEN '20250101' AND '20250131'"
    ts = predicado_periodo("TS", "timestamp", "20250101", "20250131")
    assert ts == "`TS` >= TIMESTAMP'2025-01-01 00:00:00' AND `TS` < TIMESTAMP'2025-01-31 00:00:00' + INTERVAL 1 DAY"


def test_period_predicate_rejects_unsafe_values():
    for args in (("SK;DROP", "int", "20250101", "20250131"), ("SK", "int", "2025-01-01", "20250131"),
                 ("", "int", "20250101", "20250131")):
        with pytest.raises(ValueError):
            predicado_periodo(*args)
