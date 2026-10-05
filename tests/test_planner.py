import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nzexport.planner import plan  # noqa: E402


def _assert_contiguous(blocks, start, end):
    assert blocks[0].start == start
    assert blocks[-1].end == end
    for a, b in zip(blocks, blocks[1:]):
        assert b.start == a.end + timedelta(days=1)
    assert [b.seq for b in blocks] == list(range(1, len(blocks) + 1))


def test_monthly_full_year():
    blocks = plan(date(2024, 1, 1), date(2024, 12, 31), "MONTH", 1)
    assert len(blocks) == 12
    assert blocks[1].start == date(2024, 2, 1) and blocks[1].end == date(2024, 2, 29)
    _assert_contiguous(blocks, date(2024, 1, 1), date(2024, 12, 31))


def test_monthly_truncated_edges():
    blocks = plan(date(2024, 1, 15), date(2024, 3, 10), "MONTH", 1)
    assert [(b.start, b.end) for b in blocks] == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 3, 10)),
    ]


def test_monthly_not_aligned_keeps_day_without_drift():
    blocks = plan(date(2024, 1, 31), date(2024, 5, 30), "MONTH", 1, calendar_aligned=False)
    assert [b.start for b in blocks] == [date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 31),
                                         date(2024, 4, 30)]
    _assert_contiguous(blocks, date(2024, 1, 31), date(2024, 5, 30))


def test_daily_and_weekly():
    assert len(plan(date(2024, 1, 1), date(2024, 1, 10), "DAY", 1)) == 10
    assert len(plan(date(2024, 1, 1), date(2024, 1, 10), "DAY", 3)) == 4
    weeks = plan(date(2024, 1, 3), date(2024, 1, 21), "WEEK", 1)  # qua -> dom
    assert weeks[0].end == date(2024, 1, 7) and weeks[1].start == date(2024, 1, 8)
    _assert_contiguous(weeks, date(2024, 1, 3), date(2024, 1, 21))


def test_quarter_and_year():
    q = plan(date(2023, 2, 10), date(2023, 12, 31), "MONTH", 3)
    assert [(b.start, b.end) for b in q] == [
        (date(2023, 2, 10), date(2023, 3, 31)), (date(2023, 4, 1), date(2023, 6, 30)),
        (date(2023, 7, 1), date(2023, 9, 30)), (date(2023, 10, 1), date(2023, 12, 31))]
    y = plan(date(2020, 6, 1), date(2022, 2, 1), "YEAR", 1)
    assert len(y) == 3 and y[1].start == date(2021, 1, 1)


def test_single_day_and_errors():
    assert len(plan(date(2024, 1, 1), date(2024, 1, 1), "MONTH", 1)) == 1
    with pytest.raises(ValueError):
        plan(date(2024, 2, 1), date(2024, 1, 1), "DAY", 1)
    with pytest.raises(ValueError):
        plan(date(2000, 1, 1), date(2024, 1, 1), "DAY", 1)  # > 5000 subblocos


from nzexport.planner import merge_contiguous, plan_pieces, subtract_ranges  # noqa: E402


def test_subtract_ranges():
    d = date
    assert subtract_ranges(d(2024, 1, 1), d(2024, 12, 31), []) == [(d(2024, 1, 1), d(2024, 12, 31))]
    # meio do período já executado
    assert subtract_ranges(d(2024, 1, 1), d(2024, 12, 31), [(d(2024, 3, 1), d(2024, 5, 31))]) == [
        (d(2024, 1, 1), d(2024, 2, 29)), (d(2024, 6, 1), d(2024, 12, 31))]
    # excluídos nas bordas, sobrepostos e fora do período
    assert subtract_ranges(d(2024, 1, 1), d(2024, 1, 31),
                           [(d(2023, 12, 1), d(2024, 1, 5)), (d(2024, 1, 3), d(2024, 1, 10)),
                            (d(2024, 1, 25), d(2024, 2, 10)), (d(2025, 1, 1), d(2025, 1, 2))]) == [
        (d(2024, 1, 11), d(2024, 1, 24))]
    # tudo já executado
    assert subtract_ranges(d(2024, 1, 1), d(2024, 1, 31), [(d(2024, 1, 1), d(2024, 1, 31))]) == []


def test_plan_pieces_and_merge():
    d = date
    blocks = plan_pieces([(d(2024, 1, 1), d(2024, 2, 29)), (d(2024, 6, 1), d(2024, 7, 15))], "MONTH", 1)
    assert [(b.seq, b.start, b.end) for b in blocks] == [
        (1, d(2024, 1, 1), d(2024, 1, 31)), (2, d(2024, 2, 1), d(2024, 2, 29)),
        (3, d(2024, 6, 1), d(2024, 6, 30)), (4, d(2024, 7, 1), d(2024, 7, 15))]
    assert merge_contiguous([(d(2024, 1, 1), d(2024, 1, 31)), (d(2024, 2, 1), d(2024, 2, 29)),
                             (d(2024, 4, 1), d(2024, 4, 30))]) == [
        (d(2024, 1, 1), d(2024, 2, 29)), (d(2024, 4, 1), d(2024, 4, 30))]
