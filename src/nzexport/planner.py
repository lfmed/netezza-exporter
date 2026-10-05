"""Geração de subblocos a partir do período total (datas inclusivas)."""
from dataclasses import dataclass
from datetime import date, timedelta

UNITS = ("DAY", "WEEK", "MONTH", "YEAR")
MAX_SUBBLOCKS_PER_TABLE = 5000


@dataclass(frozen=True)
class Subblock:
    seq: int
    start: date          # data_ini enviada à procedure
    end: date            # data_fim enviada à procedure (inclusiva)

    @property
    def upper_exclusive(self) -> date:
        return self.end + timedelta(days=1)


def _add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


def _advance(d: date, unit: str, size: int) -> date:
    if unit == "DAY":
        return d + timedelta(days=size)
    if unit == "WEEK":
        return d + timedelta(weeks=size)
    if unit == "MONTH":
        return _add_months(d, size) if d.day == 1 else _add_months_keep_day(d, size)
    if unit == "YEAR":
        return _add_months(d, 12 * size) if (d.day, d.month) == (1, 1) else _add_months_keep_day(d, 12 * size)
    raise ValueError(f"unidade inválida: {unit}")


def _add_months_keep_day(d: date, n: int) -> date:
    first = _add_months(date(d.year, d.month, 1), n)
    nxt = _add_months(first, 1)
    return first.replace(day=min(d.day, (nxt - timedelta(days=1)).day))


def _align(d: date, unit: str) -> date:
    """Início do período de calendário que contém d."""
    if unit == "WEEK":
        return d - timedelta(days=d.weekday())
    if unit == "MONTH":
        return d.replace(day=1)
    if unit == "YEAR":
        return date(d.year, 1, 1)
    return d


def plan(period_start: date, period_end: date, unit: str, size: int,
         calendar_aligned: bool = True) -> list[Subblock]:
    """Divide [period_start, period_end] (inclusivo) em subblocos contíguos e sem sobreposição.

    Com calendar_aligned, os cortes caem no início de semana/mês/ano; o primeiro e o
    último subbloco são truncados aos limites do período total.
    """
    if unit not in UNITS:
        raise ValueError(f"unidade inválida: {unit}")
    if size < 1:
        raise ValueError("tamanho do subbloco deve ser >= 1")
    if period_start > period_end:
        raise ValueError("data inicial maior que a data final")

    upper = period_end + timedelta(days=1)
    cursor = period_start
    base = _align(period_start, unit) if calendar_aligned else period_start
    if calendar_aligned and unit == "MONTH" and size > 1 and 12 % size == 0:
        # bimestre/trimestre/semestre: cortes contados a partir de janeiro
        base = date(base.year, (base.month - 1) // size * size + 1, 1)
    k = 1
    blocks: list[Subblock] = []
    while cursor < upper:
        # cortes sempre calculados a partir da base, para não acumular desvio de fim de mês
        nxt = _advance(base, unit, k * size)
        k += 1
        if nxt <= cursor:
            continue
        stop = min(nxt, upper)
        blocks.append(Subblock(seq=len(blocks) + 1, start=cursor, end=stop - timedelta(days=1)))
        if len(blocks) > MAX_SUBBLOCKS_PER_TABLE:
            raise ValueError(f"mais de {MAX_SUBBLOCKS_PER_TABLE} subblocos; aumente o tamanho do subbloco")
        cursor = stop
    return blocks
