"""SourceAdapter 协议与行模型（设计 §5）。

适配层产出统一行模型，ETL 只认行模型；真实源（tdx）与测试源（fake）同构。
fixtures 录制回放 + 故障注入的宿主也在这一层（M2 完善限速/退避）。
"""

from __future__ import annotations

import calendar as _calendar
from dataclasses import dataclass, field
from datetime import date
from typing import Protocol, runtime_checkable


@dataclass(slots=True)
class SecurityRow:
    symbol: str
    name: str
    category: str
    exchange: str
    currency: str = "CNY"
    lot_size: int | None = None


@dataclass(slots=True)
class CalendarRow:
    trade_date: date
    is_open: bool
    note: str = ""


@dataclass(slots=True)
class Bar1dRow:
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    amount: float = 0.0
    turnover: float | None = None
    adj_factor: float | None = None


@dataclass(slots=True)
class UpdateReport:
    dataset: str
    market: str
    symbols: list[str] = field(default_factory=list)
    rows: int = 0
    date_min: date | None = None
    date_max: date | None = None


def full_year_calendar(year: int, trade_dates: set[date]) -> list[CalendarRow]:
    """把「交易日集合」补全为「整年逐日」日历（§5.2）。

    源只负责说清哪些天开市；补全全年是 ETL 的职责，保证 calendar 表语义唯一：
    任意一天都查得到，is_open 可信。
    """
    rows: list[CalendarRow] = []
    for month in range(1, 13):
        for day in range(1, _calendar.monthrange(year, month)[1] + 1):
            d = date(year, month, day)
            rows.append(CalendarRow(trade_date=d, is_open=d in trade_dates))
    return rows


@runtime_checkable
class SourceAdapter(Protocol):
    name: str
    capabilities: frozenset[str]

    async def fetch_securities(self, market: str) -> list[SecurityRow]: ...

    async def fetch_bars_1d(
        self, market: str, symbols: list[str], start: date | None, end: date | None
    ) -> list[Bar1dRow]: ...

    async def fetch_calendar(self, market: str, year: int) -> list[CalendarRow]: ...


class FakeAdapter:
    """确定性测试源：可注入任意行；不打真网（[CI] 层数据走 seed，本源供单测与 CLI 替身）。"""

    name = "fake"
    capabilities = frozenset({"securities", "bars_1d", "calendar"})

    def __init__(
        self,
        securities: list[SecurityRow] | None = None,
        bars: list[Bar1dRow] | None = None,
        calendar: dict[int, list[CalendarRow]] | None = None,
    ) -> None:
        self.securities = securities or []
        self.bars = bars or []
        self.calendar = calendar or {}
        self.calls: list[tuple] = []

    async def open(self) -> None:
        self.calls.append(("open",))

    async def close(self) -> None:
        self.calls.append(("close",))

    async def fetch_securities(self, market: str) -> list[SecurityRow]:
        self.calls.append(("securities", market))
        return list(self.securities)

    async def fetch_bars_1d(
        self, market: str, symbols: list[str], start: date | None, end: date | None
    ) -> list[Bar1dRow]:
        self.calls.append(("bars_1d", market, tuple(symbols), start, end))
        sym_set = set(symbols)
        return [
            b
            for b in self.bars
            if b.symbol in sym_set and (start is None or b.date >= start) and (end is None or b.date <= end)
        ]

    async def fetch_calendar(self, market: str, year: int) -> list[CalendarRow]:
        self.calls.append(("calendar", market, year))
        return list(self.calendar.get(year, []))
