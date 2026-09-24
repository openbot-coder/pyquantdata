"""TdxAdapter 映射逻辑（注入 fake client，不打真网）。"""

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from pyquantdata.sources import TdxAdapter
from pyquantdata.sources.classify import classify_cn
from pyquantdata.sources.tdx import bar_to_row, info_to_row, year_calendar_rows


class _FakeInfo:
    def __init__(self, symbol, code="600000", name="浦发银行", volunit=100):
        self.symbol = symbol
        self.code = code
        self.name = name
        self.market = 1
        self.volunit = volunit
        self.decimal_point = 2
        self.pre_close = 10.0
        self.kind = "A股"


class _FakeBar:
    def __init__(self, symbol, y, m, d, close=10.0, vol=100.0, amount=1000.0):
        self.symbol = symbol
        self.market = 1
        self.code = "600000"
        self.open = 9.9
        self.high = 10.2
        self.low = 9.8
        self.close = close
        self.vol = vol
        self.amount = amount
        self.year = y
        self.month = m
        self.day = d
        self.hour = 0
        self.minute = 0


class _FakeTdx:
    def __init__(self):
        self.universe_calls = []
        self.bar_calls = []

    async def get_universe(self, market):
        self.universe_calls.append(market)
        if market == "sh":
            return [_FakeInfo("sh600000"), _FakeInfo("sh000001")]
        if market == "sz":
            return [_FakeInfo("sz000001")]
        return [_FakeInfo("bj430047")]

    async def get_bars(self, symbols, period, *, adjust=None, start_date=None,
                       end_date=None, **kw):
        self.bar_calls.append((tuple(symbols) if isinstance(symbols, (list, tuple)) else (symbols,), start_date, end_date))
        out = []
        for sym in symbols if isinstance(symbols, (list, tuple)) else [symbols]:
            for d in (1, 2):
                out.append(_FakeBar(sym, 2026, 9, d))
        return out


def test_info_to_row():
    row = info_to_row(_FakeInfo("sh600000"))
    assert row.symbol == "sh600000"
    assert row.name == "浦发银行"
    assert row.category == "stock"
    assert row.exchange == "sh"
    assert row.currency == "CNY"
    assert row.lot_size == 100


def test_bar_to_row():
    row = bar_to_row(_FakeBar("sh600000", 2026, 9, 1, close=10.5, vol=500, amount=5200))
    assert (row.date, row.close, row.volume, row.amount) == (date(2026, 9, 1), 10.5, 500, 5200)


def test_year_calendar_rows_full_year():
    rows = year_calendar_rows(2026, {date(2026, 1, 1)})
    assert len(rows) == 365
    assert rows[0].is_open is True
    assert rows[1].is_open is False


def test_year_calendar_rows_leap_year():
    assert len(year_calendar_rows(2024, set())) == 366


def test_adapter_requires_open():
    adapter = TdxAdapter(client=None)
    with pytest.raises(RuntimeError, match="open"):
        asyncio.run(adapter.fetch_securities("cn"))


def test_adapter_securities_dedup_and_all_exchanges():
    fake = _FakeTdx()
    adapter = TdxAdapter(client=fake)  # 注入：不 open 也不拥有
    rows = asyncio.run(adapter.fetch_securities("cn"))
    symbols = [r.symbol for r in rows]
    assert symbols == ["sh600000", "sh000001", "sz000001", "bj430047"]
    assert fake.universe_calls == ["sh", "sz", "bj"]


def test_adapter_non_cn_market_empty():
    adapter = TdxAdapter(client=_FakeTdx())
    assert asyncio.run(adapter.fetch_securities("us")) == []
    assert asyncio.run(adapter.fetch_calendar("us", 2026)) == []


def test_adapter_bars_mapping_and_range():
    fake = _FakeTdx()
    adapter = TdxAdapter(client=fake)
    rows = asyncio.run(
        adapter.fetch_bars_1d("cn", ["sh600000"], date(2026, 9, 1), date(2026, 9, 2))
    )
    assert len(rows) == 2
    assert all(r.symbol == "sh600000" for r in rows)
    # start/end 透传为 datetime
    called_syms, start, end = fake.bar_calls[0]
    assert called_syms == ("sh600000",)
    assert (start.year, start.month, start.day) == (2026, 9, 1)
    assert (end.year, end.month, end.day) == (2026, 9, 2)


def test_adapter_bars_empty_symbols():
    adapter = TdxAdapter(client=_FakeTdx())
    assert asyncio.run(adapter.fetch_bars_1d("cn", [], None, None)) == []


def test_adapter_calendar_from_index_bars():
    class _IdxTdx(_FakeTdx):
        async def get_bars(self, symbols, period, **kw):
            return [_FakeBar("sh000001", 2026, 9, d) for d in (1, 2, 7, 8)]

    adapter = TdxAdapter(client=_IdxTdx())
    rows = asyncio.run(adapter.fetch_calendar("cn", 2026))
    opens = [r for r in rows if r.is_open]
    assert [(r.trade_date.day) for r in opens] == [1, 2, 7, 8]
    assert len(rows) == 365  # 全年日历


def test_adapter_close_noop_without_ownership():
    adapter = TdxAdapter(client=_FakeTdx())
    asyncio.run(adapter.close())  # 注入 client：不 close 外部对象


def test_adapter_open_creates_client(monkeypatch):
    """默认构造：open() 自建 TdxData 并 start（本测注入替身）。"""
    import pyquantdata.sources.tdx as tdx_mod

    started = {"n": 0}

    class _StubTdxData:
        async def start(self):
            started["n"] += 1

        async def close(self):
            started["n"] -= 1

    monkeypatch.setattr(tdx_mod, "TdxData", _StubTdxData)
    adapter = TdxAdapter()
    asyncio.run(adapter.open())
    assert started["n"] == 1
    asyncio.run(adapter.close())
    assert started["n"] == 0
