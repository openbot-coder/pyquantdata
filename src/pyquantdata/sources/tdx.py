"""pytdxdata 适配器（主源，设计 §5.2）。

pytdxdata 是纯 asyncio 客户端（动态连接池），本适配器只做模型映射与调度编排；
TDX 断连/坏节点重连由 pytdxdata 内建（评审 P2），这里不重复造。
"""

from __future__ import annotations

from datetime import date, datetime

from pytdxdata import Adjust, KlinePeriod, TdxData
from pytdxdata.models import SecurityBar, SecurityInfo

from .base import Bar1dRow, CalendarRow, SecurityRow, full_year_calendar
from .classify import classify_cn, exchange_of

_MARKET_PREFIXES = ("sh", "sz", "bj")


def info_to_row(info: SecurityInfo) -> SecurityRow:
    """SecurityInfo → 统一行（category 用规则分类，kind 仅作参考不直接入库）。"""
    symbol = info.symbol or ""
    return SecurityRow(
        symbol=symbol,
        name=info.name,
        category=classify_cn(symbol),
        exchange=exchange_of(symbol),
        currency="CNY",
        lot_size=int(info.volunit) if info.volunit else None,
    )


def bar_to_row(bar: SecurityBar) -> Bar1dRow:
    return Bar1dRow(
        symbol=bar.symbol,
        date=date(bar.year, bar.month, bar.day),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.vol,
        amount=bar.amount,
    )


def year_calendar_rows(year: int, trade_dates: set[date]) -> list[CalendarRow]:
    """从交易日集合合成全年日历（复用 base.full_year_calendar，§5.2）。"""
    return full_year_calendar(year, trade_dates)


class TdxAdapter:
    name = "tdx"
    capabilities = frozenset({"securities", "bars_1d", "calendar"})

    def __init__(self, client: TdxData | None = None) -> None:
        self._client = client
        self._owns_client = client is None

    async def open(self) -> None:
        if self._client is None:
            self._client = TdxData()
        if self._owns_client:
            await self._client.start()

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.close()
            self._owns_client = False

    def _require(self) -> TdxData:
        if self._client is None:
            raise RuntimeError("TdxAdapter 未打开：先 await adapter.open()（或注入 client）")
        return self._client

    async def fetch_securities(self, market: str) -> list[SecurityRow]:
        client = self._require()
        if market != "cn":
            return []
        rows: list[SecurityRow] = []
        seen: set[str] = set()
        for prefix in _MARKET_PREFIXES:
            for info in await client.get_universe(prefix):
                if info.symbol and info.symbol not in seen:
                    seen.add(info.symbol)
                    rows.append(info_to_row(info))
        return rows

    async def fetch_bars_1d(
        self, market: str, symbols: list[str], start: date | None, end: date | None
    ) -> list[Bar1dRow]:
        client = self._require()
        if market != "cn" or not symbols:
            return []
        bars = await client.get_bars(
            symbols,
            KlinePeriod.DAY,
            adjust=Adjust.NONE,  # 存不复权原始价（§4.3）
            start_date=datetime(start.year, start.month, start.day) if start else None,
            end_date=datetime(end.year, end.month, end.day) if end else None,
        )
        return [bar_to_row(b) for b in bars if b.symbol]

    async def fetch_calendar(self, market: str, year: int) -> list[CalendarRow]:
        """TDX 交易日回调校验：用上证指数日K日期序列当交易日全集（§5.2）。"""
        client = self._require()
        if market != "cn":
            return []
        bars = await client.get_bars(
            "sh000001",
            KlinePeriod.DAY,
            adjust=Adjust.NONE,
            start_date=datetime(year, 1, 1),
            end_date=datetime(year, 12, 31),
        )
        trade_dates = {date(b.year, b.month, b.day) for b in bars}
        return year_calendar_rows(year, trade_dates)
