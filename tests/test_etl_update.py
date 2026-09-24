"""update ETL 编排：FakeAdapter 确定性数据，幂等重放。"""

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from pyquantdata.etl import run_update
from pyquantdata.sources import FakeAdapter
from pyquantdata.sources.base import Bar1dRow, CalendarRow, SecurityRow


def _bars():
    out = []
    for sym in ("sh600000", "sh600519"):
        for d in range(1, 6):
            out.append(
                Bar1dRow(
                    symbol=sym, date=date(2026, 9, d),
                    open=10.0 + d, high=11.0 + d, low=9.0 + d, close=10.5 + d,
                    volume=1000.0 + d, amount=10500.0 + d,
                )
            )
    return out


def _securities():
    return [
        SecurityRow(symbol="sh600000", name="浦发银行", category="stock", exchange="sh", lot_size=100),
        SecurityRow(symbol="sh600519", name="贵州茅台", category="stock", exchange="sh", lot_size=100),
        SecurityRow(symbol="sh000001", name="上证指数", category="index", exchange="sh"),
    ]


def _calendar():
    return [
        CalendarRow(trade_date=date(2026, 9, d), is_open=True, note="")
        for d in (1, 2, 3, 7, 8, 9)
    ]


@pytest.fixture
def adapter():
    return FakeAdapter(
        securities=_securities(), bars=_bars(), calendar={2026: _calendar()}
    )


def test_run_update_full_chain(catalog, adapter):
    reports = asyncio.run(
        run_update(catalog, "cn", adapter, datasets=["securities", "calendar", "bars_1d"], years=[2026])
    )
    assert [r.dataset for r in reports] == ["securities", "calendar", "bars_1d"]
    assert reports[0].rows == 3
    assert catalog.execute("SELECT count(*) FROM securities").fetchone()[0] == 3
    # calendar 落库恒为整年逐日（2026 非闰年 = 365），is_open 只标 6 个交易日
    assert catalog.execute("SELECT count(*) FROM calendar WHERE market='cn'").fetchone()[0] == 365
    assert catalog.execute("SELECT count(*) FROM calendar WHERE is_open").fetchone()[0] == 6
    # 日K：只有 securities 里的 stock/index/etf/cbond 进入回填清单
    assert reports[2].rows == 10  # 2 stock × 5 天；index 在清单内但 FakeAdapter 无其 bar
    assert reports[2].date_min == date(2026, 9, 1)
    assert reports[2].date_max == date(2026, 9, 5)
    # checkpoint 已写
    cps = catalog.execute(
        "SELECT dataset, status FROM etl_checkpoint ORDER BY dataset"
    ).fetchall()
    assert [c[0] for c in cps] == ["bars_1d", "calendar", "securities"]


def test_bars_replay_idempotent(catalog, adapter):
    asyncio.run(run_update(catalog, "cn", adapter, datasets=["securities", "bars_1d"]))
    n1 = catalog.execute("SELECT count(*) FROM bars_1d").fetchone()[0]
    asyncio.run(run_update(catalog, "cn", adapter, datasets=["bars_1d"]))
    n2 = catalog.execute("SELECT count(*) FROM bars_1d").fetchone()[0]
    assert n1 == n2 == 10


def test_bars_with_explicit_symbols_and_range(catalog, adapter):
    asyncio.run(run_update(catalog, "cn", adapter, datasets=["securities"]))
    report = asyncio.run(
        run_update(
            catalog, "cn", adapter, datasets=["bars_1d"],
            symbols=["sh600000"], start=date(2026, 9, 2), end=date(2026, 9, 4),
        )
    )[0]
    assert report.rows == 3
    rows = catalog.execute(
        "SELECT count(*), min(date), max(date) FROM bars_1d WHERE symbol='sh600000'"
    ).fetchone()
    assert rows == (3, date(2026, 9, 2), date(2026, 9, 4))


def test_bars_empty_securities_raises(catalog, adapter):
    with pytest.raises(RuntimeError, match="securities"):
        asyncio.run(run_update(catalog, "cn", adapter, datasets=["bars_1d"]))


def test_partial_overlap_replay_replaces(catalog, adapter):
    """重放窗口小于已有区间：重放区替换、区外保留（§7.3 整区替换）。"""
    asyncio.run(run_update(catalog, "cn", adapter, datasets=["securities", "bars_1d"]))
    # 现在只重放 9/1~9/2，且数据变化（close 改为 99）
    slim = FakeAdapter(securities=_securities())
    slim.bars = [
        Bar1dRow(symbol="sh600000", date=date(2026, 9, d), open=1, high=2, low=0.5,
                close=99, volume=1, amount=2)
        for d in (1, 2)
    ]
    asyncio.run(
        run_update(catalog, "cn", slim, datasets=["bars_1d"], symbols=["sh600000"],
                   start=date(2026, 9, 1), end=date(2026, 9, 2))
    )
    n_99 = catalog.execute("SELECT count(*) FROM bars_1d WHERE close=99").fetchone()[0]
    n_kept = catalog.execute(
        "SELECT count(*) FROM bars_1d WHERE symbol='sh600000' AND date >= DATE '2026-09-03'"
    ).fetchone()[0]
    assert n_99 == 2
    assert n_kept == 3  # 旧区间未被动


def test_calendar_replay_idempotent(catalog, adapter):
    asyncio.run(run_update(catalog, "cn", adapter, datasets=["calendar"], years=[2026]))
    n1 = catalog.execute("SELECT count(*) FROM calendar").fetchone()[0]
    asyncio.run(run_update(catalog, "cn", adapter, datasets=["calendar"], years=[2026]))
    n2 = catalog.execute("SELECT count(*) FROM calendar").fetchone()[0]
    assert n1 == n2 == 365  # 整年逐日，重放不翻倍
