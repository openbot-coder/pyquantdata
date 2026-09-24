"""update 命令的 ETL 编排（设计 §6/§7.3）。

幂等模式：以 (market, symbol, 日期区间) 粒度整区替换（事务内 DELETE+INSERT），
重跑任意次数结果一致。CLI 一次性场景允许事件循环内同步 DuckDB 调用
（§7.6 的线程池隔离约束针对 serve 常驻场景，M2 接入调度器时迁移）。
"""

from __future__ import annotations

import asyncio
from datetime import date

import duckdb

from ..sources.base import SourceAdapter, UpdateReport, full_year_calendar
from ..sources.classify import classify_cn
from .checkpoint import upsert_checkpoint

_CHUNK = 400  # 每次 fetch 的 symbol 数（TDX 一次全拉列表太大时分块）


async def update_securities(
    conn: duckdb.DuckDBPyConnection, market: str, adapter: SourceAdapter
) -> UpdateReport:
    rows = await adapter.fetch_securities(market)
    conn.begin()
    try:
        conn.execute("DELETE FROM securities WHERE market=?", [market])
        conn.executemany(
            "INSERT INTO securities (market, symbol, name, category, exchange, currency, lot_size) "
            "VALUES (?,?,?,?,?,?,?)",
            [
                (market, r.symbol, r.name, r.category, r.exchange, r.currency, r.lot_size)
                for r in rows
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    upsert_checkpoint(conn, "securities", market, "all", "", len(rows))
    return UpdateReport(dataset="securities", market=market, symbols=[r.symbol for r in rows], rows=len(rows))


async def update_calendar(
    conn: duckdb.DuckDBPyConnection, market: str, adapter: SourceAdapter, years: list[int]
) -> UpdateReport:
    """calendar 落库恒为「整年逐日」：adapter 只需给出交易日（§5.2）。

    源（tdx/fake）给出的是交易日集合，本函数负责补全全年（非交易日 is_open=False），
    保证 calendar 表的语义唯一：任意一天查得到、is_open 可信。
    """
    total = 0
    for year in years:
        rows = await adapter.fetch_calendar(market, year)
        trade_dates = {r.trade_date for r in rows if r.is_open}
        full = full_year_calendar(year, trade_dates)
        conn.begin()
        try:
            conn.execute(
                "DELETE FROM calendar WHERE market=? AND trade_date BETWEEN ? AND ?",
                [market, date(year, 1, 1), date(year, 12, 31)],
            )
            conn.executemany(
                "INSERT INTO calendar (market, trade_date, is_open, note) VALUES (?,?,?,?)",
                [(market, r.trade_date, r.is_open, r.note) for r in full],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        total += len(full)
        open_rows = sum(1 for r in full if r.is_open)
        upsert_checkpoint(conn, "calendar", market, str(year), date(year, 12, 31).isoformat(), open_rows)
    return UpdateReport(dataset="calendar", market=market, rows=total)


async def update_bars_1d(
    conn: duckdb.DuckDBPyConnection,
    market: str,
    adapter: SourceAdapter,
    *,
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> UpdateReport:
    if symbols is None:
        rows = conn.execute(
            "SELECT symbol FROM securities WHERE market=? "
            "AND category IN ('stock','index','etf','cbond') ORDER BY symbol",
            [market],
        ).fetchall()
        symbols = [r[0] for r in rows]
        if not symbols:
            raise RuntimeError(
                "securities 表为空：请先 update --dataset securities 建证券主数据"
            )
    total_rows = 0
    seen_dates: list[date] = []
    for i in range(0, len(symbols), _CHUNK):
        chunk = symbols[i : i + _CHUNK]
        bars = await adapter.fetch_bars_1d(market, chunk, start, end)
        conn.begin()
        try:
            for symbol in chunk:
                sym_bars = [b for b in bars if b.symbol == symbol]
                if not sym_bars:
                    continue
                category = classify_cn(symbol) if market == "cn" else "stock"
                d_min, d_max = min(b.date for b in sym_bars), max(b.date for b in sym_bars)
                conn.execute(
                    "DELETE FROM bars_1d WHERE market=? AND category=? AND symbol=? "
                    "AND date BETWEEN ? AND ?",
                    [market, category, symbol, d_min, d_max],
                )
                conn.executemany(
                    "INSERT INTO bars_1d (market, category, symbol, date, open, high, low, "
                    "close, volume, amount, turnover, adj_factor) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        (market, category, b.symbol, b.date, b.open, b.high, b.low, b.close,
                         b.volume, b.amount, b.turnover, b.adj_factor)
                        for b in sym_bars
                    ],
                )
                total_rows += len(sym_bars)
                seen_dates.extend([b.date for b in sym_bars])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    watermark = max(seen_dates).isoformat() if seen_dates else ""
    upsert_checkpoint(conn, "bars_1d", market, "daily", watermark, total_rows)
    return UpdateReport(
        dataset="bars_1d", market=market, symbols=symbols, rows=total_rows,
        date_min=min(seen_dates) if seen_dates else None,
        date_max=max(seen_dates) if seen_dates else None,
    )


async def run_update(
    conn: duckdb.DuckDBPyConnection,
    market: str,
    adapter: SourceAdapter,
    *,
    datasets: list[str],
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    years: list[int] | None = None,
) -> list[UpdateReport]:
    """按依赖顺序编排：securities → calendar → bars_1d。"""
    reports: list[UpdateReport] = []
    if "securities" in datasets:
        reports.append(await update_securities(conn, market, adapter))
    if "calendar" in datasets:
        reports.append(await update_calendar(conn, market, adapter, years or [date.today().year]))
    if "bars_1d" in datasets:
        reports.append(
            await update_bars_1d(conn, market, adapter, symbols=symbols, start=start, end=end)
        )
    return reports


def run_update_sync(conn: duckdb.DuckDBPyConnection, market: str, adapter: SourceAdapter, **kw) -> list[UpdateReport]:
    """CLI 同步入口。"""
    return asyncio.run(run_update(conn, market, adapter, **kw))
