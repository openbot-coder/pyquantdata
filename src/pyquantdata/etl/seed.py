"""seed 导入（init --seed）：把现成数据（如 stock_prediction 的 A股日K）搬进库。

- 支持列名：symbol, date, open, high, low, close, volume(=vol), amount(=amt),
  turnover, adj_factor（后两者可选）；
- 幂等：事务内 DELETE 覆盖区（同 symbol 的 [min,max] 日期）后 INSERT；
- parquet / csv 由扩展名路由。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as paparquet

from ..sources.classify import classify_cn

_COLUMN_ALIASES = {
    "symbol": "symbol",
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "vol": "volume",
    "amount": "amount",
    "amt": "amount",
    "turnover": "turnover",
    "adj_factor": "adj_factor",
}
_REQUIRED = ("symbol", "date", "open", "high", "low", "close")


class SeedError(ValueError):
    pass


@dataclass(slots=True)
class SeedReport:
    path: str
    rows: int
    symbols: list[str]
    date_min: date | None
    date_max: date | None


def _read_table(path: Path) -> pa.Table:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return paparquet.read_table(path)
    if suffix == ".csv":
        return pacsv.read_csv(path)
    raise SeedError(f"seed 仅支持 .parquet/.csv，收到 {path.name}")


def _as_date(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def _rows_from_table(table: pa.Table) -> list[tuple]:
    cols = {}
    for name in table.column_names:
        canonical = _COLUMN_ALIASES.get(name.strip().lower())
        if canonical:
            cols[canonical] = table.column(name).to_pylist()
    missing = [c for c in _REQUIRED if c not in cols]
    if missing:
        raise SeedError(f"seed 缺必需列：{missing}（实际列：{table.column_names}）")
    n = len(cols["symbol"])
    out: list[tuple] = []
    for i in range(n):
        symbol = str(cols["symbol"][i]).strip().lower()
        if not symbol:
            raise SeedError(f"seed 第 {i} 行 symbol 为空")
        out.append(
            (
                symbol,
                _as_date(cols["date"][i]),
                float(cols["open"][i]),
                float(cols["high"][i]),
                float(cols["low"][i]),
                float(cols["close"][i]),
                float(cols["volume"][i] or 0),
                float(cols["amount"][i] or 0),
                None if cols.get("turnover", [None] * n)[i] is None else float(cols["turnover"][i]),
                None if cols.get("adj_factor", [None] * n)[i] is None else float(cols["adj_factor"][i]),
            )
        )
    return out


def import_seed(
    conn: duckdb.DuckDBPyConnection,
    seed_path: Path | str,
    *,
    market: str = "cn",
    category: str | None = None,
) -> SeedReport:
    """导入 seed 到 bars_1d。category 缺省按 symbol 规则分类（A股）。"""
    path = Path(seed_path)
    if not path.exists():
        raise SeedError(f"seed 文件不存在：{path}")
    rows = _rows_from_table(_read_table(path))
    if not rows:
        raise SeedError(f"seed 无数据行：{path}")
    conn.begin()
    try:
        symbols = sorted({r[0] for r in rows})
        dates = [r[1] for r in rows]
        for symbol in symbols:
            sym_rows = [r for r in rows if r[0] == symbol]
            cat = category or classify_cn(symbol)
            conn.execute(
                "DELETE FROM bars_1d WHERE market=? AND category=? AND symbol=? "
                "AND date BETWEEN ? AND ?",
                [market, cat, symbol, min(dates), max(dates)],
            )
            conn.executemany(
                "INSERT INTO bars_1d (market, category, symbol, date, open, high, low, "
                "close, volume, amount, turnover, adj_factor) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [(market, cat, *r) for r in sym_rows],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return SeedReport(
        path=str(path),
        rows=len(rows),
        symbols=symbols,
        date_min=min(dates),
        date_max=max(dates),
    )
