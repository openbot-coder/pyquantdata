"""联邦视图 DDL（设计 §3.3）。视图全部纳入 schema_migrations（评审 P1）。

规则：
- 显式列名，禁 ``*``（union/视图列错位不报错的隐患直接堵死）；
- bars_1m 系视图 M2 加（1m 数据 M2 回填；视图 DDL 跟 migration 版本走）。
"""

from __future__ import annotations

_BAR_COLUMNS = (
    "market, category, symbol, date, open, high, low, close, volume, amount, turnover, adj_factor"
)

# (view_name, market, category) —— M1 只做日K视图（数据在 bars_1d 物理表）
DAY_VIEWS: tuple[tuple[str, str, str], ...] = (
    ("cn_stock_1d", "cn", "stock"),
    ("cn_index_1d", "cn", "index"),
    ("cn_etf_1d", "cn", "etf"),
    ("cn_cbond_1d", "cn", "cbond"),
    ("us_stock_1d", "us", "stock"),
    ("us_index_1d", "us", "index"),
    ("hk_stock_1d", "hk", "stock"),
    ("hk_index_1d", "hk", "index"),
)


def day_view_sql(name: str, market: str, category: str) -> str:
    return (
        f"CREATE OR REPLACE VIEW {name} AS "
        f"SELECT {_BAR_COLUMNS} FROM bars_1d "
        f"WHERE market = '{market}' AND category = '{category}'"
    )


def all_day_views() -> tuple[str, ...]:
    return tuple(day_view_sql(*v) for v in DAY_VIEWS)
