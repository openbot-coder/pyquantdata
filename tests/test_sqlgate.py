"""SQL 闸（§8.3）：绕过样例矩阵全部入单测。"""

from __future__ import annotations

import pytest

from pyquantdata.sqlgate import SqlRejected, validate

VALID = [
    "SELECT 1",
    "SELECT date, close FROM cn_stock_1d WHERE symbol = ? ORDER BY date DESC LIMIT 5",
    "WITH latest AS (SELECT symbol, max(date) AS d FROM cn_stock_1d GROUP BY symbol) "
    "SELECT * FROM latest",
    "SELECT 'cn' AS market, symbol FROM cn_stock_1d "
    "UNION ALL SELECT 'us', symbol FROM us_stock_1d",
    "SELECT count(*), avg(close) FROM bars_1d GROUP BY market",
    "SELECT b.ts, b.close * d.adj_factor FROM cn_stock_1m b JOIN bars_1d d USING (symbol, date)",
    "SELECT unnest([1,2,3])",
    "SELECT abs(-1), date_trunc('day', now()), upper('a')",
    "SELECT symbol FROM cn_stock_1d WHERE date >= DATE '2026-01-01'",
    "  SELECT 1  ",  # 空白容忍
]

FORBIDDEN_SQL = [
    ("", "空 SQL"),
    ("   ", None),
    ("SELECT 1; SELECT 2", None),
    ("SELECT 1; ATTACH 'x.db' AS x", None),
    ("SELEC 1", None),  # 解析失败 fail-closed
    ("ATTACH 'evil.db' AS e", None),
    ("-- comment\nATTACH 'evil.db' AS e", None),  # 注释混淆
    ("COPY bars_1d TO '/tmp/steal.csv'", None),
    ("INSERT INTO bars_1d VALUES (1)", None),
    ("UPDATE bars_1d SET close = 0", None),
    ("DELETE FROM bars_1d", None),
    ("CREATE TABLE evil (a INT)", None),
    ("DROP TABLE bars_1d", None),
    ("SET memory_limit='100GB'", None),
    ("PRAGMA database_list", None),
    ("EXPLAIN SELECT 1", None),
    ("CALL pragma_version()", None),
    # 函数默认拒绝（文件/网络/环境）
    ("SELECT read_parquet('data/**/*.parquet')", None),
    ("SELECT read_csv('/etc/passwd')", None),
    ("SELECT read_csv_auto('x.csv')", None),
    ("SELECT read_json('x.json')", None),
    ("SELECT glob('*.parquet')", None),
    ("SELECT getenv('HOME')", None),
    ("SELECT current_setting('memory_limit')", None),
    ("SELECT * FROM parquet_scan('x.parquet')", None),
    ("SELECT * FROM iceberg_scan('x')", None),
    ("SELECT * FROM read_csv('/etc/passwd')", None),
    ("SELECT (SELECT read_csv('y') LIMIT 1)", None),  # 子查询里也拦
    ("WITH t AS (SELECT glob('x') AS g) SELECT g FROM t", None),  # CTE 里也拦
]


@pytest.mark.parametrize("sql", VALID)
def test_valid_sql_passes(sql):
    validate(sql)


@pytest.mark.parametrize("sql,_", FORBIDDEN_SQL, ids=range(len(FORBIDDEN_SQL)))
def test_forbidden_sql_rejected(sql, _):
    with pytest.raises(SqlRejected) as e:
        validate(sql)
    assert e.value.reason


def test_allow_functions_extends_whitelist():
    # 额外放行清单语义：显式列出的函数放行（§8.3 allow_functions）
    validate("SELECT read_csv('x.csv')", allow_functions=["read_csv"])
    # 未列出的仍然拒绝
    with pytest.raises(SqlRejected):
        validate("SELECT read_parquet('x')", allow_functions=["read_csv"])
    # 清空 allow_functions ≠ 全放行（语义反转点）
    with pytest.raises(SqlRejected):
        validate("SELECT read_csv('x.csv')", allow_functions=[])


def test_rejected_error_carries_reason():
    with pytest.raises(SqlRejected) as e:
        validate("ATTACH 'x' AS y")
    assert "只读" in e.value.reason
