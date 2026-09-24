"""migrations：版本管理、幂等、TooNew 拒启、视图列集。"""

from __future__ import annotations

import pytest

from pyquantdata.store import (
    KNOWN_MAX_VERSION,
    MIGRATIONS,
    SchemaTooNewError,
    applied_versions,
    current_version,
    migrate,
)

BAR_COLUMNS = {
    "market", "category", "symbol", "date", "open", "high", "low",
    "close", "volume", "amount", "turnover", "adj_factor",
}


def test_fresh_migrate(catalog):
    assert current_version(catalog) == KNOWN_MAX_VERSION
    assert applied_versions(catalog) == [1]


def test_migrate_idempotent(catalog):
    assert migrate(catalog) == KNOWN_MAX_VERSION
    assert migrate(catalog) == KNOWN_MAX_VERSION
    assert applied_versions(catalog) == [1]


def test_core_tables_exist(catalog):
    rows = catalog.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main' "
        "AND table_type='BASE TABLE' ORDER BY table_name"
    ).fetchall()
    names = {r[0] for r in rows}
    assert {
        "exchanges", "securities", "calendar", "corp_actions", "sw_industry",
        "stock_status", "futures_meta", "bars_1d", "announcements", "reports",
        "news", "etl_checkpoint", "export_jobs", "quality_reports",
        "schema_migrations",
    } <= names


def test_day_views_exist_with_exact_columns(catalog):
    from pyquantdata.store.views import DAY_VIEWS

    for view, market, category in DAY_VIEWS:
        cols = [
            r[0]
            for r in catalog.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
                [view],
            ).fetchall()
        ]
        assert set(cols) == BAR_COLUMNS, view
        # 列集恒等：三市场同品类视图列集一致（§3.3）
    stock_views = [v for v, _, c in DAY_VIEWS if c == "stock"]
    assert len(stock_views) >= 3  # cn/us/hk


def test_day_view_filters_market(catalog):
    catalog.execute(
        "INSERT INTO bars_1d VALUES ('cn','stock','sh600000',DATE '2026-09-01',"
        "10,11,9,10.5,1000,10500,1.2,1.0)"
    )
    assert catalog.execute("SELECT count(*) FROM cn_stock_1d").fetchone()[0] == 1
    assert catalog.execute("SELECT count(*) FROM us_stock_1d").fetchone()[0] == 0


def test_schema_too_new_rejected(catalog):
    catalog.execute("INSERT INTO schema_migrations (version) VALUES (99)")
    with pytest.raises(SchemaTooNewError):
        migrate(catalog)


def test_migration_failure_rolls_back(catalog, monkeypatch):
    """迁移包事务：中途失败回滚，版本停在最后一个成功迁移（§11）。"""
    import duckdb

    from pyquantdata.store import migrations as m

    broken = list(m.MIGRATIONS) + [
        (2, "0002_bad", "CREATE TABLE oops (x INT); CREATE TABLE oops (x INT)")
    ]
    monkeypatch.setattr(m, "MIGRATIONS", tuple(broken))
    monkeypatch.setattr(m, "KNOWN_MAX_VERSION", 2)
    conn2 = duckdb.connect(":memory:")
    with pytest.raises(Exception):
        m.migrate(conn2)
    # migration 1 已提交、migration 2 回滚：无 oops 表
    assert m.current_version(conn2) == 1
    has_oops = conn2.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name='oops'"
    ).fetchone()[0]
    assert has_oops == 0
    conn2.close()


def test_known_max_version_matches_list():
    assert KNOWN_MAX_VERSION == MIGRATIONS[-1][0]
