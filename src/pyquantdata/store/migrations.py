"""schema_migrations 版本管理（设计 §11 / §3.3）。

- 每个迁移在事务内执行（失败回滚）；
- 库版本 > 代码已知最高版 → 报错退出（不带病跑）；
- 视图 DDL 纳入迁移（评审 P1）。
"""

from __future__ import annotations

import duckdb

from .views import all_day_views

MIGRATION_0001_STATEMENTS: tuple[str, ...] = (
# ---- 参考数据 -------------------------------------------------------
"""
CREATE TABLE exchanges (
    exchange_code VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    market VARCHAR NOT NULL,
    timezone VARCHAR NOT NULL,
    session_json VARCHAR NOT NULL DEFAULT '{}'
)
""",
    """
    CREATE TABLE securities (
        market VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        name VARCHAR NOT NULL DEFAULT '',
        category VARCHAR NOT NULL,
        exchange VARCHAR NOT NULL DEFAULT '',
        list_date DATE,
        delist_date DATE,
        currency VARCHAR NOT NULL DEFAULT '',
        lot_size BIGINT,
        underlying VARCHAR,
        strike DOUBLE,
        maturity DATE,
        PRIMARY KEY (market, symbol)
    )
    """,
    """
    CREATE TABLE calendar (
        market VARCHAR NOT NULL,
        trade_date DATE NOT NULL,
        is_open BOOLEAN NOT NULL,
        note VARCHAR NOT NULL DEFAULT '',
        PRIMARY KEY (market, trade_date)
    )
    """,
    """
    CREATE TABLE corp_actions (
        market VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        ex_date DATE NOT NULL,
        type VARCHAR NOT NULL,
        ratio DOUBLE,
        cash_div DOUBLE,
        PRIMARY KEY (market, symbol, ex_date, type)
    )
    """,
    # 申万 SCD2：官方发新版/追溯修订 → 开新 version（评审 P2）
    """
    CREATE TABLE sw_industry (
        version VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        l1 VARCHAR NOT NULL DEFAULT '',
        l2 VARCHAR NOT NULL DEFAULT '',
        l3 VARCHAR NOT NULL DEFAULT '',
        effective_from DATE,
        effective_to DATE
    )
    """,
    """
    CREATE TABLE stock_status (
        symbol VARCHAR NOT NULL,
        date DATE NOT NULL,
        is_st BOOLEAN NOT NULL DEFAULT FALSE,
        st_reason VARCHAR NOT NULL DEFAULT '',
        is_suspended BOOLEAN NOT NULL DEFAULT FALSE,
        limit_up DOUBLE,
        limit_down DOUBLE,
        pct_limit DOUBLE,
        source VARCHAR NOT NULL DEFAULT '',
        PRIMARY KEY (symbol, date)
    )
    """,
    """
    CREATE TABLE futures_meta (
        symbol VARCHAR PRIMARY KEY,
        contract VARCHAR NOT NULL DEFAULT '',
        multiplier DOUBLE,
        expiry DATE,
        daily_limit_rule VARCHAR NOT NULL DEFAULT ''
    )
    """,
    # ---- 行情（日K；1m 在 parquet，M2）-----------------------------------
    """
    CREATE TABLE bars_1d (
        market VARCHAR NOT NULL,
        category VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        date DATE NOT NULL,
        open DOUBLE NOT NULL,
        high DOUBLE NOT NULL,
        low DOUBLE NOT NULL,
        close DOUBLE NOT NULL,
        volume DOUBLE NOT NULL DEFAULT 0,
        amount DOUBLE NOT NULL DEFAULT 0,
        turnover DOUBLE,
        adj_factor DOUBLE,
        PRIMARY KEY (market, category, symbol, date)
    )
    """,
    # ---- 信息流 ----------------------------------------------------------
    """
    CREATE TABLE announcements (
        id VARCHAR PRIMARY KEY,
        symbol VARCHAR NOT NULL DEFAULT '',
        title VARCHAR NOT NULL DEFAULT '',
        category VARCHAR NOT NULL DEFAULT '',
        publish_ts TIMESTAMPTZ,
        url VARCHAR NOT NULL DEFAULT '',
        source VARCHAR NOT NULL DEFAULT '',
        hash VARCHAR NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE TABLE reports (
        id VARCHAR PRIMARY KEY,
        symbol VARCHAR NOT NULL DEFAULT '',
        org VARCHAR NOT NULL DEFAULT '',
        analyst VARCHAR NOT NULL DEFAULT '',
        rating VARCHAR NOT NULL DEFAULT '',
        title VARCHAR NOT NULL DEFAULT '',
        publish_ts TIMESTAMPTZ,
        url VARCHAR NOT NULL DEFAULT '',
        source VARCHAR NOT NULL DEFAULT ''
    )
    """,
    # 分钟新闻（NewsBridge 落库；断线补偿游标，评审 P1 补 DDL）
    """
    CREATE TABLE news (
        id VARCHAR PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL,
        source VARCHAR NOT NULL DEFAULT '',
        market VARCHAR NOT NULL DEFAULT '',
        symbols_json VARCHAR NOT NULL DEFAULT '[]',
        level VARCHAR NOT NULL DEFAULT 'normal',
        title VARCHAR NOT NULL DEFAULT '',
        url VARCHAR NOT NULL DEFAULT '',
        hash VARCHAR NOT NULL DEFAULT ''
    )
    """,
    # ---- 运行时元数据 ------------------------------------------------------
    """
    CREATE TABLE etl_checkpoint (
        dataset VARCHAR NOT NULL,
        market VARCHAR NOT NULL,
        scope VARCHAR NOT NULL,
        watermark VARCHAR NOT NULL,
        rows BIGINT NOT NULL DEFAULT 0,
        status VARCHAR NOT NULL DEFAULT 'ok',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (dataset, market, scope)
    )
    """,
    """
    CREATE TABLE export_jobs (
        job_id VARCHAR PRIMARY KEY,
        spec_json VARCHAR NOT NULL DEFAULT '{}',
        fmt VARCHAR NOT NULL,
        path VARCHAR NOT NULL DEFAULT '',
        rows BIGINT,
        bytes BIGINT,
        state VARCHAR NOT NULL DEFAULT 'queued',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE quality_reports (
        date DATE NOT NULL,
        market VARCHAR NOT NULL,
        check_name VARCHAR NOT NULL,
        result VARCHAR NOT NULL,
        detail_json VARCHAR NOT NULL DEFAULT '{}',
        PRIMARY KEY (date, market, check_name)
    )
    """,
    *all_day_views(),
)


# (版本, 名称, 语句元组) —— 追加新迁移时只往列表尾部加。
# DuckDB 的 execute() 一次只吃一条语句，故迁移以「语句元组」为单位逐条执行。
MIGRATIONS: tuple[tuple[int, str, tuple[str, ...]], ...] = (
    (1, "0001_initial", MIGRATION_0001_STATEMENTS),
)

KNOWN_MAX_VERSION = MIGRATIONS[-1][0]


class SchemaTooNewError(RuntimeError):
    """库 schema 版本 > 代码已知最高版：拒绝启动（§11，不带病跑）。"""

    def __init__(self, db_version: int, known_max: int) -> None:
        self.db_version = db_version
        self.known_max = known_max
        super().__init__(
            f"catalog.duckdb schema 版本 {db_version} 高于代码已知最高版 {known_max}："
            f"请升级 pyquantdata 后再启动（§11 不带病跑）"
        )


def _ensure_migrations_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    )


def current_version(conn: duckdb.DuckDBPyConnection) -> int:
    """当前库版本；空库（无 schema_migrations 表）返回 0。"""
    has_table = conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='main' AND table_name='schema_migrations'"
    ).fetchone()
    if not has_table or has_table[0] == 0:
        return 0
    row = conn.execute("SELECT max(version) FROM schema_migrations").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def migrate(conn: duckdb.DuckDBPyConnection) -> int:
    """把库升到代码已知最高版，返回最终版本。幂等：已应用过则跳过。"""
    _ensure_migrations_table(conn)
    db_version = current_version(conn)
    if db_version > KNOWN_MAX_VERSION:
        raise SchemaTooNewError(db_version, KNOWN_MAX_VERSION)
    for version, _name, statements in MIGRATIONS:
        if version <= db_version:
            continue
        conn.begin()
        try:
            for stmt in statements:
                conn.execute(stmt)
            conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", [version])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return current_version(conn)


def applied_versions(conn: duckdb.DuckDBPyConnection) -> list[int]:
    _ensure_migrations_table(conn)
    rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    return [int(r[0]) for r in rows]
