"""共享 fixtures：迷你库、迷你 seed、app client。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as paparquet
import pytest
from fastapi.testclient import TestClient

from pyquantdata.config import default_config
from pyquantdata.paths import DbPathLayout
from pyquantdata.store import migrate, open_catalog


@pytest.fixture
def dbpath(tmp_path: Path) -> Path:
    return tmp_path / "db"


@pytest.fixture
def layout(dbpath: Path) -> DbPathLayout:
    lay = DbPathLayout(dbpath)
    lay.ensure_dirs()
    return lay


@pytest.fixture
def catalog(layout: DbPathLayout, dbpath: Path):
    """已 migrate 的写连接（含派生 cursor 配置）。"""
    conn = open_catalog(layout.root)
    migrate(conn)
    yield conn
    conn.close()


def make_seed_table() -> pa.Table:
    dates = [date(2026, 9, d) for d in range(1, 11)]
    n = len(dates)
    return pa.table(
        {
            "symbol": ["sh600000"] * n,
            "date": dates,
            "open": [10.0 + i * 0.1 for i in range(n)],
            "high": [10.5 + i * 0.1 for i in range(n)],
            "low": [9.5 + i * 0.1 for i in range(n)],
            "close": [10.2 + i * 0.1 for i in range(n)],
            "volume": [1_000_000.0] * n,
            "amount": [10_200_000.0 + i * 1e4 for i in range(n)],
        }
    )


@pytest.fixture
def mini_seed(tmp_path: Path) -> Path:
    p = tmp_path / "mini_seed_1d.parquet"
    paparquet.write_table(make_seed_table(), p)
    return p


def make_client(dbpath: Path, cursor, *, config=None, schema_version: int = 1) -> TestClient:
    from pyquantdata.serve.app import AppContext, create_app

    ctx = AppContext(
        dbpath=dbpath, config=config or default_config(), cursor=cursor,
        schema_version=schema_version,
    )
    app = create_app(ctx)
    return TestClient(app)


@pytest.fixture
def client(catalog, dbpath: Path) -> TestClient:
    from pyquantdata.store.db import configure_query_conn

    cursor = catalog.cursor()
    configure_query_conn(cursor)
    with make_client(dbpath, cursor) as c:
        yield c
