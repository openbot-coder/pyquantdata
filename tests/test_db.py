"""store/db.py：连接管理、只读约束、查询连接安全预设。"""

from __future__ import annotations

import pytest

from pyquantdata.store import CatalogNotInitialized, open_catalog
from pyquantdata.store.db import configure_query_conn


def test_open_read_only_uninitialized_raises(tmp_path):
    with pytest.raises(CatalogNotInitialized):
        open_catalog(tmp_path, read_only=True)


def test_open_write_creates_catalog(layout):
    conn = open_catalog(layout.root)
    conn.execute("SELECT 1")
    conn.close()


def test_configure_query_conn_sets_guards(catalog):
    configure_query_conn(catalog, memory_limit="1GB")
    autoinstall = catalog.execute(
        "SELECT current_setting('autoinstall_known_extensions')"
    ).fetchone()[0]
    autoload = catalog.execute(
        "SELECT current_setting('autoload_known_extensions')"
    ).fetchone()[0]
    assert autoinstall in ("false", False)
    assert autoload in ("false", False)
    mem = catalog.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    # duckdb 会把 1GB 规范化显示（如 '953.6 MiB'）；断言数值≈1GiB 而非字面量
    import re as _re

    m = _re.match(r"([\d.]+)\s*(KiB|MiB|GiB|KB|MB|GB)", str(mem))
    assert m, mem
    val, unit = float(m.group(1)), m.group(2)
    gib = val / 1024 if unit in ("MiB", "MB") else val
    assert 0.9 <= gib <= 1.1, mem


def test_read_only_blocks_write(layout):
    conn = open_catalog(layout.root)
    from pyquantdata.store import migrate

    migrate(conn)
    conn.close()
    ro = open_catalog(layout.root, read_only=True)
    with pytest.raises(Exception):
        ro.execute("CREATE TABLE nope (a INT)")
    ro.close()
