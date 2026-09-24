"""DuckDB 连接管理：写连接（init/ETL/serve）与只读连接（查询/导出）分离（§8.3）。"""

from __future__ import annotations

from pathlib import Path

import duckdb


class CatalogNotInitialized(RuntimeError):
    pass


def open_catalog(dbpath: Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """打开 catalog.duckdb。只读模式要求库已存在（未初始化给清晰报错）。"""
    catalog = Path(dbpath) / "catalog.duckdb"
    if read_only and not catalog.exists():
        raise CatalogNotInitialized(
            f"{catalog} 不存在：库未初始化，请先执行 quantdata init -d {dbpath}"
        )
    return duckdb.connect(str(catalog), read_only=read_only)


def configure_query_conn(
    conn: duckdb.DuckDBPyConnection, *, memory_limit: str = "2GB"
) -> None:
    """查询连接安全预设（§8.3）：断扩展自动加载（堵 httpfs 出站）+ 内存上限。"""
    conn.execute("SET autoinstall_known_extensions=false")
    conn.execute("SET autoload_known_extensions=false")
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET max_expression_depth=1000")
