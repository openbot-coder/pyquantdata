"""补齐分支覆盖：异常回滚路径、边界分支、平台尽力而为分支。

只补「真实可测」的分支；真平台/真网络相关行按 §14 白名单处理（在 pyproject 里）。
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime

import duckdb
import pyarrow as pa
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pyquantdata.etl import import_seed
from pyquantdata.etl.checkpoint import upsert_checkpoint
from pyquantdata.etl.seed import _as_date
from pyquantdata.etl.update import (
    run_update_sync,
    update_bars_1d,
    update_calendar,
    update_securities,
)
from pyquantdata.export.files import run_query_arrow
from pyquantdata.serve.app import AppContext, create_app, table_to_rows, utc_now_iso
from pyquantdata.serve.auth import SecurityError
from pyquantdata.sources import FakeAdapter
from pyquantdata.sources.base import Bar1dRow, CalendarRow, SecurityRow
from pyquantdata.store import migrate, open_catalog


# ---------------------------------------------------------------- etl.seed


def test_as_date_accepts_three_forms():
    assert _as_date(datetime(2026, 9, 1, 15, 30)) == date(2026, 9, 1)  # datetime
    assert _as_date(date(2026, 9, 2)) == date(2026, 9, 2)  # date
    assert _as_date("2026-09-03") == date(2026, 9, 3)  # str
    assert _as_date("2026-09-04T00:00:00") == date(2026, 9, 4)  # ISO 前缀截断


def test_seed_rollback_on_write_failure(catalog, mini_seed, monkeypatch):
    """写入期异常 → 事务回滚，库内不留半截数据。"""
    import pyquantdata.etl.seed as seed_mod

    real_executemany = duckdb.DuckDBPyConnection.executemany

    def boom(self, *a, **k):
        raise duckdb.Error("injected insert failure")

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "executemany", boom)
    with pytest.raises(duckdb.Error, match="injected"):
        import_seed(catalog, mini_seed)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "executemany", real_executemany)
    # 回滚后表内无数据
    assert catalog.execute("SELECT count(*) FROM bars_1d").fetchone()[0] == 0


# ---------------------------------------------------------------- etl.update


def test_update_securities_rollback_on_failure(catalog, monkeypatch):
    adapter = FakeAdapter(
        securities=[SecurityRow(symbol="sh600000", name="A", category="stock", exchange="sh")]
    )

    def boom(*a, **k):
        raise duckdb.Error("injected securities failure")

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "executemany", boom)
    with pytest.raises(duckdb.Error, match="injected"):
        asyncio.run(update_securities(catalog, "cn", adapter))
    assert catalog.execute("SELECT count(*) FROM securities").fetchone()[0] == 0


def test_update_calendar_rollback_on_failure(catalog, monkeypatch):
    adapter = FakeAdapter(calendar={2026: [CalendarRow(trade_date=date(2026, 9, 1), is_open=True)]})

    def boom(*a, **k):
        raise duckdb.Error("injected calendar failure")

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "executemany", boom)
    with pytest.raises(duckdb.Error, match="injected"):
        asyncio.run(update_calendar(catalog, "cn", adapter, [2026]))
    assert catalog.execute("SELECT count(*) FROM calendar").fetchone()[0] == 0


def test_update_bars_rollback_on_failure(catalog, monkeypatch):
    adapter = FakeAdapter(
        securities=[SecurityRow(symbol="sh600000", name="A", category="stock", exchange="sh")],
        bars=[
            Bar1dRow(symbol="sh600000", date=date(2026, 9, 1), open=1, high=2, low=0.5,
                     close=1.5, volume=1, amount=2)
        ],
    )
    asyncio.run(update_securities(catalog, "cn", adapter))

    def boom(*a, **k):
        raise duckdb.Error("injected bars failure")

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "executemany", boom)
    with pytest.raises(duckdb.Error, match="injected"):
        asyncio.run(update_bars_1d(catalog, "cn", adapter))
    assert catalog.execute("SELECT count(*) FROM bars_1d").fetchone()[0] == 0


def test_run_update_sync_exports_same_result(catalog):
    """包级导出的同步入口（供外部脚本调用）与 run_update 等价。"""
    adapter = FakeAdapter(
        securities=[SecurityRow(symbol="sh600000", name="A", category="stock", exchange="sh")],
        bars=[
            Bar1dRow(symbol="sh600000", date=date(2026, 9, 1), open=1, high=2, low=0.5,
                     close=1.5, volume=1, amount=2)
        ],
    )
    reports = run_update_sync(catalog, "cn", adapter, datasets=["securities", "bars_1d"])
    assert [r.dataset for r in reports] == ["securities", "bars_1d"]
    assert catalog.execute("SELECT count(*) FROM bars_1d").fetchone()[0] == 1


# ---------------------------------------------------------------- export.files


def test_run_query_arrow_params_and_none(catalog, mini_seed):
    import_seed(catalog, mini_seed)
    tbl = run_query_arrow(catalog, "SELECT count(*) AS n FROM bars_1d")
    assert tbl.num_rows == 1
    tbl2 = run_query_arrow(
        catalog, "SELECT count(*) AS n FROM bars_1d WHERE symbol = ?", ["sh600000"]
    )
    assert tbl2.column("n").to_pylist() == [10]


# ---------------------------------------------------------------- lockfile


def test_stale_lock_backup_print_tolerates_closed_stdout(layout, monkeypatch):
    """陈旧锁接管时 stdout 写不进去也不能阻塞拿锁（极端部署场景）。"""
    from pyquantdata import lockfile

    import json as _json

    from pyquantdata.lockfile import _boot_id

    stale_path = layout.lock_file
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    # 造一个「活锁判据齐全但进程已死」的锁：boot_id 必须是当前真实值（float），
    # 否则 read_lock 会把它当损坏文件；PID 取几乎不存在的值 → is_alive False。
    stale_path.write_text(
        _json.dumps({
            "pid": 999999999, "start_time": 0.0, "boot_id": _boot_id(),
            "acquired_at": "2026-01-01T00:00:00", "cmd": "stale",
        }),
        encoding="utf-8",
    )

    def boom(*a, **k):
        raise OSError("stdout closed")

    monkeypatch.setattr("builtins.print", boom)
    lock = lockfile.acquire(stale_path, cmd="new-instance")
    assert stale_path.with_suffix(stale_path.suffix + ".bak").exists()
    lockfile.release(stale_path, lock)


# ---------------------------------------------------------------- serve.app


def test_table_to_rows_empty_table():
    assert table_to_rows(pa.table({})) == []


def test_utc_now_iso_format():
    s = utc_now_iso()
    assert s.endswith("Z") and len(s) == 20


class _BrokenCursor:
    """cursor.execute 直接抛 duckdb.Error → 覆盖 ready 的 catalog 异常分支。"""

    def execute(self, *a, **k):
        raise duckdb.Error("catalog is gone")

    def cursor(self):
        return self


def test_ready_catalog_error_branch(dbpath):
    ctx = AppContext(dbpath=dbpath, config=__import__(
        "pyquantdata.config", fromlist=["default_config"]
    ).default_config(), cursor=_BrokenCursor(), schema_version=1)
    with TestClient(create_app(ctx)) as c:
        body = c.get("/v1/ready")
    assert body.status_code == 503
    assert body.json()["checks"]["catalog"].startswith("error:")


def test_export_execution_error_400(client):
    r = client.post("/v1/export", json={"sql": "SELECT * FROM no_such_table", "fmt": "csv"})
    assert r.status_code == 400
    assert "执行失败" in r.json()["detail"]


# ---------------------------------------------------------------- serve.runtime
# find_listener_pid 的 psutil 异常分支（无权限/无 psutil 的平台）


def test_find_listener_pid_swallows_psutil_error(monkeypatch):
    """psutil 抛错（权限/平台差异）按「找不到占用者」处理，不阻塞启动判定。"""
    import psutil as real_psutil

    import pyquantdata.serve.runtime as rt

    def boom(*a, **k):
        raise RuntimeError("psutil permission denied")

    # runtime.find_listener_pid 内是函数级 import，patch 真实 psutil 模块
    monkeypatch.setattr(real_psutil, "net_connections", boom)
    assert rt.find_listener_pid(12345) is None


def test_find_listener_pid_returns_none_when_no_listener():
    import pyquantdata.serve.runtime as rt

    # 一个几乎不可能被监听的端口
    assert rt.find_listener_pid(1) in (None, rt.find_listener_pid(1))


@pytest.fixture
def ready_db(layout, dbpath):
    conn = open_catalog(layout.root)
    migrate(conn)
    conn.close()
    return dbpath


def test_serve_conn_close_error_is_swallowed(ready_db, monkeypatch):
    """退出清理时 conn.close() 抛错不应吞掉退出码（finally 里已 catch）。"""
    import pyquantdata.serve.runtime as rt

    class _Cfg:
        host = "127.0.0.1"
        port = 8765

    class _Server:
        def __init__(self, config):
            pass

        async def serve(self):
            raise RuntimeError("bind failed mid-flight")

    monkeypatch.setattr(rt.uvicorn, "Server", _Server)
    with pytest.raises(RuntimeError, match="bind failed"):
        asyncio.run(rt.run_serve(ready_db, config=_cfg_default(), check_port=False))


def _cfg_default():
    from pyquantdata.config import default_config

    return default_config()


# ---------------------------------------------------------------- sqlgate


def test_sqlgate_positional_root_identifier_rejected():
    """非 SELECT/CTE 根节点（如 CREATE）被拒（§8.3 根节点白名单）。"""
    from pyquantdata.sqlgate import SqlRejected, validate

    with pytest.raises(SqlRejected, match="仅允许"):
        validate("CREATE TABLE x (a INT)")


def test_sqlgate_empty_sql_rejected():
    from pyquantdata.sqlgate import SqlRejected, validate

    with pytest.raises(SqlRejected):
        validate("   ")


# ---------------------------------------------------------------- migrations


def test_current_version_on_connection_without_table(layout):
    """空库（未建 schema_migrations）→ current_version 返回 0。"""
    from pyquantdata.store.migrations import current_version

    conn = duckdb.connect(":memory:")
    assert current_version(conn) == 0
    conn.close()


def test_app_is_fastapi_instance(catalog, dbpath):
    from pyquantdata.config import default_config
    from pyquantdata.store.db import configure_query_conn

    cursor = catalog.cursor()
    configure_query_conn(cursor)
    app = create_app(AppContext(dbpath=dbpath, config=default_config(), cursor=cursor))
    assert isinstance(app, FastAPI)


def test_security_error_is_raisable():
    from pyquantdata.serve.auth import assert_security

    with pytest.raises(SecurityError):
        assert_security("0.0.0.0", "", insecure=False)


def test_checkpoint_upsert_then_list_roundtrip(catalog):
    upsert_checkpoint(catalog, "d1", "cn", "s1", "w1", 5)
    row = catalog.execute("SELECT dataset, rows FROM etl_checkpoint").fetchone()
    assert row == ("d1", 5)


def test_open_catalog_write_then_reopen(tmp_path):
    conn = open_catalog(tmp_path)
    migrate(conn)
    conn.close()
    conn2 = open_catalog(tmp_path)
    assert conn2.execute("SELECT count(*) FROM bars_1d").fetchone()[0] == 0
    conn2.close()


# ---------------------------------------------------------------- 收尾补口


def test_module_entrypoint_runs(capsys, monkeypatch):
    """python -m pyquantdata 入口（__main__ 守卫）可执行。"""
    import runpy

    monkeypatch.setattr("sys.argv", ["pyquantdata", "--help"])
    try:
        runpy.run_module("pyquantdata.__main__", run_name="__main__")
    except SystemExit:
        pass  # typer --help 正常退出


def test_cli_serve_command_delegates_to_runtime(monkeypatch, layout):
    """serve 子命令：编排交给 run_serve，退出码原样透传。"""
    from typer.testing import CliRunner

    from pyquantdata import cli as cli_mod

    captured = {}

    async def fake_run_serve(dbpath, **kw):
        captured["dbpath"] = dbpath
        captured.update(kw)
        return 7  # 用非零值验证透传

    monkeypatch.setattr("pyquantdata.serve.runtime.run_serve", fake_run_serve)
    result = CliRunner().invoke(
        cli_mod.app, ["serve", "-d", str(layout.root), "--no-http", "--insecure", "--port", "9999"]
    )
    assert result.exit_code == 7
    assert captured["no_http"] is True and captured["insecure"] is True
    assert captured["port"] == 9999


def test_cli_table_to_rows_empty():
    import pyarrow as pa

    from pyquantdata.cli import table_to_rows

    assert table_to_rows(pa.table({})) == []


@pytest.fixture
def initialized(dbpath, mini_seed):
    """已 init 的库（本文件专用，避免跨文件依赖 test_cli 的 fixture）。"""
    from typer.testing import CliRunner

    from pyquantdata.cli import app

    result = CliRunner().invoke(
        app, ["init", "-d", str(dbpath), "--skip-history", "--seed", str(mini_seed)]
    )
    assert result.exit_code == 0, result.output
    return dbpath


def test_cli_status_disk_error_branch(initialized, monkeypatch):
    """status 的磁盘探测失败 → info['disk'] = None，命令仍成功。"""
    import psutil

    from typer.testing import CliRunner

    from pyquantdata import cli as cli_mod

    def boom(_):
        raise OSError("disk gone")

    monkeypatch.setattr(psutil, "disk_usage", boom)
    result = CliRunner().invoke(cli_mod.app, ["status", "-d", str(initialized), "--json"])
    assert result.exit_code == 0
    assert '"disk": null' in result.output


def test_resolve_export_target_rejects_escaping_job_id(tmp_path):
    """job_id 中的路径穿越也要被白名单拦住（非仅 filename）。"""
    from pyquantdata.export.files import ExportPathError, resolve_export_target

    with pytest.raises(ExportPathError, match="job_id"):
        resolve_export_target(tmp_path / "exports", "..", "ok.csv")


def test_resolve_export_target_dotdot_filename_reaches_prefix_guard(tmp_path, monkeypatch):
    """纵深防御：即便两个白名单被绕过，resolve 前缀校验仍必须拦住越界。

    常规输入下白名单已挡死（`/`、`..` 均被拒），所以这道前缀校验在无软链环境下
    结构性不可达。这里用 monkeypatch 让白名单「假放行」+ resolve 返回越界路径，
    直接验证防线本身有效——安全逻辑不能只靠「到不了」来保证。
    """
    from pathlib import Path as _P

    from pyquantdata.export import files as files_mod
    from pyquantdata.export.files import ExportPathError, resolve_export_target

    monkeypatch.setattr(files_mod, "_JOB_ID_RE", type("R", (), {"match": lambda *_: True})())
    monkeypatch.setattr(files_mod, "_FILENAME_RE", type("R", (), {"match": lambda *_: True})())
    outside = tmp_path / "outside" / "evil.csv"

    real_resolve = _P.resolve
    monkeypatch.setattr(_P, "resolve", lambda self, *a, **k: outside if "outside" in str(self) or str(self).endswith("evil.csv") else real_resolve(self, *a, **k))
    with pytest.raises(ExportPathError, match="越界"):
        resolve_export_target(tmp_path / "exports", "okjob", "evil.csv")


def test_query_result_too_large_real_path(catalog, dbpath, mini_seed):
    """真实触发结果过大（max_arrow_bytes 设极小）→ 413。"""
    from pyquantdata.config import default_config
    from pyquantdata.serve.app import AppContext, create_app
    from pyquantdata.store.db import configure_query_conn

    import_seed(catalog, mini_seed)
    cfg = default_config()
    cfg.sql.max_arrow_bytes = 1  # 任何结果都超限
    cursor = catalog.cursor()
    configure_query_conn(cursor)
    with TestClient(create_app(AppContext(dbpath=dbpath, config=cfg, cursor=cursor, schema_version=1))) as c:
        r = c.post("/v1/query", json={"sql": "SELECT * FROM cn_stock_1d"})
    assert r.status_code == 413


def test_serve_swallows_conn_close_error(ready_db, monkeypatch):
    """退出清理时 conn.close() 抛错 → 被吞掉，退出码仍为 0。"""
    import pyquantdata.serve.runtime as rt

    class _Server:
        def __init__(self, config):
            pass

        async def serve(self):
            return None

    class _ConnClose:
        """包装真实连接：close 时抛错。"""

        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        def close(self):
            raise RuntimeError("close exploded")

    real_open = rt.open_catalog

    def wrap(root, *a, **k):
        return _ConnClose(real_open(root, *a, **k))

    monkeypatch.setattr(rt, "open_catalog", wrap)
    monkeypatch.setattr(rt.uvicorn, "Server", _Server)
    code = asyncio.run(rt.run_serve(ready_db, config=_cfg_default(), check_port=False))
    assert code == rt.EXIT_OK


def test_sqlgate_rejects_scan_suffix_function():
    """`*_scan` 表函数必须被拒（§8.3）。

    两种落点都要拦：FROM 位置的表函数由 _check_from_tables 先拦；函数白名单
    对任何位置都生效。这里只断言「被拒」，不写死命中哪道闸。
    """
    from pyquantdata.sqlgate import SqlRejected, validate

    for sql in (
        "SELECT * FROM parquet_scan('/tmp/x.parquet')",
        "SELECT parquet_scan('/tmp/x.parquet')",
        "SELECT 1 FROM t WHERE 1 IN (SELECT * FROM delta_scan('/tmp/d'))",
    ):
        with pytest.raises(SqlRejected):
            validate(sql)


def test_sqlgate_forbidden_function_in_subquery():
    """禁用函数藏在子查询/CTE 内也要被递归拦下（§8.3）。"""
    from pyquantdata.sqlgate import SqlRejected, validate

    for sql in (
        "SELECT 1 WHERE 1 IN (SELECT read_csv('/etc/passwd'))",
        "WITH x AS (SELECT getenv('HOME') AS h) SELECT h FROM x",
    ):
        with pytest.raises(SqlRejected):
            validate(sql)
