"""serve runtime：锁、迁移校验、安全校验、端口探测、退出码（真 uvicorn 段按 §14 白名单替身）。"""

from __future__ import annotations

import asyncio
import socket

import pytest

from pyquantdata.config import default_config
from pyquantdata.lockfile import acquire, read_lock
from pyquantdata.paths import DbPathLayout
from pyquantdata.store import migrate, open_catalog
from pyquantdata.serve.runtime import (
    EXIT_LOCK_HELD,
    EXIT_OK,
    EXIT_PORT_BUSY,
    EXIT_SCHEMA_TOO_NEW,
    EXIT_SECURITY,
    find_listener_pid,
    run_serve,
)


@pytest.fixture
def ready_db(layout, dbpath):
    conn = open_catalog(layout.root)
    migrate(conn)
    conn.close()
    return dbpath


def _cfg():
    return default_config()


def test_dbpath_missing_returns_1(tmp_path):
    code = asyncio.run(run_serve(tmp_path / "nope", config=_cfg()))
    assert code == 1


def test_lock_held_returns_2(ready_db):
    layout = DbPathLayout(ready_db)
    acquire(layout.lock_file, cmd="other-instance")
    code = asyncio.run(run_serve(ready_db, no_http=True, stop_event=_set_event(), config=_cfg()))
    assert code == EXIT_LOCK_HELD
    # run_serve 失败不释放别人的锁
    assert read_lock(layout.lock_file).cmd == "other-instance"


def _set_event():
    ev = asyncio.Event()
    ev.set()
    return ev


def test_no_http_graceful_exit(ready_db):
    code = asyncio.run(run_serve(ready_db, no_http=True, stop_event=_set_event(), config=_cfg()))
    assert code == EXIT_OK
    # 锁已释放
    assert read_lock(DbPathLayout(ready_db).lock_file) is None


def test_schema_too_new_returns_3(layout, dbpath):
    conn = open_catalog(layout.root)
    migrate(conn)
    conn.execute("INSERT INTO schema_migrations (version) VALUES (99)")
    conn.close()
    code = asyncio.run(run_serve(dbpath, no_http=True, stop_event=_set_event(), config=_cfg()))
    assert code == EXIT_SCHEMA_TOO_NEW


def test_security_rejects_insecure_bind(ready_db):
    cfg = _cfg()
    cfg.http.token = ""
    code = asyncio.run(
        run_serve(ready_db, host="0.0.0.0", no_http=True, stop_event=_set_event(), config=cfg)
    )
    assert code == EXIT_SECURITY


def test_insecure_flag_allows_start(ready_db, monkeypatch):
    code = asyncio.run(
        run_serve(
            ready_db, host="0.0.0.0", insecure=True, no_http=True,
            stop_event=_set_event(), config=_cfg(),
        )
    )
    assert code == EXIT_OK


def test_port_busy_returns_5(ready_db):
    """端口真被占 → 打印占用 PID 后非零退出（§7.1 bind 失败语义）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        busy_port = s.getsockname()[1]
        assert find_listener_pid(busy_port) is not None
        code = asyncio.run(run_serve(ready_db, port=busy_port, config=_cfg()))
    assert code == EXIT_PORT_BUSY


def test_http_path_full_setup_with_fake_uvicorn(ready_db, monkeypatch):
    """HTTP 路径全编排（锁→migrate→cursor→app→uvicorn.Config），真 serve 用替身（§14 白名单）。"""
    import pyquantdata.serve.runtime as rt

    served = {}

    class _FakeServer:
        def __init__(self, config):
            served["config"] = config
            assert config.host == "127.0.0.1"
            assert config.port == 8765
            assert config.timeout_graceful_shutdown == 30

        async def serve(self):
            served["served"] = True

    monkeypatch.setattr(rt.uvicorn, "Server", _FakeServer)
    code = asyncio.run(run_serve(ready_db, config=_cfg(), check_port=False))
    assert code == EXIT_OK
    assert served["served"] is True
    assert read_lock(DbPathLayout(ready_db).lock_file) is None
