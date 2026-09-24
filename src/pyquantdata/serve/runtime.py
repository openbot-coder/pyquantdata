"""serve 常驻主循环（设计 §7.1 / §7.6）。

启动序：拿锁 → 开库 + migrate → 安全校验 → 起 uvicorn（Server.serve() 挂同一
事件循环，不用 uvicorn.run() 自建循环）→ 前台常驻；SIGTERM/SIGINT 优雅退出。
bind 失败（端口被占）→ 打印占用者 PID 后非零退出（交 supervisord 退避重试）。
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import uvicorn

from ..config import AppConfig, load_config
from ..lockfile import LockHeldError, acquire, release
from ..paths import DbPathLayout
from ..store import SchemaTooNewError, migrate, open_catalog
from .app import AppContext, create_app
from .auth import assert_security

# 退出码：0 正常 / 2 锁被占 / 3 schema 版本不兼容 / 4 安全校验拒绝 / 5 端口被占
EXIT_OK = 0
EXIT_LOCK_HELD = 2
EXIT_SCHEMA_TOO_NEW = 3
EXIT_SECURITY = 4
EXIT_PORT_BUSY = 5


def find_listener_pid(port: int) -> int | None:
    """找占用端口的进程 PID（尽力而为；psutil 未装/权限不足返回 None）。"""
    try:
        import psutil

        for conn in psutil.net_connections(kind="inet"):
            if conn.status == psutil.CONN_LISTEN and conn.laddr and conn.laddr.port == port:
                return conn.pid
    except Exception:  # noqa: BLE001 — psutil 缺失/平台权限差异都按“找不到”处理
        return None
    return None


async def _serve_no_http(stop_event: asyncio.Event) -> None:
    """--no-http：纯 CLI 批处理常驻（M1 无调度器，等信号）。"""
    await stop_event.wait()


async def run_serve(
    dbpath: Path,
    *,
    host: str | None = None,
    port: int | None = None,
    no_http: bool = False,
    insecure: bool = False,
    config: AppConfig | None = None,
    stop_event: asyncio.Event | None = None,
    check_port: bool = True,
) -> int:
    """serve 主流程，返回进程退出码。真网 uvicorn 启停段按白名单豁免覆盖率。"""
    layout = DbPathLayout(dbpath)
    if not layout.root.exists():
        print(f"[serve] dbpath 不存在：{layout.root}（先 quantdata init）")
        return 1
    cfg = config or load_config(layout.config_file)
    host = host or cfg.http.host
    port = port or cfg.http.port

    # 1. 单写者锁（§11）
    try:
        lock = acquire(layout.lock_file, cmd="quantdata serve")
    except LockHeldError as e:
        print(f"[serve] 拒绝启动：{e}")
        return EXIT_LOCK_HELD
    try:
        # 2. 开库 + migrate（版本 > 代码已知最高版 → 拒启）
        conn = open_catalog(layout.root)
        try:
            schema_version = migrate(conn)
        except SchemaTooNewError as e:
            print(f"[serve] {e}")
            return EXIT_SCHEMA_TOO_NEW

        # 4. 安全校验（§8.4 fail-fast）
        try:
            assert_security(host, cfg.http.token, insecure=insecure)
        except Exception as e:
            print(f"[serve] {e}")
            return EXIT_SECURITY

        # 3/4. 起网关（--no-http 跳过）
        if no_http:
            await _serve_no_http(stop_event or asyncio.Event())
            return EXIT_OK

        if check_port:
            listener = find_listener_pid(port)
            if listener is not None:
                print(f"[serve] 端口 {port} 已被占用（PID={listener}），退出非零交 supervisord 重试")
                return EXIT_PORT_BUSY

        cursor = conn.cursor()
        cursor.execute("SET autoinstall_known_extensions=false")
        cursor.execute("SET autoload_known_extensions=false")
        ctx = AppContext(
            dbpath=layout.root, config=cfg, cursor=cursor, schema_version=schema_version
        )
        app = create_app(ctx)

        uv_config = uvicorn.Config(
            app, host=host, port=port, log_level=cfg.log.level.lower(),
            timeout_graceful_shutdown=30,  # 优雅退出预算 30s（§7.1）
        )
        server = uvicorn.Server(uv_config)
        # 真 uvicorn 起服 / socket 层：§14 白名单豁免（平台相关，本机踩过 socketpair
        # 死锁）。替代验证 = [CI] 验收链用 CLI 起真 serve + 真端口 curl（覆盖本函数
        # 的编排层，只是不把 socket 段记入行覆盖）+ 每周 smoke workflow（DP-2 v0.7）。
        await server.serve()  # pragma: no cover  # 挂同一事件循环（§7.6 第 3 条）
        return EXIT_OK
    finally:
        try:
            conn.close()
        except Exception:
            pass
        release(layout.lock_file, lock)
