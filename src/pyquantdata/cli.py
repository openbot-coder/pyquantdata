"""quantdata CLI（设计 §6）：init / serve / update / query / export / status。

约定：``-d`` 可用环境变量 QUANTDATA_HOME 兜底；输出对齐「简洁分阶段」偏好；
CLI 一次性命令可离线跑（serve --no-http 纯批处理）。
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import AppConfig, default_config, load_config, write_config
from .etl import SeedError, import_seed
from .export.files import (
    EXPORT_FORMATS,
    write_arrow_file,
    write_export,
)
from .paths import DbPathLayout, resolve_dbpath
from .sqlgate import SqlRejected, validate
from .store import CatalogNotInitialized, migrate, open_catalog

app = typer.Typer(add_completion=False, help="quantdata：单目录自包含的量化数据中台")
console = Console()
err = Console(stderr=True)

# 容量估算表（设计 §3.4，GB）：数据终态 + buffer ≥200GB ≈ 600GB 预算口径
_CAPACITY_GB = {"cn": (150, 200), "us": (80, 90), "hk": (40, 45)}
_BUFFER_GB = 200


def estimate_capacity(markets: list[str]) -> dict:
    """init --estimate 只算不写（§3.4）。"""
    per = {}
    lo_total = hi_total = 0
    for m in markets:
        lo, hi = _CAPACITY_GB.get(m, (0, 0))
        per[m] = {"gb_low": lo, "gb_high": hi}
        lo_total += lo
        hi_total += hi
    return {
        "per_market_gb": per,
        "data_gb": [lo_total, hi_total],
        "buffer_gb": _BUFFER_GB,
        "budget_gb": [lo_total + _BUFFER_GB, hi_total + _BUFFER_GB],
    }


def _open_ro(dbpath: Path):
    """只读打开；未初始化时打印可操作提示并 exit(1)（所有只读命令共用）。"""
    try:
        return open_catalog(dbpath, read_only=True)
    except CatalogNotInitialized as e:
        err.print(f"[red]未初始化：[/red]{e}")
        raise typer.Exit(1) from e


@app.command()
def init(
    d: Optional[str] = typer.Option(None, "--dbpath", "-d", envvar="QUANTDATA_HOME", help="库目录"),
    markets: str = typer.Option("cn,us,hk", "--markets", help="启用市场，逗号分隔"),
    seed: Optional[Path] = typer.Option(None, "--seed", help="现成数据文件（parquet/csv）导入日K"),
    skip_history: bool = typer.Option(False, "--skip-history", help="跳过历史回填（[CI] 层）"),
    estimate: bool = typer.Option(False, "--estimate", help="只估算磁盘预算，不写库"),
) -> None:
    """建目录 + config.json + schema（migrations）+ seed 导入 + 自检。幂等：重跑=续跑。"""
    dbpath = resolve_dbpath(d)
    layout = DbPathLayout(dbpath)
    market_list = [m.strip() for m in markets.split(",") if m.strip()]
    if estimate:
        console.print_json(json.dumps(estimate_capacity(market_list)))
        raise typer.Exit(0)

    console.print(f"[bold]quantdata init[/bold] → {dbpath}")
    console.print("  [1/4] 目录结构")
    layout.ensure_dirs()
    config_file = layout.config_file
    if config_file.exists():
        cfg = load_config(config_file)
        console.print("  [2/4] config.json 已存在，保留（幂等）")
    else:
        cfg = default_config(market_list)
        write_config(cfg, config_file)
        console.print(f"  [2/4] config.json 生成（markets={market_list}）")

    conn = open_catalog(layout.root)
    try:
        console.print("  [3/4] schema migrations")
        version = migrate(conn)
        console.print(f"        schema 版本 = {version}")

        console.print("  [4/4] 自检")
        seed_report = None
        if seed is not None:
            try:
                seed_report = import_seed(conn, seed)
            except SeedError as e:
                err.print(f"[red]seed 导入失败：[/red]{e}")
                raise typer.Exit(1) from e
        _print_selfcheck(conn, layout, seed_report)
    finally:
        conn.close()
    if skip_history:
        console.print("[dim]--skip-history：历史回填跳过（[CI] 层验收口径）[/dim]")
    console.print("[green]init 完成[/green]")


def _print_selfcheck(conn, layout: DbPathLayout, seed_report) -> None:
    tables = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='main' "
        "AND table_type='BASE TABLE'"
    ).fetchone()[0]
    views = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='main' "
        "AND table_type='VIEW'"
    ).fetchone()[0]
    bars_rows = conn.execute("SELECT count(*) FROM bars_1d").fetchone()[0]
    console.print(f"        tables={tables} views={views} bars_1d rows={bars_rows}")
    if seed_report is not None:
        console.print(
            f"        seed: {seed_report.rows} 行 / {len(seed_report.symbols)} symbols "
            f"[{seed_report.date_min} ~ {seed_report.date_max}]"
        )


@app.command()
def serve(
    d: Optional[str] = typer.Option(None, "--dbpath", "-d", envvar="QUANTDATA_HOME"),
    host: Optional[str] = typer.Option(None, "--host", help="监听地址（默认 config http.host）"),
    port: Optional[int] = typer.Option(None, "--port", help="监听端口（默认 config http.port）"),
    no_http: bool = typer.Option(False, "--no-http", help="纯 CLI 批处理，不起网关"),
    insecure: bool = typer.Option(False, "--insecure", help="显式允许非 loopback 且无 token（§8.4）"),
) -> None:
    """常驻：单写者锁 + 调度 + FastAPI 网关（恒前台，Linux 交 supervisord）。"""
    from .serve.runtime import run_serve

    dbpath = resolve_dbpath(d)
    code = asyncio.run(
        run_serve(dbpath, host=host, port=port, no_http=no_http, insecure=insecure)
    )
    raise typer.Exit(code)


@app.command()
def update(
    d: Optional[str] = typer.Option(None, "--dbpath", "-d", envvar="QUANTDATA_HOME"),
    dataset: str = typer.Option("all", "--dataset", help="securities,calendar,bars_1d 或 all"),
    market: str = typer.Option("cn", "--market"),
    symbols: Optional[str] = typer.Option(None, "--symbols", help="逗号分隔，缺省=securities 全量"),
    start: Optional[str] = typer.Option(None, "--start", help="日K 起始 YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, "--end", help="日K 结束 YYYY-MM-DD"),
) -> None:
    """单次更新（cron 兜底）：主源 pytdxdata。幂等，重跑安全。"""
    from .sources import TdxAdapter

    dbpath = resolve_dbpath(d)
    layout = DbPathLayout(dbpath)
    conn = open_catalog(layout.root)
    try:
        migrate(conn)
        datasets = (
            ["securities", "calendar", "bars_1d"] if dataset == "all" else
            [x.strip() for x in dataset.split(",") if x.strip()]
        )
        sym_list = [s.strip() for s in symbols.split(",")] if symbols else None
        adapter = TdxAdapter()
        console.print(f"[bold]quantdata update[/bold] market={market} datasets={datasets}")

        # 适配器 open/close 与 ETL 必须在同一事件循环（pytdxdata 纯 asyncio）
        from .etl.update import run_update as _run_update

        async def _run():
            await adapter.open()
            try:
                return await _run_update(
                    conn, market, adapter,
                    datasets=datasets, symbols=sym_list,
                    start=date.fromisoformat(start) if start else None,
                    end=date.fromisoformat(end) if end else None,
                )
            finally:
                await adapter.close()

        reports = asyncio.run(_run())
        for r in reports:
            console.print(
                f"  {r.dataset}: rows={r.rows} symbols={len(r.symbols)} "
                f"range=[{r.date_min} ~ {r.date_max}]"
            )
        console.print("[green]update 完成[/green]")
    finally:
        conn.close()


@app.command()
def query(
    sql: str = typer.Argument(..., help="SQL（仅只读查询）"),
    d: Optional[str] = typer.Option(None, "--dbpath", "-d", envvar="QUANTDATA_HOME"),
    fmt: str = typer.Option("table", "--fmt", help="table|json|csv|parquet|arrow"),
    o: Optional[Path] = typer.Option(None, "-o", help="csv/parquet/arrow 输出文件"),
) -> None:
    """本地直查（只读连接，不开服务也可用）。"""
    if fmt not in ("table", "json", "csv", "parquet", "arrow"):
        err.print(f"[red]--fmt 不支持：{fmt}[/red]")
        raise typer.Exit(1)
    if fmt in ("csv", "parquet", "arrow") and o is None:
        err.print(f"[red]--fmt {fmt} 需要 -o 输出文件[/red]")
        raise typer.Exit(1)
    dbpath = resolve_dbpath(d)
    conn = _open_ro(dbpath)
    try:
        from .store.db import configure_query_conn

        configure_query_conn(conn)
        try:
            validate(sql)  # 本地 CLI 也过闸：与 HTTP 同规则（§8.3）
        except SqlRejected as e:
            err.print(f"[red]SQL 被拒绝：[/red]{e.reason}")
            raise typer.Exit(1) from e
        tbl = conn.execute(sql).arrow().read_all()
        if fmt == "table":
            table = Table(*tbl.schema.names)
            for row in table_to_rows(tbl):
                table.add_row(*[str(v) for v in row])
            console.print(table)
            console.print(f"[dim]{tbl.num_rows} rows[/dim]")
        elif fmt == "json":
            # 纯 stdout JSON（无 rich 前缀/换行噪声），便于 `| jq` 与程序消费
            print(
                json.dumps(
                    {
                        "columns": tbl.schema.names,
                        "dtypes": [str(f.type) for f in tbl.schema],
                        "rows": table_to_rows(tbl),
                        "row_count": tbl.num_rows,
                    },
                    default=str,
                    ensure_ascii=False,
                )
            )
        else:
            # arrow 单走 IPC file 落盘（write_export 仅 csv/parquet，直接调会抛
            # ExportFormatError —— 0.1.0 实测 bug，此处曾把 arrow 透传进去）
            result = (
                write_arrow_file(tbl, Path(o))
                if fmt == "arrow"
                else write_export(tbl, fmt, Path(o))
            )
            console.print(f"[green]已写出[/green] {result.path}（{result.rows} 行 / {result.bytes_written} 字节）")
    finally:
        conn.close()


def table_to_rows(tbl):
    if tbl.num_columns == 0:
        return []
    cols = [c.to_pylist() for c in tbl.columns]
    return [list(row) for row in zip(*cols)]


@app.command()
def export(
    sql: str = typer.Option(..., "--sql", help="导出查询"),
    d: Optional[str] = typer.Option(None, "--dbpath", "-d", envvar="QUANTDATA_HOME"),
    fmt: str = typer.Option("csv", "--fmt", help="csv|parquet"),
    o: Path = typer.Option(..., "-o", help="输出文件"),
) -> None:
    """导出落盘格式（csv/parquet）；pandas/polars 走 query --fmt arrow（§9）。"""
    if fmt not in EXPORT_FORMATS:
        err.print(f"[red]export 仅支持 {sorted(EXPORT_FORMATS)}；pandas/polars 走 query --fmt arrow[/red]")
        raise typer.Exit(1)
    dbpath = resolve_dbpath(d)
    conn = _open_ro(dbpath)
    try:
        from .store.db import configure_query_conn

        configure_query_conn(conn)
        try:
            validate(sql)
        except SqlRejected as e:
            err.print(f"[red]SQL 被拒绝：[/red]{e.reason}")
            raise typer.Exit(1) from e
        tbl = conn.execute(sql).arrow().read_all()
        result = write_export(tbl, fmt, Path(o))
        console.print(f"[green]导出完成[/green] {result.path}（{result.rows} 行 / {result.bytes_written} 字节）")
    finally:
        conn.close()


@app.command()
def status(
    d: Optional[str] = typer.Option(None, "--dbpath", "-d", envvar="QUANTDATA_HOME"),
    json_out: bool = typer.Option(False, "--json", help="结构化 JSON 输出（脚本化）"),
) -> None:
    """水位、锁、HTTP 配置、磁盘占用（运维入口）。"""
    from .lockfile import is_alive, read_lock
    from .store import current_version

    dbpath = resolve_dbpath(d)
    layout = DbPathLayout(dbpath)
    info: dict = {"dbpath": str(layout.root)}
    lock = read_lock(layout.lock_file)
    info["lock"] = (
        {"held": True, "pid": lock.pid, "alive": is_alive(lock)} if lock else {"held": False}
    )
    info["http"] = {"enabled": True, "host": "127.0.0.1", "port": 8765, "note": "config 段 http"}
    conn = _open_ro(layout.root)
    try:
        info["schema_version"] = current_version(conn)
        cps = conn.execute(
            "SELECT dataset, market, scope, watermark, rows, status FROM etl_checkpoint "
            "ORDER BY dataset, market, scope"
        ).fetchall()
        info["checkpoints"] = [
            {"dataset": r[0], "market": r[1], "scope": r[2], "watermark": r[3], "rows": r[4], "status": r[5]}
            for r in cps
        ]
        info["bars_1d_rows"] = conn.execute("SELECT count(*) FROM bars_1d").fetchone()[0]
    finally:
        conn.close()
    try:
        import psutil

        du = psutil.disk_usage(str(layout.root))
        info["disk"] = {"used_pct": round(du.percent, 1), "free_gb": round(du.free / 1e9, 1)}
    except Exception:  # noqa: BLE001 — 磁盘信息尽力而为
        info["disk"] = None
    # --json 与默认都输出同一份 JSON（默认人类可读，--json 供程序消费；M1 共用渲染）
    console.print_json(json.dumps(info, default=str, ensure_ascii=False))


def main() -> None:
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
