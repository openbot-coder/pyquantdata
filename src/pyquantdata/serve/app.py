"""FastAPI 服务网关（设计 §8）：REST /v1/query /v1/export /v1/meta /v1/stats /v1/state + 探针。

执行模型（§7.6）：查询一律线程池执行（事件循环零阻塞）；
超时 = ``connection.interrupt()`` 真中断；并发信号量 + 等待超时 429（§8.3）。
"""

from __future__ import annotations

import asyncio
import base64
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import psutil
import pyarrow as pa
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

from .. import __version__
from ..config import AppConfig
from ..export.files import (
    EXPORT_FORMATS,
    ExportPathError,
    get_export_job,
    new_job_id,
    record_export_job,
    resolve_export_target,
    write_export,
)
from ..sqlgate import SqlRejected, validate
from .auth import token_ok
from .state import MARKET_TZ, market_state


class ResultTooLargeError(ValueError):
    pass


class QueryTimeoutError(RuntimeError):
    pass


@dataclass
class AppContext:
    """app 依赖注入载体。cursor 由调用方提供（serve 从写连接派生；测试注入）。"""

    dbpath: Path
    config: AppConfig
    cursor: duckdb.DuckDBPyConnection
    schema_version: int = 0


class QueryRequest(BaseModel):
    sql: str
    params: list[Any] = Field(default_factory=list)
    fmt: str = "json"  # json | arrow
    max_rows: int | None = None
    timeout_ms: int | None = None


class ExportRequest(BaseModel):
    fmt: str  # csv | parquet
    sql: str
    params: list[Any] = Field(default_factory=list)
    filename: str | None = None
    sync: bool = True


def execute_query_blocking(
    cursor: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any],
    max_rows: int,
    max_arrow_bytes: int,
) -> tuple[pa.Table, bool]:
    """线程池内执行：fetch Arrow → 上限校验 → 截断。返回 (table, truncated)。"""
    result = cursor.execute(sql, params or [])
    tbl = result.arrow().read_all()
    if tbl.nbytes > max_arrow_bytes:
        raise ResultTooLargeError(
            f"结果 {tbl.nbytes} 字节超过上限 {max_arrow_bytes}：请改走 /v1/export（§8.3）"
        )
    truncated = tbl.num_rows > max_rows
    if truncated:
        tbl = tbl.slice(0, max_rows)
    return tbl, truncated


def arrow_stream_b64(tbl: pa.Table) -> str:
    """Arrow IPC **streaming format** → base64（附录 A 解码路径）。"""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as writer:
        writer.write_table(tbl)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")


def table_to_rows(tbl: pa.Table) -> list[list[Any]]:
    if tbl.num_columns == 0:
        return []
    cols = [c.to_pylist() for c in tbl.columns]
    return [list(row) for row in zip(*cols)]


def utc_now_iso() -> str:
    """ISO8601 UTC 带 Z（§8.2 全文统一）。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def query_response(
    tbl: pa.Table, truncated: bool, elapsed_ms: int, fmt: str
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "columns": tbl.schema.names,
        "dtypes": [str(f.type) for f in tbl.schema],
        "row_count": tbl.num_rows,
        "elapsed_ms": elapsed_ms,
        "truncated": truncated,
    }
    if fmt == "arrow":
        out["b64_arrow"] = arrow_stream_b64(tbl)
    else:
        out["rows"] = table_to_rows(tbl)
    return out


def disk_usage_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


async def _run_with_timeout(
    ctx: AppContext,
    sql: str,
    params: list[Any],
    max_rows: int,
    timeout_s: float,
) -> tuple[pa.Table, bool]:
    """线程池执行 + 超时真中断（§7.6）。query / export 共用同一执行语义。

    不能用 ``asyncio.wait_for(to_thread(...))``：超时时 wait_for 会 cancel 那个
    已成 future，随后收割阶段抛的 CancelledError 会逃逸出调用方的 except 分支
    （Python 3.13 实测）。这里用 shield + 手动超时，超时后 interrupt 连接，
    再把线程异常全部吞掉，只向外报 504。
    """
    task = asyncio.ensure_future(
        asyncio.to_thread(
            execute_query_blocking, ctx.cursor, sql, params, max_rows, ctx.config.sql.max_arrow_bytes
        )
    )
    done, _pending = await asyncio.wait({task}, timeout=timeout_s)
    if not done:
        ctx.cursor.interrupt()  # 真中断：线程内查询被打断
        try:
            await asyncio.shield(task)  # 收割线程异常，保证不悬空
        except BaseException:  # noqa: BLE001 — 中断/取消异常统一吞掉，对外只报 504
            pass
        raise HTTPException(
            status_code=504, detail=f"查询超时（>{timeout_s:.1f}s），已 interrupt 中断"
        )
    return task.result()


def create_app(ctx: AppContext) -> FastAPI:
    sql_cfg = ctx.config.sql
    query_sem = asyncio.Semaphore(max(1, sql_cfg.max_concurrent))

    def _validate_sql(sql: str) -> None:
        try:
            validate(sql, sql_cfg.allow_functions)
        except SqlRejected as e:
            raise HTTPException(status_code=400, detail=e.reason) from e

    async def _acquire_query_slot() -> None:
        try:
            await asyncio.wait_for(query_sem.acquire(), timeout=sql_cfg.acquire_timeout_s)
        except asyncio.TimeoutError as e:
            raise HTTPException(
                status_code=429,
                detail=f"查询并发已达上限 {sql_cfg.max_concurrent}，排队超时（§8.3）",
            ) from e

    async def require_token(
        authorization: str | None = Header(default=None),
    ) -> None:
        provided = None
        if authorization and authorization.startswith("Bearer "):
            provided = authorization[len("Bearer ") :].strip()
        if not token_ok(provided, ctx.config.http.token):
            raise HTTPException(status_code=401, detail="invalid or missing token")

    app = FastAPI(title="quantdata", version=__version__, docs_url="/docs")

    # ---- 探针 ---------------------------------------------------------------
    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/v1/ready")
    def ready() -> Any:
        checks: dict[str, Any] = {}
        try:
            ctx.cursor.execute("SELECT 1")
            checks["catalog"] = "ok"
        except duckdb.Error as e:
            checks["catalog"] = f"error: {e}"
        checks["schema_version"] = ctx.schema_version
        checks["schema"] = "ok" if ctx.schema_version >= 1 else "not_migrated"
        free_pct: float | None = None
        if ctx.dbpath.exists():
            usage = psutil.disk_usage(str(ctx.dbpath))
            free_pct = round(100.0 - usage.percent, 2)
        checks["disk_free_pct"] = free_pct
        disk_ok = free_pct is None or free_pct > 5.0  # <5% 硬阈值（§13）
        if checks["catalog"] == "ok" and checks["schema"] == "ok" and disk_ok:
            return {"ready": True, "checks": checks}
        return JSONResponse(status_code=503, content={"ready": False, "checks": checks})

    # ---- query ---------------------------------------------------------------
    @app.post("/v1/query", dependencies=[Depends(require_token)])
    async def query(body: QueryRequest) -> dict[str, Any]:
        if body.fmt not in ("json", "arrow"):
            raise HTTPException(status_code=422, detail=f"fmt 仅支持 json|arrow，收到 {body.fmt!r}")
        _validate_sql(body.sql)
        max_rows = min(body.max_rows or sql_cfg.max_rows, sql_cfg.max_rows)
        timeout_s = (body.timeout_ms if body.timeout_ms is not None else sql_cfg.timeout_ms) / 1000
        started = time.monotonic()
        await _acquire_query_slot()
        try:
            tbl, truncated = await _run_with_timeout(
                ctx, body.sql, body.params, max_rows, timeout_s
            )
        except ResultTooLargeError as e:
            raise HTTPException(status_code=413, detail=str(e)) from e
        except duckdb.Error as e:
            if "interrupt" in str(e).lower():
                raise HTTPException(status_code=504, detail=f"查询被中断：{e}") from e
            raise HTTPException(status_code=400, detail=f"执行失败：{e}") from e
        elapsed = int((time.monotonic() - started) * 1000)
        return query_response(tbl, truncated, elapsed, body.fmt)

    # ---- export（M1 同步路径：小导出直回文件流；job 化在 M2）--------------------
    @app.post("/v1/export", dependencies=[Depends(require_token)])
    async def export(body: ExportRequest) -> FileResponse:
        if body.fmt not in EXPORT_FORMATS:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"export 仅支持落盘格式 {sorted(EXPORT_FORMATS)}；"
                    f"pandas/polars 走 /v1/query fmt=arrow（§9 双语义归位）"
                ),
            )
        _validate_sql(body.sql)
        timeout_s = sql_cfg.timeout_ms / 1000
        await _acquire_query_slot()
        try:
            tbl, _truncated = await _run_with_timeout(
                ctx, body.sql, body.params, sql_cfg.max_rows, timeout_s
            )
        except duckdb.Error as e:
            raise HTTPException(status_code=400, detail=f"执行失败：{e}") from e
        job_id = new_job_id()
        filename = body.filename or f"export.{body.fmt}"
        try:
            target = resolve_export_target(
                ctx.dbpath / ctx.config.export.out_dir, job_id, filename
            )
        except ExportPathError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        result = await asyncio.to_thread(write_export, tbl, body.fmt, target)
        record_export_job(
            ctx.cursor,
            job_id,
            fmt=body.fmt,
            path=str(target.relative_to(ctx.dbpath)),
            rows=result.rows,
            bytes_written=result.bytes_written,
        )
        media = "text/csv" if body.fmt == "csv" else "application/octet-stream"
        return FileResponse(target, media_type=media, filename=filename)

    @app.get("/v1/export/{job_id}", dependencies=[Depends(require_token)])
    def export_job(job_id: str) -> dict:
        job = get_export_job(ctx.cursor, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"导出任务不存在：{job_id}")
        return job

    # ---- meta / stats / state -------------------------------------------------
    @app.get("/v1/meta", dependencies=[Depends(require_token)])
    def meta() -> dict:
        names = [
            r[0]
            for r in ctx.cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_type='VIEW' ORDER BY table_name"
            ).fetchall()
        ]
        views = []
        for name in names:
            info: dict[str, Any] = {"name": name}
            info["columns"] = [
                r[0]
                for r in ctx.cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
                    [name],
                ).fetchall()
            ]
            info["row_count"] = ctx.cursor.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            cols = set(info["columns"])
            if "date" in cols:
                row = ctx.cursor.execute(
                    f"SELECT min(date), max(date) FROM {name}"
                ).fetchone()
                info["date_min"] = None if row[0] is None else row[0].isoformat()
                info["date_max"] = None if row[1] is None else row[1].isoformat()
            if "symbol" in cols:
                info["sample_symbols"] = [
                    r[0]
                    for r in ctx.cursor.execute(
                        f"SELECT DISTINCT symbol FROM {name} WHERE symbol IS NOT NULL LIMIT 3"
                    ).fetchall()
                ]
            views.append(info)
        return {
            "version": __version__,
            "schema_version": ctx.schema_version,
            "views": views,
        }

    @app.get("/v1/stats", dependencies=[Depends(require_token)])
    def stats() -> dict:
        cps = ctx.cursor.execute(
            "SELECT dataset, market, scope, watermark, rows, status, updated_at "
            "FROM etl_checkpoint ORDER BY dataset, market, scope"
        ).fetchall()
        jobs = ctx.cursor.execute(
            "SELECT state, count(*) FROM export_jobs GROUP BY state"
        ).fetchall()
        usage = None
        if ctx.dbpath.exists():
            usage = {"bytes": disk_usage_bytes(ctx.dbpath)}
            try:
                du = psutil.disk_usage(str(ctx.dbpath))
                usage["free_pct"] = round(100.0 - du.percent, 2)
            except OSError:
                usage["free_pct"] = None
        return {
            "checkpoints": [
                {
                    "dataset": r[0], "market": r[1], "scope": r[2], "watermark": r[3],
                    "rows": r[4], "status": r[5],
                    "updated_at": None if r[6] is None else r[6].isoformat(),
                }
                for r in cps
            ],
            "export_jobs": {state: n for state, n in jobs},
            "disk": usage,
            "http": {"ws_conns": 0},  # WS 在 M3；指标占位（§13）
        }

    @app.get("/v1/state/{market}", dependencies=[Depends(require_token)])
    def market_state_endpoint(market: str) -> dict:
        tz = MARKET_TZ.get(market)
        if tz is None:
            raise HTTPException(status_code=404, detail=f"未知市场：{market}")
        now_local = datetime.now(ZoneInfo(tz))
        row = ctx.cursor.execute(
            "SELECT is_open FROM calendar WHERE market=? AND trade_date=?",
            [market, now_local.date()],
        ).fetchone()
        is_trading_day = bool(row[0]) if row else False
        if row is None:
            reason = "no_calendar_data"
        else:
            reason = None
        hhmm = now_local.hour * 100 + now_local.minute
        ms = market_state(market, hhmm, is_trading_day=is_trading_day)
        if reason:
            ms.reason = reason
        return {
            "market": market,
            "state": ms.state,
            "ts": utc_now_iso(),
            "next_at": ms.next_at,
            "session_tz": ms.session_tz,
            "reason": ms.reason,
        }

    return app
