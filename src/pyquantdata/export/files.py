"""导出落盘（设计 §9）。

- export 只管落盘格式：csv / parquet（pandas/polars 走 /v1/query fmt=arrow 消费）；
- 路径安全（评审 P1）：输出根恒为 exports/{job_id}/，名称字段白名单字符过滤 +
  resolve 后必须仍在 exports/ 前缀内；杜绝 ../ 穿出 dbpath；
- job 结果记 export_jobs 表（spec hash 缓存 24h 的键在 M2 worker 化时接入）。
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as paparquet

_JOB_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

EXPORT_FORMATS: frozenset[str] = frozenset({"csv", "parquet"})


class ExportPathError(ValueError):
    pass


class ExportFormatError(ValueError):
    pass


def new_job_id() -> str:
    return uuid.uuid4().hex[:12]


def spec_hash(spec: dict) -> str:
    """导出请求的稳定指纹（缓存键，§9）。"""
    canonical = json.dumps(spec, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def resolve_export_target(exports_dir: Path, job_id: str, filename: str) -> Path:
    """exports/{job_id}/{filename}；白名单 + resolve 前缀校验（§9 路径安全）。

    纵深防御：``_JOB_ID_RE`` / ``_FILENAME_RE`` 已禁掉 ``/`` 与 ``..``，因此常规
    输入下 resolve 后必然在 exports/ 内。最后的 startswith 校验是为**符号链接**
    （exports/{job_id} 被替换成指向外部的软链）兜底——该分支在无软链的环境里
    结构性不可达，保留为防线而非死代码。
    """
    if not _JOB_ID_RE.match(job_id):
        raise ExportPathError(f"非法 job_id：{job_id!r}")
    if not _FILENAME_RE.match(filename):
        raise ExportPathError(f"非法导出文件名：{filename!r}")
    exports_dir = Path(exports_dir).resolve()
    target = (exports_dir / job_id / filename).resolve()
    root_str = str(exports_dir).rstrip("\\/") + "\\"
    posix_root = str(exports_dir).rstrip("\\/") + "/"
    if not (str(target).startswith(root_str) or str(target).startswith(posix_root)):
        raise ExportPathError(f"导出路径越界：{target}")
    return target


@dataclass
class ExportResult:
    path: Path
    rows: int
    bytes_written: int
    fmt: str


def run_query_arrow(
    conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None
) -> pa.Table:
    """执行查询返回 Arrow 表（查询/导出共用；调用方负责 sqlgate 校验与线程池）。"""
    result = conn.execute(sql, params or [])
    return result.arrow().read_all()


def write_export(table: pa.Table, fmt: str, target: Path) -> ExportResult:
    if fmt not in EXPORT_FORMATS:
        raise ExportFormatError(f"export 仅支持落盘格式 {sorted(EXPORT_FORMATS)}，收到 {fmt!r}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        pacsv.write_csv(table, target)
    else:
        paparquet.write_table(table, target, compression="zstd")
    size = target.stat().st_size
    return ExportResult(path=target, rows=table.num_rows, bytes_written=size, fmt=fmt)


def write_arrow_file(table: pa.Table, target: Path) -> ExportResult:
    """``query --fmt arrow -o`` 专用：Arrow IPC **file** 格式落盘。

    与 HTTP ``/v1/query`` 的 ``b64_arrow``（streaming format）刻意区分：
    落盘产物要给 ``polars.read_ipc`` / ``pyarrow.ipc.open_file`` 随时重开，
    file 格式带 footer 与随机访问；streaming 只适合一次性管道传输。
    不进 ``EXPORT_FORMATS`` —— 那是 export 命令 / HTTP export 的语义（csv/parquet）。
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    with pa.ipc.new_file(target, table.schema) as writer:
        writer.write_table(table)
    size = target.stat().st_size
    return ExportResult(path=target, rows=table.num_rows, bytes_written=size, fmt="arrow")


def record_export_job(
    conn: duckdb.DuckDBPyConnection,
    job_id: str,
    *,
    fmt: str,
    path: str,
    rows: int,
    bytes_written: int,
    state: str = "done",
    spec_json: str = "{}",
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO export_jobs "
        "(job_id, spec_json, fmt, path, rows, bytes, state, created_at, finished_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, now(), now())",
        [job_id, spec_json, fmt, path, rows, bytes_written, state],
    )


def get_export_job(conn: duckdb.DuckDBPyConnection, job_id: str) -> dict | None:
    row = conn.execute(
        "SELECT job_id, spec_json, fmt, path, rows, bytes, state, "
        "created_at, finished_at FROM export_jobs WHERE job_id = ?",
        [job_id],
    ).fetchone()
    if row is None:
        return None
    return {
        "job_id": row[0],
        "spec_json": row[1],
        "fmt": row[2],
        "path": row[3],
        "rows": row[4],
        "bytes": row[5],
        "state": row[6],
        "created_at": None if row[7] is None else row[7].isoformat(),
        "finished_at": None if row[8] is None else row[8].isoformat(),
    }
