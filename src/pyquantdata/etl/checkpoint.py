"""etl_checkpoint 断点与水位（设计 §7.4）。"""

from __future__ import annotations

import duckdb


def upsert_checkpoint(
    conn: duckdb.DuckDBPyConnection,
    dataset: str,
    market: str,
    scope: str,
    watermark: str,
    rows: int,
    status: str = "ok",
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO etl_checkpoint "
        "(dataset, market, scope, watermark, rows, status, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, now())",
        [dataset, market, scope, watermark, rows, status],
    )


def list_checkpoints(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = conn.execute(
        "SELECT dataset, market, scope, watermark, rows, status, updated_at "
        "FROM etl_checkpoint ORDER BY dataset, market, scope"
    ).fetchall()
    return [
        {
            "dataset": r[0],
            "market": r[1],
            "scope": r[2],
            "watermark": r[3],
            "rows": r[4],
            "status": r[5],
            "updated_at": None if r[6] is None else r[6].isoformat(),
        }
        for r in rows
    ]


def failed_checkpoints(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    return [c for c in list_checkpoints(conn) if c["status"] != "ok"]
