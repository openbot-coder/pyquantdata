"""[CI] 验收链（设计 §14/§15）：init --skip-history + 迷你 seed → serve → POST /v1/query 查日K → export csv。

一条链必绿，全部离线。
"""

from __future__ import annotations

import base64

import pyarrow as pa
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from pyquantdata.cli import app
from pyquantdata.config import default_config
from pyquantdata.serve.app import AppContext, create_app
from pyquantdata.store.db import configure_query_conn

runner = CliRunner()


def test_m1_ci_acceptance_chain(catalog, dbpath, mini_seed, tmp_path):
    # 1. init --skip-history + seed（CLI 入口，建目录/config/schema/seed）
    result = runner.invoke(app, [
        "init", "-d", str(dbpath), "--skip-history", "--seed", str(mini_seed),
    ])
    assert result.exit_code == 0, result.output

    # 2. serve 语境（TestClient 同事件循环；真网 uvicorn 按 §14 白名单每周冒烟）
    cursor = catalog.cursor()
    configure_query_conn(cursor)
    ctx = AppContext(dbpath=dbpath, config=default_config(), cursor=cursor, schema_version=1)
    with TestClient(create_app(ctx)) as client:
        # 3. POST /v1/query 查到日K（参数化 + JSON）
        r = client.post("/v1/query", json={
            "sql": "SELECT date, close FROM cn_stock_1d WHERE symbol = ? ORDER BY date",
            "params": ["sh600000"],
        })
        assert r.status_code == 200
        body = r.json()
        assert body["row_count"] == 10
        assert body["rows"][0][1] == 10.2  # 9/1 close

        # 3b. fmt=arrow → Arrow IPC streaming → DataFrame 生态解码路径（附录 A）
        r_arrow = client.post("/v1/query", json={
            "sql": "SELECT date, close FROM cn_stock_1d ORDER BY date", "fmt": "arrow",
        })
        tbl = pa.ipc.open_stream(
            pa.BufferReader(base64.b64decode(r_arrow.json()["b64_arrow"]))
        ).read_all()
        df = tbl.to_pandas()
        assert len(df) == 10 and float(df["close"].iloc[-1]) == 11.1

        # 4. export csv（文件流回传 + exports/{job_id}/ 落盘 + job 记录）
        r_exp = client.post("/v1/export", json={
            "sql": "SELECT symbol, date, open, close FROM cn_stock_1d ORDER BY date",
            "fmt": "csv", "filename": "mini_daily.csv",
        })
        assert r_exp.status_code == 200
        lines = r_exp.content.decode("utf-8").strip().splitlines()
        header = [c.strip('"') for c in lines[0].split(",")]
        assert header == ["symbol", "date", "open", "close"] and len(lines) == 11

        job_row = client.post(
            "/v1/query", json={"sql": "SELECT job_id, rows, state FROM export_jobs"}
        ).json()
        assert job_row["rows"][0][1] == 10 and job_row["rows"][0][2] == "done"

        # 6. SQL 闸在链路内生效
        blocked = client.post("/v1/query", json={"sql": "SELECT read_parquet('x')"})
        assert blocked.status_code == 400

    # 7. CLI 本地直查（只读连接，serve 之外也可用）
    # DuckDB 同进程不允许同一库文件「写连接 + 只读连接」并存（配置不同会直接报错）——
    # 先释放本测试持有的写连接，再走 CLI 只读路径。
    cursor.close()
    catalog.close()
    q = runner.invoke(app, [
        "query", "-d", str(dbpath), "--fmt", "json",
        "SELECT count(*) AS n FROM cn_stock_1d",
    ])
    assert q.exit_code == 0, q.output
    assert '"row_count": 1' in q.output
