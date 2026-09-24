"""FastAPI 全端点（TestClient）：query/export/meta/stats/state/ready/token。"""

from __future__ import annotations

import base64
import threading
import time

import duckdb
import pyarrow as pa
import pytest

from pyquantdata.config import default_config
from pyquantdata.etl import import_seed
from pyquantdata.serve.app import AppContext, ResultTooLargeError, create_app
from pyquantdata.store.db import configure_query_conn


@pytest.fixture
def seeded(catalog, dbpath, mini_seed):
    import_seed(catalog, mini_seed)
    catalog.execute(
        "INSERT INTO calendar VALUES ('cn', CURRENT_DATE, TRUE, 'test'), "
        "('us', CURRENT_DATE, FALSE, 'weekend-test')"
    )
    cursor = catalog.cursor()
    configure_query_conn(cursor)
    ctx = AppContext(dbpath=dbpath, config=default_config(), cursor=cursor, schema_version=1)
    return catalog, ctx


@pytest.fixture
def client(seeded):
    from fastapi.testclient import TestClient

    _catalog, ctx = seeded
    with TestClient(create_app(ctx)) as c:
        yield c


@pytest.fixture
def client_429(catalog, dbpath, mini_seed):
    import_seed(catalog, mini_seed)
    cfg = default_config()
    cfg.sql.max_concurrent = 1
    cfg.sql.acquire_timeout_s = 0.2
    cursor = catalog.cursor()
    configure_query_conn(cursor)
    ctx = AppContext(dbpath=dbpath, config=cfg, cursor=cursor, schema_version=1)
    from fastapi.testclient import TestClient

    with TestClient(create_app(ctx)) as c:
        yield c


# 合法表上的超慢查询：cn_stock_1d 自笛卡尔积（10^10 行级）→ 必被 timeout 中断。
# 不用 range() 表函数：SQL 闸按设计拒绝 FROM 位置的表函数（§8.3 fail-closed）。
SLOW_SQL = "SELECT count(*) FROM " + " CROSS JOIN ".join(
    f"cn_stock_1d t{i}" for i in range(10)
)


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_ready_ok(client):
    r = client.get("/v1/ready")
    assert r.status_code == 200
    body = r.json()
    assert body["ready"] is True
    assert body["checks"]["schema_version"] == 1
    assert body["checks"]["disk_free_pct"] is not None


def test_ready_503_when_not_migrated(catalog, dbpath):
    from fastapi.testclient import TestClient

    cursor = catalog.cursor()
    ctx = AppContext(dbpath=dbpath, config=default_config(), cursor=cursor, schema_version=0)
    with TestClient(create_app(ctx)) as c:
        r = c.get("/v1/ready")
    assert r.status_code == 503
    assert r.json()["ready"] is False


def test_query_json_basics(client):
    r = client.post("/v1/query", json={"sql": "SELECT date, close FROM cn_stock_1d ORDER BY date"})
    assert r.status_code == 200
    body = r.json()
    assert body["columns"] == ["date", "close"]
    assert body["row_count"] == 10
    assert body["truncated"] is False
    assert body["elapsed_ms"] >= 0
    assert body["rows"][0][0] == "2026-09-01"
    assert isinstance(body["rows"][0][1], float)


def test_query_arrow_stream_b64(client):
    r = client.post("/v1/query", json={
        "sql": "SELECT symbol, date, close FROM cn_stock_1d ORDER BY date", "fmt": "arrow",
    })
    assert r.status_code == 200
    b64 = r.json()["b64_arrow"]
    tbl = pa.ipc.open_stream(pa.BufferReader(base64.b64decode(b64))).read_all()
    assert tbl.schema.names == ["symbol", "date", "close"]
    assert tbl.num_rows == 10
    assert tbl.column("symbol")[0].as_py() == "sh600000"


def test_query_fmt_invalid(client):
    r = client.post("/v1/query", json={"sql": "SELECT 1", "fmt": "feather"})
    assert r.status_code == 422


def test_query_params_placeholder(client):
    r = client.post("/v1/query", json={
        "sql": "SELECT close FROM cn_stock_1d WHERE symbol = ? AND date = ?",
        "params": ["sh600000", "2026-09-03"],
    })
    assert r.status_code == 200
    assert r.json()["row_count"] == 1


def test_query_max_rows_truncates(client):
    r = client.post("/v1/query", json={"sql": "SELECT * FROM cn_stock_1d", "max_rows": 3})
    assert r.status_code == 200
    body = r.json()
    assert body["row_count"] == 3
    assert body["truncated"] is True


def test_query_max_rows_clamped_to_config(client):
    r = client.post("/v1/query", json={"sql": "SELECT * FROM cn_stock_1d", "max_rows": 10**9})
    assert r.status_code == 200  # clamp 到 config.max_rows，不报错


def test_query_rejects_forbidden_sql(client):
    r = client.post("/v1/query", json={"sql": "SELECT read_csv('/etc/passwd')"})
    assert r.status_code == 400
    assert "read_csv" in r.json()["detail"]


def test_query_execution_error_400(client):
    r = client.post("/v1/query", json={"sql": "SELECT * FROM table_not_here"})
    assert r.status_code == 400
    assert "执行失败" in r.json()["detail"]


def test_query_timeout_interrupts(client):
    started = time.monotonic()
    r = client.post("/v1/query", json={"sql": SLOW_SQL, "timeout_ms": 200})
    elapsed = time.monotonic() - started
    assert r.status_code == 504
    assert elapsed < 5  # interrupt 真中断，不等查询跑完
    # 连接复用：interrupt 后仍可查询
    r2 = client.post("/v1/query", json={"sql": "SELECT 1"})
    assert r2.status_code == 200


def test_query_interrupt_exception_maps_504(client, monkeypatch):
    import pyquantdata.serve.app as app_mod

    def boom(*a, **k):
        raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")

    monkeypatch.setattr(app_mod, "execute_query_blocking", boom)
    r = client.post("/v1/query", json={"sql": "SELECT 1"})
    assert r.status_code == 504


def test_query_result_too_large_maps_413(client, monkeypatch):
    import pyquantdata.serve.app as app_mod

    def too_big(*a, **k):
        raise ResultTooLargeError("结果过大：请走 export")

    monkeypatch.setattr(app_mod, "execute_query_blocking", too_big)
    r = client.post("/v1/query", json={"sql": "SELECT 1"})
    assert r.status_code == 413


def test_query_429_when_slot_busy(client_429):
    results: dict[str, int] = {}

    def slow():
        r = client_429.post("/v1/query", json={"sql": SLOW_SQL, "timeout_ms": 4000})
        results["slow"] = r.status_code

    t = threading.Thread(target=slow)
    t.start()
    time.sleep(0.5)  # slow 已占用唯一信号量槽
    r2 = client_429.post("/v1/query", json={"sql": "SELECT 1"})
    t.join(timeout=20)
    assert r2.status_code == 429
    assert "并发" in r2.json()["detail"]
    assert results["slow"] in (200, 504)  # 大查询最终超时中断


def test_export_csv_roundtrip(client, dbpath):
    r = client.post("/v1/export", json={
        "sql": "SELECT symbol, date, close FROM cn_stock_1d ORDER BY date",
        "fmt": "csv", "filename": "daily.csv",
    })
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    lines = r.content.decode("utf-8").strip().splitlines()
    # pyarrow 默认 RFC4180 加引号；解析后断言列名与行数
    assert [c.strip('"') for c in lines[0].split(",")] == ["symbol", "date", "close"]
    assert len(lines) == 11  # header + 10
    # exports/{job_id}/ 落盘 + job 记录可查
    jobs = client.post("/v1/query", json={"sql": "SELECT job_id FROM export_jobs"}).json()
    job_id = jobs["rows"][0][0]
    job = client.get(f"/v1/export/{job_id}").json()
    assert job["state"] == "done"
    assert job["rows"] == 10
    assert job["path"].startswith("exports")
    assert (dbpath / job["path"]).exists()


def test_export_parquet_roundtrip(client, dbpath):
    import io

    import pyarrow.parquet as paparquet

    r = client.post("/v1/export", json={"sql": "SELECT * FROM cn_stock_1d", "fmt": "parquet"})
    assert r.status_code == 200
    tbl = paparquet.read_table(io.BytesIO(r.content))
    assert tbl.num_rows == 10


def test_export_pandas_rejected_with_hint(client):
    r = client.post("/v1/export", json={"sql": "SELECT 1", "fmt": "pandas"})
    assert r.status_code == 422
    assert "/v1/query" in r.json()["detail"]


def test_export_path_escape_rejected(client):
    r = client.post("/v1/export", json={
        "sql": "SELECT 1", "fmt": "csv", "filename": "../evil.csv",
    })
    assert r.status_code == 400
    assert "非法" in r.json()["detail"]


def test_export_job_404(client):
    assert client.get("/v1/export/nope").status_code == 404


def test_meta_views_and_stats(client):
    meta = client.get("/v1/meta").json()
    assert meta["schema_version"] == 1
    names = {v["name"] for v in meta["views"]}
    assert "cn_stock_1d" in names
    v = next(v for v in meta["views"] if v["name"] == "cn_stock_1d")
    assert v["row_count"] == 10
    assert v["date_min"] == "2026-09-01"
    assert v["sample_symbols"] == ["sh600000"]
    stats = client.get("/v1/stats").json()
    assert stats["checkpoints"] == []
    assert stats["disk"]["bytes"] > 0
    assert stats["http"]["ws_conns"] == 0


def test_meta_handles_special_views(catalog, seeded):
    _cat, ctx = seeded
    from fastapi.testclient import TestClient

    catalog.execute("CREATE OR REPLACE VIEW meta_special AS SELECT 42 AS v")
    with TestClient(create_app(ctx)) as c:
        meta = c.get("/v1/meta").json()
    special = next(v for v in meta["views"] if v["name"] == "meta_special")
    assert special["columns"] == ["v"]
    assert "date_min" not in special
    assert "sample_symbols" not in special


def test_state_endpoint(client):
    r = client.get("/v1/state/cn")
    assert r.status_code == 200
    body = r.json()
    assert body["market"] == "cn"
    assert body["state"] in {"CLOSED", "PRE_OPEN_AUCTION", "OPEN", "LUNCH_BREAK"}
    assert body["session_tz"] == "Asia/Shanghai"
    assert body["ts"].endswith("Z")  # ISO8601 UTC 带 Z（§8.2）


def test_state_no_calendar_data_is_closed(client):
    body = client.get("/v1/state/hk").json()
    assert body["state"] == "CLOSED"
    assert body["reason"] == "no_calendar_data"


def test_state_unknown_market_404(client):
    assert client.get("/v1/state/uk").status_code == 404


def test_ready_disk_free_branch_none(catalog, tmp_path):
    """dbpath 不存在 → free_pct None 分支（只读探针仍 ready）。"""
    from fastapi.testclient import TestClient

    cursor = catalog.cursor()
    cursor.execute("SELECT 1")
    ctx = AppContext(
        dbpath=tmp_path / "missing", config=default_config(),
        cursor=cursor, schema_version=1,
    )
    with TestClient(create_app(ctx)) as c:
        assert c.get("/v1/ready").status_code == 200


def test_stats_disk_oserror_branch(catalog, dbpath, monkeypatch):
    from fastapi.testclient import TestClient

    cursor = catalog.cursor()
    cursor.execute("SELECT 1")
    ctx = AppContext(dbpath=dbpath, config=default_config(), cursor=cursor, schema_version=1)

    import pyquantdata.serve.app as app_mod

    def boom(_):
        raise OSError("disk gone")

    monkeypatch.setattr(app_mod.psutil, "disk_usage", boom)
    with TestClient(create_app(ctx)) as c:
        body = c.get("/v1/stats").json()
    assert body["disk"]["free_pct"] is None


def test_token_auth(catalog, dbpath, mini_seed):
    import_seed(catalog, mini_seed)
    cfg = default_config()
    cfg.http.token = "abcd1234"
    cursor = catalog.cursor()
    configure_query_conn(cursor)
    from fastapi.testclient import TestClient

    with TestClient(create_app(AppContext(dbpath=dbpath, config=cfg, cursor=cursor, schema_version=1))) as c:
        assert c.post("/v1/query", json={"sql": "SELECT 1"}).status_code == 401
        assert c.post(
            "/v1/query", json={"sql": "SELECT 1"},
            headers={"Authorization": "Bearer wrong"},
        ).status_code == 401
        ok = c.post(
            "/v1/query", json={"sql": "SELECT 1"},
            headers={"Authorization": "Bearer abcd1234"},
        )
        assert ok.status_code == 200
        # 探针不设防
        assert c.get("/healthz").status_code == 200
