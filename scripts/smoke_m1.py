"""M1 交付自检冒烟脚本（[CI] 验收链的可复用副本 + 每周 smoke workflow 素材）。

设计 §14 白名单：uvicorn 真起服 / socket 层不计入覆盖率，替代验证 = 本脚本。
与 `.github/workflows/ci.yml` 的验收链同源，可本地或 CI 直接跑：

    python scripts/smoke_m1.py                 # 用临时 dbpath，自建自销
    python scripts/smoke_m1.py -d /path/db     # 用已有库
    python scripts/smoke_m1.py --port 18766

覆盖：init --skip-history + 迷你 seed → 真 serve（真 socket）→
/healthz、/v1/ready、POST /v1/query（json + arrow）、POST /v1/export（csv 落盘 + 行数）、
SQL 闸 fail-closed 400 → 关闭 → 端口释放。任一步失败非零退出。
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
FIXTURE = REPO / "tests" / "fixtures" / "ci_seed_1d.parquet"
PY = REPO / ".venv" / "Scripts" / "python.exe"
if not PY.exists():  # 非 venv 环境（Linux CI 用 uv run 时走 sys.executable）
    PY = Path(sys.executable)

FAILED: list[str] = []

# 本机常见坑：环境里挂着 `http_proxy=http://127.0.0.1:xxxxx`（指向已死的代理），
# urllib 默认走它 → 连 127.0.0.1 也超时，误判成「服务没起来」。
# 冒烟目标是本机 loopback，必须显式 bypass。
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def check(label: str, ok: bool, detail: str = "") -> None:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""), flush=True)
    if not ok:
        FAILED.append(label)


def http(base: str, path: str, *, data: dict | None = None, method: str = "GET",
         timeout: float = 20.0) -> tuple[int, bytes]:
    req = urllib.request.Request(base + path, method=method)
    body = None
    if data is not None:
        req.add_header("Content-Type", "application/json")
        body = json.dumps(data).encode()
    with _OPENER.open(req, data=body, timeout=timeout) as r:
        return r.status, r.read()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="M1 交付自检冒烟")
    ap.add_argument("-d", "--dbpath", default=None, help="库目录（缺省=临时目录，跑完清理）")
    ap.add_argument("--port", type=int, default=18766)
    args = ap.parse_args(argv)

    tmp_root: Path | None = None
    if args.dbpath:
        dbpath = Path(args.dbpath).resolve()
    else:
        tmp_root = Path(tempfile.mkdtemp(prefix="pyqd_smoke_"))
        dbpath = tmp_root / "db"

    base = f"http://127.0.0.1:{args.port}"
    proc: subprocess.Popen | None = None
    try:
        # ---------- 1. init --skip-history + 迷你 seed ----------
        print("[1/4] init --skip-history + 迷你 seed")
        r = subprocess.run(
            [str(PY), "-m", "pyquantdata.cli", "init", "-d", str(dbpath),
             "--skip-history", "--seed", str(FIXTURE)],
            capture_output=True, text=True, cwd=str(REPO),
        )
        check("init 退出码 0", r.returncode == 0, r.stdout.strip().splitlines()[-1:] and
              r.stdout.strip().splitlines()[-1] or r.stderr[:200])
        check("init 自检输出含 bars_1d rows=20", "bars_1d rows=20" in r.stdout)

        # ---------- 2. 真 serve（真 socket） ----------
        print("[2/4] 起真 serve（真 uvicorn + 真端口）")
        log = tempfile.NamedTemporaryFile(prefix="pyqd_smoke_srv_", suffix=".log",
                                         delete=False)
        proc = subprocess.Popen(
            [str(PY), "-m", "pyquantdata.cli", "serve", "-d", str(dbpath),
             "--port", str(args.port)],
            stdout=log, stderr=subprocess.STDOUT, cwd=str(REPO),
        )
        up = False
        for _ in range(60):  # 最多等 30s
            if proc.poll() is not None:
                break
            try:
                http(base, "/healthz", timeout=2)
                up = True
                break
            except Exception:
                time.sleep(0.5)
        check("serve 起服可连（真端口）", up,
              "" if up else f"进程退出码={proc.poll()} 日志={Path(log.name).read_text()[-300:]}")

        if up:
            # ---------- 3. 端点面 ----------
            print("[3/4] 端点面")
            s, b = http(base, "/healthz")
            check("/healthz 200", s == 200, b.decode()[:80])

            s, b = http(base, "/v1/ready")
            ready = json.loads(b)
            check("/v1/ready ready=true", s == 200 and ready.get("ready") is True,
                  f"schema_version={ready.get('checks', {}).get('schema_version')}")

            s, b = http(base, "/v1/query", method="POST",
                        data={"sql": "SELECT count(*) AS n FROM cn_stock_1d"})
            q = json.loads(b)
            check("POST /v1/query 日K row_count=1 且 n=20",
                  s == 200 and q.get("row_count") == 1 and q["rows"][0][0] == 20,
                  f"n={q.get('rows')}")

            # arrow IPC 解码路径（附录 A：pandas/polars 消费端）
            s, b = http(base, "/v1/query", method="POST",
                        data={"sql": "SELECT date, close FROM cn_stock_1d ORDER BY date",
                              "fmt": "arrow"})
            arrow_ok = False
            n_rows = -1
            if s == 200:
                try:
                    import pyarrow as pa
                    tbl = pa.ipc.open_stream(
                        pa.BufferReader(base64.b64decode(json.loads(b)["b64_arrow"]))
                    ).read_all()
                    n_rows = tbl.num_rows
                    arrow_ok = n_rows == 20
                except Exception as e:  # noqa: BLE001
                    arrow_ok = False
                    n_rows = repr(e)
            check("POST /v1/query fmt=arrow 可解出 20 行", arrow_ok, f"rows={n_rows}")

            # 恶意查询必须被 SQL 闸拒（fail-closed，§8.3）
            evil = "SELECT read_parquet('/etc/passwd')"
            code = -1
            try:
                code, _ = http(base, "/v1/query", method="POST", data={"sql": evil})
            except urllib.error.HTTPError as e:
                code = e.code
            check("SQL 闸拒绝 read_parquet（400）", code == 400, f"status={code}")

            # ---------- 4. export csv（真落盘 exports/{job_id}/） ----------
            print("[4/4] export csv")
            s, b = http(base, "/v1/export", method="POST", data={
                "sql": "SELECT symbol, date, close FROM cn_stock_1d ORDER BY date",
                "fmt": "csv", "filename": "smoke_daily.csv",
            })
            lines = b.decode("utf-8").strip().splitlines() if s == 200 else []
            header = [c.strip('"') for c in lines[0].split(",")] if lines else []
            check("export csv 表头+20 行", s == 200 and len(lines) == 21
                  and header == ["symbol", "date", "close"],
                  f"status={s} lines={len(lines)} header={header}")

            s, b = http(base, "/v1/query", method="POST", data={
                "sql": "SELECT job_id, rows, state FROM export_jobs"})
            job = json.loads(b)
            check("export_jobs 落库 rows=20 state=done",
                  s == 200 and job["rows"][0][1] == 20 and job["rows"][0][2] == "done",
                  str(job.get("rows")))

            # 落盘校验（CSV 内容语义，不受引号风格影响）
            on_disk = list((dbpath / "exports").glob("*/smoke_daily.csv"))
            disk_ok = False
            if on_disk:
                with on_disk[0].open(encoding="utf-8", newline="") as fh:
                    rows = list(csv.reader(fh))
                disk_ok = len(rows) == 21 and rows[0] == ["symbol", "date", "close"]
            check("export 落盘 exports/{job_id}/ 内容正确", disk_ok,
                  str(on_disk[0].relative_to(dbpath)) if on_disk else "未找到文件")

            # CLI 只读直查（需先释放 serve 的写连接）
            print("[5/5] CLI 只读直查（停服后）")
        else:
            print("[3/4] 跳过（服务未起）")
            print("[4/4] 跳过（服务未起）")

        # ---------- 关闭 + 端口释放 ----------
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        freed = False
        for _ in range(20):
            try:
                http(base, "/healthz", timeout=1)
                time.sleep(0.3)
            except Exception:
                freed = True
                break
        check("停服后端口已释放", freed)

        if up:
            r = subprocess.run(
                [str(PY), "-m", "pyquantdata.cli", "query", "-d", str(dbpath),
                 "--fmt", "json", "SELECT count(*) AS n FROM cn_stock_1d"],
                capture_output=True, text=True, cwd=str(REPO),
            )
            check("CLI 只读直查 --fmt json 正常",
                  r.returncode == 0 and '"row_count": 1' in r.stdout,
                  (r.stdout or r.stderr)[:150])

        # ---------- 汇总 ----------
        print()
        if FAILED:
            print(f"SMOKE FAILED：{len(FAILED)} 项 —— " + "; ".join(FAILED))
            return 1
        print("SMOKE OK：M1 全链路（真起服 + 真端口 + 全端点）通过")
        return 0
    finally:
        if tmp_root is not None:
            shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
