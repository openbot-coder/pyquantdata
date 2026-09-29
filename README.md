# pyquantdata（CLI 名 `quantdata`）

[![PyPI](https://img.shields.io/pypi/v/pyquantdata)](https://pypi.org/project/pyquantdata/)
[![CI](https://github.com/openbot-coder/pyquantdata/actions/workflows/ci.yml/badge.svg)](https://github.com/openbot-coder/pyquantdata/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/pyquantdata)](https://pypi.org/project/pyquantdata/)
[![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/openbot-coder/pyquantdata/blob/main/LICENSE)

单目录自包含的量化数据中台：`init` 建库，`update` 回填，`query`/`export` 只读消费，`serve` 常驻 + FastAPI 对外服务——一切状态收敛在 `-d` 指定的 dbpath，可整体拷贝/备份。

**当前版本 0.1.1（M1 最小闭环 + arrow 落盘修复）**，已发布 [PyPI](https://pypi.org/project/pyquantdata/)。设计方案见 [docs/quantdata-design.md](https://github.com/openbot-coder/pyquantdata/blob/main/docs/quantdata-design.md)（v0.7，圆桌评审 45 条已采纳）。

## 已实现功能

| 能力 | 说明 |
|---|---|
| `init` | 建目录 + config.json + DuckDB schema（**14 业务表 + 8 联邦视图**，migrations 版本化）+ seed 导入 + 自检；`--estimate` 只算磁盘预算不写库；幂等（重跑=续跑） |
| `serve` | 单写者锁（PID 三元判活）+ FastAPI 网关恒前台常驻；`--no-http` 纯批处理模式 |
| `update` | A股证券 / 交易日历 / 日K 三数据集（主源 pytdxdata），整区替换幂等、checkpoint 水位、重跑安全 |
| `query` | 本地只读直查（**不开服务也可用**，与 HTTP 同一套 SQL 闸）；`--fmt table\|json\|csv\|parquet\|arrow` |
| `export` | csv / parquet 落盘（恒 `exports/{job_id}/`，路径双判防穿越） |
| `status` | 水位 checkpoint / 锁 / schema 版本 / 磁盘占用，`--json` 结构化输出 |
| REST API | 8 端点：查询、导出、元数据、水位统计、市场状态机、liveness/readiness 探针 |
| 安全闸 | SQL fail-closed + Bearer 鉴权 + 非 loopback 无 token 拒绝启动 + 只读连接断扩展/限内存 |

## 安装

```bash
uv pip install pyquantdata     # PyPI
# 或开发模式
git clone git@github.com:openbot-coder/pyquantdata.git && cd pyquantdata && uv sync
```

Python `>=3.11,<3.14`（3.14 在 Windows 上 asyncio socketpair 死锁，已排除）。

## 快速开始

```bash
# 1. 建库（离线迷你 seed 演示；真实回填去掉 --skip-history、接 --seed 或跑 update）
quantdata init -d tmp-db --skip-history --seed tests/fixtures/ci_seed_1d.parquet

# 2. 本地直查（不开服务）
quantdata query -d tmp-db "SELECT date, close FROM cn_stock_1d ORDER BY date DESC LIMIT 5"

# 3. 起服务（默认 127.0.0.1:8765）
quantdata serve -d tmp-db
```

```bash
curl -s localhost:8765/healthz                    # {"status":"ok"}（免鉴权）
curl -s localhost:8765/v1/ready                   # readiness：catalog/schema/磁盘（免鉴权）
curl -s localhost:8765/v1/meta -H "Authorization: Bearer $TOKEN"
curl -s -X POST localhost:8765/v1/query \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT count(*) FROM cn_stock_1d"}'
```

## CLI 命令参考

所有命令支持 `-d/--dbpath`（或环境变量 `QUANTDATA_HOME`）指定库目录。

| 命令 | 关键参数 | 说明 |
|---|---|---|
| `init` | `--markets cn,us,hk`、`--seed <parquet/csv>`、`--skip-history`、`--estimate` | 建目录 + config + schema + seed 导入 + 自检 |
| `serve` | `--host`、`--port`、`--no-http`、`--insecure` | 单写者锁 + 网关，恒前台（Linux 交 supervisord） |
| `update` | `--dataset securities,calendar,bars_1d\|all`、`--market cn`、`--symbols sh600000,...`、`--start/--end` | 单次更新（cron 兜底），主源 pytdxdata，幂等 |
| `query` | `--fmt table\|json\|csv\|parquet\|arrow`、`-o <file>`（文件格式必填） | 本地只读直查，纯 stdout JSON 可直接 `\| jq` |
| `export` | `--sql <SQL>`、`--fmt csv\|parquet`、`-o <file>`（必填） | 落盘导出 |
| `status` | `--json` | 水位 / 锁 / schema 版本 / 磁盘 |

### 接 pandas / polars

```bash
# 方式一：CLI 导出 Arrow IPC file 格式（footer 完整，可随机重开）
quantdata query -d tmp-db --fmt arrow -o out.arrow "SELECT * FROM cn_stock_1d"
```

```python
import polars as pl
df = pl.read_ipc("out.arrow")          # 或 pyarrow.ipc.open_file("out.arrow").read_all()
```

```python
# 方式二：HTTP 拿 Arrow IPC **streaming** 格式 base64（字段名 b64_arrow）
import base64, json, urllib.request, pyarrow as pa
req = urllib.request.Request(
    "http://127.0.0.1:8765/v1/query",
    data=json.dumps({"sql": "SELECT * FROM cn_stock_1d", "fmt": "arrow"}).encode(),
    headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"},
)
b64 = json.load(urllib.request.urlopen(req))["b64_arrow"]
tbl = pa.ipc.open_stream(pa.py_buffer(base64.b64decode(b64))).read_all()
df = tbl.to_pandas()                   # polars: pl.from_arrow(tbl)
```

## REST API（前缀 `/v1`）

| 端点 | 方法 | 鉴权 | 用途 |
|---|---|---|---|
| `/healthz` | GET | 免 | liveness 探针 |
| `/v1/ready` | GET | 免 | readiness：catalog / schema / 磁盘 |
| `/v1/query` | POST | Bearer | `{sql, params, fmt: json\|arrow, max_rows, timeout_ms}` → 结果；arrow 时字段为 `b64_arrow` |
| `/v1/export` | POST | Bearer | `{sql, fmt: csv\|parquet, filename, params, sync}` → 文件流 + 落盘 |
| `/v1/export/{job_id}` | GET | Bearer | 导出任务状态 |
| `/v1/meta` | GET | Bearer | 字典结构（表/列/视图） |
| `/v1/stats` | GET | Bearer | 水位 / 磁盘统计 |
| `/v1/state/{market}` | GET | Bearer | 市场状态机（CLOSED / OPEN / LUNCH_BREAK…） |

- 鉴权：`Authorization: Bearer <token>`（token 在 dbpath/`config.json` 的 `http.token`，支持 `${ENV}` 展开），缺失或错误 → 401。
- Swagger UI：起服后访问 `http://127.0.0.1:8765/docs`（OpenAPI 免费得）。
- 并发：查询走线程池，超时 = `connection.interrupt()` 真中断 → 504；信号量 4，超出排队 → 429。

## 数据模型（init 产物）

**14 张业务表**（+ `schema_migrations` 版本表）：

| 分类 | 表 |
|---|---|
| 行情 | `bars_1d`（日K 物理表） |
| 参考数据 | `securities`、`calendar`、`exchanges`、`sw_industry`、`stock_status` |
| 公司行动 | `corp_actions`、`announcements` |
| 资讯/其它 | `news`、`reports`、`futures_meta` |
| 运维 | `etl_checkpoint`（水位）、`quality_reports`（质检）、`export_jobs`（导出任务） |

**8 个联邦视图**（SQL 透明层，直接查视图名即可，无需关心物理表）：

| 市场 | 视图 |
|---|---|
| A股 | `cn_stock_1d`、`cn_index_1d`、`cn_etf_1d`、`cn_cbond_1d` |
| 美股 | `us_stock_1d`、`us_index_1d` |
| 港股 | `hk_stock_1d`、`hk_index_1d` |

> M1 视图仅覆盖日K；1 分钟级与其它数据范围属 M2（设计 §4.2/§3.3）。

## dbpath 目录布局（§3.1）

```
<dbpath>/
├── config.json        # 配置（JSON、extra=forbid、${ENV} 展开）
├── catalog.duckdb     # 元数据 + 数据（单文件可拷贝）
├── quantdata.lock     # 单写者锁（PID + start_time + boot_id 三元判活）
├── data/              # 数据文件（parquet 分区）
├── exports/{job_id}/  # 导出落盘（恒在此前缀内，防穿越）
├── scripts/           # 用户脚本
└── logs/              # 日志
```

## 配置（dbpath/config.json）

`init` 自动生成，`extra=forbid` 严格校验，字符串支持 `${ENV}` 展开。主要段：

| 段 | 字段（节选） |
|---|---|
| `markets` | 每市场 `enabled` / `history_from`（cn 默认 2004-01-01，us/hk 2020-01-01） |
| `http` | `host` / `port`（默认 127.0.0.1:8765）/ `token` / `max_ws_conns` / `ws_queue_len` |
| `etl` | `bars` / `realtime` / `reference` / `news` / `fallback` / `cross_check` 等源开关 |
| `rate_limit` | `global` / `per_source` / `rate_limit_rps` |

## 安全约束（设计 §8.3 / §8.4）

- **SQL 闸 fail-closed**：sqlglot 解析 → 根节点白名单（仅 SELECT）→ 多语句拒绝 → 文件/网络表函数默认拒绝（`SELECT read_csv('/etc/passwd')` → 400）；**本地 CLI 与 HTTP 同一套规则**。
- 只读连接：禁扩展自动加载（堵 httpfs 出站）+ `memory_limit`。
- **非 loopback 且 token 为空 → 拒绝启动**（`--insecure` 显式放行）。
- 导出路径安全：输出恒 `exports/{job_id}/`，文件名白名单 + resolve 前缀双校验，杜绝 `../` 穿出 dbpath（符号链接兜底）。
- 单写者锁：`serve` 独占写锁，PID + start_time + boot_id 三元判活，防误杀/误接管。

## 测试与验收

```bash
# 单测 + 100% 覆盖率门禁（[CI] 验收链 tests/test_ci_chain.py 含在内）
uv run pytest --cov=pyquantdata --cov-report=term-missing --cov-fail-under=100

# M1 交付自检冒烟（真起服 + 真端口 + 全端点，11 项断言）
uv run python scripts/smoke_m1.py
```

**254 测试 / 100% 覆盖率**。CI（GitHub Actions）Linux + Windows 矩阵：lint → 覆盖率门禁 → fixture 一致性校验（`build_ci_fixture.py --check`）→ **真起服验收链**（init → serve → query → export → SQL 闸 400 → 停服端口释放断言）。

## 项目结构

```
src/pyquantdata/
├── cli.py            # typer 命令：init/serve/update/query/export/status
├── config.py         # config.json 模型（JSON、extra=forbid、${ENV} 展开）
├── paths.py          # dbpath 目录规范（§3.1）
├── lockfile.py       # 单写者锁：PID + start_time + boot_id 三元判活（§11）
├── store/            # DuckDB 连接管理 + migrations（14 表 8 视图）
├── sqlgate/          # SQL 闸（sqlglot fail-closed，§8.3）
├── sources/          # SourceAdapter 协议 + TdxAdapter + 日历补全
├── etl/              # update/checkpoint/seed（整区替换幂等，§7.3）
├── serve/            # FastAPI 网关 + runtime + 状态机 + 鉴权
└── export/           # csv/parquet 落盘（路径安全，§9）
```

## 路线图（设计 §15）

- **M1 ✅** 最小闭环（v0.1.0 交付；0.1.1 = 修复 + 文档，当前状态）
- **M2** 1m 回填 + compaction + 除权/ST/涨跌停 + 每日质检 + qlib/backtrader 导出
- **M3** 美/港三市场实际回填 + WS 推流（快照/状态/新闻）
- **M4** 运维加固（supervisord 部署、doctor、薄 SDK）

> M1 明确未含：历史回填调度（`backfill`）、`doctor` 自愈、1 分钟 K 线、美/港数据实际写入（表与视图已建，`update` 主源为 A 股 pytdxdata）、WebSocket 推流。

## License

MIT
