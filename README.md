# pyquantdata（CLI 名 `quantdata`）

单目录自包含、三市场（A/美/港）覆盖的量化数据中台：`init` 建库回填，`serve` 常驻更新 + FastAPI 对外服务，一切状态收敛在 `-d` 指定的 dbpath。

设计方案见 [docs/quantdata-design.md](docs/quantdata-design.md)（v0.7，圆桌评审 45 条已采纳）。

## 当前状态：M1 最小闭环

| 能力 | 说明 |
|---|---|
| `init` | 建目录 + config.json + DuckDB schema（migrations：14 表 + 8 联邦视图）+ seed 导入 + 自检；`--estimate` 只算磁盘预算 |
| `serve` | 单写者锁（PID 三元判活）+ FastAPI 网关，恒前台常驻 |
| `update` | A股证券 / 交易日历 / 日K（主源 pytdxdata），整区替换幂等 |
| `query` | 本地只读直查（不开服务也可用），`--fmt table\|json\|csv\|parquet\|arrow` |
| `export` | csv / parquet 落盘（恒 `exports/{job_id}/`，路径双判防穿越） |
| `status` | 水位 / 锁 / HTTP / 磁盘，`--json` 结构化输出 |

**安全约束**（设计 §8.3 / §8.4）：

- SQL 闸 **fail-closed**：sqlglot 解析、根节点白名单、多语句拒绝、文件/网络表函数默认拒绝（`SELECT read_csv('/etc/passwd')` → 400）；
- 查询走线程池，超时 = `connection.interrupt()` 真中断 → 504；并发信号量 4，超出排队 429；
- 只读连接断扩展自动加载（堵 httpfs 出站）+ `memory_limit`；
- 非 loopback 且 token 为空 → 拒绝启动（`--insecure` 显式放行）。

## 安装

```bash
uv sync                      # 开发安装（含 dev 依赖）
# 或
uv pip install pyquantdata   # PyPI 安装（发布后）
```

Python `>=3.11,<3.14`（3.14 在 Windows 上 asyncio socketpair 死锁，已排除）。

## 快速开始

```bash
# 1. 建库（离线迷你 seed 演示；真实回填去掉 --skip-history）
uv run quantdata init -d tmp-db --skip-history --seed tests/fixtures/ci_seed_1d.parquet

# 2. 本地直查
uv run quantdata query -d tmp-db "SELECT date, close FROM cn_stock_1d ORDER BY date DESC LIMIT 5"

# 3. 起服务（默认 127.0.0.1:8765）
uv run quantdata serve -d tmp-db
```

```bash
curl -s localhost:8765/healthz          # {"status":"ok"}
curl -s localhost:8765/v1/ready         # readiness：catalog/schema/磁盘
curl -s -X POST localhost:8765/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT count(*) FROM cn_stock_1d"}'
```

### REST 端点（前缀 `/v1`）

| 端点 | 方法 | 用途 |
|---|---|---|
| `/v1/query` | POST | `{sql, params, fmt: json\|arrow}` → 结果（arrow = Arrow IPC base64，pandas/polars 直吃） |
| `/v1/export` | POST | `{sql, fmt: csv\|parquet, filename}` → 文件流 + `exports/{job_id}/` 落盘 |
| `/v1/export/{job_id}` | GET | 导出任务状态 |
| `/v1/meta` / `/v1/stats` | GET | 字典结构 / 水位磁盘统计 |
| `/v1/state/{market}` | GET | 市场状态机（CLOSED/OPEN/LUNCH_BREAK…） |
| `/healthz` `/v1/ready` | GET | liveness / readiness 探针 |

Swagger UI：起服后访问 `http://127.0.0.1:8765/docs`（OpenAPI 免费得）。

## 测试与验收

```bash
# 单测 + 100% 覆盖率门禁（[CI] 验收链 tests/test_ci_chain.py 含在内）
uv run pytest --cov=pyquantdata --cov-report=term-missing --cov-fail-under=100

# M1 交付自检冒烟（真起服 + 真端口 + 全端点，11 项断言）
uv run python scripts/smoke_m1.py
```

CI（GitHub Actions）：Linux + Windows 矩阵 → lint → 覆盖率门禁 → fixture 一致性校验 → **真起服验收链**（init → serve → query → export → SQL 闸 400 → 端口释放）。

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

- **M1 ✅** 最小闭环（本仓库当前状态）
- **M2** 1m 回填 + compaction + 除权/ST/涨跌停 + 每日质检 + qlib/backtrader 导出
- **M3** 美/港三市场 + WS 推流（快照/状态/新闻）
- **M4** 运维加固（supervisord 部署、doctor、薄 SDK）

## License

MIT
