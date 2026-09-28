# pyquantdata（CLI 名 `quantdata`）设计方案

**版本**：v0.7（M1 实施校准版）  
**日期**：2026-09-23  
**修订**：v0.2 —— ①数据源调整（去 tqcenter，加 yfinance 参考 + akshare 摘抄/primp 兜底，不安装 akshare）②调度任务脚本化（dbpath/scripts/ 每任务一脚本）③config.json 扩展（抓取时间/代理服务器/数据源要求）  
**评审**：v0.3 —— 八项开放问题全部拍板（§16 转为决策记录）：质量校验确认 / 起始年限 cn2004·us2020·hk2020 / 暂缓品类走可扩展结构 / 研报·申万=akshare 摘抄 / 3s 快照够用 / nats-server 随库自带不依赖预装 / 部署=Linux / 加 union 视图  
**修订**：v0.4 —— NATS 去掉 local/remote/none 三模式（宝爷提议）：客户端只剩 `servers`（默认 `nats://127.0.0.1:4222`）+ `autostart`（仅本机且无监听才拉起 bin/nats-server）+ `enabled`（false=纯 CLI）  
**修订**：v0.5 —— 放弃 NATS（宝爷拍板 A 方案）：对外服务改为 serve 进程内 FastAPI —— REST 管 query/export/meta/stats，WebSocket `/v1/stream` 管订阅推送（quote/state/news/flow/ev）；`--no-http` 纯 CLI  
**修订**：v0.6 —— 圆桌评审 45 条全采纳（6 席全票「修改后可实施」）：新增 §7.6 事件循环隔离、SQL 函数默认拒绝、compaction 两阶段 .trash 协议、覆盖率排除白名单、验收数据两层、qlib 测试两层、news 表 DDL、复权口径、WS 统一时序、export 同步/异步归位  
**修订**：v0.7 —— M1 实施校准 3 条（实施阶段才暴露、评审阶段发现不了的）：DP-1 §11 锁判活从「三元一致」改为可实现的「PID 存活 + 三元全可读且一致」，后两项降为补强证据、取不到即接管（否则锁死库）；DP-2 §14 白名单「CI 自己真起服」表述纠偏 —— CI 的真是**用 CLI 起真 serve + 真端口 curl**（用户面全覆盖），仅 `runtime.py` uvicorn/socket 段 pragma 豁免，真起服交付自检交每周 smoke；DP-3 §14 验收 fixture 表名笔误（`cn_stock_1d` 实为 `bars_1d`，视图名≠物理表名）。  
**实现状态**：2026-09-24 **M1 已交付，发布 PyPI `pyquantdata==0.1.0`**（254 测试 100% 覆盖、CI 双平台真起服验收链绿、release.yml OIDC 自动发布）。落地明细与功能清单见 §15 实现状态附记及 [README](https://github.com/openbot-coder/pyquantdata/blob/main/README.md)。  
**定位**：单目录自包含、三市场（A/美/港）覆盖的量化数据中台 —— `init` 建库回填，`serve` 常驻更新 + 每日质检 + 通过 FastAPI（REST+WebSocket）对外服务。

---

## 0. 结论摘要（TL;DR）

| 维度   | 决策                                                                                                                                                                                                                                                                                                            | 理由                                                                      |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| 形态   | 单一 Python 包 `pyquantdata`，入口命令 `quantdata`；一切状态收敛在 `-d` 指定的 dbpath；dbpath/config.json 统一配置抓取数据时间、代理服务器、数据源要求                                                                                                                                                                           | 换机器 = 拷一个目录；与 pytdxdata/pywestockdata 同风格（uv + src-layout）              |
| 存储   | **DuckDB**（参考数据、日K、公告研报、元数据）+ **Parquet 分区文件**（1分钟K线），查询层用统一视图联邦                                                                                                                                                                                                                                              | 1m 数据是几十亿行量级，Parquet 分区不可变文件最适合"每日增量追加"；DuckDB 负责小表事务与秒查；两层用视图缝合，SQL 无感 |
| 写入模型 | **单写者**：只有 `serve`（或一次性 `update/backfill`）持有写锁；查询/导出一律只读                                                                                                                                                                                                                                                      | DuckDB 单写多读 + Parquet 不可变文件，天然无并发写冲突                                    |
| 服务协议 | **FastAPI 进程内网关（REST + WebSocket）**：serve 单进程同起 uvicorn（默认 `127.0.0.1:8765`）；`/v1/query` 等 REST 管请求-响应，`/v1/stream` WebSocket 管订阅推送；`--no-http` = 纯本地仅 CLI | 一条进程零外部依赖；开内网只改 host（+token）                                                  |
| 对外服务 | 三类:① REST `POST /v1/query` / `POST /v1/export` / `GET /v1/meta`;② WS `/v1/stream` 行情快照 + 自选股逐笔;③ WS 事件流 市场状态 / 分钟新闻 / 公告研报 / ETL进度 / 质检报告                                                                                                                                                                                                       | 对应你列的三种服务形式，全部走 HTTP 路径 + WS topic 分层                                         |
| 数据源  | **pytdxdata 为主**（三市场K线/行情/复权/期货/期权），**pywestockdata 为辅**（A股实时与财务交叉验证、公告分红），**美/港参考 yfinance**；长尾兜底移植 akshare 源码（摘抄其实现、改用 primp 发请求过反爬，**不安装 akshare 库**）；日历/状态机吸收 market-event-driver；新闻采集脚本归入 dbpath/scripts/ | 不重造轮子，全部是已有项目                                                           |
| 历史回填 | 支持 **seed 导入**：直接把 stock_prediction 已有的 A股 1m（2004 至今）搬进库，避免重复回填 22 年                                                                                                                                                                                                                                         | 手上有现成数据                                                                 |
| 容量   | 默认口径估算 数据 **300–400 GB** + 运行 buffer ≥200GB（预算口径 ~600GB，见 §3.4）；起始年限已确认 cn=2004 / us=2020 / hk=2020                                                                                                                                                                                                                                                                  | 建库前先给用户看到磁盘预算                                                           |

---

## 1. 目标、非目标、假设

### 1.1 目标（来自需求）

1. **初始化**：`quantdata init -d /xxx/dbpath/` —— 自动建目录结构、建元数据库、按配置回填各市场历史数据，断点可续。
2. **日常更新**：`quantdata serve -d /xxx/dbpath/` —— 常驻进程：盘中增量更新、收盘后日频任务、每日数据质量校验，并常驻对外服务。
3. **对外服务（HTTP + WebSocket）**：
   - `query_sql`：执行 SQL 查询（请求/响应）；
   - `export`：导出 backtrader / qlib / pandas / polars / csv 等格式；
   - **订阅**：股票实时行情、市场状态变化（开盘/收盘/午休等）、分钟级新闻推流。
4. **数据范围**：A股（指数、股票、期货、可转债、ETF/LOF、REITS、期权的证券信息、交易日历、1m K、日K、除权除息、公告、研报、申万行业、ST、停牌、涨跌停）；美股、港股（指数、股票、期货、ETF/LOF、REITS 等证券信息、交易日历、1m K、日K）。
5. **对外服务协议**：默认 serve 进程内 FastAPI（REST + WebSocket），零外部依赖；NATS 方案已放弃（v0.5 拍板）。

### 1.2 非目标（明确边界，防止范围膨胀）

- ❌ 因子计算 / 特征工程 —— 归 factor-pipeline / vxdata，quantdata 只负责把干净的原始数据喂出去；
- ❌ 回测引擎 / 交易执行 —— export 成 backtrader/qlib 格式供那边用；
- ❌ 逐笔撮合级实时行情 —— 实时层是 **秒级快照 + 自选股 tick**，不是 Level-2；
- ❌ 多租户权限体系 —— v1 是单用户/内网工具，认证靠可选 `http.token`（默认空 = 内网免认证）。

### 1.3 评审已确认事项（2026-09-23 拍板）

| 事项 | 决策 |
|------|------|
| "每日对数据进行…" | ✅ **每日数据质量校验**（gap / OHLC / 涨跌停 / 复权对账 / 双源抽查 1%），见 §7.5 |
| 各市场起始年限 | ✅ **cn=2004、us=2020、hk=2020** |
| 美股可转债 / LOF 等无零售级数据源的品类 | ✅ **先放空，走可扩展结构**：结构预留、需要时写一个 `scripts/` 采集脚本即可接入（机制见 §4.2） |
| 研报 / 申万行业数据源 | ✅ **akshare 对应接口（研报 choices 等），摘抄源码 + primp 重写请求**，不安装 akshare |
| 实时行情精度 | ✅ **3 秒快照 + 自选 tick**，不做逐笔 |
| 对外服务协议 | ✅ **v0.5 改判：放弃 NATS** —— serve 进程内 FastAPI：REST 管 query/export，WS `/v1/stream` 管订阅；一条进程全包 |
| 部署目标 | ✅ **Linux 服务器长期常驻**（supervisord），Windows 仅开发用 |
| 跨市场 union 视图 | ✅ **加 `bars_1m_all`**（见 §3.3） |

---

## 2. 总体架构

```
                     ┌────────────────────────────────────────────────┐
   quantdata init    │                本机 / 服务器                     │
   quantdata serve   │                                                │
   quantdata query ──▶  CLI (typer+rich)                              │
   quantdata export   │                                                │
                     │  ┌───────────── 应用层（serve 常驻）───────────┐ │
                     │  │ Scheduler 调度器（任务=dbpath/scripts/*.py 每任务一脚本，META 声明触发时机）          │ │
                     │  │  ├─ IntradayUpdater  盘中 1m/快照 增量       │ │
                     │  │  ├─ DailyJob         收盘后日频任务链         │ │
                     │  │  ├─ QualityChecker   每日质检                │ │
                     │  │  ├─ StateEngine      市场状态机（开盘/午休…） │ │
                     │  │  ├─ QuotePoller      实时行情轮询→推流        │ │
                     │  │  └─ NewsBridge       新闻采集→分钟窗口推流    │ │
                     │  └────────────────────────────────────────────┘ │
                     │  ┌────────── 服务网关（FastAPI 同进程）─────────┐ │
                     │  │ REST: /v1/query /v1/export /v1/meta /stats  │ │
                     │  │ WS:   /v1/stream (quote state news flow ev) │ │
                     │  │ --no-http 可关；Swagger 自带 /docs            │ │
                     │  └────────────────────────────────────────────┘ │
                     │  ┌───────────── 存储层 ───────────────────────┐ │
                     │  │ catalog.duckdb   参考数据/日K/公告/元数据    │ │
                     │  │ data/**/*.parquet  1m K线（分区、不可变）    │ │
                     │  │ exports/  logs/  tmp/  quantdata.lock       │ │
                     │  └────────────────────────────────────────────┘ │
                     └───────────────────────┬────────────────────────┘
                                             │ SourceAdapter 协议
                     ┌───────────────────────┴────────────────────────┐
                     │ pytdxdata（主：三市场K线/行情/复权/期货/期权）    │
                     │ pywestockdata（辅：A股实时/财务/公告/分红/交叉源） │
                     │ yfinance（美/港参考）  静态节假日表（日历兜底）      │
                     │ akshare 兜底（摘抄源码+primp 请求，不装库）        │
                     │ 新闻采集 = dbpath/scripts/fetch_news.py          │
                     └────────────────────────────────────────────────┘
                                             │
                     ┌───────────────────────▼────────────────────────┐
                     │ 消费端：curl / 任意 HTTP 客户端 / WebSocket 订阅    │
                     │ （脚本、notebook、下游服务皆可直连 serve 端口）      │
                     └────────────────────────────────────────────────┘

**分层原则**：

| 层    | 职责                          | 关键约束                              |
| ---- | --------------------------- | --------------------------------- |
| CLI  | 命令解析、进度展示、本地直查              | 任何命令都可离线跑（`--no-http` 时服务层降级为无） |
| 应用层  | 调度、ETL、质检、状态机、推流            | 全部幂等：重复执行不产生重复数据；DuckDB/脚本走线程池（§7.6） |
| 服务网关 | FastAPI REST + WebSocket 协议实现 | 路径带版本号 `/v1`，WS 帧带 envelope          |
| 存储层  | DuckDB + Parquet + 视图联邦     | 单写者、原子替换、checkpoint 可续            |
| 源适配层 | 统一 `SourceAdapter` 接口对接各数据源 | 限速、重试、录制回放 fixtures 供测试           |

---

## 3. 存储设计

### 3.1 dbpath 目录布局

```
/xxx/dbpath/
├── config.json               # 库配置（init 生成，可改；JSON 格式）
├── catalog.duckdb            # 元数据库：参考数据+日K+公告研报+checkpoint+jobs
├── data/                     # 大数据：Parquet 分区（仅 1m K线）
│   ├── cn/
│   │   ├── stock/symbol=sh600000/year=2026/month=09/20260923.parquet   # 当日段（新写）
│   │   │                    .../month=09.parquet                       # 月度合并（compaction 后）
│   │   ├── index/…  fut/…  cbond/…  etf/…  reits/…  opt/…
│   ├── us/…
│   └── hk/…
├── scripts/                  # 抓取/更新任务脚本（每任务一文件，文件内 META 声明触发时机）
│   ├── update_bars_1d.py     # 示例：日K日频更新
│   ├── update_bars_1m.py     # 示例：1m 盘中增量
│   └── fetch_news.py         # 示例：新闻采集（news-integration 逻辑迁入）
├── exports/                  # export 任务输出（按 job_id 分目录）
├── logs/                     # serve 日志（按天滚动，JSON lines）
├── tmp/                      # 原子写中转（写完 os.replace 到位）
└── quantdata.lock            # 单写者锁（PID + 启动时间）
```

**1m 写入策略（LSM 风格，兼顾"每日增量"与"文件数可控"）**：

1. **盘中/每日增量**：每 symbol 每交易日写一个新段文件 `…/month=MM/YYYYMMDD.parquet` —— 不重写任何旧文件；
2. **月度 compaction**：每月 1 号把上月的日段合并成 `month=MM.parquet`（单 symbol 单月约 5–10k 行，合并成本毫秒级）——多文件切换走两阶段协议（见下）；
3. **年度归并（可选开关）**：把 12 个月文件并成年文件，冷数据文件数从 `symbol×12×年数` 降到 `symbol×年数`。

> 1m 数据只追加新文件、不改旧文件 → 只读并发永远安全。**注意：compaction 是「写1个月段+移走12个日段」的多文件操作，`os.replace` 只保护单文件**（评审 P0），切换走两阶段协议：
> ① 月段在 tmp 合并 → `os.replace` **先入位**（此刻月段与残余日段并存）；
> ② 日段 **rename 进 `.trash/` 子目录**（立即移出 glob 范围）→ **延迟 7 天再 unlink**（已枚举打开的读句柄不致 ENOENT）；
> ③ 查询撞上枚举竞态（TOCTOU IO 错误）→ **自动重试一次**即恢复；compaction 后行数守恒质检照跑（§7.5），另监控「compaction 窗口内查询失败计数」。

### 3.2 为什么是 DuckDB + Parquet，而不是单选一个

| 方案                       | 优点                                                                         | 放弃的原因                                                     |
| ------------------------ | -------------------------------------------------------------------------- | --------------------------------------------------------- |
| 全 DuckDB 单文件             | 最简单、事务完整                                                                   | 1m 全历史约 50 亿行/百 GB 级单文件，备份=整文件复制；"每日追加"要反复重写大文件；一旦损坏爆炸半径大 |
| 全 Parquet + 无引擎          | 零依赖                                                                        | 参考数据的 upsert、公告去重、checkpoint 事务都要自己造                      |
| ClickHouse               | 超强聚合                                                                       | 多一个常驻服务，违背"一个目录自包含"；factor-pipeline 已覆盖 ClickHouse 场景     |
| **DuckDB + Parquet（选定）** | 小表事务 + 大文件追加各取所长；DuckDB 原生 `read_parquet` 联邦查询，视图缝合后 SQL 无感；与 vxdata 技术栈一致 | —                                                         |

### 3.3 联邦视图（SQL 透明层）

```sql
-- catalog.duckdb 内注册的典型视图（Hive 分区自动识别）
CREATE OR REPLACE VIEW cn_stock_1m AS
SELECT symbol, ts, date, open, high, low, close, volume, amount
FROM read_parquet('data/cn/stock/**/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW cn_stock_1d AS   -- 日K直接物理表在 DuckDB 内
SELECT * FROM bars_1d WHERE market = 'cn' AND category = 'stock';

-- 跨层查询示例（query_sql 直接支持）
SELECT d.date, d.close, m.industry_l1
FROM cn_stock_1d d
JOIN securities s USING (symbol)
JOIN sw_industry m USING (symbol)
WHERE d.symbol = 'sh600000' AND d.date >= '2026-01-01';

-- 跨市场统一视图（评审已确认）：带 market 列的 union，偷懒查询用
-- 三支一律显式列名（禁 *）：float 列错位不报错，只可靠列集恒等测试拦（评审 P1）
CREATE OR REPLACE VIEW bars_1m_all AS
SELECT 'cn' AS market, symbol, ts, date, open, high, low, close, volume, amount FROM cn_stock_1m
UNION ALL SELECT 'us', symbol, ts, date, open, high, low, close, volume, amount FROM us_stock_1m
UNION ALL SELECT 'hk', symbol, ts, date, open, high, low, close, volume, amount FROM hk_stock_1m;

-- 1m 复权 = 查询期计算（bars_1m 不存因子，口径见 §4.3）
SELECT b.ts, b.close * d.adj_factor AS close_hfq
FROM cn_stock_1m b JOIN bars_1d d USING (symbol, date)
WHERE b.symbol = 'sh600000';
```

> 1m 视图按市场×品类共约 20 个，另加跨市场 union 视图 `bars_1m_all`（已确认）；DuckDB 对 `symbol/year/month` 分区做谓词下推，单股多年查询只读命中文件。跨市场全扫时建议带 date 过滤，剪枝交给 DuckDB。
> **视图 schema 演进（评审 P1）**：全部联邦视图 DDL 纳入 `schema_migrations` 版本化（改表必同步改视图）；parquet 加列 = 新列只在新段出现、历史段读为 NULL，视图 `COALESCE` 兜底，需要真回填时走一次性 `backfill --rewrite-schema`（按需启用）。
> **union 列对齐**：`bars_1m_all` 三支显式列名（禁 `*`），集成测试断言三市场视图**列集/列序恒等**。

### 3.4 容量估算（init 前提示磁盘预算）

| 数据                  | 估算口径                                               | 行数量级   | 体积（zstd parquet / duckdb） |
| ------------------- | -------------------------------------------------- | ------ | ------------------------- |
| A股 1m               | ~5000 股 × 240 行 × 243 日 × 22 年（2004→今，均摊约 60% 上市率） | ~45 亿行 | **150–200 GB**            |
| 美股 1m               | ~4500 股 × 390 行 × 252 日 × 默认 6 年                   | ~27 亿行 | ~90 GB                    |
| 港股 1m               | ~2600 股 × 330 行 × 245 日 × 默认 6 年                   | ~12 亿行 | ~45 GB                    |
| 三市场日K + 参考数据 + 公告研报 | —                                                  | <1 亿行  | <10 GB                    |
| **合计**              |                                                    |        | **~300–400 GB**（默认口径）     |

- 各市场 `history_from` **已确认：cn=2004、us=2020、hk=2020**（默认 1m 与日K 同起点）；日K 可用 `history_from_bars_1d` **独立放宽**（美/港日K 放到 2010 仅 +数 GB，长周期回测够用），历史随时 `backfill --from` 往前增量补；
- **运行放大系数（评审 P1）**：上表 = 数据终态；另计 DuckDB WAL/日频重写临时空间、compaction 当刻 tmp 2× 峰值、`exports/`（qlib 全量 float32 可达 ~100GB）、备份副本 —— 合计 **+≥200GB buffer**；
- `init --estimate` 只算不写，按 **终态 + buffer ≈ 600GB** 口径报预算；`export.out_dir` 可指到独立盘。

---

## 4. 数据模型

### 4.1 catalog.duckdb 核心表（DDL 摘要）

```sql
-- 参考数据 --------------------------------------------------------------
exchanges        (exchange_code PK, name, market, timezone, session_json)   -- 交易时段规则
securities       (market, symbol, name, category, exchange, list_date, delist_date,
                  currency, lot_size, underlying, strike, maturity, ... , PK(market, symbol))
                  -- category: index/stock/future/cbond/etf/lof/reits/option
calendar         (market, trade_date, is_open, note, PK(market, trade_date))
corp_actions     (market, symbol, ex_date, type, ratio, cash_div, ... , PK(...))   -- 除权除息
sw_industry      (version, symbol, l1, l2, l3, effective_from, effective_to)        -- 申万SCD2：官方发新版/追溯修订→开新version，effective_from=官方生效日
stock_status     (symbol, date, is_st, st_reason, is_suspended,
                  limit_up, limit_down, pct_limit, source, PK(symbol, date))       -- ST/停牌/涨跌停 合表
futures_meta     (symbol, contract, multiplier, expiry, daily_limit_rule, ...)      -- 期货合约要素

-- 行情（日K；1m 在 parquet）------------------------------------------------
bars_1d          (market, category, symbol, date, open, high, low, close,
                  volume, amount, turnover, adj_factor, PK(market, category, symbol, date))

-- 信息流 ------------------------------------------------------------------
announcements    (id PK, symbol, title, category, publish_ts, url, source, hash)
reports          (id PK, symbol, org, analyst, rating, title, publish_ts, url, source)
news             (id PK, ts, source, market, symbols_json, level, title, url, hash)          -- 分钟新闻（NewsBridge 落库；断线补偿游标）

-- 运行时元数据 ------------------------------------------------------------
etl_checkpoint   (dataset, market, scope, watermark, rows, status, updated_at, PK(前三个))
export_jobs      (job_id PK, spec_json, fmt, path, rows, bytes, state, created_at, finished_at)
quality_reports  (date, market, check_name, result, detail_json, PK(date, market, check_name))
schema_migrations(version PK, applied_at)
```

### 4.2 三市场 × 数据范围矩阵（✅ 有 / ⚠️ 有条件 / ❌ 无）

| 数据                              |       A股       |              美股             |         港股        | 主数据源                                      |
| ------------------------------- | :------------: | :-------------------------: | :---------------: | ----------------------------------------- |
| 证券信息（指数/股票/期货/可转债/ETF/REITS/期权） |      ✅ 全品类     | ⚠️ 无美债转、无 LOF、期权/期货看 TDX 覆盖 | ⚠️ 可转债/期权看 TDX 覆盖 | pytdxdata（+westock 补 A股字段）                |
| 交易日历 + 交易时段                     |        ✅       |              ✅              |         ✅         | 静态节假日 + TDX 交易日 + exchange 表              |
| 1分钟K线                           |        ✅       |              ✅              |         ✅         | pytdxdata（seed：stock_prediction 现成 A股 1m） |
| 日K线                             |        ✅       |              ✅              |         ✅         | pytdxdata + pywestockdata 交叉验证            |
| 除权除息                            |        ✅       |         ⚠️ 复权因子走 TDX        |       ⚠️ 同左       | TDX gbbq / westock 分红                     |
| 公告信息                            |        ✅       |          ⚠️ 覆盖面待验证          |       ⚠️ 同左       | pywestockdata（东财）                         |
| 研报信息                            | ✅ akshare 摘抄 |           ⏸ 暂缓（可扩展）          |      ⏸ 暂缓（可扩展）     | akshare 研报接口（choices 等）源码移植 + primp      |
| 申万行业分类                          |     ✅（A股专属）    |              ❌              |         ❌         | akshare 申万行业接口源码移植 + primp                 |
| ST / 停牌 / 涨跌停                   |     ✅ 日状态合表    |          ❌（无 ST 制度）         |       ⚠️ 停牌✅      | westock 状态 + 规则推导（10/20/30% 分板块）          |
| 实时行情                            |     ✅ 秒级快照     |              ✅              |         ✅         | pytdxdata quotes / westock 全市场 <500ms     |
| 市场状态事件                          |        ✅       |              ✅              |         ✅         | StateEngine 本地推导（日历+时钟，确定性）               |
| 分钟新闻                            |        ✅       |        ✅（global feed）       |         ✅         | news-integration → NewsBridge             |


> **品类可扩展机制（已确认）**：⏸ 暂缓品类 = 结构预留、先放空，绝不塞假数据。接入一个新品类只需三步 —— ①`config.json` 的 categories 加一项 → ②写一个 `scripts/` 采集脚本（META 声明）→ ③注册一条 `CREATE VIEW`，核心代码零改动。

### 4.3 统一 K 线 schema

```
symbol: str        # 规范代码：cn 用 sh600000/sz000001/bj430047，us 用 AAPL，hk 用 00700（与 pytdxdata 一致）
ts: datetime64[ns, UTC]   # 1m 用 UTC 毫秒存 + exchange 本地日期做分区键（美/港跨时区不乱）
date: date32       # 交易所本地交易日（分区、日K主键）
open/high/low/close: float64
volume: float64    # A股=股/张，期货=手（在 futures_meta 有乘数）
amount: float64    # 成交额（元）
```

**时间与复权口径（全格式统一约定，评审 P1）**：
- 存储一律 UTC；REST/WS JSON 时间字段一律 **ISO8601 带 `Z`**，状态类 payload 另带 `session_tz`（如 `"Asia/Shanghai"`）标明交易所本地；
- 导出（feather/csv）的 datetime 列 = **tz-naive 交易所本地时间**（backtrader/qlib 惯例）；
- **bars_1m / bars_1d 一律存不复权原始价，复权是查询期计算**；source of truth = `corp_actions`（TDX gbbq / westock 分红只是输入，对账以 corp_actions 重算为准）；1m 取因子 `JOIN bars_1d USING (symbol, date)`（视图示例见 §3.3），export 的 fq 参数在导出期施加；**除权日无需重算历史 bars**（存的就是不复权价）；因子被源站修正 → 质检「复权连续性」告警并提示下游重导出。

---

## 5. 数据源适配层

### 5.1 SourceAdapter 协议

```python
class SourceAdapter(Protocol):
    name: str
    capabilities: set[str]          # {"bars_1d","bars_1m","realtime","announcements",...}

    async def fetch_securities(self, market: str) -> list[Security]: ...
    async def fetch_bars(self, market, category, symbol, freq, start, end) -> list[Bar]: ...
    async def fetch_realtime(self, symbols: list[str]) -> list[Quote]: ...
    async def fetch_calendar(self, market, year: int) -> list[CalendarDay]: ...
    # 其余能力按 capabilities 声明，调度器只调度声明过的能力
```

### 5.2 源分工与交叉验证

| 能力            | 主源                                  | 备源（抽查 1% 对账，不一致记 quality_reports） |
| ------------- | ----------------------------------- | --------------------------------- |
| 三市场 K 线/复权/日历 | pytdxdata                           | pywestockdata（仅 A股日K）             |
| A股实时快照        | pytdxdata quotes                    | pywestockdata（全市场 <500ms，作为快照源更快） |
| A股财务/分红/公告    | pywestockdata                       | —                                 |
| 期货要素/行情       | pytdxdata                           | yfinance（参考对账）                |
| 新闻            | news-integration（RSS 统一采集器）         | —                                 |
| 节假日           | 静态 holidays（market-event-driver 吸收） | TDX 交易日回调校验                       |
| 研报（A股）       | akshare 研报接口（choices 等，摘抄源码+primp）  | —                                 |
| 申万行业          | akshare 申万行业接口（摘抄源码+primp）            | —                                 |
| 长尾兜底          | akshare 源码移植（摘抄实现、primp 发请求，不安装 akshare） | pytdxdata/pywestockdata 优先 |

### 5.3 工程规则

- **限速**：每源独立令牌桶（配置驱动），被限流指数退避重试；
- **断点**：所有 fetch→write 以 `(dataset, market, scope)` 为粒度写 `etl_checkpoint`，重启续跑；
- **可测试**：适配层带录制模式（fixtures 标注录制日期），单测/集成测试离线回放；nightly record job 打真网重录 diff —— 生命周期规则见 §14；
- **连接级容错**：TDX 断连/坏节点的重连与多服务器切换由 pytdxdata **内建**，适配层只管限速与指数退避（故障注入测试见 §14）；
- **代理**：代理服务器在 config.json 配置（全局 + per-source 覆盖），akshare 移植源的请求一律走 primp（与 pywestockdata 同款反爬栈）；
- **合规**：只用公开接口与已有本地数据，遵守源站频率约定；**不安装 akshare 库**，只摘抄其实现逻辑。

---

## 6. CLI 设计

```bash
quantdata init    -d /xxx/dbpath/   [--markets cn,us,hk] [--years-cn 22 --years-us 6 --years-hk 6]
                                     [--seed /path/to/old_1m] [--estimate] [--skip-history]
quantdata serve   -d /xxx/dbpath/   [--host 127.0.0.1] [--port 8765] [--no-http] [--insecure]   # 恒前台常驻，无 daemon 模式
quantdata update  -d /xxx/dbpath/   [--dataset bars_1d] [--date 2026-09-22]   # 单次更新，cron 兜底
quantdata backfill -d /xxx/dbpath/  --dataset bars_1m --market cn [--from 2004] [--resume]
quantdata query   -d /xxx/dbpath/   "SELECT ..." [--fmt table|csv|json|parquet] [-o out]  # 本地直查，不开服务也可用
quantdata export  -d /xxx/dbpath/   --fmt qlib|backtrader|csv|parquet --sql "..." -o out/
quantdata status  -d /xxx/dbpath/   # 水位、锁、HTTP 服务、最近质检、磁盘占用
quantdata doctor  -d /xxx/dbpath/   # 手动触发全量质检 + 修复建议
```

```python
# Python 客户端标准路径（pandas 用户，评审 P1 补齐）
import httpx, base64 as b64, pyarrow as pa
r = httpx.post("http://127.0.0.1:8765/v1/query", json={
    "sql": "SELECT date, close FROM cn_stock_1d WHERE symbol=? ORDER BY date DESC LIMIT 5",
    "params": ["sh600000"], "fmt": "arrow"}).json()
tbl = pa.ipc.open_stream(pa.BufferReader(b64.b64decode(r["b64_arrow"]))).read_all()
df = tbl.to_pandas()          # dtypes 可用 r["dtypes"] 校验

# WS 订阅：连上即收初帧（state + snapshot），断线重连=重新收初帧；新闻补偿走 query 游标
# import websockets / httpx-ws：ws://127.0.0.1:8765/v1/stream?topics=quote.snapshot,state,news.minute&market=cn
```

| 命令                | 行为要点                                                                                                                  |
| ----------------- | --------------------------------------------------------------------------------------------------------------------- |
| `init`            | ①建目录+config.json ②建 DuckDB schema（schema_migrations 管版本）③拉参考数据 ④回填历史（有 `--seed` 先导入再增量补齐）⑤自检输出报告。**幂等**：重跑=续跑，不破坏已有数据 |
| `serve`           | 拿 `quantdata.lock`（拿不到→报已有实例 PID 后退出）→ 起调度器 → 起 FastAPI 网关（uvicorn 挂同一事件循环；**bind 失败=打印占用者 PID 后非零退出**，交 supervisord 退避重试）→ 前台常驻（Linux 交 supervisord，恒前台） |
| `query`/`export`  | 本地直连只读 DuckDB，不开服务也能用；与 serve 并存靠"单写多读"（§11）                                                                          |
| `status`/`doctor` | 运维入口，输出全部是结构化 JSON（`--json`）便于脚本化                                                                                     |

全局约定：`-d` 可用环境变量 `QUANTDATA_HOME` 兜底；所有命令支持 `--json` 输出；进度条用 rich。

---

## 7. serve 常驻设计

### 7.1 组件与生命周期

```
serve 启动
  ├─ 1. 获取写锁 quantdata.lock（失败即退出；判活 = PID 存活且 start time/boot_id 可读并一致，任一取不到→陈旧锁接管，见 §11）
  ├─ 2. 打开 catalog.duckdb（读写）+ 校验 schema 版本（自动跑 migration；schema 版本 > 代码已知最高版 → 报错退出）
  ├─ 3. 启动 Scheduler（asyncio 单进程多任务；DuckDB 与 scripts 一律走线程池，见 §7.6）
  ├─ 4. 起服务网关：--no-http 跳过；否则 Server.serve() 挂同一事件循环起 uvicorn（默认 127.0.0.1:8765）
  │      启动校验：监听地址非 loopback 且 http.token 为空 → 拒绝启动（除非显式 --insecure，打醒目告警）
  │      bind 失败（端口占用）→ 打印占用者 PID 后非零退出
  ├─ 5. 循环运行直到 SIGTERM：优雅关闭 = 停接新任务 → 等当前任务落 checkpoint（总预算 30s，超时主动释放锁再退出）
  │      → 关 HTTP（断开 WS）→ 释放锁；supervisord 侧 stopwaitsecs=60 兜底
  └─ 任意组件崩溃 ≠ 进程退出：内部 supervisor 拉起 + WS 广播 ev.update fail + 日志告警；
     同一组件连续 5 次拉起失败 → 进程自杀非零退出（把故障暴露给外部监控，不许闷声吞）
```

### 7.2 调度（显式绑定 Asia/Shanghai，不读系统时区）

> **任务脚本化**：每个抓取/更新任务是 `dbpath/scripts/` 下的独立 Python 文件（一个任务一个脚本），文件内声明 `META = {"cron": "...", "dataset": "...", "market": "..."}`；Scheduler 启动时扫描 scripts/ 并按 META 触发，支持热加载（改脚本不用重启 serve）。用户可自行增删脚本扩展任务，无需改核心代码。config.json 的 `schedule` 段配置默认抓取时间，脚本 META 可覆盖。
>
> **时区与时钟（评审 P1）**：调度时区显式绑定 `schedule.timezone`（默认 `Asia/Shanghai`，**不读系统 TZ** —— 容器默认 UTC 也不会让日频链移位）；每个 cron 槽记录最近触发时间戳，时钟回拨后跳过重复槽并在 `ev.update` 告警（幂等 §7.3 兜数据正确性，这里兜「跑没跑」的可观测）。

| 任务                  | 触发                                                     | 内容                                                                          |
| ------------------- | ------------------------------------------------------ | --------------------------------------------------------------------------- |
| 盘中 1m 增量            | 各市场交易时段内每 60–120s                                      | 拉当段 1m → 写日段文件 → 更新 checkpoint                                              |
| 快照推流 QuotePoller    | 交易时段每 3s（cn 全市场；us/hk 按 watchlist+指数）                  | 归一化 → WS `topic=quote.snapshot`（market 字段区分）；watchlist 另发 `topic=quote.tick` |
| 状态机 StateEngine     | 每秒 tick（按 exchange.session_json 确定性推导）                 | 状态迁移时向 WS 推一次 `topic=state`（不刷屏；重连对齐时序统一见 §8.2 客户端模式） |
| 收盘日频链（A股）           | 15:05 起：日K定稿 → 复权因子 → ST/停牌/涨跌停 → 证券主数据 → 行业 → 公告/研报增量 | 每步写 checkpoint，失败从该步续                                                       |
| 收盘日频链（港股）           | 16:10 起同上                                              |                                                                             |
| 收盘日频链（美股）           | 次日 04:30（16:00 ET = 04:00/05:00 CST 次日，含夏令时表）          |                                                                             |
| 质量校验 QualityChecker | 每日各市场收盘链完成后                                            | 见 §7.5，产出 quality_reports + WS `topic=ev.quality` 事件 |
| 月度 compaction       | 每月 1 号 07:00                                           | 合并上月 1m 日段（§3.1）                                                            |
| 新闻 NewsBridge       | 常驻 collector + 每分钟窗口                                   | 见 §10.3                                                                     |

### 7.3 日频任务链的幂等模式

每个任务定义为 `step(dataset, market, date)`：执行 = 以 `(market, date)` 为粒度 **整分区替换**（DuckDB DELETE+INSERT 同事务 / Parquet 重写 tmp 后 os.replace）。重跑任意次数结果一致 → 调度重试、手工 `update --date` 重放都安全。

**盘中增量是另一条路径（评审 P1）**：交易时段内每 60–120s 只 `os.replace` **当日日段文件**，不碰历史分区（天然幂等）；「整分区 DELETE+重写」仅用于日频链与手工重放。各 symbol 当日段替换非原子 —— 读端短暂见「部分股新、部分股旧」可接受（旧段 = 略早快照、单调变新），跨股严格同刻不在 v1 承诺内。

### 7.4 断点与水位

`etl_checkpoint.watermark` 语义：`bars_1m` 存"最后完整交易日"（按日为粒度，不做分钟级水位——K线按日整段写，日粒度幂等最简单）；公告/研报存 `publish_ts` 游标 + **content hash**：主键保持 `id`，改稿走 `INSERT ... ON CONFLICT(id) DO UPDATE WHERE excluded.hash <> hash`（同 id 改稿可进库）；hash 只做内容比对，不与主键语义混用。

### 7.5 每日质检清单（假设 = 需求中截断的"每日对数据进行…"）

| 检查       | 逻辑                                            | 失败动作                                |
| -------- | --------------------------------------------- | ----------------------------------- |
| 交易日 gap  | 日历声明开盘但该日 0 行                                 | `ev.quality` critical + 自动重拉一次      |
| OHLC 合理性 | H≥max(O,C)、L≤min(O,C)、L≥0、volume≥0            | 隔离异常行到 quarantine 表，记 detail        |
| 涨跌停越界    | close 相对昨收超出 stock_status 声明的 limit（新股/除权日豁免） | warning + 抽样人工                      |
| 复权连续性    | adj_factor 单调性/跳变与 corp_actions 对账            | critical（错复权是最隐蔽的脏数据）               |
| 行数环比     | 当日行数 vs 前 5 日中位数偏离 >30%                       | warning                             |
| 双源抽查     | 随机 1% symbol 的日K收盘价 tdx vs westock            | 不一致记 report，连续 3 天同标的不一致升级 critical |
| 文件完整性    | compaction 后行数守恒、无孤立 tmp                      | 自动修复（重合并）                           |

### 7.6 执行模型（事件循环隔离 —— 评审 P0）

单进程里跑着调度器 + FastAPI + 用户脚本，**事件循环是稀缺资源**，硬约束三条：

1. **DuckDB 调用一律出循环**：query/export/ETL 的 DuckDB 执行全部走专用线程池（`asyncio.to_thread` / 独立 executor），事件循环内只做编排与分发；查询超时 = 线程内 `connection.interrupt()` 真中断（asyncio `wait_for` 只放弃等待，同步调用仍会烧 CPU —— 由此解决）；
2. **scripts 任务宿主 = 线程池**：`scripts/*.py` 是热加载的用户外部代码，同步阻塞不可接受 —— Scheduler 一律丢线程池执行，绝不作 loop 内协程直跑；
3. **uvicorn 挂同一循环**：用 `Server(config).serve()` 协程方式挂入调度器所在 event loop（不用 `uvicorn.run()` 自建循环），WS 推送、调度 tick、状态机共用一个 loop 且全部非阻塞。

---

## 8. 对外服务协议（FastAPI：REST + WebSocket）

### 8.1 服务形态（serve 进程内，零外部依赖）

| 配置 / 参数 | 默认 | 行为 |
| ---- | ---- | ---- |
| `--host` / `http.host` | `127.0.0.1` | 监听地址：默认仅本机（安全默认）；`0.0.0.0` 开放内网访问 |
| `--port` / `http.port` | `8765` | 监听端口 |
| `http.enabled`（CLI 开关 `--no-http`） | `true` | 两语义拆清：配置默认 true（起网关）；`--no-http` 的作用 = 把 enabled 置 false（flag 本身默认不触发）。false = 纯 CLI 批处理，不起网关（本地 query/export 不受影响） |
| `http.token` | 空 = 不校验 | 非空时 REST 带 `Authorization: Bearer <token>`；WS **优先握手 Header 同款**，`?token=` 仅兼容降级（会进日志）；**非 loopback 且 token 空 → 拒绝启动**（除非 `--insecure`，见 §8.4） |

> **实现分工（相对原 NATS 方案的变化）**：
> - FastAPI + uvicorn 是 pyquantdata 的**普通 pip 依赖**，随包安装 —— 无二进制、无子进程、无外部服务，`bin/` 目录整个取消；
> - REST 与 WebSocket 挂在同一个 app 上，共享依赖注入（只读 DuckDB 连接池、进程内事件总线）；
> - Swagger UI 自带 `/docs`、OpenAPI 在 `/openapi.json` —— 外部接入零文档成本（NATS 方案要自己维护 subject 文档）；
> - **订阅语义**：WS 连接即订阅（`topics` 过滤）；推送发生在**同进程内** —— Scheduler/QuotePoller 产出事件 → asyncio 广播到各 WS 连接，不经任何外部 broker；
> - `dbpath/scripts/` 只放抓取/更新任务脚本，与服务网关无关；
> - **背压与扇出（评审 P1）**：每 WS 连接一条**有界队列（`ws_queue_len`=256 帧）** —— 快照类溢出丢最旧保最新、事件类超限直接断开该连接；全局连接数上限 `max_ws_conns`=64；全市场快照帧**序列化一次、N 连接共享同一 bytes**（禁止 per-connection 重复 `json.dumps`）；队列深度纳入 §13 指标告警。

### 8.2 REST 端点 + WebSocket Topic 总表（前缀 `/v1`）

**REST（请求-响应）**

| 端点 | 方法 | 用途 |
| ---- | ---- | ---- |
| `/v1/query` | POST | **query_sql**：body `{sql, params, fmt: arrow\|json, max_rows, timeout_ms}` → `{columns, dtypes, b64_arrow 或 rows, row_count, elapsed_ms, truncated}`；**fmt=arrow = Arrow IPC streaming format**（JSON 内 base64，解码路径见附录 A） |
| `/v1/export` | POST | **export**：body `{fmt, sql\|dataset spec, opts, sync?}` → 小导出（<100MB 或 `sync=true`）**直接回文件流**；大导出 → `{job_id, state: queued\|running\|done\|failed, path, rows, bytes}` |
| `/v1/export/{job_id}` | GET | 导出任务状态/结果；下载 `GET /v1/export/{job_id}/file`（长任务进度同时走 WS `ev.export`） |
| `/v1/meta` | GET | 字典查询：日历、证券列表、view 列结构（**每 view 附 row_count、min/max date、最新分区、3 个样例 symbol**）、库版本 |
| `/v1/stats` | GET | 水位、磁盘、任务队列、质检摘要（status 命令的远程版） |
| `/v1/state/{market}` | GET | 当前市场状态（**REST-only 客户端**的对齐入口；WS 客户端收初帧即可） |
| `/healthz` | GET | Liveness：进程活着即 200（supervisord / 容器探针） |
| `/v1/ready` | GET | Readiness：写锁 / schema 版本 / 调度器 / 磁盘余量，异常 503 |

**WebSocket `/v1/stream?topics=…&market=cn&symbols=sh600000`**

| Topic | 节奏 | 用途 |
| ---- | ---- | ---- |
| `quote.snapshot` | 每 3s | 全市场快照批 `{ts, n, rows:[[sym,price,pct,vol,…列序号表]]}`（列字典在 `/v1/meta`，批消息不重复带列名） |
| `quote.tick` | 1s | 自选股逐笔/高频（`symbols=` 参数指定，服务端 watchlist 同步维护） |
| `state` | 迁移时 | 市场状态迁移 `{state, ts, next_at, reason}`；状态集见 §10.2 |
| `news.minute` | 每 60s | 分钟新闻批 `{window_start, items:[{id, ts, source, market, symbols, level, title, url}], count}` |
| `flow.announce` / `flow.report` | 增量 | 公告/研报增量（结构同新闻单条） |
| `ev.update` | 事件 | ETL 进度/完成/失败 `{dataset, step, date, rows, state}` |
| `ev.quality` | 每日 | 质检结果 `{date, market, pass, fails:[…]}` |
| `ev.export` | 事件 | 导出任务进度 `{job_id, state, rows, bytes}` |

**Envelope 统一首部（WS 帧）**：`{v:1, msg_id, ts, producer:"quantdata/0.1", topic, payload…}` —— 与日志事件同构；REST 响应不套 envelope，直接回 payload。**ts 一律 ISO8601 UTC（带 Z）**，状态类 payload 另带 `session_tz`。

> **客户端模式（统一时序 —— 全文唯一定义，其它章节引用本段）**：
> ① WS 一连上，服务端**必发初帧** = 当前 `state` + `quote.snapshot`（REST-only 客户端才用 `GET /v1/state/{market}` 对齐）→ 之后才是增量；
> ② 断线客户端自动重连，**重连即重新收初帧**，无需手动 GET；
> ③ **新闻/公告类 topic 断线期间不重放** —— 补偿走 `/v1/query` 按 ts 游标（`SELECT * FROM news WHERE ts > ? ORDER BY ts`）。重连三步 = 收初帧 → 按需 query 补偿 → 收流。

### 8.3 query_sql 安全约束（对外开 SQL 必须有闸）

1. **只读 + 断网**：DuckDB `read_only=true`（serve 写连接与查询连接分离）；`SET autoinstall_known_extensions=false, autoload_known_extensions=false`（堵 httpfs 自动加载出站）；
2. **语句白名单（sqlglot）**：**fail-closed**（解析失败一律拒绝）、显式 `dialect="duckdb"`、按 **AST 根节点**判定（非字符串前缀）、`parse_all` 长度必须 =1；拒绝 ATTACH/COPY/PRAGMA/多语句/DDL/DML；sqlglot 进 lock **pin 版本**，升级须过 SQL 闸回归测试（绕过样例：分号多语句、CTE 套 INSERT、EXPLAIN 变体、注释混淆 —— 全部入单测）；
3. **函数/表函数白名单 —— 默认拒绝、显式放行（评审 P0）**：默认只放行纯计算函数 + 库内已注册视图；**显式拒绝 `read_csv` / `read_parquet` / `*_scan` / `glob` 等文件与网络表函数**（否则 `SELECT read_csv('/etc/passwd')` = 任意文件读、`read_csv('http://169.254.169.254/…')` = SSRF）；`sql.allow_functions` 语义 = **额外放行清单**（不是「清空即全放行」的总开关）；
4. **结果上限**：默认 `max_rows=1,000,000`、Arrow 结果 ≤64MB，超限报错并提示改走 `export`；
5. **超时与并发闸**：超时默认 30s，查询在**线程池**执行、到点 `connection.interrupt()` 真中断（§7.6）；全局查询信号量默认 4（超出排队，等 >5s 返 429），查询连接设 `memory_limit` —— 查询风暴不许饿死调度器；
6. 防御参数：`SET max_expression_depth` 等预设。

### 8.4 认证

- 默认只监听 `127.0.0.1`：本机可信，不设认证；
- **启动强制校验（fail-fast）**：监听地址非 `127.0.0.1/::1` 且 `http.token` 为空 → **拒绝启动**；确要裸奔须显式 `--insecure`，打醒目告警日志 —— 文字约定不算数（评审 P1）；
- **凭证传递**：REST 用 `Authorization: Bearer`；WS 优先握手 Header 同款，`?token=` 仅兼容降级；**日志对 token 统一脱敏**（只留前 4 位，uvicorn access log 同样处理，入测试断言）；
- WS 与 REST **共用同一鉴权中间件**（含 `ev.*` topic，不给 WS 开旁路）；
- v1 不做多租户与细粒度权限（见 §1.2 非目标）。
---

## 9. 导出格式设计

| 格式           | 产物                                                                                                                                      | 细节                                                                                                                                                                                |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `csv`        | `exports/{job}/XXX.csv`                                                                                                                 | 按 symbol 分文件或单文件可选；ISO 日期、UTF-8                                                                                                                                                   |
| `parquet`    | 单文件或按 symbol 分区目录                                                                                                                       | 默认输出，qlib/backtrader 之外的一切下游首选                                                                                                                                                    |
| `pandas`     | **不属于 export** —— 归 `/v1/query?fmt=arrow` 消费端（b64_arrow → Arrow → DataFrame，10 行示例见附录 A）                              | 本地 CLI `query --fmt pandas` 同理；**export 只管落盘格式**（csv/parquet/qlib/backtrader）                                                                                                |
| `polars`     | 同 pandas（Arrow streaming 直吃）                                                                                                         | 列类型映射在 meta 公布                                                                                                                                                                      |
| `backtrader` | `exports/{job}/feather/{symbol}.feather`（+ 可选 csv）+ `loader.py`                                                                         | 用 `bt.feathers.GenericPandasData` 可直接吃的列（datetime/open/…/volume/openinterest，volume 补 0）；loader 提供 `load_feeds(spec) -> list[bt.feeds]`；复权可选（fq=qfq/hfq/none）；1m feed 显式 `timeframe=Minutes`、`fill_missing`，datetime 列 tz-naive 交易所本地（§4.3 约定） |
| `qlib`       | 标准 qlib 数据目录：`calendars/`（day.bin, minute.bin, float32）、`instruments/*.txt`、`features/{market}/{symbol}/{freq}/{field}.bin`（float32 序列） | **实现方式=自写 dump_bin 兼容写入器（numpy tofile），运行时不依赖安装 qlib**；测试两层（评审 P0）：①核心 = 按 dump_bin 布局**字节级/数值级读回断言**（CI 必绿、不装 qlib）；②`qlib` extra **可选集成 job**（装 qlib 真跑 `qlib.init()`，单独标记不阻塞主链）                                               |

**Export 任务流**：POST /v1/export 进来 → 校验 spec → 写 `export_jobs` → **worker = 独立线程池（默认 2 并发 + 排队上限 8；只读连接从池独占获取，让路日频链）** → 进度走 WS `topic=ev.export` + `GET /v1/export/{job_id}` 轮询 → 完成回 path；**小导出（<100MB 或 sync=true）直接回文件流不建 job**。job 结果按 spec hash 缓存 24h，重复导出直接回缓存路径。

**路径安全（评审 P1）**：输出根**恒为 `exports/{job_id}/`**（job_id 服务端生成）；请求带来的名称字段（symbol/目录段）白名单字符过滤 + `resolve()` 后必须仍在 exports/ 前缀内；`opts` 为显式字段枚举，**禁止自由 path 键** —— 杜绝 `../` 穿出 dbpath 再经 `/file` 回读。

---

## 10. 实时与推流设计

### 10.1 行情 QuotePoller

- **源**：cn 全市场快照 pywestockdata（<500ms 拉全量）或 pytdxdata quotes；us/hk 走 pytdxdata；
- **节奏**：快照 3s 一批；watchlist（WS 连接参数 `symbols=` 指定，默认指数+自选）1s tick；
- **归一化**：各源字段映射到统一列字典（§8.2），消息体只发列序号数组省带宽；
- **状态**：非交易时段自动停poll（状态机驱动），WS 断线由客户端自动重连，**重连即收服务端初帧（state + snapshot）**——统一时序见 §8.2 客户端模式。

### 10.2 市场状态机 StateEngine

```
状态集（每市场一套）：
CLOSED → PRE_OPEN_AUCTION → OPEN → LUNCH_BREAK → OPEN → CLOSED
         （A股 09:15-09:25 集合竞价；港股 09:00-09:30；美股无午休、有盘后 AH 可扩展）
驱动：exchange.session_json（时段规则） + calendar（节假日） + 本地时钟 —— 纯本地确定性推导，
      不依赖任何上游"开市通知"，断网也准；夏令时用 zoneinfo 按年表算。
迁移才推消息；WS 客户端收初帧即得当前态，REST-only 客户端用 `GET /v1/state/{market}`（时序统一见 §8.2）。
```

### 10.3 分钟新闻 NewsBridge

```
news-integration collectors（RSS 等）→ 归一化（统一 NewsItem schema + hash 去重）
  → 60s 滚动窗口攒批 → WS `topic=news.minute`（一帧一批，不逐条发）
  → 同步落库 news 表（§4.1 DDL；断线补偿 = /v1/query 按 ts 游标补齐）
过滤：level（important/normal）、market 关联（关键词→symbol 映射表）、黑名单源。
```

---

## 11. 并发、锁与一致性

| 问题               | 方案                                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------------- |
| 双实例写坏库           | `quantdata.lock`（**PID + 进程 start time + boot_id 三元判活**，防 PID 复用误判；发现陈旧锁先备份再接管并记日志）**。后两项是「补强证据」而非判活前提 —— 任一取不到（权限 deny / psutil 缺失 / 非同一 boot）即视为不匹配、按时旧锁接管；判活 = PID 存活 且 三元全部可读且一致（DP-1 v0.7）**；serve/update/backfill/init 互斥，query/export 只读无需锁 |
| serve 写、CLI 查询并存 | DuckDB 单写多读跨进程成立；查询连接固定 `read_only=true`；Parquet 段文件不可变 + compaction 按 §3.1 **两阶段 .trash 协议**（月段先入位、日段延迟 unlink、查询竞态重试一次）消除丢段/重复窗口 |
| Windows 文件语义     | 一律 `os.replace`（同卷原子）；tmp 与 data 同盘；启动时扫残留 tmp 清理                                                             |
| 重复更新产生重复行        | 日粒度整分区替换（§7.3）；1m 段文件按日期命名天然幂等（重写=同名覆盖）                                                                       |
| 公告/研报重复抓取        | 主键 `id` + content hash：`ON CONFLICT(id) DO UPDATE WHERE excluded.hash <> hash`（改稿可进库，见 §7.4）                |
| schema 演进        | `schema_migrations` 版本表，serve 启动自动升级（**版本 > 代码已知最高版 → 报错退出**，不带病跑；migration 包事务、失败回滚）；**视图 DDL 同步纳入 migration（§3.3）**；升级流程 = 停 serve → 备份 catalog.duckdb → 换包 → 启动 |
| 回填与 serve 抢锁     | 同一把锁：backfill 中途起 serve 会明确报错，不静默并发写                                                                          |

---

## 12. 配置文件（dbpath/config.json）

> config.json 统一承载三类配置：**抓取数据时间**（`schedule` 默认值，可被 `scripts/` 各脚本 META 覆盖）、**代理服务器**（`proxy` 全局 + per-source）、**数据源要求**（`sources` 主备源、`fetch` 限速/超时/重试）。

```json
{
  "version": 1,
  "dbpath_created": "2026-09-23T10:40:00+08:00",
  "markets": {
    "cn": { "enabled": true, "history_from": "2004-01-01", "history_from_bars_1d": null },
    "us": { "enabled": true, "history_from": "2020-01-01", "history_from_bars_1d": null },
    "hk": { "enabled": true, "history_from": "2020-01-01", "history_from_bars_1d": null }
  },
  "categories": { "cn": ["index","stock","future","cbond","etf","reits","option"],
                  "us": ["index","stock","future","etf","reits"],
                  "hk": ["index","stock","future","etf","reits"] },
  "sources": {
    "bars": "tdx", "realtime": "westock", "reference": ["tdx","westock"],
    "futures": ["tdx","yfinance"], "news": ["scripts/fetch_news.py"],
    "fallback": ["akshare_port"],
    "cross_check": true, "cross_check_ratio": 0.01
  },
  "proxy": { "global": "", "per_source": { "akshare_port": "http://127.0.0.1:7890", "yfinance": "" } },
  "fetch": { "rate_limit_rps": { "akshare_port": 3, "yfinance": 2 }, "timeout_ms": 10000, "max_retry": 3 },
  "http": {
    "enabled": true,
    "host": "127.0.0.1",
    "port": 8765,
    "token": "",
    "max_ws_conns": 64,
    "ws_queue_len": 256
  },
  "schedule": {
    "timezone": "Asia/Shanghai",
    "cn_intraday_interval_s": 90, "snapshot_interval_s": 3,
    "daily_chain": { "cn": "15:05", "hk": "16:10", "us": "04:30" },
    "compaction": "monthly"
  },
  "quality": { "enabled": true, "checks": ["gap","ohlc","limit","adj","rowcount","dualsource"] },
  "sql": { "max_rows": 1000000, "timeout_ms": 30000, "max_arrow_bytes": 67108864, "max_concurrent": 4, "allow_functions": [] },
  "export": { "out_dir": "exports", "cache_ttl_h": 24, "keep_days": 7, "formats": ["csv","parquet","qlib","backtrader"] },
  "log": { "level": "INFO", "keep_days": 30 }
}
```


> `http.token` 支持 `${ENV}` 展开，不明文入库亦可；格式保持 JSON（你的约定）。
> `sql.allow_functions` = **额外放行清单**（默认拒绝策略见 §8.3，清空 ≠ 放开表函数）；`export.keep_days`：exports/ 按 job 保留天数，日频链自动清理；`markets.*.history_from_bars_1d` 为 null 时跟随 `history_from`（日K 可独立放宽，见 §3.4）。

---

## 13. 可观测性与运维

- **日志**：`logs/serve-YYYYMMDD.jsonl`（事件字段与 WS envelope 同构）；**token 脱敏**（query 参数只留前 4 位，含 uvicorn access log）；
- **指标**：进程内计数器（更新行数、延迟、HTTP 往返、WS 连接数、队列深度、**磁盘余量**），经 `GET /v1/stats` 查询（v1 不引 Prometheus，接口留好）；磁盘余量 <10% → `ev.quality` warning，**<5% 硬阈值拒写**（任务失败退避、不 crash loop）；另给 `/healthz`（liveness）与 `/v1/ready`（readiness，锁/schema/调度器/磁盘）；
- **质检即看板**：`status --json` / `quality_reports` 表 = 最近一次体检；
- **备份**：DuckDB 必须走**只读连接**（`ATTACH ... READ_ONLY` / `EXPORT DATABASE` 出快照）—— 热拷文件 = 损坏快照；Parquet 段不可变先 rsync；**备份窗口内暂停 compaction**（或备份后跑行数守恒校验 §7.5），防 rsync×os.replace 竞态拷到半套月段；v1 只给文档不给命令；
- **部署**（已确认 = Linux 服务器长期常驻）：supervisord 加一个 program，常驻 `quantdata serve`（恒前台）+ **stopwaitsecs=60**（优雅退出预算 30s 的兜底）；探针挂 `/healthz` + `/v1/ready`；`exports/` 清理由 `export.keep_days` 日频链执行；Windows 仅开发/临时使用（计划任务可选）。

---

## 14. 测试策略（对齐 100% 覆盖率要求）

| 层 | 手段 |
| --------------------------- | --------------------------------------------------------------------------------------- |
| 纯逻辑（状态机、调度、幂等分区、质检规则、SQL 闸） | pytest 单测，分支全覆盖 —— 全部设计成**无网络纯函数/可注入时钟**，是拿满覆盖率的关键 |
| 源适配层 | 录制 fixtures 回放（contract test）不打真网；**另加故障注入**：mock transport 编排错误序列（429→500→成功），fake clock 断言退避时序与重试上限，断言 checkpoint 半途失败后 resume 不重不漏；令牌桶用注入时钟做纯逻辑单测 |
| 存储层 | tmp_path 建迷你库：init→update→compact→query 全链路集成测试（含 compaction 两阶段协议的竞态注入） |
| 服务网关协议 | FastAPI TestClient/httpx：REST 往返、WS 推送、envelope 版本、token 鉴权（**含日志脱敏断言**）、断线重连初帧时序 |
| qlib/backtrader 导出 | **两层（评审 P0）**：①核心 = dump_bin 布局字节级/数值级读回断言（**不装 qlib**，CI 必绿）+ feather 喂 backtrader 跑一根 K 线；②`qlib` extra 可选集成 job（装 qlib 真 `qlib.init()`，单独标记） |
| 质检元测试 | **golden 脏数据样本集**：每类规则 ≥1 阳性 + 1 阴性 + 1 边界豁免（除权日涨跌停），断言 quality_reports 输出；「失败动作」回路（自动重拉/重合并）写回路测试；双源抽查固定随机种子保 CI 可复现 |
| CI | GitHub Actions（同 pytdxdata 模板）：Linux+Windows 矩阵，**`--cov-fail-under=100` + 排除白名单（下表）**；`--no-http` 路径必绿，HTTP/WS 用 TestClient；**`[CI]` 验收链用 CLI 起真 serve + 真端口 curl（覆盖 `cli.serve` → `run_serve` → `Server.serve()` 编排层），`runtime.py` 真 uvicorn/socket 段按白名单豁免（DP-2 v0.7）**；fastapi/uvicorn/sqlglot 全部 lock pin + Renovate 跟进，sqlglot 升级过 SQL 闸回归 |

**async 测试策略（评审 P1）**：
- 调度器抽象 `Clock/ticker` 接口，**cron 判定写成纯函数** —— 参数化构造时间点测全部边界（夏令时切换日、时钟回拨跳槽）；
- pytest-asyncio 显式 mode；集成测试中调度器与 FastAPI app **共用同一事件循环**（与 §7.6 执行模型一致），用可控 ticker 驱动「到点 → 写 checkpoint → WS 广播 ev.update」确定性链路；
- 线程池路径用同步 mock 断言「事件循环内零阻塞调用」。

**验收数据两层（评审 P0 —— CI 跑不了真回填）**：
- **[CI 层]**：`init --skip-history` + 随仓库迷你 seed fixture（`tests/fixtures/ci_seed_1d.parquet`：2 symbols × 10 交易日日K，落到 `bars_1d`；生成脚本 `build_ci_fixture.py` 固化，CI 先 `--check` 校验一致性）→ M1 链 `init → serve → POST /v1/query → export csv` CI 必绿；
- **[夜间/发布层]**：真数据回填与全量对齐，独立 workflow，不计入覆盖率、不阻塞 PR。

**fixture 生命周期（评审 P1）**：每个 fixture 标注录制日期与源接口版本；nightly `record` job 打真网重录 + diff 报告（失败开 issue、不入必绿项）；超 90 天未刷新在 CI 给 warning。

**覆盖率排除白名单（评审 P0 —— 验收前一次拍板）**：

| 排除项 | 理由 | 替代验证 |
| ---- | ---- | ---- |
| uvicorn 真起服 / socket 层 | 平台相关、CI 不稳（本机踩过 socketpair 死锁） | **每周 smoke workflow + M1 交付自检**：真 `serve` + 真端口 curl `/healthz`、`/v1/ready`、`POST /v1/query`、`/v1/export`、SQL 闸 400。CI 验收链的用户面同上（用 CLI 起真 serve + curl），仅 `runtime.py` 内 uvicorn/socket 段 pragma 豁免（DP-2 v0.7） |
| WS 多连接扇出 / 3s 节奏 | TestClient 只测单连接帧语义 | 冒烟脚本 4 连接收帧计数 |
| supervisord 模板 / Windows 计划任务 | 非 Python 代码 | 模板渲染单测 + 手工部署检查表（§15 [手工]） |
| 300GB 真数据导入路径 | 数据量不可进 CI | [夜间/发布层] workflow |
| 用户自定义 `scripts/*.py` 本体 | 运行时热加载的外部代码 | 核心只测加载器与 META 解析 |

---

## 15. 分阶段路线图

> **实现状态（2026-09-24 更新）**：**M1 已交付并发布 PyPI `pyquantdata==0.1.0`**（GitHub Actions release.yml 自动构建 + OIDC Trusted Publishing）。落地对照：CLI 六命令（init/serve/update/query/export/status，比原计划多出 `export` 与 `status` 独立命令）、14 业务表 + 8 联邦视图、REST **8 端点**（原计划的 query/export 外，meta/stats/state/ready/export-job 均已实现）、Bearer 鉴权、SQL 闸本地/HTTP 同规则、254 测试 100% 覆盖率、CI 双平台真起服验收链。M1 未含（对应下方 M2–M4）：backfill/doctor 命令、1m 数据、美/港实际写入、WS 推流。功能清单以 [README](https://github.com/openbot-coder/pyquantdata/blob/main/README.md) 为准。

| 里程碑           | 交付物                                                                                                                    | 验收标准                                                                  |
| ------------- | ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| **M1 最小闭环 ✅ 已交付（v0.1.0）** | CLI 骨架（init/serve/query/update/status）、目录规范、schema+migrations、A股证券信息+日历+日K、FastAPI + `POST /v1/query`、csv/parquet/pandas 导出 | **[CI]** `init --skip-history`+迷你 seed → CLI 起真 `serve` → 真端口 `POST /v1/query` 查到日K → export csv 一条链必绿（含 SQL 闸 400 fail-closed 断言）；单测覆盖核心逻辑 + `--cov-fail-under=100` —— **达成：254 passed / 100% / CI 双平台绿** |
| **M2 A股完整**   | 1m 回填（seed 导入 + 断点续传）、compaction、除权/ST/停牌/涨跌停/申万行业、公告研报、每日质检、qlib+backtrader 导出、市场状态机                                  | A股数据矩阵全 ✅ **[夜间]**；qlib extra job 实读 + backtrader 单测 **[CI]**；质检连续 7 天绿 **[手工]**                          |
| **M3 三市场+推流** | 美/港股日线+1m（时区调度）、快照/tick WS 推流、分钟新闻桥、token 鉴权开放内网、export 远端下载 | 三市场矩阵定稿；三市场 1m 回填完成度 ≥99% 交易日 **[夜间]**；从另一台机器完成 query/订阅/导出全流程演练 **[手工]** |
| **M4 运维加固**   | 陈旧锁恢复、stats 完整化、备份文档、Linux supervisord 部署物（主）+ Windows 本机跑法（次）、README/协议文档、双源对账常态化、**quantdata-client 薄 SDK + `GET /v1/bars` 糖（可选）** | 连续运行 2 周无人工干预 **[手工]**；`doctor` 全绿；覆盖率白名单复审                                             |

> 先 M1 跑通再谈后面的：每个里程碑都保持"能用的完整切片"，不铺一次性大坑。
> **验收标签**：`[CI]` = GitHub Actions 必绿、计入 100% 覆盖率；`[夜间]` = 独立 workflow 不阻塞 PR；`[手工]` = 发布检查表签收项。100% 覆盖率只约束 `[CI]` 项（白名单见 §14）。

---

## 16. 评审决策记录（2026-09-23 已拍板）

| # | 问题 | 决策 | 落点 |
|---|------|------|------|
| 1 | "每日对数据进行…"截断 | ✅ 每日质量校验（gap/OHLC/涨跌停/复权对账/双源抽查 1%） | §1.3、§7.5 |
| 2 | 起始年限 / 磁盘 | ✅ cn=2004、us=2020、hk=2020；dbpath 落位由部署时 `-d` 指定（容量 §3.4，建议大盘） | §1.3、§3.4 |
| 3 | 美股可转债/LOF 等品类 | ✅ 先放空，走可扩展结构（config+脚本+视图三步接入，核心零改动） | §1.3、§4.2 |
| 4 | 研报 / 申万行业数据源 | ✅ akshare 接口（研报 choices 等），摘抄源码 + primp 重写（不安装 akshare） | §1.3、§4.2、§5.2 |
| 5 | 实时行情精度 | ✅ 3 秒快照 + 自选 tick，不做逐笔 | §1.3、§10.1 |
| 6 | 对外服务协议 | ✅ **v0.5 改判：放弃 NATS** —— serve 进程内 FastAPI（REST + WebSocket 订阅），`--no-http` 纯 CLI；v0.4 的 servers/autostart/bin 设计作废 | §1.3、§7.1、§8 |
| 7 | 部署目标 | ✅ Linux 服务器长期常驻（supervisord），Windows 仅开发用 | §1.3、§13、§15 |
| 8 | 跨市场 union 视图 | ✅ 加 `bars_1m_all`（market 列 + UNION ALL） | §1.3、§3.3 |
| 9 | news-integration 批注归属 | 未另行指示 → 维持默认：采集逻辑迁入 `dbpath/scripts/fetch_news.py` | §2、§3.1、附录B |
| 10 | v0.6 圆桌评审 45 条 | ✅ **全部采纳**（6 席全票「修改后可实施」，P0×6 / P1×24 / P2×15）：事件循环隔离、SQL 函数默认拒绝、compaction 两阶段、覆盖率白名单、验收两层、qlib 两层、news 表、复权口径、WS 时序统一、export 归位 | §7.6、§8.3、§3.1、§14 等 |

**剩余风险**（非阻塞）：①akshare 类接口源站结构变动频繁，摘抄+primp 的移植代码需在质检中加 **schema 漂移检测**（列名/结构突变即告警），M2 跑通后观察两周；②按 ~600GB（数据 300–400GB + buffer ≥200GB）确认落位盘符。

---

## 附录 A：核心接口示例

```bash
# query —— REST 直接调
curl -s http://127.0.0.1:8765/v1/query -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT date, close FROM cn_stock_1d WHERE symbol=? ORDER BY date DESC LIMIT 5",
       "params":["sh600000"],"fmt":"json"}'
→ {"columns":["date","close"],"dtypes":["date32","float64"],
   "rows":[["2026-09-22",32.15],["2026-09-21",31.98]],"row_count":5,"elapsed_ms":12,"truncated":false}
```

```json
// WebSocket 帧示例：ws://127.0.0.1:8765/v1/stream?topics=state,news.minute&market=cn
{"v":1,"msg_id":"...","topic":"state","payload":{"market":"cn","state":"LUNCH_BREAK",
 "ts":"2026-09-23T03:30:00Z","next_at":"2026-09-23T05:00:00Z","session_tz":"Asia/Shanghai","reason":"session_rule"}}

{"v":1,"msg_id":"...","topic":"news.minute","payload":{"window_start":"2026-09-23T02:39:00Z","count":3,
 "items":[{"id":"h1","ts":"...","source":"rss1","market":"cn","symbols":["sh600000"],
           "level":"important","title":"...","url":"..."}]}}
```

## 附录 B：已有资产复用映射

| 已有项目                                                                   | 在 quantdata 中的角色                             |
| ---------------------------------------------------------------------- | -------------------------------------------- |
| **pytdxdata**（PyPI 0.6.0，三市场 K 线/行情/复权/tick）                           | 主数据源适配器（bars/realtime/ticks/futures/options） |
| **pywestockdata**（腾讯/东财 SDK，全市场 <500ms）                                | A股快照源 + 财务/分红/公告 + 日K双源对账                    |
| **stock_prediction 的 A股 1m（2004→今）**                                   | `init --seed` 直接导入，省 22 年回填                  |
| **market-event-driver**（holidays.py / markets.py）                      | 日历与交易时段规则吸收为 `exchange.session` + 静态节假日表     |
| **news-integration**（RSS 统一采集器）                                        | 采集逻辑迁入 `dbpath/scripts/fetch_news.py`，NewsBridge 消费其输出 |
| **yfinance**（参考源）                                                  | 美/港数据参考对账                                  |
| **akshare 源码**（只摘抄，不安装）                                            | 研报（choices 等）、申万行业、长尾兜底：摘抄实现、请求层用 primp 重写过反爬 |
| **nats-supervisor / event_center**（三节点集群、TLS、supervisord 模板） | ❌ v0.5 起不再需要（NATS 已放弃）；supervisord 部署模板仍可参考 |
| **vxdata / factor-pipeline**                                           | 下游邻居：quantdata 只出干净原始数据，特征与因子归它们             |
