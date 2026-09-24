"""config.json 模型与读写（设计 §12）。JSON 格式；extra=forbid 防拼错字段静默失效。"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env(value: str) -> str:
    """展开 ``${ENV_VAR}``（设计 §12：http.token 支持环境变量展开，不明文入库）。"""
    return _ENV_PATTERN.sub(lambda m: os_environ_get(m.group(1)), value)


def os_environ_get(name: str) -> str:
    import os

    return os.environ.get(name, "")


class MarketConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    history_from: str
    history_from_bars_1d: str | None = None  # null 时跟随 history_from（§3.4）


class HttpConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8765
    token: str = ""
    max_ws_conns: int = 64
    ws_queue_len: int = 256


class SourcesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bars: str = "tdx"
    realtime: str = "westock"
    reference: list[str] = Field(default_factory=lambda: ["tdx", "westock"])
    futures: list[str] = Field(default_factory=lambda: ["tdx", "yfinance"])
    news: list[str] = Field(default_factory=lambda: ["scripts/fetch_news.py"])
    fallback: list[str] = Field(default_factory=lambda: ["akshare_port"])
    cross_check: bool = True
    cross_check_ratio: float = 0.01


class ProxyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    global_: str = Field(default="", alias="global")
    per_source: dict[str, str] = Field(default_factory=dict)


class FetchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rate_limit_rps: dict[str, float] = Field(default_factory=dict)
    timeout_ms: int = 10000
    max_retry: int = 3


class DailyChainConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cn: str = "15:05"
    hk: str = "16:10"
    us: str = "04:30"


class ScheduleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timezone: str = "Asia/Shanghai"  # 显式绑定，不读系统 TZ（评审 P1）
    cn_intraday_interval_s: int = 90
    snapshot_interval_s: int = 3
    daily_chain: DailyChainConfig = Field(default_factory=DailyChainConfig)
    compaction: str = "monthly"


class QualityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    checks: list[str] = Field(
        default_factory=lambda: ["gap", "ohlc", "limit", "adj", "rowcount", "dualsource"]
    )


class SqlConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_rows: int = 1_000_000
    timeout_ms: int = 30_000
    max_arrow_bytes: int = 64 * 1024 * 1024
    max_concurrent: int = 4
    acquire_timeout_s: float = 5.0  # 信号量等待超过即 429（§8.3）
    memory_limit: str = "2GB"
    allow_functions: list[str] = Field(default_factory=list)  # 额外放行清单（§8.3）


class ExportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    out_dir: str = "exports"
    cache_ttl_h: int = 24
    keep_days: int = 7
    formats: list[str] = Field(default_factory=lambda: ["csv", "parquet", "qlib", "backtrader"])


class LogConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: str = "INFO"
    keep_days: int = 30


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = 1
    dbpath_created: str
    markets: dict[str, MarketConfig]
    categories: dict[str, list[str]]
    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    fetch: FetchConfig = Field(default_factory=FetchConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    quality: QualityConfig = Field(default_factory=QualityConfig)
    sql: SqlConfig = Field(default_factory=SqlConfig)
    export: ExportConfig = Field(default_factory=ExportConfig)
    log: LogConfig = Field(default_factory=LogConfig)


_DEFAULT_CATEGORIES = {
    "cn": ["index", "stock", "future", "cbond", "etf", "reits", "option"],
    "us": ["index", "stock", "future", "etf", "reits"],
    "hk": ["index", "stock", "future", "etf", "reits"],
}


def default_config(markets: list[str] | None = None, now: datetime | None = None) -> AppConfig:
    """init 生成的默认 config（§12）。markets 默认三市场全开。"""
    markets = markets or ["cn", "us", "hk"]
    defaults = {
        "cn": ("2004-01-01", None),
        "us": ("2020-01-01", None),
        "hk": ("2020-01-01", None),
    }
    now = now or datetime.now(timezone.utc)
    return AppConfig(
        dbpath_created=now.isoformat(),
        markets={
            m: MarketConfig(enabled=True, history_from=defaults[m][0], history_from_bars_1d=None)
            for m in markets
        },
        categories={m: list(_DEFAULT_CATEGORIES[m]) for m in markets},
    )


def write_config(config: AppConfig, path: Path) -> None:
    path.write_text(config.model_dump_json(indent=2, by_alias=True), encoding="utf-8")


def _expand_strings(obj: Any) -> Any:
    if isinstance(obj, str):
        return expand_env(obj)
    if isinstance(obj, list):
        return [_expand_strings(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _expand_strings(v) for k, v in obj.items()}
    return obj


def load_config(path: Path) -> AppConfig:
    """读 config.json 并展开 ${ENV}；字段拼错/多余直接报错（extra=forbid）。"""
    raw = json.loads(path.read_text(encoding="utf-8"))
    return AppConfig.model_validate(_expand_strings(raw))
