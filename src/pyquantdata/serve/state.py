"""市场状态机纯函数（设计 §10.2 的 M1 简版：日历 + 时段推导，无网络依赖）。

纯本地确定性推导：is_trading_day 由 calendar 表提供；时段规则表内置。
StateEngine 完整事件机（迁移推流）在 M2 接入 serve 调度器。
"""

from __future__ import annotations

from dataclasses import dataclass

MARKET_TZ: dict[str, str] = {
    "cn": "Asia/Shanghai",
    "us": "America/New_York",
    "hk": "Asia/Hong_Kong",
}

# (开, 收) 本地时间，24h 制；M1 不含美股盘前盘后（可扩展）
SESSIONS: dict[str, tuple[tuple[str, str], ...]] = {
    "cn": (("09:30", "11:30"), ("13:00", "15:00")),
    "hk": (("09:30", "12:00"), ("13:00", "16:00")),
    "us": (("09:30", "16:00"),),
}

_AUCTION_START = "09:15"  # A股集合竞价
_MORNING_OPEN = "09:30"


@dataclass(slots=True)
class MarketState:
    market: str
    state: str  # CLOSED / PRE_OPEN_AUCTION / OPEN / LUNCH_BREAK
    session_tz: str
    reason: str
    next_at: str | None = None  # 本地时间 "HH:MM"


def _hhmm(s: str) -> int:
    """'HH:MM' → HHMM 整数（与 market_state 入参口径一致，如 '09:30' → 930）。"""
    h, m = s.split(":")
    return int(h) * 100 + int(m)


def market_state(market: str, now_local_hhmm: int, *, is_trading_day: bool) -> MarketState:
    """给定交易所本地 ``HHMM`` 整数（如 930、1301）与是否交易日，确定性推导状态。

    参数化时间点便于测试全部边界（开盘/午休/收盘/竞价/节假日）。
    """
    tz = MARKET_TZ.get(market, "Asia/Shanghai")
    if market not in SESSIONS:
        raise KeyError(f"未知市场：{market}")
    if not is_trading_day:
        return MarketState(market, "CLOSED", tz, "non_trading_day")
    sessions = SESSIONS[market]
    if market == "cn" and _hhmm(_AUCTION_START) <= now_local_hhmm < _hhmm(_MORNING_OPEN):
        return MarketState(market, "PRE_OPEN_AUCTION", tz, "session_rule", _MORNING_OPEN)
    for open_s, close_s in sessions:
        if _hhmm(open_s) <= now_local_hhmm < _hhmm(close_s):
            return MarketState(market, "OPEN", tz, "session_rule", close_s)
    # 午休：第一段收后到第二段开前（单段市场无午休）
    first_close = _hhmm(sessions[0][1])
    second_open = _hhmm(sessions[1][0]) if len(sessions) > 1 else None
    if second_open is not None and first_close <= now_local_hhmm < second_open:
        return MarketState(market, "LUNCH_BREAK", tz, "session_rule", sessions[1][0])
    return MarketState(market, "CLOSED", tz, "session_rule", sessions[0][0])
