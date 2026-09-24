"""市场状态机纯函数：参数化时间边界全覆盖。"""

from __future__ import annotations

import pytest

from pyquantdata.serve.state import MARKET_TZ, market_state


def test_cn_session_boundaries():
    # (hhmm, expected_state)
    cases = [
        (914, "CLOSED"),          # 开盘前
        (915, "PRE_OPEN_AUCTION"),  # 集合竞价开始
        (925, "PRE_OPEN_AUCTION"),
        (929, "PRE_OPEN_AUCTION"),
        (930, "OPEN"),
        (1129, "OPEN"),
        (1130, "LUNCH_BREAK"),
        (1259, "LUNCH_BREAK"),
        (1300, "OPEN"),
        (1459, "OPEN"),
        (1500, "CLOSED"),         # 收盘
        (2359, "CLOSED"),
    ]
    for hhmm, state in cases:
        ms = market_state("cn", hhmm, is_trading_day=True)
        assert ms.state == state, f"hhmm={hhmm}: got {ms.state}, want {state}"


def test_cn_non_trading_day():
    ms = market_state("cn", 1000, is_trading_day=False)
    assert ms.state == "CLOSED"
    assert ms.reason == "non_trading_day"


def test_us_no_lunch_break():
    assert market_state("us", 1200, is_trading_day=True).state == "OPEN"
    assert market_state("us", 929, is_trading_day=True).state == "CLOSED"
    assert market_state("us", 930, is_trading_day=True).state == "OPEN"
    assert market_state("us", 1559, is_trading_day=True).state == "OPEN"
    assert market_state("us", 1600, is_trading_day=True).state == "CLOSED"


def test_hk_lunch_break():
    assert market_state("hk", 1200, is_trading_day=True).state == "LUNCH_BREAK"
    assert market_state("hk", 1259, is_trading_day=True).state == "LUNCH_BREAK"
    assert market_state("hk", 1300, is_trading_day=True).state == "OPEN"
    assert market_state("hk", 1600, is_trading_day=True).state == "CLOSED"


def test_fields_complete():
    ms = market_state("cn", 1000, is_trading_day=True)
    assert ms.market == "cn"
    assert ms.session_tz == "Asia/Shanghai"
    assert ms.reason == "session_rule"
    assert ms.next_at == "11:30"  # 收段时刻
    ms2 = market_state("cn", 1200, is_trading_day=True)
    assert ms2.next_at == "13:00"  # 午休结束
    ms3 = market_state("cn", 1600, is_trading_day=True)
    assert ms3.next_at == "09:30"  # 次日开盘
    ms4 = market_state("cn", 920, is_trading_day=True)
    assert ms4.next_at == "09:30"  # 竞价结束


def test_market_tz_table():
    assert set(MARKET_TZ) == {"cn", "us", "hk"}


def test_unknown_market_raises():
    with pytest.raises(KeyError, match="未知市场"):
        market_state("uk", 1000, is_trading_day=True)
