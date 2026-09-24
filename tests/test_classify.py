"""A股分类纯函数。"""

from __future__ import annotations

import pytest

from pyquantdata.sources.classify import classify_cn, exchange_of


@pytest.mark.parametrize(
    "symbol,category",
    [
        ("sh600000", "stock"),
        ("sh600519", "stock"),
        ("sh688001", "stock"),  # 科创板
        ("sz000001", "stock"),
        ("sz300750", "stock"),  # 创业板
        ("bj430047", "stock"),
        ("bj833171", "stock"),
        ("sh000001", "index"),  # 上证指数
        ("sz399001", "index"),
        ("sh510300", "etf"),
        ("sz159915", "etf"),
        ("sh513100", "etf"),
        ("sh508056", "reits"),  # 特例优先于 etf 前缀 sh50
        ("sh510050", "etf"),
        ("sh110059", "cbond"),
        ("sz123456", "cbond"),
        ("sz127045", "cbond"),
        ("SH600000", "stock"),  # 大小写归一
    ],
)
def test_classify_cn(symbol, category):
    assert classify_cn(symbol) == category


@pytest.mark.parametrize("symbol", ["", "xx123", "sh999999", "hk00700"])
def test_classify_cn_unknown_is_other(symbol):
    assert classify_cn(symbol) == "other"


@pytest.mark.parametrize(
    "symbol,ex",
    [("sh600000", "sh"), ("sz000001", "sz"), ("bj430047", "bj"), ("AAPL", "")],
)
def test_exchange_of(symbol, ex):
    assert exchange_of(symbol) == ex
