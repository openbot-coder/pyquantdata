"""A股证券分类（纯函数，表驱动）。symbol 规范 = pytdxdata 形态：sh600000 / sz000001 / bj430047。"""

from __future__ import annotations

# (category, 前缀元组) —— 顺序敏感：先长/特例后短/泛化
_CLASSIFY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("index", ("sh000", "sh899", "sz399", "sz395")),
    ("reits", ("sh508",)),
    ("etf", ("sh50", "sh51", "sh56", "sh58", "sz15", "sz16", "sz18")),
    ("cbond", ("sh10", "sh11", "sh12", "sz12")),
    ("stock", ("sh60", "sh68", "sz00", "sz30", "sz20")),
    ("stock", ("bj",)),
)


def classify_cn(symbol: str) -> str:
    """按 symbol 前缀归类 A股品类；未匹配归 other（绝不猜）。"""
    s = symbol.strip().lower()
    for category, prefixes in _CLASSIFY_RULES:
        if any(s.startswith(p) for p in prefixes):
            return category
    return "other"


def exchange_of(symbol: str) -> str:
    """sh/sz/bj 前缀即交易所；无法识别返回空串。"""
    s = symbol.strip().lower()
    for ex in ("sh", "sz", "bj"):
        if s.startswith(ex):
            return ex
    return ""
