"""数据源适配层：SourceAdapter 协议、A股分类、pytdxdata 适配器。"""

from .base import (
    Bar1dRow,
    CalendarRow,
    FakeAdapter,
    SecurityRow,
    SourceAdapter,
    UpdateReport,
)
from .classify import classify_cn, exchange_of
from .tdx import TdxAdapter

__all__ = [
    "Bar1dRow",
    "CalendarRow",
    "FakeAdapter",
    "SecurityRow",
    "SourceAdapter",
    "TdxAdapter",
    "UpdateReport",
    "classify_cn",
    "exchange_of",
]
