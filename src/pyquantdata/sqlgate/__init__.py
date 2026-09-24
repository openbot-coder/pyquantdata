"""SQL 闸：query_sql 安全约束（§8.3）。"""

from .gate import FORBIDDEN_FUNCTIONS, SqlRejected, validate

__all__ = ["FORBIDDEN_FUNCTIONS", "SqlRejected", "validate"]
