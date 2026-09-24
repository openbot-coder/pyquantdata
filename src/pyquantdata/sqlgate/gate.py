"""query_sql 安全闸（设计 §8.3，评审 P0/P1 多条）。

六条闸：
1. 只读连接 + 扩展自动加载关闭（store/db.configure_query_conn）；
2. 语句白名单：sqlglot fail-closed（解析失败一律拒）、dialect=duckdb、
   AST 根节点判定（非字符串前缀）、多语句拒绝（parse_all 长度必须 =1）；
3. 函数/表函数默认拒绝：显式拒 read_csv / read_parquet / *_scan / glob /
   getenv / current_setting 等文件·网络·环境函数；FROM/JOIN 出现表函数直接拒；
   ``allow_functions`` 语义 = 额外放行清单（不是清空即全放行的总开关）；
4. 结果上限（执行层 max_rows / arrow bytes）；
5. 超时与并发闸（执行层线程池 + interrupt + 信号量）；
6. 防御参数（SET max_expression_depth）。

sqlglot 进 lock pin 版本（pyproject >=,< 由 lock 冻结），升级须过本模块回归测试。
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

_DIALECT = "duckdb"

# 显式拒绝的函数（文件/网络/环境注入面），小写；`*_scan` 走后缀规则
FORBIDDEN_FUNCTIONS: frozenset[str] = frozenset(
    {
        "read_csv",
        "read_csv_auto",
        "read_parquet",
        "read_json",
        "read_json_auto",
        "read_ndjson",
        "read_text",
        "glob",
        "getenv",
        "current_setting",
        "iceberg_scan",
        "delta_scan",
    }
)

_SCAN_SUFFIX = "_scan"

# 允许的 AST 根节点：纯读取查询（CTE 附着在 Select 上）
_ALLOWED_ROOTS: tuple[type[exp.Expression], ...] = (
    exp.Select,
    exp.Union,
    exp.Intersect,
    exp.Except,
    exp.Subquery,
)


class SqlRejected(ValueError):
    """SQL 被安全闸拒绝。reason 面向调用方，可直接进 HTTP 400 detail。"""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _func_name(node: exp.Expression) -> str | None:
    if isinstance(node, exp.Anonymous):
        return str(node.name).lower()
    if isinstance(node, exp.Func):
        return node.sql_name().lower()
    return None


def _check_root(stmt: exp.Expression) -> None:
    if not isinstance(stmt, _ALLOWED_ROOTS):
        raise SqlRejected(f"仅允许只读查询（SELECT/UNION），收到：{type(stmt).__name__}")


def _check_from_tables(stmt: exp.Expression) -> None:
    """FROM/JOIN 出现函数（表函数）一律拒绝：库内视图都是普通表引用。"""
    for table in stmt.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise SqlRejected(
                f"FROM/JOIN 仅允许库内普通表或视图，发现表函数：{table.sql(dialect=_DIALECT)}"
            )


def _check_functions(stmt: exp.Expression, allow_functions: list[str]) -> None:
    allowed = {a.lower().strip() for a in allow_functions if a.strip()}
    for node in stmt.walk():
        name = _func_name(node)
        if name is None:
            continue
        if name in FORBIDDEN_FUNCTIONS and name not in allowed:
            raise SqlRejected(
                f"函数默认拒绝（文件/网络/环境注入面）：{name}；"
                f"确需放行请在 config sql.allow_functions 显式加入（§8.3）"
            )
        if name.endswith(_SCAN_SUFFIX) and name not in allowed:
            raise SqlRejected(f"函数默认拒绝（*_scan 表函数）：{name}")


def parse_one(sql: str) -> exp.Expression:
    """fail-closed 解析：任何解析失败/空语句/多语句 → SqlRejected。"""
    if not sql or not sql.strip():
        raise SqlRejected("空 SQL")
    try:
        statements = sqlglot.parse(sql, read=_DIALECT)
    except Exception as e:  # sqlglot.errors.ParseError 及一切解析期异常
        raise SqlRejected(f"SQL 解析失败（fail-closed）：{e}") from e
    if len(statements) != 1 or statements[0] is None:
        raise SqlRejected(f"必须且只能包含一条语句，收到 {len(statements)} 条（多语句拒绝）")
    return statements[0]


def validate(sql: str, allow_functions: list[str] | None = None) -> exp.Expression:
    """通过全部闸则返回解析后的 AST；否则抛 SqlRejected。"""
    allow_functions = allow_functions or []
    stmt = parse_one(sql)
    _check_root(stmt)
    _check_from_tables(stmt)
    _check_functions(stmt, allow_functions)
    return stmt
