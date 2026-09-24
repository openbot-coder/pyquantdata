"""鉴权与日志脱敏（设计 §8.4）。

- token 非空时：REST ``Authorization: Bearer <token>``；WS（M3）优先握手 Header；
- 非本机且 token 为空 → 拒绝启动（fail-fast，--insecure 显式放行）；
- 日志对 token 统一脱敏（只留前 4 位）。
"""

from __future__ import annotations

import re

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

_TOKEN_IN_URL = re.compile(r"(token=)([^&\s]{4})[^&\s]*")


def mask_token(token: str) -> str:
    """token 脱敏：留前 4 位 + ***；空串原样。"""
    if not token:
        return ""
    return token[:4] + "***"


def mask_query_string(qs: str) -> str:
    """日志里的 ``?token=...`` / ``&token=...`` 统一脱敏（含 uvicorn access log 场景）。"""
    return _TOKEN_IN_URL.sub(r"\1\2***", qs)


def is_loopback(host: str) -> bool:
    return host in _LOOPBACK_HOSTS


def assert_security(host: str, token: str, *, insecure: bool) -> None:
    """启动强制校验（§8.4）：非本机 + 空 token + 未显式 --insecure → 拒绝启动。"""
    if is_loopback(host) or token or insecure:
        return
    raise SecurityError(
        f"拒绝启动：监听地址 {host} 非 loopback 且 http.token 为空；"
        f"请设置 token 或显式使用 --insecure（§8.4 fail-fast）"
    )


class SecurityError(RuntimeError):
    pass


def token_ok(provided: str | None, expected: str) -> bool:
    """expected 为空 = 不校验；否则必须精确匹配。"""
    if not expected:
        return True
    return bool(provided) and provided == expected
