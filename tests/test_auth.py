"""鉴权与脱敏（§8.4）。"""

from __future__ import annotations

import pytest

from pyquantdata.serve.auth import (
    SecurityError,
    assert_security,
    is_loopback,
    mask_query_string,
    mask_token,
    token_ok,
)


def test_mask_token():
    assert mask_token("") == ""
    assert mask_token("ab") == "ab***"
    assert mask_token("abcd1234") == "abcd***"


def test_mask_query_string():
    assert mask_query_string("token=abcd1234&topics=state") == "token=abcd***&topics=state"
    assert mask_query_string("/v1/stream?topics=state&token=zzzz9999") == (
        "/v1/stream?topics=state&token=zzzz***"
    )
    assert mask_query_string("no-token-here") == "no-token-here"
    assert mask_query_string("token=ab") == "token=ab"  # 不足 4 位不匹配模板（保持原样）


def test_is_loopback():
    assert is_loopback("127.0.0.1")
    assert is_loopback("::1")
    assert is_loopback("localhost")
    assert not is_loopback("0.0.0.0")
    assert not is_loopback("192.168.1.10")


def test_assert_security_matrix():
    assert_security("127.0.0.1", "", insecure=False)  # loopback 不需要 token
    assert_security("0.0.0.0", "tok", insecure=False)  # 有 token
    assert_security("0.0.0.0", "", insecure=True)  # 显式裸奔
    with pytest.raises(SecurityError, match="拒绝启动"):
        assert_security("0.0.0.0", "", insecure=False)


def test_token_ok():
    assert token_ok(None, "") is True  # 空 token = 不校验
    assert token_ok("any", "") is True
    assert token_ok("right", "right") is True
    assert token_ok("wrong", "right") is False
    assert token_ok(None, "right") is False
