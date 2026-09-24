"""config.py：默认生成、roundtrip、extra=forbid、${ENV} 展开。"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from pyquantdata.config import (
    AppConfig,
    default_config,
    expand_env,
    load_config,
    write_config,
)


def test_default_config_three_markets():
    cfg = default_config()
    assert set(cfg.markets) == {"cn", "us", "hk"}
    assert cfg.markets["cn"].history_from == "2004-01-01"
    assert cfg.markets["us"].history_from == "2020-01-01"
    assert cfg.markets["hk"].history_from_bars_1d is None  # 跟随 history_from
    assert cfg.http.port == 8765
    assert cfg.schedule.timezone == "Asia/Shanghai"
    assert cfg.sql.max_concurrent == 4
    assert cfg.export.formats == ["csv", "parquet", "qlib", "backtrader"]


def test_default_config_subset_markets():
    cfg = default_config(["cn"])
    assert set(cfg.markets) == {"cn"}


def test_roundtrip(tmp_path):
    cfg = default_config()
    path = tmp_path / "config.json"
    write_config(cfg, path)
    loaded = load_config(path)
    assert loaded == cfg
    text = path.read_text(encoding="utf-8")
    assert '"version": 1' in text


def test_extra_field_forbidden(tmp_path):
    cfg = default_config()
    import json

    raw = json.loads(cfg.model_dump_json(by_alias=True))
    raw["hhtp"] = {"port": 1}  # 拼错字段必须报错而不是静默失效
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValidationError):
        load_config(bad)


def test_expand_env_token(monkeypatch):
    monkeypatch.setenv("QD_TOKEN", "s3cret")
    assert expand_env("${QD_TOKEN}") == "s3cret"


def test_expand_env_missing_var():
    assert expand_env("${QD_NOT_SET_XYZ}") == ""


def test_expand_env_plain_string():
    assert expand_env("no-var-here") == "no-var-here"


def test_load_applies_env_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("QD_TOKEN", "abcd1234")
    cfg = default_config()
    cfg.http.token = "${QD_TOKEN}"
    path = tmp_path / "config.json"
    write_config(cfg, path)
    loaded = load_config(path)
    assert loaded.http.token == "abcd1234"


def test_now_override_is_used():
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    cfg = default_config(now=now)
    assert cfg.dbpath_created.startswith("2026-01-02T03:04:05")


def test_app_config_forbids_unknown_section():
    import json

    raw = json.loads(default_config().model_dump_json(by_alias=True))
    raw["mystery"] = 1
    with pytest.raises(ValidationError):
        AppConfig.model_validate(raw)
