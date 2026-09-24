"""paths.py：目录规范与 dbpath 解析。"""

from __future__ import annotations

import pytest

from pyquantdata.paths import DbPathLayout, resolve_dbpath


def test_layout_properties(tmp_path):
    lay = DbPathLayout(tmp_path / "db")
    assert lay.config_file.name == "config.json"
    assert lay.catalog.name == "catalog.duckdb"
    assert lay.lock_file.name == "quantdata.lock"
    assert lay.trash_dir.name == ".trash"
    assert lay.data_dir.parent == lay.root


def test_ensure_dirs_creates_all(tmp_path):
    lay = DbPathLayout(tmp_path / "db")
    made = lay.ensure_dirs()
    for d in (lay.root, lay.data_dir, lay.scripts_dir, lay.exports_dir, lay.logs_dir, lay.tmp_dir, lay.trash_dir):
        assert d.exists()
    assert lay.root in made


def test_ensure_dirs_idempotent(tmp_path):
    lay = DbPathLayout(tmp_path / "db")
    lay.ensure_dirs()
    made2 = lay.ensure_dirs()
    assert len(made2) == 7


def test_resolve_explicit_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTDATA_HOME", str(tmp_path / "from_env"))
    assert resolve_dbpath(str(tmp_path / "explicit")) == (tmp_path / "explicit").resolve()


def test_resolve_env_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTDATA_HOME", str(tmp_path / "from_env"))
    assert resolve_dbpath(None) == (tmp_path / "from_env").resolve()


def test_resolve_missing_raises(monkeypatch):
    monkeypatch.delenv("QUANTDATA_HOME", raising=False)
    with pytest.raises(SystemExit):
        resolve_dbpath(None)
