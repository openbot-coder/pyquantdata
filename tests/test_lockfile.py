"""lockfile.py：三元判活单写者锁（§11）。"""

from __future__ import annotations

import json

import pytest

from pyquantdata.lockfile import (
    LockHeldError,
    LockInfo,
    acquire,
    is_alive,
    read_lock,
    release,
)


@pytest.fixture
def lock_path(tmp_path):
    return tmp_path / "quantdata.lock"


def test_acquire_and_release(lock_path):
    info = acquire(lock_path, cmd="test")
    assert lock_path.exists()
    assert info.pid > 0
    assert release(lock_path, info) is True
    assert not lock_path.exists()
    assert release(lock_path, info) is False  # 已删


def test_second_acquire_blocked_same_process(lock_path):
    info = acquire(lock_path)
    with pytest.raises(LockHeldError) as e:
        acquire(lock_path)
    assert e.value.info.pid == info.pid


def test_stale_lock_backed_up_and_taken(lock_path):
    stale = LockInfo(
        pid=999_999_999,  # 不存在的进程
        start_time=1_000_000.0,
        boot_id=__import__("pyquantdata.lockfile", fromlist=["_boot_id"])._boot_id(),
        acquired_at="2020-01-01T00:00:00",
        cmd="old",
    )
    lock_path.write_text(stale.to_json(), encoding="utf-8")
    info = acquire(lock_path)
    assert info.pid == __import__("os").getpid()
    bak = lock_path.with_suffix(lock_path.suffix + ".bak")
    assert bak.exists()
    assert json.loads(bak.read_text(encoding="utf-8"))["pid"] == 999_999_999


def test_boot_id_mismatch_is_stale(lock_path, monkeypatch):
    info = acquire(lock_path)
    assert is_alive(info) is True
    fake = LockInfo(info.pid, info.start_time, boot_id=1.0, acquired_at="", cmd="")
    assert is_alive(fake) is False  # 重启过系统


def test_start_time_mismatch_is_stale(lock_path):
    info = acquire(lock_path)
    fake = LockInfo(info.pid, info.start_time + 999.0, info.boot_id, "", "")
    assert is_alive(fake) is False


def test_dead_pid_is_stale(lock_path):
    fake = LockInfo(999_999_999, 1.0, __import__("pyquantdata.lockfile", fromlist=["_boot_id"])._boot_id(), "", "")
    assert is_alive(fake) is False


def test_read_lock_corrupt_json_returns_none(lock_path):
    lock_path.write_text("{not json", encoding="utf-8")
    assert read_lock(lock_path) is None


def test_read_lock_missing_returns_none(lock_path):
    assert read_lock(lock_path) is None


def test_read_lock_bad_fields_returns_none(lock_path):
    lock_path.write_text('{"pid": "x"}', encoding="utf-8")
    assert read_lock(lock_path) is None


def test_release_refuses_to_delete_other_owner(lock_path):
    info = acquire(lock_path)
    other = LockInfo(
        pid=info.pid, start_time=info.start_time + 5.0, boot_id=info.boot_id,
        acquired_at="", cmd="",
    )
    assert release(lock_path, other) is False
    assert lock_path.exists()  # 后来者的锁不被误删


def test_lock_info_roundtrip(tmp_path):
    info = acquire(tmp_path / "l.lock", cmd="quantdata serve")
    parsed = LockInfo.from_json((tmp_path / "l.lock").read_text(encoding="utf-8"))
    assert parsed == info
    assert "quantdata serve" in parsed.cmd
