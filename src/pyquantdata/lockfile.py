"""单写者锁 quantdata.lock（设计 §11）。

判活 = PID + 进程 start time + boot_id 三元一致（防 PID 复用误判）。
发现陈旧锁：先备份 .lock.bak 再接管，并记日志。
"""

from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import psutil


class LockHeldError(RuntimeError):
    def __init__(self, info: "LockInfo") -> None:
        self.info = info
        super().__init__(
            f"已有 quantdata 实例持有锁：PID={info.pid}（ acquired_at={info.acquired_at}）。"
            f"如确认无实例在跑，可删除锁文件。"
        )


@dataclass
class LockInfo:
    pid: int
    start_time: float
    boot_id: float
    acquired_at: str
    cmd: str

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, text: str) -> "LockInfo":
        d = json.loads(text)
        return cls(
            pid=int(d["pid"]),
            start_time=float(d["start_time"]),
            boot_id=float(d["boot_id"]),
            acquired_at=str(d.get("acquired_at", "")),
            cmd=str(d.get("cmd", "")),
        )


def _boot_id() -> float:
    """boot 标识：psutil.boot_time() 秒级时间戳，Windows/Linux 通用。"""
    return psutil.boot_time()


def _process_start_time(pid: int) -> float | None:
    """进程当前 start time；进程不存在返回 None。"""
    try:
        return psutil.Process(pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return None


def is_alive(info: LockInfo, *, now_boot_id: float | None = None) -> bool:
    """三元判活：进程存在 + start_time 一致 + boot_id 一致。"""
    now_boot_id = _boot_id() if now_boot_id is None else now_boot_id
    if info.boot_id != now_boot_id:
        return False  # 重启过系统 → 一律陈旧
    start = _process_start_time(info.pid)
    if start is None:
        return False
    # start_time 允许极小浮点误差
    return abs(start - info.start_time) < 0.01


def read_lock(path: Path) -> LockInfo | None:
    try:
        return LockInfo.from_json(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, KeyError, ValueError):
        # 损坏的锁文件视为陈旧（半写状态）
        return None


def acquire(lock_path: Path, *, cmd: str = "", acquired_at: str | None = None) -> LockInfo:
    """拿锁；活锁持有 → LockHeldError；陈旧/损坏锁 → 备份后接管。"""
    existing = read_lock(lock_path)
    if existing is not None and is_alive(existing):
        raise LockHeldError(existing)
    if existing is not None:
        # 陈旧锁：先备份再接管（§11）
        backup = lock_path.with_suffix(lock_path.suffix + ".bak")
        shutil.copy2(lock_path, backup)
        try:
            print(f"[lock] 发现陈旧锁（PID={existing.pid} 已不存在），备份到 {backup} 后接管")
        except OSError:  # stdout 关闭等极端场景不阻塞拿锁
            pass
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    me = psutil.Process(os.getpid())
    info = LockInfo(
        pid=me.pid,
        start_time=me.create_time(),
        boot_id=_boot_id(),
        acquired_at=acquired_at or time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        cmd=cmd,
    )
    lock_path.write_text(info.to_json(), encoding="utf-8")
    return info


def release(lock_path: Path, info: LockInfo) -> bool:
    """释放锁：仅当锁内仍是自己（防止误删后来者的锁）才删除。"""
    current = read_lock(lock_path)
    if current is None:
        return False
    if current.pid == info.pid and abs(current.start_time - info.start_time) < 0.01:
        lock_path.unlink(missing_ok=True)
        return True
    return False
