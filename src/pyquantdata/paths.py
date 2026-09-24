"""dbpath 目录规范（设计 §3.1）。一切状态收敛在 -d 指定的 dbpath。"""

from __future__ import annotations

import os
from pathlib import Path


class DbPathLayout:
    """dbpath 内固定相对位置的访问器。不负责创建目录（init 时 ensure）。"""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).resolve()

    @property
    def config_file(self) -> Path:
        return self.root / "config.json"

    @property
    def catalog(self) -> Path:
        return self.root / "catalog.duckdb"

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def scripts_dir(self) -> Path:
        return self.root / "scripts"

    @property
    def exports_dir(self) -> Path:
        return self.root / "exports"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def tmp_dir(self) -> Path:
        return self.root / "tmp"

    @property
    def lock_file(self) -> Path:
        return self.root / "quantdata.lock"

    @property
    def trash_dir(self) -> Path:
        """compaction 两阶段协议的延迟删除区（M2 使用，目录先规范）。"""
        return self.root / "data" / ".trash"

    def ensure_dirs(self) -> list[Path]:
        """init 用：建全部目录，返回已确保存在的目录列表。幂等。"""
        made: list[Path] = []
        for d in (
            self.root,
            self.data_dir,
            self.scripts_dir,
            self.exports_dir,
            self.logs_dir,
            self.tmp_dir,
            self.trash_dir,
        ):
            if not d.exists():
                d.mkdir(parents=True)
            made.append(d)
        return made


def resolve_dbpath(explicit: str | None) -> Path:
    """-d 显式指定优先；否则环境变量 QUANTDATA_HOME 兜底；都没有则报错。"""
    if explicit:
        return Path(explicit).resolve()
    env = os.environ.get("QUANTDATA_HOME")
    if env:
        return Path(env).resolve()
    raise SystemExit("缺少 dbpath：请用 -d /xxx/dbpath 或设置环境变量 QUANTDATA_HOME")
