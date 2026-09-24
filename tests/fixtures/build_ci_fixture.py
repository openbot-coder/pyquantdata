"""生成 [CI] 验收链所需的迷你 seed fixture（离线、确定性、可复现）。

设计 §14/§15：[CI] 层 = ``init --skip-history`` + 迷你 seed → serve → query → export。
CI 不联网、不拉真数据，因此把 fixture 生成逻辑固化成脚本 + 产物入库：

用法::

    python tests/fixtures/build_ci_fixture.py            # 写入 ci_seed_1d.parquet
    python tests/fixtures/build_ci_fixture.py --check    # 只校验现有产物是否一致

产物 schema 必须与 ``bars_1d`` 的 seed 读取列一致：
symbol/date/open/high/low/close/volume/amount（§7.3）。
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as paparquet

FIXTURE = Path(__file__).with_name("ci_seed_1d.parquet")

# 10 个交易日 × 2 只股票（A 股规则 symbol，classify_cn 可判定 category）
_DATES = [date(2026, 9, d) for d in range(1, 11)]
_SYMBOLS = ("sh600000", "sz000001")


def build_table() -> pa.Table:
    symbols: list[str] = []
    dates: list[date] = []
    open_: list[float] = []
    high: list[float] = []
    low: list[float] = []
    close: list[float] = []
    volume: list[float] = []
    amount: list[float] = []
    for si, sym in enumerate(_SYMBOLS):
        base = 10.0 + si * 20.0  # 第二只股票价格区间错开，便于区分
        for i, d in enumerate(_DATES):
            symbols.append(sym)
            dates.append(d)
            open_.append(round(base + i * 0.1, 4))
            high.append(round(base + 0.5 + i * 0.1, 4))
            low.append(round(base - 0.5 + i * 0.1, 4))
            close.append(round(base + 0.2 + i * 0.1, 4))
            volume.append(1_000_000.0)
            amount.append(round(10_200_000.0 + i * 1e4 + si * 1e6, 2))
    return pa.table(
        {
            "symbol": symbols,
            "date": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "amount": amount,
        }
    )


def write_fixture() -> Path:
    paparquet.write_table(build_table(), FIXTURE)
    return FIXTURE


def check_fixture() -> bool:
    if not FIXTURE.exists():
        print(f"[check] 缺失：{FIXTURE}")
        return False
    existing = paparquet.read_table(FIXTURE)
    expected = build_table()
    if existing.equals(expected):
        print(f"[check] OK：{FIXTURE.name} 与生成逻辑一致（{existing.num_rows} 行）")
        return True
    print(f"[check] 不一致：{FIXTURE.name} 需重新生成（产物 {existing.num_rows} 行）")
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成/校验 [CI] 迷你 seed fixture")
    parser.add_argument("--check", action="store_true", help="只校验，不写盘")
    args = parser.parse_args(argv)
    if args.check:
        return 0 if check_fixture() else 1
    p = write_fixture()
    print(f"[write] {p}（{build_table().num_rows} 行 × {len(_SYMBOLS)} symbols）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
