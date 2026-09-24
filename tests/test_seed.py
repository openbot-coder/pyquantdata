"""seed 导入：parquet/csv、幂等、校验。"""

from __future__ import annotations

from datetime import date

import pyarrow as pa
import pyarrow.parquet as paparquet
import pytest

from pyquantdata.etl import SeedError, import_seed


def test_import_parquet(catalog, mini_seed):
    report = import_seed(catalog, mini_seed)
    assert report.rows == 10
    assert report.symbols == ["sh600000"]
    assert report.date_min == date(2026, 9, 1)
    assert report.date_max == date(2026, 9, 10)
    rows = catalog.execute(
        "SELECT count(*), min(date), max(date) FROM bars_1d WHERE symbol='sh600000'"
    ).fetchone()
    assert rows == (10, report.date_min, report.date_max)
    # category 按规则分类
    assert catalog.execute(
        "SELECT DISTINCT category FROM bars_1d WHERE symbol='sh600000'"
    ).fetchone()[0] == "stock"


def test_seed_idempotent_replay(catalog, mini_seed):
    import_seed(catalog, mini_seed)
    import_seed(catalog, mini_seed)
    assert catalog.execute("SELECT count(*) FROM bars_1d").fetchone()[0] == 10


def test_import_csv_with_alias_columns(catalog, tmp_path):
    p = tmp_path / "seed.csv"
    p.write_text(
        "symbol,date,open,high,low,close,vol,amt\n"
        "sz000001,2026-09-01,15.0,15.5,14.8,15.2,2000,30000\n"
        "sz000001,2026-09-02,15.2,15.8,15.0,15.6,2200,34000\n",
        encoding="utf-8",
    )
    report = import_seed(catalog, p)
    assert report.rows == 2
    row = catalog.execute(
        "SELECT volume, amount FROM bars_1d WHERE symbol='sz000001' AND date='2026-09-01'"
    ).fetchone()
    assert row == (2000.0, 30000.0)


def test_import_with_optional_adjust_factor(catalog, tmp_path):
    p = tmp_path / "adj.parquet"
    paparquet.write_table(
        pa.table(
            {
                "symbol": ["sh600000"],
                "date": [date(2026, 9, 1)],
                "open": [10.0],
                "high": [10.5],
                "low": [9.5],
                "close": [10.2],
                "volume": [100.0],
                "amount": [1000.0],
                "adj_factor": [1.5],
                "turnover": [1.2],
            }
        ),
        p,
    )
    import_seed(catalog, p)
    row = catalog.execute(
        "SELECT adj_factor, turnover FROM bars_1d WHERE symbol='sh600000'"
    ).fetchone()
    assert row == (1.5, 1.2)


def test_missing_required_column(catalog, tmp_path):
    p = tmp_path / "bad.parquet"
    paparquet.write_table(pa.table({"symbol": ["x"], "close": [1.0]}), p)
    with pytest.raises(SeedError, match="缺必需列"):
        import_seed(catalog, p)


def test_missing_file(catalog, tmp_path):
    with pytest.raises(SeedError, match="不存在"):
        import_seed(catalog, tmp_path / "nope.parquet")


def test_empty_table(catalog, tmp_path):
    p = tmp_path / "empty.parquet"
    paparquet.write_table(
        pa.table(
            {"symbol": [], "date": [], "open": [], "high": [], "low": [], "close": []}
        ),
        p,
    )
    with pytest.raises(SeedError, match="无数据行"):
        import_seed(catalog, p)


def test_unsupported_extension(catalog, tmp_path):
    p = tmp_path / "seed.xlsx"
    p.write_text("x", encoding="utf-8")
    with pytest.raises(SeedError, match="仅支持"):
        import_seed(catalog, p)


def test_empty_symbol_rejected(catalog, tmp_path):
    p = tmp_path / "blank.parquet"
    paparquet.write_table(
        pa.table(
            {
                "symbol": ["  "],
                "date": [date(2026, 9, 1)],
                "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
            }
        ),
        p,
    )
    with pytest.raises(SeedError, match="symbol 为空"):
        import_seed(catalog, p)


def test_import_failure_rolls_back(catalog, tmp_path):
    """半途失败：先前 symbol 的写入也要回滚。"""
    p = tmp_path / "bad_mid.parquet"
    paparquet.write_table(
        pa.table(
            {
                "symbol": ["sh600000", "sz000001"],
                "date": ["2026-09-01", "not-a-date"],  # 全字符串列；坏日期在行转换期抛错
                "open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0], "close": [1.0, 1.0],
            }
        ),
        p,
    )
    with pytest.raises(Exception):
        import_seed(catalog, p)
    assert catalog.execute("SELECT count(*) FROM bars_1d").fetchone()[0] == 0
