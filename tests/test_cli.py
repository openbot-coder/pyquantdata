"""CLI：typer CliRunner 全命令（update 用 FakeAdapter 替身，不打真网）。"""

from __future__ import annotations

import json
from datetime import date

import pytest
from typer.testing import CliRunner

from pyquantdata.cli import app
from pyquantdata.paths import DbPathLayout

runner = CliRunner()


@pytest.fixture
def initialized(dbpath, mini_seed):
    result = runner.invoke(app, ["init", "-d", str(dbpath), "--skip-history", "--seed", str(mini_seed)])
    assert result.exit_code == 0, result.output
    return dbpath


def test_init_creates_layout_and_config(dbpath, mini_seed):
    result = runner.invoke(app, ["init", "-d", str(dbpath), "--skip-history", "--seed", str(mini_seed)])
    assert result.exit_code == 0, result.output
    assert "init 完成" in result.output
    assert "[1/4]" in result.output and "[4/4]" in result.output  # 分阶段进度
    lay = DbPathLayout(dbpath)
    assert lay.config_file.exists() and lay.catalog.exists()
    assert "bars_1d rows=10" in result.output  # seed 已导入
    assert "seed: 10 行" in result.output


def test_init_idempotent_keeps_config(dbpath):
    assert runner.invoke(app, ["init", "-d", str(dbpath)]).exit_code == 0
    cfg = DbPathLayout(dbpath).config_file.read_text(encoding="utf-8")
    cfg_obj = json.loads(cfg)
    cfg_obj["http"]["port"] = 9999
    DbPathLayout(dbpath).config_file.write_text(json.dumps(cfg_obj), encoding="utf-8")
    result = runner.invoke(app, ["init", "-d", str(dbpath)])
    assert result.exit_code == 0
    assert "已存在，保留" in result.output
    assert json.loads(DbPathLayout(dbpath).config_file.read_text(encoding="utf-8"))["http"]["port"] == 9999


def test_init_estimate_does_not_write(tmp_path):
    target = tmp_path / "never-created"
    result = runner.invoke(app, ["init", "-d", str(target), "--estimate"])
    assert result.exit_code == 0
    assert not target.exists()  # 只算不写
    assert "budget_gb" in result.output


def test_init_seed_failure_exit_1(dbpath, tmp_path):
    result = runner.invoke(
        app, ["init", "-d", str(dbpath), "--seed", str(tmp_path / "nope.parquet")]
    )
    assert result.exit_code == 1
    assert "seed 导入失败" in result.output


def test_query_table_format(initialized):
    result = runner.invoke(app, [
        "query", "-d", str(initialized),
        "SELECT date, close FROM cn_stock_1d ORDER BY date LIMIT 3",
    ])
    assert result.exit_code == 0, result.output
    assert "sh600000" not in result.output  # 未选 symbol 列
    assert "3 rows" in result.output


def test_query_json_format(initialized):
    result = runner.invoke(app, [
        "query", "-d", str(initialized), "--fmt", "json",
        "SELECT count(*) AS n FROM cn_stock_1d",
    ])
    assert result.exit_code == 0
    payload = json.loads(result.output.strip())  # 纯 stdout JSON（无 rich 前缀）
    assert payload["row_count"] == 1
    assert payload["rows"][0][0] == 10


def test_query_csv_to_file(initialized, tmp_path):
    out = tmp_path / "q.csv"
    result = runner.invoke(app, [
        "query", "-d", str(initialized), "--fmt", "csv", "-o", str(out),
        "SELECT symbol, close FROM cn_stock_1d",
    ])
    assert result.exit_code == 0
    assert "已写出" in result.output
    assert out.exists()


def test_query_arrow_to_file(initialized, tmp_path):
    """query --fmt arrow 落盘 = Arrow IPC **file** 格式（0.1.0 bug 回归）。

    原实现把 arrow 透传给只认 csv/parquet 的 write_export → ExportFormatError
    裸 traceback；修复后必须写出可被 open_file 随机重开的 footer 完整文件。
    """
    out = tmp_path / "q.arrow"
    result = runner.invoke(app, [
        "query", "-d", str(initialized), "--fmt", "arrow", "-o", str(out),
        "SELECT symbol, close FROM cn_stock_1d",
    ])
    assert result.exit_code == 0, result.output
    assert "已写出" in result.output
    assert out.exists() and out.stat().st_size > 0
    import pyarrow as pa
    import pyarrow.ipc as pai

    with pai.open_file(out) as reader:  # file 格式（footer 随机访问），非 stream
        tbl = reader.read_all()
    assert tbl.num_rows == 10
    assert tbl.schema.names == ["symbol", "close"]


def test_query_csv_without_out_exit_1(initialized):
    result = runner.invoke(app, [
        "query", "-d", str(initialized), "--fmt", "csv", "SELECT 1",
    ])
    assert result.exit_code == 1
    assert "-o" in result.output


def test_query_bad_fmt_exit_1(initialized):
    result = runner.invoke(app, [
        "query", "-d", str(initialized), "--fmt", "xlsx", "SELECT 1",
    ])
    assert result.exit_code == 1


def test_query_sql_rejected_exit_1(initialized):
    result = runner.invoke(app, [
        "query", "-d", str(initialized), "SELECT read_csv('/etc/passwd')",
    ])
    assert result.exit_code == 1
    assert "SQL 被拒绝" in result.output


def test_query_uninitialized_db_exit_1(tmp_path):
    result = runner.invoke(app, ["query", "-d", str(tmp_path / "nope"), "SELECT 1"])
    assert result.exit_code == 1
    assert "未初始化" in result.output


def test_export_csv(initialized, tmp_path):
    out = tmp_path / "exp.csv"
    result = runner.invoke(app, [
        "export", "-d", str(initialized), "--fmt", "csv", "-o", str(out),
        "--sql", "SELECT * FROM cn_stock_1d",
    ])
    assert result.exit_code == 0, result.output
    assert out.exists()
    assert "导出完成" in result.output


def test_export_parquet(initialized, tmp_path):
    out = tmp_path / "exp.parquet"
    result = runner.invoke(app, [
        "export", "-d", str(initialized), "--fmt", "parquet", "-o", str(out),
        "--sql", "SELECT * FROM cn_stock_1d",
    ])
    assert result.exit_code == 0
    import pyarrow.parquet as paparquet

    assert paparquet.read_table(out).num_rows == 10


def test_export_pandas_hint_exit_1(initialized, tmp_path):
    result = runner.invoke(app, [
        "export", "-d", str(initialized), "--fmt", "pandas", "-o", str(tmp_path / "x"),
        "--sql", "SELECT 1",
    ])
    assert result.exit_code == 1
    assert "query --fmt arrow" in result.output


def test_export_sql_rejected_exit_1(initialized, tmp_path):
    result = runner.invoke(app, [
        "export", "-d", str(initialized), "--fmt", "csv", "-o", str(tmp_path / "x.csv"),
        "--sql", "ATTACH 'x'",
    ])
    assert result.exit_code == 1


def test_status_json(initialized):
    result = runner.invoke(app, ["status", "-d", str(initialized), "--json"])
    assert result.exit_code == 0
    # rich.print_json 输出在 result.output；去掉可能的空行
    start = result.output.index("{")
    payload = json.loads(result.output[start:])
    assert payload["schema_version"] == 1
    assert payload["bars_1d_rows"] == 10
    assert payload["lock"]["held"] is False
    assert payload["disk"] is not None


def test_status_plain_same_payload(initialized):
    result = runner.invoke(app, ["status", "-d", str(initialized)])
    assert result.exit_code == 0
    assert "schema_version" in result.output


def test_quantdata_home_env(dbpath, mini_seed, monkeypatch):
    monkeypatch.setenv("QUANTDATA_HOME", str(dbpath))
    result = runner.invoke(app, ["init", "--skip-history", "--seed", str(mini_seed)])
    assert result.exit_code == 0, result.output
    assert DbPathLayout(dbpath).config_file.exists()


def test_update_with_fake_adapter(initialized, monkeypatch):
    """update 全链（--dataset securities/calendar/bars_1d）用替身适配器。"""
    import pyquantdata.cli as cli_mod
    from pyquantdata.sources import FakeAdapter
    from pyquantdata.sources.base import Bar1dRow, CalendarRow, SecurityRow

    adapter = FakeAdapter(
        securities=[SecurityRow(symbol="sh600000", name="浦发银行", category="stock", exchange="sh")],
        bars=[Bar1dRow(symbol="sh600000", date=date(2026, 9, 1), open=10, high=11, low=9, close=10.5, volume=1, amount=2)],
        calendar={2026: [CalendarRow(trade_date=date(2026, 9, 1), is_open=True)]},
    )
    monkeypatch.setattr("pyquantdata.sources.TdxAdapter", lambda: adapter)
    result = runner.invoke(app, [
        "update", "-d", str(initialized), "--dataset", "all",
        "--start", "2026-09-01", "--end", "2026-09-02",
    ])
    assert result.exit_code == 0, result.output
    assert "securities" in result.output and "update 完成" in result.output
    assert ("open",) in adapter.calls and ("close",) in adapter.calls
    # calendar 年份缺省 = 当前年
    assert any(call[0] == "calendar" for call in adapter.calls)


def test_update_partial_dataset(initialized, monkeypatch):
    from pyquantdata.sources import FakeAdapter
    from pyquantdata.sources.base import SecurityRow

    adapter = FakeAdapter(
        securities=[SecurityRow(symbol="sh600000", name="x", category="stock", exchange="sh")]
    )
    monkeypatch.setattr("pyquantdata.sources.TdxAdapter", lambda: adapter)
    result = runner.invoke(app, ["update", "-d", str(initialized), "--dataset", "securities"])
    assert result.exit_code == 0
    assert ("securities", "cn") in adapter.calls
