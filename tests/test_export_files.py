"""导出文件层：路径安全、落盘、job 记录。"""

from __future__ import annotations

import pytest
import pyarrow.parquet as paparquet

from pyquantdata.export.files import (
    ExportFormatError,
    ExportPathError,
    get_export_job,
    new_job_id,
    record_export_job,
    resolve_export_target,
    spec_hash,
    write_export,
)


def test_new_job_id_shape():
    jid = new_job_id()
    assert len(jid) == 12
    assert all(c in "0123456789abcdef" for c in jid)


def test_spec_hash_stable_and_distinct():
    a = spec_hash({"sql": "SELECT 1", "fmt": "csv"})
    assert a == spec_hash({"fmt": "csv", "sql": "SELECT 1"})  # 键序无关
    assert a != spec_hash({"sql": "SELECT 2", "fmt": "csv"})


def test_resolve_target_ok(tmp_path):
    exports = tmp_path / "exports"
    target = resolve_export_target(exports, "abc123", "out.csv")
    assert target == exports.resolve() / "abc123" / "out.csv"


@pytest.mark.parametrize(
    "job_id,filename",
    [
        ("Bad!ID", "ok.csv"),  # job_id 非法字符
        ("", "ok.csv"),
        ("a" * 100, "ok.csv"),  # 超长
        ("okjob", "../evil.csv"),  # 路径穿越
        ("okjob", "..\\evil.csv"),  # Windows 分隔符
        ("okjob", "sub/dir/x.csv"),  # 斜杠
        ("okjob", ""),
        ("okjob", "a" * 200),  # 超长文件名
    ],
)
def test_resolve_target_rejects(tmp_path, job_id, filename):
    with pytest.raises(ExportPathError):
        resolve_export_target(tmp_path / "exports", job_id, filename)


def test_resolve_target_dotdot_filename_rejected(tmp_path):
    with pytest.raises(ExportPathError):
        resolve_export_target(tmp_path / "exports", "okjob", "..")


def test_write_export_csv(tmp_path):
    import pyarrow as pa

    tbl = pa.table({"a": [1, 2], "b": ["x", "y"]})
    result = write_export(tbl, "csv", tmp_path / "out.csv")
    assert result.rows == 2
    assert result.path.exists()
    text = result.path.read_text(encoding="utf-8")
    # pyarrow 默认按 RFC4180 加引号（"a","b"）——断言语义而非引号风格
    assert '"a","b"' in text and '1,"x"' in text


def test_write_export_parquet(tmp_path):
    import pyarrow as pa

    tbl = pa.table({"a": [1, 2, 3]})
    result = write_export(tbl, "parquet", tmp_path / "out.parquet")
    back = paparquet.read_table(result.path)
    assert back.column("a").to_pylist() == [1, 2, 3]


def test_write_export_bad_format(tmp_path):
    import pyarrow as pa

    with pytest.raises(ExportFormatError, match="落盘格式"):
        write_export(pa.table({"a": [1]}), "pandas", tmp_path / "x.pandas")


def test_record_and_get_job(catalog):
    record_export_job(
        catalog, "job123", fmt="csv", path="exports/job123/out.csv",
        rows=10, bytes_written=128,
    )
    job = get_export_job(catalog, "job123")
    assert job["job_id"] == "job123"
    assert job["fmt"] == "csv"
    assert job["rows"] == 10
    assert job["bytes"] == 128
    assert job["state"] == "done"
    assert job["finished_at"] is not None
    assert get_export_job(catalog, "missing") is None
