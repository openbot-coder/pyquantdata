"""etl_checkpoint 语义。"""

from __future__ import annotations

from pyquantdata.etl import failed_checkpoints, list_checkpoints, upsert_checkpoint


def test_upsert_and_list(catalog):
    upsert_checkpoint(catalog, "bars_1d", "cn", "daily", "2026-09-22", 100)
    upsert_checkpoint(catalog, "securities", "cn", "all", "", 5123)
    cps = list_checkpoints(catalog)
    assert [(c["dataset"], c["watermark"], c["rows"]) for c in cps] == [
        ("bars_1d", "2026-09-22", 100),
        ("securities", "", 5123),
    ]
    assert all(c["status"] == "ok" for c in cps)
    assert all(c["updated_at"] for c in cps)


def test_upsert_replaces(catalog):
    upsert_checkpoint(catalog, "bars_1d", "cn", "daily", "2026-09-21", 50)
    upsert_checkpoint(catalog, "bars_1d", "cn", "daily", "2026-09-22", 60)
    cps = list_checkpoints(catalog)
    assert len(cps) == 1
    assert cps[0]["watermark"] == "2026-09-22"
    assert cps[0]["rows"] == 60


def test_failed_filter(catalog):
    upsert_checkpoint(catalog, "bars_1d", "cn", "daily", "x", 0, status="failed")
    upsert_checkpoint(catalog, "calendar", "cn", "2026", "2026-12-31", 244)
    failed = failed_checkpoints(catalog)
    assert [c["dataset"] for c in failed] == ["bars_1d"]
