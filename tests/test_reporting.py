from __future__ import annotations

import json
from datetime import datetime, timezone

from quacky_denue.models import FileProcessingStats, PipelineReport
from quacky_denue.reporting import utcnow, write_report


def test_utcnow():
    t = utcnow()
    assert t.tzinfo == timezone.utc


def test_write_report(tmp_path):
    report_path = tmp_path / "report.json"
    report = PipelineReport(
        started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
        discovered_links=10,
        selected_links=10,
        downloaded_files=9,
        processed_files=8,
        expected_files=10,
        total_rows=8000,
        written_rows=7900,
        file_reports=[
            FileProcessingStats(
                source_file="a.zip",
                federation="09",
                snapshot_period="2024",
                total_rows=1000,
                written_rows=990,
                missing_required_columns=["id"],
                unknown_columns=["extra"],
                errors=["warning"],
            )
        ],
        errors=["global error"],
    )

    write_report(report, report_path)
    payload = json.loads(report_path.read_text())
    assert payload["started_at"] == "2024-01-01T00:00:00+00:00"
    assert payload["finished_at"] == "2024-01-01T00:05:00+00:00"
    assert payload["completeness_ratio"] == 0.8
    assert payload["processed_files"] == 8
    assert payload["written_rows"] == 7900
    assert len(payload["file_reports"]) == 1
    assert payload["errors"] == ["global error"]
