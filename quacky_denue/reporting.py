from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from quacky_denue.models import PipelineReport


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def write_report(report: PipelineReport, path: Path) -> None:
    payload = asdict(report)
    payload["started_at"] = report.started_at.isoformat()
    payload["finished_at"] = report.finished_at.isoformat() if report.finished_at else None
    payload["completeness_ratio"] = round(report.completeness_ratio(), 4)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
