from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class DownloadLink:
    href: str
    text: str
    federation: str


@dataclass(slots=True)
class FileProcessingStats:
    source_file: str
    federation: str
    snapshot_period: str
    total_rows: int = 0
    written_rows: int = 0
    missing_required_columns: list[str] = field(default_factory=list)
    unknown_columns: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PipelineReport:
    started_at: datetime
    finished_at: datetime | None = None
    discovered_links: int = 0
    selected_links: int = 0
    downloaded_files: int = 0
    processed_files: int = 0
    expected_files: int = 0
    total_rows: int = 0
    written_rows: int = 0
    file_reports: list[FileProcessingStats] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def completeness_ratio(self) -> float:
        if self.expected_files == 0:
            return 1.0
        return self.processed_files / self.expected_files
