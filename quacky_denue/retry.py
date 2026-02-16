from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry(
    operation_name: str,
    fn: Callable[[], T],
    retries: int = 3,
    base_delay_seconds: float = 1.5,
    logger: logging.Logger | None = None,
) -> T:
    """Run fn with bounded exponential backoff retries."""
    attempt = 0
    last_error: Exception | None = None

    while attempt < retries:
        attempt += 1
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - intentionally broad for retries
            last_error = exc
            if logger:
                logger.warning(
                    "%s failed on attempt %s/%s: %s",
                    operation_name,
                    attempt,
                    retries,
                    exc,
                )
            if attempt < retries:
                time.sleep(base_delay_seconds * (2 ** (attempt - 1)))

    assert last_error is not None
    raise last_error
