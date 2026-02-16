from __future__ import annotations

import logging

import pytest

from quacky_denue.retry import retry


def test_retry_success():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("fail once")
        return "ok"

    result = retry("test", flaky, retries=3, base_delay_seconds=0.01)
    assert result == "ok"
    assert len(calls) == 2


def test_retry_exhaustion():
    def always_fail():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        retry("test", always_fail, retries=2, base_delay_seconds=0.01)


def test_retry_logs(caplog):
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_retry")

    def fail_once():
        raise ValueError("fail")

    with pytest.raises(ValueError):
        retry("test", fail_once, retries=2, base_delay_seconds=0.01, logger=logger)

    assert "test failed on attempt 1/2" in caplog.text
