from datetime import datetime

import main


def test_scheduled_daily_blocks_weekends():
    assert main._scheduled_daily_allowed(datetime(2026, 10, 3, 9, 30)) is False


def test_scheduled_daily_allows_weekdays():
    assert main._scheduled_daily_allowed(datetime(2026, 10, 5, 9, 30)) is True
