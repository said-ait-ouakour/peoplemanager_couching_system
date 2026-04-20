"""Business run date helpers in Europe/London (previous working day)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, time, timezone
from typing import Optional, Tuple, Union
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")


def previous_working_day(d: date) -> date:
    """
    Previous working day relative to `d`.

    - Monday -> previous Friday
    - Sunday -> previous Friday
    - Tuesday-Saturday -> previous calendar day
    """
    weekday = d.weekday()  # Monday=0 ... Sunday=6
    if weekday == 0:  # Monday
        return d - timedelta(days=3)
    if weekday == 6:  # Sunday
        return d - timedelta(days=2)
    return d - timedelta(days=1)


def london_today_date() -> date:
    """Current calendar date in Europe/London."""
    return datetime.now(LONDON).date()


def yesterday_london_date() -> date:
    """Backward-compatible alias: previous working day in London."""
    return previous_working_day(london_today_date())


def yesterday_london_iso() -> str:
    """YYYY-MM-DD for previous working day in London (default run_date everywhere)."""
    return yesterday_london_date().isoformat()


def yesterday_london_weekday_name() -> str:
    """Weekday name for previous working day in London (e.g., Friday)."""
    return yesterday_london_date().strftime("%A")


def _coerce_run_date(run_date: Optional[Union[str, date]]) -> date:
    if run_date is None:
        return yesterday_london_date()
    if isinstance(run_date, date):
        return run_date
    return date.fromisoformat(run_date)


def yesterday_london_utc_bounds(
    run_date: Optional[Union[str, date]] = None,
) -> Tuple[datetime, datetime]:
    """
    Start of yesterday London and start of today London, as UTC-aware datetimes.
    Mongo half-open range on UTC datetimes: {"$gte": start_utc, "$lt": end_utc}.
    """
    y = _coerce_run_date(run_date)
    start_london = datetime.combine(y, time.min, tzinfo=LONDON)
    end_london = start_london + timedelta(days=1)
    return (start_london.astimezone(timezone.utc), end_london.astimezone(timezone.utc))
