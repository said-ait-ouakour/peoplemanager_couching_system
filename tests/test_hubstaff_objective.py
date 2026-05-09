"""Unit tests for ``hubstaff.build_objective_of_the_day`` (no API calls)."""

from dataclasses import replace

from hubstaff import HubstaffSummary, build_objective_of_the_day


def _sample_summary(**kwargs: object) -> HubstaffSummary:
    base = HubstaffSummary(
        idle_over_30=False,
        low_activity=False,
        late_start_after_9am_london=False,
        activity_percentage=50.0,
        latest_idle_minutes=0,
        tracked_seconds=3600.0,
        first_activity_time="2026-05-08T08:00:00Z",
        last_activity_time="2026-05-08T17:00:00Z",
        idle_start_time=None,
        idle_end_time=None,
        raw_activities_count=10,
    )
    return replace(base, **kwargs)


def test_objective_prefers_hubstaff_flags_over_generic() -> None:
    s = _sample_summary(idle_over_30=True, late_start_after_9am_london=True)
    out = build_objective_of_the_day(
        s,
        hubstaff_integration_active=True,
        has_hubstaff_id=True,
        performance_tier="INTERMEDIATE",
        calls_yesterday=3,
        meetings_yesterday=1,
    )
    assert "09:00 London" in out or "30 minutes" in out
    assert "tier INTERMEDIATE" in out


def test_objective_fallback_when_hubstaff_missing_but_expected() -> None:
    out = build_objective_of_the_day(
        None,
        hubstaff_integration_active=True,
        has_hubstaff_id=True,
        performance_tier="LOW",
        calls_yesterday=0,
        meetings_yesterday=0,
    )
    assert "unavailable" in out.lower()
    assert "LOW" in out


def test_objective_when_hubstaff_not_linked() -> None:
    out = build_objective_of_the_day(
        None,
        hubstaff_integration_active=True,
        has_hubstaff_id=False,
        performance_tier="HIGH",
        calls_yesterday=6,
        meetings_yesterday=2,
    )
    assert "Hubstaff" in out


def test_objective_crm_only_when_hubstaff_disabled() -> None:
    out = build_objective_of_the_day(
        None,
        hubstaff_integration_active=False,
        has_hubstaff_id=False,
        performance_tier="LOW",
        calls_yesterday=0,
        meetings_yesterday=0,
    )
    assert "tier LOW" in out
    assert "yesterday shows 0 calls" in out
