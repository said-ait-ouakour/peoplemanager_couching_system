"""Hubstaff activities: token via n8n variables webhook (same as workflow), v2 activities API, n8n-equivalent metrics."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from dates_london import LONDON, yesterday_london_utc_bounds

logger = logging.getLogger("advisor-orchestration")

# Lunch strip matches n8n "Code2": 13:00–14:00 UTC (per segment day).
_LUNCH_START_HOUR_UTC = 13
_LUNCH_END_HOUR_UTC = 14

LOW_ACTIVITY_PCT_THRESHOLD = 20.0
IDLE_SECONDS_THRESHOLD = 30 * 60


@dataclass(frozen=True)
class HubstaffSummary:
    idle_over_30: bool
    low_activity: bool
    late_start_after_9am_london: bool
    activity_percentage: float
    latest_idle_minutes: int
    tracked_seconds: float
    first_activity_time: Optional[str]
    last_activity_time: Optional[str]
    idle_start_time: Optional[str]
    idle_end_time: Optional[str]
    raw_activities_count: int


def hubstaff_configured() -> bool:
    """True when token webhook and org id are available."""
    if os.environ.get("HUBSTAFF_ENABLED", "").strip().lower() in ("0", "false", "no", "off"):
        return False
    url = default_token_url()
    org = (os.environ.get("HUBSTAFF_ORG_ID") or "").strip()
    return bool(url and org)


def default_token_url() -> str:
    return (os.environ.get("HUBSTAFF_N8N_TOKEN_URL") or "https://n8n.aipersonalassistants.ai/webhook/hubstaff-token").strip()


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def refresh_hubstaff_access_token() -> str:
    """
    Fetch latest Hubstaff access token from n8n webhook.
    Response JSON may include the JWT in `access_token` or `value`.
    """
    url = default_token_url()
    if not url:
        raise RuntimeError("Hubstaff token webhook: set HUBSTAFF_N8N_TOKEN_URL")
    r = requests.post(
        url,
        json={},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        for key in ("access_token", "value"):
            token = data.get(key)
            if token and isinstance(token, str) and token.strip():
                return token.strip()
    raise RuntimeError(f"Hubstaff token webhook: unexpected JSON (keys={list(data) if isinstance(data, dict) else type(data)})")


def time_slot_for_run_date(run_date: str) -> Tuple[str, str]:
    """Full Europe/London calendar day for `run_date` as UTC ISO interval [start, end)."""
    lo, hi = yesterday_london_utc_bounds(run_date)
    return (lo.isoformat().replace("+00:00", "Z"), hi.isoformat().replace("+00:00", "Z"))


def _filter_activities_lunch_utc(activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Same cases as n8n \"Code2\": strip 13:00–14:00 UTC on the segment start's UTC calendar day."""
    filtered: List[Dict[str, Any]] = []
    for a in activities:
        raw_start = a.get("starts_at")
        if not raw_start:
            continue
        start = datetime.fromisoformat(str(raw_start).replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        duration_ms = float(a.get("tracked") or 0) * 1000
        end = start + timedelta(milliseconds=duration_ms)
        s = start.astimezone(timezone.utc)
        lunch_start = datetime(
            s.year, s.month, s.day, _LUNCH_START_HOUR_UTC, 0, 0, tzinfo=timezone.utc
        )
        lunch_end = datetime(
            s.year, s.month, s.day, _LUNCH_END_HOUR_UTC, 0, 0, tzinfo=timezone.utc
        )
        st = start.timestamp()
        en = end.timestamp()
        ls = lunch_start.timestamp()
        le = lunch_end.timestamp()

        if en <= ls:
            filtered.append(dict(a))
            continue
        if st >= le:
            filtered.append(dict(a))
            continue
        if st >= ls and en <= le:
            continue
        if st < ls and en > ls and en <= le:
            new_duration = (ls - st) / 1000.0
            na = dict(a)
            na["tracked"] = new_duration
            filtered.append(na)
            continue
        if st >= ls and st < le and en > le:
            new_duration = (en - le) / 1000.0
            na = dict(a)
            na["starts_at"] = datetime.fromtimestamp(le, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            na["tracked"] = new_duration
            filtered.append(na)
            continue
        if st < ls and en > le:
            before_duration = (ls - st) / 1000.0
            after_duration = (en - le) / 1000.0
            na1 = dict(a)
            na1["tracked"] = before_duration
            filtered.append(na1)
            na2 = dict(a)
            na2["starts_at"] = datetime.fromtimestamp(le, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            na2["tracked"] = after_duration
            filtered.append(na2)
            continue
    return filtered


def _parse_iso(ts: str) -> datetime:
    d = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d


def is_late_start_after_9am_london(first_activity_iso: Optional[str]) -> bool:
    """Late if first activity local time is strictly after 09:00 in Europe/London."""
    if not first_activity_iso:
        return False
    dt = _parse_iso(first_activity_iso).astimezone(LONDON)
    cutoff = dt.replace(hour=9, minute=0, second=0, microsecond=0)
    return dt > cutoff


def compute_hubstaff_summary(activities_in: List[Dict[str, Any]]) -> HubstaffSummary:
    """
    Same rules as n8n "All users activity" for one user's activity list (already filtered by user_ids).
    """
    activities = sorted(
        _filter_activities_lunch_utc(list(activities_in)),
        key=lambda x: _parse_iso(str(x.get("starts_at", ""))).timestamp(),
    )

    tracked_seconds = 0.0
    overall_sum = 0.0
    first_activity_time: Optional[str] = None
    last_normal_end: Optional[str] = None

    current_idle_start: Optional[float] = None
    current_idle_seconds = 0.0
    latest_idle_start: Optional[str] = None
    latest_idle_end: Optional[str] = None
    latest_idle_seconds = 0.0

    for a in activities:
        start_ms = _parse_iso(str(a.get("starts_at", ""))).timestamp() * 1000
        duration_ms = float(a.get("tracked") or 0) * 1000
        end_ms = start_ms + duration_ms
        tracked_seconds += float(a.get("tracked") or 0)
        overall_sum += float(a.get("overall") or 0)

        if first_activity_time is None:
            first_activity_time = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )

        time_type = str(a.get("time_type") or "")
        if time_type == "normal":
            last_normal_end = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )

        if time_type == "idle":
            if current_idle_start is None:
                current_idle_start = start_ms
                current_idle_seconds = 0.0
            current_idle_seconds += float(a.get("tracked") or 0)
        else:
            if current_idle_start is not None:
                latest_idle_start = datetime.fromtimestamp(current_idle_start / 1000, tz=timezone.utc).isoformat().replace(
                    "+00:00", "Z"
                )
                latest_idle_end = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat().replace(
                    "+00:00", "Z"
                )
                latest_idle_seconds = current_idle_seconds
            current_idle_start = None
            current_idle_seconds = 0.0

    if current_idle_start is not None and activities:
        last_block = activities[-1]
        lb_start = _parse_iso(str(last_block.get("starts_at", ""))).timestamp() * 1000
        lb_end = lb_start + float(last_block.get("tracked") or 0) * 1000
        latest_idle_start = datetime.fromtimestamp(current_idle_start / 1000, tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        latest_idle_end = datetime.fromtimestamp(lb_end / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        latest_idle_seconds = current_idle_seconds

    activity_pct = (overall_sum / tracked_seconds) * 100.0 if tracked_seconds > 0 else 0.0
    active = tracked_seconds > 0
    idle_over_30 = latest_idle_seconds > IDLE_SECONDS_THRESHOLD
    low_activity = active and activity_pct < LOW_ACTIVITY_PCT_THRESHOLD
    late = is_late_start_after_9am_london(first_activity_time)

    return HubstaffSummary(
        idle_over_30=idle_over_30,
        low_activity=low_activity,
        late_start_after_9am_london=late,
        activity_percentage=round(activity_pct, 1),
        latest_idle_minutes=int(round(latest_idle_seconds / 60.0)) if latest_idle_seconds else 0,
        tracked_seconds=tracked_seconds,
        first_activity_time=first_activity_time,
        last_activity_time=last_normal_end,
        idle_start_time=latest_idle_start,
        idle_end_time=latest_idle_end,
        raw_activities_count=len(activities_in),
    )


def summary_to_payload_dict(s: HubstaffSummary) -> Dict[str, Any]:
    """Fields passed to VAPI daily_payload Hubstaff block (flags + latest idle window only)."""
    return {
        "idle_over_30": s.idle_over_30,
        "low_activity": s.low_activity,
        "late_start_after_9am_london": s.late_start_after_9am_london,
        "idle_start_time": s.idle_start_time,
        "idle_end_time": s.idle_end_time,
    }


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    retry=retry_if_exception_type(Exception),
)
def fetch_hubstaff_activities(
    organization_id: str,
    hubstaff_user_id: int,
    time_slot_start: str,
    time_slot_stop: str,
    access_token: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """GET v2/organizations/{id}/activities with include=users. Returns (activities, users)."""
    url = f"https://api.hubstaff.com/v2/organizations/{organization_id}/activities"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    params = {
        "time_slot[start]": time_slot_start,
        "time_slot[stop]": time_slot_stop,
        "user_ids": str(hubstaff_user_id),
        "include": "users",
    }
    r = requests.get(url, headers=headers, params=params, timeout=120)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        return [], []
    act = data.get("activities") or []
    users = data.get("users") or []
    if not isinstance(act, list):
        act = []
    if not isinstance(users, list):
        users = []
    return act, users


def fetch_hubstaff_for_advisor(run_date: str, hubstaff_user_id: int) -> HubstaffSummary:
    """Refresh token (n8n webhook), then fetch activities for run_date London day, compute metrics."""
    org = (os.environ.get("HUBSTAFF_ORG_ID") or "").strip()
    if not org:
        raise RuntimeError("HUBSTAFF_ORG_ID is not set")
    token = refresh_hubstaff_access_token()
    t0, t1 = time_slot_for_run_date(run_date)
    activities, _users = fetch_hubstaff_activities(org, hubstaff_user_id, t0, t1, token)
    return compute_hubstaff_summary(activities)
