"""Live Hubstaff flow test: n8n token webhook -> Hubstaff activities API -> computed summary.

Enable:
  set RUN_HUBSTAFF_LIVE_TEST=1
  set HUBSTAFF_TEST_USER_ID=<numeric hubstaff user id>

Required env:
  HUBSTAFF_ORG_ID

Optional env:
  HUBSTAFF_N8N_TOKEN_URL (defaults to hubstaff.py value)
  OVERRIDE_RUN_DATE=YYYY-MM-DD (defaults to yesterday in Europe/London)
  HUBSTAFF_LIVE_TEST_PRINT=0 (disable stdout prints)

Run:
  pytest tests/test_hubstaff_flow.py -v -s -m mongo_live
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from dates_london import yesterday_london_iso
from hubstaff import fetch_hubstaff_for_advisor, summary_to_payload_dict

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

pytestmark = pytest.mark.mongo_live


def _live_enabled() -> bool:
    return os.environ.get("RUN_HUBSTAFF_LIVE_TEST", "").strip() == "1"


def _hubstaff_user_id() -> int | None:
    raw = (os.environ.get("HUBSTAFF_TEST_USER_ID") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _should_print() -> bool:
    return os.environ.get("HUBSTAFF_LIVE_TEST_PRINT", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


@pytest.mark.skipif(not _live_enabled(), reason="Set RUN_HUBSTAFF_LIVE_TEST=1 to run")
def test_hubstaff_live_flow_token_to_activities_to_summary() -> None:
    uid = _hubstaff_user_id()
    if uid is None:
        pytest.skip("Set HUBSTAFF_TEST_USER_ID to a numeric Hubstaff user id")

    run_date = (os.environ.get("OVERRIDE_RUN_DATE") or "").strip() or yesterday_london_iso()
    summary = fetch_hubstaff_for_advisor(run_date, hubstaff_user_id=uid)
    payload = summary_to_payload_dict(summary)

    assert isinstance(summary.low_activity, bool)
    assert isinstance(summary.idle_over_30, bool)
    assert isinstance(summary.late_start_after_9am_london, bool)
    assert isinstance(summary.activity_percentage, float)
    assert isinstance(summary.latest_idle_minutes, int)
    assert isinstance(summary.raw_activities_count, int)

    # Ensure production payload keys are present for downstream workflow_engine usage.
    assert set(payload.keys()) == {
        "idle_over_30",
        "low_activity",
        "late_start_after_9am_london",
        "idle_start_time",
        "idle_end_time",
        "activity_percentage",
        "tracked_hours",
    }

    if _should_print():
        printable = {
            "run_date": run_date,
            "hubstaff_user_id": uid,
            "computed": {
                "low_activity": summary.low_activity,
                "idle_over_30": summary.idle_over_30,
                "late_start_after_9am_london": summary.late_start_after_9am_london,
                "activity_percentage": summary.activity_percentage,
                "latest_idle_minutes": summary.latest_idle_minutes,
                "first_activity_time": summary.first_activity_time,
                "last_activity_time": summary.last_activity_time,
                "idle_start_time": summary.idle_start_time,
                "idle_end_time": summary.idle_end_time,
                "raw_activities_count": summary.raw_activities_count,
            },
            "daily_payload_hubstaff_block": payload,
        }
        print("\n=== HUBSTAFF LIVE FLOW OUTPUT ===", flush=True)
        print(json.dumps(printable, indent=2, ensure_ascii=False), flush=True)
