"""Unit tests: Mongo advisors, Supabase phone, Mongo calls/NAC/coaching."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import mongomock

from dates_london import yesterday_london_iso, yesterday_london_utc_bounds
from nac_feedback import extract_nac_feedback_texts
from tests.fake_supabase import FakeSupabaseClient
from workflow_engine import (
    AdvisorRecord,
    ConceptWorkflow,
    SharedServiceConfig,
    _compute_performance_tier,
)


@patch("workflow_engine.MongoClient", lambda *_a, **_k: mongomock.MongoClient())
def test_get_advisors_from_mongo_full_documents_and_query(sample_concept) -> None:
    wf = ConceptWorkflow(sample_concept, SharedServiceConfig())
    col = wf.mongo_db[sample_concept.mongo_users_collection]
    ins = col.insert_many(
        [
            {"email": "a@x.com", "name": "A", "role": "advisor", "extra": "noise"},
            {"email": "b@x.com", "name": "B", "role": "user"},
        ]
    )
    rows = wf.get_advisors_from_mongo()
    assert len(rows) == 1
    assert rows[0]["email"] == "a@x.com"
    assert rows[0]["_id"] == str(ins.inserted_ids[0])
    assert rows[0].get("extra") == "noise"


@patch("workflow_engine.MongoClient", lambda *_a, **_k: mongomock.MongoClient())
def test_map_advisors_loads_phone_via_mongo_id_on_supabase_peoplemanager_id(sample_concept) -> None:
    wf = ConceptWorkflow(sample_concept, SharedServiceConfig())
    col = wf.mongo_db[sample_concept.mongo_users_collection]
    ins = col.insert_one(
        {
            "email": "adv@x.com",
            "name": "Ann",
            "role": "advisor",
        }
    )
    mongo_id = str(ins.inserted_id)

    fake = FakeSupabaseClient(
        {
            "users": [
                {
                    "peoplemanager_id": mongo_id,
                    "phone": "+1 415 555 2671",
                    "id": "pm-supabase-advisor-1",
                    "role": "advisor",
                },
            ]
        }
    )
    wf._tls.supabase = fake  # type: ignore[attr-defined]

    mongo_rows = wf.get_advisors_from_mongo()
    mapped = wf.map_advisors_to_supabase_phone(mongo_rows)
    assert len(mapped) == 1
    assert mapped[0].email == "adv@x.com"
    assert mapped[0].peoplemanager_id == mongo_id
    assert mapped[0].mongo_user_id == mongo_id
    assert mapped[0].e164_phone == "+14155552671"
    assert mapped[0].supabase_advisor_id == "pm-supabase-advisor-1"
    assert mapped[0].mongo_document.get("role") == "advisor"


@patch("workflow_engine.MongoClient", lambda *_a, **_k: mongomock.MongoClient())
def test_yesterday_calls_nac_coaching_from_mongo(sample_concept) -> None:
    wf = ConceptWorkflow(sample_concept, SharedServiceConfig())
    col = wf.mongo_db[sample_concept.mongo_users_collection]
    ins = col.insert_one({"email": "x@y.com", "name": "X", "role": "advisor"})
    uid = str(ins.inserted_id)
    run_date = yesterday_london_iso()
    lo, _hi = yesterday_london_utc_bounds()

    wf.mongo_db[sample_concept.mongo_calls_collection].insert_many(
        [
            {"userId": uid, "date": lo, "feedback": "First NAC"},
            {"userId": uid, "date": lo, "summary": "Second row"},
            {"userId": uid, "date": datetime(2000, 1, 1, tzinfo=timezone.utc), "feedback": "old"},
        ]
    )

    wf.mongo_db[sample_concept.mongo_nac_collection].insert_one(
        {"userId": uid, "date": run_date, "nac_feedback": "Latest narrative"}
    )

    wf.mongo_db[sample_concept.mongo_coaching_collection].insert_many(
        [
            {"userId": uid, "date": run_date, "previous_call_summary": "Coach A"},
            {"userId": uid, "date": "1999-01-01", "previous_call_summary": "old"},
        ]
    )

    ycalls = wf.fetch_yesterday_customer_calls_from_mongo(uid, run_date)
    assert len(ycalls) == 2

    last_call = wf.last_customer_call_yesterday(ycalls, run_date)
    assert last_call is not None

    nac = wf.fetch_latest_nac_from_mongo(uid)
    assert nac is not None
    assert nac.get("nac_feedback") == "Latest narrative"

    coaching = wf.fetch_latest_coaching_from_mongo(uid)
    assert coaching is not None
    assert coaching.get("previous_call_summary") == "Coach A"

    nac_texts = extract_nac_feedback_texts(ycalls)
    assert nac_texts == ["First NAC", "Second row"]


def test_yesterday_london_iso_shape() -> None:
    import re

    d = yesterday_london_iso()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", d)


@patch("workflow_engine.MongoClient", lambda *_a, **_k: mongomock.MongoClient())
def test_build_daily_payload_vapi_contract(sample_concept) -> None:
    wf = ConceptWorkflow(sample_concept, SharedServiceConfig())
    advisor = AdvisorRecord(
        mongo_user_id="u1",
        advisor_name="Jane Advisor",
        email="j@x.com",
        e164_phone="+441234567890",
        peoplemanager_id="u1",
        supabase_advisor_id="adv-1",
        mongo_document={"_id": "u1", "email": "j@x.com"},
    )
    nac = {
        "userId": "u1",
        "KeyCoachingRecommendations": {"reportText": "Coach the close."},
    }
    coaching = {"userId": "u1", "previous_call_summary": "Last call recap."}

    payload = wf.build_daily_payload(advisor, 1, 1, nac, coaching)

    assert payload["Advisor Name"] == "Jane Advisor"
    assert payload["Performance Tier"] == "INTERMEDIATE"
    assert payload["Calls yesterday"] == 1
    assert payload["Meetings yesterday"] == 1
    assert payload["Feedback Summary"] == "Coach the close."
    assert payload["Coaching Context"] == "CRM PM"
    assert payload["Memory"] == "Last call recap."
    assert set(payload.keys()) == {
        "Advisor Name",
        "Performance Tier",
        "Calls yesterday",
        "Meetings yesterday",
        "Feedback Summary",
        "Coaching Context",
        "Memory",
    }


@patch("workflow_engine.MongoClient", lambda *_a, **_k: mongomock.MongoClient())
def test_meetings_yesterday_count_supabase(sample_concept) -> None:
    wf = ConceptWorkflow(sample_concept, SharedServiceConfig())
    day = yesterday_london_iso()
    lo, _hi = yesterday_london_utc_bounds()
    fake = FakeSupabaseClient(
        {
            "meetings": [
                {"advisor_id": "supa-1", "meeting_date": lo.isoformat()},
                {"advisor_id": "supa-1", "meeting_date": lo.isoformat()},
                {"advisor_id": "supa-2", "meeting_date": "2000-01-01T00:00:00+00:00"},
            ]
        }
    )
    wf._tls.supabase = fake  # type: ignore[attr-defined]

    assert wf.fetch_meetings_yesterday_count_from_supabase("supa-1", day) == 2
    assert wf.fetch_meetings_yesterday_count_from_supabase("supa-2", day) == 0


def test_compute_performance_tier_rules() -> None:
    assert _compute_performance_tier(0, 0) == "LOW"
    assert _compute_performance_tier(1, 0) == "LOW"
    assert _compute_performance_tier(2, 0) == "INTERMEDIATE"
    assert _compute_performance_tier(3, 1) == "INTERMEDIATE"
    assert _compute_performance_tier(1, 1) == "INTERMEDIATE"
    assert _compute_performance_tier(5, 0) == "HIGH"
    assert _compute_performance_tier(4, 2) == "HIGH"
    assert _compute_performance_tier(1, 2) == "HIGH"
