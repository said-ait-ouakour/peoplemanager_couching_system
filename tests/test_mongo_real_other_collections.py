"""
Live MongoDB: fetch from calls, NAC, and coaching collections (same queries as ConceptWorkflow).

Uses the first advisor from `mongo_users_collection` + `advisor_query` as `userId` for related rows.
Yesterday calls use London yesterday UTC bounds on `calls_date_col`; NAC/coaching use latest-by-date per user.

Enable with:
  set RUN_MONGO_OTHER_COLLECTIONS_TEST=1
  set MONGO_URI (or MONGO_URL)
  set MONGO_DB_NAME (or MONGO_DB)

Optional:
  set INTEGRATION_CONCEPT=people_manager   # or t_and_b — collection names match concepts (shared Mongo)

Run:
  pytest tests/test_mongo_real_other_collections.py -v -s -m mongo_live

Placeholder OPENAI_API_KEY / VAPI_API_KEY are applied only if unset so SharedServiceConfig() can construct;
this test does not call OpenAI or VAPI.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from concepts import ResolvedConcept, get_concept_definition
from dates_london import yesterday_london_iso
from workflow_engine import ConceptWorkflow, SharedServiceConfig, _shared_mongo_uri_and_db


requires_mongo_other = pytest.mark.skipif(
    os.environ.get("RUN_MONGO_OTHER_COLLECTIONS_TEST", "").strip().lower() not in ("1", "true", "yes"),
    reason=(
        "Set RUN_MONGO_OTHER_COLLECTIONS_TEST=1 and MONGO_URI + MONGO_DB_NAME "
        "to exercise real MongoDB for non-users collections."
    ),
)


def _resolved_concept_for_mongo_only(concept_id: str) -> ResolvedConcept:
    """Build ResolvedConcept for Mongo reads only (Supabase/VAPI placeholders)."""
    raw = get_concept_definition(concept_id)
    uri, db_name = _shared_mongo_uri_and_db()
    return ResolvedConcept(
        concept_id=concept_id,
        mongo_uri=uri,
        mongo_db_name=db_name,
        mongo_users_collection=raw["mongo_users_collection"],
        mongo_calls_collection=raw["mongo_calls_collection"],
        mongo_nac_collection=raw["mongo_nac_collection"],
        mongo_coaching_collection=raw["mongo_coaching_collection"],
        advisor_query=dict(raw["advisor_query"]),
        supabase_lookup_mode=raw["supabase_lookup_mode"],
        supabase_user_table=raw["supabase_user_table"],
        supabase_email_col=raw["supabase_email_col"],
        supabase_phone_col=raw["supabase_phone_col"],
        supabase_user_role_col=raw["supabase_user_role_col"],
        supabase_user_role_value=raw["supabase_user_role_value"],
        supabase_peoplemanager_id_col=raw["supabase_peoplemanager_id_col"],
        calls_user_id_col=raw["calls_user_id_col"],
        calls_date_col=raw["calls_date_col"],
        nac_user_id_col=raw["nac_user_id_col"],
        nac_date_col=raw["nac_date_col"],
        nac_text_col=raw["nac_text_col"],
        nac_order_col=raw["nac_order_col"],
        coaching_user_id_col=raw["coaching_user_id_col"],
        coaching_summary_col=raw["coaching_summary_col"],
        coaching_order_col=raw["coaching_order_col"],
        supabase_advisor_id_col=raw["supabase_advisor_id_col"],
        supabase_meetings_table=raw["supabase_meetings_table"],
        supabase_meetings_advisor_id_col=raw["supabase_meetings_advisor_id_col"],
        supabase_meetings_date_col=raw["supabase_meetings_date_col"],
        meetings_date_match_mode=raw["meetings_date_match_mode"],
        supabase_url="https://mongo-live-test.invalid",
        supabase_service_role_key="mongo-live-test-placeholder",
        vapi_assistant_id="mongo-live-test-placeholder",
        vapi_phone_number_id=None,
    )


@pytest.mark.mongo_live
@requires_mongo_other
def test_fetch_calls_nac_coaching_from_mongo_via_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    except ImportError:
        pass

    if not os.environ.get("OPENAI_API_KEY"):
        monkeypatch.setenv("OPENAI_API_KEY", "mongo-other-collections-test-placeholder")
    if not os.environ.get("VAPI_API_KEY"):
        monkeypatch.setenv("VAPI_API_KEY", "mongo-other-collections-test-placeholder")

    concept_id = (os.environ.get("INTEGRATION_CONCEPT") or "t_and_b").strip()
    concept = _resolved_concept_for_mongo_only(concept_id)
    shared = SharedServiceConfig()
    wf = ConceptWorkflow(concept, shared)
    try:
        wf.mongo.admin.command("ping")

        advisors = wf.get_advisors_from_mongo()
        if not advisors:
            pytest.skip(
                "No Mongo advisors for configured advisor_query — cannot resolve userId for related collections."
            )

        uid = str(advisors[0].get("_id") or "").strip()
        print("uid", uid)
        assert uid, "advisor document must expose _id as str after serialize"

        run_date = yesterday_london_iso()
        calls = wf.fetch_yesterday_customer_calls_from_mongo(uid, run_date)
        nac = wf.fetch_latest_nac_from_mongo(uid)
        coaching = wf.fetch_latest_coaching_from_mongo(uid)

        assert isinstance(calls, list)
        assert nac is None or isinstance(nac, dict)
        assert coaching is None or isinstance(coaching, dict)

        print(
            f"\n[Mongo live — other collections] concept={concept_id} db={concept.mongo_db_name} "
            f"userId={uid!r} run_date_yesterday={run_date!r}\n"
            f"  calls ({concept.mongo_calls_collection}): {len(calls)} rows\n"
            f"  nac ({concept.mongo_nac_collection}): {'1' if nac else '0'} row (latest)\n"
            f"  coaching ({concept.mongo_coaching_collection}): {'1' if coaching else '0'} row (latest)\n"
        )
        if nac and "_id" in nac:
            print(f"  nac _id={nac['_id']}")
        if coaching and "_id" in coaching:
            print(f"  coaching _id={coaching['_id']}")
    finally:
        wf.close()


@pytest.mark.mongo_live
@requires_mongo_other
def test_each_configured_collection_is_readable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Smoke: list_collection_names includes each concept collection; optional find.limit(1) per table."""
    try:
        from dotenv import load_dotenv

        load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    except ImportError:
        pass

    if not os.environ.get("OPENAI_API_KEY"):
        monkeypatch.setenv("OPENAI_API_KEY", "mongo-other-collections-test-placeholder")
    if not os.environ.get("VAPI_API_KEY"):
        monkeypatch.setenv("VAPI_API_KEY", "mongo-other-collections-test-placeholder")

    concept_id = (os.environ.get("INTEGRATION_CONCEPT") or "people_manager").strip()
    concept = _resolved_concept_for_mongo_only(concept_id)
    shared = SharedServiceConfig()
    wf = ConceptWorkflow(concept, shared)
    try:
        wf.mongo.admin.command("ping")
        names = set(wf.mongo_db.list_collection_names())

        coll_specs = [
            ("users / advisors", concept.mongo_users_collection),
            ("customer calls", concept.mongo_calls_collection),
            ("nac", concept.mongo_nac_collection),
            ("coaching", concept.mongo_coaching_collection),
        ]
        missing = [label for label, n in coll_specs if n not in names]
        if missing:
            print(
                f"\n[Mongo live] Warning: collections not found in DB list: {missing}. "
                f"(list may omit some; still trying find.)"
            )

        for label, coll_name in coll_specs:
            col = wf.mongo_db[coll_name]
            one = col.find_one({})
            print(f"  [{label}] {coll_name}: find_one → {'doc' if one else 'empty'}")

        assert wf.mongo_db[concept.mongo_users_collection].find_one({}) is not None, (
            f"Expected at least one document in {concept.mongo_users_collection} for smoke read."
        )
    finally:
        wf.close()
