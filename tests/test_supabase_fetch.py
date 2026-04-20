"""
Live Supabase (PM + TB): fetch the first N rows from each project’s `users` table.

Enable with `RUN_DATA_INTEGRATION_TESTS=1` (same gate as other integration tests).

Requires:
  PM_SUPABASE_URL, PM_SUPABASE_SERVICE_ROLE_KEY
  TB_SUPABASE_URL, TB_SUPABASE_SERVICE_ROLE_KEY
  OPENAI_API_KEY, VAPI_API_KEY, VAPI_PM_ASSISTANT_ID, VAPI_TB_ASSISTANT_ID

`.env` is loaded in `tests/conftest.py`. Integration tests skip `conftest` Mongo monkeypatch so real URLs are used.

Optional (meetings join test): `SUPABASE_MEETINGS_USER_FK_COL` — column on `meetings` that stores the Supabase `users`
primary key. If unset, the test uses `concepts.py` → `supabase_meetings_advisor_id_col` (usually `advisor_id`).
Set to `user_id` when your schema links meetings that way.

Run:
  RUN_DATA_INTEGRATION_TESTS=1 pytest tests/test_supabase_fetch.py -v -s -m integration
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import pytest

from workflow_engine import ConceptWorkflow, SharedServiceConfig, resolve_concept_from_env


requires_integration = pytest.mark.skipif(
    os.environ.get("RUN_DATA_INTEGRATION_TESTS", "").strip().lower() not in ("1", "true", "yes"),
    reason="Set RUN_DATA_INTEGRATION_TESTS=1 and PM/TB Supabase + VAPI/OpenAI env vars.",
)

_FETCH_LIMIT = 102
_MEETINGS_SAMPLE_LIMIT = 25


def _meetings_user_fk_column(wf: ConceptWorkflow) -> str:
    """meetings.<col> = users.<pk>; env override, else same column as `fetch_meetings_yesterday_count_from_supabase`."""
    raw = (os.environ.get("SUPABASE_MEETINGS_USER_FK_COL") or "").strip()
    return raw or wf.concept.supabase_meetings_advisor_id_col


def _meetings_rows_for_supabase_user_id(
    wf: ConceptWorkflow,
    supabase_users_id: Any,
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    fk = wf.concept.supabase_meetings_advisor_id_col
    tbl = wf.concept.supabase_meetings_table
    key = supabase_users_id if supabase_users_id is None else str(supabase_users_id).strip()
    res = wf.supabase.table(tbl).select("*").eq(fk, key).limit(limit).execute()
    return list(res.data or [])


@requires_integration
@pytest.mark.integration
def test_live_mongo_advisors_exist_in_pm_supabase_users() -> None:
    """Fetch advisors from Mongo `users` and verify existence in PM Supabase `users` via peoplemanager_id."""
    shared = SharedServiceConfig()
    wf_pm: ConceptWorkflow | None = None
    try:
        wf_pm = ConceptWorkflow(resolve_concept_from_env("people_manager"), shared)
        table_pm = wf_pm.concept.supabase_user_table
        pm_id_col = wf_pm.concept.supabase_peoplemanager_id_col
        pm_role_col = wf_pm.concept.supabase_user_role_col
        pm_role_val = wf_pm.concept.supabase_user_role_value

        mongo_advisors = wf_pm.get_advisors_from_mongo()[:_FETCH_LIMIT]
        assert mongo_advisors, "No Mongo advisors returned for people_manager advisor query."

        mongo_ids: List[str] = []
        for adv in mongo_advisors:
            mongo_id = str(adv.get("_id") or "").strip()
            if mongo_id:
                mongo_ids.append(mongo_id)

        assert mongo_ids, "No valid Mongo advisor ids found."

        # Explicit AND filter: role = advisor AND peoplemanager_id in Mongo advisor ids.
        pm_rows_res = (
            wf_pm.supabase.table(table_pm)
            .select(pm_id_col)
            .eq(pm_role_col, pm_role_val)
            .in_(pm_id_col, mongo_ids)
            .execute()
        )
        pm_rows = list(pm_rows_res.data or [])
        found_ids = sorted(
            {
                str(row.get(pm_id_col)).strip()
                for row in pm_rows
                if row.get(pm_id_col) is not None and str(row.get(pm_id_col)).strip()
            }
        )
        found_set = set(found_ids)
        missing_ids = [mid for mid in mongo_ids if mid not in found_set]
        checks: List[Dict[str, Any]] = [
            {"mongo_user_id": mid, "exists_in_pm_users": mid in found_set}
            for mid in mongo_ids
        ]

        result: Dict[str, Any] = {
            "fetch_limit": _FETCH_LIMIT,
            "mongo_advisors_checked": len(checks),
            "pm_supabase": {
                "table": table_pm,
                "project_env": "PM_SUPABASE_URL",
                "matching_column": pm_id_col,
                "role_filter": {pm_role_col: pm_role_val},
            },
            "found_count": len(found_ids),
            "found_mongo_user_ids": found_ids,
            "missing_count": len(missing_ids),
            "missing_mongo_user_ids": missing_ids,
            "checks": checks,
        }

        print("\n=== Mongo advisors existence in PM Supabase users ===\n")
        print(json.dumps(result, indent=2, default=str))

        assert checks, "No valid Mongo advisor ids found to check against PM Supabase."
        assert found_ids, (
            f"No Mongo advisors were found in PM Supabase users by {pm_id_col}. "
            "Check PM project/env mapping and peoplemanager_id sync."
        )
    finally:
        if wf_pm is not None:
            wf_pm.close()


@requires_integration
@pytest.mark.integration
def test_live_fetch_meetings_where_user_fk_matches_users_pk() -> None:
    """
    Fetch `meetings` rows where meetings.<FK> equals the Supabase users primary key (`id` / supabase_advisor_id_col).

    Join semantics: meetings.<FK> = users.id. Default FK column matches `concepts.supabase_meetings_advisor_id_col`
    (typically `advisor_id`, same as `fetch_meetings_yesterday_count_from_supabase`). Set
    `SUPABASE_MEETINGS_USER_FK_COL=user_id` when meetings link via `user_id`.
    """
    shared = SharedServiceConfig()
    wf_pm: ConceptWorkflow | None = None
    wf_tb: ConceptWorkflow | None = None
    try:
        wf_pm = ConceptWorkflow(resolve_concept_from_env("people_manager"), shared)
        wf_tb = ConceptWorkflow(resolve_concept_from_env("t_and_b"), shared)

        def _sample_for_project(wf: ConceptWorkflow, label: str) -> Dict[str, Any]:
            u_table = wf.concept.supabase_user_table
            id_col = wf.concept.supabase_advisor_id_col
            user_res = wf.supabase.table(u_table).select(id_col).limit(1).execute()
            users_rows = list(user_res.data or [])
            if not users_rows:
                pytest.skip(f"{label}: no rows in {u_table} — cannot resolve supabase_users_id.")
            print("users_rows", users_rows)
            supabase_users_id = users_rows[0].get(id_col)
            assert supabase_users_id is not None and str(supabase_users_id).strip() != "", (
                f"{label}: users row missing {id_col}"
            )

            meetings = _meetings_rows_for_supabase_user_id(
                wf,
                supabase_users_id,
                limit=_MEETINGS_SAMPLE_LIMIT,
            )
            fk_col = _meetings_user_fk_column(wf)
            return {
                "concept": wf.concept.concept_id,
                "users_table": u_table,
                "users_pk_column": id_col,
                "supabase_users_id": supabase_users_id,
                "meetings_table": wf.concept.supabase_meetings_table,
                "meetings_user_fk_column": fk_col,
                "meetings_rows_returned": len(meetings),
                "meetings_sample": meetings,
            }

        out_pm = _sample_for_project(wf_pm, "PM")
        out_tb = _sample_for_project(wf_tb, "TB")

        pm_fk = str(out_pm.get("meetings_user_fk_column") or "")
        print(
            "\n=== Live Supabase meetings fetch (meetings.%s = users.%s) ===\n"
            % (pm_fk, wf_pm.concept.supabase_advisor_id_col)
        )
        print(json.dumps({"pm_supabase": out_pm, "tb_supabase": out_tb}, indent=2, default=str))

        assert isinstance(out_pm["meetings_sample"], list)
        assert isinstance(out_tb["meetings_sample"], list)
    finally:
        if wf_pm is not None:
            wf_pm.close()
        if wf_tb is not None:
            wf_tb.close()
