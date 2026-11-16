from datetime import datetime

import main
from concepts import get_concept_definition


def test_scheduled_daily_blocks_weekends():
    assert main._scheduled_daily_allowed(datetime(2026, 10, 3, 9, 30)) is False


def test_scheduled_daily_allows_weekdays():
    assert main._scheduled_daily_allowed(datetime(2026, 10, 5, 9, 30)) is True


def test_supabase_lookup_only_allows_advisors_and_estate_planners():
    expected_roles = {
        "t_and_b": {"advisor", "estate_planner"},
        # PM's role enum has no estate_planner value.
        "people_manager": {"advisor"},
    }
    for concept_id, roles in expected_roles.items():
        definition = get_concept_definition(concept_id)

        assert definition["supabase_user_role_col"] == "role"
        assert set(definition["supabase_user_role_values"]) == roles
        assert definition["supabase_user_active_col"] == "is_active"


def test_daily_coach_advisor_queries_exclude_said_user():
    for concept_id in ("people_manager", "t_and_b"):
        query = get_concept_definition(concept_id)["advisor_query"]

        assert query["role"] == "advisor"
        assert query["isActive"] is True
        assert {"email": {"$ne": "aitouakoursaid@gmail.com"}} in query["$and"]
        assert {"$expr": {"$ne": [{"$toString": "$_id"}, "67b2f8a63782cb04fa0e3e31"]}} in query["$and"]
        assert {"$expr": {"$ne": [{"$toString": "$_id"}, "68ff9661eaeb31f2eeac69fc"]}} in query["$and"]
