from datetime import datetime

import main
from concepts import get_concept_definition


def test_scheduled_daily_blocks_weekends():
    assert main._scheduled_daily_allowed(datetime(2026, 10, 3, 9, 30)) is False


def test_scheduled_daily_allows_weekdays():
    assert main._scheduled_daily_allowed(datetime(2026, 10, 5, 9, 30)) is True


def test_daily_coach_advisor_queries_exclude_said_user():
    for concept_id in ("people_manager", "t_and_b"):
        query = get_concept_definition(concept_id)["advisor_query"]

        assert query["role"] == "advisor"
        assert {"email": {"$ne": "aitouakoursaid@gmail.com"}} in query["$and"]
        assert {"$expr": {"$ne": [{"$toString": "$_id"}, "67b2f8a63782cb04fa0e3e31"]}} in query["$and"]
