"""Shared fixtures: env defaults, resolved concept aligned with concepts.people_manager."""

from __future__ import annotations

import pytest

from concepts import ResolvedConcept


def pytest_configure(config: pytest.Config) -> None:
    """Load project `.env` so integration tests see MONGO_URI without manual export."""
    try:
        from pathlib import Path

        from dotenv import load_dotenv

        root = Path(__file__).resolve().parents[1]
        load_dotenv(root / ".env")
    except ImportError:
        pass


@pytest.fixture(autouse=True)
def _minimal_service_env(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    # Real MongoDB live tests use MONGO_URI / MONGO_DB_NAME from the environment — do not override.
    if request.node.get_closest_marker("mongo_live"):
        return
    # Integration tests hit real Mongo + Supabase — keep .env values (pytest_configure loads dotenv).
    if request.node.get_closest_marker("integration"):
        return

    monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-role")
    monkeypatch.setenv("PM_SUPABASE_URL", "https://pm.test.supabase.co")
    monkeypatch.setenv("PM_SUPABASE_SERVICE_ROLE_KEY", "pm-test-service-role")
    monkeypatch.setenv("TB_SUPABASE_URL", "https://tb.test.supabase.co")
    monkeypatch.setenv("TB_SUPABASE_SERVICE_ROLE_KEY", "tb-test-service-role")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("VAPI_API_KEY", "test-vapi")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4.1-mini")
    monkeypatch.setenv("VAPI_BASE_URL", "https://api.vapi.ai")
    monkeypatch.setenv("MONGO_URI", "mongodb://localhost:27017")
    monkeypatch.setenv("MONGO_DB_NAME", "test_db")


@pytest.fixture
def sample_concept() -> ResolvedConcept:
    return ResolvedConcept(
        concept_id="people_manager",
        mongo_uri="mongodb://localhost:27017",
        mongo_db_name="test_db",
        mongo_users_collection="users",
        mongo_calls_collection="customernacfeedbacks_tb",
        mongo_nac_collection="nacfeedbacks_tb",
        mongo_coaching_collection="daily_users_coaching_vapi_tb",
        advisor_query={"role": "advisor"},
        supabase_lookup_mode="peoplemanager_id",
        supabase_user_table="users",
        supabase_email_col="email",
        supabase_phone_col="phone",
        supabase_user_role_col="role",
        supabase_user_role_value="advisor",
        supabase_peoplemanager_id_col="peoplemanager_id",
        calls_user_id_col="userId",
        calls_date_col="date",
        nac_user_id_col="userId",
        nac_date_col="date",
        nac_text_col="nac_feedback",
        nac_order_col="date",
        coaching_user_id_col="userId",
        coaching_summary_col="previous_call_summary",
        coaching_order_col="date",
        supabase_advisor_id_col="id",
        supabase_meetings_table="meetings",
        supabase_meetings_advisor_id_col="advisor_id",
        supabase_meetings_date_col="meeting_date",
        meetings_date_match_mode="utc_bounds",
        supabase_url="https://pm.test.supabase.co",
        supabase_service_role_key="pm-test-service-role",
        vapi_assistant_id="asst_test",
        vapi_phone_number_id=None,
    )


@pytest.fixture
def tb_concept() -> ResolvedConcept:
    """T&B: phone lookup by email; separate TB Supabase project (see TB_SUPABASE_* env in tests)."""
    return ResolvedConcept(
        concept_id="t_and_b",
        mongo_uri="mongodb://localhost:27017",
        mongo_db_name="test_db",
        mongo_users_collection="users",
        mongo_calls_collection="customernacfeedbacks_tb",
        mongo_nac_collection="nacfeedbacks_tb",
        mongo_coaching_collection="daily_users_coaching_vapi_tb",
        advisor_query={"role": "advisor"},
        supabase_lookup_mode="email",
        supabase_user_table="users",
        supabase_email_col="email",
        supabase_phone_col="phone",
        supabase_user_role_col="role",
        supabase_user_role_value="advisor",
        supabase_peoplemanager_id_col="peoplemanager_id",
        calls_user_id_col="userId",
        calls_date_col="date",
        nac_user_id_col="userId",
        nac_date_col="date",
        nac_text_col="nac_feedback",
        nac_order_col="date",
        coaching_user_id_col="userId",
        coaching_summary_col="previous_call_summary",
        coaching_order_col="date",
        supabase_advisor_id_col="id",
        supabase_meetings_table="meetings",
        supabase_meetings_advisor_id_col="advisor_id",
        supabase_meetings_date_col="meeting_date",
        meetings_date_match_mode="utc_bounds",
        supabase_url="https://tb.test.supabase.co",
        supabase_service_role_key="tb-test-service-role",
        vapi_assistant_id="asst_tb_test",
        vapi_phone_number_id=None,
    )
