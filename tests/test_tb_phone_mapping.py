"""
T&B: Mongo `users` (advisors) + phone from T&B Supabase `users` matched by email.
`userId` elsewhere = str(Mongo users._id).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import mongomock
import pytest

from workflow_engine import (
    ConceptWorkflow,
    SharedServiceConfig,
    resolve_concept_from_env,
    resolve_supabase_credentials_for_concept,
)


def test_resolve_supabase_credentials_prefers_tb_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TB_SUPABASE_URL", "https://tb-project.supabase.co")
    monkeypatch.setenv("TB_SUPABASE_SERVICE_ROLE_KEY", "tb-secret-key")
    monkeypatch.delenv("SUPABASE_URL", raising=False)

    url, key = resolve_supabase_credentials_for_concept("t_and_b")
    assert url == "https://tb-project.supabase.co"
    assert key == "tb-secret-key"


def test_resolve_supabase_credentials_tb_falls_back_to_shared_supabase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TB_SUPABASE_URL", raising=False)
    monkeypatch.delenv("TB_SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.setenv("SUPABASE_URL", "https://fallback.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "fallback-key")

    url, key = resolve_supabase_credentials_for_concept("t_and_b")
    assert url == "https://fallback.supabase.co"
    assert key == "fallback-key"


@patch("workflow_engine.MongoClient", lambda *_a, **_k: mongomock.MongoClient())
def test_tb_maps_mongo_advisor_email_to_phone_via_tb_supabase(tb_concept) -> None:
    """Insert advisor in Mongo; Supabase users row matched by email returns phone → E.164 + mongo _id linkage."""
    from tests.fake_supabase import FakeSupabaseClient

    wf = ConceptWorkflow(tb_concept, SharedServiceConfig())
    col = wf.mongo_db[tb_concept.mongo_users_collection]
    ins = col.insert_one(
        {
            "email": "advisor.tb@example.com",
            "name": "TB Advisor",
            "role": "advisor",
        }
    )
    mongo_id = str(ins.inserted_id)

    fake = FakeSupabaseClient(
        {
            "users": [
                {
                    "email": "advisor.tb@example.com",
                    "phone": "+44 20 7946 0958",
                    "id": "tb-supabase-advisor-1",
                    "role": "advisor",
                }
            ]
        }
    )
    wf._tls.supabase = fake  # type: ignore[attr-defined]

    mongo_rows = wf.get_advisors_from_mongo()
    mapped = wf.map_advisors_to_supabase_phone(mongo_rows)

    assert len(mapped) == 1
    assert mapped[0].email == "advisor.tb@example.com"
    assert mapped[0].mongo_user_id == mongo_id
    assert mapped[0].peoplemanager_id == mongo_id
    assert mapped[0].e164_phone == "+442079460958"
    assert mapped[0].supabase_advisor_id == "tb-supabase-advisor-1"


@patch("workflow_engine.create_client")
def test_tb_supabase_client_uses_tb_url_and_key_from_concept(mock_create: MagicMock, tb_concept) -> None:
    """When the real client is created, it must use ResolvedConcept TB credentials."""
    mock_create.return_value = MagicMock()

    wf = ConceptWorkflow(tb_concept, SharedServiceConfig())
    _ = wf.supabase

    mock_create.assert_called_once_with(
        "https://tb.test.supabase.co",
        "tb-test-service-role",
    )


def test_resolve_concept_from_env_loads_tb_supabase_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "mongodb://localhost")
    monkeypatch.setenv("MONGO_DB_NAME", "db")
    monkeypatch.setenv("TB_SUPABASE_URL", "https://tb-real.supabase.co")
    monkeypatch.setenv("TB_SUPABASE_SERVICE_ROLE_KEY", "tb-real-key")
    monkeypatch.setenv("VAPI_TB_ASSISTANT_ID", "asst_tb")

    c = resolve_concept_from_env("t_and_b")
    assert c.supabase_url == "https://tb-real.supabase.co"
    assert c.supabase_service_role_key == "tb-real-key"
    assert c.supabase_lookup_mode == "email"
