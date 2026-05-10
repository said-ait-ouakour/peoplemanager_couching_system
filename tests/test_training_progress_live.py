"""Live MongoDB: fetch ``trainingprogresses`` by ``userId``, map to ``daily_payload['trainings']``.

**What gets validated / printed**

After loading rows from the progress collection, we map through the same logic as production:

1. **Prefetch** ``workspaces`` + ``trainingsteps`` (``_id`` / ``workspaceId`` / ``stepId`` → **display name**).
2. **Fetch** all ``trainingprogresses`` rows for ``userId`` (``ObjectId`` in DB). Each doc may use
   **``workspaceId`` + ``stepsProgress``** (``stepId`` per row) or the older **``stepsProgresses``** nesting.
3. **Build** the object assigned to ``daily_payload["trainings"]``:

   - Keys = **workspace display names** (not Mongo ids).
   - Each value::

        {
          "not_started": [ "<step name>", ... ],       # never started (still incomplete)
          "not_completed_yet": [ "<step name>", ... ] # started but not completed
        }

   Fully completed steps are omitted. Empty overall → ``{}``.

This test prints **that exact sample** so you can confirm formatting before it reaches VAPI.

**Enable**

  set RUN_TRAINING_PROGRESS_MONGO_TEST=1
  set TRAINING_PROGRESS_TEST_USER_ID=<24-char hex Mongo users._id>

**Required env**

  MONGO_URI or MONGO_URL, MONGO_DB_NAME or MONGO_DB
  OPENAI_API_KEY, VAPI_API_KEY (placeholders injected if unset)

**Optional**

  TRAINING_PROGRESS_TEST_CONCEPT_ID=people_manager
  MONGO_TRAINING_PROGRESS_COLLECTION — e.g. ``trainingProgresses``
  TRAINING_PROGRESS_TEST_PRINT=0 — silence stdout
  TRAINING_PROGRESS_TEST_REQUIRE_ROWS=1 — fail if no progress rows for userId

Run::

  pytest tests/test_training_progress_live.py -v -s -m mongo_live
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Generator

import pytest

from concepts import get_concept_definition
from dates_london import yesterday_london_iso
from dotenv import load_dotenv
from training_progress import (
    build_training_progress_summary,
    fetch_training_progress_documents,
    training_progress_collection_name,
)
from workflow_engine import AdvisorRecord, ConceptWorkflow, SharedServiceConfig, resolve_concept_from_env

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

pytestmark = pytest.mark.mongo_live


def _live_enabled() -> bool:
    return os.environ.get("RUN_TRAINING_PROGRESS_MONGO_TEST", "").strip() == "1"


def _test_user_id() -> str:
    return (os.environ.get("TRAINING_PROGRESS_TEST_USER_ID") or "").strip()


def _should_print() -> bool:
    return os.environ.get("TRAINING_PROGRESS_TEST_PRINT", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _require_rows() -> bool:
    return os.environ.get("TRAINING_PROGRESS_TEST_REQUIRE_ROWS", "").strip() == "1"


def _validate_daily_payload_trainings_shape(trainings: Dict[str, Any]) -> None:
    """
    Same structure as ``workflow_engine.build_daily_payload(..., training=<this>)`` under ``trainings``.
    """
    assert isinstance(trainings, dict)
    for workspace_name, block in trainings.items():
        assert isinstance(workspace_name, str) and workspace_name.strip(), "workspace key must be a display name"
        assert isinstance(block, dict), f"{workspace_name!r}: value must be an object"
        keys = set(block.keys())
        assert keys == {"not_started", "not_completed_yet"}, (
            f"{workspace_name!r}: expected keys not_started + not_completed_yet, got {sorted(keys)}"
        )
        for k in ("not_started", "not_completed_yet"):
            lst = block[k]
            assert isinstance(lst, list), f"{workspace_name!r}.{k} must be a list"
            assert all(isinstance(x, str) for x in lst), f"{workspace_name!r}.{k} must be lists of step names (str)"


@pytest.fixture
def concept_id() -> str:
    return (os.environ.get("TRAINING_PROGRESS_TEST_CONCEPT_ID") or "people_manager").strip() or "people_manager"


@pytest.fixture
def workflow_for_training(monkeypatch: pytest.MonkeyPatch, concept_id: str) -> Generator[ConceptWorkflow, None, None]:
    raw = get_concept_definition(concept_id)
    ast_env = str(raw["vapi_assistant_id_env"])
    if not os.environ.get(ast_env):
        monkeypatch.setenv(ast_env, "00000000-0000-4000-8000-000000000001")
    if not os.environ.get("OPENAI_API_KEY"):
        monkeypatch.setenv("OPENAI_API_KEY", "integration-test-placeholder-openai")
    if not os.environ.get("VAPI_API_KEY"):
        monkeypatch.setenv("VAPI_API_KEY", "integration-test-placeholder-vapi")

    concept = resolve_concept_from_env(concept_id)
    shared = SharedServiceConfig()
    wf = ConceptWorkflow(concept, shared)
    override = (os.environ.get("OVERRIDE_RUN_DATE") or "").strip()
    wf._active_run_date = override if override else yesterday_london_iso()
    wf.prefetch_training_reference_maps()
    try:
        yield wf
    finally:
        wf.close()


@pytest.mark.skipif(not _live_enabled(), reason="Set RUN_TRAINING_PROGRESS_MONGO_TEST=1 to run")
def test_fetch_trainingprogresses_by_user_id_and_build_trainings_payload(
    workflow_for_training: ConceptWorkflow,
) -> None:
    uid = _test_user_id()
    if not uid:
        pytest.skip("Set TRAINING_PROGRESS_TEST_USER_ID to a Mongo users._id (24-char hex)")

    coll_name = training_progress_collection_name()
    progress_docs = fetch_training_progress_documents(workflow_for_training.mongo_db, uid)

    if _require_rows() and len(progress_docs) == 0:
        pytest.fail(
            f"No documents in collection {coll_name!r} for userId={uid!r}. "
            f"Check TRAINING_PROGRESS_TEST_USER_ID and MONGO_TRAINING_PROGRESS_COLLECTION."
        )

    # Map raw progress docs → same object as production puts on daily_payload["trainings"]
    sample_daily_payload_trainings = build_training_progress_summary(
        progress_docs,
        workflow_for_training._training_workspace_names,
        workflow_for_training._training_step_names,
    )
    _validate_daily_payload_trainings_shape(sample_daily_payload_trainings)

    assert sample_daily_payload_trainings == workflow_for_training.trainings_payload_for_mongo_user_id(uid)

    run_date = (os.environ.get("OVERRIDE_RUN_DATE") or "").strip() or yesterday_london_iso()
    stub = AdvisorRecord(
        mongo_user_id=uid,
        advisor_name="training_payload_check",
        email="training-test@example.com",
        e164_phone="+443300000000",
        peoplemanager_id=uid,
        hubstaff_id=None,
        supabase_advisor_id="0",
        mongo_document={},
    )
    daily_payload = workflow_for_training.build_daily_payload(
        stub,
        run_date,
        1,
        1,
        None,
        None,
        None,
        sample_daily_payload_trainings,
        objective_of_the_day="Training integration — objective stub.",
    )
    assert daily_payload.get("trainings") is sample_daily_payload_trainings

    if _should_print():
        print(f"\n{'=' * 72}", flush=True)
        print("SAMPLE — exact value for daily_payload['trainings'] (after mapping)", flush=True)
        print(f"{'=' * 72}", flush=True)
        print(
            "Mapping: trainingprogresses[userId] + workspaces/trainingsteps names →",
            "per workspace: not_started | not_completed_yet (step display names only).\n",
            flush=True,
        )
        print(f"MongoDB: db={workflow_for_training.concept.mongo_db_name!r}", flush=True)
        print(f"Collection: {coll_name!r}  userId: {uid!r}  raw_rows: {len(progress_docs)}", flush=True)
        print(f"Workspace name map size: {len(workflow_for_training._training_workspace_names)}", flush=True)
        print(f"Training step name map size: {len(workflow_for_training._training_step_names)}", flush=True)
        if progress_docs:
            print(f"Raw doc _id (first): {progress_docs[0].get('_id')}", flush=True)
        print(f"\n--- JSON: daily_payload['trainings'] ---\n", flush=True)
        print(json.dumps(sample_daily_payload_trainings, indent=2, ensure_ascii=False), flush=True)
        print(flush=True)
