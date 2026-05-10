"""Unit tests for training progress payload shaping (no Mongo).

For **real MongoDB** → same ``trainings`` object as ``daily_payload["trainings"]``, see
``test_training_progress_live.py`` (env ``RUN_TRAINING_PROGRESS_MONGO_TEST=1``).
"""

from training_progress import build_training_progress_summary


def test_merge_multiple_docs_same_workspace() -> None:
    docs = [
        {
            "stepsProgresses": [
                {
                    "workspace_id": "w1",
                    "steps": [
                        {"stepId": "s1", "started": True, "completed": False},
                    ],
                }
            ]
        },
        {
            "stepsProgresses": [
                {
                    "workspace_id": "w1",
                    "steps": [
                        {"stepId": "s1", "started": False, "completed": False},
                        {"stepId": "s2", "started": False, "completed": False},
                    ],
                }
            ]
        },
    ]
    ws = {"w1": "Workspace One"}
    st = {"s1": "Step A", "s2": "Step B"}
    out = build_training_progress_summary(docs, ws, st)
    assert out == {
        "Workspace One": {
            "not_started": ["Step B"],
            "not_completed_yet": ["Step A"],
        }
    }
    assert "available" not in out


def test_completed_anywhere_omits_step() -> None:
    docs = [
        {
            "stepsProgresses": [
                {"workspace_id": "w1", "steps": [{"stepId": "s1", "started": True, "completed": False}]}
            ]
        },
        {
            "stepsProgresses": [
                {"workspace_id": "w1", "steps": [{"stepId": "s1", "started": False, "completed": True}]}
            ]
        },
    ]
    ws = {"w1": "WS"}
    st = {"s1": "Only"}
    out = build_training_progress_summary(docs, ws, st)
    assert out == {}


def test_dict_shaped_steps_progresses() -> None:
    doc = {
        "stepsProgresses": {
            "w99": [
                {"stepId": "s9", "started": False, "completed": False},
            ]
        }
    }
    ws = {"w99": "Other WS"}
    st = {"s9": "Only step"}
    out = build_training_progress_summary([doc], ws, st)
    assert out == {"Other WS": {"not_started": ["Only step"], "not_completed_yet": []}}


def test_empty_docs() -> None:
    assert build_training_progress_summary([], {}, {}) == {}


def test_canonical_trainingprogresses_document_workspace_id_and_steps_progress() -> None:
    """
    Collection shape: one doc with ``workspaceId`` + ``stepsProgress``; ``stepId`` is the training step id.
    """
    wid = "507f191e810c19729de860ea"
    s_a = "507f191e810c19729de860eb"
    s_b = "507f191e810c19729de860ec"
    doc = {
        "_id": "64a700000000000000000001",
        "userId": "507f191e810c19729de860e0",
        "currentStep": s_a,
        "workspaceId": wid,
        "stepsProgress": [
            {"stepId": s_a, "started": True, "completed": False, "score": 0, "_id": "64a700000000000000000099"},
            {"stepId": s_b, "started": False, "completed": False, "score": 0},
        ],
    }
    ws = {wid: "Sales workspace"}
    st = {s_a: "Discovery call", s_b: "Proposal"}
    out = build_training_progress_summary([doc], ws, st)
    assert out == {
        "Sales workspace": {
            "not_started": ["Proposal"],
            "not_completed_yet": ["Discovery call"],
        }
    }
