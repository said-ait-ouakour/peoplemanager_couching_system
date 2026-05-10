"""Mongo training progress: workspaces + steps (global lookup), per-advisor trainingprogresses by userId."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Tuple

from bson import ObjectId
from pymongo.database import Database

logger = logging.getLogger("advisor-orchestration")


def training_progress_enabled() -> bool:
    """When false, skip Mongo training collections (default: enabled)."""
    return os.environ.get("TRAINING_PROGRESS_ENABLED", "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _coll_workspaces() -> str:
    return (os.environ.get("MONGO_TRAINING_WORKSPACES_COLLECTION") or "workspaces").strip()


def _coll_steps() -> str:
    return (os.environ.get("MONGO_TRAINING_STEPS_COLLECTION") or "trainingsteps").strip()


def _coll_progress() -> str:
    return (os.environ.get("MONGO_TRAINING_PROGRESS_COLLECTION") or "trainingprogresses").strip()


def training_progress_collection_name() -> str:
    """Mongo collection for per-user rows (default ``trainingprogresses``). Override ``MONGO_TRAINING_PROGRESS_COLLECTION``."""
    return _coll_progress()


def _norm_key(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, ObjectId):
        return str(val)
    return str(val).strip()


def _workspace_display_name(doc: Dict[str, Any]) -> str:
    for k in ("name", "title", "workspace_name", "label"):
        v = doc.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _step_display_name(doc: Dict[str, Any]) -> str:
    for k in ("name", "title", "stepName", "step_name", "label"):
        v = doc.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def load_workspace_name_map(db: Database) -> Dict[str, str]:
    """All workspaces: map id string(s) -> display name."""
    out: Dict[str, str] = {}
    coll = db[_coll_workspaces()]
    for doc in coll.find({}):
        if not isinstance(doc, dict):
            continue
        name = _workspace_display_name(doc)
        if not name:
            name = _norm_key(doc.get("_id")) or "workspace"
        oid = doc.get("_id")
        if oid is not None:
            out[_norm_key(oid)] = name
        for alt in ("workspace_id", "workspaceId", "id"):
            v = doc.get(alt)
            if v is not None and _norm_key(v):
                out[_norm_key(v)] = name
    return out


def load_step_name_map(db: Database) -> Dict[str, str]:
    """All training steps: map step id string -> display name."""
    out: Dict[str, str] = {}
    coll = db[_coll_steps()]
    for doc in coll.find({}):
        if not isinstance(doc, dict):
            continue
        name = _step_display_name(doc)
        if not name:
            name = _norm_key(doc.get("_id")) or "step"
        oid = doc.get("_id")
        if oid is not None:
            out[_norm_key(oid)] = name
        for alt in ("stepId", "step_id", "id"):
            v = doc.get(alt)
            if v is not None and _norm_key(v):
                out[_norm_key(v)] = name
    return out


def load_training_reference_maps(db: Database) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Prefetch workspace names and step names (same DB as concepts)."""
    ws = load_workspace_name_map(db)
    st = load_step_name_map(db)
    logger.info(
        "[training] reference maps loaded workspaces=%d steps=%d",
        len(ws),
        len(st),
    )
    return ws, st


def _user_id_filter(mongo_user_id: str) -> Dict[str, Any]:
    """
    Match ``trainingprogresses.userId``.

    In MongoDB this field is stored as **ObjectId** (same id as ``users._id``). We query ``ObjectId``
    first; a **string** equality is included as ``$or`` for older or mixed data.
    """
    uid = (mongo_user_id or "").strip()
    if not uid:
        return {"userId": {"$in": []}}
    if len(uid) == 24 and ObjectId.is_valid(uid):
        oid = ObjectId(uid)
        return {"$or": [{"userId": oid}, {"userId": uid}]}
    return {"userId": uid}


def _progress_doc_to_workspace_step_groups(doc: Dict[str, Any]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """
    Parse one ``trainingprogresses`` document.

    **Canonical shape** (one row per user/workspace workflow)::

        workspaceId (ObjectId), stepsProgress: [ { stepId, started, completed, ... }, ... ]

    **Legacy shape**: ``stepsProgresses`` / ``steps_progresses`` nested blocks (array or dict).

    ``stepId`` / ``workspaceId`` are normalized with ``_norm_key`` for lookup in ``trainingsteps`` /
    ``workspaces`` prefetch maps.
    """
    if not isinstance(doc, dict):
        return []
    wid = doc.get("workspaceId") or doc.get("workspace_id")
    steps_raw = doc.get("stepsProgress") or doc.get("steps_progress")
    if wid is not None and steps_raw is not None:
        wkey = _norm_key(wid)
        if not wkey:
            return []
        if isinstance(steps_raw, list):
            return [(wkey, [s for s in steps_raw if isinstance(s, dict)])]
        return [(wkey, [])]

    raw = doc.get("stepsProgresses") or doc.get("steps_progresses")
    if raw is not None:
        return _iter_workspace_step_groups(raw)
    return []


def _iter_workspace_step_groups(steps_progresses_raw: Any) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """Normalize legacy ``stepsProgresses`` into (workspace_id_str, [step dicts])."""
    if steps_progresses_raw is None:
        return []
    if isinstance(steps_progresses_raw, list):
        out: List[Tuple[str, List[Dict[str, Any]]]] = []
        for block in steps_progresses_raw:
            if not isinstance(block, dict):
                continue
            wid = block.get("workspace_id") or block.get("workspaceId")
            wkey = _norm_key(wid)
            steps = block.get("steps") or block.get("stepsProgress") or []
            if isinstance(steps, dict):
                steps = list(steps.values())
            if not isinstance(steps, list):
                steps = []
            out.append((wkey, [s for s in steps if isinstance(s, dict)]))
        return out
    if isinstance(steps_progresses_raw, dict):
        out = []
        for k, v in steps_progresses_raw.items():
            wkey = _norm_key(k)
            if isinstance(v, list):
                out.append((wkey, [s for s in v if isinstance(s, dict)]))
            elif isinstance(v, dict):
                inner = v.get("steps") or v.get("stepsProgress") or []
                if isinstance(inner, dict):
                    inner = list(inner.values())
                out.append((wkey, [s for s in inner if isinstance(s, dict)]))
            else:
                out.append((wkey, []))
        return out
    return []


def _step_id_from_obj(step: Dict[str, Any]) -> str:
    """Prefer ``stepId`` (training step definition); fallback ``_id`` for legacy rows."""
    for k in ("stepId", "step_id", "_id", "id"):
        v = step.get(k)
        if v is not None and _norm_key(v):
            return _norm_key(v)
    return ""


def build_training_progress_summary(
    progress_docs: List[Dict[str, Any]],
    workspace_names: Dict[str, str],
    step_names: Dict[str, str],
) -> Dict[str, Any]:
    """
    Input: all MongoDB ``trainingprogresses`` documents for this ``userId`` (People Manager id),
    plus name maps from ``workspaces`` and ``trainingsteps``.

    Output: **workspace display name** (never raw ids) ->

    - ``not_started``: step **names** that are not started (and not completed).
    - ``not_completed_yet``: step **names** that are **started** but **not** completed (in progress).

    Fully completed steps are omitted. If there is nothing to show, returns ``{}``.

    When the same user has several progress rows (e.g. one document per workspace/workflow), step state
    is merged: completed wins; else started is OR'd across rows. Duplicate workspace display names merge lists.
    """
    # workspace_id -> step_id -> merged flags
    merged: Dict[str, Dict[str, Dict[str, bool]]] = {}

    for doc in progress_docs or []:
        if not isinstance(doc, dict):
            continue
        for wid, steps in _progress_doc_to_workspace_step_groups(doc):
            if not wid:
                continue
            bucket = merged.setdefault(wid, {})
            for st in steps:
                sid = _step_id_from_obj(st)
                if not sid:
                    continue
                started = bool(st.get("started"))
                completed = bool(st.get("completed"))
                cur = bucket.get(sid)
                if cur is None:
                    bucket[sid] = {"started": started, "completed": completed}
                else:
                    bucket[sid] = {
                        "started": cur["started"] or started,
                        "completed": cur["completed"] or completed,
                    }

    # workspace_name -> { not_started, not_completed_yet }
    by_name: Dict[str, Dict[str, List[str]]] = {}

    for wid, steps_map in merged.items():
        wname = workspace_names.get(wid) or "Unknown workspace"
        not_started_names: List[str] = []
        not_completed_yet_names: List[str] = []
        for sid, flags in steps_map.items():
            if flags["completed"]:
                continue
            sname = step_names.get(sid) or "Unknown step"
            if flags["started"]:
                not_completed_yet_names.append(sname)
            else:
                not_started_names.append(sname)
        if not not_started_names and not not_completed_yet_names:
            continue
        not_started_names = sorted(set(not_started_names))
        not_completed_yet_names = sorted(set(not_completed_yet_names))
        if wname in by_name:
            ex = by_name[wname]
            ex["not_started"] = sorted(set(ex["not_started"] + not_started_names))
            ex["not_completed_yet"] = sorted(
                set(ex["not_completed_yet"] + not_completed_yet_names)
            )
        else:
            by_name[wname] = {
                "not_started": not_started_names,
                "not_completed_yet": not_completed_yet_names,
            }

    if not by_name:
        return {}

    # Stable order by workspace name for JSON readability
    return {w: by_name[w] for w in sorted(by_name.keys())}


def fetch_training_progress_documents(db: Database, mongo_user_id: str) -> List[Dict[str, Any]]:
    """All training progress rows where ``userId`` matches the advisor (ObjectId in DB; pass 24-char hex string)."""
    coll = db[_coll_progress()]
    q = _user_id_filter(mongo_user_id)
    return [dict(d) for d in coll.find(q)]
