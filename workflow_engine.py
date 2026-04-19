"""Shared pipeline: Mongo advisors (full docs) → Supabase phone + advisor id → Mongo calls + Supabase meetings → NAC/coaching → VAPI."""

from __future__ import annotations

import logging
import os
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import phonenumbers
import requests
from bson import ObjectId
from pymongo import MongoClient
from supabase import Client, create_client
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from concepts import ResolvedConcept, get_concept_definition
from dates_london import yesterday_london_iso, yesterday_london_utc_bounds

logger = logging.getLogger("advisor-orchestration")


def yesterday_utc_date() -> str:
    """Deprecated for scheduling; use yesterday_london_iso(). Kept for tests/import compatibility."""
    return yesterday_london_iso()


def _normalize_mongo_uri(raw: str) -> str:
    """Fix common .env typo: MONGO_URI=MONGO_URL=mongodb+srv://..."""
    uri = (raw or "").strip()
    if uri.startswith("MONGO_URL="):
        uri = uri[len("MONGO_URL=") :].strip()
    return uri


def _shared_mongo_uri_and_db() -> Tuple[str, str]:
    uri = _normalize_mongo_uri(
        os.environ.get("MONGO_URI")
        or os.environ.get("MONGO_URL")
        or os.environ.get("PM_MONGO_URI")
        or os.environ.get("TB_MONGO_URI")
        or ""
    )
    if not uri:
        raise RuntimeError(
            "Set MONGO_URI or MONGO_URL (shared MongoDB for all concepts). "
            "Legacy: PM_MONGO_URI / TB_MONGO_URI also work."
        )
    db_name = (
        os.environ.get("MONGO_DB_NAME")
        or os.environ.get("MONGO_DB")
        or os.environ.get("TB_MONGO_DB")
        or os.environ.get("PM_MONGO_DB")
        or ""
    ).strip()
    if not db_name:
        raise RuntimeError(
            "Set MONGO_DB_NAME or MONGO_DB (shared DB name). Legacy: TB_MONGO_DB / PM_MONGO_DB also work."
        )
    return uri, db_name


def default_run_date() -> str:
    """London yesterday, unless OVERRIDE_RUN_DATE=YYYY-MM-DD (manual/debug)."""
    override = (os.environ.get("OVERRIDE_RUN_DATE") or "").strip()
    if override:
        return override
    return yesterday_london_iso()


_NAC_FEEDBACK_TEXT_PATHS: Tuple[Tuple[str, ...], ...] = (
    ("areasForImprovement", "reportText"),
    ("AreasForImprovement", "reportText"),
    ("dailynacfeedback", "areasForImprovement", "reportText"),
    ("KeyCoachingRecommendations", "reportText"),
    ("keyCoachingRecommendations", "reportText"),
    ("KeyCoachingRecommendations", "report_text"),
    ("key_coaching_recommendations", "report_text"),
)


def _get_nested(doc: Dict[str, Any], *keys: str) -> Any:
    cur: Any = doc
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _feedback_summary_from_nac_row(concept_text_col: str, row: Dict[str, Any]) -> str:
    """Prefer nested NAC paths (e.g. areasForImprovement.reportText); else legacy text column."""
    for path in _NAC_FEEDBACK_TEXT_PATHS:
        val = _get_nested(row, *path)
        if val is not None and str(val).strip():
            return str(val).strip()
    legacy = row.get(concept_text_col)
    if legacy is not None and str(legacy).strip():
        return str(legacy).strip()
    return ""


def _compute_performance_tier(calls_yesterday: int, meetings_yesterday: int) -> str:
    """Derived from yesterday's call/meeting counts (not stored in Mongo)."""
    performance_tier = "INTERMEDIATE"
    if calls_yesterday <= 1 and meetings_yesterday <= 0:
        performance_tier = "LOW"
    if calls_yesterday >= 5 or meetings_yesterday >= 2:
        performance_tier = "HIGH"
    return performance_tier


def _coaching_context_label(concept_id: str) -> str:
    if concept_id == "people_manager":
        return "CRM PM"
    if concept_id == "t_and_b":
        return "CRM TB"
    return concept_id


def _first_env(*keys: str) -> str:
    for k in keys:
        v = os.environ.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    raise RuntimeError(f"Missing required env (tried): {keys}")


def resolve_supabase_credentials_for_concept(concept_id: str) -> Tuple[str, str]:
    """PM/T&B-specific Supabase projects; falls back to SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY."""
    if concept_id == "people_manager":
        url = _first_env("PM_SUPABASE_URL", "SUPABASE_URL")
        key = _first_env("PM_SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_ROLE_KEY")
    elif concept_id == "t_and_b":
        url = _first_env("TB_SUPABASE_URL", "SUPABASE_URL")
        key = _first_env("TB_SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_ROLE_KEY")
    else:
        url = _first_env("SUPABASE_URL")
        key = _first_env("SUPABASE_SERVICE_ROLE_KEY")
    return url, key


def resolve_concept_from_env(concept_id: str) -> ResolvedConcept:
    raw = get_concept_definition(concept_id)

    def _req(name: str) -> str:
        v = os.environ.get(name)
        if not v:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return v

    def _opt(env_key: str) -> Optional[str]:
        v = os.environ.get(env_key)
        return v.strip() if v and str(v).strip() else None

    phone_id_env = raw.get("vapi_phone_number_id_env")
    vapi_phone_number_id = _opt(str(phone_id_env)) if phone_id_env else None

    mongo_uri, mongo_db_name = _shared_mongo_uri_and_db()
    supabase_url, supabase_key = resolve_supabase_credentials_for_concept(concept_id)

    return ResolvedConcept(
        concept_id=concept_id,
        mongo_uri=mongo_uri,
        mongo_db_name=mongo_db_name,
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
        supabase_url=supabase_url,
        supabase_service_role_key=supabase_key,
        vapi_assistant_id=_req(raw["vapi_assistant_id_env"]),
        vapi_phone_number_id=vapi_phone_number_id,
    )


@dataclass
class AdvisorRecord:
    mongo_user_id: str
    advisor_name: str
    email: str
    e164_phone: str
    # Canonical linkage id = str(Mongo users document _id); matches Supabase userId & users.peoplemanager_id (PM).
    peoplemanager_id: Optional[str]
    # Supabase users row primary key (or configured column); joins meetings.advisor_id.
    supabase_advisor_id: str
    # Full Mongo users document (all fields) after serialization (_id as str).
    mongo_document: Dict[str, Any]


class SharedServiceConfig:
    """Env shared across all concepts (Supabase URL/key are per-concept on ResolvedConcept)."""

    def __init__(self) -> None:
        self.openai_api_key = os.environ["OPENAI_API_KEY"]
        self.openai_model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
        self.vapi_api_key = os.environ["VAPI_API_KEY"]
        self.vapi_base_url = os.environ.get("VAPI_BASE_URL", "https://api.vapi.ai")
        self.default_region = os.environ.get("PHONE_DEFAULT_REGION", "US")
        self.audit_table = os.environ.get("ADVISOR_OUTREACH_AUDIT_TABLE", "").strip() or None
        self.vapi_max_concurrent = max(1, int(os.environ.get("VAPI_MAX_CONCURRENT_PER_CONCEPT", "3")))


class ConceptWorkflow:
    def __init__(self, concept: ResolvedConcept, shared: SharedServiceConfig) -> None:
        self.concept = concept
        self.shared = shared
        self.mongo = MongoClient(concept.mongo_uri)
        self.mongo_db = self.mongo[concept.mongo_db_name]
        # Thread-local HTTP/Supabase: Session is not shared safely across threads.
        self._tls = threading.local()

    @property
    def supabase(self) -> Client:
        client = getattr(self._tls, "supabase", None)
        if client is None:
            client = create_client(
                self.concept.supabase_url,
                self.concept.supabase_service_role_key,
            )
            self._tls.supabase = client
        return client

    @property
    def http(self) -> requests.Session:
        session = getattr(self._tls, "http", None)
        if session is None:
            session = requests.Session()
            session.headers.update(
                {
                    "Authorization": f"Bearer {self.shared.vapi_api_key}",
                    "Content-Type": "application/json",
                }
            )
            self._tls.http = session
        return session

    def close(self) -> None:
        self.mongo.close()

    def _audit_enabled(self) -> bool:
        return self.shared.audit_table is not None

    def _already_succeeded(self, run_date: str, email: str) -> bool:
        if not self._audit_enabled():
            return False
        assert self.shared.audit_table is not None
        r = (
            self.supabase.table(self.shared.audit_table)
            .select("id")
            .eq("concept", self.concept.concept_id)
            .eq("advisor_email", email)
            .eq("run_date", run_date)
            .eq("status", "success")
            .limit(1)
            .execute()
        )
        return bool(r.data)

    def _audit_insert(
        self,
        batch_run_id: str,
        run_date: str,
        email: str,
        status: str,
        reason: Optional[str] = None,
        vapi_call_id: Optional[str] = None,
    ) -> None:
        if not self._audit_enabled():
            return
        assert self.shared.audit_table is not None
        row: Dict[str, Any] = {
            "batch_run_id": batch_run_id,
            "concept": self.concept.concept_id,
            "advisor_email": email,
            "run_date": run_date,
            "status": status,
            "reason": reason,
            "vapi_call_id": vapi_call_id,
        }
        self.supabase.table(self.shared.audit_table).insert(row).execute()

    def _audit_update(
        self,
        batch_run_id: str,
        run_date: str,
        email: str,
        status: str,
        reason: Optional[str] = None,
        vapi_call_id: Optional[str] = None,
    ) -> None:
        if not self._audit_enabled():
            return
        assert self.shared.audit_table is not None
        # Update latest matching row for this batch + advisor
        q = (
            self.supabase.table(self.shared.audit_table)
            .update({"status": status, "reason": reason, "vapi_call_id": vapi_call_id})
            .eq("batch_run_id", batch_run_id)
            .eq("concept", self.concept.concept_id)
            .eq("advisor_email", email)
            .eq("run_date", run_date)
        )
        q.execute()

    def get_advisors_from_mongo(self, mongo_user_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Advisors matching concept `advisor_query`; optional filter by Mongo `users._id` (hex or str)."""
        collection = self.mongo_db[self.concept.mongo_users_collection]
        query: Dict[str, Any] = dict(self.concept.advisor_query)
        if mongo_user_ids:
            ids_in: List[Any] = []
            for raw in mongo_user_ids:
                s = (raw or "").strip()
                if not s:
                    continue
                if len(s) == 24 and ObjectId.is_valid(s):
                    ids_in.append(ObjectId(s))
                else:
                    ids_in.append(s)
            if not ids_in:
                return []
            query["_id"] = {"$in": ids_in}
        return [self._serialize_mongo_doc(doc) for doc in collection.find(query)]

    def _mongo_advisor_id_str(self, doc: Dict[str, Any]) -> Optional[str]:
        """Mongo users._id — same value as userId / peoplemanager_id keys elsewhere."""
        oid = doc.get("_id")
        if oid is None:
            return None
        s = str(oid).strip()
        return s or None

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_supabase_user_row_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        c = self.concept
        cols = f"{c.supabase_email_col},{c.supabase_phone_col},{c.supabase_advisor_id_col}"
        response = (
            self.supabase.table(c.supabase_user_table)
            .select(cols)
            .eq(c.supabase_email_col, email)
            .eq(c.supabase_user_role_col, c.supabase_user_role_value)
            .limit(1)
            .execute()
        )
        if not response.data:
            return None
        return dict(response.data[0])

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_supabase_user_row_by_peoplemanager_id(self, mongo_user_id_str: str) -> Optional[Dict[str, Any]]:
        """PM: Supabase users.peoplemanager_id column stores str(Mongo users._id)."""
        c = self.concept
        cols = f"{c.supabase_phone_col},{c.supabase_advisor_id_col}"
        response = (
            self.supabase.table(c.supabase_user_table)
            .select(cols)
            .eq(c.supabase_peoplemanager_id_col, mongo_user_id_str)
            .eq(c.supabase_user_role_col, c.supabase_user_role_value)
            .limit(1)
            .execute()
        )
        if not response.data:
            return None
        return dict(response.data[0])

    def fetch_phone_from_supabase_by_email(self, email: str) -> Optional[str]:
        row = self.fetch_supabase_user_row_by_email(email)
        return row.get(self.concept.supabase_phone_col) if row else None

    def fetch_phone_from_supabase_by_peoplemanager_id(self, mongo_user_id_str: str) -> Optional[str]:
        row = self.fetch_supabase_user_row_by_peoplemanager_id(mongo_user_id_str)
        return row.get(self.concept.supabase_phone_col) if row else None

    def map_advisors_to_supabase_phone(
        self, mongo_advisors: List[Dict[str, Any]]
    ) -> List[AdvisorRecord]:
        valid: List[AdvisorRecord] = []
        mode = self.concept.supabase_lookup_mode
        c = self.concept
        for advisor in mongo_advisors:
            email = (advisor.get("email") or "").strip().lower()
            name = (advisor.get("name") or "Unknown advisor").strip()
            mongo_id_str = self._mongo_advisor_id_str(advisor)
            if not mongo_id_str:
                logger.warning("Skipping advisor %s: missing Mongo _id", email or "?")
                continue

            if mode == "email":
                if not email:
                    continue
                row = self.fetch_supabase_user_row_by_email(email)
            elif mode == "peoplemanager_id":
                row = self.fetch_supabase_user_row_by_peoplemanager_id(mongo_id_str)
            else:
                raise RuntimeError(f"Unknown supabase_lookup_mode: {mode}")

            if not row:
                continue
            phone = row.get(c.supabase_phone_col)
            advisor_id_raw = row.get(c.supabase_advisor_id_col)
            if not phone:
                continue
            if advisor_id_raw is None or str(advisor_id_raw).strip() == "":
                logger.warning(
                    "Skipping advisor %s: Supabase row missing %s",
                    email or mongo_id_str,
                    c.supabase_advisor_id_col,
                )
                continue

            e164_phone = self.to_e164(phone)
            if not e164_phone:
                logger.warning("Skipping advisor %s, invalid phone: %s", email or mongo_id_str, phone)
                continue

            mongo_doc = dict(advisor)
            valid.append(
                AdvisorRecord(
                    mongo_user_id=mongo_id_str,
                    advisor_name=name,
                    email=email,
                    e164_phone=e164_phone,
                    peoplemanager_id=mongo_id_str,
                    supabase_advisor_id=str(advisor_id_raw).strip(),
                    mongo_document=mongo_doc,
                )
            )
        return valid

    def to_e164(self, raw_phone: str) -> Optional[str]:
        try:
            parsed = phonenumbers.parse(raw_phone, self.shared.default_region)
            if not phonenumbers.is_valid_number(parsed):
                return None
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except phonenumbers.NumberParseException:
            return None

    def _serialize_mongo_doc(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(doc)
        if "_id" in out:
            out["_id"] = str(out["_id"])
        return out

    def _user_id_match(self, uid: str) -> Any:
        """Match Mongo userId stored as string or ObjectId."""
        if len(uid) == 24 and ObjectId.is_valid(uid):
            return {"$in": [uid, ObjectId(uid)]}
        return uid

    def _calls_date_range_yesterday(self) -> Dict[str, Any]:
        """London calendar yesterday as UTC half-open interval [lo, hi) on BSON/datetime `date` field."""
        lo, hi = yesterday_london_utc_bounds()
        return {"$gte": lo, "$lt": hi}

    def _advisor_supabase_user_id(self, advisor: AdvisorRecord) -> Optional[str]:
        """Mongo userId on related collections = str(Mongo users._id)."""
        return advisor.peoplemanager_id

    def fetch_yesterday_customer_calls_from_mongo(
        self, user_id: str, run_date: str
    ) -> List[Dict[str, Any]]:
        """All call rows for userId with `calls_date_col` in [start, end) of yesterday in Europe/London."""
        c = self.concept
        coll = self.mongo_db[c.mongo_calls_collection]
        q: Dict[str, Any] = {
            c.calls_user_id_col: self._user_id_match(user_id),
            c.calls_date_col: self._calls_date_range_yesterday(),
        }
        docs = list(coll.find(q))
        return [self._serialize_mongo_doc(d) for d in docs]

    def last_customer_call_yesterday(
        self, yesterday_calls: List[Dict[str, Any]], run_date: str
    ) -> Optional[Dict[str, Any]]:
        """Most recent call row by date field among yesterday's rows."""
        if not yesterday_calls:
            return None
        c = self.concept
        date_key = c.calls_date_col

        def _sort_key(row: Dict[str, Any]) -> Any:
            return row.get(date_key) or ""

        return max(yesterday_calls, key=_sort_key)

    def fetch_latest_nac_from_mongo(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Latest nacfeedbacks row for userId (by nac_order_col), no date filter."""
        c = self.concept
        coll = self.mongo_db[c.mongo_nac_collection]
        q = {c.nac_user_id_col: self._user_id_match(user_id)}
        cur = coll.find(q).sort(c.nac_order_col, -1).limit(1)
        docs = list(cur)
        if not docs:
            return None
        return self._serialize_mongo_doc(docs[0])

    def fetch_latest_coaching_from_mongo(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Latest coaching row for userId (Mongo users._id); uses previous_call_summary column."""
        c = self.concept
        coll = self.mongo_db[c.mongo_coaching_collection]
        q = {c.coaching_user_id_col: self._user_id_match(user_id)}
        cur = coll.find(q).sort(c.coaching_order_col, -1).limit(1)
        docs = list(cur)
        if not docs:
            return None
        return self._serialize_mongo_doc(docs[0])

    def _coaching_summary_text(self, row: Dict[str, Any]) -> str:
        c = self.concept
        primary = row.get(c.coaching_summary_col)
        if primary is not None and str(primary).strip():
            return str(primary).strip()
        for key in ("coaching", "summary", "feedback", "notes", "daily_summary"):
            val = row.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
        return ""

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_meetings_yesterday_count_from_supabase(self, advisor_supabase_id: str, run_date: str) -> int:
        """Meetings linked to Supabase advisor id with meeting_date in London yesterday (range or calendar day)."""
        c = self.concept
        mode = (c.meetings_date_match_mode or "utc_bounds").strip().lower()
        if mode == "iso_day":
            q = (
                self.supabase.table(c.supabase_meetings_table)
                .select("*", count="exact")
                .eq(c.supabase_meetings_advisor_id_col, advisor_supabase_id)
                .eq(c.supabase_meetings_date_col, run_date)
            )
        else:
            lo, hi = yesterday_london_utc_bounds()
            q = (
                self.supabase.table(c.supabase_meetings_table)
                .select("*", count="exact")
                .eq(c.supabase_meetings_advisor_id_col, advisor_supabase_id)
                .gte(c.supabase_meetings_date_col, lo.isoformat())
                .lt(c.supabase_meetings_date_col, hi.isoformat())
            )
        response = q.execute()
        cnt = getattr(response, "count", None)
        if cnt is not None:
            return int(cnt)
        data = getattr(response, "data", None) or []
        return len(data)

    def build_daily_payload(
        self,
        advisor: AdvisorRecord,
        calls_yesterday: int,
        meetings_yesterday: int,
        last_nac_feedback_row: Optional[Dict[str, Any]],
        last_coaching_row: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Structured fields for VAPI assistantOverrides.variableValues.daily_payload (display labels)."""
        c = self.concept
        nac_payload = last_nac_feedback_row or {}
        coaching_payload = last_coaching_row or {}
        feedback_summary = _feedback_summary_from_nac_row(c.nac_text_col, nac_payload)
        memory = self._coaching_summary_text(coaching_payload)
        calls_n = calls_yesterday
        meetings_n = meetings_yesterday
        tier = _compute_performance_tier(calls_n, meetings_n)
        return {
            "Advisor Name": advisor.advisor_name,
            "Performance Tier": tier,
            "Calls yesterday": calls_n,
            "Meetings yesterday": meetings_n,
            "Feedback Summary": feedback_summary,
            "Coaching Context": _coaching_context_label(c.concept_id),
            "Memory": memory,
        }

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def call_vapi_advisor(
        self, advisor: AdvisorRecord, daily_payload: Dict[str, Any]
    ) -> Tuple[int, str, Optional[str]]:
        endpoint = f"{self.shared.vapi_base_url}/call/phone"
        phone_number_id = self.concept.vapi_phone_number_id or self.shared.vapi_phone_number_id
        body: Dict[str, Any] = {
            "assistantId": self.concept.vapi_assistant_id,
            "customer": {"number": advisor.e164_phone},
            "assistantOverrides": {
                "variableValues": {
                    "daily_payload": daily_payload,
                },
            },
        }
        if phone_number_id:
            body["phoneNumberId"] = phone_number_id
        response = self.http.post(endpoint, json=body, timeout=30)
        vapi_id: Optional[str] = None
        try:
            payload = response.json()
            if isinstance(payload, dict):
                vapi_id = payload.get("id") or payload.get("callId")
        except Exception:
            pass
        if response.status_code >= 400:
            raise RuntimeError(
                f"VAPI call failed for {advisor.email}: {response.status_code} {response.text}"
            )
        return response.status_code, response.text, vapi_id

    def process_single_advisor(self, advisor: AdvisorRecord, run_date: str, batch_run_id: str) -> str:
        """Returns outcome: success | skipped | failed."""
        if self._already_succeeded(run_date, advisor.email):
            logger.info("Idempotent skip (prior success): %s %s", self.concept.concept_id, advisor.email)
            return "skipped"

        if self._audit_enabled():
            self._audit_insert(batch_run_id, run_date, advisor.email, "pending")

        try:
            uid = self._advisor_supabase_user_id(advisor)
            if not uid:
                logger.info(
                    "Skipping advisor %s: missing Mongo advisor id (needed for Supabase userId joins)",
                    advisor.email,
                )
                if self._audit_enabled():
                    self._audit_update(
                        batch_run_id,
                        run_date,
                        advisor.email,
                        "skipped",
                        reason="missing_advisor_mongo_id",
                    )
                return "skipped"

            yesterday_calls = self.fetch_yesterday_customer_calls_from_mongo(uid, run_date)
            calls_count = len(yesterday_calls)
            meetings_count = self.fetch_meetings_yesterday_count_from_supabase(
                advisor.supabase_advisor_id, run_date
            )

            if calls_count == 0 and meetings_count == 0:
                logger.info(
                    "Skipping advisor %s: no calls or meetings yesterday (Mongo calls=%s Supabase meetings=%s)",
                    advisor.email,
                    calls_count,
                    meetings_count,
                )
                if self._audit_enabled():
                    self._audit_update(
                        batch_run_id,
                        run_date,
                        advisor.email,
                        "skipped",
                        reason="no_calls_and_meetings_yesterday",
                    )
                return "skipped"

            nac_row = self.fetch_latest_nac_from_mongo(uid)
            coaching = self.fetch_latest_coaching_from_mongo(uid)

            daily_payload = self.build_daily_payload(
                advisor,
                calls_count,
                meetings_count,
                nac_row,
                coaching,
            )
            _status, _text, vapi_id = self.call_vapi_advisor(advisor, daily_payload)
            logger.info("VAPI call queued for advisor: %s", advisor.email)
            if self._audit_enabled():
                self._audit_update(
                    batch_run_id,
                    run_date,
                    advisor.email,
                    "success",
                    vapi_call_id=vapi_id,
                )
            return "success"
        except Exception as exc:  # noqa: BLE001 — top-level per-advisor boundary
            logger.exception("Advisor failed: %s", advisor.email)
            if self._audit_enabled():
                self._audit_update(
                    batch_run_id,
                    run_date,
                    advisor.email,
                    "failed",
                    reason=str(exc)[:2000],
                )
            return "failed"

    def run(
        self,
        run_date: str,
        batch_run_id: str,
        mongo_user_ids: Optional[List[str]] = None,
        metrics_cb: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, int]:
        counts = {"processed": 0, "success": 0, "skipped": 0, "failed": 0}

        advisors = self.get_advisors_from_mongo(mongo_user_ids)
        logger.info("[%s] Mongo advisors found: %d", self.concept.concept_id, len(advisors))

        mapped = self.map_advisors_to_supabase_phone(advisors)
        logger.info("[%s] Advisors with valid phone numbers: %d", self.concept.concept_id, len(mapped))

        max_workers = self.shared.vapi_max_concurrent
        lock = threading.Lock()

        def _run_one(adv: AdvisorRecord) -> str:
            return self.process_single_advisor(adv, run_date, batch_run_id)

        def _record(out: str) -> None:
            with lock:
                counts["processed"] += 1
                counts[out] = counts.get(out, 0) + 1
            if metrics_cb:
                metrics_cb(out)

        if max_workers <= 1:
            for adv in mapped:
                _record(_run_one(adv))
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [pool.submit(_run_one, adv) for adv in mapped]
                for fut in as_completed(futures):
                    _record(fut.result())

        return counts


def process_concept(
    concept_id: str,
    run_date: Optional[str] = None,
    batch_run_id: Optional[str] = None,
    mongo_user_ids: Optional[List[str]] = None,
    metrics_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[str, Dict[str, int]]:
    """Run pipeline for one concept. Optional `mongo_user_ids` restricts Mongo users._id (hex strings)."""
    rid = batch_run_id or str(uuid.uuid4())
    date_s = run_date or default_run_date()
    concept = resolve_concept_from_env(concept_id)
    shared = SharedServiceConfig()
    wf = ConceptWorkflow(concept, shared)
    try:
        metrics = wf.run(date_s, rid, mongo_user_ids=mongo_user_ids, metrics_cb=metrics_cb)
        return rid, metrics
    finally:
        wf.close()
