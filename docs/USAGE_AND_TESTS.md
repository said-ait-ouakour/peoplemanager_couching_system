# Advisor outreach automation — usage, execution, and tests

This document explains **how to run** the automation (HTTP API, CLI, optional scheduler), **what happens** during a run (data flow and decisions), and **which tests** exist and how to run them.

For deployment-oriented sequencing, see [`DEPLOYMENT_AND_SEQUENCE_REPORT.md`](DEPLOYMENT_AND_SEQUENCE_REPORT.md).

---

## 1. What this project does

The pipeline connects:

- **MongoDB** — shared database. `users` (and related collections) hold advisors, **yesterday’s customer calls** (`customernacfeedbacks`, `feedbackType: "call"`), **NAC** (`dailynacfeedbacks`), and **coaching** (`daily_users_coaching_vapi`). Training metadata and per-user progress live in **`workspaces`**, **`trainingsteps`**, and **`trainingprogresses`** (names overridable via env).
- **Supabase** — **two** projects: **People Manager (PM)** and **T&B**. Each concept uses one project for **phone + Supabase user id**, **meetings counts**, optional **`hubstaff_id`** on `users`, and optional **`daily_coach_tracking`** rows for **recall** follow-up dials.
- **VAPI** — outbound voice calls. Each dial sends **`assistantOverrides.variableValues`** with **`advisor_name`** and a structured JSON **`daily_payload`** (counts, tier, NAC/coaching slices, optional **Hubstaff** flags, optional **trainings** summary).

**Concepts** (`concepts.py`): `people_manager` and `t_and_b`. Each has its own Supabase credentials and VAPI assistant IDs; both share the same Mongo URI and database name.

**Optional layers**

- **Hubstaff** — after the calls/meetings gate, for advisors who will be dialled: Hubstaff **v2 activities** for the same **London calendar day** as `run_date` (via `yesterday_london_utc_bounds`), token from an **n8n webhook** by default. Summary fields exposed on the payload are **flags + idle window times** (see `hubstaff.summary_to_payload_dict`).
- **Training progress** — once per concept run, **`workspaces`** and **`trainingsteps`** are prefetched for id→name maps; per advisor, all **`trainingprogresses`** rows for that Mongo `userId` are merged into **`daily_payload["trainings"]`** (workspace display name → `not_started` / `not_completed_yet` step **names**). Disable with `TRAINING_PROGRESS_ENABLED=0` (or `false` / `no` / `off`).
- **Recall** — when **`enable_recall_tracking`** is on (`POST /run-all`, scheduled daily job), the workflow upserts **`daily_coach_tracking`** in Supabase. A separate **`process_recalls_for_concept`** job (scheduler **interval** or **cron**) polls VAPI call status and may place **additional outbound dials** according to end-reason rules and caps (see `workflow_engine.py`).

---

## 2. Configuration

1. Copy `.env.example` to `.env` and fill in real values.
2. **Required for a full dial run:** `MONGO_URI` (or `MONGO_URL`), `MONGO_DB_NAME` (or `MONGO_DB` / legacy aliases), per-concept Supabase URL + **service role** key, `VAPI_API_KEY`, `VAPI_PM_ASSISTANT_ID`, `VAPI_TB_ASSISTANT_ID`, and a usable **`VAPI_PHONE_NUMBER_ID`** or per-concept `VAPI_PM_PHONE_NUMBER_ID` / `VAPI_TB_PHONE_NUMBER_ID` when your VAPI project requires it.
3. **`OPENAI_API_KEY`** — required by `resolve_concept_from_env` / shared config even when tests inject placeholders; production runs expect it set.
4. **Hubstaff (optional):** `HUBSTAFF_ORG_ID`, token webhook URL (`HUBSTAFF_N8N_TOKEN_URL` or default in `hubstaff.py`). Set `HUBSTAFF_ENABLED=0` to force-disable. Supabase **`users.hubstaff_id`** must be set for that advisor to fetch; otherwise an empty **`Hubstaff`** object is still attached when Hubstaff is globally configured.
5. **Training (optional):** defaults `TRAINING_PROGRESS_ENABLED` on; override collection names with `MONGO_TRAINING_WORKSPACES_COLLECTION`, `MONGO_TRAINING_STEPS_COLLECTION`, `MONGO_TRAINING_PROGRESS_COLLECTION` if your DB uses different names (e.g. camelCase).
6. **Recall / scheduler:** `ENABLE_SCHEDULER=1` (or `true` / `yes`), `DAILY_CRON`, `SCHEDULER_TZ`, optional `SCHEDULER_SKIP_DAILY_CRON`, `SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS`, `RECALL_POLL_CRON` or `RECALL_POLL_INTERVAL_SECONDS`, and recall tuning env vars documented in `workflow_engine.py` / `test_scheduler_integration.py`.
7. **pytest:** there is no root `tests/conftest.py`; individual live tests call **`load_dotenv`** on `.env` where needed. Install **`python-dotenv`** and **`pytest`** for local runs.

**Run date:** Business data is keyed to **`run_date`** as **`YYYY-MM-DD`** — by default **yesterday in `Europe/London`** (`default_run_date()`), unless **`OVERRIDE_RUN_DATE=YYYY-MM-DD`** is set for debugging.

**Mongo “yesterday” calls:** Collection and columns come from `concepts.py` (e.g. `customernacfeedbacks`, `userId`, `date`). **`MONGO_CALLS_DATE_KIND`** (and related flags in `.env.example`) control whether **`date`** is matched as an **ISO day string**, **BSON datetime range** for London yesterday, etc.

**Meetings:** Per concept, **`meetings`** (default) filtered by advisor FK and date column from `concepts.py` (e.g. **`created_at`** with **`meetings_date_match_mode`: `utc_bounds`**).

---

## 3. How to run the script

### 3.1 FastAPI server (recommended for production)

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness check. |
| `POST /run/{concept}` | Run **one** concept (`people_manager` or `t_and_b`) for **all** Mongo advisors matching that concept’s `advisor_query`. **Recall tracking is off** for this path (`enable_recall_tracking=False`). |
| `POST /run-all` | Run **every** concept in sequence for London yesterday. **Recall tracking is on** per concept (`enable_recall_tracking=True`). |
| `POST /run/{concept}/advisors` | Run **one** concept for **only** the listed Mongo `users._id` values. **Body:** `{"mongo_user_ids": ["<24-char hex>", ...]}`. Recall tracking **off**. |
| `GET /runs/{run_id}` | Status of a run started via POST (in-memory registry; **per process**). |

**Scheduler (optional):** `ENABLE_SCHEDULER=1`. On startup, APScheduler runs **`_scheduled_daily`** at `DAILY_CRON` (default `30 9 * * *`) in `SCHEDULER_TZ` (default `Europe/London`). That job runs **all concepts** with recall tracking enabled. Optionally **`SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS`** schedules a **one-shot** daily batch after N seconds. **`SCHEDULER_SKIP_DAILY_CRON=1`** skips the repeating cron (useful in tests). A second job runs **`process_recalls_for_concept`** on **`RECALL_POLL_INTERVAL_SECONDS`** or **`RECALL_POLL_CRON`** (default every 30 minutes).

### 3.2 Command line (`advisor_daily_workflow.py`)

Same pipeline as the API, without HTTP:

```bash
# Default: one concept (people_manager), all matching advisors
python -m advisor_daily_workflow

python -m advisor_daily_workflow --concept t_and_b

# All concepts, full advisor lists
python -m advisor_daily_workflow --all-concepts

# One concept, subset of Mongo user ids (comma-separated ObjectId hex strings)
python -m advisor_daily_workflow --concept people_manager --mongo-user-ids 507f1f77bcf86cd799439011,507f191e810c19729de860ea
```

Legacy: if only `VAPI_ASSISTANT_ID` is set, the CLI maps it to `VAPI_PM_ASSISTANT_ID` for PM runs.

### 3.3 Programmatic

```python
from workflow_engine import process_concept, default_run_date

batch_id, metrics = process_concept("people_manager", run_date=default_run_date())
batch_id, metrics = process_concept(
    "people_manager",
    mongo_user_ids=["507f1f77bcf86cd799439011"],
    enable_recall_tracking=True,
)
```

`metrics` is a count dict: `processed`, `success`, `skipped`, `failed`.

---

## 4. Execution process (step by step)

### 4.1 Entry

`process_concept(concept_id, …)` → loads env → `resolve_concept_from_env` → **`ConceptWorkflow.run(run_date, batch_run_id, mongo_user_ids=…, enable_recall_tracking=…)`**.

### 4.2 Load advisors from Mongo

**`get_advisors_from_mongo(mongo_user_ids)`** — `find` on `mongo_users_collection` with `advisor_query` (e.g. `role: advisor`). If `mongo_user_ids` is set, adds `_id: { $in: [...] }` (ObjectId for 24-char hex strings).

### 4.3 Merge with Supabase (phone + Supabase user id + hubstaff_id)

**`map_advisors_to_supabase_phone`** — For each Mongo advisor:

- **`people_manager`:** Supabase `users` row where `peoplemanager_id` = `str(Mongo users._id)`.
- **`t_and_b`:** Supabase `users` row matched by **email**.

Requires valid **phone** and Supabase **`id`** (used as `supabase_advisor_id`). **`hubstaff_id`** is read when present (numeric Hubstaff user id). Advisors without a match or valid E.164 phone are skipped (not counted as VAPI failures).

### 4.4 Per concept, before parallel dials

**`prefetch_training_reference_maps()`** — Loads **`workspaces`** and **`trainingsteps`** once for display-name resolution used by **`daily_payload["trainings"]`**.

### 4.5 Per advisor (`process_single_advisor`)

Runs in parallel up to **`VAPI_MAX_CONCURRENT_PER_CONCEPT`** (default **3** in code; `.env.example` may show another value).

1. **Optional audit** — If `ADVISOR_OUTREACH_AUDIT_TABLE` is set, inserts/updates rows; idempotent skip if prior **success** for same `run_date` + email.

2. **Mongo “calls yesterday”** — `fetch_yesterday_customer_calls_from_mongo`: `customernacfeedbacks` (or `mongo_calls_collection`), **`userId`** = advisor Mongo id, **`feedbackType`: `"call"`**, and **`date`** (or alternate columns) in the **London-yesterday** window per `MONGO_CALLS_DATE_KIND` / concept config. **Count** = **`Calls yesterday`**.

3. **Supabase meetings yesterday** — Count in that concept’s Supabase **`meetings`** table: advisor FK = Supabase user `id`, **`created_at`** (or configured date column) in the same window per **`meetings_date_match_mode`**.

4. **Gate** — If **both** calls count and meetings count are **zero**, the advisor is **skipped** (no Hubstaff fetch, no training aggregation for payload, no VAPI).

5. **Hubstaff (only if not skipped)** — If **`hubstaff_configured()`** and advisor has **`hubstaff_id`**, fetch activities into **`hubstaff_payload`** (flags dict; **`{}`** on failure or missing id). If Hubstaff is **not** configured, **`hubstaff_payload`** stays **`None`** so the **`Hubstaff`** key is omitted when building the payload.

6. **Training progress** — **`_compute_trainings_payload(mongo_user_id)`** always runs and produces **`training_payload`**: workspace→step lists or **`{}`** when disabled or nothing to show.

7. **NAC + coaching** — Latest NAC row (`mongo_nac_collection`) and latest coaching row (`mongo_coaching_collection`) by `userId`, for **`Feedback Summary`** and **`Memory`** (`coaching_insights` shape).

8. **`daily_payload`** — **`build_daily_payload`** merges the above: always includes **`repport_date`**, **`Advisor Name`**, **`Performance Tier`** (LOW / INTERMEDIATE / HIGH from calls + meetings), **`Calls yesterday`**, **`Meetings yesterday`**, **`Feedback Summary`**, **`Coaching Context`**, **`Memory`**, and **`trainings`** (from **`training_payload`**, often **`{}`**). **`Hubstaff`** is included only when **`hubstaff_payload`** is not **`None`**; then it is a dict of flags (possibly **`{}`** if no `hubstaff_id` or fetch failed).

9. **VAPI** — **`POST {VAPI_BASE_URL}/call`** (not `/call/phone`) with JSON body: `assistantId`, optional `phoneNumberId`, `customer.number` = E.164, **`assistantOverrides.variableValues`** = `{ "advisor_name", "daily_payload" }`.

10. **Tracking** — If **`enable_recall_tracking`**, upsert **`daily_coach_tracking`** with `current_vapi_call_id` etc. for later recall polls.

---

## 5. Tests

Tests live under **`tests/`**. Run everything:

```bash
python -m pytest tests/ -v
```

On Windows PowerShell, enable gated tests with for example:

```powershell
$env:RUN_DATA_INTEGRATION_TESTS = "1"
python -m pytest tests/test_advisor_data_fetch_live.py -v -s -m integration
```

### 5.1 Markers (`pytest.ini`)

| Marker | Meaning |
|--------|---------|
| `integration` | May hit **real** Mongo + Supabase; gated by env (see `test_advisor_data_fetch_live.py`). |
| `mongo_live` | Real Mongo and/or Hubstaff token/API; gated by env. |
| `vapi_live` | Real VAPI outbound (reserved; see `.env.example` if you add `test_vapi_call_live.py`). |
| `scheduler` | Scheduler / recall wiring; see `test_scheduler_integration.py`. |

### 5.2 Default (offline / fast)

| File | What it covers |
|------|----------------|
| `test_training_progress.py` | Unit tests for **`training_progress.build_training_progress_summary`** (canonical `workspaceId` + `stepsProgress`, legacy shapes, merging). |

### 5.3 Integration — data fetch (`test_advisor_data_fetch_live.py`)

**Enable:** `RUN_DATA_INTEGRATION_TESTS=1` plus Mongo + PM/TB Supabase credentials.

Read-only (or lightly printing): loads advisors, maps Supabase, counts calls/meetings, optionally builds **`daily_payload`** and prints Hubstaff slice when configured. See the file docstring for **`DATA_FETCH_TEST_*`** toggles.

```bash
pytest tests/test_advisor_data_fetch_live.py -v -s -m integration
```

### 5.4 Mongo live — Hubstaff (`test_hubstaff_flow.py`)

**Enable:** `RUN_HUBSTAFF_LIVE_TEST=1`, `HUBSTAFF_TEST_USER_ID=<numeric>`, `HUBSTAFF_ORG_ID`, token webhook reachable.

```bash
pytest tests/test_hubstaff_flow.py -v -s -m mongo_live
```

### 5.5 Mongo live — training payload (`test_training_progress_live.py`)

**Enable:** `RUN_TRAINING_PROGRESS_MONGO_TEST=1`, `TRAINING_PROGRESS_TEST_USER_ID=<24-char hex>`.

```bash
pytest tests/test_training_progress_live.py -v -s -m mongo_live
```

### 5.6 Scheduler + recall + tracking (`test_scheduler_integration.py`)

**Enable:** `RUN_SCHEDULER_TRACKING_LIVE_TEST=1` plus full `.env` (Mongo, Supabase, VAPI, assistants). **Long-running** and places **real** calls — see file docstring.

```bash
pytest tests/test_scheduler_integration.py -v -s -m scheduler
```

---

## 6. Quick troubleshooting

| Symptom | Things to check |
|---------|-------------------|
| No advisors processed | `advisor_query` in `concepts.py`, Mongo URI/DB, subset ids valid. |
| All skipped after mapping | Supabase keys, lookup mode (PM vs TB), phone + `id` on `users`. |
| Skipped “no calls or meetings yesterday” | Data for **London** `run_date`; Mongo **`date`** / `MONGO_CALLS_DATE_KIND`; Supabase meetings date column + `meetings_date_match_mode`. |
| VAPI errors | `VAPI_API_KEY`, assistant IDs, **`POST /call`** body shape, `phoneNumberId` if required. |
| `run_id` not found | `GET /runs/...` is **in-memory** for that server process only. |
| Hubstaff missing or always empty | `HUBSTAFF_ORG_ID`, webhook token, **`hubstaff_id`** on Supabase `users`, advisor passed the calls/meetings gate. If integration is off, **`Hubstaff`** is omitted from the payload (not the same as `{}`). |
| `trainings` always `{}` | `TRAINING_PROGRESS_ENABLED` not off; **`trainingprogresses.userId`** matches Mongo `users._id` (often **ObjectId** in DB); collection name env; prefetch collections populated. |
| **`pip install` / import errors on Windows** | Some stacks (e.g. Python 3.14 + **supabase** → **pyiceberg**) need a supported Python (3.11–3.12) or C++ build tools; use a venv with pinned versions from `requirements.txt`. |

---

## 7. File map

| File | Role |
|------|------|
| `main.py` | FastAPI app, routes, optional APScheduler (daily + recall poll). |
| `advisor_daily_workflow.py` | CLI entrypoint. |
| `workflow_engine.py` | `process_concept`, `process_recalls_for_concept`, `ConceptWorkflow`, Mongo/Supabase/VAPI, `build_daily_payload`, tracking. |
| `concepts.py` | Concept definitions and `ResolvedConcept` fields. |
| `dates_london.py` | London `run_date` and UTC bounds for “yesterday”. |
| `hubstaff.py` | Token webhook, Hubstaff v2 activities, summary → payload flags. |
| `training_progress.py` | Training prefetch maps, progress fetch/merge, `trainings` payload. |
| `nac_feedback.py` | Optional helpers for extracting text from call-shaped documents. |
| `.env.example` | Environment variable template. |
