# UI-0 Audit Report — ChaosFlow Frontend Overhaul

**Date:** 2026-10-03  
**Scope:** Full read-only audit of frontend `src/`, backend `app/api/routes/`, `app/pipeline/`, `app/reliability/`, `app/spark/`, `app/storage/`, `app/models/`, `app/messaging/`, `spark-jobs/`, and all supporting files.  
**Status:** Phase UI-0 complete — no code changed in this phase.

---

## 1. Confirmed Issues from Section 1.1 (Correctness and Honesty)

### Issue 1 — `mockSamples` in `ReliabilityTesting.tsx`

**Confirmed.** `frontend/src/pages/ReliabilityTesting.tsx` lines 12–16 generate 20 synthetic
`{timestamp, available}` samples client-side with alternating hard-coded `true/false` values:

```tsx
// Comment in the source code confirms intent:
// "Mocking timeline data since Phase 8 doesn't require a strict real-time socket connection"
const mockSamples = Array.from({length: 20}).map((_, i) => ({
  timestamp: new Date(Date.now() - (20-i)*5000).toISOString(),
  available: i < 5 || i > 12   // hard-coded pattern
}));
```

The backend's `MetricsCollector.capture_during_experiment()` already collects real per-second
`{timestamp, available}` samples and stores them in `ExperimentRun.results["samples"]` via
`_merge_results`. They are **stored** in the database but **not surfaced** by
`GET /api/experiments/{id}` — the route returns only `id, name, target, faultType, status,
lifecycleStage, logs, results` where `results` is the raw JSON blob. The `samples` key is present
in that blob but the frontend never reads it.

**Fix required:** Surface `samples` explicitly in the `GET /api/experiments/{id}` response.
Remove `mockSamples` entirely. Pass real samples to `RecoveryTimeline`.

---

### Issue 2 — `GET /api/experiments` contract mismatch

**Confirmed.** `backend/app/api/routes/experiments.py` lines 175–191 returns:

```json
{
  "experiment_id": "uuid",
  "name": "...",
  "created_at": "ISO",
  "latest_run_status": "running|succeeded|failed|null",
  "latest_run_id": "uuid|null"
}
```

`frontend/src/types/experiment.ts` declares `Experiment` as
`{ id, name, target, faultType, status, createdAt }`.

`Dashboard.tsx` line 32 maps `exp.id` and `exp.status` — both `undefined` because the actual keys
are `experiment_id` and `latest_run_status`. The Recent Experiments section therefore renders empty
name slots and broken status badges for every experiment.

**Fix required:** Add a mapper in `api/experiments.ts` that converts the API response keys to the
frontend `Experiment` type, or redefine `Experiment` to match the actual API shape exactly.

---

### Issue 3 — Static logs in `GET /api/experiments/{id}`

**Confirmed.** `experiments.py` lines 237–241 returns:

```python
"logs": [
    f"Target: {params.get('target_service', 'not started')}",
    f"Fault: {params.get('fault_type', 'not started')}",
    f"Stage: {latest_run.status if latest_run else 'pending'}",
],
```

Three synthetic strings constructed from stored params — not real execution log lines.
`ExperimentDetail.results` in the frontend is typed as
`{ recoveryTimeSeconds: number; availabilityPercentage: number }` (camelCase) but the backend
stores raw results in snake_case (`recovery_time_seconds`, `availability_percentage`), so
`results.recoveryTimeSeconds` always resolves to `undefined`.

**Fix required:** Introduce structured log lines in `ExperimentRun.results["logs"]` during runner
stage transitions and expose them via the API. Add a snake→camelCase mapper for the `results`
object.

---

### Issue 4 — `POST /api/pipelines/run` is fully synchronous; no `running` state possible

**Confirmed.** `pipelines.py` lines 201–209:

```python
result = await asyncio.to_thread(run_pipeline_from_yaml, request.yaml_content)
if result.get("status") != "invalid":
    result["pipeline_run_id"] = await _record_pipeline_run(db, request.yaml_content, result)
return JSONResponse(content=result, status_code=200)
```

`asyncio.to_thread` offloads blocking I/O from the event loop but the HTTP response is still
blocked until the thread finishes. `_record_pipeline_run` creates the `PipelineRun` row with
`status="succeeded"` or `status="failed"` **after** execution completes. There is never a `running`
row visible to any concurrent request.

`PipelineDetails.tsx` line 14 polls with
`refetchInterval: (query) => (query.state.data?.status === 'running' ? 2000 : false)` — this
interval is never activated because the run is never in `running` state by the time the GET returns.

**Fix required:** See Section 3.1 of the master prompt. Return 202 immediately with a `running`
row. Continue execution in a background task.

---

### Issue 5 — Pre-filled credentials in UI and hardcoded defaults in backend

**Confirmed.** `Login.tsx` lines 7–8:

```tsx
const [username, setUsername] = useState('mohamed');
const [password, setPassword] = useState('123456');
```

`config.py` lines 53–54:

```python
ADMIN_USERNAME: str = 'mohamed'
ADMIN_PASSWORD: str = '123456'
```

**Fix required:** Replace `useState('mohamed')` / `useState('123456')` with `useState('')`.
In `config.py`, remove the default values and raise a `ValueError` at startup if they are unset
in non-dev mode. Document in `.env.example`.

---

### Issue 6 — Stale comments and hard-redirect 401 handler in `api/client.ts`

**Confirmed.** `api/client.ts` lines 9–13 contain a multi-line comment describing a "dirty hack"
and architectural uncertainty. The 401 handler on line 23:

```ts
window.location.href = '/login';
```

This causes a full browser navigation, wiping all React state (TanStack Query cache, auth context,
unsaved YAML in the editor).

**Fix required:** Delete the comment block. Replace the 401 handler with a navigation callback
via the auth context so the 401 triggers `navigate('/login', { state: { sessionExpired: true } })`
and `logout()`. Show "session expired" banner in `Login.tsx` when `location.state?.sessionExpired`.

---

### Issue 7 — Client-side elapsed timer in `LiveExperimentMonitor.tsx`

**Confirmed.** `LiveExperimentMonitor.tsx` line 9: `const [elapsed, setElapsed] = useState(0)`,
incremented by `usePolling(() => setElapsed(e => e + 1), 1000, !isFinished)`. This counter:
- Resets to 0 on every remount.
- Drifts from real start time because the first poll may happen after a delay.
- Cannot reflect a run already running 30 seconds before the component mounts.

The backend stores `ExperimentRun.started_at` (ISO timestamp) and `ExperimentRun.finished_at`.

**Fix required:** Remove `setElapsed` polling. Compute
`elapsedSeconds = (Date.now() - new Date(data.createdAt).getTime()) / 1000` from real `started_at`.
Re-render every second via `useEffect` + `setInterval`.

---

### Issue 8 — `PipelineRunResult` declared but inconsistent with API payloads

**Confirmed.** `types/pipeline.ts` declares `PipelineRunResult { recordsProcessed, durationMs }` as
the type for the optional `result?` field of `PipelineRun`. The API never returns a `result` field
at all — it returns `recordsProcessed` directly on the run object and `metrics` as a flat
`{ name: value }` dict.

Also, `PipelineExecutionResult` has `run_id: string | null` and `pipeline_run_id: string`. The API
only returns `pipeline_run_id`; `run_id` is a dead field in the type.

**Fix required:** Remove `PipelineRunResult`. Reconcile `PipelineRun` to exactly match the
`GET /api/pipeline-runs/{id}` response shape. Remove `run_id` from `PipelineExecutionResult`.

---

## 2. Additional Issues Found (Not in Section 1.1)

### Issue 9 — `ServiceStatusBadge` receives an object where it expects a boolean

`Dashboard.tsx` line 21:

```tsx
Object.entries(status).map(([service, connected]) => (
  <ServiceStatusBadge key={service} serviceName={service} connected={connected} />
))
```

`GET /api/system/status` returns `{ database: { connected: bool, latency_ms: float, error: str|null }, kafka: {...}, spark: {...}, hdfs: {...} }` — each value is an **object**, not a boolean. `ServiceStatusBadge` evaluates `connected ? 'Connected' : 'Disconnected'` — this is always `'Connected'` because a non-null object is truthy, even if `database.connected === false`.

**Fix required:** Map `status[service].connected` explicitly. Update `SystemStatus` type in
`system.ts` to reflect the richer per-service shape.

---

### Issue 10 — `GET /api/reports` response shape is incompatible with frontend `Report` type

`experiments.py` lines 468–477 returns `{ report_id, title, created_at, summary }`. The frontend
`Report` type expects `{ id, experiment_name, target, failure_type, recovery_time_seconds, ... }`.
The list endpoint returns a summary; the individual fields are inside `summary` (== `metadata_`).

`ReportCard` renders empty values for all metrics. `ReportChart` renders empty data.

**Fix required:** Extract canonical fields from `metadata_` in the list endpoint response, or add
a mapper in `api/experiments.ts` that extracts metrics from `summary`. The `Report.id` field must
map to `report_id`.

---

### Issue 11 — Availability samples not surfaced from experiment results

`MetricsCollector.capture_during_experiment()` stores `{ samples, total_samples,
available_samples, availability_percentage }` in `ExperimentRun.results`. The `get_experiment`
endpoint returns `"results": results` as a raw dict — the samples are technically accessible as
`results.samples` but the frontend `ExperimentDetail.results` type only declares
`{ recoveryTimeSeconds, availabilityPercentage }`, so `samples` is never extracted.

**Fix required:** Add explicit `availabilitySamples: Array<{ timestamp: string; available: boolean }>` to `ExperimentDetail`. Map it from `results.samples` in the API response or add a dedicated field.

---

### Issue 12 — Experiment list endpoint does not include `target` or `faultType`

`GET /api/experiments` does not return `target_service` or `fault_type`. Dashboard cannot show
fault type or target in the Recent Experiments section. The detail endpoint does return them (from
`ExperimentRun.params`).

**Fix required:** Add `target_service` and `fault_type` to the list endpoint response (they live in
`ExperimentRun.params` of the latest run).

---

### Issue 13 — `GET /api/auth/me` role field not stored or used by frontend

`auth.py` line 35 returns `{"username": username, "role": "admin"}`. The frontend `AuthContext`
only stores `username`. Role is unused in the UI despite the spec requiring three roles.

**Fix required:** Add `role` to `AuthContext`. Fetch `GET /api/auth/me` after login to populate it.
Show role in the TopBar user menu.

---

### Issue 14 — Spark mid-run progress is architecturally unavailable

**Critical.** `spark_job_submitter.py` calls `proc.communicate()` which reads all stdout **after
the process exits**. There is no mid-run progress from stdout. The Spark driver REST API (port 4040)
could provide stage/task progress, but:
- It is not confirmed whether port 4040 is exposed in docker-compose.
- The `submit_batch_job` method is `async` and run via `asyncio.run()` inside a sync thread — the
  driver UI would be at `http://localhost:4040` during execution.

**Fix required for UI-3:** Modify `_run_subprocess` to read stdout line-by-line during execution.
Have `batch_processing_job.py` emit `SPARK_JOB_PROGRESS: {...}` structured lines at key milestones.
Parse them in the `RunProgressTracker`. Also attempt to poll Spark driver REST API if reachable.
Fall back to indeterminate state if neither is available (per Section 2.1).

---

### Issue 15 — `nginx.conf` has no SSE-compatible proxy settings

The `/api/` location block is missing all SSE-required headers:

```nginx
# Missing:
# proxy_http_version 1.1;
# proxy_set_header Connection "";
# proxy_buffering off;
# proxy_read_timeout 3600s;
```

Without these, nginx will buffer SSE events and not flush them to the browser in real time.

**Fix required:** Add SSE-compatible headers per Section 3.7 of the master prompt.

---

### Issue 16 — `QueryClient` uses default retry rules; no error boundaries

`main.tsx` creates `new QueryClient()` with all defaults. Default retry is 3 times with exponential
backoff — a broken API causes a 30-second wait before showing an error. No error boundary wraps
the app or individual routes.

**Fix required:** Configure `QueryClient` with `retry: 1`, `staleTime: 30_000`. Wrap routes with
per-route error boundaries.

---

### Issue 17 — After run submission, UI does not navigate to run detail page

`PipelineManagement.tsx` shows a success/error banner after `runPipeline()` but does not navigate
to `/pipelines/${pipeline_run_id}`. After UI-2 (async runs), the expected flow is immediate
navigation.

**Fix required (UI-4):** After successful `runPipeline()` call, navigate to
`/pipelines/${execution.pipeline_run_id}`.

---

### Issue 18 — `backend/sample-configs/` directory does not exist

The master prompt says to load pipeline templates from `backend/sample-configs`. The directory was
not found in the full directory listing. Only `experiments/*.yaml` files exist (fault-injection
experiments, not pipeline configs).

**Fix required (UI-4):** Create `backend/sample-configs/` with three sample pipeline configs:
Sales (CSV), IoT (JSON), Network/Log.

---

## 3. UX and Completeness Gap Verification

| Gap | Verified |
|---|---|
| Dashboard lacks active pipelines, KPIs, trend charts | ✅ Only run count + experiments list |
| Dashboard Recent Experiments renders broken (Issue 2) | ✅ `exp.id` / `exp.status` are undefined |
| Dashboard `ServiceStatusBadge` always shows Connected (Issue 9) | ✅ Object truthy check |
| Pipeline Management has a plain `<textarea>` | ✅ `YamlEditor.tsx` is literally a `<textarea>` |
| No YAML upload, templates, or download | ✅ Confirmed |
| No stop/cancel for active pipeline run | ✅ Confirmed |
| `PipelineDetails` has no live progress, logs, services panel, output panel | ✅ Confirmed |
| Reports shows only recovery time bar chart | ✅ `ReportChart` only plots `recovery` field |
| Reports renders empty metric values (Issue 10) | ✅ Confirmed |
| No loading skeletons | ✅ Confirmed |
| No empty states with call to action | ✅ Only "No pipeline runs found" text |
| No error states with retry | ✅ Query errors silently ignored |
| No toasts | ✅ Confirmed |
| No confirmation dialogs before destructive actions | ✅ `cancelExperiment` fires immediately |
| Sidebar has no icons, no collapse | ✅ Confirmed |
| TopBar has no breadcrumbs, no service health strip | ✅ Confirmed |
| No way to view or download pipeline output | ✅ No endpoints, no UI |
| `RecoveryTimeline` only ever receives mock data | ✅ Confirmed |

---

## 4. Backend Completeness Gaps

The following endpoints required by the master prompt do not yet exist:

| Endpoint | Status |
|---|---|
| `POST /api/pipelines/run` → 202 async with immediate `running` row | ❌ Missing (synchronous, 200) |
| `GET /api/pipeline-runs/{id}/progress` | ❌ Missing |
| `GET /api/pipeline-runs/{id}/stream` (SSE) | ❌ Missing |
| `POST /api/pipeline-runs/{id}/cancel` | ❌ Missing |
| `GET /api/pipeline-runs/{id}/events` | ❌ Missing |
| `GET /api/pipeline-runs/{id}/output` | ❌ Missing |
| `GET /api/pipeline-runs/{id}/output/preview` | ❌ Missing |
| `GET /api/pipeline-runs/{id}/output/download` | ❌ Missing |
| `GET /api/experiments/{id}/events` | ❌ Missing |
| `RunProgressTracker` service | ❌ Missing |
| `pipeline_run_events` table + Alembic migration | ❌ Missing |

---

## 5. Mapping Table: Real Signals vs. Current Availability

| Visual / metric | Real data source | Currently stored? | Currently surfaced live? |
|---|---|---|---|
| Source rows total | `len(df)` after source read, in `context.metrics` as `rows_out` | ✅ Yes | ❌ No progress API |
| Kafka published count | `produce_batch()` returns `{"produced": N}` in `context.metrics["kafka_publish"]` | ✅ Yes | ❌ No progress API |
| Kafka consumer lag | Not applicable in batch mode | N/A — must show `null` | N/A |
| Spark task progress | Not available — `communicate()` blocks until completion | ❌ No mid-run | ❌ Requires streaming stdout or Spark REST |
| Spark records in/out | `SPARK_JOB_RESULT:` line → `input_rows`, `output_rows` | ✅ After job | ❌ Not mid-run |
| HDFS rows written | `write_meta["rows_written"]` from storage plugin | ✅ After write | ❌ No progress API |
| Experiment availability samples | `ExperimentRun.results["samples"]` (real per-second data) | ✅ Yes | ❌ Not typed in API response |
| Recovery time (real, seconds) | `ExperimentRun.results["recovery_time_seconds"]` | ✅ Yes | ⚠️ Raw blob only (snake_case) |
| Throughput (records/sec) | `ExperimentRun.results["throughput_records_per_sec"]` | ✅ Yes | ⚠️ Raw blob only |
| Experiment stage lifecycle | `ExperimentRun.stage` column updated in real-time | ✅ Yes | ✅ Via polling |

---

## 6. Prompt Adjustments Based on Reality

1. **`database.py` does not exist at `backend/app/database.py`:** The module is at
   `backend/app/database/session.py` (confirmed from imports). All planned files must use
   `from app.database.session import get_db`.

2. **`fault_injectors.py` is a package, not a single file:** The import is
   `from app.reliability.fault_injectors.base_fault import FAULT_INJECTOR_REGISTRY` — it lives
   under `app/reliability/fault_injectors/`. Injectors are registered via a dict.

3. **`backend/sample-configs/` does not exist.** Must be created in UI-4.

4. **Spark deploy mode:** `spark-submit --master spark://spark-master:7077` in default client mode
   runs the driver inside the backend container. The Spark driver UI would be at
   `http://localhost:4040`. Port exposure in docker-compose needs to be verified before UI-3.
   Primary progress mechanism: `SPARK_JOB_PROGRESS:` lines parsed from streaming stdout.

5. **`ExperimentRun.stage` is already updated in real-time** by the runner at every lifecycle
   transition. The `GET /api/experiments/{id}` endpoint correctly returns it as `lifecycleStage`.
   This is working; no backend change needed for the stepper, only for real log lines.

---

## 7. Test Infrastructure Observations

- `@testing-library/user-event` is **not** installed — needed for UI-1 interaction tests.
- `vitest-axe` / `jest-axe` are **not** installed — needed for accessibility tests in UI-9.
- No Playwright is configured — deferred to optional E2E test in UI-9.
- Existing tests (`ValidationErrors`, `ReliabilityInsight`, `LiveExperimentMonitor`) are structural
  only and will need to be updated when contracts change (per the master prompt requirement).

---

## 8. Planned File Change Index

| Phase | Key files touched |
|---|---|
| **UI-1** | `tailwind.config.js`, `index.css`, `package.json`, `api/client.ts`, `api/experiments.ts`, `api/pipelines.ts`, `api/system.ts`, `types/*.ts`, `context/AuthContext.tsx`, `components/ui/` (new), `components/layout/*.tsx`, `pages/Login.tsx`, `main.tsx`, `backend/app/core/config.py`, `.env.example` |
| **UI-2** | `backend/app/api/routes/pipelines.py`, `backend/app/api/routes/pipeline_runs.py`, `backend/app/services/pipeline_progress_tracker.py` (new), `backend/app/pipeline/executor.py`, `backend/alembic/` (new migration), `frontend/nginx.conf`, `backend/tests/` (new tests) |
| **UI-3** | `backend/app/spark/spark_job_submitter.py`, `spark-jobs/batch_processing_job.py`, `backend/app/api/routes/pipeline_runs.py`, `backend/app/messaging/kafka_producer.py` |
| **UI-4** | `frontend/src/pages/PipelineManagement.tsx`, `frontend/src/components/pipelines/YamlEditor.tsx`, `frontend/src/components/pipelines/PipelineList.tsx`, `backend/sample-configs/` (new) |
| **UI-5** | `frontend/src/pages/PipelineDetails.tsx` (full rewrite), `frontend/src/hooks/useRunLive.ts` (new) |
| **UI-6** | `backend/app/api/routes/pipeline_runs.py` (output endpoints), `frontend/src/pages/PipelineDetails.tsx` (output panel) |
| **UI-7** | `frontend/src/components/pipeline-scene/` (new), `frontend/src/pages/PipelineDetails.tsx` (scene integration) |
| **UI-8** | `frontend/src/pages/ReliabilityTesting.tsx`, `frontend/src/pages/Reports.tsx`, `frontend/src/components/experiments/*.tsx`, `frontend/src/components/reports/*.tsx`, `backend/app/api/routes/experiments.py` |
| **UI-9** | `frontend/src/pages/Dashboard.tsx`, `docs/README.md`, `docs/ui-overhaul.md` (new) |

---

## 9. Summary

All 8 confirmed issues from Section 1.1 of the master prompt are verified with exact file
references. Ten additional issues were found (Issues 9–18) that must be addressed. The most
architecturally significant findings are:

- **Issue 4** (synchronous run endpoint): blocks all live progress features — must be fixed in UI-2
  before any visualization work.
- **Issue 14** (no mid-run Spark progress): requires streaming stdout changes in UI-3.
- **Issues 2, 8, 10** (type/contract mismatches): cause silent rendering failures on Dashboard,
  PipelineDetails, and Reports — fix in UI-1.

The backend already has strong foundations: real per-second availability sampling, real Kafka
produce counts, real HDFS client, and a 12-stage experiment lifecycle. The gap is surfacing and
streaming these signals to the frontend.

**No code was changed in this phase.**
