# Service work — open tickets

Two pieces of service support that are designed but not built. Both depend only on the address
recording that already ships; neither blocks the other.

Full specs were removed from `titan-docs` because they describe unbuilt work — user documentation
should only describe what runs. This file is the working record.

---

## TICKET-1 · Service health checking

**Status:** not started · **Effort:** ~150 lines, 1–2 days with tests

### Problem
The Master knows one thing about a running service: whether its PID exists. Readiness is probed
once at deploy and never again, so a service whose process dies quietly still reads LIVE on the
dashboard. The Services panel is a roster, not a health check.

### Two existing bugs this also fixes
- **Unbounded restart loop.** `ServiceHandler` restarts a crashing service every 3s with no
  ceiling and no backoff. Each restart spawns a fresh reader thread and `LogBatcher` and recurses
  inside the callback, so the old frames never unwind — a real resource leak, not just noise.
- **Crashes are invisible to the Master.** The worker restarts a service without reporting it,
  which is why the dashboard's `Deploys` column can only honestly count deploy attempts.

### Design
- Probe runs **on the worker**, not the Master — it already owns the process.
- TCP connect by default; optional HTTP path check.
- States: `STARTING → HEALTHY → UNHEALTHY → CRASH_LOOP`.
- Require **N consecutive failures (default 2)** before declaring DOWN — a busy service with a full
  accept backlog can refuse a connect while healthy.
- Bounded restarts with exponential backoff, replacing the flat 3s.

### Wire
Append health state to the existing heartbeat payload. **Backward compatible in both directions**
— the Master already parses with `parts.length` guards, so an old worker omitting the field is
fine and a new worker talking to an old Master has the extra field ignored. This is the same
append-only pattern used to add `host_cpu_pct` / `host_mem_pct` / `host_load_x100`.

### Open questions
- Restart budget: a plain counter reset on any successful probe means a service flapping every 11s
  never reaches `CRASH_LOOP`. A sliding window (5 restarts in 5 minutes) is stricter.
- `CRASH_LOOP` is currently terminal with no recovery path. Either accept teardown + redeploy as
  the recovery, or add a resurrect command.
- Probe cost is one TCP connect per service per 10s — negligible at tens of services, revisit at
  hundreds.

---

## TICKET-2 · Logical service names and address injection

**Status:** not started · **Effort:** ~200 lines, 2 days with tests

### Problem
To call a service today a consumer needs the service ID — which is `DAG-<job_id>`, regenerated on
every deploy — and has to import the SDK and perform a resolution step before doing its actual
work. The identity is unstable and the lookup is the caller's problem.

### Design
- **Logical names**: a stable name that survives redeploys, registered alongside the instance.
- **Address injection**: a job declaring `needs_services` gets `TITAN_SVC_<NAME>_HOST` and
  `TITAN_SVC_<NAME>_PORT` in its environment. The consumer calls nothing.
- Wire format follows the existing optional-suffix precedent — no new opcode.
- **Fail loudly on an unresolved dependency.** `Unresolved service dependency: 'embeddings'` is a
  diagnosis; silently running without the service produces a worse error deeper in.

### Open questions
- Stale entries: a worker that dies without teardown leaves its instance in the name set.
  TICKET-1's health state is the correct filter. Without it, resolution should at least verify the
  address answers before returning it.
- Name collisions are intentional (that is how replicas will work), but with no load balancing
  resolution picks arbitrarily. Document it rather than presenting it as a feature.
- Ephemeral ports (`port 0`) are the natural follow-up once names exist, but the worker currently
  takes the port as argv and discards it — it cannot report the actual bound port back. Separate
  piece of work.
- Resolution costs two KV reads per declared dependency. Inline on the dispatch thread is fine for
  a few; a job declaring many belongs in the readiness executor instead.

---

## Order

TICKET-2 delivers more day-to-day value — it removes a step from every consumer script.
TICKET-1 makes the Services panel trustworthy, which is the gap called out in the dashboard docs.

Do **TICKET-1 first** if the dashboard is the priority: it is the only panel that currently asserts
something it has not verified.
