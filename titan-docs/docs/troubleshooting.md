# Troubleshooting

Start with the symptom. Each entry names where to look, the likely cause, and what to do.

| Symptom | Go to |
|---|---|
| A job failed and I need the reason | [My script failed](#my-script-failed) |
| Job sits in `PENDING`, nothing runs | [Nothing is running](#nothing-is-running) |
| Job never runs, shows as parked | [Parked jobs](#parked-jobs) |
| Job ended `DEAD` | [Dead-letter queue](#dead-letter-queue) |
| A worker will not register, or vanished | [Worker problems](#worker-problems) |
| `Worker unreachable: accepted no reply` | [The job never ran](#the-job-never-ran) |
| A service deployed but is not reachable | [Service problems](#service-problems) |
| A service shows `LIVE` but is dead | [Service problems](#service-problems) |
| The Timeline is empty for a pipeline | [No spans to show](#no-spans-to-show) |
| The DAG list shows everything as `PENDING` | [Status looks wrong](#status-looks-wrong) |
| Store disconnected, or dropped writes | [Store problems](#store-problems) |
| Ephemeral workers are not going away | [Workers will not descale](#workers-will-not-descale) |
| `running` is higher than occupied slots | [Orphaned work](#orphaned-work) |

---

## My script failed

### 1. Find out that it failed

Any of these will tell you:

- **Timeline tab** — the failures block lists every failed span, and always renders even when empty.
- **Cluster tab** — the `Dead letter` tile turns red when a job exhausted its retries.
- **SDK** — `client.get_job_status("DAG-my-job")` returns `FAILED` or `DEAD`.
- **DAG Visualizer** — the node turns red. Note the caveat on that page: live status resolves through
  the client-side manifest and can lag, so confirm against the Timeline.

### 2. Get the reason

The Timeline's failures block gives a **short reason** per attempt: the exit code plus the last
meaningful line of the traceback. The *last* line is deliberate, because that is the line naming the
error; the first only says `Traceback (most recent call last)`.

```
DAG-etl-transform #2 · FAILED on 127.0.0.1:8080
exit 1: ValueError: could not convert string to float: 'n/a'
```

Three reason shapes mean different things:

| Reason | Meaning | Where the fault is |
|---|---|---|
| `exit 5: ValueError: …` | The script ran and failed | Your code |
| `Deployment Failed: … port 9972 never became reachable after 20s` | Process started, never bound its port | Your service's startup |
| `Worker unreachable: … accepted no reply` | Transport failure | The cluster, **not your script** |

For the full stdout and stderr rather than the one-line summary:

- **Dashboard** — click the node in the DAG Visualizer for its live log stream, or open
  `/logs/<job_id>`.
- **SDK** — `client.fetch_logs("DAG-my-job")`.
- **Workspace files** — the file panel lists everything the job wrote, including any log or output
  file it produced itself.

### 3. Understand what already happened automatically

Before you re-run anything, know that Titan already tried:

- **Bounded retry.** A failed job is retried while `retryCount <= 3`, so **up to 3 retries, 4 attempts
  total**. Each attempt is its own span, which is why one job can draw four bars on the Timeline.
- **Fail-fast.** Deterministic failures are *not* retried — a service that never bound its port will
  not bind on a retry, so those are failed immediately rather than burning the retry budget.
- **Dead-letter.** Once retries are exhausted the job is marked `DEAD`, quarantined in the DLQ with
  its reason preserved, and **its dependent children are cancelled** so the rest of the graph is not
  left waiting on something that will never arrive.

So `FAILED` means it may still retry. `DEAD` means it is finished and nothing downstream will run.

### 4. Re-run it

| What you want | How |
|---|---|
| Re-run **one job**, immediately | Dashboard: **Replay** on the node. Re-submits that job with its dependencies stripped, so it runs now rather than waiting on parents. |
| Re-run **the whole pipeline** | Dashboard: **Redeploy** on the DAG. Replays the stored submission payload from the manifest. |
| Re-run after **fixing the script** | Re-deploy the file first (`client.deploy_script("path/to/job.py")`), then Replay or Redeploy. Replay re-runs the *stored job definition*, so it picks up the new script but not a changed job definition. |
| Re-run with a **changed definition** | Submit it again from the SDK or YAML. Job IDs are unique per run, so nothing is overwritten. |
| **Stop** the rest of the graph | Dashboard cancel, or `client.cancel_job("DAG-my-job")` if available in your SDK build. Cancelling cascades to children. |

!!! warning "Replay and Redeploy read the manifest"
    Both resolve the job or DAG payload from `.titan_dag_manifest.json` on the machine running the
    dashboard. If that file was lost or reset, they return `No manifest found` — re-submit from the
    SDK or YAML instead.

---

## Nothing is running

Cluster tab → **Why work isn't moving** → *Pending reasons*. The scheduler states why it could not
place the job on its last attempt, and the wording tells you which fix applies:

| Reason | Fix |
|---|---|
| `no worker registered with capability X` | Start a worker offering `X`. See [Parked jobs](#parked-jobs). |
| `N worker(s) match X but all are at capacity` | Wait, or add capacity. Not an error. |

If the `Ready queue` tile is red, there is queued work and **nothing running** — a genuine stall
rather than a busy cluster. Check the `Workers` tile is non-zero and the store is `UP`.

---

## Parked jobs

A job whose required capability no worker offers is moved out of the queue entirely and **parked**,
rather than being re-queued forever. Cluster tab → *Before dispatch* → **Parked** lane.

The capability panel prints the exact command. Start a matching worker and parked jobs are released
**automatically, event-driven** — no resubmit needed:

```bash
java -jar Worker.jar <port> <master-host> 9090 GPU
```

Parked jobs do not expire by default (`titan.scheduler.unschedulable.deadline.ms` is `0`), so they
accumulate until a worker appears or you cancel them.

---

## Dead-letter queue

Cluster tab → *Why work isn't moving* → **Dead-letter queue**. Each entry keeps the job ID, its
attempt count, and the reason it died. Non-zero means something is failing **repeatably**, not just
slowly. See [My script failed](#my-script-failed) for how to read the reason and re-run.

---

## Worker problems

**It never registered.** Check the worker's own startup output:

```
Capability:  GENERAL
Mode:        PERMANENT (Protected)
```

If that line is missing the process did not start. If it started but never appears in Topology,
the Master is unreachable from it — check port 9090 and any firewall between them.

**It was there and vanished.** Cluster tab → *Scaling history*. A `WORKER_LOST` entry with
`heartbeat failed` means the Master stopped getting answers on the 10-second heartbeat. Its
in-flight jobs are recovered and re-queued automatically.

**It registered as ephemeral when you wanted permanent.** Permanence is a startup argument, not a
property of the port. Pass it explicitly:

```bash
java -jar Worker.jar 8080 localhost 9090 GENERAL true
```

Without the trailing `true` the worker defaults to ephemeral and becomes eligible for reclaim.

---

## The job never ran

```
Worker unreachable: 127.0.0.1:8099 accepted no reply
(process gone, port closed, or network down). The job never ran —
this is not a script failure.
```

Transport failure, so do not debug your script. The worker died, its port closed, or the network
dropped between dispatch and reply. The job is re-queued through the normal retry path. If it
repeats, see [Worker problems](#worker-problems).

---

## Service problems

**Deployed but not reachable.** The reason will read
`port never became reachable after 20s`. The process started; the readiness probe failed. Either it
binds a different port than declared, it binds `127.0.0.1` instead of `0.0.0.0`, or it takes longer
than the readiness window to come up.

**Port already in use.** Titan pre-checks the port and refuses rather than deploying on top of a
squatter. Pick another port or stop what is holding it.

**Shows `LIVE` but is dead.** A known limitation: readiness is probed **once, at deploy, and never
re-probed**. `LIVE` means the Master still has it registered, not that it answered recently. Treat
the Services panel as a roster and confirm by dialling the address it lists.

**The deploy job says `COMPLETED` while the service runs.** That is correct, not a bug. A `SERVICE`
job finishes the moment readiness passes; the process it launched keeps serving.

---

## No spans to show

The Timeline's pipeline picker labels what is actually available:

| Label | Meaning |
|---|---|
| `· 33 spans` | In memory, draws immediately |
| `· 30 spans (disk only)` | Ran before this Master started — switch **window** to a **from disk** option |
| `· no spans on record` | Never dispatched (a parked job has no span), or it predates span persistence |

Spans live in a 5000-entry in-memory ring that is **cleared when the Master restarts**, plus
day-partitioned JSONL on disk retained for `titan.spans.retain.days` (7). If a whole window looks
empty, check the **window** selector first — a narrow window hides older spans.

---

## Status looks wrong

If the DAG list or graph shows stale or uniformly `PENDING` states, the live view is resolving
status through the client-side manifest, which is being reworked. The authoritative sources are:

- **Timeline** — reads execution spans
- **Cluster** — reads the store
- **SDK** — `client.get_job_status(job_id)` resolves from live state, then the store

Trust those when they disagree with the graph.

---

## Store problems

Cluster tab → the **TitanStore strip**.

- `Disconnected` — the store is not reachable at the endpoint shown. Titan **fails open**, so jobs
  keep running, but nothing is being persisted and a Master restart will lose in-flight state.
- **Dropped writes > 0** is the real outage signal. Because the store fails open, the error count
  stays at zero while writes are being discarded.
- **State drift** — the store lists workers the Master no longer has. Self-correcting; if it
  persists, the store has entries no live node is refreshing.

---

## Workers will not descale

Reclaim requires **all** of: the worker is ephemeral, not the root node, at zero load, hosting no
service, and idle for more than 45 seconds. Highest port first, one per cycle.

A node hosting a long-running service is never reclaimed, which is intended.

---

## Orphaned work

If the `Running` count exceeds occupied slots, the Master is counting jobs no live worker is
executing. The Saturation legend says so directly:
`N job(s) counted running with no live slot`.

This is recovered automatically — within one heartbeat cycle when the worker's death is detected, or
within 30 seconds by the reconciliation sweep for any other exit. If it **persists** beyond that,
that is a bug worth reporting.

---

## Useful settings

| Variable | Effect |
|---|---|
| `TITAN_MASTER_HOST`, `TITAN_MASTER_PORT` | Point the dashboard at a Master on another machine |
| `TITAN_DASHBOARD_DEMO=0` | Disable the demo runner, the only endpoint that mutates the cluster |
| `titan.spans.retain.days` | How long span history is kept on disk (default 7) |
| `titan.metrics.spans.max` | In-memory span ring size (default 5000) |
| `titan.scheduler.unschedulable.deadline.ms` | Expire parked jobs after this long (default 0, never) |
| `titan.worker.heartbeat.interval` | Heartbeat period in seconds (default 10) |

## Which page answers which question

| Question | Page |
|---|---|
| What ran, and did it succeed? | DAG Visualizer |
| Why did the run take as long as it did? | Execution Timeline |
| Why isn't work moving right now? | Cluster & Control Plane |
| Is the cluster getting slower over time? | Cluster → Analytics |
