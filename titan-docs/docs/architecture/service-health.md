# Phase 2 — Service Health Checking

**Status:** not started
**Depends on:** Phase 1 (service address must be recorded before it can be probed)
**Blocks:** nothing — Phase 3 is independent of this
**Estimated effort:** ~150 lines, 1–2 days including tests

---

## The problem

Titan currently knows exactly one thing about a running service: **whether its PID still exists.**

That signal comes from `process.onExit()` in `ServiceHandler.launchDetachedProcess`. It is event-driven — it fires once, when the process terminates — and nothing ever asks the service a question. The three registries (`runningServices`, `ProcessRegistry`, `liveServiceMap`) are all records of *intent*, written on start and mutated on exit. None is a measurement.

Everything below leaves `onExit` silent:

| Failure | PID alive | `onExit` fires | Detected today |
|---|---|---|---|
| Process crashed | no | yes | ✅ |
| Deadlocked / infinite loop | yes | no | ❌ |
| Died during init, port never bound | yes | no | ❌ |
| Bound to the wrong interface | yes | no | ❌ |
| Running, DB connection lost, all 500s | yes | no | ❌ |

Phase 1 added a **one-shot** TCP probe at deploy time (`awaitServiceReady`). Phase 2 turns that same probe into a **periodic** one and acts on the result.

## Two bugs this also fixes

### 1. The restart loop is unbounded

`ServiceHandler.java`, inside the `onExit` callback:

```java
} else {
    System.out.println("[WARN] Service Crashed: " + serviceId + " — restarting in 3s...");
    try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
    launchDetachedProcess(serviceId, executionDir, command);   // <-- recurses, forever
}
```

Three defects in five lines:

- **No ceiling.** A service that crashes on startup (bad config, missing dependency) restarts every 3 seconds until the worker dies.
- **No backoff.** Flat 3s regardless of how many times it has failed.
- **It recurses inside the callback.** Each restart spawns a fresh reader thread and a fresh `LogBatcher`, and the old frames never unwind. This is a genuine resource leak, not just noisy logging.

### 2. Crashes are invisible to the Master

The crash branch prints locally and relaunches. `notifyMasterOfServiceStop` is only called on *intentional* stop. So `liveServiceMap` never learns the service bounced, and the dashboard shows a healthy service through an infinite crash loop.

---

## Design

### Where the probe runs: on the worker

Not the Master. Three reasons:

1. The worker is local to the service — it's a `localhost` connect. Cheap, no firewall concerns.
2. It avoids N×M probe fan-out from a single Master.
3. The Master already polls every worker every 10s. **Piggyback on that** — no new opcode, no new connections.

The worker already has a `ScheduledExecutorService` (`RpcWorkerServer.java:177`, the 30s re-registration loop). Add a second one at a tighter interval, or reuse it.

### Probe types

| Level | Check | Catches | Needs cooperation |
|---|---|---|---|
| **TCP connect** | `new Socket(host, port)` succeeds | not listening, wrong interface, dead-on-init | no |
| **HTTP GET** | `/health` returns 200 in time | app-level brokenness, lost dependencies | yes — opt-in |
| **Process exit** | existing `onExit` | crash | no |

TCP connect is the default: it needs nothing from the user's service and catches most of the table above. HTTP is opt-in per service.

### Health states

```
STARTING  -> probing, within the initial grace window
UP        -> last probe succeeded
DOWN      -> last N probes failed, process still alive
RESTARTING-> process exited, relaunch scheduled
CRASH_LOOP-> exceeded the restart ceiling; no further restarts
STOPPED   -> intentional teardown
```

`CRASH_LOOP` is terminal. It exists so a broken service stops consuming resources and starts being visible instead.

---

## Implementation

### 1. Worker: bounded restart with exponential backoff

Replace the recursive `onExit` branch with a scheduled, counted relaunch.

```java
private static final Map<String, Integer> restartCounts = new ConcurrentHashMap<>();
private static final ScheduledExecutorService restartExecutor =
        Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "titan-svc-restart");
            t.setDaemon(true);
            return t;
        });

private static final int MAX_RESTARTS = TitanConfig.getInt("titan.service.restart.max", 5);
private static final long BASE_BACKOFF_MS = TitanConfig.getInt("titan.service.restart.backoff.ms", 3000);
private static final long MAX_BACKOFF_MS = 60_000;

// inside onExit, replacing the recursive call:
int n = restartCounts.merge(serviceId, 1, Integer::sum);
if (n > MAX_RESTARTS) {
    System.err.println("[CRASH_LOOP] " + serviceId + " exceeded " + MAX_RESTARTS + " restarts. Giving up.");
    serviceHealth.put(serviceId, "CRASH_LOOP");
    parentServer.notifyMasterOfServiceStop(serviceId);
    return;
}
long delay = Math.min(BASE_BACKOFF_MS * (1L << (n - 1)), MAX_BACKOFF_MS);  // 3s 6s 12s 24s 48s
System.out.println("[WARN] " + serviceId + " crashed (" + n + "/" + MAX_RESTARTS + "), restart in " + delay + "ms");
serviceHealth.put(serviceId, "RESTARTING");
restartExecutor.schedule(
        () -> launchDetachedProcess(serviceId, executionDir, command),
        delay, TimeUnit.MILLISECONDS);
```

Two things this changes beyond the ceiling: the relaunch happens on a **scheduler thread, not inside the callback**, so frames unwind; and `restartCounts` must be **cleared on a successful probe** so a service that runs fine for an hour and then crashes once gets a fresh budget.

### 2. Worker: the periodic probe

```java
private final Map<String, Integer> servicePorts = new ConcurrentHashMap<>();   // populated at start
private final Map<String, String>  serviceHealth = new ConcurrentHashMap<>();

healthExecutor.scheduleAtFixedRate(() -> {
    for (Map.Entry<String, Integer> e : servicePorts.entrySet()) {
        String id = e.getKey();
        if ("CRASH_LOOP".equals(serviceHealth.get(id)) || "STOPPED".equals(serviceHealth.get(id))) continue;
        boolean up = probe("127.0.0.1", e.getValue());
        serviceHealth.put(id, up ? "UP" : "DOWN");
        if (up) restartCounts.remove(id);          // healthy run resets the budget
    }
}, 10, 10, TimeUnit.SECONDS);
```

### 3. Wire: extend the existing heartbeat

Current (`RpcWorkerServer.java:244`):
```java
String stats = "PONG|" + activeThreads + "|" + maxThreads;
```

Extended:
```java
String health = serviceHealth.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining(","));
String stats = "PONG|" + activeThreads + "|" + maxThreads + (health.isEmpty() ? "" : "|" + health);
```

**This is backward compatible in both directions.** The Master already parses defensively (`if (parts.length > 1)`, `if (parts.length > 2)`), so an old worker that omits the field is fine, and a new worker talking to an old Master just has its fourth field ignored.

> ⚠️ Service IDs must not contain `|`, `,` or `=` for this encoding to survive. They are generated as `DAG-<job_id>`, so this holds today — but it is exactly the in-band-delimiter fragility described in the protocol notes. If job IDs ever become user-supplied, this breaks silently.

### 4. Master: consume it

In `checkHeartBeat`, after the existing `parts.length > 2` block:

```java
if (parts.length > 3 && !parts[3].isEmpty()) {
    for (String entry : parts[3].split(",")) {
        String[] kv = entry.split("=", 2);
        if (kv.length == 2) {
            serviceHealthMap.put(kv[0], kv[1]);
            safeRedisSet("service:" + kv[0] + ":health", kv[1]);
            if ("CRASH_LOOP".equals(kv[1])) {
                forgetServiceAddress(kv[0]);     // stop handing out a dead address
            }
        }
    }
}
```

### 5. Dashboard

Add `health` to each service entry in `getStatsJson()`. The visualizer reads `OP_STATS_JSON`, so a state chip appears with no dashboard-side protocol work.

---

## Tests

Run against a live cluster, red-first.

| # | Scenario | Assertion |
|---|---|---|
| 1 | Service binds, then the port is closed while the process lives | health flips `UP → DOWN` within 2 probe intervals |
| 2 | Service crashes once | restarts, health returns to `UP`, restart count resets |
| 3 | Service crashes on every start | exactly `MAX_RESTARTS` attempts, then `CRASH_LOOP`, then **no further launches** |
| 4 | Backoff timing | intervals approximate 3s, 6s, 12s, 24s, 48s |
| 5 | Thread hygiene | JVM thread count stable across 5 crash-restart cycles (guards the leak) |
| 6 | Master visibility | `CRASH_LOOP` appears in `OP_STATS_JSON` and the address is withdrawn from discovery |
| 7 | Old-worker compatibility | a worker sending 3-field `PONG` still registers and dispatches normally |

Test 5 is the one that matters most — it is the regression guard for the recursion leak, and it is the failure mode that is invisible until a worker falls over hours later.

---

## Risks

- **Probe cost.** One TCP connect per service per 10s. Negligible at tens of services; revisit at hundreds.
- **False DOWN on a busy service.** A service with a full accept backlog can refuse a connect while healthy. Require N consecutive failures (default 2) before declaring `DOWN`.
- **Restart budget semantics.** Resetting on any successful probe means a service that flaps every 11 seconds never reaches `CRASH_LOOP`. If that matters, switch to a sliding window (5 restarts in 5 minutes) instead of a plain counter.
- **`CRASH_LOOP` is terminal with no recovery path.** There is currently no "resurrect this service" command. Either accept that teardown + redeploy is the recovery, or add one.

## Out of scope

Replica sets, load balancing, rolling restarts, readiness-vs-liveness distinction beyond the deploy gate. Those need Phase 3's naming layer first.
