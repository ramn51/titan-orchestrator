# Phase 3 — Logical Service Names and Address Injection

**Status:** not started
**Depends on:** Phase 1 (the address must be recorded before it can be named or injected)
**Independent of:** Phase 2 — neither blocks the other
**Estimated effort:** ~200 lines, 2 days including tests

---

## Why this one matters most

Phase 1 made services *resolvable*. A consumer can call `get_service_address(service_id)` and get back a reachable `(host, port)`.

That is the lookup. It is not yet discovery, because of what the caller must already know:

- **The service ID**, which is `DAG-<job_id>` — generated per deploy and different every time.
- **That it needs to call the SDK at all**, which means every consumer script imports `titan_sdk` and does a resolution step before doing its actual work.

So today, an agent that deploys a service and then generates twenty consumer jobs has to thread an identifier through twenty payloads by hand. That is the gap between *"you can resolve a service if you write code to"* and *"services are addressable."*

This phase closes it, and it is the phase that makes the project's central claim literally true:

> An agent can keep a service alive and dispatch batch jobs against it inside a single cluster.

Without name-based resolution and injection, that sentence requires a human to hardcode a port somewhere. With it, it doesn't.

---

## Design

Two independent additions. Either is useful alone; together they are the feature.

### A. Logical names — stable identity across redeploys

A service declares a **name** at deploy time. The name outlives any particular instance.

```
service:name:<logical>        -> SET of instance service IDs
service:<instance-id>:host    -> host          (Phase 1, exists)
service:<instance-id>:port    -> port          (Phase 1, exists)
service:<instance-id>:name    -> logical name  (new, for reverse lookup on teardown)
```

`safeRedisSadd` / `safeRedisSrem` / `safeRedisSMembers` already exist on the Scheduler, so the set primitive is there. Resolution by name picks any healthy member — which is also the seam where replicas and load balancing land later, without another redesign.

### B. Address injection — the consumer doesn't call anything

A job declares the services it needs. The Master resolves them **at dispatch time** and the worker injects them into the child process's environment.

```python
TitanJob(job_id="rank", filename="rank.py", needs_services=["embeddings"])
```

```python
# rank.py — no SDK import, no resolution step
host = os.environ["TITAN_SVC_EMBEDDINGS_HOST"]
port = os.environ["TITAN_SVC_EMBEDDINGS_PORT"]
```

Resolution happens at dispatch, not submission, so a service redeployed between submit and run still resolves correctly.

---

## Implementation

### 1. Wire format — follow the existing optional-suffix precedent

`TitanJob.to_string()` already ends with an optional trailing token:

```python
affinity_suffix = "|AFFINITY" if self.affinity else ""
return f"{self.id}|{header}|{payload_content}|{self.priority}|{self.delay}|{parents_str}{affinity_suffix}"
```

Add two more in the same style:

```python
name_suffix  = f"|SVCNAME:{self.service_name}" if self.service_name else ""
needs_suffix = f"|NEEDS:{','.join(self.needs_services)}" if self.needs_services else ""
return f"...{parents_str}{affinity_suffix}{name_suffix}{needs_suffix}"
```

Optional trailing tokens are backward compatible: an older Master ignores what it doesn't parse, and a job without them serializes byte-identically to today.

> ⚠️ Service names must exclude `|` and `,`. Validate at the SDK boundary and reject early with a clear error rather than letting a bad name corrupt the payload split. This is the in-band-delimiter constraint again — the same one that bites `DEPLOY_PAYLOAD` when a filename contains a pipe.

### 2. Master — register under the logical name

Extend `recordServiceAddress` (added in Phase 1):

```java
private void recordServiceAddress(String serviceId, String host, int port, String logicalName) {
    if (port <= 0) return;
    safeRedisSet("service:" + serviceId + ":host", host);
    safeRedisSet("service:" + serviceId + ":port", String.valueOf(port));
    safeRedisSadd("system:live_services", serviceId);

    if (logicalName != null && !logicalName.isEmpty()) {
        safeRedisSet("service:" + serviceId + ":name", logicalName);
        safeRedisSadd("service:name:" + logicalName, serviceId);
    }
    System.out.println("[DISCOVERY] Registered " + serviceId
            + (logicalName != null ? " as '" + logicalName + "'" : "") + " -> " + host + ":" + port);
}
```

And the teardown counterpart, which needs the reverse lookup to clean the name set:

```java
private void forgetServiceAddress(String serviceId) {
    String logicalName = redisKVGet("service:" + serviceId + ":name");
    if (logicalName != null && !logicalName.isEmpty()) {
        safeRedisSrem("service:name:" + logicalName, serviceId);
    }
    safeRedisSrem("system:live_services", serviceId);
    safeRedisSet("service:" + serviceId + ":host", "");
    safeRedisSet("service:" + serviceId + ":port", "");
}
```

### 3. Master — resolve at dispatch

In `executeStandardTask` / `executeRunOneOff`, before sending to the worker:

```java
/** Resolves each declared service name to host:port. Returns "" when the job needs nothing. */
private String resolveServiceEnv(Job job) {
    if (job.getNeedsServices() == null || job.getNeedsServices().isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    for (String name : job.getNeedsServices()) {
        String instanceId = pickHealthyInstance(name);        // any member; Phase 2 filters by health
        if (instanceId == null) {
            throw new RuntimeException("Unresolved service dependency: '" + name
                    + "' required by " + job.getId() + " has no live instance.");
        }
        String host = redisKVGet("service:" + instanceId + ":host");
        String port = redisKVGet("service:" + instanceId + ":port");
        if (sb.length() > 0) sb.append(",");
        sb.append(name.toUpperCase()).append("=").append(host).append(":").append(port);
    }
    return sb.toString();
}
```

**Fail loudly on an unresolved dependency.** A job that silently runs without its service will fail deeper in, with a worse error. Throwing here produces `Unresolved service dependency: 'embeddings'` in the dashboard, which is a diagnosis rather than a symptom.

The resolved string rides to the worker as another optional suffix on the `OP_RUN` payload.

### 4. Worker — inject into the environment

`ScriptExecutorHandler.java:223` already builds a `ProcessBuilder`, so this is three lines:

```java
ProcessBuilder pb = new ProcessBuilder(command);
pb.directory(executionDir);

for (String pair : serviceEnv.split(",")) {          // "EMBEDDINGS=10.0.0.5:9001"
    if (pair.isEmpty()) continue;
    String[] kv = pair.split("=", 2);
    String[] hp = kv[1].split(":", 2);
    pb.environment().put("TITAN_SVC_" + kv[0] + "_HOST", hp[0]);
    pb.environment().put("TITAN_SVC_" + kv[0] + "_PORT", hp[1]);
    pb.environment().put("TITAN_SVC_" + kv[0] + "_URL",  "http://" + kv[1]);
}
```

`ProcessBuilder.environment()` inherits the parent environment by default, so this adds without clobbering.

### 5. SDK

```python
def resolve_service(self, name):
    """Resolve a LOGICAL service name to (host, port). Returns None if no live instance."""
    resp = self._send_request(OP_RESOLVE_SERVICE, f"name:{name}")
    ...

def list_service_names(self):
    """Return the logical names that currently have at least one live instance."""
```

`OP_RESOLVE_SERVICE` (`0x58`, added in Phase 1) already multiplexes on payload shape — `*` lists, an ID resolves. Add a `name:` prefix for name resolution. No new opcode.

---

## Tests

| # | Scenario | Assertion |
|---|---|---|
| 1 | Deploy with `service_name="embeddings"` | `resolve_service("embeddings")` returns a reachable pair |
| 2 | Redeploy under the same name, new port | resolution follows the new instance; the old ID leaves the name set |
| 3 | Consumer with `needs_services=["embeddings"]` | `TITAN_SVC_EMBEDDINGS_HOST/PORT/URL` present in the child env and correct |
| 4 | Consumer connects using only the injected env | succeeds with no SDK import in the script |
| 5 | Unresolvable dependency | job fails fast with `Unresolved service dependency`, not a connection error |
| 6 | Two instances under one name | resolution returns one of them; both are in the set |
| 7 | Teardown | instance leaves `service:name:<n>`; last one out makes the name unresolvable |
| 8 | No `needs_services` declared | payload is byte-identical to the pre-change format (regression guard) |
| 9 | Name containing `\|` or `,` | rejected at the SDK with a clear error, never reaches the wire |

Test 4 is the one that proves the phase. If the consumer script needs no Titan import, injection genuinely works.

---

## The payoff, concretely

**Today:**
```python
SERVICE_PORT = 9999                                    # shared constant, by hand
svc = TitanJob(job_id="emb", filename="server.py", port=SERVICE_PORT, job_type="SERVICE")
job = TitanJob(job_id="rank", filename="rank.py", parents=["emb"])
# and rank.py hardcodes 9999, or imports the SDK and resolves DAG-emb
```

**After Phase 3:**
```python
svc = TitanJob(job_id="emb", filename="server.py", port=0,
               job_type="SERVICE", service_name="embeddings")     # port 0 = let the OS pick
job = TitanJob(job_id="rank", filename="rank.py", parents=["emb"],
               needs_services=["embeddings"])
```

Nobody names a port. That is the actual deliverable.

---

## Risks

- **Port 0 / ephemeral ports** is the natural next step once names exist, but the worker must report the *actual* bound port back, which it cannot currently do — the port is passed as argv and discarded. Treat dynamic ports as a follow-up, not part of this phase.
- **Stale entries in a name set.** If a worker dies without teardown, its instance ID lingers. Phase 2's health state is the correct filter; without it, `pickHealthyInstance` should at minimum verify the address still answers before returning it.
- **Name collisions.** Two services deployed under the same name both join the set. That is intentional (it is how replicas will work), but with no load balancing yet, resolution picks arbitrarily — document it rather than pretending it's a feature.
- **Resolution cost at dispatch.** Two KV reads per declared dependency, on the dispatch thread. Negligible, but if a job declares many services it belongs in the readiness executor rather than inline.

## Interaction with Phase 2

Independent, but they compose: with Phase 2, `pickHealthyInstance` filters on `service:<id>:health == UP` and resolution never returns a wedged instance. Without Phase 2, it returns any registered instance and the consumer finds out the hard way. Build Phase 3 first; the filter is a one-line upgrade afterwards.
