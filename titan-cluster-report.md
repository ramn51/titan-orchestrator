# Titan cluster report

Generated 2026-09-17 01:13:07

## Fleet

| Node | Capability | Slots | Kind | Active job | Services |
|---|---|---|---|---|---|
| `127.0.0.1:8080` | [GENERAL] | 0/4 | ephemeral | — | 0 |
| `127.0.0.1:8096` | [GENERAL] | 0/4 | permanent | — | 0 |

## Performance

| Layer | p50 | p99 | max | samples |
|---|---|---|---|---|
| Queue wait (submit → dispatch) | 36ms | 3287ms | 3287ms | 28 |
| Dispatch placement | 6ms | 12ms | 12ms | 28 |
| Store write | 0ms | 3ms | 4ms | 289 |
| Store read | 0ms | 0ms | — | 0 |
| Heartbeat `127.0.0.1:8080` | 2ms | 5ms | 6ms | 13 |
| Heartbeat `127.0.0.1:8096` | 2ms | 3ms | 8ms | 12 |

## TitanStore

- **Connected** at `localhost:6379`
- 289 writes, 0 reads, **0 dropped writes**, 0 reconnects
- Last successful write 8473ms ago
- Store lists 2 workers / 0 services; Master has 2 workers
- 1 jobs would be recovered if the Master restarted now

## Scheduling health

- Ready queue **0**, parked **1**, dead-letter **1**
- Retry rate **10.7%** over 28 dispatches

| Capability | Waiting | Parked | Workers |
|---|---|---|---|
| GENERAL | 0 | 0 | 2 |
| TPU | 1 | 1 | 0  ⚠ dead end |

### Why queued work is not running

- `DAG-ex-tpu` — no worker registered with capability TPU

### Dead-letter queue

- `DAG-ex-bad` after 4 attempts — exit 1: RuntimeError: upstream API timeout after 30s

### Recent cluster events

- **WORKER_JOIN** — 127.0.0.1:8080 ephemeral
- **WORKER_JOIN** — 127.0.0.1:8096 permanent

---

_Series are in-memory ring buffers (~10 min at 1s resolution) and reset on Master restart._