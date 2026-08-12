# ⚙️ Internals: Protocol & TitanStore

Titan achieves its zero-dependency architecture by implementing its own network communication and state management layers from scratch. This page covers how nodes talk to each other and how cluster state survives failure.

---

## 1. The Titan Protocol (`TITAN_PROTO`)

Titan does not rely on HTTP/REST or heavy gRPC layers for internal orchestration. To maximize performance and minimize the memory footprint of the core engine, node-to-node communication happens over simple TCP sockets using a custom binary protocol.

Every message sent across the cluster uses a fixed-header framing strategy. This ensures payload integrity and prevents the fragmentation issues inherent to plain TCP streams.

| Bytes | Field | Description |
| :--- | :--- | :--- |
| `0` | **Version** | Protocol version (currently `0x01`). |
| `1` | **OpCode** | The instruction (e.g., `0x04` = Submit DAG, `0x16` = Fetch Logs). |
| `2` | **Flags** | Bitmask for modifiers (e.g., Compression, Encryption). |
| `3` | **Spare** | Reserved for future use. |
| `4-7` | **Payload Length** | 32-bit unsigned integer defining the exact size of the incoming body. |
| `8+` | **Body** | The variable-length binary or string payload. |

This header adds exactly 8 bytes of overhead per message, achieving very low latency without JSON serialization overhead.

---

## 2. TitanStore Architecture

TitanStore is a separate Java project ([RedisJava](https://github.com/ramn51/RedisJava)) that ships as a pre-built JAR (`perm_files/TitanStore.jar`). It is a multi-threaded, persistent Redis-compatible server built from scratch using standard I/O (`ServerSocket`), requiring no external frameworks like Netty.

Titan connects to TitanStore via `TitanJRedisAdapter.java`, which speaks the RESP protocol over TCP (port 6379). TitanStore's source code, issues, and development are managed in its own repository — changes to the store are made there, not in the Titan orchestrator repo.



### Core Capabilities

* **In-Memory Storage:** Thread-safe key-value and set storage using `ConcurrentHashMap`.
* **RESP Protocol:** Full implementation of the **Redis Serialization Protocol**, making it fully compatible with official Redis clients (`redis-cli`, `Jedis`, etc.).
* **Concurrent Networking:** Handles multiple concurrent worker connections using a custom Thread Pool architecture.

### Advanced Systems Features

* **Persistence (AOF):** Implements **Append-Only File** logging. Every DAG state transition is flushed to disk. If the Master crashes, data survives and is replayed on startup for zero-loss Crash Recovery.
* **Real-Time Pub/Sub:** Implements the Publish/Subscribe messaging pattern using a fan-out architecture with `CopyOnWriteArrayList` to safely manage concurrent subscribers without blocking publishers.
* **Expiry & Eviction (TTL):** Supports millisecond-precision expiration using both Lazy Eviction (checked on access) and Active Eviction (a background probabilistic thread cleans up keys every 100ms).
* **Master-Replica Replication:** Supports full PSYNC handshakes and real-time command propagation for distributed high availability.

---

## 3. Supported Data Bus Commands

Tasks running on the cluster can interact with TitanStore using the Python SDK. Under the hood, the store supports the following core RESP commands, including standard Key-Value operations and Set mathematics.

| Command | Usage | Description |
| :--- | :--- | :--- |
| **`SET`** | `SET key value [PX ms]` | Stores a string value. Optional `PX` flag sets an automatic expiration timer. |
| **`GET`** | `GET key` | Retrieves a string value. Returns null if expired or missing. |
| **`SADD`** | `SADD key member` | Adds a member to a Set. Returns `1` if the member was added, `0` if it already existed. |
| **`SMEMBERS`**| `SMEMBERS key` | Returns all members currently stored in the specified Set. |
| **`SREM`** | `SREM key member` | Removes a specific member from a Set. Returns `1` if removed, `0` if it wasn't there. |
| **`PUBLISH`** | `PUBLISH channel msg` | Broadcasts a message to all workers subscribed to the channel. |
| **`SUBSCRIBE`**| `SUBSCRIBE channel` | Listens for real-time messages on a specific channel. |

---

## 4. How Titan Uses the Store

TitanStore *supports* Pub/Sub, TTL-based expiry, and atomic sets (see the capabilities above). The Titan Master, however, deliberately keeps its **control-plane logic in-memory and event-driven** — it uses the store for durable state and cross-node data passing, not as the mechanism for liveness, scheduling, or log delivery. The distinction matters, so here is what the Master actually does:

1. **Worker liveness — active heartbeat, not TTL expiry.** The Master runs a `HeartBeatExecutor` that periodically **dials each registered worker** (`OP_HEARTBEAT`) and tracks a "Last Seen" timestamp. If a worker fails to respond, the Master marks it dead and re-queues its jobs. Worker state is *mirrored* into the store for the dashboard, but the failure detector is the Master's dial loop — it does **not** rely on a key expiring.

2. **DAG readiness — event-driven, not polling.** Jobs with unmet dependencies sit in an in-memory `dagWaitingRoom` and cost nothing until their parents finish. When a job completes, `unlockChildren()` promotes any newly-ready children into the active queue. Child tasks do **not** poll the store for a parent's status; readiness is pushed by the completion event. (Job statuses *are* written to the store, but for durability and status queries, not as a scheduling poll loop.)

3. **Log streaming — worker push + dashboard poll, not Pub/Sub.** Workers stream stdout/stderr to the Master over `OP_LOG_STREAM` / `OP_LOG_BATCH` into an in-memory buffer. The web dashboard retrieves them by **polling** the Master's log endpoints (roughly every 2 seconds). TitanStore's Pub/Sub is available for user tasks, but it is **not** the path Titan uses to deliver logs to the UI.

Where the Master *does* lean on the store: persisting job/DAG state for **crash recovery** (see [Fault Tolerance &amp; Recovery](fault-tolerance.md)), and exposing the SDK's `store_put`/`store_get` key-value bus for tasks to pass data across nodes.