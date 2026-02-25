# ⚙️ Internals: Protocol & TitanStore

Titan achieves its zero-dependency architecture by implementing its own network communication and state management layers from scratch. This page covers how nodes talk to each other and how cluster state survives failure.

---

## 1. The Wire Protocol (`TITAN_PROTO`)

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

## 2. TitanStore Architecture (Powered by JKRedis)

To eliminate the Master as a Single Point of Failure (SPOF) and provide a distributed data bus, Titan integrates **TitanStore** (built on JKRedis). It is a multi-threaded, persistent Redis clone built from scratch in Java using standard I/O (`ServerSocket`), requiring no external frameworks like Netty.



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

While you can use TitanStore to pass data between your own Python tasks, the Titan Master heavily relies on it internally:

1. **Worker Registry:** Heartbeats and node capabilities are stored as ephemeral keys with TTLs. If a worker dies, its key naturally expires, triggering the failure detector.
2. **DAG Locks:** Dependencies are managed using atomic `SET` commands. Child tasks constantly check the store to see if their parent's execution flag has been set to `COMPLETED`.
3. **Log Streaming:** The web dashboard uses the Pub/Sub architecture to subscribe to specific job IDs, allowing the Master to stream physical stdout/stderr logs directly to the UI in real-time.