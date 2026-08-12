# Titan Orchestrator — Engineering Roadmap

**Scope:** Protocol v2, Security (mTLS + TitanVault), and supporting infrastructure
**Prepared:** May 2026
**Author:** Ram Narayanan

---

## Current State

Titan is a distributed DAG execution engine with:

- Java Master (port 9090) + Python Workers + TitanStore (Redis) + Flask Dashboard
- Capability-tag routing (GENERAL, GPU, HIGH_MEM)
- Health monitoring via heartbeats
- Auto-scale: workers join/leave dynamically
- Self-healing: master detects dead workers and reschedules jobs
- Storage: TitanStore KV + artifact upload/download
- Networking: push-based service discovery (not yet robust)
- Both ephemeral scripts (RUN_PAYLOAD) and long-running services (SERVICE)

**Current gaps:** No transport security, no secrets management, no config injection, no rollback support for services.

---

## Milestone 1 — Protocol v2

**Rationale:** The current wire format is a pipe-delimited UTF-8 text body embedded inside a binary frame header. Adding new fields requires changing positional indexes in both the Java parser and Python SDK simultaneously. A TLV binary body fixes this and is the foundation for mTLS (which must be done in the same pass to avoid two breaking protocol changes).

### 1.1 Header Expansion

```
Current (8 bytes):
  VERSION(1) | OPCODE(1) | FLAGS(1) | RESERVED(1) | BODY_LEN(4)

v2 (16 bytes):
  VERSION(1) | OPCODE(1) | FLAGS(1) | RESERVED(1) | REQUEST_ID(4) | RESERVED2(4) | BODY_LEN(4)
```

- `VERSION` bumped from 1 to 2
- `REQUEST_ID` enables correlating responses to requests and future multiplexing
- `FLAGS` byte gains meaning: bit 0 = compressed, bit 1 = encrypted body

### 1.2 Body — Pipe-delimited Text → Binary TLV

**Current body (text, positional):**
```
train|RUN_PAYLOAD|train.py|--lr 0.01|<base64_script>|GPU|5|0|[prep,data]
```

**v2 body (TLV: Tag 1 byte | Length 2 bytes | Value N bytes):**
```
0x01 | 0x0005 | "train"       JOB_ID
0x02 | 0x0001 | 0x00          HEADER_TYPE enum (0=RUN_PAYLOAD)
0x03 | 0x0008 | "train.py"    FILENAME
0x04 | 0x0009 | "--lr 0.01"   ARGS
0x05 | 0x0001 | 0x01          REQUIREMENT enum (1=GPU)
0x06 | 0x0001 | 0x05          PRIORITY
0x07 | 0x0004 | 0x00000000    DELAY (uint32)
0x08 | 0x0004 | "prep"        PARENT (repeatable tag)
0x09 | 0x0016 | <raw bytes>   SCRIPT PAYLOAD — no base64
```

Unknown tags are skipped by old parsers — new fields can be added without breaking deployed components.

### 1.3 Script Payload

Base64 encoding is removed for script payloads. Raw bytes are sent directly in the TLV value field. Saves ~33% on script size (relevant for large worker archives).

### 1.4 Dual-Parse During Rollout

Master supports both v1 and v2 during transition:

```java
byte version = header[0];
if (version == 2) parseTLV(body);
else              parseLegacyPipe(body);   // removed after full rollout
```

Upgrade order: Master → SDK → Workers → remove v1 path.

### 1.5 Raw Socket Helpers — Replace with SDK

Several worker scripts bypass the SDK with hand-rolled socket helpers:

- `pipeline_planner.py`
- `research_subtopic.py`
- `synthesize_report.py`
- `hitl_gate.py`

Replace with `TitanClient` SDK calls. These scripts get v2 for free when the SDK is upgraded.

### Files Changed

| File | Change |
|---|---|
| `TitanProtocol.java` | Tag constants for v2 TLV fields |
| `SchedulerServer.java` | Dual-parse inbound, TLV body parser |
| `Worker*.java` | Outbound to master + inbound job dispatch |
| `titan_sdk.py` | `TitanJob.to_string()` → TLV builder, `_send_request()` 16-byte header |
| Worker scripts (4) | Replace raw socket helpers with SDK |

### Effort

| Task | Estimate |
|---|---|
| Java dual-parse + TLV inbound parser | 1–2 days |
| Java worker outbound rewrite | 1 day |
| SDK `to_string()` + `_send_request()` | Half day |
| Replace raw socket helpers | Half day |
| Version negotiation + rollout logic | Half day |
| End-to-end testing | 1–2 days |
| **Total** | **4–6 days** |

---

## Milestone 2 — mTLS (Transport Security)

**Rationale:** Currently any process that reaches port 9090 can submit jobs, read/write any TitanStore key, and fetch artifacts. mTLS is the foundational security layer — certificate identity unlocks role-based authorization and is a hard prerequisite for TitanVault.

**Do Milestone 1 and Milestone 2 in the same pass** — both touch the Java network stack. Doing them separately means two breaking changes.

### 2.1 Certificate Setup

```
CA cert (self-signed, held by operator)
  ├── master.crt + master.key    (master node)
  ├── worker.crt + worker.key    (all worker nodes, shared or per-node)
  └── client.crt + client.key    (SDK clients + dashboard)
```

Generation: `openssl` or `keytool`. CA cert distributed to all nodes at deploy time.

### 2.2 Java Master

```java
// ServerSocket → SSLServerSocket
SSLContext ctx = SSLContext.getInstance("TLSv1.3");
ctx.init(keyManagerFactory.getKeyManagers(),
         trustManagerFactory.getTrustManagers(), null);
SSLServerSocket server = (SSLServerSocket)
    ctx.getServerSocketFactory().createServerSocket(9090);
server.setNeedClientAuth(true);   // mutual — client must present cert
```

### 2.3 Java Workers

Workers wrap their outbound connection to master with `SSLSocket`, presenting the worker cert.

### 2.4 Python SDK

```python
import ssl
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_cert_chain("client.crt", "client.key")
ctx.load_verify_locations("ca.crt")
sock = ctx.wrap_socket(socket.socket(), server_hostname="titan-master")
```

`_send_request()` uses the wrapped socket. Cert paths configurable via env vars:
```
TITAN_CERT=client.crt
TITAN_KEY=client.key
TITAN_CA=ca.crt
```

### 2.5 Role Enforcement via Cert Identity

The CN/SAN of the connecting certificate identifies the role:

| Cert CN | Allowed opcodes |
|---|---|
| `titan-worker` | KV set/get, log batch, upload artifact |
| `titan-client` | Submit DAG, KV set/get, upload/download |
| `titan-dashboard` | Read-only: stats, logs, KV get |

~50 lines in the Java opcode dispatcher. Rejects unauthorized calls with an error response.

### Files Changed

| File | Change |
|---|---|
| `SchedulerServer.java` | SSLServerSocket, client auth, role enforcement |
| `Worker*.java` | SSLSocket for outbound connections |
| `titan_sdk.py` | `_send_request()` uses SSLContext |
| `titan-dev.sh` | Cert generation step at startup |
| `config.properties` | Cert/key paths |

### Effort

| Task | Estimate |
|---|---|
| CA + cert generation script | Half day |
| Java SSLServerSocket + client auth | 1 day |
| Java worker SSLSocket | Half day |
| SDK SSLContext in `_send_request()` | Half day |
| Role enforcement (opcode allowlist) | Half day |
| Testing end-to-end with certs | 1 day |
| **Total** | **3–4 days** |

---

## Milestone 3 — TitanVault (Secrets Management)

**Hard dependency: Milestone 2 (mTLS) must be complete.**
Setting a secret over plaintext is worse than the current `.env` approach.

**Rationale:** API keys, database passwords, and service tokens currently travel as plaintext CLI args or sit in `.env` files on every worker node. TitanVault holds secrets encrypted on the master and injects them as environment variables at job dispatch time. Worker scripts never call the master for secrets — they just read `os.environ`.

### 3.1 Storage

```
master node disk
└── titan_vault.dat    AES-256-GCM encrypted JSON blob
                       decrypted into in-memory map at startup
                       never written plaintext to disk
```

In-memory representation:
```java
Map<String, String> vault = new LinkedHashMap<>();
// {"GEMINI_API_KEY": "sk-...", "DB_PASSWORD": "hunter2"}
```

### 3.2 Key Management

**Recommended: Option A — Passphrase at startup**

```bash
TITAN_VAULT_KEY=passphrase ./titan-dev.sh up
```

Master derives 256-bit AES key from passphrase using PBKDF2 (100,000 iterations, SHA-256). If `TITAN_VAULT_KEY` is not set at startup, vault does not load and jobs with `secrets=` fields fail with a clear error.

Future upgrade path: derive vault key from the master's mTLS private key (Option C) — no separate passphrase needed.

### 3.3 Encryption

Java standard library only — no external dependencies:

```java
// Encrypt
SecretKey key = deriveKey(passphrase, salt);   // PBKDF2
byte[] iv = new byte[12];
new SecureRandom().nextBytes(iv);
Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, iv));
byte[] encrypted = cipher.doFinal(plaintext.getBytes(UTF_8));
// Store: salt(16) + iv(12) + ciphertext

// Decrypt
// Read salt + iv from file header, derive key, decrypt
```

### 3.4 New Opcodes

```
OP_SECRET_SET = 0x70    payload: "KEY|value"     → "SECRET_OK" or "ERROR"
OP_SECRET_DEL = 0x71    payload: "KEY"            → "SECRET_OK" or "ERROR"
```

No `OP_SECRET_GET` — secrets are never returned to a client after being set. They exist only in master memory and in worker env vars at dispatch time.

### 3.5 SDK Interface

```python
client.set_secret("GEMINI_API_KEY", "sk-...")
client.delete_secret("GEMINI_API_KEY")
```

### 3.6 TitanJob — secrets field

```python
TitanJob(
    job_id="research",
    filename="research_worker.py",
    secrets=["GEMINI_API_KEY", "DB_PASSWORD"],
)
```

Serialised in TLV body as repeatable tag `0x0A`:
```
0x0A | 0x000E | "GEMINI_API_KEY"
0x0A | 0x000B | "DB_PASSWORD"
```

### 3.7 Dispatch Injection

At job dispatch the master looks up each secret name, injects into the worker process environment:

```java
ProcessBuilder pb = new ProcessBuilder("python", scriptPath, args);
Map<String, String> env = pb.environment();
for (String secretName : job.secrets) {
    String value = vault.get(secretName);
    if (value == null) {
        throw new RuntimeException("Secret not found: " + secretName);
    }
    env.put(secretName, value);
}
```

### 3.8 Worker Script — No Changes

```python
import os
api_key = os.environ.get("GEMINI_API_KEY")   # injected by master at dispatch
```

### Files Changed

| File | Change |
|---|---|
| `SchedulerServer.java` | Vault load/persist, two opcode handlers, env injection at dispatch |
| `AesGcm.java` (new) | Encrypt/decrypt utility |
| `titan_sdk.py` | `set_secret()`, `delete_secret()`, `secrets` field on `TitanJob` |
| `titan_yaml_parser.py` | `secrets` list field in YAML job definition |
| `sample_yaml_agent.yaml` | Example showing secrets usage |
| `config.properties` | `vault.file` path |

### Effort

| Task | Estimate |
|---|---|
| `AesGcm.java` encrypt/decrypt | Half day |
| Vault load/persist + in-memory map | Half day |
| Two opcode handlers (SET, DEL) | Half day |
| Env var injection at dispatch | Half day |
| SDK `set_secret()`, `delete_secret()` | Half day |
| `TitanJob.secrets` field + TLV serialisation | Half day |
| YAML parser `secrets` field | 1 hour |
| Testing | Half day |
| **Total** | **2–3 days** |

---

## Milestone 4 — Saved DAGs

**Rationale:** Currently the orchestrator must hold all job definitions. A worker script cannot trigger a sub-workflow. Saved DAGs allow pre-registered pipeline templates to be submitted by name from anywhere — including from inside a running worker — enabling true sub-workflow composition.

### 4.1 New Opcodes

```
OP_SAVE_DAG         = 0x58    payload: "name|dag_payload_string"
OP_SUBMIT_SAVED_DAG = 0x59    payload: "name"
OP_LIST_SAVED_DAGS  = 0x5A    payload: ""
OP_DELETE_SAVED_DAG = 0x5B    payload: "name"
```

### 4.2 Master Storage

In-memory map persisted to disk as JSON:

```java
Map<String, String> savedDags = new LinkedHashMap<>();
// {"arena_cycle": "<dag_payload>", "export_deploy": "<dag_payload>"}
```

### 4.3 SDK Interface

```python
# Register
client.save_dag("arena_cycle", jobs)

# Submit by name (from orchestrator or from inside a worker)
client.submit_saved_dag("arena_cycle", wait=True)

# List registered DAGs
client.list_saved_dags()

# Remove
client.delete_saved_dag("arena_cycle")
```

### 4.4 Usage Pattern

```python
# Pre-register templates once
client.save_dag("bootstrap",      bootstrap_jobs)
client.save_dag("arena_cycle",    arena_jobs)
client.save_dag("da_gym",         da_jobs)
client.save_dag("export_deploy",  export_jobs)

# Orchestrator loop — clean and declarative
client.submit_saved_dag("bootstrap", wait=True)

for cycle in range(nmax):
    client.submit_saved_dag("arena_cycle", wait=True)
    win_rate = read_win_rate("match_history.json")
    if meets_stopping_criterion(win_rate):
        break
    elif win_rate < tau_da:
        client.submit_saved_dag("da_gym", wait=True)
    else:
        client.submit_saved_dag("ca_gym", wait=True)

client.submit_saved_dag("export_deploy", wait=True)
```

### Effort

| Task | Estimate |
|---|---|
| Four new opcodes + master handlers | 1 day |
| Persist/load saved DAG map | Half day |
| SDK four new methods | Half day |
| Dashboard — Saved DAGs panel | 1 day |
| Testing | Half day |
| **Total** | **3 days** |

---

## Delivery Order

```
Milestone 1 (Protocol v2)  ──┐
                              ├── same pass, same PR
Milestone 2 (mTLS)         ──┘

Milestone 3 (TitanVault)   ── after Milestone 2 is merged and stable

Milestone 4 (Saved DAGs)   ── independent, can be done any time
```

---

## Total Effort Summary

| Milestone | Scope | Estimate |
|---|---|---|
| 1 — Protocol v2 | TLV binary body, remove base64, dual-parse rollout | 4–6 days |
| 2 — mTLS | Transport security, cert auth, role enforcement | 3–4 days |
| 1+2 combined | Single pass, shared network stack changes | 6–8 days |
| 3 — TitanVault | AES-GCM encrypted secrets, env injection | 2–3 days |
| 4 — Saved DAGs | Named pipeline templates, sub-workflow support | 3 days |
| **Total** | | **11–14 days** |

---

## What This Unlocks

After all four milestones:

- Any process on a trusted network can be a Titan client — no plaintext traffic
- Secrets never appear in logs, args, or `.env` files on worker nodes
- Worker scripts can trigger sub-workflows by name without knowing their structure
- New protocol fields can be added without coordinated upgrades across all components
- Role enforcement prevents workers from submitting DAGs or reading other jobs' secrets
