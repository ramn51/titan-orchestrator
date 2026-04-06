#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
hitl_gate.py — Human-in-the-Loop (HITL) checkpoint for Titan DAG tasks.

Usage (submit as a standalone job or call from within a DAG task):
    python hitl_gate.py <job_id> [message]

The script registers itself in TitanStore and blocks until an operator
approves or rejects the checkpoint via the Titan Dashboard.

Exit code 0  → APPROVED  (downstream DAG tasks will proceed)
Exit code 1  → REJECTED  (downstream DAG tasks will be blocked / failed)

Example DAG usage:
    from titan_sdk.titan_sdk import TitanClient, TitanJob
    client = TitanClient()
    jobs = [
        TitanJob("preprocess",  "preprocess.py"),
        TitanJob("hitl-review", "hitl_gate.py",
                 args="hitl-review 'Review preprocessed data before training'",
                 parents=["preprocess"]),
        TitanJob("train",       "train.py", parents=["hitl-review"]),
    ]
    client.submit_dag("ml-pipeline", jobs)
"""

import sys
import time
import socket
import struct

# --- Connection Config (override via env vars if needed) ---
import os
TITAN_HOST = os.environ.get("TITAN_HOST", "127.0.0.1")
TITAN_PORT = int(os.environ.get("TITAN_PORT", "9090"))

VERSION       = 1
OP_KV_SET     = 0x60
OP_KV_GET     = 0x61
OP_KV_SADD    = 0x62

HITL_QUEUE_KEY      = "titan:hitl:queue"
HITL_STATUS_PREFIX  = "titan:hitl:status:"
HITL_MSG_PREFIX     = "titan:hitl:message:"
HITL_TS_PREFIX      = "titan:hitl:ts:"

POLL_INTERVAL_S   = 5
DEFAULT_MAX_WAIT_S = 48 * 3600   # 48 hours


def _send(op_code, payload):
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((TITAN_HOST, TITAN_PORT))
        body = payload.encode('utf-8')
        header = struct.pack('>BBBBI', VERSION, op_code, 0, 0, len(body))
        s.sendall(header + body)

        raw = b''
        while len(raw) < 8:
            chunk = s.recv(8 - len(raw))
            if not chunk:
                break
            raw += chunk
        if len(raw) < 8:
            return ""

        _, _, _, _, resp_len = struct.unpack('>BBBBI', raw)
        if resp_len == 0:
            return ""

        data = b''
        while len(data) < resp_len:
            chunk = s.recv(min(resp_len - len(data), 4096))
            if not chunk:
                break
            data += chunk
        return data.decode('utf-8', errors='ignore')
    except Exception as e:
        return f"ERROR: {e}"
    finally:
        if s:
            try:
                s.close()
            except Exception:
                pass


def kv_set(key, value):
    return _send(OP_KV_SET, f"{key}|{value}")


def kv_get(key):
    return _send(OP_KV_GET, key).strip()


def kv_sadd(key, member):
    return _send(OP_KV_SADD, f"{key}|{member}")


def main():
    if len(sys.argv) < 2:
        print("Usage: hitl_gate.py <job_id> [max_wait_seconds] [message]", flush=True)
        sys.exit(1)

    job_id = sys.argv[1]

    # argv[2] is optionally the timeout in seconds (always a plain integer when
    # injected by the SDK).  If it looks like a number treat it as the timeout;
    # everything after it is the human-readable message.  If it is not a number
    # (direct / legacy call), fall back to the default and treat argv[2:] as the
    # message so existing manual usage is not broken.
    max_wait_s = DEFAULT_MAX_WAIT_S
    msg_start  = 2
    if len(sys.argv) > 2 and sys.argv[2].lstrip("-").isdigit():
        max_wait_s = int(sys.argv[2])
        msg_start  = 3

    message = " ".join(sys.argv[msg_start:]) if len(sys.argv) > msg_start else "Awaiting human approval"

    # Check if this is a retry run — the Scheduler may re-run a failed job.
    # If a decision (APPROVED / REJECTED) was already recorded, honour it
    # immediately rather than overwriting with WAITING and looping forever.
    current_status = kv_get(HITL_STATUS_PREFIX + job_id)
    if current_status == "APPROVED":
        print(f"[HITL] Already approved (retry run) — proceeding.", flush=True)
        sys.exit(0)
    if current_status == "REJECTED":
        print(f"[HITL] Already rejected (retry run) — halting pipeline.", flush=True)
        sys.exit(1)

    # Fresh start — register this checkpoint in TitanStore
    kv_sadd(HITL_QUEUE_KEY, job_id)
    kv_set(HITL_STATUS_PREFIX + job_id, "WAITING")
    kv_set(HITL_MSG_PREFIX   + job_id, message)
    kv_set(HITL_TS_PREFIX    + job_id, str(int(time.time() * 1000)))

    deadline = time.time() + max_wait_s
    hours    = max_wait_s // 3600

    print(f"[HITL] ========================================", flush=True)
    print(f"[HITL]  Checkpoint registered: {job_id}", flush=True)
    print(f"[HITL]  Message : {message}", flush=True)
    print(f"[HITL]  Timeout : {hours}h ({max_wait_s}s)", flush=True)
    print(f"[HITL]  Open the Titan Dashboard → DAG Pipelines view", flush=True)
    print(f"[HITL]  and use the Approve / Reject buttons to proceed.", flush=True)
    print(f"[HITL] ========================================", flush=True)

    while True:
        if time.time() >= deadline:
            kv_set(HITL_STATUS_PREFIX + job_id, "TIMEOUT")
            print(f"[HITL] Timed out after {hours}h — no decision received. Failing pipeline.", flush=True)
            sys.exit(1)

        status = kv_get(HITL_STATUS_PREFIX + job_id)

        if status == "APPROVED":
            print(f"[HITL] Approved — proceeding with pipeline.", flush=True)
            sys.exit(0)

        elif status == "REJECTED":
            print(f"[HITL] Rejected — halting pipeline.", flush=True)
            sys.exit(1)

        else:
            remaining = int(deadline - time.time())
            print(f"[HITL] Still waiting... (status: {status or 'WAITING'}, {remaining}s remaining)", flush=True)
            time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
