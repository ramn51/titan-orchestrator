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

import socket
import struct
import base64
import hashlib
import os
import copy
import re
import threading
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path
import json as _json

from .dag_journal import write_run as _write_dag_run

# --- CONFIGURATION ---
TITAN_HOST = "127.0.0.1"
TITAN_PORT = 9090
VERSION = 1
OP_SUBMIT_DAG = 4
OP_LOG_BATCH = 0x17
OP_GET_LOGS = 0x16
OP_UPLOAD_ASSET = 0x53
OP_KV_SET = 0x60
OP_KV_GET = 0x61
OP_KV_SADD = 0x62
OP_KV_SMEMBERS = 0x63
OP_GET_JOB_STATUS = 0x55
OP_CANCEL_JOB = 0x56
OP_DEPLOY_SCRIPT = 0x57
OP_RESOLVE_SERVICE = 0x58
_manifest_locks_guard = threading.Lock()
_manifest_locks = {}


@contextmanager
def _manifest_lock(path, timeout=60.0):
    """Serialize SDK manifest updates across clients without another dependency."""
    lock_path = os.path.abspath(path) + ".lock"
    with _manifest_locks_guard:
        local_lock = _manifest_locks.setdefault(lock_path, threading.RLock())
    with local_lock, open(lock_path, "a+b") as lock_file:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"0")
            lock_file.flush()
        deadline = time.monotonic() + timeout
        while True:
            try:
                lock_file.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for manifest lock: {lock_path}")
                time.sleep(0.01)
        try:
            yield
        finally:
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

class TitanJob:
    def __init__(self, job_id, filename, job_type="RUN_PAYLOAD", args=None,
             parents=None, port=0, is_archive=False, priority=1, delay=0, affinity=False,
             requirement="GENERAL", input_versions=None, expected_outputs=None,
             hitl_message=None, max_wait_seconds=172800):
        self.id = job_id
        self.filename = filename
        self.job_type = job_type
        self.args = args if args else ""
        self.parents = parents if parents else []
        self.port = port
        self.is_archive = is_archive
        self.priority = priority   # Default 1
        self.delay = delay         # Default 0 (ms or sec depends on Scheduler logic)
        self.affinity = affinity
        self.requirement = requirement
        self.input_versions = dict(input_versions or {})
        self.expected_outputs = list(expected_outputs or [])
        if hitl_message is not None and (not isinstance(hitl_message, str)
                                         or not hitl_message.strip() or len(hitl_message) > 500):
            raise ValueError("hitl_message must contain 1–500 characters")
        if hitl_message is not None and (not isinstance(max_wait_seconds, int)
                                         or not 1 <= max_wait_seconds <= 172800):
            raise ValueError("max_wait_seconds must be 1–172800")
        self.hitl_message = hitl_message
        self.max_wait_seconds = max_wait_seconds
        

        if not self.is_archive:
            self.payload_b64 = self._load_file(filename)
        else:
            self.payload_b64 = "REMOTE_ASSET"

    def _load_file(self, filename):
        # 1. Resolve Path (Absolute or Relative to CWD)
        if os.path.exists(filename):
            real_path = os.path.abspath(filename)
            # print(f"[SDK] Loading Job File: {real_path}") 
            with open(real_path, 'rb') as f: 
                return base64.b64encode(f.read()).decode('utf-8')
        
        # 2. Fail loudly if not found (No magic guessing)
        raise FileNotFoundError(f"❌ File '{filename}' not found. Please provide the correct absolute path.")

    def to_string(self):
        if self.job_type in ("SERVICE", "DEPLOY_PAYLOAD"):
            try:
                listening_port = int(self.port)
            except (TypeError, ValueError) as error:
                raise ValueError("Service jobs require a numeric listening port") from error
            if not 1 <= listening_port <= 65535:
                raise ValueError("Service jobs require a listening port from 1 to 65535")
        parents_str = "[" + ",".join(self.parents) + "]"
        simple_filename = os.path.basename(self.filename)
        safe_args = self.args.replace("|", " ")

        affinity_suffix = "|AFFINITY" if self.affinity else ""
        safe_req = self.requirement.replace("|", "") if self.requirement else "GENERAL"

        #  SERVICE / DEPLOYMENT ---
        # Handles: Worker.jar, Web Servers, Long-running Agents
        if self.is_archive:
            
            if self.job_type == "SERVICE" or self.job_type == "DEPLOY_PAYLOAD":
                # [ARCHIVE SERVICE] - Project Zip based
                # We replace the Base64 slot with 'args' since we don't send code.
                # Format: filename | args | port | REQUIREMENT
                # Eg: zip_name/entry.py | args | port | Req
                header = "START_ARCHIVE_SERVICE"
                payload_content = f"{self.filename}|{safe_args}|{self.port}|{safe_req}"
            
            else:

                header = "RUN_ARCHIVE"
                # Format: RUN_ARCHIVE | zip.zip/entry.py | args | Req
                # Eg: zip_name/entry.py | args | Req
                payload_content = f"{self.filename}|{safe_args}|{safe_req}"
        #  TASK / SCRIPT ---
        # Handles: Ephemeral Python scripts, One-off calculations
        # [INLINE SERVICE] - Single File (e.g. Worker.jar or server_dashboard.py)
        # This preserves your existing logic for Worker Deployment
        # Format: filename | base64 | port
        else:
            simple_filename = os.path.basename(self.filename)
            if self.job_type == "SERVICE" or self.job_type == "DEPLOY_PAYLOAD":
                header = "DEPLOY_PAYLOAD"
                payload_content = f"{simple_filename}|{self.payload_b64}|{self.port}|{safe_req}"
            else:
                header = "RUN_PAYLOAD"
                payload_content = f"{simple_filename}|{safe_args}|{self.payload_b64}|{safe_req}"

        return f"{self.id}|{header}|{payload_content}|{self.priority}|{self.delay}|{parents_str}{affinity_suffix}"

class TitanClient:
    def _inject_hitl_gates(self, jobs):
        """Insert a real worker checkpoint after each reviewed job and rewire its children."""
        ids = {job.id for job in jobs}
        if len(ids) != len(jobs):
            raise ValueError("DAG job IDs must be unique")
        gated = {}
        script = Path(__file__).with_name("hitl_gate.py")
        for job in jobs:
            if not job.hitl_message:
                continue
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", job.id):
                raise ValueError(f"Invalid HITL source job ID: {job.id}")
            gate_id = "hitl-gate-" + job.id
            if gate_id in ids:
                raise ValueError(f"Generated HITL gate ID already exists: {gate_id}")
            gated[job.id] = gate_id
        expanded = []
        for job in jobs:
            clone = copy.copy(job)
            clone.parents = [gated.get(parent, parent) for parent in job.parents]
            expanded.append(clone)
            if job.id in gated:
                gate_id = gated[job.id]
                encoded = base64.b64encode(job.hitl_message.encode("utf-8")).decode("ascii")
                gate = TitanJob(gate_id, str(script), args=f"DAG-{gate_id} {job.max_wait_seconds} {encoded}",
                                parents=[job.id], requirement="GENERAL")
                gate.hitl_prompt = job.hitl_message
                gate.hitl_source = job.id
                expanded.append(gate)
                print(f"[SDK] HITL gate injected: {job.id} -> {gate_id}")
        return expanded, gated

    def submit_dag(self, name, jobs, agent_run_id=None, agent_stage=None,
                   agent_stage_order=None, agent_run_name=None):
        """Submit jobs as a DAG, optionally grouping it as one agent stage.

        Agent metadata is dashboard-only: it does not alter scheduler behavior,
        job identifiers, dependencies, or the wire payload sent to the master.
        """
        agent_metadata = None
        if agent_run_id is not None:
            if not isinstance(agent_run_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", agent_run_id):
                raise ValueError("agent_run_id must use 1-128 letters, numbers, '.', '_', ':', or '-'")
            stage = agent_stage or name
            if not isinstance(stage, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", stage):
                raise ValueError("agent_stage must use 1-64 letters, numbers, '.', '_', ':', or '-'")
            if agent_stage_order is not None and (isinstance(agent_stage_order, bool)
                                                   or not isinstance(agent_stage_order, int)
                                                   or not 0 <= agent_stage_order <= 10000):
                raise ValueError("agent_stage_order must be an integer from 0 to 10000")
            display_name = agent_run_name or agent_run_id
            if not isinstance(display_name, str) or not 1 <= len(display_name) <= 160:
                raise ValueError("agent_run_name must contain 1-160 characters")
            agent_metadata = {"agent_run_id": agent_run_id, "agent_run_name": display_name,
                              "agent_stage": stage, "agent_stage_order": agent_stage_order}
        jobs, gates = self._inject_hitl_gates(jobs)
        for gate_id in gates.values():
            full_id = "DAG-" + gate_id
            response = self.store_put("titan:hitl:status:" + full_id, "CLEARED")
            if response != "OK":
                raise RuntimeError(f"Could not reset HITL gate {full_id}: {response}")
        print(f"[SDK] Submitting DAG: {name}")
        dag_payload = " ; ".join([j.to_string() for j in jobs])
        # Capture the boundary before the Master can finish a fast first job.
        import time as _time
        submitted_at = int(_time.time() * 1000)
        manifest_dir = os.path.dirname(os.path.abspath(
            os.environ.get("TITAN_DAG_MANIFEST", ".titan_dag_manifest.json")))
        workspace = os.environ.get("TITAN_DAG_WORKSPACE_DIR", os.path.join(manifest_dir, "titan_workspace"))
        offsets = {}
        for job in jobs:
            path = os.path.join(workspace, "shared", f"DAG-{job.id}.log")
            offsets[f"DAG-{job.id}"] = os.path.getsize(path) if os.path.isfile(path) else 0
        result = self._send_request(OP_SUBMIT_DAG, dag_payload)
        if result == "DAG_ACCEPTED":
            recorded_at = self._write_dag_manifest(
                name, jobs, submitted_at, dag_payload, offsets, agent_metadata)
            self._append_dag_run_history(
                name, jobs, recorded_at or submitted_at, agent_metadata)
        return result

    def _append_dag_run_history(self, dag_name, jobs, run_ts, agent_metadata=None):
        """Append the compact fields needed by historical dashboard charts."""
        manifest_path = os.path.abspath(os.environ.get(
            "TITAN_DAG_MANIFEST", ".titan_dag_manifest.json"))
        history_path = os.path.abspath(os.environ.get(
            "TITAN_DAG_RUN_HISTORY",
            os.path.join(os.path.dirname(manifest_path), ".titan_dag_run_history.jsonl")))
        record = {"name": dag_name, "run_ts": run_ts,
                  "jobs": [f"DAG-{job.id}" for job in jobs]}
        if agent_metadata:
            record.update(agent_metadata)
        try:
            encoded = (_json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8")
            descriptor = os.open(history_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(descriptor, encoded)
            finally:
                os.close(descriptor)
        except OSError as exc:
            print(f"[SDK][WARN] Could not append DAG run history: {exc}")

    def _write_dag_manifest(self, dag_name, jobs, run_ts, dag_payload=None, offsets=None,
                            agent_metadata=None):
        """Record a successful submission for dashboard discovery, atomically."""
        manifest_path = os.path.abspath(os.environ.get("TITAN_DAG_MANIFEST", ".titan_dag_manifest.json"))
        try:
            with _manifest_lock(manifest_path):
                return self._write_dag_manifest_unlocked(
                    manifest_path, dag_name, jobs, run_ts, dag_payload, offsets,
                    agent_metadata)
        except (OSError, TimeoutError, ValueError) as exc:
            print(f"[SDK][WARN] Could not update DAG journal: {exc}")

    def _write_dag_manifest_unlocked(self, manifest_path, dag_name, jobs, run_ts,
                                     dag_payload=None, offsets=None, agent_metadata=None):
        try:
            run_jobs = {}
            for job in jobs:
                full_id = f"DAG-{job.id}"
                full_deps = [f"DAG-{p}" for p in job.parents]
                run_jobs[full_id] = {"dag": dag_name, "deps": full_deps, "run_ts": run_ts,
                                     "log_offset": (offsets or {}).get(full_id, 0),
                                     "spec": {"script": os.path.basename(job.filename),
                                              "sha256": hashlib.sha256(base64.b64decode(job.payload_b64)).hexdigest()
                                              if not job.is_archive else None,
                                              "args": job.args, "requirement": job.requirement,
                                              "job_type": "service" if job.job_type in ("SERVICE", "DEPLOY_PAYLOAD") else "run",
                                              "port": job.port,
                                              "priority": job.priority, "delay_ms": job.delay,
                                              "input_versions": job.input_versions,
                                              "expected_outputs": job.expected_outputs,
                                              "hitl_message": getattr(job, "hitl_prompt", None),
                                              "hitl_source": getattr(job, "hitl_source", None)}}
                if agent_metadata:
                    run_jobs[full_id].update(agent_metadata)
            return _write_dag_run(manifest_path, dag_name, run_ts, run_jobs, dag_payload)["run_ts"]
        except (OSError, ValueError) as exc:
            print(f"[SDK][WARN] Could not update DAG journal: {exc}")

    def submit_job(self, job):
        return self.submit_dag(job.id, [job])
    
    def fetch_logs(self, job_id):
        return self._send_request(OP_GET_LOGS, job_id)

    def store_put(self, key, value):
        """Saves a string value to the distributed store (Redis)"""
        payload = f"{key}|{value}"
        return self._send_request(OP_KV_SET, payload)

    def store_get(self, key):
        """Retrieves a string value from the distributed store"""
        return self._send_request(OP_KV_GET, key)

    def store_sadd(self, key, member):
        """Adds a member to a set. Returns 1 if new, 0 if exists."""
        payload = f"{key}|{member}"
        resp = self._send_request(OP_KV_SADD, payload)
        try:
            return int(resp)
        except:
            return 0
        
    def store_smembers(self, key):
        """Returns a python list of all members in the set."""
        resp = self._send_request(OP_KV_SMEMBERS, key)
        
        if not resp:
            return []
        
        # Split CSV back into list
        return resp.split(",")

    def get_job_status(self, job_id):
        """Securely queries the Master for a job's internal system status."""
        return self._send_request(OP_GET_JOB_STATUS, job_id)

    def cancel_job(self, job_id):
        """Cancel a queued or running job and its dependent DAG jobs."""
        return self._send_request(OP_CANCEL_JOB, job_id)

    def resolve_service(self, service_id):
        """Return host|port for a ready service, or an error response."""
        return self._send_request(OP_RESOLVE_SERVICE, service_id)

    def stage_script(self, filepath):
        """Copy a local script into the master's permanent-files directory."""
        filename = os.path.basename(filepath)
        if not os.path.isfile(filepath):
            return f"ERROR: File not found: {filepath}"
        with open(filepath, "rb") as script:
            encoded = base64.b64encode(script.read()).decode("ascii")
        return self._send_request(OP_DEPLOY_SCRIPT, f"{filename}|{encoded}")

    def deploy_script(self, filepath):
        """Stage a worker script before DAG submission (SDK compatibility name)."""
        return self.stage_script(filepath)

    def upload_file(self, filepath):
        """Uploads a single file to Master's uploads directory."""
        # STRICT PATHING
        if not os.path.exists(filepath):
            return f"ERROR: File not found at: {filepath}"

        real_path = os.path.abspath(filepath)
        clean_filename = os.path.basename(real_path)
        print(f"[SDK] Uploading {clean_filename} from {real_path}...")

        with open(real_path, 'rb') as f:
            b64_content = base64.b64encode(f.read()).decode('utf-8')
        
        payload = f"{clean_filename}|{b64_content}"
        return self._send_request(OP_UPLOAD_ASSET, payload)

    def fetch_artifact(self, filename, save_path=None):
        """Used by a running job to download a file from Master"""
        print(f"[SDK] Fetching artifact: {filename}...")

        # OP_FETCH_ASSET = 0x54 (84)
        # Response is the Base64 string of the file
        b64_data = self._send_request(0x54, filename)

        if not b64_data or b64_data.startswith("ERROR"):
            print(f"[SDK] Failed to fetch: {b64_data}")
            return False

        # Decode and Save
        file_bytes = base64.b64decode(b64_data)

        target_path = save_path if save_path else filename
        with open(target_path, "wb") as f:
            f.write(file_bytes)

        print(f"[SDK] Saved to: {os.path.abspath(target_path)}")
        return True

    def upload_project_folder(self, folder_path, project_name=None):
        """Zips a folder and uploads it as project_name.zip"""
        # STRICT PATHING
        if not os.path.exists(folder_path):
             return f"ERROR: Folder not found at: {folder_path}"

        real_folder_path = os.path.abspath(folder_path)
        
        if not project_name:
            project_name = os.path.basename(real_folder_path)
        
        zip_filename = f"{project_name}.zip"
        print(f"[SDK] Zipping folder '{real_folder_path}' to '{zip_filename}'...")

        # Create Zip in the CURRENT WORKING DIRECTORY (Temporary)
        try:
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(real_folder_path):
                    for file in files:
                        if file == zip_filename: continue 
                        if "__pycache__" in root: continue
                        
                        file_path = os.path.join(root, file)
                        # Store relative path inside zip so unzipping is clean
                        arcname = os.path.relpath(file_path, real_folder_path)
                        zipf.write(file_path, arcname)
            
            # Upload the newly created zip
            print(f"[SDK] Uploading zipped project '{zip_filename}'...")
            # Re-use our strict upload_file method
            response = self.upload_file(zip_filename)
            
            return response
            
        finally:
            # Cleanup local zip file
            if os.path.exists(zip_filename):
                try: os.remove(zip_filename) 
                except: pass

    def _send_request(self, op_code, payload):
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((TITAN_HOST, TITAN_PORT))
            
            payload_bytes = payload.encode('utf-8')
            header = struct.pack('>BBBBI', VERSION, op_code, 0, 0, len(payload_bytes))
            s.sendall(header + payload_bytes)

            s.settimeout(10)
            
            # 1. Read First 8 Bytes
            initial_data = self._recv_exact(s, 8)
            if not initial_data: return "ERROR: No Response"

            is_valid_protocol = False
            resp_len = 0

            # 2. Smart Parse
            try:
                ver, op, flags, reserved, length = struct.unpack('>BBBBI', initial_data)
                if ver == 1: 
                    is_valid_protocol = True
                    resp_len = length
            except:
                is_valid_protocol = False

            if is_valid_protocol:
                if resp_len > 0:
                    response_bytes = self._recv_exact(s, resp_len)
                    return response_bytes.decode('utf-8')
                return ""
            else:
                # Fallback
                remaining_data = b""
                try:
                    while True:
                        chunk = s.recv(4096)
                        if not chunk: break
                        remaining_data += chunk
                except socket.timeout:
                    pass
                
                full_response = initial_data + remaining_data
                return full_response.decode('utf-8', errors='ignore')

        except Exception as e:
            return f"CONNECTION_ERROR: {e}"
        finally:
            if s: s.close()

    # def _recv_exact(self, sock, n):
    #     data = b''
    #     while len(data) < n:
    #         try:
    #             packet = sock.recv(n - len(data))
    #             if not packet: return None
    #             data += packet
    #         except:
    #             return data if len(data) > 0 else None
    #     return data

    def _recv_exact(self, sock, n):
        """Robustly receive exactly n bytes from the socket."""
        data = bytearray() # Use bytearray for efficient appending
        while len(data) < n:
            try:
                packet = sock.recv(min(n - len(data), 8192))
                if not packet:
                    # Connection closed early
                    return None
                data.extend(packet)
            except socket.timeout:
                # If we have some data, return it; otherwise, it's a true timeout
                return bytes(data) if data else None
            except Exception:
                return None
        return bytes(data)
