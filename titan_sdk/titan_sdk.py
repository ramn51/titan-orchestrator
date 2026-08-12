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
import os
import zipfile
import json as _json

# --- CONFIGURATION ---
TITAN_HOST = os.environ.get("TITAN_HOST", "127.0.0.1")
TITAN_PORT = int(os.environ.get("TITAN_PORT", 9090))
VERSION = 1
OP_SUBMIT_DAG = 4
OP_LOG_BATCH = 0x17
OP_GET_LOGS = 0x16
OP_UPLOAD_ASSET  = 0x53
OP_DEPLOY_SCRIPT = 0x57
OP_KV_SET = 0x60
OP_KV_GET = 0x61
OP_KV_SADD = 0x62
OP_KV_SMEMBERS = 0x63
OP_GET_JOB_STATUS = 0x55
OP_STOP = 0x07

class TitanJob:
    def __init__(self, job_id, filename, job_type="RUN_PAYLOAD", args=None,
             parents=None, port=0, is_archive=False, priority=1, delay=0, affinity=False, requirement="GENERAL",
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
        # When set, submit_dag auto-injects a HITL gate job after this job completes.
        # Downstream jobs that depend on this job are automatically re-wired to wait
        # for the gate approval before proceeding.
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
    def submit_dag(self, name, jobs, agent_run_id=None):
        """Submits a list of TitanJobs as a DAG.

        agent_run_id: optional string — links this DAG to a logical agent run
                      so the Agent Runs view in the dashboard can show all
                      stages of one run as a connected chain.
                      Has no effect on scheduling or execution.
        """
        jobs = self._inject_hitl_gates(jobs)
        print(f"[SDK] Submitting DAG: {name}")
        dag_payload = " ; ".join([j.to_string() for j in jobs])
        result = self._send_request(OP_SUBMIT_DAG, dag_payload)
        self._write_dag_manifest(name, jobs, dag_payload, agent_run_id=agent_run_id)
        return result

    def _inject_hitl_gates(self, jobs):
        """
        For every TitanJob with hitl_message set, automatically inserts an
        intermediate HITL gate job after it and re-wires all downstream jobs
        that depended on the original job to depend on the gate instead.

        Example — before injection:
            preprocess  (hitl_message="Approve before training")
            train        parents=["preprocess"]

        After injection:
            preprocess
            hitl-gate-preprocess   parents=["preprocess"]   ← auto-inserted
            train                  parents=["hitl-gate-preprocess"]  ← re-wired
        """
        # Locate hitl_gate.py (same directory as this SDK file, one level up in perm_files)
        _sdk_dir = os.path.dirname(os.path.abspath(__file__))
        _gate_candidates = [
            os.path.join(_sdk_dir, "..", "perm_files", "hitl_gate.py"),
            os.path.join(_sdk_dir, "hitl_gate.py"),
        ]
        gate_script = next((p for p in _gate_candidates if os.path.exists(p)), None)
        if gate_script:
            gate_script = os.path.abspath(gate_script)

        # Build a map of job_id → gate_id for every job that has hitl_message
        remap = {}   # original_id -> gate_id
        gates = []   # new gate TitanJob objects to insert

        for job in jobs:
            if not job.hitl_message:
                continue
            if gate_script is None:
                print(f"[SDK][WARN] hitl_gate.py not found — skipping HITL injection for '{job.id}'")
                continue
            gate_id = f"hitl-gate-{job.id}"
            remap[job.id] = gate_id
            safe_msg = job.hitl_message.replace("|", " ")
            gates.append(TitanJob(
                job_id   = gate_id,
                filename = gate_script,
                args     = f"{gate_id} {job.max_wait_seconds} {safe_msg}",
                parents  = [job.id],
                priority = job.priority,
            ))
            # Clear any stale decision from a previous run so the gate waits fresh
            self.store_put(f"titan:hitl:status:{gate_id}", "CLEARED")
            print(f"[SDK] HITL gate injected: {job.id} → {gate_id}")

        if not remap:
            return jobs   # nothing to inject

        # Re-wire: any job whose parent is in remap should point to the gate instead
        rewired = []
        for job in jobs:
            job.parents = [remap.get(p, p) for p in job.parents]
            rewired.append(job)
            # Insert the gate immediately after its source job
            if job.id in remap:
                rewired.append(next(g for g in gates if g.parents == [job.id]))

        return rewired

    def _write_dag_manifest(self, dag_name, jobs, dag_payload=None, agent_run_id=None):
        """Writes job→DAG mapping to .titan_dag_manifest.json for dashboard discovery."""
        manifest_path = ".titan_dag_manifest.json"
        try:
            import re as _re
            existing = {}
            if os.path.exists(manifest_path):
                with open(manifest_path) as f:
                    existing = _json.load(f)
            import time as _time
            run_ts = int(_time.time() * 1000)
            for job in jobs:
                full_id = f"DAG-{job.id}"
                full_deps = [f"DAG-{p}" for p in job.parents]
                existing[full_id] = {"dag": dag_name, "deps": full_deps, "run_ts": run_ts}
                if agent_run_id:
                    existing[full_id]["agent_run_id"] = agent_run_id

            # Track agent run summary — ordered list of stage DAG names
            if agent_run_id:
                key = f"__agent_run__{agent_run_id}"
                run_entry = existing.get(key, {"agent_run_id": agent_run_id, "stages": [], "run_ts": run_ts})
                if dag_name not in run_entry["stages"]:
                    run_entry["stages"].append(dag_name)
                run_entry["run_ts"] = run_ts
                existing[key] = run_entry
            # Store the full payload string so the dashboard can redeploy this DAG
            if dag_payload is not None:
                existing[f"__payload__{dag_name}"] = {"dag_payload": dag_payload, "run_ts": run_ts}
                # Store individual job payloads (parents stripped to []) for single-job replay
                for job_str in dag_payload.split(" ; "):
                    job_str = job_str.strip()
                    if not job_str:
                        continue
                    job_key = job_str.split("|")[0]
                    replay_str = _re.sub(r'\[[^\]]*\]', '[]', job_str)
                    existing[f"__job_payload__DAG-{job_key}"] = replay_str
            with open(manifest_path, 'w') as f:
                _json.dump(existing, f, indent=2)
            self._push_manifest(existing)
        except Exception:
            pass

    def _push_manifest(self, manifest_data):
        """Pushes the manifest to the remote dashboard so it can group pipelines correctly."""
        import urllib.request as _urllib
        dashboard_port = int(os.environ.get("TITAN_DASHBOARD_PORT", 5000))
        url = f"http://{TITAN_HOST}:{dashboard_port}/api/manifest/sync"
        try:
            body = _json.dumps(manifest_data).encode("utf-8")
            req = _urllib.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
            _urllib.urlopen(req, timeout=5)
        except Exception:
            pass  # Dashboard may not be running — non-fatal

    def submit_job(self, job):
        return self.submit_dag(job.id, [job])

    def submit_yaml(self, yaml_path, agent_run_id=None, wait=False,
                    poll_interval=2, timeout=300):
        """Parse a YAML pipeline file and submit it as a DAG.

        yaml_path:      path to a Titan YAML file
        agent_run_id:   links this DAG to a logical agent run in the Dashboard
        wait:           if True, blocks until all jobs reach a terminal state
        poll_interval:  seconds between status polls (only used when wait=True)
        timeout:        max seconds to wait before giving up (only used when wait=True)

        Returns the master's submit response string, or False if wait=True and
        the DAG did not complete within the timeout.

        Example — sequential agentic loop:

            client = TitanClient()
            run_id = uuid.uuid4().hex[:12]

            client.submit_yaml("bootstrap.yaml", agent_run_id=run_id, wait=True)

            for cycle in range(max_cycles):
                client.submit_yaml("arena_cycle.yaml", agent_run_id=run_id, wait=True)
                win_rate = read_win_rate("match_history.json")
                if meets_stopping_criterion(win_rate):
                    break
                elif win_rate < tau_da:
                    client.submit_yaml("da_gym.yaml", agent_run_id=run_id, wait=True)
                else:
                    client.submit_yaml("ca_gym.yaml", agent_run_id=run_id, wait=True)

            client.submit_yaml("export_deploy.yaml", agent_run_id=run_id, wait=True)
        """
        import time as _time
        from titan_sdk.titan_yaml_parser import TitanYAMLParser

        parser   = TitanYAMLParser(yaml_path)
        jobs     = parser.build_jobs()
        dag_name = f"{parser.get_project_name()}-{int(_time.time())}"
        result   = self.submit_dag(dag_name, jobs, agent_run_id=agent_run_id)

        if wait:
            completed = self._wait_for_dag(jobs, poll_interval=poll_interval, timeout=timeout)
            if not completed:
                return False

        return result

    def _wait_for_dag(self, jobs, poll_interval=2, timeout=300):
        """Block until every job in the list reaches a terminal state.

        Terminal states: COMPLETED, FAILED, REJECTED, ERROR
        HITL jobs stay in a non-terminal state until approved/rejected —
        this method waits for them too, so set timeout generously when
        a human decision is involved (e.g. timeout=3600 for a 1-hour gate).

        Returns True if all jobs completed, False if timeout was reached.
        """
        import time as _time

        TERMINAL = {"COMPLETED", "FAILED", "REJECTED", "ERROR"}
        pending  = {j.id for j in jobs}
        deadline = _time.time() + timeout

        while pending and _time.time() < deadline:
            done = set()
            for job_id in pending:
                status = (self.get_job_status(job_id) or "").strip().upper()
                if status in TERMINAL:
                    print(f"[SDK] {job_id} → {status}", flush=True)
                    done.add(job_id)
            pending -= done
            if pending:
                _time.sleep(poll_interval)

        if pending:
            print(f"[SDK] _wait_for_dag: timeout — still pending: {pending}", flush=True)
            return False

        return True
    
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

    def stop_service(self, service_id):
        """Tear down a running service by the job id it was deployed under.

        Sends OP_STOP to the Master, which forwards it to the worker hosting the
        service and forcibly terminates the service's process tree, then removes
        it from the live service registry.

        service_id: the job id used when the SERVICE / DEPLOY_PAYLOAD job was
                    submitted. The "DAG-" prefix is added automatically if absent
                    (services submitted via submit_dag / submit_job are keyed as
                    "DAG-<job_id>" on the Master).

        Returns the Master's response string (contains "STOPPED"/"SUCCESS" on
        success, or an "ERROR"/"COMMUNICATION_ERROR" message on failure).
        """
        if not service_id:
            raise ValueError("stop_service requires a non-empty service_id")
        prefixed = service_id if service_id.startswith("DAG-") else f"DAG-{service_id}"
        return self._send_request(OP_STOP, prefixed)

    def publish_artifact(self, key, filename):
        """Upload a local file to master and register it under key.

        Worker writes the file to CWD (titan_workspace/shared — local to this
        node only), then calls publish_artifact. Uploads to master's uploads/
        directory and stores the basename in TitanStore at key.

        Orchestrator retrieves with: get_artifact(key, save_path=...)

        Returns True on success, False on upload failure.
        """
        result = self.upload_file(filename)
        if result != "UPLOAD_SUCCESS":
            print(f"[SDK] publish_artifact: upload failed — {result}")
            return False
        self.store_put(key, os.path.basename(filename))
        return True

    def get_artifact(self, key, save_path=None):
        """Download a file previously published under key.

        Reads the basename from TitanStore, fetches the file from master's
        uploads/ directory. Returns True on success, False if the key has no
        artifact registered or the download fails.
        """
        filename = self.store_get(key)
        if not filename or filename in ("NULL", "CLEARED"):
            return False
        target = save_path or f"/tmp/{filename}"
        return self.fetch_artifact(filename, save_path=target)

    def deploy_script(self, filepath):
        """Deploy a worker script to perm_files/ on the master.

        Call this from an orchestrator before submitting a DAG that references
        the script by filename. After deployment the master's scheduler can
        locate the script via its normal perm_files/ scan.

        Usage:
            client.deploy_script("workers/my_worker.py")
            TitanJob(job_id="...", filename="my_worker.py", ...)

        Returns "DEPLOY_SUCCESS" on success, an error string otherwise.
        """
        if not os.path.exists(filepath):
            return f"ERROR: File not found at: {filepath}"

        real_path = os.path.abspath(filepath)
        clean_filename = os.path.basename(real_path)
        print(f"[SDK] Deploying script: {clean_filename}...")

        with open(real_path, 'rb') as f:
            b64_content = base64.b64encode(f.read()).decode('utf-8')

        payload = f"{clean_filename}|{b64_content}"
        return self._send_request(OP_DEPLOY_SCRIPT, payload)

    def upload_file(self, filepath):
        """Uploads a single file to master's uploads/ directory."""
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