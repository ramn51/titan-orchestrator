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

# --- CONFIGURATION ---
TITAN_HOST = "127.0.0.1"
TITAN_PORT = 9090
VERSION = 1
OP_SUBMIT_DAG = 4
OP_LOG_BATCH = 0x17
OP_GET_LOGS = 0x16
OP_UPLOAD_ASSET = 0x53

class TitanJob:
    def __init__(self, job_id, filename, job_type="RUN_PAYLOAD", args=None,
             parents=None, port=0, is_archive=False, priority=1, delay=0, affinity=False, requirement="GENERAL"):
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
    def submit_dag(self, name, jobs):
        """Submits a list of TitanJobs as a DAG"""
        print(f"[SDK] Submitting DAG: {name}")
        dag_payload = " ; ".join([j.to_string() for j in jobs])
        return self._send_request(OP_SUBMIT_DAG, dag_payload)
    
    def submit_job(self, job):
        return self.submit_dag(job.id, [job])
    
    def fetch_logs(self, job_id):
        return self._send_request(OP_GET_LOGS, job_id)

    def upload_file(self, filepath):
        """Uploads a single file to Master's perm_files"""
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

    def _recv_exact(self, sock, n):
        data = b''
        while len(data) < n:
            try:
                packet = sock.recv(n - len(data))
                if not packet: return None
                data += packet
            except:
                return data if len(data) > 0 else None
        return data