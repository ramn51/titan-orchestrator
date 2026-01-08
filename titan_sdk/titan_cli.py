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

import sys
import os
import time
from titan_sdk import TitanClient
from titan_yaml_parser import TitanYAMLParser

def get_stats(self):
    """Fetches JSON stats using the SDK's existing connection logic."""
    # This reuses your robust recv_all and socket handling
    response = self._send_command(OP_STATS_JSON, b"")
    return json.loads(response.decode('utf-8'))

def main():
    if len(sys.argv) < 3:
        print("Usage: python titan_cli.py deploy <agent.yaml>")
        return

    command = sys.argv[1]
    yaml_file = sys.argv[2]

    if command == "deploy":
        print(f"🚀 [Titan CLI] Parsing {yaml_file}...")
        try:
            parser = TitanYAMLParser(yaml_file)
        except Exception as e:
            print(f"❌ YAML Error: {e}")
            return
        
        client = TitanClient()
        project_name = parser.get_project_name()
        
        # --- UPLOAD STRATEGY ---
        if parser.is_project_mode():
            # STRATEGY A: Zip Entire Folder (Good for imports/dependencies)
            print(f"📦 [CLI] Packaging Project Mode: '{project_name}'...")
            resp = client.upload_project_folder(parser.project_base, project_name)
            if "ERROR" in resp:
                print(f"❌ Upload Failed: {resp}")
                return
        else:
            # STRATEGY B: Single Files (Good for simple scripts)
            print(f"📄 [CLI] Single File Mode detected.")
            # We don't need to upload manually here because TitanJob() 
            # loads the file content into Base64 automatically when is_archive=False.
            # The Protocol handles the upload inline.
            pass 
        
        # -----------------------

        # 2. Submit the DAG
        jobs = parser.build_jobs()
        print(f"🔗 [CLI] Submitting {len(jobs)} jobs...")

        gpu_jobs = [j.id for j in jobs if j.requirement != "GENERAL"]
        if gpu_jobs:
            print(f"[CLI] Specialized Hardware Request detected for: {gpu_jobs}")
        
        dag_id = f"{project_name}-{int(time.time())}"
        resp = client.submit_dag(dag_id, jobs)
        print(f"✅ [CLI] Deploy Response: {resp}")

if __name__ == "__main__":
    main()