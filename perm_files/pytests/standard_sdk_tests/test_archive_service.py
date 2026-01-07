import os, sys
import time
import requests # pip install requests
import shutil

from titan_sdk import TitanClient, TitanJob

# --- CONFIG ---
SERVICE_NAME = "web_agent_v1"
SERVICE_PORT = 9999
SERVER_FILE = "server.py"

# 1. Create a simple HTTP Server Project
os.makedirs(SERVICE_NAME, exist_ok=True)

with open(f"{SERVICE_NAME}/{SERVER_FILE}", "w") as f:
    f.write(f"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

PORT = {SERVICE_PORT}

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Titan Service is ALIVE!")
        print(f"[SERVER] Handled Request from {{self.client_address}}")

print(f"[SERVER] Starting Agent on port {{PORT}}...")
# Flush stdout is critical for Titan logs!
sys.stdout.flush() 

httpd = HTTPServer(('0.0.0.0', PORT), SimpleHandler)
httpd.serve_forever()
""")

# 2. Upload & Deploy
client = TitanClient()

print("\n--- STEP 1: Upload Service Project ---")
resp = client.upload_project_folder(SERVICE_NAME)
print(f"Upload Response: {resp}")

print("\n--- STEP 2: Deploy Archive Service ---")
# Pointer: zip/entry_file
pointer = f"{SERVICE_NAME}.zip/{SERVER_FILE}"

# NOTE: job_type="SERVICE" + is_archive=True triggers START_ARCHIVE_SERVICE
job = TitanJob(
    job_id=f"SVC-TEST-{int(time.time())}", 
    filename=pointer, 
    job_type="SERVICE", 
    port=SERVICE_PORT,
    is_archive=True
)

resp = client.submit_job(job)
print(f"Service Deployment: {resp}")

# 3. Validation Loop
print(f"\n--- STEP 3: Pinging Service on Port {SERVICE_PORT} ---")
time.sleep(3) # Give it a moment to unzip and start python

try:
    url = f"http://127.0.0.1:{SERVICE_PORT}"
    response = requests.get(url, timeout=2)
    print(f"✅ SERVICE STATUS: {response.status_code}")
    print(f"✅ RESPONSE: {response.content.decode()}")
except Exception as e:
    print(f"❌ CONNECTION FAILED: {e}")
    # Fetch logs to debug why it failed
    print("--- SERVER LOGS ---")
    print(client.fetch_logs(job.id))

# 4. Cleanup (Optional: Stop the service)
# In a real scenario, you'd send an OP_STOP command. 
# For now, we leave it running or kill the worker.
print("\nTest Complete. You should see 'Titan Service is ALIVE!' above.")