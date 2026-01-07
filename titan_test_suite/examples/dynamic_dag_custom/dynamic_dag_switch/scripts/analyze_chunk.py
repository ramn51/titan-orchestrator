import time
import sys
import random

# Simulate processing a specific chunk
worker_id = random.randint(1000, 9999)
print(f"[WORKER-{worker_id}] Received data chunk.")

process_time = 3
print(f"[WORKER-{worker_id}] Crunching numbers (Duration: {process_time}s)...")
time.sleep(process_time)

print(f"[WORKER-{worker_id}] Analysis complete. Confidence Score: 98%.")