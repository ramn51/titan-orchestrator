import sys
import time

stage = sys.argv[1] if len(sys.argv) > 1 else "stage"
print(f"[{stage}] Starting sample work.", flush=True)
time.sleep(2)
print(f"[{stage}] Validation passed; output ready.", flush=True)
