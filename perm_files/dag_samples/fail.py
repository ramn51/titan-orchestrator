import sys
import time

stage = sys.argv[1] if len(sys.argv) > 1 else "stage"
print(f"[{stage}] Checking sample input.", flush=True)
time.sleep(2)
print(f"[{stage}] ERROR: sample input failed validation.", file=sys.stderr, flush=True)
raise SystemExit(1)
