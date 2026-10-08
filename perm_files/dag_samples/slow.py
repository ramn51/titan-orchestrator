import sys
import time

stage = sys.argv[1] if len(sys.argv) > 1 else "stage"
print(f"[{stage}] Long-running sample started.", flush=True)
for step in range(8):
    print(f"[{stage}] Progress {step + 1}/8", flush=True)
    time.sleep(5)
print(f"[{stage}] Complete.", flush=True)
