"""Bounded worker task used by the Cluster Monitor presets."""

import sys
import time


stage = sys.argv[1] if len(sys.argv) > 1 else "stage"
seconds = min(20.0, max(0.1, float(sys.argv[2]))) if len(sys.argv) > 2 else 1.0
steps = max(1, min(8, round(seconds)))

print(f"[{stage}] Started; expected work {seconds:g}s", flush=True)
for step in range(steps):
    time.sleep(seconds / steps)
    print(f"[{stage}] Progress {step + 1}/{steps}", flush=True)
print(f"[{stage}] Finished successfully", flush=True)
