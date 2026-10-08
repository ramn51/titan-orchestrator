"""Tiny deterministic stand-in for a scientific task."""
import sys
import time

print(f"WfCommons surrogate {sys.argv[1]}: start", flush=True)
time.sleep(float(sys.argv[2]))
print(f"WfCommons surrogate {sys.argv[1]}: complete", flush=True)
