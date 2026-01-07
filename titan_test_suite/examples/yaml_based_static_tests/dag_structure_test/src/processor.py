import sys
import time
import os

mode = sys.argv[1] if len(sys.argv) > 1 else "UNKNOWN"
print(f"[{mode}] Running on Worker...")
print(f"[{mode}] Current Directory: {os.getcwd()}")

# Simulate work
time.sleep(1)
print(f"[{mode}] Complete.")