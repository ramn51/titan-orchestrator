# print('Result: ' + str(sum(range(101))))

import time
import sys

# Simulation of a heavy workload
print("--- [HEAVY TASK] Starting ---")

# This keeps the 'Slot' occupied for 30 seconds
# This ensures the Scaler (running every 15s) catches the worker in a busy state
time.sleep(30)

result = sum(range(101))
print(f"Result: {result}")
print("--- [HEAVY TASK] Finished ---")