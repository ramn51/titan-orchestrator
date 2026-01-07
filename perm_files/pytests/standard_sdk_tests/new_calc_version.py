import time
import sys

def long_calculation_new_calc():
    print("[Calc] Starting complex calculation job...", flush=True)

    # Simulate initialization (2 seconds)
    time.sleep(2)
    print("[Calc] Step 1: Loading data...", flush=True)

    # Simulate processing (2 seconds)
    time.sleep(2)
    print("[Calc] Step 2: Crunching numbers...", flush=True)

    # Simulate finalization (2 seconds)
    time.sleep(2)
    print("[Calc] Step 3: Verifying results...", flush=True)

    # Actual calculation
    total = sum(range(101))

    # Final Result
    print(f"Result: {total}", flush=True)

if __name__ == "__main__":
    long_calculation_new_calc()