"""
Step 1 of HITL demo pipeline: Preprocess
Simulates a data preparation stage that produces a summary report
which a human then reviews before training begins.
"""
import time
import random

print("=" * 50)
print("[PREPROCESS] Starting data preprocessing...")
print("=" * 50)

# Simulate work
samples = random.randint(9800, 10200)
nulls   = random.randint(12, 47)
classes = {"cat": random.randint(490, 510),
           "dog": random.randint(490, 510),
           "bird": samples - random.randint(980, 1020)}

time.sleep(2)

print(f"\n[PREPROCESS] Dataset scan complete:")
print(f"  Total samples  : {samples}")
print(f"  Null values    : {nulls}  (dropped)")
print(f"  Class balance  :")
for label, count in classes.items():
    print(f"    {label:10s}: {count}")

# Write a summary file the next stage (and the human) can read
with open("preprocessing_report.txt", "w") as f:
    f.write(f"=== Preprocessing Report ===\n")
    f.write(f"Samples     : {samples}\n")
    f.write(f"Nulls dropped: {nulls}\n")
    f.write(f"Classes     : {classes}\n")
    f.write(f"Status      : READY_FOR_REVIEW\n")

print("\n[PREPROCESS] Report written to preprocessing_report.txt")
print("[PREPROCESS] Done. Waiting for human review before training starts.")
