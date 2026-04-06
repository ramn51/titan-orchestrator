"""
Step 3 of HITL demo pipeline: Train
Only runs after the human approves the preprocessing report.
"""
import time
import random

print("=" * 50)
print("[TRAIN] Human approved — starting training run!")
print("=" * 50)

# Read the report from the shared workspace (written by preprocess.py)
try:
    with open("preprocessing_report.txt") as f:
        report = f.read()
    print("\n[TRAIN] Loaded preprocessing report:")
    for line in report.strip().splitlines():
        print(f"  {line}")
except FileNotFoundError:
    print("[TRAIN] (preprocessing_report.txt not found in shared workspace — continuing anyway)")

print()

# Simulate training epochs
epochs = 5
for epoch in range(1, epochs + 1):
    loss = round(1.0 / epoch + random.uniform(0.01, 0.05), 4)
    acc  = round(1.0 - loss + random.uniform(-0.02, 0.02), 4)
    print(f"[TRAIN] Epoch {epoch}/{epochs}  loss={loss:.4f}  acc={acc:.4f}")
    time.sleep(1)

print()
print("[TRAIN] Training complete.")
print(f"[TRAIN] Final accuracy: {acc:.4f}")
print("[TRAIN] Model checkpoint saved.")
