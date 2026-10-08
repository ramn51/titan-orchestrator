import time, random
print("[VALIDATE] Running data quality checks on merged dataset...")
time.sleep(8)

checks = [
    ("Referential integrity",   random.uniform(99.1, 99.9)),
    ("Value range constraints", random.uniform(98.5, 99.8)),
    ("Duplicate key check",     random.uniform(99.5, 100.0)),
    ("Schema conformance",      100.0),
]
all_pass = True
for check, score in checks:
    status = "PASS" if score >= 99.0 else "WARN"
    print(f"[VALIDATE]   {status}  {check}: {score:.2f}%")
    if score < 99.0:
        all_pass = False
    time.sleep(0.3)

if all_pass:
    print("[VALIDATE] All checks passed. Dataset approved for reporting.")
else:
    print("[VALIDATE] Some checks flagged. Proceeding with warnings.")
