import time, random, sys

shard = sys.argv[1] if len(sys.argv) > 1 else "SHARD_X"
print(f"[TRANSFORM:{shard}] Received shard. Starting transformations...")
time.sleep(8)

steps = ["Deduplication", "Schema normalization", "Null imputation", "Type casting"]
for step in steps:
    print(f"[TRANSFORM:{shard}]   -> {step}...")
    time.sleep(0.4)

dropped = random.randint(100, 500)
print(f"[TRANSFORM:{shard}] Complete. Dropped {dropped} invalid rows. Shard ready.")
