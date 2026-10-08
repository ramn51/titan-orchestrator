import time, random
print("[INGEST] Starting data ingestion from 3 source systems...")
time.sleep(8)
sources = ["postgres://sales-db", "s3://events-bucket", "kafka://user-stream"]
for src in sources:
    print(f"[INGEST] Pulling records from {src}...")
    time.sleep(0.5)
total = random.randint(80000, 120000)
print(f"[INGEST] Done. {total:,} raw records loaded. Partitioning into 3 shards.")
