# TitanStore versus Redis

This suite compares RESP-compatible Store backends without changing Titan's
scheduler or worker code. A valid comparison uses the same master and worker
VMs, JARs, configuration, scripts, duration sweep, and pass protocol. Only the
process listening on `titan.redis.host:titan.redis.port` changes.

The runner measures direct Store SET, GET, SADD, SMEMBERS and SREM latency plus
sequential operation throughput. These are the operations Titan's scheduler
uses. It can attach the `summary.json` from the five-pass task-granularity
runner for each backend and rejects scheduler summaries whose controlled
configuration does not match.

```powershell
python titan_test_suite/store_backend_benchmark/runner.py `
  --backend titanstore=10.168.0.2:6379 `
  --backend redis=10.168.0.2:6380 `
  --scheduler-summary titanstore=path/to/titanstore/summary.json `
  --scheduler-summary redis=path/to/redis/summary.json `
  --output benchmark_results/store-backend-comparison
```

Use separate ports only for the direct protocol probe. For scheduler runs,
restart the master against one backend at a time on the configured Store port.

