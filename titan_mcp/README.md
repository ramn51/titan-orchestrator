# Titan MCP sidecar

`titan_mcp` exposes Titan through the Model Context Protocol without changing the Java engine, wire protocol, scheduler, Store or opcode list. It is a local stdio sidecar built on `TitanClient` and `OP_STATS_JSON`.

## Start it

The sidecar has no third-party dependency and supports Python 3.10 or newer.

```bash
TITAN_HOST=127.0.0.1 \
TITAN_PORT=9090 \
TITAN_MCP_ALLOWED_ROOTS=/absolute/workspace \
python -m titan_mcp
```

The default profile is read-only. Enable submission, cancellation, HITL decisions, asset transfer and namespaced KV writes explicitly:

```bash
TITAN_MCP_OPERATOR=true python -m titan_mcp
```

On Windows, use semicolon-separated allowed roots. On Unix, use colon-separated roots. Logs are written to stderr because stdout is the MCP wire.

## Tool groups

- Observe: cluster status, workers, services, queue, jobs, logs, timeline, metrics and span history.
- Execute: single jobs and DAGs, including services and agent-stage metadata.
- Control: cancellation, service resolution, service stop and HITL decisions.
- Data: guarded asset transfer and KV data under `user:mcp:` by default.

The server intentionally omits worker termination, stats deletion and unrestricted internal Store keys. Local paths are canonicalized, bounded by size and limited to `TITAN_MCP_ALLOWED_ROOTS`.

## Example MCP host configuration

```json
{
  "mcpServers": {
    "titan": {
      "command": "python",
      "args": ["-m", "titan_mcp"],
      "cwd": "/absolute/path/to/titan-orchestrator",
      "env": {
        "TITAN_HOST": "127.0.0.1",
        "TITAN_PORT": "9090",
        "TITAN_MCP_OPERATOR": "false",
        "TITAN_MCP_ALLOWED_ROOTS": "/absolute/workspace"
      }
    }
  }
}
```

## Verification

Run the dependency-free contract suite:

```bash
python -m unittest discover -s titan_mcp/tests -v
```

The two-VM live smoke gate passed on 2026-10-25. It initialized over stdio, inspected the cluster, submitted a dependency chain, verified logs, round-tripped namespaced Store data and a SHA-256 checked artifact, cancelled a running job, approved a HITL gate, deployed/resolved/stopped a service and confirmed timeline visibility. Run it with:

```bash
TITAN_HOST=127.0.0.1 TITAN_PORT=9090 \
python -m titan_mcp.tests.live_smoke --repo . --output evidence.json
```

Remaining release gates are MCP Inspector compatibility, a second independent MCP host, the full fault campaign, a 1000-task submission and cross-platform live runs.
