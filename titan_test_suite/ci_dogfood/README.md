# Titan dogfood CI

GitHub Actions first builds and tests Titan conventionally. It then starts a
local TitanStore, master, and two workers before submitting a Titan DAG that
runs selected MCP, benchmark, and dashboard checks.

The bootstrap job remains independent of Titan. This avoids a circular test in
which a broken runtime prevents CI from reporting why the runtime did not start.
The dogfood job uploads stage results and service logs on both success and
failure.
