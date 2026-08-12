# Changelog

All notable changes to Titan Orchestrator are recorded here.
Format: `[version or date] — description`. Older history reconstructed from commits.

---

## [2026-05-17] — Session 2

### DAG Constructor — Undo / Redo
- History stack (max 60 snapshots) tracking structural changes: add node, delete node, add/delete edge, duplicate
- `Ctrl+Z` / `Ctrl+Y` / `Ctrl+Shift+Z` shortcuts; ↩ ↪ buttons in topbar disabled when nothing to undo/redo

### DAG Constructor — Multi-select
- Drag on empty canvas draws a blue selection rectangle; all nodes inside are selected
- `Shift+click` to add/remove individual nodes from selection
- Drag any selected node to move the whole group by delta
- `Delete` removes all selected nodes and their edges atomically
- Sidebar shows multi-select info panel when multiple nodes are selected

### DAG Constructor — Node duplication
- `Ctrl+D` or sidebar Copy button clones selected node, offsets +40px, appends `-copy` to Job ID

### DAG Constructor — Dirty state indicator
- Yellow `● unsaved` dot next to pipeline name when canvas differs from last deployed version
- Disappears after successful deploy; persists after Save Draft (draft ≠ deployed)

### DAG Constructor — Autosave + Save Draft button
- 2-second debounce autosave on any canvas change; writes to `.dag_constructor_states.json`
- Explicit **Save Draft** button for immediate save without deploying
- Topbar status indicator: `saving…` / `✓ draft saved` / `● unsaved`

### DAG Constructor — Redeploy confirmation
- Deploying a pipeline name that was previously deployed shows a confirmation dialog
- Prevents accidental double-deploys when intending to save draft only

### DAG Constructor — Cycle detection
- Client-side DFS cycle check runs before every deploy
- Blocks deploy with `"Cycle detected — check your dependencies."` if a cycle is found

### Bug fix — sendCallback retry (RpcWorkerServer)
- `sendCallback()` rewritten with 5-attempt exponential backoff (1s → 2s → 4s → 8s)
- Fixes jobs stuck as `RUNNING` when a single socket failure caused silent completion loss

### Bug fix — Worker re-registration after master restart
- Added 30-second periodic re-registration in `RpcWorkerServer.start()`
- Workers automatically rejoin the master registry after a master restart without manual intervention

### Bug fix — GPU requirement showing as GENERAL in visualizer
- Fixed key mismatch: `_write_constructor_manifest` wrote `"req"` but reader expected `"requirement"`
- Fixed `discover_dags_from_stats` overwriting `job_meta` without preserving the `requirement` field
- Fixed initial `job_meta` entry not including `requirement`

### Bug fix — Stale job entries in manifest after DAG rename
- `_write_constructor_manifest` now purges all existing entries for a DAG before writing the new job list
- Renamed jobs (e.g. `task3` → `task-final`) no longer leave ghost nodes in the visualizer

### Workers
- Added permanent GPU worker on port 8087 (`isPermanent=true`, capability=GPU)

### Documentation — DAG Constructor (new section, 5 pages)
- `constructor/overview.md` — layout, quick start, node types, capability routing
- `constructor/building-dags.md` — full field reference, multi-select, undo/redo, cycle detection, live codegen
- `constructor/hitl.md` — gate configuration, injection flow, timeout, YAML/SDK examples
- `constructor/managing-dags.md` — autosave, Save Draft, dirty state, Load DAG, redeploy
- `constructor/keyboard-shortcuts.md` — full shortcut reference

### Documentation — DAG Visualizer (new section, 5 pages)
- `visualizer/overview.md` — layout, how to open, live polling
- `visualizer/monitoring.md` — status colours, log panel, DAG list view
- `visualizer/hitl-approval.md` — approval banner, Approve/Reject flow, gate timeout
- `visualizer/workspace-files.md` — file panel, filtering, download
- `visualizer/agent-runs.md` — why the view exists, agent DAG vs regular DAG, stage drill-down

### Documentation — Screenshots
- 15+ real screenshots added across constructor and visualizer docs pages; all placeholder `!!! note` blocks replaced

### Documentation — index.md
- Updated hero dashboard screenshot to show GENERAL + GPU worker nodes
- Updated visualizer screenshot to live pipeline view
- Added Agent Runs to Built-In Dashboard section (was listed as "two views", now three)
- Removed duplicate `UI_Screenshot.png` reference

---

## [2026-05-17] — Session 1

### DAG Constructor — Script File selector
- Added `FileSelector` component: dropdown of all `.py` files in `perm_files/` replaces the plain text input
- Added "+ Upload" button that opens a hidden file input, uploads the script via `POST /api/upload_script`, and refreshes the dropdown in place
- Server: added `GET /api/perm_files` and `POST /api/upload_script` endpoints

### DAG Constructor — HITL fields
- Added **Gate Message** and **Gate Timeout** fields to the node sidebar
- HITL badge rendered on nodes that have a gate message set
- `genYaml` and `genSdk` codegen emit `hitl_message` / `max_wait_seconds` when set

### DAG Constructor — Load / Edit mode
- Added `LoadDagButton` component: dropdown of previously deployed DAGs, loads full canvas state (node positions, all field values, edges)
- Canvas state (nodes + edges) saved to `.dag_constructor_states.json` at deploy time
- Server: added `GET /api/dag/constructor_states` and `GET /api/dag/constructor_state/<name>` endpoints
- Fixed `_id` counter collision: on load, counter advances past the max loaded node ID

### DAG Constructor — Deploy redirect
- After successful deploy, "Deploy to Titan" button replaced by green **"✓ Deployed — View DAG →"** link opening the DAG visualizer for the submitted pipeline

### DAG Visualizer — Workspace Files panel
- Added files panel to DAG detail view showing output files from `titan_workspace/shared/`
- Files filtered by job short-name tokens (token expansion: `analyst-airflow` → matches "analyst" OR "airflow" in filename)
- Each file shows size, last-modified, and a direct download link
- Server: added `GET /api/workspace/files` and `GET /api/workspace/file/<filename>` endpoints

### DAG Submit — HITL gate injection (server-side)
- When a job has `hitl_message`, server injects a `hitl_gate.py` job between the source job and all its downstream dependents
- Gate is pre-loaded once (single base64 encode), stale KV decisions cleared at submit time
- No manual wiring needed in the constructor

### Workers
- Added permanent GPU worker (port 8086, `isPermanent=true`) to handle GPU-capability jobs without auto-scaler eviction

---

## [2026-05] — Agentic Orchestration (reconstructed from commits)

- Multi-agent orchestration support: `pipeline_planner.py`, `research_subtopic.py`, `synthesize_report.py`
- HITL gate: `hitl_gate.py` worker script + dashboard approval UI
- DAG Constructor initial version (`dag_constructor.html`)
- DAG Visualizer initial version (SVG graph, live polling, log drill-down)
- Stop/Cancel feature for running jobs (see `docs/stop-cancel-feature.md`)
- Cron scheduling support (see `docs/titan-roadmap-cron-and-agentic-apps.md`)
- SDK simplification: `TitanClient` API cleanup
- Cycle detection added to master scheduler

---

## Roadmap

See `TITAN_ROADMAP.md` for planned milestones:
- **M1** Protocol v2 (TLV binary body, remove base64)
- **M2** mTLS transport security + role enforcement
- **M3** TitanVault secrets management
- **M4** Saved DAGs (named pipeline templates, sub-workflow support)
