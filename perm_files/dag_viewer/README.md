# DAG viewer

The baseline worker dashboard stays in `server_dashboard.py`. It injects its
existing Titan RPC function into the DAG viewer blueprint.

- `catalog.py` joins SDK/YAML DAG definitions with the master's worker snapshot.
  The manifest is the source of run timestamps and DAG membership; the master
  supplies current execution state. Terminal history older than a run is ignored.
- `graph.py` lays out dependency levels and emits a small SVG with stable job IDs.
- `routes.py` exposes the page and `/api/dag_status`; each API request obtains a
  fresh master snapshot. `catalog.py` saves confirmed terminal outcomes in a
  run-scoped status sidecar so they remain visible after a master restart.
- `../templates/dag_viewer.html` contains structure, `../static/dag_viewer.css`
  handles presentation, `../static/dag_viewer.js` patches live state, and
  `../static/dag_viewer_zoom.js` scales only the graph SVG.
- The constructor uses browser-loaded React and Babel; no npm installation is
  needed. `dag_constructor_model.js` handles graph validation and code previews,
  `dag_constructor_app.jsx` handles editing, and
  `dag_constructor_builder.css` owns its visual theme.
- `submission.py` validates constructor jobs, builds the existing engine
  payload, and atomically records successful submissions for redeploy.
  `routes.py` also provides draft storage and a script picker/upload limited
  to `perm_files/dag_samples` (or `TITAN_DAG_SCRIPT_DIR`).
- The graph starts at 80% scale, with 50–150% zoom controls; its viewport
  scrolls horizontally and vertically. Current execution records from the
  master override old worker history so a failed retry does not inherit a
  previous run's completed state.
- Job files include the selected job's log and files written beneath
  `titan_workspace/shared/artifacts/<full-job-id>/`. Other files under
  `shared/` appear in a separate Shared workspace tab because their producer
  cannot be inferred from modification time.

Run `python perm_files/tests/test_dag_viewer.py` and
`python perm_files/tests/test_dag_submission.py` for focused checks.
The manifest represents the latest run of each job ID. The constructor
currently rejects HITL gates; its editor does not offer that setting.
