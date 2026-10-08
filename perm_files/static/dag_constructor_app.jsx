(function () {
  const { useEffect, useMemo, useRef, useState } = React;
  const model = window.TitanBuilderModel;
  const REQUIREMENTS = ['GENERAL', 'GPU', 'HIGH_MEM', 'PYTHON'];
  const CANVAS_WIDTH = 1280;
  const CANVAS_HEIGHT = 680;

  function Field({ label, children }) {
    return <label className="builder-field"><span>{label}</span>{children}</label>;
  }

  function JobCard({ node, selected, onConnect, onFinishLink, onDrag }) {
    return <div className={'builder-node ' + (selected ? 'selected ' : '') + node.kind}
      data-node-key={node.key} style={{ left: node.x, top: node.y }}>
      <button className="node-port input" title="Connect to this job"
        onPointerDown={event => event.stopPropagation()}
        onClick={event => { event.stopPropagation(); onFinishLink(node.key); }} />
      <div className="node-body" onPointerDown={event => onDrag(event, node)}>
        <span className="node-kind">{node.kind === 'service' ? 'SERVICE' : 'TASK'}</span>
        <strong>{node.id || 'Untitled job'}</strong>
        <small>{node.script || 'Choose a script'}</small>
      </div>
      <button className="node-port output" title="Connect this job to a child"
        onPointerDown={event => { event.stopPropagation(); onConnect(event, node); }} />
    </div>;
  }

  function Inspector({ node, selectionCount, nodes, edges, scripts, onChange,
                       onRemove, onRemoveSelection, onRemoveEdge, onUpload }) {
    if (selectionCount > 1) return <aside className="builder-inspector">
      <p className="eyebrow">GROUP SELECTION</p><h2>{selectionCount} jobs selected</h2>
      <p className="subtle">Drag one selected job to move the group. Copy with Ctrl+C,
        paste with Ctrl+V, or delete with Delete.</p>
      <button className="text-danger" onClick={onRemoveSelection}>Delete selected jobs</button>
    </aside>;
    if (!node) return <aside className="builder-inspector">
      <p className="eyebrow">INSPECTOR</p>
      <h2>Choose a job</h2>
      <p className="subtle">Select a node to edit its script, resources, timing, and dependencies.</p>
      <div className="hint">
        <b>Connect jobs</b>
        <p>Drag from the output port to a child job, or click the ports in sequence.</p>
        <b>Select and edit</b>
        <p>Shift+click to select several jobs. Drag on empty canvas for box selection.</p>
      </div>
    </aside>;
    const incoming = edges.filter(edge => edge.to === node.key);
    const update = change => onChange(node.key, change);
    return <aside className="builder-inspector">
      <div className="inspector-heading"><div><p className="eyebrow">JOB SETTINGS</p><h2>{node.id}</h2></div>
        <button className="text-danger" onClick={() => onRemove(node.key)}>Remove</button></div>
      <Field label="Job ID"><input value={node.id} onChange={event => update({ id: event.target.value })} /></Field>
      <Field label="Python script"><select value={node.script} onChange={event => update({ script: event.target.value })}>
        <option value="">Select a script</option>
        {scripts.map(file => <option key={file} value={file}>{file}</option>)}
      </select></Field>
      <label className="upload-link">+ Upload .py script<input type="file" accept=".py" hidden onChange={onUpload} /></label>
      <div className="field-row">
        <Field label="Type"><select value={node.kind} onChange={event => update({ kind: event.target.value })}>
          <option value="run">Task</option><option value="service">Service</option>
        </select></Field>
        <Field label="Requirement"><select value={node.requirement}
          onChange={event => update({ requirement: event.target.value })}>
          {REQUIREMENTS.map(value => <option key={value}>{value}</option>)}
        </select></Field>
      </div>
      <Field label="Arguments"><input value={node.args} placeholder="Optional"
        onChange={event => update({ args: event.target.value })} /></Field>
      <div className="field-row">
        <Field label="Priority"><input type="number" min="0" max="10" value={node.priority}
          onChange={event => update({ priority: Number(event.target.value) })} /></Field>
        <Field label="Delay (ms)"><input type="number" min="0" value={node.delay}
          onChange={event => update({ delay: Number(event.target.value) })} /></Field>
      </div>
      {node.kind === 'service' && <Field label="Port"><input type="number" min="0" max="65535"
        value={node.port} onChange={event => update({ port: Number(event.target.value) })} /></Field>}
      <label className="check-field"><input type="checkbox" checked={node.affinity}
        onChange={event => update({ affinity: event.target.checked })} /> Keep on parent worker</label>
      <div className="dependencies"><p className="eyebrow">PARENTS</p>
        {incoming.length ? incoming.map(edge => {
          const parent = nodes.find(item => item.key === edge.from);
          return <div className="dependency" key={edge.from}>
            <span>{parent?.id || 'Unknown'}</span>
            <button title="Remove dependency" onClick={() => onRemoveEdge(edge.from, edge.to)}>×</button>
          </div>;
        }) : <p className="subtle">No dependencies. This job can start immediately.</p>}
      </div>
    </aside>;
  }

  function CodePreview({ name, nodes, edges }) {
    const [tab, setTab] = useState('yaml');
    const [expanded, setExpanded] = useState(false);
    const code = tab === 'yaml' ? model.yaml(name, nodes, edges) : model.python(name, nodes, edges);
    return <section className={'builder-preview ' + (expanded ? 'expanded' : '')}>
      <div className="preview-heading"><strong>Generated code</strong>
        <button className={tab === 'yaml' ? 'active' : ''} onClick={() => setTab('yaml')}>YAML</button>
        <button className={tab === 'python' ? 'active' : ''} onClick={() => setTab('python')}>Python SDK</button>
        <button className="expand-button" aria-expanded={expanded} onClick={() => setExpanded(!expanded)}>
          {expanded ? 'Collapse ↑' : 'Expand ↓'}</button>
        <button className="copy-button" onClick={() => navigator.clipboard?.writeText(code)}>Copy</button>
      </div>
      <pre>{code}</pre>
    </section>;
  }

  function Builder() {
    const [name, setName] = useState('my-pipeline');
    const [nodes, setNodes] = useState([]);
    const [edges, setEdges] = useState([]);
    const [scripts, setScripts] = useState([]);
    const [drafts, setDrafts] = useState([]);
    const [selection, setSelection] = useState([]);
    const [linkFrom, setLinkFrom] = useState(null);
    const [wireCursor, setWireCursor] = useState(null);
    const [lasso, setLasso] = useState(null);
    const [panMode, setPanMode] = useState(false);
    const [message, setMessage] = useState('');
    const [deployed, setDeployed] = useState('');
    const [busy, setBusy] = useState(false);
    const [saveState, setSaveState] = useState('');
    const nextIndex = useRef(1);
    const initialLoad = useRef(true);
    const clipboard = useRef(null);
    const canvasRef = useRef(null);
    const scrollRef = useRef(null);
    const gestureCleanup = useRef(null);
    const selectedNode = selection.length === 1 ? nodes.find(node => node.key === selection[0]) : null;

    const refreshFiles = () => fetch('/api/perm_files').then(response => response.json())
      .then(data => setScripts(data.files || [])).catch(() => setMessage('Could not load scripts.'));
    const refreshDrafts = () => fetch('/api/dag/constructor_states').then(response => response.json())
      .then(data => setDrafts(data.states || [])).catch(() => {});
    useEffect(() => { refreshFiles(); refreshDrafts(); }, []);

    useEffect(() => {
      if (!nodes.length || initialLoad.current) {
        initialLoad.current = false;
        return;
      }
      setSaveState('Unsaved changes');
      const timer = setTimeout(async () => {
        try {
          const response = await fetch('/api/dag/save_draft', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, nodes, edges })
          });
          if (!response.ok) throw new Error();
          setSaveState('Draft saved');
          refreshDrafts();
        } catch { setSaveState('Draft save failed'); }
      }, 1500);
      return () => clearTimeout(timer);
    }, [name, nodes, edges]);

    useEffect(() => () => gestureCleanup.current?.(), []);

    function trackPointer(move, finish) {
      gestureCleanup.current?.();
      const onMove = event => move(event);
      const onUp = event => { cleanup(); finish(event); };
      const cleanup = () => {
        window.removeEventListener('pointermove', onMove);
        window.removeEventListener('pointerup', onUp);
        window.removeEventListener('pointercancel', onUp);
        gestureCleanup.current = null;
      };
      window.addEventListener('pointermove', onMove);
      window.addEventListener('pointerup', onUp);
      window.addEventListener('pointercancel', onUp);
      gestureCleanup.current = cleanup;
    }

    function canvasPoint(event) {
      const rect = canvasRef.current.getBoundingClientRect();
      return { x: event.clientX - rect.left, y: event.clientY - rect.top };
    }

    function scrollAtEdge(event) {
      const scroller = scrollRef.current;
      const rect = scroller.getBoundingClientRect();
      if (event.clientX > rect.right - 35) scroller.scrollLeft += 20;
      if (event.clientX < rect.left + 35) scroller.scrollLeft -= 20;
      if (event.clientY > rect.bottom - 35) scroller.scrollTop += 20;
      if (event.clientY < rect.top + 35) scroller.scrollTop -= 20;
    }

    function dragNode(event, node) {
      if (event.button !== 0 || linkFrom) return;
      event.preventDefault();
      event.stopPropagation();
      const keys = event.shiftKey
        ? (selection.includes(node.key)
          ? selection.filter(key => key !== node.key) : [...selection, node.key])
        : (selection.includes(node.key) ? selection : [node.key]);
      setSelection(keys);
      if (!keys.includes(node.key)) return;
      const bases = new Map(nodes.filter(item => keys.includes(item.key))
        .map(item => [item.key, { x: item.x, y: item.y }]));
      const startX = event.clientX, startY = event.clientY;
      const startScrollX = scrollRef.current.scrollLeft;
      const startScrollY = scrollRef.current.scrollTop;
      trackPointer(move => {
        scrollAtEdge(move);
        const dx = move.clientX - startX + scrollRef.current.scrollLeft - startScrollX;
        const dy = move.clientY - startY + scrollRef.current.scrollTop - startScrollY;
        setNodes(previous => previous.map(item => {
          const base = bases.get(item.key);
          return base ? { ...item, x: Math.max(8, base.x + dx), y: Math.max(8, base.y + dy) } : item;
        }));
      }, () => {});
    }

    function connect(from, to) {
      if (model.createsCycle(edges, from, to)) {
        setMessage('That connection would create a cycle.');
        setLinkFrom(null);
        return;
      }
      setEdges(previous => previous.some(edge => edge.from === from && edge.to === to)
        ? previous : [...previous, { from, to }]);
      setLinkFrom(null);
      setSelection([to]);
      setMessage('');
    }

    function startConnection(event, node) {
      if (event.button !== 0) return;
      event.preventDefault();
      setLinkFrom(node.key);
      const startX = event.clientX, startY = event.clientY;
      setWireCursor({ x: node.x + 172, y: node.y + 36 });
      trackPointer(move => {
        scrollAtEdge(move);
        setWireCursor(canvasPoint(move));
      }, end => {
        setWireCursor(null);
        const point = canvasPoint(end);
        const target = nodes.find(item => item.key !== node.key &&
          point.x >= item.x - 12 && point.x <= item.x + 184 &&
          point.y >= item.y - 12 && point.y <= item.y + 84);
        if (target) {
          connect(node.key, target.key);
        } else if (Math.hypot(end.clientX - startX, end.clientY - startY) > 6) {
          setMessage('Drop the arrow on a child job, or click its input port.');
        }
      });
    }

    function backgroundPointerDown(event) {
      if (event.target !== event.currentTarget && event.target.closest('.builder-node')) return;
      if (event.button !== 0 && event.button !== 1) return;
      event.preventDefault();
      const scroller = scrollRef.current;
      const startX = event.clientX, startY = event.clientY;
      if (panMode || event.button === 1) {
        const left = scroller.scrollLeft, top = scroller.scrollTop;
        trackPointer(move => {
          scroller.scrollLeft = left - (move.clientX - startX);
          scroller.scrollTop = top - (move.clientY - startY);
        }, () => {});
        return;
      }
      const start = canvasPoint(event);
      setLasso({ x: start.x, y: start.y, width: 0, height: 0 });
      trackPointer(move => {
        scrollAtEdge(move);
        const point = canvasPoint(move);
        setLasso({ x: Math.min(start.x, point.x), y: Math.min(start.y, point.y),
          width: Math.abs(point.x - start.x), height: Math.abs(point.y - start.y) });
      }, end => {
        const point = canvasPoint(end);
        const box = { left: Math.min(start.x, point.x), right: Math.max(start.x, point.x),
          top: Math.min(start.y, point.y), bottom: Math.max(start.y, point.y) };
        const found = nodes.filter(item => item.x < box.right && item.x + 172 > box.left &&
          item.y < box.bottom && item.y + 72 > box.top).map(item => item.key);
        setSelection(event.shiftKey ? [...new Set([...selection, ...found])] : found);
        setLasso(null);
      });
    }

    function addNode(kind) {
      const node = model.makeNode(kind, nextIndex.current++);
      setNodes(previous => [...previous, node]);
      setSelection([node.key]);
      setMessage('');
    }
    function updateNode(key, change) {
      setNodes(previous => previous.map(node => node.key === key ? { ...node, ...change } : node));
    }
    function removeNode(key) {
      setNodes(previous => previous.filter(node => node.key !== key));
      setEdges(previous => previous.filter(edge => edge.from !== key && edge.to !== key));
      setSelection(previous => previous.filter(item => item !== key));
      setLinkFrom(null);
    }
    function finishLink(to) {
      if (!linkFrom) return;
      connect(linkFrom, to);
    }
    function removeSelected() {
      const keys = new Set(selection);
      setNodes(previous => previous.filter(node => !keys.has(node.key)));
      setEdges(previous => previous.filter(edge => !keys.has(edge.from) && !keys.has(edge.to)));
      setSelection([]);
      setLinkFrom(null);
    }
    function copySelected() {
      const keys = new Set(selection);
      if (!keys.size) return;
      clipboard.current = {
        nodes: nodes.filter(node => keys.has(node.key)),
        edges: edges.filter(edge => keys.has(edge.from) && keys.has(edge.to))
      };
      setMessage(`${keys.size} job${keys.size === 1 ? '' : 's'} copied.`);
    }
    function pasteSelected() {
      if (!clipboard.current?.nodes.length) return;
      const keys = new Map();
      const usedIds = new Set(nodes.map(node => node.id));
      const clones = clipboard.current.nodes.map(node => {
        const key = `node-${Date.now()}-${nextIndex.current++}`;
        keys.set(node.key, key);
        let id = `${node.id}_copy`, suffix = 2;
        while (usedIds.has(id)) id = `${node.id}_copy_${suffix++}`;
        usedIds.add(id);
        return { ...node, key, id, x: node.x + 40, y: node.y + 40 };
      });
      setNodes(previous => [...previous, ...clones]);
      setEdges(previous => [...previous, ...clipboard.current.edges.map(edge =>
        ({ from: keys.get(edge.from), to: keys.get(edge.to) }))]);
      setSelection(clones.map(node => node.key));
      clipboard.current = { nodes: clones, edges: clipboard.current.edges.map(edge =>
        ({ from: keys.get(edge.from), to: keys.get(edge.to) })) };
      setMessage(`${clones.length} job${clones.length === 1 ? '' : 's'} pasted.`);
    }
    useEffect(() => {
      const onKey = event => {
        const target = event.target;
        if (target.closest('input, textarea, select, [contenteditable="true"]')) return;
        const mod = event.ctrlKey || event.metaKey;
        const key = event.key.toLowerCase();
        if (mod && key === 'c') { event.preventDefault(); copySelected(); }
        else if (mod && key === 'v') { event.preventDefault(); pasteSelected(); }
        else if (mod && key === 'd') { event.preventDefault(); copySelected(); pasteSelected(); }
        else if (mod && key === 'a') { event.preventDefault(); setSelection(nodes.map(node => node.key)); }
        else if (event.key === 'Delete' || event.key === 'Backspace') {
          if (selection.length) { event.preventDefault(); removeSelected(); }
        } else if (event.key === 'Escape') { setSelection([]); setLinkFrom(null); }
      };
      window.addEventListener('keydown', onKey);
      return () => window.removeEventListener('keydown', onKey);
    }, [selection, nodes, edges]);
    async function upload(event) {
      const file = event.target.files?.[0];
      if (!file) return;
      const form = new FormData();
      form.append('file', file);
      try {
        const response = await fetch('/api/upload_script', { method: 'POST', body: form });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Upload failed');
        await refreshFiles();
        if (selectedNode) updateNode(selectedNode.key, { script: data.filename });
        setMessage('Script uploaded: ' + data.filename);
      } catch (error) { setMessage(error.message); }
      event.target.value = '';
    }
    async function loadDraft(draftName) {
      if (!draftName) return;
      try {
        const response = await fetch('/api/dag/constructor_state/' + encodeURIComponent(draftName));
        if (!response.ok) throw new Error('Could not load draft.');
        const graph = model.migrateDraft(await response.json());
        setNodes(graph.nodes);
        setEdges(graph.edges);
        setName(draftName);
        setSelection([]);
        setLinkFrom(null);
        nextIndex.current = graph.nodes.length + 1;
        setMessage('Loaded draft ' + draftName);
      } catch (error) { setMessage(error.message); }
    }
    async function deploy() {
      const error = model.validate(name, nodes, edges);
      if (error) { setMessage(error); return; }
      setBusy(true);
      setMessage('');
      try {
        const response = await fetch('/api/dag/submit', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: name.trim(), jobs: model.jobsFromGraph(nodes, edges),
            _nodes: nodes, _edges: edges })
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Master rejected the DAG.');
        setDeployed(data.dag_id);
        setMessage('DAG accepted by Titan.');
        refreshDrafts();
      } catch (error) { setMessage(error.message); }
      setBusy(false);
    }

    const lookup = useMemo(() => Object.fromEntries(nodes.map(node => [node.key, node])), [nodes]);
    const canvasWidth = Math.max(CANVAS_WIDTH, ...nodes.map(node => node.x + 240));
    const canvasHeight = Math.max(CANVAS_HEIGHT, ...nodes.map(node => node.y + 140));
    return <div className="builder-shell">
      <div className="builder-titlebar">
        <div><p className="eyebrow">DESIGN STUDIO / WORKFLOW</p><h1>Build a pipeline</h1>
          <p className="subtle">Arrange jobs, connect dependencies, then deploy to Titan.</p></div>
        <div className="title-actions">
          <span className="save-state">{saveState}</span>
          {deployed && <a className="view-link" href={'/dags/' + encodeURIComponent(deployed)}>View DAG →</a>}
          <button className="deploy-button" disabled={busy} onClick={deploy}>{busy ? 'Deploying…' : 'Deploy pipeline'}</button>
        </div>
      </div>
      {message && <div className={'builder-message ' + (deployed ? 'success' : '')} role="status">{message}</div>}
      <div className="builder-main">
        <aside className="builder-toolbox">
          <p className="eyebrow">PIPELINE</p>
          <Field label="Name"><input value={name} onChange={event => setName(event.target.value)} /></Field>
          <p className="eyebrow toolbox-heading">ADD STAGES</p>
          <button className="tool-add" onClick={() => addNode('run')}><span className="tool-icon task">▣</span>
            <span><b>Task</b><small>Run a Python script</small></span><strong>+</strong></button>
          <button className="tool-add" onClick={() => addNode('service')}><span className="tool-icon service">◈</span>
            <span><b>Service</b><small>Deploy a long-lived process</small></span><strong>+</strong></button>
          <div className="toolbox-divider" />
          <p className="eyebrow">STAGES <span>{nodes.length}</span></p>
          <div className="stage-list">{nodes.map(node =>
            <button key={node.key} className={selection.includes(node.key) ? 'active' : ''}
              onClick={event => setSelection(event.shiftKey ? [...new Set([...selection, node.key])] : [node.key])}><span className={'stage-dot ' + node.kind} />
              <span>{node.id || 'Untitled'}</span></button>)}
            {!nodes.length && <p className="subtle">Your stages will appear here.</p>}
          </div>
          <div className="toolbox-divider" />
          <Field label="Saved draft"><select value="" onChange={event => loadDraft(event.target.value)}>
            <option value="">Load a draft…</option>
            {drafts.map(draft => <option key={draft.name} value={draft.name}>{draft.name}</option>)}
          </select></Field>
        </aside>
        <div className="builder-workspace">
          <div className="canvas-toolbar"><span><b>Canvas</b> · {nodes.length} jobs · {edges.length} dependencies</span>
            <div className="canvas-actions">
              <button disabled={!selection.length} onClick={copySelected} title="Ctrl+C">Copy</button>
              <button disabled={!clipboard.current} onClick={pasteSelected} title="Ctrl+V">Paste</button>
              <button disabled={!selection.length} onClick={removeSelected} title="Delete">Delete</button>
              <button className={panMode ? 'active' : ''} onClick={() => setPanMode(!panMode)}>Pan</button>
            </div>
            {linkFrom ? <span className="link-prompt">Choose the child input port
              <button onClick={() => setLinkFrom(null)}>Cancel</button></span> :
              <span className="canvas-hint">Drag jobs · Connect output → input</span>}</div>
          <div className="canvas-scroll" ref={scrollRef}>
            <div className="builder-canvas" ref={canvasRef} onPointerDown={backgroundPointerDown}
              style={{ width: canvasWidth, height: canvasHeight }}>
              <svg width={canvasWidth} height={canvasHeight} aria-label="Dependency edges">
                <defs><marker id="builder-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4"
                  orient="auto"><path d="M0 0 L8 4 L0 8" fill="#7791ad" /></marker></defs>
                {edges.map(edge => {
                  const from = lookup[edge.from], to = lookup[edge.to];
                  if (!from || !to) return null;
                  const x1 = from.x + 172, y1 = from.y + 36, x2 = to.x, y2 = to.y + 36;
                  return <path key={edge.from + ':' + edge.to} className="builder-edge"
                    d={`M ${x1} ${y1} C ${x1 + 55} ${y1}, ${x2 - 55} ${y2}, ${x2} ${y2}`}
                    markerEnd="url(#builder-arrow)" />;
                })}
                {linkFrom && wireCursor && lookup[linkFrom] && <path className="builder-edge preview-edge"
                  d={`M ${lookup[linkFrom].x + 172} ${lookup[linkFrom].y + 36} L ${wireCursor.x} ${wireCursor.y}`}
                  markerEnd="url(#builder-arrow)" />}
              </svg>
              {nodes.map(node => <JobCard key={node.key} node={node} selected={selection.includes(node.key)}
                onConnect={startConnection} onFinishLink={finishLink} onDrag={dragNode} />)}
              {lasso && <div className="selection-box" style={{ left: lasso.x, top: lasso.y,
                width: lasso.width, height: lasso.height }} />}
              {!nodes.length && <div className="canvas-empty"><span>✦</span><h2>Your workflow starts here</h2>
                <p>Add a task from the left to build the first stage.</p></div>}
            </div>
          </div>
          <CodePreview name={name} nodes={nodes} edges={edges} />
        </div>
        <Inspector node={selectedNode} selectionCount={selection.length} nodes={nodes} edges={edges} scripts={scripts}
          onChange={updateNode} onRemove={removeNode} onRemoveSelection={removeSelected}
          onRemoveEdge={(from, to) => setEdges(previous =>
            previous.filter(edge => edge.from !== from || edge.to !== to))}
          onUpload={upload} />
      </div>
    </div>;
  }

  ReactDOM.createRoot(document.getElementById('root')).render(<Builder />);
})();
