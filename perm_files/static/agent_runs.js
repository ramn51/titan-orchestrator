(() => {
  "use strict";

  const body = document.body;
  const runId = body.dataset.agentId;
  const dataNode = document.getElementById("agent-run-data");
  const svg = document.getElementById("stage-graph");
  const scroll = document.getElementById("stage-scroll");
  if (!runId || !dataNode || !svg || !scroll) return;

  const NS = "http://www.w3.org/2000/svg";
  const colors = {COMPLETED: "#48d99b", RUNNING: "#efc46b", FAILED: "#ff777f", WAITING: "#73b9ed", PENDING: "#73b9ed", CANCELLED: "#8e9da8", UNKNOWN: "#8e9da8"};
  let scale = 1;
  let run = JSON.parse(dataNode.textContent);

  const make = (tag, attributes = {}, text = "") => {
    const node = document.createElementNS(NS, tag);
    Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
    if (text) node.textContent = text;
    return node;
  };
  const clipped = (value, size) => value.length > size ? value.slice(0, size - 1) + "…" : value;

  function graphLayout(stages) {
    const columns = new Map();
    stages.forEach(stage => {
      const order = Number(stage.order) || 0;
      if (!columns.has(order)) columns.set(order, []);
      columns.get(order).push(stage);
    });
    const ordered = [...columns.entries()].sort((a, b) => a[0] - b[0]);
    const positions = new Map();
    const nodeWidth = 210, nodeHeight = 100, gapX = 90, gapY = 34, margin = 34;
    let xIndex = 0, maxRows = 1;
    ordered.forEach(([, values]) => {
      maxRows = Math.max(maxRows, values.length);
      values.forEach((stage, row) => positions.set(stage.id, {x: margin + xIndex * (nodeWidth + gapX), y: margin + row * (nodeHeight + gapY)}));
      xIndex += 1;
    });
    return {positions, ordered, nodeWidth, nodeHeight, width: Math.max(520, margin * 2 + Math.max(1, ordered.length) * nodeWidth + Math.max(0, ordered.length - 1) * gapX), height: Math.max(210, margin * 2 + maxRows * nodeHeight + Math.max(0, maxRows - 1) * gapY)};
  }

  function renderGraph() {
    svg.replaceChildren();
    const layout = graphLayout(run.stages);
    svg.setAttribute("width", String(layout.width * scale));
    svg.setAttribute("height", String(layout.height * scale));
    svg.setAttribute("viewBox", `0 0 ${layout.width} ${layout.height}`);

    const defs = make("defs");
    const marker = make("marker", {id: "stage-arrow", markerWidth: "8", markerHeight: "8", refX: "7", refY: "4", orient: "auto"});
    marker.append(make("path", {d: "M0,0 L8,4 L0,8 z", fill: "#506b66"}));
    defs.append(marker); svg.append(defs);

    for (let index = 1; index < layout.ordered.length; index += 1) {
      const previous = layout.ordered[index - 1][1];
      const current = layout.ordered[index][1];
      previous.forEach(from => current.forEach(to => {
        const a = layout.positions.get(from.id), b = layout.positions.get(to.id);
        const startX = a.x + layout.nodeWidth, startY = a.y + layout.nodeHeight / 2;
        const endX = b.x, endY = b.y + layout.nodeHeight / 2;
        const middle = (startX + endX) / 2;
        svg.append(make("path", {class: "stage-edge", d: `M${startX},${startY} C${middle},${startY} ${middle},${endY} ${endX},${endY}`, "marker-end": "url(#stage-arrow)"}));
      }));
    }

    run.stages.forEach((stage, index) => {
      const point = layout.positions.get(stage.id);
      const link = make("a", {href: stage.url, "aria-label": `Open ${stage.label} DAG`});
      const group = make("g", {class: "stage-node", transform: `translate(${point.x} ${point.y})`, "data-status": stage.status, style: `--status:${colors[stage.status] || colors.UNKNOWN}`});
      group.append(make("rect", {width: layout.nodeWidth, height: layout.nodeHeight, rx: "8"}));
      group.append(make("text", {x: "14", y: "19", class: "node-kicker"}, `PHASE ${Number(stage.order) + 1}`));
      group.append(make("text", {x: "14", y: "43", class: "node-label"}, clipped(stage.label.replaceAll("_", " "), 23)));
      group.append(make("text", {x: "14", y: "62", class: "node-dag"}, clipped(stage.dag_name, 28)));
      group.append(make("text", {x: "14", y: "84", class: "node-status"}, stage.status));
      group.append(make("text", {x: "196", y: "84", class: "node-jobs", "text-anchor": "end"}, `${stage.completed}/${stage.total} jobs`));
      link.append(group); svg.append(link);
    });
  }

  function renderCards() {
    const root = document.getElementById("stage-cards");
    root.replaceChildren(...run.stages.map((stage, index) => {
      const card = document.createElement("a");
      card.href = stage.url; card.className = "stage-card";
      card.dataset.status = stage.status;
      card.innerHTML = `<span>P${Number(stage.order) + 1}</span><div><strong></strong><small></small></div><b></b>`;
      card.querySelector("strong").textContent = stage.label.replaceAll("_", " ");
      card.querySelector("small").textContent = stage.dag_name;
      card.querySelector("b").textContent = `${stage.status} · ${stage.completed}/${stage.total}`;
      return card;
    }));
  }

  function updateSummary() {
    document.querySelector(".heading-status .state").textContent = run.status;
    document.querySelector(".heading-status .state").dataset.status = run.status;
    document.getElementById("stage-total").textContent = run.total_stages;
    document.getElementById("stage-complete").textContent = run.completed_stages;
    document.getElementById("job-total").textContent = run.total_jobs;
    document.getElementById("job-complete").textContent = run.completed_jobs;
  }

  function render() { updateSummary(); renderGraph(); renderCards(); }
  const setZoom = value => { scale = Math.max(0.5, Math.min(1.5, value)); document.getElementById("zoom-value").textContent = `${Math.round(scale * 100)}%`; renderGraph(); };
  document.getElementById("zoom-out").addEventListener("click", () => setZoom(scale - 0.1));
  document.getElementById("zoom-in").addEventListener("click", () => setZoom(scale + 0.1));
  document.getElementById("zoom-reset").addEventListener("click", () => setZoom(1));

  document.getElementById("agent-search").addEventListener("input", event => {
    const query = event.target.value.trim().toLocaleLowerCase();
    document.querySelectorAll(".agent-link").forEach(link => { link.hidden = query && !link.dataset.search.toLocaleLowerCase().includes(query); });
  });

  document.querySelectorAll("time[data-timestamp]").forEach(node => {
    const timestamp = Number(node.dataset.timestamp);
    if (timestamp) node.textContent = new Date(timestamp).toLocaleString();
  });

  async function poll() {
    const indicator = document.getElementById("poll-state");
    try {
      const response = await fetch(`/api/agents/${encodeURIComponent(runId)}`, {cache: "no-store"});
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      run = await response.json(); render(); indicator.textContent = "Updated now";
    } catch (error) { indicator.textContent = "Update unavailable"; }
  }

  render();
  window.setInterval(poll, 5000);
})();
