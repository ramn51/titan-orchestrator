/* Pure graph operations shared by the editor, previews, and deploy request. */
(function () {
  const safe = value => JSON.stringify(String(value ?? ''));
  const scriptPath = file => file ? 'perm_files/dag_samples/' + file : 'choose-a-script.py';

  function makeNode(kind, index) {
    return {
      key: 'job-' + Date.now() + '-' + index,
      id: 'task-' + index,
      kind,
      script: '',
      args: '',
      requirement: 'GENERAL',
      priority: 1,
      delay: 0,
      port: 0,
      affinity: false,
      x: 75 + ((index - 1) % 4) * 230,
      y: 85 + Math.floor((index - 1) / 4) * 150
    };
  }

  function jobsFromGraph(nodes, edges) {
    return nodes.map(node => ({
      id: node.id.trim(),
      filename: node.script,
      job_type: node.kind,
      args: node.args,
      requirement: node.requirement,
      priority: Number(node.priority),
      delay: Number(node.delay),
      port: Number(node.port),
      affinity: Boolean(node.affinity),
      depends_on: edges.filter(edge => edge.to === node.key)
        .map(edge => nodes.find(parent => parent.key === edge.from)?.id)
        .filter(Boolean)
    }));
  }

  function createsCycle(edges, from, to) {
    if (from === to) return true;
    const children = new Map();
    for (const edge of edges) {
      if (!children.has(edge.from)) children.set(edge.from, []);
      children.get(edge.from).push(edge.to);
    }
    const seen = new Set();
    function reaches(current) {
      if (current === from) return true;
      if (seen.has(current)) return false;
      seen.add(current);
      return (children.get(current) || []).some(reaches);
    }
    return reaches(to);
  }

  function validate(name, nodes, edges) {
    if (!/^[A-Za-z0-9_-]{1,64}$/.test(name.trim()))
      return 'Use 1–64 letters, digits, _ or - for the pipeline name.';
    if (!nodes.length) return 'Add at least one job.';
    const jobs = jobsFromGraph(nodes, edges);
    const ids = new Set();
    for (const job of jobs) {
      if (!/^[A-Za-z0-9_-]{1,64}$/.test(job.id)) return 'Every job needs a valid ID.';
      if (ids.has(job.id)) return 'Job IDs must be unique.';
      if (!job.filename) return job.id + ' needs a Python script.';
      ids.add(job.id);
    }
    const parents = new Map(nodes.map(node => [node.key,
      edges.filter(edge => edge.to === node.key).map(edge => edge.from)]));
    const visited = new Set(), active = new Set();
    function visit(key) {
      if (active.has(key)) return true;
      if (visited.has(key)) return false;
      active.add(key);
      for (const parent of parents.get(key) || []) if (visit(parent)) return true;
      active.delete(key);
      visited.add(key);
      return false;
    }
    if (nodes.some(node => visit(node.key))) return 'Dependencies contain a cycle.';
    return '';
  }

  function yaml(name, nodes, edges) {
    const lines = ['name: ' + safe(name), 'jobs:'];
    for (const job of jobsFromGraph(nodes, edges)) {
      lines.push('  - id: ' + safe(job.id),
        '    type: ' + safe(job.job_type),
        '    file: ' + safe(scriptPath(job.filename)),
        '    requirement: ' + safe(job.requirement),
        '    priority: ' + job.priority,
        '    delay: ' + job.delay);
      if (job.args) lines.push('    args: ' + safe(job.args));
      if (job.affinity) lines.push('    affinity: true');
      if (job.job_type === 'service') lines.push('    port: ' + job.port);
      if (job.depends_on.length)
        lines.push('    depends_on: [' + job.depends_on.map(safe).join(', ') + ']');
    }
    return lines.join('\n');
  }

  function python(name, nodes, edges) {
    const lines = ['from titan_sdk import TitanClient, TitanJob', '', 'jobs = ['];
    for (const job of jobsFromGraph(nodes, edges)) {
      lines.push('    TitanJob(',
        '        job_id=' + safe(job.id) + ',',
        '        filename=' + safe(scriptPath(job.filename)) + ',',
        '        job_type=' + safe(job.job_type === 'service' ? 'SERVICE' : 'RUN_PAYLOAD') + ',',
        '        args=' + safe(job.args) + ',',
        '        parents=' + JSON.stringify(job.depends_on) + ',',
        '        requirement=' + safe(job.requirement) + ',',
        '        priority=' + job.priority + ', delay=' + job.delay + ',',
        '        affinity=' + (job.affinity ? 'True' : 'False') + ',',
        '        port=' + job.port + ',',
        '    ),');
    }
    lines.push(']', 'print(TitanClient().submit_dag(' + safe(name) + ', jobs))');
    return lines.join('\n');
  }

  function migrateDraft(draft) {
    const nodes = (draft.nodes || []).map((node, index) => node.data ? {
      key: node.id,
      id: node.data.jobId,
      kind: node.data.type || 'run',
      script: node.data.file || '',
      args: node.data.args || '',
      requirement: node.data.requirement || 'GENERAL',
      priority: node.data.priority || 1,
      delay: node.data.delay || 0,
      port: node.data.port || 0,
      affinity: Boolean(node.data.affinity),
      x: node.x || 75 + index * 230,
      y: node.y || 85
    } : node);
    return { nodes, edges: draft.edges || [] };
  }

  window.TitanBuilderModel = { makeNode, jobsFromGraph, createsCycle, validate, yaml, python, migrateDraft };
})();
