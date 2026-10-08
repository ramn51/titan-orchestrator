// HITL decisions use the existing TitanStore gate state; graph rendering stays in dag_viewer.js.
(() => {
  const dagId = document.body.dataset.selectedDag;
  const historical = document.body.dataset.historical === 'true';
  const panel = document.getElementById('hitl-gates');
  if (!dagId || !panel || historical) return;

  async function decide(gateId, decision, button) {
    button.disabled = true;
    try {
      const response = await fetch(`/api/dag/hitl/${encodeURIComponent(dagId)}/${encodeURIComponent(gateId)}`, {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({decision})
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Decision failed');
      await refresh();
    } catch (error) {
      const feedback = document.createElement('p');
      feedback.className = 'hitl-feedback';
      feedback.textContent = error.message;
      panel.append(feedback);
      button.disabled = false;
    }
  }

  async function refresh() {
    try {
      const response = await fetch(`/api/dag/hitl/${encodeURIComponent(dagId)}`, {cache: 'no-store'});
      if (!response.ok) return;
      const {gates} = await response.json();
      document.querySelectorAll('.node.hitl-waiting').forEach(node => node.classList.remove('hitl-waiting'));
      panel.replaceChildren();
      for (const gate of gates.filter(gate => gate.decision === 'WAITING' && gate.job_status === 'RUNNING')) {
        const node = [...document.querySelectorAll('.node')].find(item => item.dataset.jobId === gate.id);
        node?.classList.add('hitl-waiting');
        const row = document.createElement('div'); row.className = 'hitl-gate';
        const description = document.createElement('div');
        const title = document.createElement('strong'); title.textContent = `⏳ Awaiting human approval · ${gate.id}`;
        const message = document.createElement('p'); message.textContent = gate.message;
        description.append(title, message);
        const actions = document.createElement('div'); actions.className = 'hitl-actions';
        for (const [label, decision] of [['Approve', 'APPROVED'], ['Reject', 'REJECTED']]) {
          const button = document.createElement('button'); button.type = 'button'; button.textContent = label;
          button.addEventListener('click', () => decide(gate.id, decision, button));
          actions.append(button);
        }
        row.append(description, actions); panel.append(row);
      }
      panel.hidden = !panel.children.length;
    } catch {
      // The main dashboard connection indicator reports master availability.
    }
  }
  refresh();
  setInterval(refresh, 3000);
})();
