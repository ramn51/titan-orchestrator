const $ = selector => document.querySelector(selector);
let busy = false;

function show(message, error = false) {
  const status = $('#demo-status');
  status.hidden = false;
  status.classList.toggle('error', error);
  status.replaceChildren();
  status.append(document.createTextNode(message));
}

function renderDemo(demo) {
  if (!demo) return;
  const counts = Object.entries(demo.counts || {}).map(([name,value]) => `${value} ${name.toLowerCase()}`).join(' · ');
  show(`${demo.label}: ${counts || 'submitted'}`);
  const link = document.createElement('a');
  link.href = '/dags/' + encodeURIComponent(demo.dag_id);
  link.textContent = 'Open pipeline ↗';
  $('#demo-status').append('  ', link);
  $('#run-demo').disabled = !demo.finished;
}

async function status() {
  if (busy) return;
  try {
    const response = await fetch('/api/cluster/demo', {cache:'no-store'});
    if (response.ok) renderDemo((await response.json()).demo);
  } catch (_) { /* The live dashboard may be offline during restart. */ }
}

export function mountDemoControls() {
  const button = $('#run-demo');
  if (document.querySelector('.section-status')?.textContent.includes('Sample preview')) {
    button.disabled = true;
    button.title = 'Run demos from the live dashboard on port 5000';
    return;
  }
  button.addEventListener('click', async () => {
    if (busy) return;
    busy = true;button.disabled = true;show('Submitting ' + $('#demo-preset').selectedOptions[0].textContent + '…');
    try {
      const response = await fetch('/api/cluster/demo', {method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({preset:$('#demo-preset').value})});
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Demo submission failed');
      renderDemo(data.demo);
    } catch (error) {show(error.message,true);button.disabled = false;}
    finally {busy = false;}
  });
  status();
  setInterval(status,3000);
}
