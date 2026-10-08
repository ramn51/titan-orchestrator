// Scale only the SVG graph. The surrounding dashboard and detail panel stay fixed.
(() => {
  const viewport = document.querySelector('.graph-scroll');
  const svg = viewport?.querySelector('svg');
  if (!svg) return;

  const DEFAULT = 0.8;
  const MIN = 0.5;
  const MAX = 1.5;
  const STEP = 0.1;
  const baseWidth = Number(svg.getAttribute('width'));
  const baseHeight = Number(svg.getAttribute('height'));
  const out = document.getElementById('zoom-out');
  const level = document.getElementById('zoom-level');
  const into = document.getElementById('zoom-in');
  const reset = document.getElementById('zoom-reset');
  if (!baseWidth || !baseHeight || !out || !level || !into || !reset) return;

  let zoom = DEFAULT;
  function apply(next, preserveCenter = true) {
    next = Math.max(MIN, Math.min(MAX, Math.round(next * 10) / 10));
    if (preserveCenter) {
      const centerX = (viewport.scrollLeft + viewport.clientWidth / 2) / zoom;
      const centerY = (viewport.scrollTop + viewport.clientHeight / 2) / zoom;
      svg.style.width = `${baseWidth * next}px`;
      svg.style.height = `${baseHeight * next}px`;
      viewport.scrollLeft = centerX * next - viewport.clientWidth / 2;
      viewport.scrollTop = centerY * next - viewport.clientHeight / 2;
    } else {
      svg.style.width = `${baseWidth * next}px`;
      svg.style.height = `${baseHeight * next}px`;
    }
    zoom = next;
    level.value = `${Math.round(zoom * 100)}%`;
    out.disabled = zoom <= MIN;
    into.disabled = zoom >= MAX;
    reset.disabled = zoom === DEFAULT;
  }

  out.addEventListener('click', () => apply(zoom - STEP));
  into.addEventListener('click', () => apply(zoom + STEP));
  reset.addEventListener('click', () => apply(DEFAULT));
  viewport.addEventListener('keydown', event => {
    if (event.key === '+' || event.key === '=') apply(zoom + STEP);
    else if (event.key === '-') apply(zoom - STEP);
    else if (event.key === '0') apply(DEFAULT);
    else return;
    event.preventDefault();
  });
  apply(DEFAULT, false);

  // A selected deep-stage link reloads the page; keep that stage in view.
  if (new URLSearchParams(location.search).has('job')) {
    requestAnimationFrame(() => {
      const selected = svg.querySelector('.node.selected');
      if (!selected) return;
      const node = selected.getBoundingClientRect();
      const frame = viewport.getBoundingClientRect();
      viewport.scrollLeft += node.left + node.width / 2 - frame.left - frame.width / 2;
      viewport.scrollTop += node.top + node.height / 2 - frame.top - frame.height / 2;
    });
  }
})();
