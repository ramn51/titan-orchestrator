import {lines, bars, stackedAreas} from './cluster_charts.js?v=6';

const models = new WeakMap();
let source = null;
let zoom = 1;
let pan = 100;
const $ = selector => document.querySelector(selector);
const dialog = $('#chart-dialog');
const expanded = $('#chart-expanded');

function draw(target, model) {
  if (model.kind === 'bars') bars(target, model.labels, model.values, model.color, model.options);
  else if (model.kind === 'stacked') stackedAreas(target, model.times, model.series, model.options);
  else lines(target, model.times, model.series, model.options);
  models.set(target, model);
  attachHover(target);
}

export function lineChart(target, times, series, options = {}) {
  if (!target) return;
  draw(target, {kind:'lines', times, series, options});
  if (dialog.open && source === target) renderExpanded();
}

export function stackedChart(target, times, series, options = {}) {
  if (!target) return;
  draw(target, {kind:'stacked', times, series, options});
  if (dialog.open && source === target) renderExpanded();
}

export function barChart(target, labels, values, color, options = {}) {
  if (!target) return;
  draw(target, {kind:'bars', labels, values, color, options});
  if (dialog.open && source === target) renderExpanded();
}

function attachHover(target) {
  if (target.dataset.inspectable) return;
  target.dataset.inspectable = 'true';
  const tip = document.createElement('div');
  tip.className = 'chart-tip';
  target.addEventListener('pointermove', event => {
    const model = models.get(target), svg = target.querySelector('svg');
    if (!model || !svg) return;
    const rect = svg.getBoundingClientRect();
    const fraction = Math.max(0, Math.min(1, (event.clientX - rect.left - 49 * rect.width / 500) /
      (rect.width * 439 / 500)));
    const count = model.kind === 'bars' ? model.labels.length : model.times.length;
    const index = model.kind === 'bars' ? Math.max(0, Math.min(count - 1, Math.floor(fraction * count))) :
      model.times.reduce((best,time,i) => Math.abs(time-(model.times[0]+fraction*(model.times.at(-1)-model.times[0]))) <
        Math.abs(model.times[best]-(model.times[0]+fraction*(model.times.at(-1)-model.times[0]))) ? i : best,0);
    if (count < 1) return;
    const rows = model.kind === 'bars'
      ? [model.labels[index] + ': ' + model.values[index] + (model.options?.unit || ' jobs')]
      : [new Date(model.times[index]).toLocaleString()+' ('+
        Math.round((Date.now()-model.times[index])/1000)+'s ago)',
        ...(model.kind === 'stacked' ? ['Total: '+model.series.reduce((sum,series)=>
          sum+(Number(series.values[index])||0),0)+' jobs'] : []),
        ...model.series.map(series => series.name + ': ' +
          (Number.isFinite(series.values[index]) ? Number(series.values[index]).toFixed(2) + (model.options.unit || '') : 'n/a'))];
    tip.textContent = rows.join('  ·  ');
    target.append(tip);
    tip.style.left = Math.min(target.clientWidth - tip.offsetWidth - 8,
      Math.max(8, event.clientX - target.getBoundingClientRect().left + 12)) + 'px';
    tip.style.top = Math.max(0, event.clientY - target.getBoundingClientRect().top - 32) + 'px';
  });
  target.addEventListener('pointerleave', () => tip.remove());
}

function renderExpanded() {
  const model = models.get(source);
  if (!model) return;
  const count = model.kind === 'bars' ? model.labels.length : model.times.length;
  const length = Math.max(1, Math.ceil(count / zoom));
  const start = Math.round(Math.max(0, count - length) * pan / 100);
  const end = start + length;
  if (model.kind === 'bars') draw(expanded, {...model,
    labels:model.labels.slice(start,end), values:model.values.slice(start,end)});
  else draw(expanded, {...model, times:model.times.slice(start,end),
    series:model.series.map(item => ({...item,values:item.values.slice(start,end)})),
    options:{...model.options,markers:(model.options.markers || []).filter(marker =>
      marker.at >= model.times[start] && marker.at <= model.times[end - 1])}});
  $('#chart-zoom-label').textContent = zoom + '×';
  $('#chart-zoom-out').disabled = zoom === 1;
  $('#chart-zoom-in').disabled = zoom >= 16 || count <= zoom;
  $('#chart-pan').disabled = zoom === 1;
}

export function mountChartInspector() {
  document.querySelectorAll('.chart-card').forEach(card => {
    const target = card.querySelector('.chart'), title = card.querySelector('h3');
    if (!target || !title) return;
    const button = document.createElement('button');
    button.className = 'chart-expand';
    button.textContent = '⤢';
    button.type = 'button';
    button.setAttribute('aria-label', 'Expand ' + title.textContent + ' chart');
    button.addEventListener('click', () => {
      source = target; zoom = 1; pan = 100; $('#chart-pan').value = 100;
      $('#chart-dialog-title').textContent = title.textContent;
      $('#chart-dialog-subtitle').textContent = card.querySelector('p')?.textContent || '';
      renderExpanded();
      dialog.showModal();
    });
    title.after(button);
  });
  $('#chart-close').addEventListener('click', () => dialog.close());
  $('#chart-zoom-in').addEventListener('click', () => {zoom = Math.min(16,zoom * 2);renderExpanded();});
  $('#chart-zoom-out').addEventListener('click', () => {zoom = Math.max(1,zoom / 2);renderExpanded();});
  $('#chart-reset').addEventListener('click', () => {zoom = 1;pan = 100;$('#chart-pan').value = 100;renderExpanded();});
  $('#chart-pan').addEventListener('input', event => {pan = +event.target.value;renderExpanded();});
  dialog.addEventListener('click', event => {if (event.target === dialog) dialog.close();});
}
