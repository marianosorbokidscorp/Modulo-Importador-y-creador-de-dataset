/* SPA — 3 vistas: catalog → setup → data */

const state = {
  view: 'catalog',
  catalog: null,
  importers: null,
  selectedReport: null,
  selectedImporter: null,
  currentTable: null,
  page: 0,
  size: 50,
  sort: null,
  dir: 'asc',
};

const $ = id => document.getElementById(id);

function showView(name) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  $('view-' + name).classList.add('active');
  state.view = name;
  $('btn-back').style.display = name === 'catalog' ? 'none' : 'inline-block';
}

function setBreadcrumb(text) {
  $('breadcrumb').textContent = text;
}

/* ================ CATALOG VIEW ================ */
async function loadCatalog() {
  const r = await fetch('/api/catalog');
  state.catalog = await r.json();
  renderCatalog();
}

function renderCatalog() {
  const container = $('catalog-grid');
  container.innerHTML = state.catalog.map(rep => `
    <div class="report-card" data-slug="${rep.slug}">
      <h3>${rep.name}</h3>
      <div class="stats">${rep.tables_visible} tablas visibles · ${rep.tables_total} total</div>
      <div class="sources">
        ${rep.sources.map(s => `<span class="tag ${s}">${s}</span>`).join('')}
      </div>
    </div>
  `).join('');
  container.querySelectorAll('.report-card').forEach(c =>
    c.addEventListener('click', () => selectReport(c.dataset.slug)));
}

/* ================ SETUP VIEW ================ */
async function selectReport(slug) {
  const r = await fetch('/api/report/' + slug);
  state.selectedReport = await r.json();
  if (!state.importers) {
    const ir = await fetch('/api/importers');
    state.importers = await ir.json();
  }
  setBreadcrumb(`Catálogo › ${state.selectedReport.name}`);
  renderSetup();
  showView('setup');
}

function renderSetup() {
  const rep = state.selectedReport;

  // tablas
  $('setup-title').textContent = rep.name;
  $('setup-tables').innerHTML = rep.tables.map(t => `
    <div class="table-row">
      <div>
        <div class="name">${t.name}</div>
        <div class="meta">${t.n_columns} columnas</div>
      </div>
      <span class="tag ${t.source}">${t.source}</span>
    </div>
  `).join('');

  // sources
  $('setup-sources').innerHTML = rep.sources.length
    ? rep.sources.map(s => `<span class="tag ${s}">${s}</span>`).join(' ')
    : '<span class="tag unknown">sin source detectado</span>';

  // importer tabs
  const recommended = rep.sources.find(s => state.importers[s]);
  const order = ['synthetic', recommended, ...Object.keys(state.importers)]
    .filter((v, i, a) => v && a.indexOf(v) === i);

  $('importer-tabs').innerHTML = order.map(name => {
    const imp = state.importers[name];
    const isRec = name === recommended && name !== 'synthetic';
    return `<button class="importer-tab${isRec ? ' recommended' : ''}" data-importer="${name}">
      ${imp.label}${isRec ? ' ★' : ''}
    </button>`;
  }).join('');

  $('importer-sections').innerHTML = order.map(name => {
    const imp = state.importers[name];
    return `
      <div class="importer-section" data-importer="${name}">
        <p class="desc">${imp.description}</p>
        <form data-importer="${name}">
          ${imp.fields.map(f => renderField(f)).join('')}
          <button type="submit" class="btn-primary">Importar</button>
        </form>
      </div>
    `;
  }).join('');

  // bind tabs
  $('importer-tabs').querySelectorAll('.importer-tab').forEach(t => {
    t.addEventListener('click', () => selectImporter(t.dataset.importer));
  });

  // bind forms
  $('importer-sections').querySelectorAll('form').forEach(f => {
    f.addEventListener('submit', e => { e.preventDefault(); runImport(f); });
  });

  // default tab
  selectImporter(recommended || 'synthetic');
  $('import-status').className = 'import-status';
  $('import-status').innerHTML = '';
}

function renderField(f) {
  if (f.type === 'file') {
    return `
      <div class="form-group">
        <label>${f.label}${f.required ? ' *' : ''}</label>
        <input type="file" name="${f.key}" multiple accept=".csv,.xlsx,.xls">
        ${f.hint ? `<div class="hint">${f.hint}</div>` : ''}
      </div>`;
  }
  return `
    <div class="form-group">
      <label>${f.label}${f.required ? ' *' : ''}</label>
      <input type="${f.type}" name="${f.key}" value="${f.default || ''}" placeholder="${f.placeholder || ''}">
      ${f.hint ? `<div class="hint">${f.hint}</div>` : ''}
    </div>`;
}

function selectImporter(name) {
  state.selectedImporter = name;
  document.querySelectorAll('.importer-tab').forEach(t =>
    t.classList.toggle('active', t.dataset.importer === name));
  document.querySelectorAll('.importer-section').forEach(s =>
    s.classList.toggle('active', s.dataset.importer === name));
}

/* ================ IMPORT ================ */
async function runImport(form) {
  const importer = form.dataset.importer;
  const status = $('import-status');
  const submitBtn = form.querySelector('button[type=submit]');
  submitBtn.disabled = true;

  const showStatus = (kind, html) => {
    status.className = 'import-status show ' + kind;
    status.innerHTML = html;
  };

  showStatus('info', '<span class="spinner"></span> Iniciando import...');

  try {
    const fileInput = form.querySelector('input[type=file]');
    let resp;
    if (fileInput && fileInput.files.length > 0) {
      const fd = new FormData();
      fd.append('importer', importer);
      for (const f of fileInput.files) fd.append('files', f);
      for (const inp of form.querySelectorAll('input:not([type=file])')) {
        fd.append(inp.name, inp.value);
      }
      resp = await fetch('/api/import/' + state.selectedReport.slug, {method: 'POST', body: fd});
    } else {
      const body = {importer};
      for (const inp of form.querySelectorAll('input')) body[inp.name] = inp.value;
      resp = await fetch('/api/import/' + state.selectedReport.slug, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      });
    }
    const d = await resp.json();
    if (!d.ok) { showStatus('error', 'Error: ' + (d.error || 'unknown')); submitBtn.disabled = false; return; }

    const final = await pollJob(d.job_id, s => {
      let h = '';
      if (s.user_code) {
        h += `<div><b>1.</b> Abrí este link y pegá el código:</div>
              <div class="device-code-box">
                <div style="font-size:11px;color:var(--muted)">Código:</div>
                <div class="code">${s.user_code}</div>
                <a href="${s.verification_uri}" target="_blank">Abrir Microsoft Login</a>
              </div>`;
      }
      h += `<div style="margin-top:10px"><span class="spinner"></span> ${s.message} <small>(${s.elapsed}s)</small></div>`;
      showStatus('info', h);
    });

    if (final.status === 'done') {
      const tables = Object.entries(final.tables_imported)
        .map(([n, c]) => `${n} (${c.toLocaleString()} filas)`).join(', ');
      showStatus('ok', '✓ ' + final.message + '<br><small>' + tables + '</small>');
      setTimeout(() => goToDataView(), 800);
    } else {
      showStatus('error', '✗ ' + final.message);
      submitBtn.disabled = false;
    }
  } catch (e) {
    showStatus('error', 'Error: ' + e.message);
    submitBtn.disabled = false;
  }
}

async function pollJob(jobId, onUpdate) {
  while (true) {
    const r = await fetch('/api/import/poll/' + jobId);
    const d = await r.json();
    onUpdate(d);
    if (d.status === 'done' || d.status === 'error') return d;
    await new Promise(res => setTimeout(res, 1500));
  }
}

/* ================ DATA VIEW ================ */
async function goToDataView() {
  showView('data');
  setBreadcrumb(`Catálogo › ${state.selectedReport.name} › Vista de datos`);
  await loadDatasetTables();
}

async function loadDatasetTables() {
  const r = await fetch('/api/dataset/' + state.selectedReport.slug + '/tables');
  const tables = await r.json();
  const list = $('tables-list');
  if (tables.length === 0) {
    list.innerHTML = '<div class="empty-state">No hay tablas materializadas todavía. Volvé a Setup e importá.</div>';
    return;
  }
  list.innerHTML = tables.map(t => `
    <div class="item" data-table="${t.name}">
      <div class="name">${t.name}</div>
      <div class="meta">${t.rows ? t.rows.toLocaleString() : '?'} filas · ${t.cols || '?'} cols · ${t.size_kb || 0} KB</div>
    </div>
  `).join('');
  list.querySelectorAll('.item').forEach(i =>
    i.addEventListener('click', () => selectTable(i.dataset.table)));

  // auto-select first
  if (tables.length > 0) selectTable(tables[0].name);
}

async function selectTable(name) {
  state.currentTable = name;
  state.page = 0;
  state.sort = null;
  document.querySelectorAll('#tables-list .item').forEach(i =>
    i.classList.toggle('selected', i.dataset.table === name));
  await loadTableData();
}

async function loadTableData() {
  const params = new URLSearchParams({
    table: state.currentTable,
    page: state.page,
    size: state.size,
  });
  if (state.sort) { params.set('sort', state.sort); params.set('dir', state.dir); }
  const r = await fetch('/api/dataset/' + state.selectedReport.slug + '/data?' + params);
  const d = await r.json();
  renderGrid(d);
}

function renderGrid(d) {
  $('grid-title').textContent = state.currentTable;
  $('grid-meta').textContent = `${d.total.toLocaleString()} filas · ${d.columns.length} columnas`;

  const wrap = $('grid-wrap');
  if (!d.rows || d.rows.length === 0) {
    wrap.innerHTML = '<div class="empty-state">Sin datos.</div>';
    return;
  }
  let h = '<table class="data-grid"><thead><tr>';
  h += '<th class="row-num" style="cursor:default">#</th>';
  d.columns.forEach((c, i) => {
    const isSorted = state.sort === c;
    const arrow = isSorted ? (state.dir === 'asc' ? ' ▲' : ' ▼') : '';
    h += `<th data-col="${c}">
            ${escapeHtml(c)}${arrow}
            <span class="col-type">${escapeHtml(d.types[i] || '')}</span>
          </th>`;
  });
  h += '</tr></thead><tbody>';
  d.rows.forEach((row, idx) => {
    const rowNum = d.page * d.size + idx + 1;
    h += `<tr><td class="row-num">${rowNum.toLocaleString()}</td>`;
    row.forEach(v => {
      h += `<td>${escapeHtml(String(v))}</td>`;
    });
    h += '</tr>';
  });
  h += '</tbody></table>';
  wrap.innerHTML = h;

  wrap.querySelectorAll('thead th[data-col]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (state.sort === col) state.dir = state.dir === 'asc' ? 'desc' : 'asc';
      else { state.sort = col; state.dir = 'asc'; }
      loadTableData();
    });
  });

  // pagination
  $('pg-prev').disabled = d.page === 0;
  $('pg-next').disabled = d.page + 1 >= d.pages;
  $('pg-info').textContent = `Página ${d.page + 1} de ${d.pages || 1}`;
  $('pg-size').value = d.size;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

/* ================ INIT ================ */
$('btn-back').addEventListener('click', () => {
  if (state.view === 'data') {
    setBreadcrumb(`Catálogo › ${state.selectedReport.name}`);
    showView('setup');
  } else {
    setBreadcrumb('Catálogo de reportes');
    showView('catalog');
  }
});
$('pg-prev').addEventListener('click', () => { state.page = Math.max(0, state.page - 1); loadTableData(); });
$('pg-next').addEventListener('click', () => { state.page += 1; loadTableData(); });
$('pg-size').addEventListener('change', e => { state.size = +e.target.value; state.page = 0; loadTableData(); });

setBreadcrumb('Catálogo de reportes');
loadCatalog();
