document.addEventListener('DOMContentLoaded', () => {
  // Theme: cargar preferencia y aplicar
  const themeToggle = document.getElementById('themeToggle');
  const savedTheme = localStorage.getItem('theme') || 'dark';
  if (savedTheme === 'light') document.documentElement.setAttribute('data-theme', 'light');
  else document.documentElement.removeAttribute('data-theme');
  if (themeToggle) themeToggle.checked = (savedTheme === 'light');

  if (themeToggle) themeToggle.addEventListener('change', (e) => {
    if (e.target.checked) {
      document.documentElement.setAttribute('data-theme', 'light');
      localStorage.setItem('theme', 'light');
    } else {
      document.documentElement.removeAttribute('data-theme');
      localStorage.setItem('theme', 'dark');
    }
  });

  const symbolSel = document.getElementById('symbol');
  const loadBtn = document.getElementById('loadBtn');
  const positionsBtn = document.getElementById('positionsBtn');
  const hoursInput = document.getElementById('hours');

  async function fetchSymbols(){
    const res = await fetch('/api/analysis_files');
    const list = await res.json();
    symbolSel.innerHTML = '';
    list.forEach(s => {
      const opt = document.createElement('option'); opt.value = s; opt.textContent = s; symbolSel.appendChild(opt);
    });
  }

  async function loadAnalysis(){
    const sym = symbolSel.value;
    const res = await fetch(`/api/analysis/${encodeURIComponent(sym)}`);
    if(!res.ok){ alert('Error cargando análisis'); return; }
    const data = await res.json();
    renderCharts(data);
    renderTable(data);
  }

  function renderCharts(data){
    const hours = data.map(r => r.hour_utc);
    const vol = data.map(r => r.avg_volume);
    const range = data.map(r => r.avg_range);

    // obtener color de texto desde variables CSS para mantener contraste en ambos temas
    const cssColor = getComputedStyle(document.documentElement).getPropertyValue('--text') || '#e8eef6';
    const layoutCommon = {title:'', paper_bgcolor:'rgba(0,0,0,0)', plot_bgcolor:'rgba(0,0,0,0)', font:{color:cssColor.trim()}, autosize:true, margin:{t:40,l:40,r:20,b:40}};

    Plotly.newPlot('volChart', [{x: hours, y: vol, type: 'bar', marker:{color:'#0b7fda'}}], {...layoutCommon, title:'Volumen Promedio por Hora UTC'}, {responsive:true});

    Plotly.newPlot('rangeChart', [{x: hours, y: range, type: 'bar', marker:{color:'#0f9b8e'}}], {...layoutCommon, title:'Rango Promedio por Hora UTC'}, {responsive:true});
  }

  function renderTable(data){
    const container = document.getElementById('table');
    container.innerHTML = '';
    const table = document.createElement('table');
    table.style.width = '100%';
    table.style.borderCollapse = 'collapse';
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    ['hour_utc','avg_volume','avg_range'].forEach(h => { const th = document.createElement('th'); th.textContent = h; th.style.textAlign='left'; th.style.padding='6px'; headerRow.appendChild(th)});
    thead.appendChild(headerRow); table.appendChild(thead);
    const tbody = document.createElement('tbody');
    data.forEach(r => {
      const tr = document.createElement('tr');
      [r.hour_utc, r.avg_volume, r.avg_range].forEach(c => { const td = document.createElement('td'); td.textContent = c; td.style.padding='6px'; tr.appendChild(td)});
      tbody.appendChild(tr);
    });
    table.appendChild(tbody); container.appendChild(table);
  }

  function mapStatusToBadge(status) {
    if (!status) return 'muted';
    const s = String(status).toLowerCase();
    if (s.includes('open') || s.includes('active') || s.includes('live')) return 'success';
    if (s.includes('closed') || s.includes('filled') || s.includes('cancel')) return 'muted';
    if (s.includes('partial') || s.includes('warning') || s.includes('risk')) return 'warning';
    return 'muted';
  }

  function renderPositionsTable(positions){
    const container = document.getElementById('positionsTable');
    if(!container) return;
    container.innerHTML = '';
    const table = document.createElement('table');
    table.style.width = '100%'; table.style.borderCollapse = 'collapse';
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    ['symbol','side','size','entry_price','status'].forEach(h => { const th = document.createElement('th'); th.textContent = h; th.style.textAlign='left'; th.style.padding='6px'; headerRow.appendChild(th)});
    thead.appendChild(headerRow); table.appendChild(thead);
    const tbody = document.createElement('tbody');
    positions.forEach(p => {
      const tr = document.createElement('tr');
      ['symbol','side','size','entry_price'].forEach(k => { const td = document.createElement('td'); td.textContent = p[k] ?? ''; td.style.padding='6px'; tr.appendChild(td)});
      const statusTd = document.createElement('td'); statusTd.style.padding='6px';
      const span = document.createElement('span'); span.className = 'badge ' + mapStatusToBadge(p.status); span.textContent = p.status ?? '';
      statusTd.appendChild(span); tr.appendChild(statusTd);
      tbody.appendChild(tr);
    });
    table.appendChild(tbody); container.appendChild(table);
  }

  function filterTables(q){
    const tables = document.querySelectorAll('#table table, #positionsTable table');
    tables.forEach(table => {
      const trs = table.querySelectorAll('tbody tr');
      trs.forEach(tr => {
        const text = tr.innerText.toLowerCase();
        tr.style.display = (!q || text.includes(q)) ? '' : 'none';
      });
    });
  }

  function exportCurrentTableCSV(){
    const table = document.querySelector('#table table');
    if(!table) return alert('No hay tabla para exportar');
    let csv = '';
    const rows = table.querySelectorAll('tr');
    rows.forEach(r => {
      const cols = Array.from(r.querySelectorAll('th,td')).map(c => '"'+String(c.innerText).replace(/"/g,'""')+'"');
      csv += cols.join(',') + '\n';
    });
    const blob = new Blob([csv], {type: 'text/csv;charset=utf-8;'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = 'analysis_export.csv'; a.click(); URL.revokeObjectURL(url);
  }

  loadBtn.addEventListener('click', loadAnalysis);
  positionsBtn.addEventListener('click', async () => {
    const res = await fetch('/api/positions');
    if(!res.ok) return alert('Error cargando posiciones');
    const data = await res.json();
    renderPositionsTable(data);
  });

  // header controls (pueden no existir en versiones antiguas)
  const refreshBtn = document.getElementById('refreshBtn');
  if(refreshBtn) refreshBtn.addEventListener('click', () => fetchSymbols());
  const exportBtn = document.getElementById('exportBtn'); if(exportBtn) exportBtn.addEventListener('click', exportCurrentTableCSV);
  const globalSearch = document.getElementById('globalSearch'); if(globalSearch) globalSearch.addEventListener('input', (e) => filterTables(e.target.value.toLowerCase().trim()));

  fetchSymbols();
});
