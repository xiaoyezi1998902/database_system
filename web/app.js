(() => {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  const editor = $('#sqlEditor');
  const runBtn = $('#runBtn');
  const clearBtn = $('#clearBtn');
  const tabs = $$('.tab');
  const panes = {
    rows: $('#pane-rows'),
    tokens: $('#pane-tokens'),
    ast: $('#pane-ast'),
    plan: $('#pane-plan'),
    raw: $('#pane-raw'),
  };
  const tablesEl = $('#tables');
  const refreshSchemaBtn = $('#refreshSchema');
  const resizer = $('#verticalResizer');

  function setActiveTab(name) {
    tabs.forEach(t => t.classList.toggle('active', t.dataset.tab === name));
    Object.entries(panes).forEach(([k, el]) => el.classList.toggle('active', k === name));
  }

  tabs.forEach(t => t.addEventListener('click', () => setActiveTab(t.dataset.tab)));

  async function fetchJSON(url, options) {
    const res = await fetch(url, Object.assign({
      headers: { 'Content-Type': 'application/json' },
    }, options));
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(txt || res.statusText);
    }
    return res.json();
  }

  function renderRows(results) {
    const pane = panes.rows;
    const table = pane.querySelector('table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    thead.innerHTML = '';
    tbody.innerHTML = '';

    // 合并所有结果为一个平面展示（逐语句块依次渲染）
    const blocks = [];
    for (const r of results) {
      if (r.resultType === 'rows') {
        blocks.push({ type: 'rows', rows: r.rows });
      } else if (r.resultType === 'count') {
        blocks.push({ type: 'text', text: `${r.count} 行受影响` });
      } else if (r.error) {
        const e = r.error || {};
        const pos = (e.line && e.column) ? ` (行 ${e.line} 列 ${e.column})` : '';
        const type = e.errorType ? `[${e.errorType}] ` : '';
        blocks.push({ type: 'text', text: `错误: ${type}${e.message || ''}${pos}` });
      } else {
        blocks.push({ type: 'text', text: '完成' });
      }
    }

    if (blocks.length === 0) return;

    // 找到首个行结果作为表头参考
    const firstRows = (blocks.find(b => b.type === 'rows') || {}).rows || [];
    const columns = firstRows.length ? Object.keys(firstRows[0]) : [];
    if (columns.length) {
      const tr = document.createElement('tr');
      for (const c of columns) {
        const th = document.createElement('th');
        th.textContent = c;
        tr.appendChild(th);
      }
      thead.appendChild(tr);
    }

    for (const b of blocks) {
      if (b.type === 'rows') {
        for (const row of b.rows) {
          const tr = document.createElement('tr');
          for (const c of columns) {
            const td = document.createElement('td');
            td.textContent = row[c];
            tr.appendChild(td);
          }
          tbody.appendChild(tr);
        }
      } else if (b.type === 'text') {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = Math.max(1, columns.length);
        td.textContent = b.text;
        tr.appendChild(td);
        tbody.appendChild(tr);
      }
    }
  }

  function renderDebug(results) {
    panes.tokens.textContent = results.map(r => r.error ? `ERROR: ${(r.error && r.error.message) || ''}` : (r.tokens || []).join('\n')).join('\n\n---\n\n');
    panes.ast.textContent = results.map(r => r.error ? `ERROR` : (r.ast || '')).join('\n\n---\n\n');
    panes.plan.textContent = results.map(r => r.error ? `ERROR` : (r.plan || '')).join('\n\n---\n\n');
    panes.raw.textContent = JSON.stringify(results, null, 2);
  }

  async function runSQL() {
    const sql = editor.value;
    if (!sql.trim()) return;
    setActiveTab('rows');
    try {
      const resp = await fetchJSON('/execute', { method: 'POST', body: JSON.stringify({ sql }) });
      if (!resp.ok && resp.error) throw new Error(resp.error);
      const results = resp.results || [];
      renderRows(results);
      renderDebug(results);
      await refreshSchema();
    } catch (e) {
      panes.rows.querySelector('tbody').innerHTML = `<tr><td>${e.message || e}</td></tr>`;
      panes.tokens.textContent = '';
      panes.ast.textContent = '';
      panes.plan.textContent = '';
      panes.raw.textContent = '';
    }
  }

  async function refreshSchema() {
    try {
      const data = await fetchJSON('/schema');
      const list = data.tables || [];
      tablesEl.innerHTML = '';
      for (const t of list) {
        const li = document.createElement('li');
        li.className = 'table-item';
        const name = document.createElement('div');
        name.className = 'table-name';
        name.textContent = t.name;
        name.title = '点击查看前200行';
        name.addEventListener('click', () => openTable(t.name));
        li.appendChild(name);
        const cols = document.createElement('ul');
        cols.className = 'column-list';
        for (const c of t.columns) {
          const ci = document.createElement('li');
          ci.textContent = `${c.name} : ${c.type}`;
          cols.appendChild(ci);
        }
        li.appendChild(cols);
        tablesEl.appendChild(li);
      }
    } catch (e) {
      tablesEl.innerHTML = `<li class="error">加载失败: ${e.message || e}</li>`;
    }
  }

  async function openTable(name) {
    try {
      const data = await fetchJSON(`/table/${encodeURIComponent(name)}?limit=200`);
      const results = [{ resultType: 'rows', rows: data.rows || [] }];
      setActiveTab('rows');
      renderRows(results);
      panes.tokens.textContent = '';
      panes.ast.textContent = '';
      panes.plan.textContent = '';
      panes.raw.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      panes.rows.querySelector('tbody').innerHTML = `<tr><td>${e.message || e}</td></tr>`;
    }
  }

  runBtn.addEventListener('click', runSQL);
  clearBtn.addEventListener('click', () => {
    Object.values(panes).forEach(el => {
      if (el.tagName === 'PRE') el.textContent = '';
    });
    const tbody = panes.rows.querySelector('tbody');
    if (tbody) tbody.innerHTML = '';
  });
  editor.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      runSQL();
    }
  });

  refreshSchema();

  // 垂直拖拽调整高度
  if (resizer) {
    let dragging = false;
    let startY = 0;
    let startTopHeight = 0;
    const main = document.querySelector('.main');
    const editorSection = document.querySelector('.editor-section');

    resizer.addEventListener('mousedown', (e) => {
      dragging = true;
      startY = e.clientY;
      startTopHeight = editorSection.getBoundingClientRect().height;
      document.body.style.cursor = 'row-resize';
      e.preventDefault();
    });

    window.addEventListener('mousemove', (e) => {
      if (!dragging) return;
      const dy = e.clientY - startY;
      const newTop = Math.max(120, startTopHeight + dy);
      const total = main.getBoundingClientRect().height;
      const bottom = Math.max(120, total - newTop - resizer.getBoundingClientRect().height);
      // 应用 grid 模板行高
      main.style.gridTemplateRows = `${newTop}px ${resizer.getBoundingClientRect().height}px ${bottom}px`;
    });

    window.addEventListener('mouseup', () => {
      if (!dragging) return;
      dragging = false;
      document.body.style.cursor = '';
    });
  }
})();


