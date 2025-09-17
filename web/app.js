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
    stats: $('#pane-stats'),
    raw: $('#pane-raw'),
  };
  const tablesEl = $('#tables');
  const refreshSchemaBtn = $('#refreshSchema');
  const resizer = $('#verticalResizer');
  const refreshStatsBtn = $('#refreshStats');
  const resetStatsBtn = $('#resetStats');
  const statsContent = $('#statsContent');
  const lintPanel = document.getElementById('lintPanel');

  // SQL关键词列表
  const SQL_KEYWORDS = new Set([
    'SELECT', 'FROM', 'WHERE', 'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES', 'DELETE',
    'UPDATE', 'SET', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'ORDER', 'BY',
    'GROUP', 'HAVING', 'INT', 'TEXT', 'VARCHAR', 'AND', 'OR', 'NOT', 'NULL', 'IS',
    'IN', 'BETWEEN', 'LIKE', 'EXISTS', 'UNION', 'DISTINCT', 'AS', 'ASC', 'DESC'
  ]);

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

    // 清空整个结果面板
    pane.innerHTML = '';

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
        const expected = e.expected ? ` 期望: ${e.expected}` : '';
        blocks.push({ type: 'text', text: `错误: ${type}${e.message || ''}${pos}${expected}` });
      } else {
        blocks.push({ type: 'text', text: `${r.resultType}` });
      }
    }

    if (blocks.length === 0) return;

    // 为每个结果块创建独立的容器
    for (const b of blocks) {
      if (b.type === 'rows') {
        // 为每个SELECT结果创建独立的表格容器
        const columns = b.rows.length ? Object.keys(b.rows[0]) : [];
        if (columns.length) {
          // 创建结果块容器
          const resultBlock = document.createElement('div');
          resultBlock.className = 'result-block';

          // 创建表格
          const table = document.createElement('table');

          // 创建表头
          const thead = document.createElement('thead');
          const tr = document.createElement('tr');
          for (const c of columns) {
            const th = document.createElement('th');
            th.textContent = c;
            tr.appendChild(th);
          }
          thead.appendChild(tr);
          table.appendChild(thead);

          // 创建表体
          const tbody = document.createElement('tbody');
          for (const row of b.rows) {
            const tr = document.createElement('tr');
            for (const c of columns) {
              const td = document.createElement('td');
              td.textContent = row[c];
              tr.appendChild(td);
            }
            tbody.appendChild(tr);
          }
          table.appendChild(tbody);

          resultBlock.appendChild(table);
          pane.appendChild(resultBlock);
        }
      } else if (b.type === 'text') {
        // 为文本结果创建独立的容器
        const resultBlock = document.createElement('div');
        resultBlock.className = 'result-block text-result';
        resultBlock.textContent = b.text;
        pane.appendChild(resultBlock);
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
    // 保存当前的选中状态
    const selectionStart = editor.selectionStart;
    const selectionEnd = editor.selectionEnd;

    // 获取选中的文本，如果没有选中则使用全部文本
    const selectedText = editor.value.substring(selectionStart, selectionEnd);
    const sql = selectedText.trim() || editor.value;

    // 计算选中文本的起始行号
    let startLineNum = 1;
    if (selectedText.trim()) {
      // 计算选中文本在原始文本中的起始行号
      const textBeforeSelection = editor.value.substring(0, editor.selectionStart);
      startLineNum = textBeforeSelection.split('\n').length;
    }

    if (!sql.trim()) return;
    setActiveTab('rows');
    try {
      const resp = await fetchJSON('/execute', {
        method: 'POST',
        body: JSON.stringify({
          sql: sql,
          startLineNum: startLineNum
        })
      });
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
    } finally {
      // 恢复选中状态
      editor.setSelectionRange(selectionStart, selectionEnd);
      editor.focus(); // 确保编辑器重新获得焦点
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

  // SQL语法检查
  function checkSQLSyntax(text) {
    const errors = [];
    const lines = text.split('\n');

    for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
      const line = lines[lineIdx];
      const words = line.match(/\b[A-Za-z_]+\b/g) || [];

      for (const word of words) {
        const upperWord = word.toUpperCase();
        if (!SQL_KEYWORDS.has(upperWord)) {
          // 检查是否是拼写错误的关键词
          const suggestions = findSuggestions(upperWord);
          if (suggestions.length > 0) {
            errors.push({
              line: lineIdx + 1,
              column: line.indexOf(word) + 1,
              word: word,
              suggestions: suggestions
            });
          }
        }
      }
    }

    return errors;
  }

  function findSuggestions(word) {
    const suggestions = [];
    for (const keyword of SQL_KEYWORDS) {
      if (levenshteinDistance(word, keyword) <= 2) {
        suggestions.push(keyword);
      }
    }
    return suggestions.slice(0, 3); // 最多返回3个建议
  }

  function levenshteinDistance(a, b) {
    const matrix = [];
    for (let i = 0; i <= b.length; i++) {
      matrix[i] = [i];
    }
    for (let j = 0; j <= a.length; j++) {
      matrix[0][j] = j;
    }
    for (let i = 1; i <= b.length; i++) {
      for (let j = 1; j <= a.length; j++) {
        if (b.charAt(i - 1) === a.charAt(j - 1)) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j - 1] + 1,
            matrix[i][j - 1] + 1,
            matrix[i - 1][j] + 1
          );
        }
      }
    }
    return matrix[b.length][a.length];
  }

  function highlightErrors() {
    const text = editor.value;
    if (!text.trim()) {
      if (lintPanel) {
        lintPanel.style.display = 'none';
        lintPanel.innerHTML = '';
      }
      return;
    }

    const errors = checkSQLSyntax(text);
    console.log('检测到的错误:', errors); // 调试信息

    if (errors.length > 0) {
      if (lintPanel) {
        lintPanel.style.display = 'block';
        lintPanel.innerHTML = errors.map(e => {
          const sug = (e.suggestions && e.suggestions.length) ? '，建议：' + e.suggestions.join(', ') : '';
          return `<div class="lint-item">第${e.line}行 第${e.column}列：疑似关键词拼写错误 "${e.word}"${sug}</div>`;
        }).join('');
      }
    } else {
      if (lintPanel) {
        lintPanel.style.display = 'none';
        lintPanel.innerHTML = '';
      }
    }
  }


  // 为编辑器添加实时语法检查
  editor.addEventListener('input', highlightErrors);
  editor.addEventListener('blur', highlightErrors);
  // 初始执行一次，确保面板可见
  setTimeout(highlightErrors, 100); // 延迟执行，确保DOM完全加载

  // 缓存统计功能
  async function loadStats() {
    try {
      console.log('正在加载缓存统计...');
      const data = await fetchJSON('/stats');
      console.log('缓存统计数据:', data);
      if (data.ok) {
        renderStats(data.stats);
      } else {
        statsContent.innerHTML = `<div class="error">加载统计信息失败: ${data.error || '未知错误'}</div>`;
      }
    } catch (e) {
      console.error('加载统计信息异常:', e);
      statsContent.innerHTML = `<div class="error">加载统计信息失败: ${e.message}</div>`;
    }
  }

  function renderStats(stats) {
    const hitRate = (stats.hit_rate * 100).toFixed(2);
    statsContent.innerHTML = `
      <div class="stat-card">
        <div class="stat-title">缓存命中次数</div>
        <div class="stat-value">${stats.hit_count}</div>
      </div>
      <div class="stat-card">
        <div class="stat-title">缓存未命中次数</div>
        <div class="stat-value">${stats.miss_count}</div>
      </div>
      <div class="stat-card">
        <div class="stat-title">命中率</div>
        <div class="stat-value">${hitRate}%</div>
        <div class="stat-subtitle">${stats.hit_count}/${stats.hit_count + stats.miss_count}</div>
      </div>
      <div class="stat-card">
        <div class="stat-title">驱逐次数</div>
        <div class="stat-value">${stats.evict_count}</div>
      </div>
      <div class="stat-card">
        <div class="stat-title">当前缓存大小</div>
        <div class="stat-value">${stats.cache_size}</div>
        <div class="stat-subtitle">/${stats.cache_capacity}</div>
      </div>
      <div class="evict-log">
        <h4>最近驱逐记录</h4>
        ${stats.evict_log.length > 0 ?
        stats.evict_log.map(entry => `
            <div class="evict-entry">
              <span>${entry.table}.${entry.page_id}</span>
              <span>${entry.dirty ? '脏页' : '干净页'} - ${entry.reason}</span>
            </div>
          `).join('') :
        '<div class="stat-subtitle">暂无驱逐记录</div>'
      }
      </div>
    `;
  }

  async function resetStats() {
    try {
      const data = await fetchJSON('/stats/reset', { method: 'POST' });
      if (data.ok) {
        await loadStats();
      }
    } catch (e) {
      console.error('重置统计失败:', e);
    }
  }

  refreshStatsBtn.addEventListener('click', loadStats);
  resetStatsBtn.addEventListener('click', resetStats);

  // 页面加载时自动加载统计信息
  setTimeout(loadStats, 200);

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


