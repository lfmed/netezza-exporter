// Formulário de novo pedido: linhas dinâmicas de tabelas
function initRequestForm(rows) {
  const tbody = document.querySelector('#rows tbody');
  const tpl = document.getElementById('row-tpl');
  if (!tbody || !tpl) return;
  // Ajusta a linha ao tipo de filtro da tabela: Data (datas + calendário), Número (mín./máx. + quantidade)
  // ou Sem filtro (tabela inteira). Os campos ficam no formulário (só escondidos) para as listas não desalinharem.
  const configure = (tr, ft) => {
    const q = (s) => tr.querySelector(s), all = (s) => tr.querySelectorAll(s);
    const st = q('[name=start]'), en = q('[name=end]'), size = q('[name=size]');
    const prev = tr.dataset.ft;
    tr.dataset.ft = ft;
    for (const el of [st, en]) {
      el.type = ft === 'DATE' ? 'date' : 'text';
      el.inputMode = ft === 'NUMBER' ? 'numeric' : '';
      el.pattern = ft === 'NUMBER' ? '-?[0-9.]+' : '.*';
      el.placeholder = ft === 'NUMBER' ? (el === st ? 'valor mínimo' : 'valor máximo') : '';
      el.required = ft !== 'NONE';
      el.hidden = ft === 'NONE';
      if (prev && prev !== ft) el.value = '';
    }
    size.hidden = ft === 'NONE';
    size.max = ft === 'NUMBER' ? 5000 : 1000;
    q('[name=unit]').hidden = ft !== 'DATE';
    q('[name=aligned]').hidden = ft !== 'DATE';
    q('.no-align').hidden = ft === 'DATE';
    q('.count-label').hidden = ft !== 'NUMBER';
    all('.whole').forEach((el) => { el.hidden = ft !== 'NONE'; });
  };
  const add = (data) => {
    const tr = tpl.content.firstElementChild.cloneNode(true);
    const sel = tr.querySelector('.tbl-select');
    sel.addEventListener('change', () => {
      const o = sel.selectedOptions[0];
      configure(tr, (o && o.dataset.ft) || 'DATE');
      if (o && o.dataset.unit) { tr.querySelector('[name=unit]').value = o.dataset.unit; tr.querySelector('[name=size]').value = o.dataset.size; }
      // sugere o escopo de migração da tabela quando o início e o fim ainda estão vazios
      const st = tr.querySelector('[name=start]'), en = tr.querySelector('[name=end]');
      if (o && o.dataset.start && !st.value) st.value = o.dataset.start;
      if (o && o.dataset.end && !en.value) en.value = o.dataset.end;
    });
    tr.querySelector('.rm').addEventListener('click', () => { if (tbody.children.length > 1) tr.remove(); });
    configure(tr, 'DATE');
    if (data) {
      // a tabela primeiro: o tipo de filtro define o tipo dos campos de início e fim
      sel.value = data.table || '';
      const o = sel.selectedOptions[0];
      configure(tr, (o && o.dataset.ft) || 'DATE');
      for (const [k, v] of Object.entries(data)) { const el = tr.querySelector(`[name=${k}]`); if (el && k !== 'table') el.value = v; }
    }
    tbody.appendChild(tr);
  };
  (rows && rows.length ? rows : [null]).forEach(add);
  document.getElementById('add-row').addEventListener('click', () => add(null));
}

// Atualização automática do progresso do pedido
(function () {
  const el = document.getElementById('progress');
  if (!el || el.dataset.live !== '1') return;
  const tick = async () => {
    try {
      const r = await fetch(el.dataset.src, { headers: { 'X-Requested-With': 'fetch' } });
      if (r.ok) el.innerHTML = await r.text();
    } catch (e) { /* ignora falhas transitórias */ }
  };
  setInterval(tick, 8000);
})();

// Filtros simples de tabela no painel
document.querySelectorAll('[data-filter]').forEach((inp) => {
  inp.addEventListener('input', () => {
    const q = inp.value.toLowerCase();
    document.querySelectorAll(inp.dataset.filter + ' tbody tr').forEach((tr) => {
      tr.style.display = tr.textContent.toLowerCase().includes(q) ? '' : 'none';
    });
  });
});
document.querySelectorAll('[data-filter-col]').forEach((sel) => {
  sel.addEventListener('change', () => {
    document.querySelectorAll(sel.dataset.filterCol + ' tbody tr').forEach((tr) => {
      tr.style.display = !sel.value || tr.dataset[sel.dataset.col] === sel.value ? '' : 'none';
    });
  });
});

// Ordenação da tabela de completude (seletor + cabeçalhos clicáveis); tabelas sem escopo ficam por último
function sortTable(selector, key, dir) {
  const tbody = document.querySelector(selector + ' tbody');
  if (!tbody) return;
  const rows = Array.from(tbody.querySelectorAll('tr[data-name]'));
  const val = (tr) => key === 'name' ? tr.dataset.name : parseFloat(tr.dataset[key]);
  rows.sort((a, b) => {
    const va = val(a), vb = val(b);
    if (key === 'name') return dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    if (va < 0 && vb >= 0) return 1;  // sem escopo
    if (vb < 0 && va >= 0) return -1;
    return dir === 'asc' ? va - vb : vb - va;
  });
  rows.forEach((tr) => tbody.appendChild(tr));
  document.querySelectorAll(selector + ' th.sortable').forEach((th) => {
    th.dataset.dir = th.dataset.sortKey === key ? dir : '';
  });
  const sel = document.querySelector(`[data-sort="${selector}"]`);
  if (sel) { sel.value = `${key}:${dir}`; if (sel.value !== `${key}:${dir}`) sel.selectedIndex = -1; }
  try { localStorage.setItem('sort' + selector, `${key}:${dir}`); } catch (e) { /* sem storage */ }
}
document.querySelectorAll('[data-sort]').forEach((sel) => {
  const target = sel.dataset.sort;
  sel.addEventListener('change', () => { const [k, d] = sel.value.split(':'); sortTable(target, k, d); });
  let saved = new URLSearchParams(location.search).get('ordem');  // ?ordem=pct:asc tem prioridade
  if (!saved) { try { saved = localStorage.getItem('sort' + target); } catch (e) { /* sem storage */ } }
  if (saved) { const [k, d] = saved.split(':'); sortTable(target, k, d); }
  document.querySelectorAll(target + ' th.sortable').forEach((th) => {
    th.addEventListener('click', () => {
      const k = th.dataset.sortKey;
      const d = th.dataset.dir === 'asc' ? 'desc' : 'asc';
      sortTable(target, k, d);
    });
  });
});
