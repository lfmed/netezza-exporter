// Formulário de novo pedido: linhas dinâmicas de tabelas
function initRequestForm(rows) {
  const tbody = document.querySelector('#rows tbody');
  const tpl = document.getElementById('row-tpl');
  if (!tbody || !tpl) return;
  const add = (data) => {
    const tr = tpl.content.firstElementChild.cloneNode(true);
    const sel = tr.querySelector('.tbl-select');
    sel.addEventListener('change', () => {
      const o = sel.selectedOptions[0];
      if (o && o.dataset.unit) { tr.querySelector('[name=unit]').value = o.dataset.unit; tr.querySelector('[name=size]').value = o.dataset.size; }
      // sugere o escopo de migração da tabela quando as datas ainda estão vazias
      const st = tr.querySelector('[name=start]'), en = tr.querySelector('[name=end]');
      if (o && o.dataset.start && !st.value) st.value = o.dataset.start;
      if (o && o.dataset.end && !en.value) en.value = o.dataset.end;
    });
    tr.querySelector('.rm').addEventListener('click', () => { if (tbody.children.length > 1) tr.remove(); });
    if (data) for (const [k, v] of Object.entries(data)) { const el = tr.querySelector(`[name=${k}]`); if (el) el.value = v; }
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
