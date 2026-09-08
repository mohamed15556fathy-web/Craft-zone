(function(){
  if (typeof authFetch !== 'function') return;
  const state = { rows: [], users: [] };
  function $(id){ return document.getElementById(id); }
  function safeQs(){ try { return typeof qs === 'function' ? qs() : ''; } catch (_) { return ''; } }
  function escapeValue(v){ return String(v ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
  function moneyLocal(v){ return typeof money === 'function' ? money(v) : (Number(v||0).toLocaleString('en-US',{maximumFractionDigits:2})+' ج'); }
  function isCashboxUsername(v){ const s=String(v||'').trim(); return s==='__cashbox__'||s==='cashbox'||s==='الخزنة'||s==='خزنة'; }
  function userLabel(u){ const username=String(u.username||'').trim(); if(isCashboxUsername(username)||String(u.full_name||'').trim()==='الخزنة') return 'الخزنة'; return String(u.full_name || u.username || '').trim(); }
  function actorOptions(selected){
    const sel = String(selected || '').trim();
    const blank = `<option value="" ${sel?'':'selected'}>بدون عهدة / الخزنة</option>`;
    const opts = (state.users || []).map(u => {
      const value = String(u.username || '').trim();
      return `<option value="${escapeValue(value)}" ${value===sel?'selected':''}>${escapeValue(userLabel(u))}${u.username ? ' - '+escapeValue(u.username) : ''}</option>`;
    }).join('');
    return blank + opts;
  }
  function rowCheckbox(row){
    if (!Number(row.assignable || 0)) return '';
    return `<input type="checkbox" class="outflow-custody-check" value="${escapeValue(row.key)}">`;
  }
  function rowSelect(row){
    if (!Number(row.assignable || 0)) return '<span class="hint">غير قابل للتعديل من هنا</span>';
    return `<select class="outflow-custody-select" onchange="assignOutflowCustodyRow('${escapeValue(row.key)}', this.value)">${actorOptions(row.actor_username || '')}</select>`;
  }
  async function renderOutflowCustodyDetails(){
    const content = $('detailsContent');
    const title = $('detailsTitle');
    const note = $('detailsNote');
    if (!content) return;
    if (title) title.textContent = 'تفاصيل إجمالي الخارج';
    if (note) note.textContent = 'حدد العهدة لكل بند خارج، أو اختار أكثر من بند وطبّقهم دفعة واحدة.';
    content.innerHTML = '<div class="muted">جاري تحميل بنود الخارج...</div>';
    try {
      const data = await authFetch('/outflow-items' + safeQs());
      state.rows = data.rows || [];
      state.users = data.users || [];
      if (!Number(data.can_assign_any_work_custody || 0) && data.current_user) {
        state.users = state.users.filter(u => isCashboxUsername(u.username) || String(u.username || '').trim().toLowerCase() === String(data.current_user || '').trim().toLowerCase());
      }
      const bulkOptions = actorOptions('');
      const controls = `
        <div class="form-grid" style="margin-bottom:12px;align-items:end">
          <div><label>تحديد جماعي على عهدة</label><select id="bulk_outflow_actor">${bulkOptions}</select></div>
          <div><button type="button" onclick="toggleAllOutflowCustodyRows(true)">تحديد الكل</button> <button type="button" onclick="toggleAllOutflowCustodyRows(false)">إلغاء التحديد</button> <button type="button" class="btn" onclick="applyOutflowCustodyBulk()">تطبيق على المحدد</button></div>
        </div>`;
      const rowsHtml = state.rows.length ? state.rows.map(row => `
        <tr>
          <td>${rowCheckbox(row)}</td>
          <td>${escapeValue(row.date || '-')}</td>
          <td>${escapeValue(row.source || '-')}<br><small>${escapeValue(row.details || '-')}</small></td>
          <td>${rowSelect(row)}</td>
          <td>${moneyLocal(row.amount || 0)}</td>
        </tr>`).join('') : '<tr><td colspan="5">لا توجد بنود خارج في الفترة المحددة</td></tr>';
      content.innerHTML = controls + `<div style="overflow:auto"><table><thead><tr><th style="width:44px">اختيار</th><th>التاريخ</th><th>مصدر الخارج</th><th>العهدة</th><th>القيمة</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>`;
    } catch (e) {
      content.innerHTML = `<div class="alert bad">${escapeValue(e.message || 'فشل تحميل تفاصيل الخارج')}</div>`;
    }
  }
  window.toggleAllOutflowCustodyRows = function(flag){
    document.querySelectorAll('.outflow-custody-check').forEach(cb => { cb.checked = !!flag; });
  };
  window.assignOutflowCustodyRow = async function(key, actorUsername){
    try {
      await authFetch('/assign-outflow-custody', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ actor_username: actorUsername || '', rows:[{key}] })});
      await renderOutflowCustodyDetails();
      if (typeof loadCash === 'function') await loadCash();
      if (typeof loadSummary === 'function') await loadSummary();
    } catch (e) { alert(e.message || 'فشل تحديد العهدة'); }
  };
  window.applyOutflowCustodyBulk = async function(){
    const selected = Array.from(document.querySelectorAll('.outflow-custody-check:checked')).map(cb => ({key: cb.value}));
    if (!selected.length) return alert('اختار بند واحد على الأقل');
    const actor = $('bulk_outflow_actor')?.value || '';
    try {
      await authFetch('/assign-outflow-custody', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ actor_username: actor, rows:selected })});
      await renderOutflowCustodyDetails();
      if (typeof loadCash === 'function') await loadCash();
      if (typeof loadSummary === 'function') await loadSummary();
      alert('تم تطبيق العهدة على البنود المحددة');
    } catch (e) { alert(e.message || 'فشل التطبيق الجماعي'); }
  };
  const oldShowDetails = window.showDetails;
  window.showDetails = async function(type){
    if (type !== 'outflows') return oldShowDetails ? oldShowDetails.apply(this, arguments) : undefined;
    if (typeof canOpenDetails === 'function' && !canOpenDetails(type)) return;
    const detailsEl = $('detailsPanel');
    const cashEl = $('cashPanel');
    if (detailsEl) detailsEl.classList.remove('hidden');
    if (cashEl) cashEl.classList.add('hidden');
    if (typeof activateCard === 'function') activateCard(type);
    await renderOutflowCustodyDetails();
  };
})();
