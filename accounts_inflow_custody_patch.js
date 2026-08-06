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
    const sel = String(selected || '__cashbox__').trim() || '__cashbox__';
    const list = state.users && state.users.length ? state.users : [{username:'__cashbox__',full_name:'الخزنة'}];
    const seen = new Set();
    return list.map(u => {
      const value = String(u.username || '').trim();
      if (!value || seen.has(value.toLowerCase())) return '';
      seen.add(value.toLowerCase());
      const label = userLabel(u);
      return `<option value="${escapeValue(value)}" ${value===sel?'selected':''}>${escapeValue(label)}${value && !isCashboxUsername(value) ? ' - '+escapeValue(value) : ''}</option>`;
    }).join('');
  }
  function rowCheckbox(row){ return Number(row.assignable || 0) ? `<input type="checkbox" class="inflow-custody-check" value="${escapeValue(row.key)}">` : ''; }
  function rowSelect(row){
    if (!Number(row.assignable || 0)) return '<span class="hint">غير قابل للتعديل من هنا</span>';
    return `<select class="inflow-custody-select" onchange="assignInflowCustodyRow('${escapeValue(row.key)}', this.value)">${actorOptions(row.actor_username || '__cashbox__')}</select>`;
  }
  async function renderInflowCustodyDetails(){
    const content = $('detailsContent');
    const title = $('detailsTitle');
    const note = $('detailsNote');
    if (!content) return;
    if (title) title.textContent = 'تفاصيل إجمالي الداخل';
    if (note) note.textContent = 'العربون وأي دفعة أوردر تدخل في الخزنة تلقائيًا، وتقدر تنقلها على عهدة أدمن محدد أو تطبق أكتر من بند مرة واحدة.';
    content.innerHTML = '<div class="muted">جاري تحميل بنود الداخل...</div>';
    try {
      const data = await authFetch('/inflow-items' + safeQs());
      state.rows = data.rows || [];
      state.users = data.users || [];
      if (!Number(data.can_assign_any_work_custody || 0) && data.current_user) {
        state.users = state.users.filter(u => isCashboxUsername(u.username) || String(u.username || '').trim().toLowerCase() === String(data.current_user || '').trim().toLowerCase());
      }
      if (!state.users.some(u => isCashboxUsername(u.username))) state.users.unshift({username:'__cashbox__',full_name:'الخزنة'});
      const bulkOptions = actorOptions('__cashbox__');
      const controls = `
        <div class="form-grid" style="margin-bottom:12px;align-items:end">
          <div><label>تحديد جماعي لدخول الفلوس في</label><select id="bulk_inflow_actor">${bulkOptions}</select></div>
          <div><button type="button" onclick="toggleAllInflowCustodyRows(true)">تحديد الكل</button> <button type="button" onclick="toggleAllInflowCustodyRows(false)">إلغاء التحديد</button> <button type="button" class="btn" onclick="applyInflowCustodyBulk()">تطبيق على المحدد</button></div>
        </div>`;
      const rowsHtml = state.rows.length ? state.rows.map(row => `
        <tr>
          <td>${rowCheckbox(row)}</td>
          <td>${escapeValue(row.date || '-')}</td>
          <td>${escapeValue(row.source || '-')}<br><small>${escapeValue(row.details || '-')}</small></td>
          <td>${rowSelect(row)}</td>
          <td>${moneyLocal(row.amount || 0)}</td>
        </tr>`).join('') : '<tr><td colspan="5">لا توجد بنود داخل في الفترة المحددة</td></tr>';
      content.innerHTML = controls + `<div style="overflow:auto"><table><thead><tr><th style="width:44px">اختيار</th><th>التاريخ</th><th>مصدر الداخل</th><th>دخلت في</th><th>القيمة</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>`;
    } catch (e) {
      content.innerHTML = `<div class="alert bad">${escapeValue(e.message || 'فشل تحميل تفاصيل الداخل')}</div>`;
    }
  }
  window.toggleAllInflowCustodyRows = function(flag){ document.querySelectorAll('.inflow-custody-check').forEach(cb => { cb.checked = !!flag; }); };
  window.assignInflowCustodyRow = async function(key, actorUsername){
    try {
      await authFetch('/assign-inflow-custody', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ actor_username: actorUsername || '__cashbox__', rows:[{key}] })});
      await renderInflowCustodyDetails();
      if (typeof loadCash === 'function') await loadCash();
      if (typeof loadSummary === 'function') await loadSummary();
    } catch (e) { alert(e.message || 'فشل تحديد مكان الدخول'); }
  };
  window.applyInflowCustodyBulk = async function(){
    const selected = Array.from(document.querySelectorAll('.inflow-custody-check:checked')).map(cb => ({key: cb.value}));
    if (!selected.length) return alert('اختار بند واحد على الأقل');
    const actor = $('bulk_inflow_actor')?.value || '__cashbox__';
    try {
      await authFetch('/assign-inflow-custody', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ actor_username: actor, rows:selected })});
      await renderInflowCustodyDetails();
      if (typeof loadCash === 'function') await loadCash();
      if (typeof loadSummary === 'function') await loadSummary();
      alert('تم تطبيق مكان دخول الفلوس على البنود المحددة');
    } catch (e) { alert(e.message || 'فشل التطبيق الجماعي'); }
  };
  const oldShowDetails = window.showDetails;
  window.showDetails = async function(type){
    if (type !== 'inflows') return oldShowDetails ? oldShowDetails.apply(this, arguments) : undefined;
    if (typeof canOpenDetails === 'function' && !canOpenDetails(type)) return;
    const detailsEl = $('detailsPanel');
    const cashEl = $('cashPanel');
    if (detailsEl) detailsEl.classList.remove('hidden');
    if (cashEl) cashEl.classList.add('hidden');
    if (typeof activateCard === 'function') activateCard(type);
    await renderInflowCustodyDetails();
  };
})();
