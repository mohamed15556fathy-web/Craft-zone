(function(){
  if (typeof authFetch !== 'function') return;
  const TAX_RATE = 0.14;
  let adsRows = [];
  let adsSummary = { base_total:0, tax_total:0, total:0, count:0 };
  let editingAdId = null;
  let actorUsers = [];

  function byId(id){ return document.getElementById(id); }
  function fmt(v){ return typeof money === 'function' ? money(v) : (Number(v||0).toLocaleString('en-US',{maximumFractionDigits:2})+' ج'); }
  function html(v){ return typeof esc === 'function' ? esc(v) : String(v ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
  function q(){ return typeof qs === 'function' ? qs() : ''; }
  function today(){ return typeof todayStr === 'function' ? todayStr() : new Date().toISOString().slice(0,10); }
  function n(v){ const x=Number(v); return Number.isFinite(x) ? x : 0; }
  function isSuper(){ try { return user && (user.username === 'admin' || user.role === 'super_admin'); } catch(_) { return false; } }
  function canManageAds(){ try { return isSuper() || Number(user.perm_manage_expenses || 0) === 1; } catch(_) { return false; } }
  function actorName(row){
    if (!row) return '-';
    const username = String(row.username || row.admin_username || row.actor_username || '').trim();
    const full = String(row.full_name || row.admin_name || row.actor_name || '').trim();
    if (username === '__cashbox__' || username.toLowerCase() === 'cashbox' || full === 'الخزنة') return 'الخزنة';
    return full && full !== username ? `${full} (${username})` : (full || username || '-');
  }
  function selectedActorValue(){ return byId('ad_actor_username')?.value || '__cashbox__'; }
  function actorOptionsHtml(selected=''){
    const current = String(selected || '__cashbox__').trim() || '__cashbox__';
    const list = actorUsers.length ? actorUsers : [{ username:'__cashbox__', full_name:'الخزنة', is_cashbox:1 }];
    return list.map(u => {
      const val = String(u.username || '').trim();
      return `<option value="${html(val)}" ${val===current?'selected':''}>${html(actorName(u))}</option>`;
    }).join('');
  }
  async function loadActorUsers(){
    try {
      const data = await authFetch('/active-users-lite?tracked_only=1');
      const users = Array.isArray(data.users) ? data.users : [];
      const hasCashbox = users.some(u => String(u.username || '').trim() === '__cashbox__');
      actorUsers = hasCashbox ? users : [{ username:'__cashbox__', full_name:'الخزنة', is_cashbox:1 }, ...users];
    } catch(_) {
      actorUsers = [{ username:'__cashbox__', full_name:'الخزنة', is_cashbox:1 }];
    }
  }
  function ensureStyles(){
    if (byId('adsPatchStyle')) return;
    const st = document.createElement('style');
    st.id = 'adsPatchStyle';
    st.textContent = `.ads-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:10px 0 16px}.ads-mini-card{background:#0b1220;border:1px solid var(--border);border-radius:14px;padding:12px}.ads-mini-card small{color:var(--muted);display:block}.ads-mini-card b{display:block;font-size:22px;margin-top:6px;color:#fbbf24}.ads-actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:center}.ads-tax-preview{background:#0b1220;border:1px dashed #fbbf24;border-radius:12px;padding:10px;color:#fde68a;font-weight:700}`;
    document.head.appendChild(st);
  }
  function ensureAdsCard(){
    if (byId('adsTotal')) return;
    const grid = document.querySelector('.summary-grid') || document.querySelector('.grid.summary-grid') || document.querySelector('.grid');
    if (!grid) return;
    const card = document.createElement('div');
    card.className = 'panel stat clickable sensitive-total';
    card.dataset.card = 'ads';
    card.onclick = () => window.showDetails('ads');
    card.innerHTML = `<small>إعلانات<br><span class="hint">اضغط لإضافة/عرض المصروفات</span></small><b id="adsTotal">0</b>`;
    const expensesCard = document.querySelector('[data-card="expenses"]');
    if (expensesCard && expensesCard.parentElement === grid) expensesCard.insertAdjacentElement('afterend', card);
    else grid.appendChild(card);
  }
  function ensureAdsExpenseOption(){
    const sel = byId('category');
    if (!sel) return;
    if (![...sel.options].some(o => String(o.value || o.textContent).trim() === 'إعلانات')) {
      const opt = document.createElement('option');
      opt.value = 'إعلانات';
      opt.textContent = 'إعلانات';
      const other = [...sel.options].find(o => String(o.value || o.textContent).trim() === 'أخرى');
      if (other) sel.insertBefore(opt, other); else sel.appendChild(opt);
    }
  }
  async function loadAdsSummary(){
    try {
      adsSummary = await authFetch('/ads-expenses-summary' + q());
      const total = byId('adsTotal');
      if (total) total.textContent = fmt(adsSummary.total || 0);
    } catch(_) {}
  }
  async function loadAdsRows(){
    adsRows = await authFetch('/ads-expenses' + q());
    return adsRows;
  }
  function calcTaxPreview(){
    const base = n(byId('ad_base_amount')?.value);
    const tax = +(base * TAX_RATE).toFixed(2);
    const total = +(base + tax).toFixed(2);
    const box = byId('ad_tax_preview');
    if (box) box.innerHTML = `المبلغ قبل الضريبة: ${fmt(base)} | ضريبة 14%: ${fmt(tax)} | الإجمالي المدفوع: ${fmt(total)}`;
  }
  window.calcAdTaxPreview = calcTaxPreview;
  function renderAdsDetails(){
    const title = byId('detailsTitle');
    const note = byId('detailsNote');
    const content = byId('detailsContent');
    if (!content) return;
    if (title) title.textContent = 'إعلانات';
    if (note) note.textContent = 'الإجمالي هنا داخل فترة الفلترة المختارة، وبيتحسب ضمن المصروفات ويخصم من صافي الربح.';
    const rowsHtml = adsRows.length ? adsRows.map(r => {
      const base = n(r.ad_base_amount) || (n(r.ad_tax_rate) > 0 ? n(r.amount)/(1+n(r.ad_tax_rate)) : n(r.amount));
      const tax = n(r.ad_tax_amount) || Math.max(0, n(r.amount) - base);
      return `<tr><td>${html(r.expense_date || '')}</td><td>${fmt(base)}</td><td>${fmt(tax)}</td><td><b>${fmt(r.amount)}</b></td><td>${html(r.actor_name || r.actor_username || 'الخزنة')}</td><td>${html(r.notes || '-')}</td><td class="ads-actions">${canManageAds()?`<button onclick="editAdExpense(${Number(r.id)})">تعديل</button><button onclick="deleteAdExpense(${Number(r.id)})">حذف</button>`:'-'}</td></tr>`;
    }).join('') : '<tr><td colspan="7">لا توجد مصروفات إعلانات في الفترة الحالية</td></tr>';
    content.innerHTML = `
      <div class="ads-cards">
        <div class="ads-mini-card"><small>إجمالي الإعلان قبل الضريبة</small><b>${fmt(adsSummary.base_total || 0)}</b></div>
        <div class="ads-mini-card"><small>إجمالي ضريبة 14%</small><b>${fmt(adsSummary.tax_total || 0)}</b></div>
        <div class="ads-mini-card"><small>إجمالي المدفوع</small><b>${fmt(adsSummary.total || 0)}</b></div>
        <div class="ads-mini-card"><small>عدد القيود</small><b>${Number(adsSummary.count || 0)}</b></div>
      </div>
      ${canManageAds()?`<div class="panel" style="padding:14px;background:#0b1220;margin-bottom:14px"><h4 style="margin-top:0">${editingAdId?'تعديل مصروف إعلان':'إضافة مصروف إعلان'}</h4><div class="form-grid"><div><label>المبلغ قبل الضريبة</label><input id="ad_base_amount" type="number" oninput="calcAdTaxPreview()" placeholder="مثال: 1000"></div><div><label>التاريخ</label><input id="ad_date" type="date"></div><div><label>الخصم من</label><select id="ad_actor_username">${actorOptionsHtml('__cashbox__')}</select></div><div class="ads-tax-preview" id="ad_tax_preview">المبلغ قبل الضريبة: 0 ج | ضريبة 14%: 0 ج | الإجمالي المدفوع: 0 ج</div><div style="grid-column:1/-1"><label>ملاحظات</label><textarea id="ad_note" rows="2" placeholder="مثال: حملة فيسبوك / بوست ممول"></textarea></div><div style="display:flex;gap:10px;align-items:end;flex-wrap:wrap"><button class="filled" onclick="saveAdExpense()">${editingAdId?'حفظ التعديل':'حفظ الإعلان'}</button>${editingAdId?'<button type="button" onclick="cancelAdEdit()">إلغاء التعديل</button>':''}</div></div><div class="hint" style="margin-top:8px">لو كتبت 1000، السيستم هيسجل 1140 تلقائيًا: 1000 إعلان + 140 ضريبة.</div></div>`:'<div class="hint">إضافة الإعلانات تحتاج صلاحية إدارة المصاريف.</div>'}
      <div style="overflow:auto"><table><thead><tr><th>التاريخ</th><th>قبل الضريبة</th><th>الضريبة</th><th>الإجمالي المدفوع</th><th>اتخصمت من</th><th>ملاحظات</th><th>تحكم</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>`;
    const dateEl = byId('ad_date');
    if (dateEl && !dateEl.value) dateEl.value = today();
    calcTaxPreview();
  }
  async function refreshAdsDetails(){
    await Promise.all([loadAdsSummary(), loadAdsRows(), loadActorUsers()]);
    renderAdsDetails();
  }
  window.saveAdExpense = async function(){
    try {
      const body = {
        base_amount: byId('ad_base_amount')?.value || 0,
        tax_rate: TAX_RATE,
        expense_date: byId('ad_date')?.value || today(),
        actor_username: selectedActorValue(),
        notes: byId('ad_note')?.value?.trim() || ''
      };
      if (n(body.base_amount) <= 0) return alert('اكتب مبلغ الإعلان قبل الضريبة');
      const url = editingAdId ? ('/update-ad-expense/' + Number(editingAdId)) : '/save-ad-expense';
      await authFetch(url, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
      editingAdId = null;
      await refreshAdsDetails();
      if (typeof loadSummary === 'function') await loadSummary();
      if (typeof loadCash === 'function') await loadCash();
      if (typeof loadExpenses === 'function' && byId('expenseLogWrap') && !byId('expenseLogWrap').classList.contains('hidden')) await loadExpenses();
    } catch(e) { alert(e.message || 'تعذر حفظ مصروف الإعلان'); }
  };
  window.editAdExpense = async function(id){
    const row = adsRows.find(r => Number(r.id) === Number(id));
    if (!row) return;
    editingAdId = Number(id);
    renderAdsDetails();
    const base = n(row.ad_base_amount) || (n(row.ad_tax_rate) > 0 ? n(row.amount)/(1+n(row.ad_tax_rate)) : n(row.amount));
    if (byId('ad_base_amount')) byId('ad_base_amount').value = Number(base.toFixed(2));
    if (byId('ad_date')) byId('ad_date').value = row.expense_date || today();
    if (byId('ad_actor_username')) byId('ad_actor_username').value = String(row.actor_username || '__cashbox__');
    if (byId('ad_note')) byId('ad_note').value = row.notes || '';
    calcTaxPreview();
    byId('detailsPanel')?.scrollIntoView({behavior:'smooth', block:'start'});
  };
  window.cancelAdEdit = function(){ editingAdId = null; renderAdsDetails(); };
  window.deleteAdExpense = async function(id){
    if (!confirm('حذف مصروف الإعلان؟')) return;
    await authFetch('/delete-expense/' + Number(id), { method:'DELETE' });
    if (editingAdId === Number(id)) editingAdId = null;
    await refreshAdsDetails();
    if (typeof loadSummary === 'function') await loadSummary();
    if (typeof loadCash === 'function') await loadCash();
  };
  function overrideDetails(){
    const oldShow = window.showDetails;
    window.showDetails = async function(type){
      if (type !== 'ads') return oldShow ? oldShow.apply(this, arguments) : undefined;
      const panel = byId('detailsPanel');
      const cash = byId('cashPanel');
      if (panel && currentDetailsCard === 'ads' && !panel.classList.contains('hidden')) {
        if (typeof closeDetails === 'function') closeDetails();
        return;
      }
      if (typeof activateCard === 'function') activateCard('ads');
      if (panel) panel.classList.remove('hidden');
      if (cash) cash.classList.add('hidden');
      await refreshAdsDetails();
    };
  }
  function overrideReload(){
    const oldLoadSummary = window.loadSummary;
    if (oldLoadSummary) {
      window.loadSummary = async function(){ const r = await oldLoadSummary.apply(this, arguments); await loadAdsSummary(); return r; };
    }
    const oldReload = window.reloadAll;
    if (oldReload) {
      window.reloadAll = function(){ const r = oldReload.apply(this, arguments); Promise.resolve(r).finally(loadAdsSummary); return r; };
    }
  }
  async function init(){
    ensureStyles();
    ensureAdsCard();
    ensureAdsExpenseOption();
    overrideDetails();
    overrideReload();
    await loadActorUsers();
    await loadAdsSummary();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
