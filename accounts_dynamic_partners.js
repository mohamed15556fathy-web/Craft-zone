(function(){
  if (typeof authFetch !== 'function') return;
  const canManagePartnersFromUser = !!(user && ((user.username==='admin'||user.role==='super_admin') || user.username === 'admin' || Number(user.perm_manage_financial_partners || 0) === 1));
  let financialPartners = [];
  let activeFinancialPartners = [];
  let activeAdminUsers = [];
  let dynamicPartnerFilter = 'all';
  let dynamicPartnerFundFilter = 'all';
  let dynamicPartnerFundRows = [];
  let dynamicEditingFundId = null;
  let dynamicEditingPartnerId = null;

  function normPartner(v=''){
    const name = String(v || '').trim().replace(/\s+/g,' ');
    if (!name) return '';
    if (name.replace(/\s+/g,'') === 'عبدالقادر') return 'عبدالقادر';
    return name;
  }
  window.normalizePartnerName = normPartner;
  function isMurabahaPartner(p){ return String((p && p.partner_type) || 'equity') === 'murabaha'; }
  function normAdminKey(v=''){ return String(v || '').trim().replace(/[أإآ]/g,'ا').replace(/ة/g,'ه').replace(/ى/g,'ي').replace(/\s+/g,' ').toLowerCase(); }
  function isLinkedToActiveAdmin(name=''){
    const target = normAdminKey(normPartner(name));
    if (!target) return false;
    return activeAdminUsers.some(u => [u.username, u.full_name].some(v => {
      const k = normAdminKey(v);
      return k && (k === target || k.replace(/\s+/g,'') === target.replace(/\s+/g,''));
    }));
  }
  window.isPartnerOption = function(name='', opts={}){
    const target = normPartner(name);
    return activeFinancialPartners.some(p => normPartner(p.name) === target && (!opts.excludeMurabaha || !isMurabahaPartner(p))); 
  };
  function partnerNames({ includeInactiveNames=false, includeRows=[], excludeMurabaha=false, custodyOnly=false } = {}){
    const names = [];
    const source = includeInactiveNames ? financialPartners : activeFinancialPartners;
    source.filter(p => (!excludeMurabaha || !isMurabahaPartner(p))).forEach(p => { const n = normPartner(p.name); if (n && !names.includes(n)) names.push(n); });
    includeRows.forEach(r => { const n = normPartner(r.partner_name || r.expense_partner_name || ''); if (n && !names.includes(n)) names.push(n); });
    return names;
  }
  function firstActivePartner(opts={}){ return partnerNames(opts)[0] || ''; }
  function safeMoney(v){ return typeof money === 'function' ? money(v) : (Number(v||0).toLocaleString('en-US',{maximumFractionDigits:2})+' ج'); }
  function safeEsc(v){ return typeof esc === 'function' ? esc(v) : String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
  function jsArg(v){ return safeEsc(JSON.stringify(String(v ?? ''))); }
  function todayLocal(){ return typeof todayStr === 'function' ? todayStr() : new Date().toISOString().slice(0,10); }
  function queryString(){ return typeof qs === 'function' ? qs() : ''; }
  function partnerByName(name){ return financialPartners.find(p => normPartner(p.name) === normPartner(name)) || null; }
  async function loadFinancialPartners(includeInactive){
    const q = includeInactive || canManagePartnersFromUser ? '?include_inactive=1' : '';
    const data = await authFetch('/financial-partners'+q);
    financialPartners = Array.isArray(data.partners) ? data.partners.map(p=>({ ...p, name:normPartner(p.name) })) : [];
    activeFinancialPartners = financialPartners.filter(p => Number(p.is_active) !== 0);
    activeAdminUsers = Array.isArray(data.active_users) ? data.active_users : [];
    if (!activeAdminUsers.length) {
      try { const ud = await authFetch('/active-users-lite'); activeAdminUsers = Array.isArray(ud.users) ? ud.users : []; } catch(_) { activeAdminUsers = []; }
    }
    renderPartnerPickers();
    renderPartnerManager();
    return data;
  }
  function renderButtons(containerId, selectedName, onclickName, opts={}){
    const box = document.getElementById(containerId);
    if (!box) return;
    const names = partnerNames(opts);
    box.innerHTML = names.length ? names.map(name=>`<button type="button" class="partner-choice-btn ${normPartner(selectedName)===name?'active':''}" onclick="${onclickName}(${jsArg(name)})">${safeEsc(name)}</button>`).join('') : '<span class="hint">لا يوجد شركاء نشطون. أضف شريك أولاً.</span>';
  }
  window.getPartnerExpenseOptionsHtml = function(selected=''){
    const selectedValue = String(selected || '').trim();
    return partnerNames({excludeMurabaha:true}).map(name=>{
      const value = 'partner:' + name;
      return `<option value="${safeEsc(value)}" ${selectedValue===value?'selected':''}>شريك: ${safeEsc(name)}</option>`;
    }).join('');
  };
  function renderExpensePartnerSelect(){
    const sel = document.getElementById('expense_partner_name');
    if (!sel) return;
    if (window.__adminCashCustodyMode) {
      const current = sel.value || '';
      if (typeof window.renderAdminCashExpenseFundField === 'function') window.renderAdminCashExpenseFundField();
      const partnerSelected = String(current).startsWith('partner:') ? current : '';
      const html = typeof window.getPartnerExpenseOptionsHtml === 'function' ? window.getPartnerExpenseOptionsHtml(partnerSelected) : '';
      if (html) {
        sel.insertAdjacentHTML('beforeend', html);
        const holder = sel.closest('div');
        if (holder) holder.classList.remove('hidden');
        const label = holder ? holder.querySelector('label') : null;
        if (label) label.textContent = 'الخصم من عهدة الشغل';
        const hint = [...document.querySelectorAll('#expenseFundHolder ~ .hint, #expenseFundHolder + .hint')].find(Boolean);
        if (hint) hint.textContent = 'اختَر أدمن أو شريك لو المصروف اتدفع من عهدة الشغل. لو سبتها فاضية يتحسب من الخزنة.';
      }
      if (partnerSelected) sel.value = partnerSelected;
      return;
    }
    const current = normPartner(sel.value);
    const names = partnerNames({excludeMurabaha:true});
    sel.innerHTML = '<option value="">الخزنة / بدون عهدة</option>' + names.map(name=>`<option value="${safeEsc(name)}">${safeEsc(name)}</option>`).join('');
    if (current && names.includes(current)) sel.value = current;
  }
  function renderPartnerPickers(){
    const selected = normPartner(document.getElementById('partner_name')?.value) || firstActivePartner();
    const fundSelected = normPartner(document.getElementById('fund_partner_name')?.value) || firstActivePartner({excludeMurabaha:true,custodyOnly:true});
    renderButtons('partnerQuickSelect', selected, 'selectPartner');
    renderButtons('fundQuickSelect', fundSelected, 'selectFundPartner', {excludeMurabaha:true,custodyOnly:true});
    renderExpensePartnerSelect();
    if (selected) window.selectPartner(selected); else window.selectPartner('');
    if (fundSelected) window.selectFundPartner(fundSelected); else window.selectFundPartner('');
  }

  window.selectPartner = function(name){
    const normalized = normPartner(name) || firstActivePartner();
    const hidden = document.getElementById('partner_name');
    const label = document.getElementById('selectedPartnerLabel');
    if (hidden) hidden.value = normalized;
    if (label) label.textContent = normalized ? ('الشريك المحدد: '+normalized) : 'لا يوجد شريك محدد';
    const box = document.getElementById('partnerQuickSelect');
    if (box) box.querySelectorAll('.partner-choice-btn').forEach(btn=>btn.classList.toggle('active', normPartner(btn.textContent)===normalized));
  };
  window.selectFundPartner = function(name){
    const normalized = normPartner(name) || firstActivePartner({excludeMurabaha:true,custodyOnly:true});
    const hidden = document.getElementById('fund_partner_name');
    const label = document.getElementById('selectedFundPartnerLabel');
    if (hidden) hidden.value = normalized;
    if (label) label.textContent = normalized ? ('العهدة المحددة: '+normalized) : 'لا يوجد شريك محدد';
    const box = document.getElementById('fundQuickSelect');
    if (box) box.querySelectorAll('.partner-choice-btn').forEach(btn=>btn.classList.toggle('active', normPartner(btn.textContent)===normalized));
  };

  window.partnerRowsBy = function(name='all'){
    const target = normPartner(name);
    return (Array.isArray(partnerWithdrawalRows) ? partnerWithdrawalRows : []).filter(r => target==='all' ? true : normPartner(r.partner_name)===target);
  };
  window.partnerStats = function(name){
    const rows = window.partnerRowsBy(name);
    const total = rows.reduce((a,r)=>a+Number(r.amount||0),0);
    const todayTotal = rows.filter(r=>String(r.withdrawal_date||'').slice(0,10)===todayLocal()).reduce((a,r)=>a+Number(r.amount||0),0);
    const monthTotal = rows.filter(r=>String(r.withdrawal_date||'').slice(0,7)===todayLocal().slice(0,7)).reduce((a,r)=>a+Number(r.amount||0),0);
    const last = rows.slice().sort((a,b)=>String(b.withdrawal_date||'').localeCompare(String(a.withdrawal_date||'')) || Number(b.id||0)-Number(a.id||0))[0] || null;
    return { rows, total, todayTotal, monthTotal, count: rows.length, lastDate:last?.withdrawal_date || '-', lastAmount:last?.amount || 0 };
  };
  window.renderPartnerFilterTabs = function(){
    const wrap = document.getElementById('partnerFilterTabs');
    if (!wrap) return;
    const allTotal = (Array.isArray(partnerWithdrawalRows) ? partnerWithdrawalRows : []).reduce((a,r)=>a+Number(r.amount||0),0);
    const names = partnerNames({ includeRows: Array.isArray(partnerWithdrawalRows) ? partnerWithdrawalRows : [] });
    const tabs = [{key:'all',label:'الكل',total:allTotal}].concat(names.map(name=>({key:name,label:name,total:window.partnerStats(name).total})));
    wrap.innerHTML = tabs.map(tab=>`<button type="button" class="partner-filter-btn ${dynamicPartnerFilter===tab.key?'active':''}" onclick="setPartnerFilter(${jsArg(tab.key)})">${safeEsc(tab.label)} <small>(${safeMoney(tab.total)})</small></button>`).join('');
  };
  window.setPartnerFilter = function(name='all'){
    dynamicPartnerFilter = name || 'all';
    window.renderPartnerFilterTabs();
    window.renderPartnerTable();
  };
  window.renderPartnerDashboard = function(){
    const rows = Array.isArray(partnerWithdrawalRows) ? partnerWithdrawalRows : [];
    const names = partnerNames({ includeRows: rows });
    const cards = document.getElementById('partnerCards');
    if (cards) cards.innerHTML = names.length ? names.map(name=>{
      const s = window.partnerStats(name);
      const inactive = partnerByName(name) && Number(partnerByName(name).is_active) === 0 ? ' <span class="hint">(متوقف)</span>' : '';
      return `<div class="partner-card"><h4>${safeEsc(name)}${inactive}</h4><div class="big">${safeMoney(s.total)}</div><div class="partner-metric-grid"><div class="partner-metric"><small>عدد السحوبات</small><b>${s.count}</b></div><div class="partner-metric"><small>سحب اليوم</small><b>${safeMoney(s.todayTotal)}</b></div><div class="partner-metric"><small>سحب الشهر</small><b>${safeMoney(s.monthTotal)}</b></div><div class="partner-metric"><small>آخر سحبة</small><b>${s.lastDate==='-'?'-':`${safeEsc(s.lastDate)} | ${safeMoney(s.lastAmount)}`}</b></div></div></div>`;
    }).join('') : '<span class="hint">لا توجد شركاء نشطين</span>';
    const summary = document.getElementById('partnerSummary');
    if (!summary) return;
    const allTotal = rows.reduce((a,r)=>a+Number(r.amount||0),0);
    const stats = names.map(name=>({ name, total: window.partnerStats(name).total })).sort((a,b)=>b.total-a.total);
    const max = stats[0];
    const min = stats[stats.length-1];
    const diff = max && min ? Math.abs(max.total-min.total) : 0;
    summary.innerHTML = rows.length ? `<span class="partner-pill">إجمالي السحوبات<b>${safeMoney(allTotal)}</b></span><span class="partner-pill">عدد الشركاء<b>${names.length}</b></span>${max?`<span class="partner-pill">الأكثر سحبًا<b>${safeEsc(max.name)} - ${safeMoney(max.total)}</b></span>`:''}${stats.length>1?`<span class="partner-pill">فرق أعلى وأقل شريك<b>${safeMoney(diff)}</b></span>`:''}` : '<span class="hint">لا توجد سحوبات شركاء مسجلة حتى الآن</span>';
  };
  window.renderPartnerTable = function(){
    const tbody = document.getElementById('partnerBody');
    if (!tbody) return;
    const rows = window.partnerRowsBy(dynamicPartnerFilter);
    const title = document.getElementById('partnerLogTitle');
    if (title) title.textContent = dynamicPartnerFilter==='all'?'سجل سحوبات الشركاء':`سجل سحوبات ${dynamicPartnerFilter}`;
    tbody.innerHTML = rows.length ? rows.map(r=>`<tr><td>${safeEsc(r.withdrawal_date||'')}</td><td>${safeEsc(normPartner(r.partner_name)||'-')}</td><td>${safeMoney(r.amount)}</td><td>${safeEsc(r.note||'-')}</td><td>${safeEsc(r.created_by||'-')}</td><td><button onclick="editPartnerWithdrawal(${Number(r.id)})">تعديل</button> <button onclick="delPartnerWithdrawal(${Number(r.id)})">حذف</button></td></tr>`).join('') : '<tr><td colspan="6">لا توجد سحوبات لهذا العرض</td></tr>';
  };
  window.loadPartnerWithdrawals = async function(){
    partnerWithdrawalRows = (await authFetch('/partner-withdrawals'+queryString())).map(r=>({ ...r, partner_name:normPartner(r.partner_name)||String(r.partner_name||'').trim() }));
    window.renderPartnerDashboard();
    window.renderPartnerFilterTabs();
    window.renderPartnerTable();
  };
  window.partnerPayload = function(){
    return {
      partner_name: normPartner(document.getElementById('partner_name')?.value),
      amount: document.getElementById('partner_amount')?.value || 0,
      withdrawal_date: document.getElementById('partner_date')?.value || todayLocal(),
      note: document.getElementById('partner_note')?.value?.trim() || ''
    };
  };
  window.savePartnerWithdrawal = async function(){
    const body = window.partnerPayload();
    if (!window.isPartnerOption(body.partner_name)) return alert('اختار شريك نشط من قائمة الشركاء');
    if (Number(body.amount||0) <= 0) return alert('اكتب مبلغ صحيح');
    if (editingPartnerWithdrawalId) await authFetch('/update-partner-withdrawal/'+editingPartnerWithdrawalId,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    else await authFetch('/save-partner-withdrawal',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    window.cancelPartnerEdit();
    const log = document.getElementById('partnerLogWrap'); if (log) log.classList.remove('hidden');
    await window.loadPartnerWithdrawals();
    if (typeof loadCash === 'function') await loadCash();
  };
  window.editPartnerWithdrawal = function(id){
    const r = (Array.isArray(partnerWithdrawalRows) ? partnerWithdrawalRows : []).find(x=>Number(x.id)===Number(id));
    if (!r) return;
    editingPartnerWithdrawalId = id;
    window.selectPartner(r.partner_name);
    const amountEl = document.getElementById('partner_amount'); if (amountEl) amountEl.value = r.amount || '';
    const dateEl = document.getElementById('partner_date'); if (dateEl) dateEl.value = r.withdrawal_date || todayLocal();
    const noteEl = document.getElementById('partner_note'); if (noteEl) noteEl.value = r.note || '';
    const saveBtn = document.getElementById('savePartnerBtn'); if (saveBtn) saveBtn.innerText = 'حفظ التعديل';
    const cancelBtn = document.getElementById('cancelPartnerEditBtn'); if (cancelBtn) cancelBtn.classList.remove('hidden');
    const log = document.getElementById('partnerLogWrap'); if (log) log.classList.remove('hidden');
  };
  window.cancelPartnerEdit = function(){
    editingPartnerWithdrawalId = null;
    window.selectPartner(firstActivePartner());
    const amountEl = document.getElementById('partner_amount'); if (amountEl) amountEl.value = '';
    const dateEl = document.getElementById('partner_date'); if (dateEl) dateEl.value = todayLocal();
    const noteEl = document.getElementById('partner_note'); if (noteEl) noteEl.value = '';
    const saveBtn = document.getElementById('savePartnerBtn'); if (saveBtn) saveBtn.innerText = 'حفظ السحبة';
    const cancelBtn = document.getElementById('cancelPartnerEditBtn'); if (cancelBtn) cancelBtn.classList.add('hidden');
  };
  window.delPartnerWithdrawal = async function(id){
    if (!confirm('حذف سحب الشريك؟')) return;
    await authFetch('/delete-partner-withdrawal/'+id,{method:'DELETE'});
    await window.loadPartnerWithdrawals();
    if (typeof loadCash === 'function') await loadCash();
  };

  function fundRowsBy(name='all'){
    const target = normPartner(name);
    return dynamicPartnerFundRows.filter(r => target==='all' ? true : normPartner(r.partner_name)===target);
  }
  function fundStats(name){
    const rows = fundRowsBy(name);
    const balance = rows.reduce((a,r)=>a+Number(r.delta||0),0);
    const received = rows.filter(r=>Number(r.delta||0)>0).reduce((a,r)=>a+Number(r.delta||0),0);
    const spent = rows.filter(r=>Number(r.delta||0)<0).reduce((a,r)=>a+Math.abs(Number(r.delta||0)),0);
    const todaySpent = rows.filter(r=>String(r.entry_date||'').slice(0,10)===todayLocal() && Number(r.delta||0)<0).reduce((a,r)=>a+Math.abs(Number(r.delta||0)),0);
    const last = rows.slice().sort((a,b)=>String(b.entry_date||'').localeCompare(String(a.entry_date||'')) || Number(b.id||0)-Number(a.id||0))[0] || null;
    return { rows, balance, received, spent, todaySpent, count:rows.length, last };
  }
  function fundActionLabel(kind=''){
    const k = String(kind||'');
    if (k==='add') return 'تسليم فلوس شغل';
    if (k==='sub') return 'استرداد من العهدة';
    if (k==='set') return 'تعيين الرصيد الفعلي';
    if (k==='expense') return 'مصروف من العهدة';
    if (k==='order_cost') return 'تكلفة أوردر من العهدة';
    return k || '-';
  }
  window.renderPartnerFundFilterTabs = function(){
    const wrap = document.getElementById('partnerFundFilterTabs');
    if (!wrap) return;
    const names = partnerNames({ includeRows: dynamicPartnerFundRows });
    const allBalance = dynamicPartnerFundRows.reduce((a,r)=>a+Number(r.delta||0),0);
    const tabs = [{key:'all',label:'الكل',total:allBalance}].concat(names.map(name=>({key:name,label:name,total:fundStats(name).balance})));
    wrap.innerHTML = tabs.map(tab=>`<button type="button" class="partner-filter-btn ${dynamicPartnerFundFilter===tab.key?'active':''}" onclick="setPartnerFundFilter(${jsArg(tab.key)})">${safeEsc(tab.label)} <small>(${safeMoney(tab.total)})</small></button>`).join('');
  };
  window.setPartnerFundFilter = function(name='all'){
    dynamicPartnerFundFilter = name || 'all';
    window.renderPartnerFundFilterTabs();
    window.renderPartnerFundTable();
  };
  window.renderPartnerFundSummary = function(){
    const names = partnerNames({ includeRows: dynamicPartnerFundRows });
    const cards = document.getElementById('partnerFundCards');
    if (cards) cards.innerHTML = names.length ? names.map(name=>{
      const s = fundStats(name);
      const lastText = s.last ? `${safeEsc(s.last.entry_date||'-')} | ${fundActionLabel(s.last.entry_kind)} | ${safeMoney(s.last.delta||0)}` : '-';
      const inactive = partnerByName(name) && Number(partnerByName(name).is_active) === 0 ? ' <span class="hint">(متوقف)</span>' : '';
      return `<div class="partner-card"><h4>${safeEsc(name)}${inactive}</h4><div class="big">${safeMoney(s.balance)}</div><div class="partner-metric-grid"><div class="partner-metric"><small>إجمالي فلوس الشغل المستلمة</small><b>${safeMoney(s.received)}</b></div><div class="partner-metric"><small>إجمالي المصروف/الخصم</small><b>${safeMoney(s.spent)}</b></div><div class="partner-metric"><small>خصم اليوم</small><b>${safeMoney(s.todaySpent)}</b></div><div class="partner-metric"><small>آخر حركة</small><b>${lastText}</b></div></div></div>`;
    }).join('') : '<span class="hint">لا توجد شركاء نشطين</span>';
    const summary = document.getElementById('partnerFundSummary');
    if (!summary) return;
    const stats = names.map(name=>({ name, balance: fundStats(name).balance })).sort((a,b)=>b.balance-a.balance);
    const allBalance = stats.reduce((a,r)=>a+r.balance,0);
    const max = stats[0]; const min = stats[stats.length-1];
    summary.innerHTML = `<span class="partner-pill">إجمالي عهدة الشغل<b>${safeMoney(allBalance)}</b></span><span class="partner-pill">عدد الشركاء<b>${names.length}</b></span>${max?`<span class="partner-pill">الأعلى رصيدًا<b>${safeEsc(max.name)} - ${safeMoney(max.balance)}</b></span>`:''}${stats.length>1?`<span class="partner-pill">فرق أعلى وأقل رصيد<b>${safeMoney(Math.abs(max.balance-min.balance))}</b></span>`:''}`;
  };
  window.renderPartnerFundTable = function(){
    const tbody = document.getElementById('partnerFundBody');
    if (!tbody) return;
    const rows = fundRowsBy(dynamicPartnerFundFilter);
    const title = document.getElementById('partnerFundLogTitle');
    if (title) title.textContent = dynamicPartnerFundFilter==='all' ? 'سجل عهدة الشغل' : `سجل عهدة ${dynamicPartnerFundFilter}`;
    tbody.innerHTML = rows.length ? rows.map(r=>`<tr><td>${safeEsc(r.entry_date||'')}</td><td>${safeEsc(normPartner(r.partner_name)||'-')}</td><td>${safeEsc(fundActionLabel(r.entry_kind))}</td><td>${safeMoney(r.delta||0)}</td><td>${safeMoney(r.balance_after||0)}</td><td>${safeEsc(r.note||'-')}</td><td>${safeEsc(r.created_by||'-')}</td><td>${Number(r.is_auto)===1?'<span class="hint">تلقائي</span>':`<button onclick="editPartnerFundEntry(${Number(r.id)})">تعديل</button> <button onclick="delPartnerFundEntry(${Number(r.id)})">حذف</button>`}</td></tr>`).join('') : '<tr><td colspan="8">لا توجد حركات عهدة لهذا العرض</td></tr>';
  };
  window.loadPartnerFundLog = async function(){
    dynamicPartnerFundRows = (await authFetch('/partner-fund-log'+queryString())).map(r=>({ ...r, partner_name:normPartner(r.partner_name)||String(r.partner_name||'').trim() }));
    window.renderPartnerFundSummary();
    window.renderPartnerFundFilterTabs();
    window.renderPartnerFundTable();
  };
  window.partnerFundPayload = function(){
    return {
      partner_name: normPartner(document.getElementById('fund_partner_name')?.value),
      amount: document.getElementById('fund_amount')?.value || 0,
      entry_date: document.getElementById('fund_date')?.value || todayLocal(),
      note: document.getElementById('fund_note')?.value?.trim() || '',
      entry_mode: document.getElementById('fund_mode')?.value || 'add'
    };
  };
  window.savePartnerFundEntry = async function(){
    const body = window.partnerFundPayload();
    if (!window.isPartnerOption(body.partner_name,{excludeMurabaha:true,custodyOnly:true})) return alert('اختار شريك أساسي نشط من القائمة');
    if (Number(body.amount||0) <= 0) return alert('اكتب مبلغ صحيح');
    if (dynamicEditingFundId) await authFetch('/update-partner-fund-entry/'+dynamicEditingFundId,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    else await authFetch('/save-partner-fund-entry',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    window.cancelPartnerFundEdit();
    await window.loadPartnerFundLog();
    if (typeof loadExpenses === 'function') await loadExpenses();
  };
  window.editPartnerFundEntry = function(id){
    const row = dynamicPartnerFundRows.find(r=>Number(r.id)===Number(id));
    if (!row || Number(row.is_auto)===1) return;
    dynamicEditingFundId = id;
    window.selectFundPartner(row.partner_name);
    const amountEl = document.getElementById('fund_amount'); if (amountEl) amountEl.value = row.amount || Math.abs(Number(row.delta||0)) || '';
    const dateEl = document.getElementById('fund_date'); if (dateEl) dateEl.value = row.entry_date || todayLocal();
    const noteEl = document.getElementById('fund_note'); if (noteEl) noteEl.value = row.note || '';
    const modeEl = document.getElementById('fund_mode'); if (modeEl) modeEl.value = ['add','sub','set'].includes(String(row.entry_kind||'')) ? row.entry_kind : (Number(row.delta||0)>=0 ? 'add' : 'sub');
    const saveBtn = document.getElementById('saveFundBtn'); if (saveBtn) saveBtn.innerText = 'حفظ تعديل العهدة';
    const cancelBtn = document.getElementById('cancelFundEditBtn'); if (cancelBtn) cancelBtn.classList.remove('hidden');
  };
  window.cancelPartnerFundEdit = function(){
    dynamicEditingFundId = null;
    window.selectFundPartner(firstActivePartner({excludeMurabaha:true,custodyOnly:true}));
    const amountEl = document.getElementById('fund_amount'); if (amountEl) amountEl.value = '';
    const dateEl = document.getElementById('fund_date'); if (dateEl) dateEl.value = todayLocal();
    const noteEl = document.getElementById('fund_note'); if (noteEl) noteEl.value = '';
    const modeEl = document.getElementById('fund_mode'); if (modeEl) modeEl.value = 'add';
    const saveBtn = document.getElementById('saveFundBtn'); if (saveBtn) saveBtn.innerText = 'حفظ حركة العهدة';
    const cancelBtn = document.getElementById('cancelFundEditBtn'); if (cancelBtn) cancelBtn.classList.add('hidden');
  };
  window.delPartnerFundEntry = async function(id){
    const row = dynamicPartnerFundRows.find(r=>Number(r.id)===Number(id));
    if (!row || Number(row.is_auto)===1) return alert('الحركات التلقائية تتحذف من المصروف نفسه');
    if (!confirm('حذف حركة العهدة؟')) return;
    await authFetch('/delete-partner-fund-entry/'+id,{method:'DELETE'});
    await window.loadPartnerFundLog();
  };

  function expensePayload(){
    const custodyValue = document.getElementById('expense_partner_name')?.value || '';
    return { expense_date:todayLocal(), amount:document.getElementById('amount')?.value || 0, linked_to_order:document.getElementById('linked_to_order')?.value || 0, order_id:document.getElementById('order_id')?.value || '', order_cost_field:document.getElementById('order_cost_field')?.value || '', category:document.getElementById('category')?.value || '', custom_category:document.getElementById('custom_category')?.value || '', notes:document.getElementById('notes')?.value || '', expense_partner_name:window.__adminCashCustodyMode ? '' : normPartner(custodyValue), actor_username:window.__adminCashCustodyMode ? custodyValue : '' };
  }
  window.payload = expensePayload; try { payload = expensePayload; } catch(_) {}
  window.loadExpenses = async function(){
    expensesRows = await authFetch('/get-expenses'+queryString());
    if (!expensesBody) return;
    const custodyLabel = (r)=> window.__adminCashCustodyMode ? (String(r.actor_name||r.actor_username||'').trim() || normPartner(r.expense_partner_name) || 'الخزنة') : (normPartner(r.expense_partner_name)||'الخزنة');
    expensesBody.innerHTML = expensesRows.length ? expensesRows.map(r=>`<tr><td>${safeEsc(r.expense_date||'')}</td><td>${safeEsc(Number(r.linked_to_order)===1?fieldLabel(r.order_cost_field):(r.category==='أخرى'?(r.custom_category||'أخرى'):(r.category||'')))}</td><td>${safeMoney(r.amount)}</td><td>${safeEsc(custodyLabel(r))}</td><td>${Number(r.linked_to_order)===1?'نعم':'لا'}</td><td>${Number(r.linked_to_order)===1?`أوردر #${r.order_id||''}${r.notes?`<br><small>${safeEsc(r.notes)}</small>`:''}`:safeEsc(r.notes||'-')}</td><td><button onclick="editExpense(${Number(r.id)})">تعديل</button> <button onclick="delExpense(${Number(r.id)})">حذف</button></td></tr>`).join('') : '<tr><td colspan="7">لا يوجد مصاريف</td></tr>';
  }; try { loadExpenses = window.loadExpenses; } catch(_) {}
  const oldEditExpenseDynamic = window.editExpense;
  window.editExpense = function(id){ oldEditExpenseDynamic(id); const row = (Array.isArray(expensesRows) ? expensesRows.find(x=>Number(x.id)===Number(id)) : null); const sel=document.getElementById('expense_partner_name'); if (sel) { renderExpensePartnerSelect(); sel.value = window.__adminCashCustodyMode ? String(row?.actor_username||'') : (normPartner(row?.expense_partner_name||'') || ''); } };
  try { editExpense = window.editExpense; } catch(_) {}
  const oldCancelExpenseDynamic = window.cancelExpenseEdit;
  window.cancelExpenseEdit = function(){ oldCancelExpenseDynamic(); renderExpensePartnerSelect(); const sel=document.getElementById('expense_partner_name'); if (sel) sel.value=''; };
  try { cancelExpenseEdit = window.cancelExpenseEdit; } catch(_) {}

  function partnerAccountTypeLabel(p){ return String(p?.partner_account_type || 'external') === 'admin' ? 'أدمن داخل السيستم' : 'شريك خارجي'; }
  function partnerTypeLabel(p){ return isMurabahaPartner(p) ? 'مرابحة / ممول' : 'شريك أساسي'; }
  function linkedAdminOptionsHtml(selected=''){
    const current = String(selected || '').trim();
    const users = Array.isArray(activeAdminUsers) ? activeAdminUsers : [];
    if (!users.length) return '<option value="">لا يوجد أدمن نشط</option>';
    return '<option value="">اختار الأدمن</option>' + users.map(u=>`<option value="${safeEsc(u.username||'')}" ${String(u.username||'')===current?'selected':''}>${safeEsc(u.full_name||u.username||'')} (${safeEsc(u.username||'')})</option>`).join('');
  }
  window.toggleFinancialPartnerAccountFields = function(){
    const type = document.getElementById('financialPartnerAccountType');
    const wrap = document.getElementById('financialPartnerLinkedAdminWrap');
    if (!type || !wrap) return;
    wrap.classList.toggle('hidden', String(type.value) !== 'admin');
  };
  window.syncFinancialPartnerNameFromAdmin = function(){
    const sel = document.getElementById('financialPartnerLinkedAdmin');
    const name = document.getElementById('financialPartnerName');
    if (!sel || !name) return;
    const u = activeAdminUsers.find(x=>String(x.username||'')===String(sel.value||''));
    if (u && !name.value.trim()) name.value = u.full_name || u.username || '';
  };
  window.toggleFinancialPartnerTypeFields = function(){
    const type = document.getElementById('financialPartnerType');
    const share = document.getElementById('financialPartnerShareWrap');
    const profit = document.getElementById('financialPartnerProfitRateWrap');
    if (!type) return;
    const mur = String(type.value) === 'murabaha';
    if (share) share.classList.toggle('hidden', mur);
    if (profit) profit.classList.toggle('hidden', !mur);
    if (mur) { const inp=document.getElementById('financialPartnerShare'); if (inp) inp.value=0; }
  };
  function renderPartnerManager(){
    const wrap = document.getElementById('partnerSectionWrap');
    if (!wrap) return;
    let panel = document.getElementById('financialPartnersManager');
    if (!canManagePartnersFromUser) { if (panel) panel.remove(); return; }
    if (!panel) {
      panel = document.createElement('div');
      panel.id = 'financialPartnersManager';
      panel.className = 'panel';
      panel.style.cssText = 'padding:18px;margin-bottom:18px';
      wrap.insertBefore(panel, wrap.firstChild);
    }
    const rows = financialPartners;
    const editing = financialPartners.find(p=>Number(p.id)===Number(dynamicEditingPartnerId));
    const accountType = String(editing?.partner_account_type || 'external') === 'admin' ? 'admin' : 'external';
    const pType = isMurabahaPartner(editing) ? 'murabaha' : 'equity';
    const activeVal = Number(editing?.is_active ?? 1) === 0 ? '0' : '1';
    panel.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap"><div><h3 style="margin:0">إدارة الشركاء من الحسابات</h3><div class="hint" style="margin-top:8px">نفس بيانات صفحة الشركاء: تقدر تربط الشريك بأدمن موجود أو تخليه شريك خارجي، وتحدد نوعه ونسبته.</div></div><span class="badge">إضافة / تعديل / حذف من قائمة الشركاء</span></div>
      <div class="form-grid" style="margin-top:14px">
        <input id="financialPartnerEditId" type="hidden" value="${dynamicEditingPartnerId||''}">
        <div><label>اسم الشريك</label><input id="financialPartnerName" placeholder="مثال: أحمد" value="${safeEsc(editing?.name||'')}"></div>
        <div><label>الشريك مرتبط بأدمن؟</label><select id="financialPartnerAccountType" onchange="toggleFinancialPartnerAccountFields()"><option value="external" ${accountType==='external'?'selected':''}>شريك خارجي / مش أدمن</option><option value="admin" ${accountType==='admin'?'selected':''}>أدمن موجود في السيستم</option></select></div>
        <div id="financialPartnerLinkedAdminWrap" class="${accountType==='admin'?'':'hidden'}"><label>اختار الأدمن المرتبط</label><select id="financialPartnerLinkedAdmin" onchange="syncFinancialPartnerNameFromAdmin()">${linkedAdminOptionsHtml(editing?.linked_admin_username||'')}</select></div>
        <div><label>نوع الشريك</label><select id="financialPartnerType" onchange="toggleFinancialPartnerTypeFields()"><option value="equity" ${pType==='equity'?'selected':''}>شريك أساسي</option><option value="murabaha" ${pType==='murabaha'?'selected':''}>مرابحة / ممول</option></select></div>
        <div id="financialPartnerShareWrap" class="${pType==='murabaha'?'hidden':''}"><label>نسبة الشريك الأساسي %</label><input id="financialPartnerShare" type="number" step="0.01" min="0" max="100" value="${safeEsc(editing?.share_percent ?? '')}"></div>
        <div id="financialPartnerProfitRateWrap" class="${pType==='murabaha'?'':'hidden'}"><label>نسبة ربح المرابحة %</label><input id="financialPartnerProfitRate" type="number" step="0.01" min="0" value="${safeEsc(editing?.profit_rate_percent ?? '')}"></div>
        <div><label>الهاتف</label><input id="financialPartnerPhone" placeholder="اختياري" value="${safeEsc(editing?.phone||'')}"></div>
        <div><label>الحالة</label><select id="financialPartnerActive"><option value="1" ${activeVal==='1'?'selected':''}>نشط</option><option value="0" ${activeVal==='0'?'selected':''}>موقوف</option></select></div>
        <div style="grid-column:1/-1"><label>ملاحظات</label><input id="financialPartnerNotes" placeholder="اختياري" value="${safeEsc(editing?.notes||'')}"></div>
        <div class="actions" style="grid-column:1/-1"><button onclick="saveFinancialPartnerFromForm()">${dynamicEditingPartnerId?'حفظ تعديل الشريك':'إضافة شريك'}</button><button type="button" onclick="cancelFinancialPartnerEdit()">مسح</button></div>
      </div>
      <div style="overflow:auto;margin-top:14px"><table><thead><tr><th>الشريك</th><th>مرتبط بأدمن؟</th><th>نوعه</th><th>النسبة / الربح</th><th>هاتف</th><th>الحالة</th><th>ملاحظات</th><th>تحكم</th></tr></thead><tbody>${rows.length?rows.map(p=>`<tr><td><b>${safeEsc(p.name)}</b><br><small>${safeEsc(p.linked_admin_name||p.linked_admin_username||'')}</small></td><td>${safeEsc(partnerAccountTypeLabel(p))}</td><td>${safeEsc(partnerTypeLabel(p))}</td><td>${isMurabahaPartner(p)?safeEsc((Number(p.profit_rate_percent||0)).toLocaleString('en-US',{maximumFractionDigits:2})+'% ربح'):safeEsc((Number(p.share_percent||0)).toLocaleString('en-US',{maximumFractionDigits:2})+'%')}</td><td>${safeEsc(p.phone||'-')}</td><td>${Number(p.is_active||0)?'<span class="ok">نشط</span>':'<span class="off">موقوف</span>'}</td><td>${safeEsc(p.notes||'-')}</td><td><button onclick="editFinancialPartner(${Number(p.id)})">تعديل</button> <button onclick="deleteFinancialPartner(${Number(p.id)})">حذف</button></td></tr>`).join(''):'<tr><td colspan="8">لا يوجد شركاء</td></tr>'}</tbody></table></div>`;
    window.toggleFinancialPartnerAccountFields();
    window.toggleFinancialPartnerTypeFields();
  }
  window.editFinancialPartner = function(id){ dynamicEditingPartnerId = id; renderPartnerManager(); };
  window.cancelFinancialPartnerEdit = function(){ dynamicEditingPartnerId = null; renderPartnerManager(); };
  window.saveFinancialPartnerFromForm = async function(){
    const id = Number(document.getElementById('financialPartnerEditId')?.value || 0) || undefined;
    const accountType = document.getElementById('financialPartnerAccountType')?.value || 'external';
    const linkedAdmin = accountType === 'admin' ? (document.getElementById('financialPartnerLinkedAdmin')?.value || '') : '';
    const partnerType = document.getElementById('financialPartnerType')?.value || 'equity';
    const name = document.getElementById('financialPartnerName')?.value || '';
    const body = {
      id,
      name,
      partner_account_type: accountType,
      linked_admin_username: linkedAdmin,
      partner_type: partnerType,
      share_percent: partnerType === 'murabaha' ? 0 : (document.getElementById('financialPartnerShare')?.value || 0),
      profit_rate_percent: document.getElementById('financialPartnerProfitRate')?.value || 0,
      phone: document.getElementById('financialPartnerPhone')?.value || '',
      notes: document.getElementById('financialPartnerNotes')?.value || '',
      is_active: document.getElementById('financialPartnerActive')?.value || 1
    };
    if (!normPartner(body.name)) return alert('اكتب اسم الشريك');
    if (body.partner_account_type === 'admin' && !body.linked_admin_username) return alert('اختار الأدمن المرتبط بالشريك');
    await authFetch('/save-financial-partner',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    dynamicEditingPartnerId = null;
    await loadFinancialPartners(true);
    await window.loadPartnerWithdrawals();
    await window.loadPartnerFundLog();
    if (typeof loadExpenses === 'function') await loadExpenses();
  };
  window.deleteFinancialPartner = async function(id){
    const p = financialPartners.find(x=>Number(x.id)===Number(id)); if (!p) return;
    if (!confirm(`حذف الشريك ${p.name} نهائيًا من قائمة الشركاء؟ السجلات القديمة ستظل ظاهرة باسم الشريك حتى لا تختل الحسابات.`)) return;
    await authFetch('/delete-financial-partner/'+id,{method:'DELETE'});
    await loadFinancialPartners(true); await window.loadPartnerWithdrawals(); await window.loadPartnerFundLog(); if (typeof loadExpenses === 'function') await loadExpenses();
  };

  async function initDynamicPartners(){
    try {
      await loadFinancialPartners(true);
      const badgeTexts = document.querySelectorAll('.badge, .partner-log-sub, .hint');
      badgeTexts.forEach(el => { el.innerHTML = el.innerHTML.replace(/محمد أو عبدالقادر/g,'أي شريك نشط').replace(/محمد وعبدالقادر/g,'الشركاء').replace(/محمد أو بودا/g,'أي شريك نشط'); });
      window.cancelPartnerEdit();
      window.cancelPartnerFundEdit();
      await window.loadPartnerWithdrawals();
      await window.loadPartnerFundLog();
      if (typeof loadExpenses === 'function') await loadExpenses();
    } catch (e) {
      console.warn('dynamic partners init failed', e);
    }
  }
  initDynamicPartners();
})();
