
(function(){
  if (typeof authFetch !== 'function') return;
  const PARTNERS = ['محمد','عبدالقادر'];
  let partnerFundRows = [];
  let editingPartnerFundId = null;
  let activePartnerFundFilter = 'all';

  function normPartner(v=''){
    const name = (typeof normalizePartnerName==='function' ? normalizePartnerName(v) : String(v||'').trim());
    return PARTNERS.includes(name) ? name : '';
  }
  function todayLocal(){ return typeof todayStr==='function' ? todayStr() : new Date().toISOString().slice(0,10); }
  function partnerFundRowsBy(name='all'){ const target = normPartner(name); return partnerFundRows.filter(r => target==='all' ? true : normPartner(r.partner_name)===target); }
  function partnerFundStats(name){
    const rows = partnerFundRowsBy(name);
    const balance = rows.reduce((a,r)=>a+Number(r.delta||0),0);
    const received = rows.filter(r=>Number(r.delta||0)>0).reduce((a,r)=>a+Number(r.delta||0),0);
    const spent = rows.filter(r=>Number(r.delta||0)<0).reduce((a,r)=>a+Math.abs(Number(r.delta||0)),0);
    const todaySpent = rows.filter(r=>String(r.entry_date||'').slice(0,10)===todayLocal() && Number(r.delta||0)<0).reduce((a,r)=>a+Math.abs(Number(r.delta||0)),0);
    const monthSpent = rows.filter(r=>String(r.entry_date||'').slice(0,7)===todayLocal().slice(0,7) && Number(r.delta||0)<0).reduce((a,r)=>a+Math.abs(Number(r.delta||0)),0);
    const last = rows.slice().sort((a,b)=>String(b.entry_date||'').localeCompare(String(a.entry_date||'')) || Number(b.id||0)-Number(a.id||0))[0] || null;
    return { rows, balance, received, spent, todaySpent, monthSpent, count: rows.length, last };
  }
  function partnerFundActionLabel(kind=''){
    const k = String(kind||'');
    if (k==='add') return 'تسليم فلوس شغل';
    if (k==='sub') return 'استرداد من العهدة';
    if (k==='set') return 'تعيين الرصيد الفعلي';
    if (k==='expense') return 'مصروف من العهدة';
    if (k==='order_cost') return 'تكلفة أوردر من العهدة';
    return k || '-';
  }
  function setPartnerSectionVisible(open){
    const wrap = document.getElementById('partnerSectionWrap');
    if (!wrap) return;
    wrap.classList.toggle('hidden', !open);
    if (open) {
      loadPartnerWithdrawals();
      loadPartnerFundLog();
    }
  }
  window.togglePartnerLog = function(){
    const wrap = document.getElementById('partnerSectionWrap');
    if (!wrap) return;
    setPartnerSectionVisible(wrap.classList.contains('hidden'));
  };
  window.togglePartnerRecords = function(){
    if (typeof partnerLogWrap==='undefined' || !partnerLogWrap) return;
    partnerLogWrap.classList.toggle('hidden');
    if (!partnerLogWrap.classList.contains('hidden')) loadPartnerWithdrawals();
  };
  window.togglePartnerFundLog = function(){
    const wrap = document.getElementById('partnerFundLogWrap');
    if (!wrap) return;
    wrap.classList.toggle('hidden');
    if (!wrap.classList.contains('hidden')) loadPartnerFundLog();
  };
  window.selectFundPartner = function(name){
    const normalized = normPartner(name) || PARTNERS[0];
    const hidden = document.getElementById('fund_partner_name');
    const label = document.getElementById('selectedFundPartnerLabel');
    if (hidden) hidden.value = normalized;
    if (label) label.textContent = 'العهدة المحددة: ' + normalized;
    const box = document.getElementById('fundQuickSelect');
    if (box) box.querySelectorAll('.partner-choice-btn').forEach(btn=>btn.classList.toggle('active', normPartner(btn.textContent)===normalized));
  };
  function renderPartnerFundFilterTabs(){
    const wrap = document.getElementById('partnerFundFilterTabs');
    if (!wrap) return;
    const tabs = [{key:'all',label:'الكل',total:partnerFundRows.reduce((a,r)=>a+Number(r.delta||0),0)}].concat(PARTNERS.map(name=>({key:name,label:name,total:partnerFundStats(name).balance})));
    wrap.innerHTML = tabs.map(tab=>`<button type="button" class="partner-filter-btn ${activePartnerFundFilter===tab.key?'active':''}" onclick="setPartnerFundFilter('${tab.key}')">${esc(tab.label)} <small>(${money(tab.total)})</small></button>`).join('');
  }
  window.setPartnerFundFilter = function(name='all'){
    activePartnerFundFilter = name || 'all';
    renderPartnerFundFilterTabs();
    renderPartnerFundTable();
  };
  function renderPartnerFundSummary(){
    const cards = document.getElementById('partnerFundCards');
    if (cards) cards.innerHTML = PARTNERS.map(name=>{
      const s = partnerFundStats(name);
      const lastText = s.last ? `${esc(s.last.entry_date||'-')} | ${partnerFundActionLabel(s.last.entry_kind)} | ${money(s.last.delta||0)}` : '-';
      return `<div class="partner-card"><h4>${esc(name)}</h4><div class="big">${money(s.balance)}</div><div class="partner-metric-grid"><div class="partner-metric"><small>إجمالي فلوس الشغل المستلمة</small><b>${money(s.received)}</b></div><div class="partner-metric"><small>إجمالي المصروف/الخصم</small><b>${money(s.spent)}</b></div><div class="partner-metric"><small>خصم اليوم</small><b>${money(s.todaySpent)}</b></div><div class="partner-metric"><small>آخر حركة</small><b>${lastText}</b></div></div></div>`;
    }).join('');
    const summary = document.getElementById('partnerFundSummary');
    if (!summary) return;
    const moh = partnerFundStats('محمد');
    const abd = partnerFundStats('عبدالقادر');
    const allBalance = moh.balance + abd.balance;
    const diff = Math.abs(moh.balance - abd.balance);
    const higher = moh.balance === abd.balance ? 'متساويين' : (moh.balance > abd.balance ? 'محمد' : 'عبدالقادر');
    summary.innerHTML = `<span class="partner-pill">إجمالي عهدة الشغل<b>${money(allBalance)}</b></span><span class="partner-pill">محمد معه<b>${money(moh.balance)}</b></span><span class="partner-pill">عبدالقادر معه<b>${money(abd.balance)}</b></span><span class="partner-pill">الفرق بينهما<b>${money(diff)}</b></span><span class="partner-pill">الأعلى رصيدًا<b>${esc(higher)}</b></span>`;
  }
  function renderPartnerFundTable(){
    const tbody = document.getElementById('partnerFundBody');
    const title = document.getElementById('partnerFundLogTitle');
    if (!tbody) return;
    const rows = partnerFundRowsBy(activePartnerFundFilter);
    if (title) title.textContent = activePartnerFundFilter==='all' ? 'سجل عهدة الشغل' : `سجل عهدة ${activePartnerFundFilter}`;
    tbody.innerHTML = rows.length ? rows.map(r=>`<tr><td>${esc(r.entry_date||'')}</td><td>${esc(normPartner(r.partner_name)||r.partner_name||'-')}</td><td>${esc(partnerFundActionLabel(r.entry_kind))}</td><td>${money(r.delta||0)}</td><td>${money(r.balance_after||0)}</td><td>${esc(r.note||'-')}</td><td>${esc(r.created_by||'-')}</td><td>${Number(r.is_auto)===1?'<span class="hint">تلقائي</span>':`<button onclick="editPartnerFundEntry(${r.id})">تعديل</button> <button onclick="delPartnerFundEntry(${r.id})">حذف</button>`}</td></tr>`).join('') : '<tr><td colspan="8">لا توجد حركات عهدة لهذا العرض</td></tr>';
  }
  window.loadPartnerFundLog = async function(){
    partnerFundRows = (await authFetch('/partner-fund-log'+qs())).map(r=>({ ...r, partner_name: normPartner(r.partner_name)||String(r.partner_name||'').trim() }));
    renderPartnerFundSummary();
    renderPartnerFundFilterTabs();
    renderPartnerFundTable();
  };
  function partnerFundPayload(){
    const hidden = document.getElementById('fund_partner_name');
    const amountEl = document.getElementById('fund_amount');
    const dateEl = document.getElementById('fund_date');
    const noteEl = document.getElementById('fund_note');
    const modeEl = document.getElementById('fund_mode');
    return {
      partner_name: normPartner(hidden?.value),
      amount: amountEl?.value || 0,
      entry_date: dateEl?.value || todayLocal(),
      note: noteEl?.value?.trim() || '',
      entry_mode: modeEl?.value || 'add'
    };
  }
  window.savePartnerFundEntry = async function(){
    const body = partnerFundPayload();
    if (!PARTNERS.includes(body.partner_name)) return alert('اختار محمد أو عبدالقادر');
    if (Number(body.amount||0) <= 0) return alert('اكتب مبلغ صحيح');
    if (editingPartnerFundId) {
      await authFetch('/update-partner-fund-entry/'+editingPartnerFundId,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    } else {
      await authFetch('/save-partner-fund-entry',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    }
    cancelPartnerFundEdit();
    await loadPartnerFundLog();
  };
  window.editPartnerFundEntry = function(id){
    const row = partnerFundRows.find(r=>Number(r.id)===Number(id));
    if (!row || Number(row.is_auto)===1) return;
    editingPartnerFundId = id;
    selectFundPartner(row.partner_name);
    const amountEl = document.getElementById('fund_amount');
    const dateEl = document.getElementById('fund_date');
    const noteEl = document.getElementById('fund_note');
    const modeEl = document.getElementById('fund_mode');
    const saveBtn = document.getElementById('saveFundBtn');
    const cancelBtn = document.getElementById('cancelFundEditBtn');
    if (amountEl) amountEl.value = row.amount || Math.abs(Number(row.delta||0)) || '';
    if (dateEl) dateEl.value = row.entry_date || todayLocal();
    if (noteEl) noteEl.value = row.note || '';
    if (modeEl) modeEl.value = ['add','sub','set'].includes(String(row.entry_kind||'')) ? row.entry_kind : (Number(row.delta||0)>=0 ? 'add' : 'sub');
    if (saveBtn) saveBtn.innerText = 'حفظ تعديل العهدة';
    if (cancelBtn) cancelBtn.classList.remove('hidden');
  };
  window.cancelPartnerFundEdit = function(){
    editingPartnerFundId = null;
    selectFundPartner(PARTNERS[0]);
    const amountEl = document.getElementById('fund_amount');
    const dateEl = document.getElementById('fund_date');
    const noteEl = document.getElementById('fund_note');
    const modeEl = document.getElementById('fund_mode');
    const saveBtn = document.getElementById('saveFundBtn');
    const cancelBtn = document.getElementById('cancelFundEditBtn');
    if (amountEl) amountEl.value = '';
    if (dateEl) dateEl.value = todayLocal();
    if (noteEl) noteEl.value = '';
    if (modeEl) modeEl.value = 'add';
    if (saveBtn) saveBtn.innerText = 'حفظ حركة العهدة';
    if (cancelBtn) cancelBtn.classList.add('hidden');
  };
  window.delPartnerFundEntry = async function(id){
    const row = partnerFundRows.find(r=>Number(r.id)===Number(id));
    if (!row || Number(row.is_auto)===1) return alert('الحركات التلقائية تتحذف من المصروف نفسه');
    if (!confirm('حذف حركة العهدة؟')) return;
    await authFetch('/delete-partner-fund-entry/'+id,{method:'DELETE'});
    await loadPartnerFundLog();
  };

  const oldPayload = window.payload;
  window.payload = function(){
    const body = oldPayload();
    const partnerEl = document.getElementById('expense_partner_name');
    const partner = normPartner(partnerEl ? partnerEl.value : '');
    body.expense_partner_name = partner || '';
    return body;
  };
  const oldEditExpense = window.editExpense;
  window.editExpense = function(id){
    oldEditExpense(id);
    const row = (Array.isArray(expensesRows) ? expensesRows.find(x=>Number(x.id)===Number(id)) : null) || null;
    const partnerEl = document.getElementById('expense_partner_name');
    if (partnerEl) partnerEl.value = normPartner(row?.expense_partner_name || '') || '';
  };
  const oldCancelExpenseEdit = window.cancelExpenseEdit;
  window.cancelExpenseEdit = function(){
    oldCancelExpenseEdit();
    const partnerEl = document.getElementById('expense_partner_name');
    if (partnerEl) partnerEl.value = '';
  };
  window.loadExpenses = async function(){
    expensesRows = await authFetch('/get-expenses'+qs());
    if (!expensesBody) return;
    expensesBody.innerHTML = expensesRows.length ? expensesRows.map(r=>`<tr><td>${esc(r.expense_date||'')}</td><td>${esc(Number(r.linked_to_order)===1?fieldLabel(r.order_cost_field):(r.category==='أخرى'?(r.custom_category||'أخرى'):(r.category||'')))}</td><td>${money(r.amount)}</td><td>${esc(normPartner(r.expense_partner_name)||'الخزنة')}</td><td>${Number(r.linked_to_order)===1?'نعم':'لا'}</td><td>${Number(r.linked_to_order)===1?`أوردر #${r.order_id||''}${r.notes?`<br><small>${esc(r.notes)}</small>`:''}`:esc(r.notes||'-')}</td><td><button onclick="editExpense(${r.id})">تعديل</button> <button onclick="delExpense(${r.id})">حذف</button></td></tr>`).join('') : '<tr><td colspan="7">لا يوجد مصاريف</td></tr>';
  };
  const oldReloadAll = window.reloadAll;
  window.reloadAll = function(){
    oldReloadAll();
    loadPartnerFundLog();
  };

  const fundDate = document.getElementById('fund_date');
  if (fundDate) fundDate.value = todayLocal();
  selectFundPartner(PARTNERS[0]);
  cancelPartnerFundEdit();
})();
