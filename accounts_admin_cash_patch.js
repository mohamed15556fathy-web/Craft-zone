(function(){
  if (typeof authFetch !== 'function') return;

  const currentUser = JSON.parse(localStorage.getItem('user') || 'null') || {};
  const canManageExpenses = currentUser.role === 'admin' || Number(currentUser.perm_manage_expenses || 0) === 1;
  const canUseAdminCashOnExpense = currentUser.role === 'admin' || Number(currentUser.perm_use_admin_cash_on_expense || 0) === 1;
  const canManageCurrentCash = currentUser.role === 'admin' || Number(currentUser.perm_manage_current_cash || 0) === 1;
  const canEditExpenseRecords = currentUser.role === 'admin' || Number(currentUser.perm_edit_expense_records || 0) === 1;
  const canDeleteExpenseRecords = currentUser.role === 'admin' || Number(currentUser.perm_delete_expense_records || 0) === 1;
  const canEditCashRecords = currentUser.role === 'admin' || Number(currentUser.perm_edit_cash_records || 0) === 1;
  const canDeleteCashRecords = currentUser.role === 'admin' || Number(currentUser.perm_delete_cash_records || 0) === 1;
  const canEditAdminCashRecords = currentUser.role === 'admin' || Number(currentUser.perm_edit_admin_cash_records || 0) === 1;
  const canDeleteAdminCashRecords = currentUser.role === 'admin' || Number(currentUser.perm_delete_admin_cash_records || 0) === 1;
  let actorUsers = [];
  let actorCurrent = String(currentUser.username || '').trim();
  let canActAsOther = Number(currentUser.perm_act_as_other_admin || 0) === 1;
  const canSelectExpenseActor = () => canUseAdminCashOnExpense && canActAsOther;
  let adminCashPayload = { users: [], rows: [], summary: [] };
  let executionPartnersRows = [];
  let cashAdjustmentRows = [];
  let editingCashAdjustmentId = null;
  let editingAdminCashEntryId = null;

  function byId(id){ return document.getElementById(id); }
  function fmtMoney(v){ return typeof money === 'function' ? money(v) : (Number(v||0).toFixed(2)+' ج'); }
  function escHtml(v){ return typeof esc === 'function' ? esc(v) : String(v ?? ''); }
  function todayLocal(){ return typeof todayStr === 'function' ? todayStr() : new Date().toISOString().slice(0,10); }
  function qsSafe(){ return typeof qs === 'function' ? qs() : ''; }
  function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }
  function parseMoneyFromText(text){ const s=String(text||'').replace(/,/g,'').replace(/[^\d.-]/g,''); const n=Number(s); return Number.isFinite(n)?n:0; }
  function aliasName(name=''){
    const s = String(name || '').trim();
    if (!s) return '';
    if (s.includes('عبدالقادر') || s.includes('عبد القادر') || s.includes('boda') || s.includes('Boda') || s.toLowerCase()==='abdelrahman') return 'بودا';
    if (s.includes('محمد') || s.toLowerCase()==='admin') return 'محمد';
    return s;
  }
  function actorLabel(row){
    if (!row) return '-';
    const username = actorUsername(row);
    const raw = String(row.full_name || row.admin_name || row.actor_name || row.username || row.admin_username || row.actor_username || '').trim();
    const alias = aliasName(raw) || raw || '-';
    if (!username) return alias;
    if (username.toLowerCase()==='admin') return 'محمد (admin)';
    if (username.toLowerCase()==='abdelrahman') return 'بودا (Abdelrahman)';
    return alias === username ? alias : `${alias} (${username})`;
  }
  function actorUsername(row){
    return String(row?.username || row?.admin_username || row?.actor_username || '').trim();
  }
  function adminCashActionLabel(kind=''){
    const k = String(kind || '').trim();
    if (k === 'add') return 'تسليم عهدة';
    if (k === 'sub') return 'استرداد من العهدة';
    if (k === 'set') return 'تعيين رصيد فعلي';
    if (k === 'expense') return 'مصروف من العهدة';
    if (k === 'order_cost') return 'تكلفة أوردر من العهدة';
    if (k === 'transfer_out') return 'تحويل صادر';
    if (k === 'transfer_in') return 'تحويل وارد';
    return k || '-';
  }
  function expenseSourceLabel(row){
    const actor = aliasName(row?.actor_name || row?.actor_username || '');
    return actor || 'الخزنة';
  }
  function oldExpenseSelect(){ return byId('expense_partner_name'); }

  function ensureStyles(){
    if (byId('adminCashPatchStyle')) return;
    const style = document.createElement('style');
    style.id = 'adminCashPatchStyle';
    style.textContent = `
    .admin-cash-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-top:14px}
    .admin-cash-card{background:#0b1220;border:1px solid var(--border);border-radius:16px;padding:14px}
    .admin-cash-card h4{margin:0 0 8px 0}
    .admin-cash-big{font-size:28px;font-weight:800;color:var(--cyan);margin:8px 0 12px}
    .admin-cash-metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
    .admin-cash-metric{background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:12px;padding:10px}
    .admin-cash-metric small{display:block;color:var(--muted);margin-bottom:6px}
    .admin-cash-metric b{font-size:15px}
    .admin-cash-pills{display:flex;gap:10px;flex-wrap:wrap;margin:14px 0}
    .admin-cash-pill{background:#0b1220;border:1px solid var(--border);border-radius:999px;padding:8px 12px;font-size:13px}
    .admin-cash-pill b{color:var(--cyan);margin-inline-start:6px}
    .admin-cash-form-wrap{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;margin-top:14px}
    .admin-cash-panel{background:#0b1220;border:1px solid var(--border);border-radius:16px;padding:14px}
    .admin-cash-panel h4{margin:0 0 10px 0}
    .admin-cash-note{color:var(--muted);font-size:13px;margin-top:8px}
    .admin-cash-actions{display:flex;gap:10px;flex-wrap:wrap;align-items:end}
    .ghost-btn{background:#0b1220;color:#fff;border:1px solid var(--border)}
    `;
    document.head.appendChild(style);
  }

  function setParentSectionVisibility(){
    const parent = byId('partnerSectionWrap');
    const withdrawals = byId('partnerWithdrawSectionWrap');
    const funds = byId('partnerFundSectionWrap');
    if (!parent) return;
    const show = (withdrawals && !withdrawals.classList.contains('hidden')) || (funds && !funds.classList.contains('hidden'));
    parent.classList.toggle('hidden', !show);
  }
  function separatePartnerSections(){
    const parent = byId('partnerSectionWrap');
    if (!parent || byId('partnerWithdrawSectionWrap')) return;
    const children = [...parent.children];
    const withdrawals = document.createElement('div');
    withdrawals.id = 'partnerWithdrawSectionWrap';
    withdrawals.className = 'hidden';
    const funds = document.createElement('div');
    funds.id = 'partnerFundSectionWrap';
    funds.className = 'hidden';
    if (children[0]) withdrawals.appendChild(children[0]);
    if (children[2]) withdrawals.appendChild(children[2]);
    if (children[1]) funds.appendChild(children[1]);
    if (children[3]) funds.appendChild(children[3]);
    parent.innerHTML = '';
    parent.appendChild(withdrawals);
    parent.appendChild(funds);
    parent.classList.add('hidden');
  }
  function ensurePartnerFundButton(){
    const headerActions = document.querySelector('.header > div:last-child');
    if (!headerActions || byId('togglePartnerFundBtn')) return;
    const refBtn = [...headerActions.querySelectorAll('button')].find(btn => /سحوبات الشركاء/.test(btn.textContent || ''));
    const btn = document.createElement('button');
    btn.className = 'btn';
    btn.id = 'togglePartnerFundBtn';
    btn.textContent = '💼 عهدة الشغل';
    btn.onclick = function(){ window.togglePartnerFundSection(); };
    if (refBtn && refBtn.nextSibling) headerActions.insertBefore(btn, refBtn.nextSibling);
    else headerActions.appendChild(btn);
  }
  function overridePartnerToggles(){
    window.togglePartnerLog = function(){
      const wrap = byId('partnerWithdrawSectionWrap');
      if (!wrap) return;
      const open = wrap.classList.contains('hidden');
      wrap.classList.toggle('hidden', !open);
      if (open && typeof loadPartnerWithdrawals === 'function') loadPartnerWithdrawals();
      setParentSectionVisibility();
    };
    window.togglePartnerFundSection = function(){
      const wrap = byId('partnerFundSectionWrap');
      if (!wrap) return;
      const open = wrap.classList.contains('hidden');
      wrap.classList.toggle('hidden', !open);
      if (open && typeof loadPartnerFundLog === 'function') loadPartnerFundLog();
      setParentSectionVisibility();
    };
    window.togglePartnerRecords = function(){
      const wrap = byId('partnerLogWrap');
      const section = byId('partnerWithdrawSectionWrap');
      if (!wrap || !section) return;
      section.classList.remove('hidden');
      wrap.classList.toggle('hidden');
      if (!wrap.classList.contains('hidden') && typeof loadPartnerWithdrawals === 'function') loadPartnerWithdrawals();
      setParentSectionVisibility();
    };
    window.togglePartnerFundLog = function(){
      const wrap = byId('partnerFundLogWrap');
      const section = byId('partnerFundSectionWrap');
      if (!wrap || !section) return;
      section.classList.remove('hidden');
      wrap.classList.toggle('hidden');
      if (!wrap.classList.contains('hidden') && typeof loadPartnerFundLog === 'function') loadPartnerFundLog();
      setParentSectionVisibility();
    };
  }

  async function loadActorUsers(){
    try{
      const data = await authFetch('/active-users-lite?tracked_only=1');
      const rows = Array.isArray(data.users) ? data.users : [];
      const byKey = new Map();
      for (const row of rows) {
        const key = String(actorUsername(row) || '').trim().toLowerCase();
        if (!key || byKey.has(key)) continue;
        byKey.set(key, row);
      }
      actorUsers = [...byKey.values()].sort((a,b)=>{
        const au = String(actorUsername(a)||'').toLowerCase();
        const bu = String(actorUsername(b)||'').toLowerCase();
        const pa = au==='admin' ? 0 : (au==='abdelrahman' ? 1 : 2);
        const pb = bu==='admin' ? 0 : (bu==='abdelrahman' ? 1 : 2);
        if (pa !== pb) return pa - pb;
        return String(actorLabel(a)).localeCompare(String(actorLabel(b)), 'ar');
      });
      actorCurrent = String(data.current_user || actorCurrent || '').trim();
      canActAsOther = Number(data.can_act_as_other || 0) === 1;

  }catch(e){ actorUsers = []; }
  const ensureRows = [
    { username: 'admin', full_name: 'محمد' },
    { username: 'Abdelrahman', full_name: 'بودا' }
  ];
  const existing = new Map(actorUsers.map(r => [String(actorUsername(r) || '').trim().toLowerCase(), r]));
  for (const row of ensureRows) {
    const key = String(row.username || '').trim().toLowerCase();
    if (!key || existing.has(key)) continue;
    actorUsers.push(row);
  }
  actorUsers = actorUsers.sort((a,b)=>{
    const au = String(actorUsername(a)||'').toLowerCase();
    const bu = String(actorUsername(b)||'').toLowerCase();
    const pa = au==='admin' ? 0 : (au==='abdelrahman' ? 1 : 2);
    const pb = bu==='admin' ? 0 : (bu==='abdelrahman' ? 1 : 2);
    if (pa !== pb) return pa - pb;
    return String(actorLabel(a)).localeCompare(String(actorLabel(b)), 'ar');
  });
}
  function allowedActorUsers(selected=''){
    const sel = String(selected || '').trim();
    const selectedRow = actorUsers.find(u => actorUsername(u) === sel);
    if (canActAsOther) return actorUsers.slice();
    const own = actorUsers.filter(u => actorUsername(u) === actorCurrent);
    if (sel && !own.some(u => actorUsername(u) === sel) && selectedRow) own.push(selectedRow);
    return own;
  }
  function actorOptionsHtml({ includeBlank=true, blankLabel='الخزنة / بدون عهدة', selected='' }={}){
    const opts = [];
    if (includeBlank) opts.push(`<option value="">${escHtml(blankLabel)}</option>`);
    for (const row of allowedActorUsers(selected)){
      const username = actorUsername(row);
      if (!username) continue;
      opts.push(`<option value="${escHtml(username)}" ${String(selected)===username?'selected':''}>${escHtml(actorLabel(row))}</option>`);
    }
    return opts.join('');
  }


async function loadExecutionPartnersForAccounts(){
  try {
    executionPartnersRows = await authFetch('/execution-partners');
  } catch (e) {
    executionPartnersRows = [];
  }
}
const orderOpsCache = new Map();
function expenseStepTypeFromField(field=''){
  return ({ cost_print:'print', cost_make:'make', cost_hand_fix:'handle' })[String(field || '').trim()] || '';
}
async function fetchOrderOperationSnapshot(orderId){
  const id = Number(orderId || 0);
  if (!id) return null;
  if (orderOpsCache.has(id)) return orderOpsCache.get(id);
  try {
    const data = await authFetch('/order-operations/' + id);
    orderOpsCache.set(id, data || null);
    return data || null;
  } catch (e) {
    return null;
  }
}
async function syncExecutionPartnerFromOrder(){
  const select = byId('execution_partner_id');
  const wrap = byId('executionPartnerWrap');
  const field = String(byId('order_cost_field')?.value || '').trim();
  const orderId = Number(byId('order_id')?.value || 0);
  if (!select || !wrap || wrap.classList.contains('hidden')) return;
  if (!orderId || !expenseExecStepLabel(field)) {
    select.value = '';
    select.dataset.selected = '';
    select.dataset.selectedSource = '';
    return;
  }
  const data = await fetchOrderOperationSnapshot(orderId);
  const stepType = expenseStepTypeFromField(field);
  const step = Array.isArray(data?.steps) ? data.steps.find(row => String(row.step_type || '').trim() === stepType) : null;
  const targetId = step?.partner_id ? String(step.partner_id) : '';
  if (targetId) {
    select.dataset.selected = targetId;
    select.dataset.selectedSource = 'order';
    select.value = targetId;
  } else {
    const expenseContext = String(select.dataset.expenseContext || '').trim();
    const currentContext = `${orderId}:${field}`;
    if (expenseContext !== currentContext) {
      select.value = '';
      select.dataset.selected = '';
      select.dataset.selectedSource = '';
    }
  }
}
  function expenseExecStepLabel(field=''){
    if (field === 'cost_print') return 'الطباعة';
    if (field === 'cost_make') return 'التصنيع';
    if (field === 'cost_hand_fix') return 'تركيب اليد';
    return '';
  }
  function expenseExecPartnerType(field=''){
    if (field === 'cost_print') return 'مطبعة';
    if (field === 'cost_make') return 'صنايعي';
    if (field === 'cost_hand_fix') return 'تركيب يد';
    return '';
  }
  function expensePartnerMatchesField(row, field=''){
    const need = expenseExecPartnerType(field);
    if (!need) return false;
    const type = String(row?.partner_type || '').trim();
    if (need === 'تركيب يد') return type === 'تركيب يد' || type === 'صنايعي';
    return type === need;
  }
  function ensureExecutionPartnerField(){
    if (byId('executionPartnerWrap')) return;
    const anchor = byId('orderFieldWrap') || byId('categoryWrap');
    if (!anchor || !anchor.parentElement) return;
    const wrap = document.createElement('div');
    wrap.id = 'executionPartnerWrap';
    wrap.className = 'hidden';
    wrap.innerHTML = '<label id="executionPartnerLabel">الجهة المنفذة</label><select id="execution_partner_id"><option value="">اختر الجهة</option></select>';
    anchor.insertAdjacentElement('afterend', wrap);
  }
  async function refreshExecutionPartnerField(){
    ensureExecutionPartnerField();
    const wrap = byId('executionPartnerWrap');
    const select = byId('execution_partner_id');
    const label = byId('executionPartnerLabel');
    if (!wrap || !select) return;
    const linked = String(byId('linked_to_order')?.value || '0') === '1';
    const field = String(byId('order_cost_field')?.value || '').trim();
    const orderId = Number(byId('order_id')?.value || 0);
    const stepLabel = expenseExecStepLabel(field);
    const currentContext = linked && stepLabel ? `${orderId}:${field}` : '';
    const previousContext = String(select.dataset.context || '').trim();
    const expenseContext = String(select.dataset.expenseContext || '').trim();
    const contextChanged = currentContext !== previousContext;
    if (!linked || !stepLabel) {
      wrap.classList.add('hidden');
      select.innerHTML = '<option value="">اختر الجهة</option>';
      select.value = '';
      select.dataset.selected = '';
      select.dataset.selectedSource = '';
      select.dataset.context = currentContext;
      return;
    }
    if (contextChanged && expenseContext !== currentContext) {
      select.value = '';
      select.dataset.selected = '';
      select.dataset.selectedSource = '';
    }
    wrap.classList.remove('hidden');
    select.dataset.context = currentContext;
    if (label) label.textContent = `جهة ${stepLabel}`;
    const current = String(select.dataset.selected || select.value || '').trim();
    const rows = executionPartnersRows.filter(row => Number(row?.is_active ?? 1) === 1 && expensePartnerMatchesField(row, field));
    select.innerHTML = '<option value="">اختر الجهة</option>' + rows.map(row=>`<option value="${Number(row.id||0)}" ${String(Number(row.id||0))===current?'selected':''}>${escHtml(String(row.name||'').trim())} - ${escHtml(String(row.partner_type||'').trim())}</option>`).join('');
    if (current && !rows.some(row => String(Number(row.id||0))===current)) {
      select.value = '';
      select.dataset.selected = '';
      select.dataset.selectedSource = '';
    } else if (current) {
      select.value = current;
    }
    select.dataset.selected = select.value || current || '';
    await syncExecutionPartnerFromOrder();
  }

  function prepareExpenseFundField(){
    const select = oldExpenseSelect();
    if (!select) return;
    const holder = select.closest('div');
    if (!holder) return;
    holder.id = 'expenseFundHolder';
    const label = holder.querySelector('label');
    if (label) label.textContent = 'الخصم من عهدة الشغل';
    if (canSelectExpenseActor()) {
      holder.classList.remove('hidden');
      select.innerHTML = actorOptionsHtml({ includeBlank:true, blankLabel:'الخزنة / بدون عهدة', selected: select.value || '' });
    } else {
      holder.classList.add('hidden');
      select.innerHTML = actorOptionsHtml({ includeBlank:true, blankLabel:'الخزنة / بدون عهدة', selected: '' });
      select.value = '';
    }
    const hint = [...document.querySelectorAll('#expenseFundHolder ~ .hint, #expenseFundHolder + .hint')].find(Boolean);
    if (hint) hint.textContent = canSelectExpenseActor()
      ? 'اختَر محمد أو بودا لو المصروف اتدفع من عهدة الشغل. لو سبتها فاضية يتحسب من الخزنة.'
      : 'الخانة دي بتظهر فقط للي معاه صلاحية الخصم من عهدة الشغل والعمل باسم أدمن آخر.';
  }

  function ensureCashEditButtons(){
    const panel = byId('cashPanel');
    if (!panel) return;
    const actions = panel.querySelector('div.form-grid div[style*="align-items:end"]');
    if (!actions) return;
    const saveBtn = actions.querySelector('button.filled, button');
    if (saveBtn) saveBtn.textContent = editingCashAdjustmentId ? 'حفظ تعديل السيولة' : 'حفظ التعديل';
    if (!byId('cancelCashEditBtn')) {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.id = 'cancelCashEditBtn';
      btn.className = 'ghost-btn hidden';
      btn.textContent = 'إلغاء التعديل';
      btn.onclick = function(){ cancelCashAdjustmentEdit(); };
      actions.insertBefore(btn, actions.children[1] || null);
    }
  }
  function resetCashAdjustmentForm(){
    editingCashAdjustmentId = null;
    if (byId('cash_mode')) byId('cash_mode').value = 'set';
    if (byId('cash_amount')) byId('cash_amount').value = '';
    if (byId('cash_reason')) byId('cash_reason').value = '';
    if (byId('cash_note')) byId('cash_note').value = '';
    if (byId('cash_date')) byId('cash_date').value = todayLocal();
    ensureCashEditButtons();
    const cancel = byId('cancelCashEditBtn');
    if (cancel) cancel.classList.add('hidden');
  }
  function cancelCashAdjustmentEdit(){ resetCashAdjustmentForm(); }
  window.cancelCashAdjustmentEdit = cancelCashAdjustmentEdit;

  function renderCashAdjustmentControls(row){
    if (!canEditCashRecords && !canDeleteCashRecords) return '-';
    const parts=[]; if (canEditCashRecords) parts.push(`<button onclick=\"editCashAdjustmentUi(${Number(row.id||0)})\">تعديل</button>`); if (canDeleteCashRecords) parts.push(`<button onclick=\"deleteCashAdjustmentUi(${Number(row.id||0)})\">حذف</button>`); return parts.length ? parts.join(' ') : '-';
  }
  window.editCashAdjustmentUi = function(id){
    const row = cashAdjustmentRows.find(r => Number(r.id) === Number(id));
    if (!row) return;
    editingCashAdjustmentId = Number(id);
    if (byId('cash_mode')) byId('cash_mode').value = String(row.action_type || 'set');
    if (byId('cash_amount')) byId('cash_amount').value = toNum(row.amount);
    if (byId('cash_reason')) byId('cash_reason').value = row.reason || '';
    if (byId('cash_note')) byId('cash_note').value = row.note || '';
    if (byId('cash_date')) byId('cash_date').value = row.adjustment_date || todayLocal();
    ensureCashEditButtons();
    const cancel = byId('cancelCashEditBtn');
    if (cancel) cancel.classList.remove('hidden');
    byId('cashPanel')?.classList.remove('hidden');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };
  window.deleteCashAdjustmentUi = async function(id){
    if (!confirm('حذف حركة السيولة؟')) return;
    await authFetch('/cash-adjustments/' + Number(id), { method:'DELETE' });
    if (Number(editingCashAdjustmentId) === Number(id)) cancelCashAdjustmentEdit();
    await loadCash();
    await loadCashLog();
    if (byId('adminCashDetailsBox')) await refreshAdminCashDetails();
  };
  window.saveCashAdjustment = async function(){
    if (!canManageCurrentCash || !canEditCashRecords) return alert('غير مسموح لك بتعديل السيولة الحالية');
    const body = {
      mode: byId('cash_mode')?.value || 'set',
      amount: byId('cash_amount')?.value || 0,
      reason: byId('cash_reason')?.value || '',
      note: byId('cash_note')?.value || '',
      adjustment_date: byId('cash_date')?.value || todayLocal()
    };
    if (editingCashAdjustmentId) {
      await authFetch('/cash-adjustments/' + Number(editingCashAdjustmentId), { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    } else {
      await authFetch('/cash-adjustments', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    }
    cancelCashAdjustmentEdit();
    await loadCash();
    await loadCashLog();
    if (byId('adminCashDetailsBox')) await refreshAdminCashDetails();
  };

  window.loadCashLog = async function(){
    cashAdjustmentRows = await authFetch('/cash-adjustments' + qsSafe());
    const tbody = byId('cashBody');
    if (!tbody) return;
    tbody.innerHTML = cashAdjustmentRows.length ? cashAdjustmentRows.map(r=>`<tr><td>${escHtml(r.adjustment_date||'')}</td><td>${escHtml(typeof cashActionLabel === 'function' ? cashActionLabel(r.action_type) : adminCashActionLabel(r.action_type))}</td><td>${fmtMoney(r.amount)}</td><td>${fmtMoney(r.delta)}</td><td>${fmtMoney(r.previous_balance)}</td><td>${fmtMoney(r.new_balance)}</td><td>${escHtml(r.reason||'-')}</td><td>${escHtml(r.note||'-')}</td><td>${escHtml(r.created_by||'-')}</td><td>${renderCashAdjustmentControls(r)}</td></tr>`).join('') : '<tr><td colspan="10">لا توجد تعديلات مسجلة</td></tr>';
    const headRow = tbody.closest('table')?.querySelector('thead tr');
    if (headRow) headRow.innerHTML = '<th>التاريخ</th><th>العملية</th><th>المبلغ</th><th>فرق التعديل</th><th>قبل</th><th>بعد</th><th>السبب</th><th>ملاحظات</th><th>بواسطة</th><th>تحكم</th>';
  };

  function renderExpenseControls(row){
    const parts = [];
    if (canEditExpenseRecords) parts.push(`<button onclick="editExpense(${Number(row.id||0)})">تعديل</button>`);
    if (canDeleteExpenseRecords) parts.push(`<button onclick="delExpense(${Number(row.id||0)})">حذف</button>`);
    return parts.length ? parts.join(' ') : '-';
  }
  function renderCostControls(row){
    const linkedId = Number(row.linked_expense_id || 0);
    if (!linkedId) return '<span class="hint">من الأوردر</span>';
    const parts = [];
    if (canEditExpenseRecords) parts.push(`<button onclick="editExpense(${linkedId})">تعديل</button>`);
    if (canDeleteExpenseRecords) parts.push(`<button onclick="delExpense(${linkedId})">حذف</button>`);
    return parts.length ? parts.join(' ') : '<span class="hint">مقيد بالصلاحيات</span>';
  }
  function renderPurchaseControls(row){
    return `<a href="purchases.html" class="btn">تعديل</a> <button onclick="deletePurchaseFromAccounts(${Number(row.id||0)})">حذف</button>`;
  }
  window.deletePurchaseFromAccounts = async function(id){
    if (!confirm('حذف فاتورة المشتريات؟')) return;
    await authFetch('/delete-purchase/' + Number(id), { method:'DELETE' });
    await loadPurchases();
    await loadCash();
    if (typeof loadSummary === 'function') await loadSummary();
  };

  function overrideExpenseFunctions(){
    const oldPayload = window.payload;
    window.payload = function(){
      const body = typeof oldPayload === 'function' ? oldPayload() : {};
      const fundValue = oldExpenseSelect()?.value || '';
      body.expense_partner_name = '';
      body.actor_username = canSelectExpenseActor() ? fundValue : '';
      const execField = String(byId('order_cost_field')?.value || '').trim();
      const execSel = byId('execution_partner_id');
      body.execution_partner_id = (String(body.linked_to_order || '0') === '1' && expenseExecStepLabel(execField)) ? (execSel?.value || '') : '';
      return body;
    };
    const oldEditExpense = window.editExpense;
    window.editExpense = function(id){
      if (typeof oldEditExpense === 'function') oldEditExpense(id);
      const row = Array.isArray(expensesRows) ? expensesRows.find(r => Number(r.id) === Number(id)) : null;
      const select = oldExpenseSelect();
      if (select) select.value = row?.actor_username || '';
      const execSel = byId('execution_partner_id');
      if (execSel) {
        execSel.dataset.selected = row?.execution_partner_id ? String(row.execution_partner_id) : '';
        execSel.dataset.selectedSource = row?.execution_partner_id ? 'expense' : '';
        execSel.dataset.expenseContext = row && Number(row.linked_to_order) === 1 ? `${Number(row.order_id || 0)}:${String(row.order_cost_field || '').trim()}` : '';
      }
      prepareExpenseFundField();
      refreshExecutionPartnerField();
    };
    const oldCancelExpenseEdit = window.cancelExpenseEdit;
    window.cancelExpenseEdit = function(){
      if (typeof oldCancelExpenseEdit === 'function') oldCancelExpenseEdit();
      const select = oldExpenseSelect();
      if (select) select.value = '';
      const execSel = byId('execution_partner_id');
      if (execSel) { execSel.value = ''; execSel.dataset.selected = ''; execSel.dataset.selectedSource=''; execSel.dataset.expenseContext=''; execSel.dataset.context=''; }
      prepareExpenseFundField();
      refreshExecutionPartnerField();
    };
    window.saveExpense = async function(){
      try {
        const body = window.payload();
        if (editingExpenseId) {
          await authFetch('/update-expense/' + editingExpenseId, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
        } else {
          await authFetch('/save-expense', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
        }
        if (typeof window.cancelExpenseEdit === 'function') window.cancelExpenseEdit();
        if (typeof window.reloadAll === 'function') window.reloadAll();
      } catch (e) { alert(e.message || 'تعذر حفظ المصروف'); }
    };
    window.loadExpenses = async function(){
      expensesRows = await authFetch('/get-expenses' + qsSafe());
      const tbody = byId('expensesBody');
      if (!tbody) return;
      tbody.innerHTML = expensesRows.length ? expensesRows.map(r=>`<tr><td>${escHtml(r.expense_date||'')}</td><td>${escHtml(Number(r.linked_to_order)===1 ? fieldLabel(r.order_cost_field) : (r.category==='أخرى' ? (r.custom_category||'أخرى') : (r.category||'')))}</td><td>${fmtMoney(r.amount)}</td><td>${escHtml(expenseSourceLabel(r))}</td><td>${Number(r.linked_to_order)===1?'نعم':'لا'}</td><td>${Number(r.linked_to_order)===1 ? `أوردر #${r.order_id||''}${r.execution_partner_name?`<br><small>الجهة: ${escHtml(r.execution_partner_name)}${r.execution_partner_type?` - ${escHtml(r.execution_partner_type)}`:''}</small>`:''}${r.notes?`<br><small>${escHtml(r.notes)}</small>`:''}` : escHtml(r.notes||'-')}</td><td>${renderExpenseControls(r)}</td></tr>`).join('') : '<tr><td colspan="7">لا يوجد مصاريف</td></tr>';
      const headRow = tbody.closest('table')?.querySelector('thead tr');
      if (headRow) headRow.innerHTML = '<th>التاريخ</th><th>النوع</th><th>المبلغ</th><th>من عهدة</th><th>مربوط بأوردر</th><th>التفاصيل</th><th>تحكم</th>';
    };
  }

  window.loadCosts = async function(){
    const rows = await authFetch('/get-cost-logs' + qsSafe());
    window.costsRows = rows;
    const tbody = byId('costsBody');
    if (!tbody) return;
    tbody.innerHTML = rows.length ? rows.map(r=>`<tr><td>${escHtml(r.expense_date||r.sale_date||'')}</td><td>#${r.order_id||''}</td><td>${escHtml(typeof fieldLabel === 'function' ? fieldLabel(r.order_cost_field) : (r.order_cost_field||''))}</td><td>${fmtMoney(r.amount)}</td><td>${escHtml((typeof noteLabel==='function' ? noteLabel(r.notes, r.order_cost_field) : r.notes) || '-')}</td><td>${renderCostControls(r)}</td></tr>`).join('') : '<tr><td colspan="6">لا يوجد تكاليف</td></tr>';
    const headRow = tbody.closest('table')?.querySelector('thead tr');
    if (headRow) headRow.innerHTML = '<th>التاريخ</th><th>الأوردر</th><th>البند</th><th>المبلغ</th><th>ملاحظات</th><th>تحكم</th>';
  };

  window.loadPurchases = async function(){
    purchasesRows = await authFetch('/accounts-purchases' + qsSafe());
    const tbody = byId('purchasesBody');
    if (!tbody) return;
    tbody.innerHTML = purchasesRows.length ? purchasesRows.map(r=>`<tr><td>${escHtml(r.purchase_date||'')}</td><td>${escHtml(r.supplier_name||'-')}</td><td>${escHtml(r.item_name||'-')}<br><small>${escHtml(r.item_type||'')} | ${r.quantity||0} ${escHtml(r.unit||'')}${Number(r.paper_grammage||0)>0?` | ${r.paper_grammage} جم`:''}</small></td><td>${fmtMoney(r.total_price)}</td><td>${fmtMoney(r.paid_amount)}</td><td>${fmtMoney(r.remaining_amount)}</td><td>${Number(r.stock_applied)===1?(String(r.stock_mode||'existing')==='new'?'صنف جديد':'صنف موجود'):'-'}</td><td>${renderPurchaseControls(r)}</td></tr>`).join('') : '<tr><td colspan="8">لا توجد مشتريات مسجلة</td></tr>';
    const headRow = tbody.closest('table')?.querySelector('thead tr');
    if (headRow) headRow.innerHTML = '<th>التاريخ</th><th>المورد</th><th>الصنف</th><th>الإجمالي</th><th>المدفوع</th><th>المتبقي</th><th>المخزن</th><th>تحكم</th>';
  };

  async function refreshAdminCashDetails(){
    const box = byId('adminCashDetailsBox');
    if (!box) return;
    try {
      adminCashPayload = await authFetch('/admin-cash-log' + qsSafe());
    } catch (e) {
      box.innerHTML = `<div class="hint">${escHtml(e.message || 'تعذر تحميل عهدة الشغل')}</div>`;
      return;
    }
    const summary = Array.isArray(adminCashPayload.summary) ? adminCashPayload.summary : [];
    const rows = Array.isArray(adminCashPayload.rows) ? adminCashPayload.rows : [];
    const totalAdminBalance = summary.reduce((sum,row)=>sum + toNum(row.current_balance), 0);
    const globalCash = typeof cashSummary === 'object' && cashSummary ? toNum(cashSummary.currentCash) : parseMoneyFromText(byId('cashCurrent')?.textContent);
    const drawerCash = globalCash - totalAdminBalance;
    const cardsHtml = summary.length ? summary.map(row=>{
      const last = row.last_entry;
      const lastText = last ? `${escHtml(last.entry_date||'-')} | ${escHtml(adminCashActionLabel(last.entry_kind))} | ${fmtMoney(last.delta||0)}` : '-';
      return `<div class="admin-cash-card"><h4>${escHtml(actorLabel(row))}</h4><div class="admin-cash-big">${fmtMoney(row.current_balance)}</div><div class="admin-cash-metrics"><div class="admin-cash-metric"><small>إجمالي المستلم</small><b>${fmtMoney(row.received)}</b></div><div class="admin-cash-metric"><small>إجمالي المصروف/الخصم</small><b>${fmtMoney(row.spent)}</b></div><div class="admin-cash-metric"><small>خصم اليوم</small><b>${fmtMoney(row.today_spent)}</b></div><div class="admin-cash-metric"><small>آخر حركة</small><b>${lastText}</b></div></div></div>`;
    }).join('') : '<div class="hint">لا توجد أرصدة عهدة شغل حتى الآن.</div>';

    box.innerHTML = `
      <div class="admin-cash-pills">
        <span class="admin-cash-pill">السيولة العامة بالخزنة / حساب الشركة<b>${fmtMoney(globalCash)}</b></span>
        <span class="admin-cash-pill">إجمالي العهد مع محمد وبودا<b>${fmtMoney(totalAdminBalance)}</b></span>
        <span class="admin-cash-pill">المتبقي بالخزنة/الشركة<b>${fmtMoney(drawerCash)}</b></span>
      </div>
      <div class="admin-cash-grid">${cardsHtml}</div>
      <h4 style="margin:14px 0 0 0">تقرير مستقل لحساب محمد وبودا</h4>
      ${canEditAdminCashRecords ? `
      <div class="admin-cash-form-wrap">
        <div class="admin-cash-panel">
          <h4>حركة عهدة الشغل</h4>
          <div class="form-grid">
            <div><label>الاسم</label><select id="admin_cash_admin"></select></div>
            <div><label>العملية</label><select id="admin_cash_mode"><option value="add">تسليم عهدة</option><option value="sub">استرداد من العهدة</option><option value="set">تعيين الرصيد الفعلي</option></select></div>
            <div><label>المبلغ</label><input id="admin_cash_amount" type="number" placeholder="اكتب المبلغ"></div>
            <div><label>التاريخ</label><input id="admin_cash_date" type="date"></div>
            <div style="grid-column:1/-1"><label>ملاحظات</label><textarea id="admin_cash_note" rows="2" placeholder="مثال: تسليم عهدة شراء / تسوية رصيد"></textarea></div>
            <div class="admin-cash-actions"><button class="filled" onclick="saveAdminCashEntryUi()">${editingAdminCashEntryId ? 'حفظ التعديل' : 'حفظ الحركة'}</button><button type="button" id="cancelAdminCashEditBtn" class="ghost-btn ${editingAdminCashEntryId ? '' : 'hidden'}" onclick="cancelAdminCashEntryEdit()">إلغاء التعديل</button></div>
          </div>
          <div class="admin-cash-note">هذه عهدة شغل فقط، ومختلفة تمامًا عن سحوبات الشركاء الشخصية.</div>
        </div>
        ${(canEditAdminCashRecords && canActAsOther) ? `<div class="admin-cash-panel">
          <h4>تحويل عهدة بين محمد وبودا</h4>
          <div class="form-grid">
            <div><label>من</label><select id="admin_transfer_from"></select></div>
            <div><label>إلى</label><select id="admin_transfer_to"></select></div>
            <div><label>المبلغ</label><input id="admin_transfer_amount" type="number" placeholder="اكتب المبلغ"></div>
            <div><label>التاريخ</label><input id="admin_transfer_date" type="date"></div>
            <div style="grid-column:1/-1"><label>ملاحظات</label><textarea id="admin_transfer_note" rows="2" placeholder="مثال: تحويل عهدة شراء أو تسليم شغل"></textarea></div>
            <div class="admin-cash-actions"><button class="filled" onclick="saveAdminCashTransferUi()">حفظ التحويل</button></div>
          </div>
          <div class="admin-cash-note">أي تحويل بينكم بيتسجل للطرفين ويظهر في تفاصيل السيولة.</div>
        </div>` : `<div class="admin-cash-panel"><h4>تحويل عهدة بين الأدمنين</h4><div class="hint">التحويل بين اسمين ظاهر فقط للي معه صلاحية العمل باسم أدمن آخر.</div></div>`}
      </div>` : '<div class="hint" style="margin-top:14px">إدارة عهدة الشغل متاحة فقط للي معاه صلاحياتها المستقلة.</div>'}
      <div class="panel" style="padding:14px;margin-top:16px;background:#0b1220">
        <h4 style="margin:0 0 10px 0">سجل عهدة الشغل</h4>
        <div style="overflow:auto"><table><thead><tr><th>التاريخ</th><th>الاسم</th><th>العملية</th><th>المبلغ</th><th>التغيير</th><th>الرصيد بعد</th><th>مرتبط</th><th>ملاحظات</th><th>بواسطة</th><th>تحكم</th></tr></thead><tbody>${rows.length ? rows.map(r=>`<tr><td>${escHtml(r.entry_date||'')}</td><td>${escHtml(actorLabel(r))}</td><td>${escHtml(adminCashActionLabel(r.entry_kind))}</td><td>${fmtMoney(r.amount||0)}</td><td>${fmtMoney(r.delta||0)}</td><td>${fmtMoney(r.balance_after||0)}</td><td>${escHtml(r.related_admin_name ? aliasName(r.related_admin_name) : (r.related_admin_username || (r.source_type==='expense' ? 'مصروف' : (r.source_type==='order_operation' ? 'عملية أوردر' : (r.source_type==='admin_transfer' ? 'تحويل عهدة' : '-')))))}</td><td>${escHtml(r.note||'-')}</td><td>${escHtml(r.created_by||'-')}</td><td>${renderAdminCashEntryControls(r)}</td></tr>`).join('') : '<tr><td colspan="10">لا توجد حركات عهدة لهذا العرض</td></tr>'}</tbody></table></div>
      </div>`;

    const entrySel = byId('admin_cash_admin');
    const fromSel = byId('admin_transfer_from');
    const toSel = byId('admin_transfer_to');
    if (entrySel) {
      entrySel.innerHTML = actorOptionsHtml({ includeBlank:false, selected: actorCurrent });
      if (!canActAsOther) entrySel.value = actorCurrent || entrySel.value;
    }
    if (fromSel) {
      fromSel.innerHTML = actorOptionsHtml({ includeBlank:false, selected: actorCurrent });
      if (!canActAsOther) fromSel.value = actorCurrent || fromSel.value;
    }
    if (toSel) {
      const list = actorUsers.filter(u => actorUsername(u) && actorUsername(u) !== (fromSel?.value || actorCurrent));
      toSel.innerHTML = list.map(u=>`<option value="${escHtml(actorUsername(u))}">${escHtml(actorLabel(u))}</option>`).join('');
    }
    if (byId('admin_cash_date')) byId('admin_cash_date').value = byId('admin_cash_date').value || todayLocal();
    if (byId('admin_transfer_date')) byId('admin_transfer_date').value = byId('admin_transfer_date').value || todayLocal();
  }

  function renderAdminCashEntryControls(row){
    const editable = !['expense','order_operation','admin_transfer'].includes(String(row.source_type || '').trim()) && Number(row.id || 0) > 0;
    if (!editable) return '<span class="hint">تلقائي</span>';
    const parts = [];
    if (canEditAdminCashRecords) parts.push(`<button onclick="editAdminCashEntryUi(${Number(row.id||0)})">تعديل</button>`);
    if (canDeleteAdminCashRecords) parts.push(`<button onclick="deleteAdminCashEntryUi(${Number(row.id||0)})">حذف</button>`);
    return parts.length ? parts.join(' ') : '-';
  }
  window.editAdminCashEntryUi = function(id){
    const rows = Array.isArray(adminCashPayload.rows) ? adminCashPayload.rows : [];
    const row = rows.find(r => Number(r.id) === Number(id));
    if (!row) return;
    editingAdminCashEntryId = Number(id);
    refreshAdminCashDetails().then(()=>{
      if (byId('admin_cash_admin')) byId('admin_cash_admin').value = row.admin_username || actorCurrent;
      if (byId('admin_cash_mode')) byId('admin_cash_mode').value = String(row.entry_kind || 'add');
      if (byId('admin_cash_amount')) byId('admin_cash_amount').value = toNum(row.amount || Math.abs(row.delta || 0));
      if (byId('admin_cash_date')) byId('admin_cash_date').value = row.entry_date || todayLocal();
      if (byId('admin_cash_note')) byId('admin_cash_note').value = row.note || '';
      const cancel = byId('cancelAdminCashEditBtn');
      if (cancel) cancel.classList.remove('hidden');
      window.scrollTo({ top: byId('detailsPanel')?.offsetTop || 0, behavior: 'smooth' });
    });
  };
  window.cancelAdminCashEntryEdit = function(){
    editingAdminCashEntryId = null;
    refreshAdminCashDetails();
  };
  window.deleteAdminCashEntryUi = async function(id){
    if (!confirm('حذف حركة عهدة الشغل؟')) return;
    await authFetch('/delete-admin-cash-entry/' + Number(id), { method:'DELETE' });
    if (Number(editingAdminCashEntryId) === Number(id)) editingAdminCashEntryId = null;
    await loadCash();
    await refreshAdminCashDetails();
  };
  window.saveAdminCashEntryUi = async function(){
    try {
      const body = {
        admin_username: byId('admin_cash_admin')?.value || actorCurrent,
        entry_mode: byId('admin_cash_mode')?.value || 'add',
        amount: byId('admin_cash_amount')?.value || 0,
        entry_date: byId('admin_cash_date')?.value || todayLocal(),
        note: byId('admin_cash_note')?.value?.trim() || ''
      };
      if (!body.admin_username) return alert('اختار الاسم');
      if (toNum(body.amount) <= 0) return alert('اكتب مبلغ صحيح');
      if (editingAdminCashEntryId) {
        await authFetch('/update-admin-cash-entry/' + Number(editingAdminCashEntryId), { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
      } else {
        await authFetch('/save-admin-cash-entry', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
      }
      editingAdminCashEntryId = null;
      await loadCash();
      await refreshAdminCashDetails();
    } catch (e) { alert(e.message || 'تعذر حفظ حركة العهدة'); }
  };
  window.saveAdminCashTransferUi = async function(){
    try {
      const body = {
        from_admin_username: byId('admin_transfer_from')?.value || '',
        to_admin_username: byId('admin_transfer_to')?.value || '',
        amount: byId('admin_transfer_amount')?.value || 0,
        entry_date: byId('admin_transfer_date')?.value || todayLocal(),
        note: byId('admin_transfer_note')?.value?.trim() || ''
      };
      if (!body.from_admin_username || !body.to_admin_username) return alert('اختار من وإلى');
      if (body.from_admin_username === body.to_admin_username) return alert('اختار اسمين مختلفين');
      if (toNum(body.amount) <= 0) return alert('اكتب مبلغ صحيح');
      await authFetch('/transfer-admin-cash', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
      if (byId('admin_transfer_amount')) byId('admin_transfer_amount').value = '';
      if (byId('admin_transfer_note')) byId('admin_transfer_note').value = '';
      await loadCash();
      await refreshAdminCashDetails();
    } catch (e) { alert(e.message || 'تعذر حفظ التحويل'); }
  };

  function overrideCashDetails(){
    const oldShowDetails = window.showDetails;
    if (typeof oldShowDetails !== 'function') return;
    window.showDetails = async function(type){

await oldShowDetails(type);
if (byId('detailsPanel')?.classList.contains('hidden')) return;
const detailsContent = byId('detailsContent');
if (!detailsContent) return;
if (type === 'remaining') {
  try {
    const [orderSummary, orderRows, manualSummary, manualRows] = await Promise.all([
      authFetch('/receivables-summary').catch(()=>({})),
      authFetch('/receivables').catch(()=>[]),
      authFetch('/manual-receivables-summary').catch(()=>({})),
      authFetch('/manual-receivables').catch(()=>[])
    ]);
    const totalRemain = Number(orderSummary?.remaining_amount || 0) + Number(manualSummary?.remaining_amount || 0);
    byId('detailsTitle').textContent = 'المتبقي لينا برا';
    byId('detailsNote').textContent = `إجمالي المتبقي الحالي: ${fmtMoney(totalRemain)}`;
    detailsContent.innerHTML = `
      <div class="admin-cash-pills">
        <div class="admin-cash-pill">من الأوردرات <b>${fmtMoney(orderSummary?.remaining_amount || 0)}</b></div>
        <div class="admin-cash-pill">فلوس لينا خارج الشغل <b>${fmtMoney(manualSummary?.remaining_amount || 0)}</b></div>
      </div>
      <div style="overflow:auto"><table><thead><tr><th>النوع</th><th>الرقم/الاسم</th><th>التاريخ</th><th>الإجمالي</th><th>المتحصل</th><th>المتبقي</th><th>الحالة/النوع</th></tr></thead><tbody>
        ${[
          ...orderRows.map(r=>`<tr><td>أوردر</td><td>#${r.id||''} - ${escHtml(r.custName||'-')}</td><td>${escHtml(r.orderDate||'-')}</td><td>${fmtMoney(r.total_price||0)}</td><td>${fmtMoney(r.paid_amount||0)}</td><td>${fmtMoney(r.remaining_amount||0)}</td><td>${escHtml(r.status||'-')}</td></tr>`),
          ...manualRows.map(r=>`<tr><td>فلوس لينا</td><td>${escHtml(r.debtor_name||'-')}</td><td>${escHtml(r.due_date||'-')}</td><td>${fmtMoney(r.total_amount||0)}</td><td>${fmtMoney(r.paid_amount||0)}</td><td>${fmtMoney(r.remaining_amount||0)}</td><td>${escHtml(r.receivable_type||'-')}</td></tr>`)
        ].join('') || '<tr><td colspan="7">لا توجد مستحقات حالية</td></tr>'}
      </tbody></table></div>`;
  } catch (e) {}
  return;
}
if (type !== 'cash') return;
let box = byId('adminCashDetailsBox');

      if (!box) {
        box = document.createElement('div');
        box.id = 'adminCashDetailsBox';
        box.style.marginTop = '16px';
        detailsContent.insertAdjacentElement('beforeend', box);
      }
      if (!canManageCurrentCash) byId('cashPanel')?.classList.add('hidden');
      await refreshAdminCashDetails();
    };
  }

  function hideCashEditingForNonOwners(){
    if (canManageCurrentCash) return;
    byId('cashPanel')?.classList.add('hidden');
  }

  function patchDetailsTables(){
    const oldShowDetails = window.showDetails;
    if (typeof oldShowDetails !== 'function') return;
  }

  async function init(){
    ensureStyles();
    separatePartnerSections();
    ensurePartnerFundButton();
    overridePartnerToggles();
    await loadActorUsers();
    await loadExecutionPartnersForAccounts();
    prepareExpenseFundField();
    ensureExecutionPartnerField();
    byId('linked_to_order')?.addEventListener('change', refreshExecutionPartnerField);
    byId('order_cost_field')?.addEventListener('change', refreshExecutionPartnerField);
    byId('order_id')?.addEventListener('change', refreshExecutionPartnerField);
    byId('order_id')?.addEventListener('blur', refreshExecutionPartnerField);
    overrideExpenseFunctions();
    overrideCashDetails();
    ensureCashEditButtons();
    hideCashEditingForNonOwners();
    if (typeof window.cancelExpenseEdit === 'function') window.cancelExpenseEdit();
    refreshExecutionPartnerField();
    resetCashAdjustmentForm();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
