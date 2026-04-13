
const token = localStorage.getItem('token');
const user = JSON.parse(localStorage.getItem('user') || 'null');
if(!token || !user) location.href = 'login.html';
const canManagePurchases = user.role === 'admin' || Number(user.perm_purchases || 0) === 1;
const canManageSuppliers = user.role === 'admin' || Number(user.perm_suppliers || 0) === 1 || canManagePurchases;
if(!canManagePurchases && !canManageSuppliers) location.href = 'index.html';

let suppliers = [], purchases = [], stockCache = { paper: [], handle: [], bag: [] }, currentPurchaseId = null;
let invoiceItems = [], editingInvoiceItemIndex = -1;

function authFetch(url, opts = {}){ opts.headers = Object.assign({}, opts.headers || {}, { Authorization:'Bearer ' + token }); return fetch(url, opts).then(async r => { const d = await r.json().catch(() => ({})); if(r.status===401){ localStorage.clear(); location.href='login.html'; throw new Error('unauthorized'); } if(!r.ok) throw new Error(d.error || 'error'); return d; }); }
function money(v){ return Number(v || 0).toLocaleString('en-US', { maximumFractionDigits:2 }) + ' ج'; }
function esc(s){ return String(s || '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
function today(){ return new Date().toISOString().slice(0,10); }
function goToSuppliers(){ document.getElementById('suppliersSection')?.scrollIntoView({ behavior:'smooth', block:'start' }); }
function goToPurchases(){ document.getElementById('purchaseSection')?.scrollIntoView({ behavior:'smooth', block:'start' }); }
function round2(v){ return Math.round((Number(v) || 0) * 100) / 100; }

const PURCHASE_UNITS = { 'ورق':['كجم','فرخ'], 'يد':['عدد'], 'شنط':['شنطة'], 'طباعة':['عملية'], 'نقل':['نقلة'], 'خامة أخرى':['وحدة'] };

function setPurchaseUnitOptions(type, selected=''){
  const units = PURCHASE_UNITS[type] || ['وحدة'];
  p_unit.innerHTML = units.map(u => `<option value="${u}" ${String(selected||'')===String(u)?'selected':''}>${u}</option>`).join('');
  if(!selected || !units.includes(selected)) p_unit.value = units[0];
}

function syncTotals(){
  const qty = Number(p_qty.value) || 0;
  const unit = Number(p_unit_price.value) || 0;
  if(!p_item_total.dataset.manual) p_item_total.value = (qty * unit) || '';
}
function refreshInvoiceTotals(){
  const total = round2(invoiceItems.reduce((sum, item) => sum + Number(item.total_price || 0), 0));
  p_total.value = total ? total : 0;
  syncPaid();
}
function syncPaid(){
  const total = Number(p_total.value) || 0;
  let paid = Number(p_paid.value) || 0;
  if(paid > total) paid = total;
  if(paid < 0) paid = 0;
  p_paid.value = paid;
  p_remaining.value = round2(Math.max(total - paid, 0));
}
p_item_total.addEventListener('input', () => { p_item_total.dataset.manual = '1'; });

function makePaperName(){
  const grams = Number(p_paper_grammage.value) || 0;
  const len = Number(p_paper_length.value) || 0;
  const wid = Number(p_paper_width.value) || 0;
  const color = String(p_paper_color.value || '').trim();
  let txt = 'ورق';
  if(color) txt += ' ' + color;
  if(len && wid) txt += ` ${len}×${wid}`;
  if(grams) txt += ` - ${grams} جم`;
  return txt.trim();
}
function makeHandleName(){
  const color = String(p_handle_color.value || '').trim();
  return (`يد${color ? ' ' + color : ''}`).trim();
}
function syncDerivedPurchaseName(){
  if(p_type.value === 'ورق') p_name.value = makePaperName();
  else if(p_type.value === 'يد') p_name.value = makeHandleName();
}
function syncHandleQtyToMain(){ p_qty.value = p_qty_handle.value; syncTotals(); }
function syncMainQtyToHandle(){ if(p_type.value === 'يد') p_qty_handle.value = p_qty.value; }

function clearCurrentPurchaseItem(){
  p_type.value = 'ورق';
  setPurchaseUnitOptions('ورق');
  p_name.value = '';
  p_qty.value = '';
  p_qty_handle.value = '';
  p_unit_price.value = '';
  p_item_total.value = '';
  delete p_item_total.dataset.manual;
  p_stock_type.value = 'paper';
  p_stock_mode.value = 'existing';
  p_stock_ref.innerHTML = '<option value="">اختر</option>';
  p_paper_grammage.value = '';
  p_paper_color.value = '';
  p_paper_length.value = '';
  p_paper_width.value = '';
  p_handle_color.value = '';
  editingInvoiceItemIndex = -1;
  refreshPurchaseTypeUI();
}

function resetPurchaseForm(){
  p_date.value = today();
  p_supplier.value = '';
  clearCurrentPurchaseItem();
  invoiceItems = [];
  p_total.value = 0;
  p_paid.value = '0';
  p_remaining.value = '0';
  p_due.value = '';
  p_notes.value = '';
  renderInvoiceItems();
  refreshInvoiceTotals();
}

function renderSupplierSelect(){ p_supplier.innerHTML = '<option value="">اختر المورد</option>' + suppliers.filter(s => Number(s.is_active)!==0).map(s => `<option value="${s.id}">${esc(s.name)} - ${esc(s.supplier_type || '')}</option>`).join(''); }

async function loadStockOptions(selectedValue=''){
  const t = p_stock_type.value;
  const current = String(selectedValue || p_stock_ref.value || '');
  p_stock_ref.innerHTML = '<option value="">اختر</option>';
  if(!t) return;
  const rows = stockCache[t] || [];
  const label = (r) => {
    if(t === 'paper') return `${r.paper_name || ''} ${r.color || ''} ${r.length}×${r.width} - ${r.grammage} جم`;
    if(t === 'handle') return `${r.color || 'بدون تحديد لون'}${Number(r.qty||0) ? ` | رصيد ${r.qty}` : ''}`;
    return `${r.color || ''} - ${r.handle || ''} (${r.length}×${r.width}×${r.gusset})`;
  };
  p_stock_ref.innerHTML = '<option value="">اختر</option>' + rows.map(r => `<option value="${r.id}" ${String(r.id)===current?'selected':''}>${esc(label(r))}</option>`).join('');
  if(current) p_stock_ref.value = current;
}

function fillFromStock(){
  const t = p_stock_type.value, id = Number(p_stock_ref.value) || 0;
  if(!t || !id) return;
  const row = (stockCache[t] || []).find(r => Number(r.id) === id);
  if(!row) return;
  if(t === 'paper'){
    p_type.value = 'ورق';
    setPurchaseUnitOptions('ورق', p_unit.value || 'كجم');
    p_paper_grammage.value = row.grammage || '';
    p_paper_color.value = row.color || '';
    p_paper_length.value = row.length || '';
    p_paper_width.value = row.width || '';
    syncDerivedPurchaseName();
  } else if(t === 'handle'){
    p_type.value = 'يد';
    setPurchaseUnitOptions('يد');
    p_handle_color.value = row.color || '';
    syncDerivedPurchaseName();
  } else if(t === 'bag'){
    p_type.value = 'شنط';
    setPurchaseUnitOptions('شنط');
    p_name.value = `${row.color || ''} - ${row.handle || ''} (${row.length}×${row.width}×${row.gusset})`.trim();
  }
  refreshPurchaseTypeUI();
  loadStockOptions(String(id));
  p_stock_ref.value = String(id);
}

function refreshPurchaseTypeUI(){
  const type = p_type.value;
  setPurchaseUnitOptions(type, p_unit.value);
  const isPaper = type === 'ورق';
  const isHandle = type === 'يد';
  const isBag = type === 'شنط';
  paperFields.classList.toggle('hidden', !isPaper);
  handleFields.classList.toggle('hidden', !isHandle);
  itemNameWrap.classList.toggle('hidden', isPaper || isHandle);
  mainQtyWrap.classList.toggle('hidden', isHandle);
  if(isPaper) syncDerivedPurchaseName();
  if(isHandle){ p_qty_handle.value = p_qty.value; syncDerivedPurchaseName(); }
  if(['ورق','يد','شنط'].includes(type)) p_stock_type.value = isPaper ? 'paper' : (isHandle ? 'handle' : 'bag');
  if(!['ورق','يد','شنط'].includes(type)) p_stock_type.value = '';
  toggleStockSection();
  syncTotals();
}

function toggleStockSection(){
  const supportsStock = ['ورق','يد','شنط'].includes(p_type.value);
  if(supportsStock && !p_stock_type.value) p_stock_type.value = p_type.value==='ورق' ? 'paper' : (p_type.value==='يد' ? 'handle' : 'bag');
  if(!supportsStock) p_stock_type.value = '';
  const activeType = p_stock_type.value;
  const selectedRef = p_stock_ref.value;
  stockModeWrap.classList.toggle('hidden', !activeType);
  const allowNew = activeType === 'paper' || activeType === 'handle';
  if(!allowNew) p_stock_mode.value = 'existing';
  stockRefWrap.classList.toggle('hidden', !activeType || (allowNew && p_stock_mode.value === 'new'));
  stockNewWrap.classList.toggle('hidden', !activeType || !(allowNew && p_stock_mode.value === 'new'));
  if(activeType) loadStockOptions(selectedRef);
}

function collectCurrentPurchaseItem(){
  const itemType = String(p_type.value || '').trim();
  if(itemType === 'ورق' && !(Number(p_paper_grammage.value) || 0)) throw new Error('حدد جرام الورق');
  if(itemType === 'يد') syncHandleQtyToMain();
  const quantity = Number(p_qty.value) || 0;
  if(quantity <= 0) throw new Error('حدد الكمية');
  syncDerivedPurchaseName();
  const itemName = String(p_name.value || '').trim();
  if(!itemName) throw new Error('اسم الصنف مطلوب');
  const totalPrice = round2(Number(p_item_total.value) || 0);
  if(totalPrice <= 0) throw new Error('إجمالي الصنف الحالي مطلوب');
  const stockType = String(p_stock_type.value || '').trim();
  const stockMode = String(p_stock_mode.value || '').trim() || 'existing';
  const stockRefId = stockType && stockMode === 'existing' ? String(p_stock_ref.value || '').trim() : '';
  if(stockType && stockMode === 'existing' && !stockRefId) throw new Error('اختر الصنف الموجود بالمخزن أو غيّر الوضع إلى صنف جديد');
  let stockLabel = 'بدون ربط';
  if(stockType) stockLabel = `${stockMode === 'new' ? 'صنف جديد' : 'صنف موجود'}${stockType === 'paper' ? ' | ورق' : stockType === 'handle' ? ' | يد' : ' | شنط'}`;
  return {
    item_type: itemType,
    item_name: itemName,
    quantity,
    unit: String(p_unit.value || '').trim() || 'وحدة',
    unit_price: round2(Number(p_unit_price.value) || 0),
    total_price: totalPrice,
    stock_type: stockType,
    stock_mode: stockMode,
    stock_ref_id: stockRefId,
    paper_length: Number(p_paper_length.value) || 0,
    paper_width: Number(p_paper_width.value) || 0,
    paper_grammage: Number(p_paper_grammage.value) || 0,
    paper_color: String(p_paper_color.value || '').trim(),
    handle_color: String(p_handle_color.value || '').trim(),
    stock_label: stockLabel
  };
}

function renderInvoiceItems(){
  invoiceItemsBody.innerHTML = invoiceItems.length ? invoiceItems.map((item, idx) => `
    <tr>
      <td>${esc(item.item_type || '-')}</td>
      <td><b>${esc(item.item_name || '-')}</b></td>
      <td>${item.quantity || 0} ${esc(item.unit || '')}</td>
      <td>${money(item.total_price || 0)}</td>
      <td>${esc(item.stock_label || 'بدون ربط')}</td>
      <td><div class="actions"><button class="btn dark" onclick="editPurchaseItem(${idx})">تعديل</button><button class="btn red" onclick="removePurchaseItem(${idx})">حذف</button></div></td>
    </tr>`).join('') : '<tr><td colspan="6">لا توجد أصناف مضافة</td></tr>';
  refreshInvoiceTotals();
}

function addPurchaseItem(){
  try {
    const item = collectCurrentPurchaseItem();
    if(editingInvoiceItemIndex > -1) invoiceItems[editingInvoiceItemIndex] = item;
    else invoiceItems.push(item);
    clearCurrentPurchaseItem();
    renderInvoiceItems();
  } catch(err) { alert(err.message || 'تعذر إضافة الصنف'); }
}

function editPurchaseItem(index){
  const item = invoiceItems[index];
  if(!item) return;
  editingInvoiceItemIndex = index;
  p_type.value = item.item_type || 'خامة أخرى';
  setPurchaseUnitOptions(p_type.value, item.unit || 'وحدة');
  p_name.value = item.item_name || '';
  p_qty.value = item.quantity || '';
  p_qty_handle.value = item.item_type === 'يد' ? (item.quantity || '') : '';
  p_unit_price.value = item.unit_price || '';
  p_item_total.value = item.total_price || '';
  p_item_total.dataset.manual = '1';
  p_stock_type.value = item.stock_type || '';
  p_stock_mode.value = item.stock_mode || 'existing';
  p_paper_length.value = item.paper_length || '';
  p_paper_width.value = item.paper_width || '';
  p_paper_grammage.value = item.paper_grammage || '';
  p_paper_color.value = item.paper_color || '';
  p_handle_color.value = item.handle_color || '';
  refreshPurchaseTypeUI();
  loadStockOptions(String(item.stock_ref_id || ''));
  p_stock_ref.value = String(item.stock_ref_id || '');
}

function removePurchaseItem(index){
  invoiceItems.splice(index, 1);
  if(editingInvoiceItemIndex === index) clearCurrentPurchaseItem();
  else if(editingInvoiceItemIndex > index) editingInvoiceItemIndex -= 1;
  renderInvoiceItems();
}

function hasDraftCurrentItem(){
  return !!(
    String(p_name.value || '').trim() || Number(p_qty.value) || Number(p_qty_handle.value) || Number(p_unit_price.value) || Number(p_item_total.value)
    || Number(p_paper_length.value) || Number(p_paper_width.value) || Number(p_paper_grammage.value) || String(p_handle_color.value || '').trim() || String(p_paper_color.value || '').trim()
  );
}

function statsPurchases(summary){ sCount.innerText = summary.purchasesCount || 0; sPurchases.innerText = money(summary.totalPurchases); sPaid.innerText = money(summary.totalPaid); sRemaining.innerText = money(summary.totalRemaining); }

function renderPurchases(){
  const s = (purchaseSearch.value || '').trim().toLowerCase();
  const rows = purchases.filter(r => String(r.item_name || '').toLowerCase().includes(s) || String(r.supplier_name || '').toLowerCase().includes(s));
  body.innerHTML = rows.length ? rows.map(r => {
    const grams = Number(r.paper_grammage || 0) > 0 ? ` | ${Number(r.paper_grammage || 0)} جم` : '';
    const handleColor = String(r.handle_color || '').trim() ? ` | لون اليد: ${esc(r.handle_color)}` : '';
    const stockText = Number(r.stock_applied) ? `<span class="tag">${String(r.stock_mode || 'existing') === 'new' ? 'صنف جديد' : 'صنف موجود'}</span>` : '-';
    return `<tr><td>#${r.id}</td><td>${esc(r.purchase_date || '-')}</td><td>${esc(r.supplier_name || '-')}</td><td><b>${esc(r.item_name || '-')}</b><br><small class="muted">${esc(r.item_type || '')} | كمية ${r.quantity || 0} ${esc(r.unit || '')}${grams}${handleColor}</small></td><td>${money(r.total_price)}<br><small class="muted">مدفوع ${money(r.paid_amount)}</small></td><td>${money(r.remaining_amount)}</td><td>${stockText}</td><td><div class="actions"><button class="btn dark" onclick="openPayments(${r.id})">دفعات</button><button class="btn red" onclick="delPurchase(${r.id})">حذف</button></div></td></tr>`;
  }).join('') : '<tr><td colspan="8">لا توجد مشتريات</td></tr>';
}

async function savePurchase(){
  if(!canManagePurchases){ alert('غير مصرح لك بإضافة مشتريات'); return; }
  if(!String(p_supplier.value || '').trim()){ alert('اختر المورد'); return; }
  if(editingInvoiceItemIndex > -1){ alert('أنت فاتح صنف للتعديل. حدّثه أو امسحه الأول.'); return; }
  if(hasDraftCurrentItem()){
    try {
      const item = collectCurrentPurchaseItem();
      invoiceItems.push(item);
      clearCurrentPurchaseItem();
      renderInvoiceItems();
    } catch(err) {
      if(invoiceItems.length === 0){ alert(err.message || 'أضف صنف واحد على الأقل'); return; }
      alert('في صنف مكتوب ولسه ما اتضافش للفاتورة. كمّله واضغط إضافة صنف للفاتورة أو امسحه.');
      return;
    }
  }
  if(!invoiceItems.length){ alert('أضف صنف واحد على الأقل للفاتورة'); return; }
  const totalInvoice = round2(invoiceItems.reduce((sum, item) => sum + Number(item.total_price || 0), 0));
  const paidInvoice = round2(Math.min(Math.max(Number(p_paid.value) || 0, 0), totalInvoice));
  p_total.value = totalInvoice;
  p_paid.value = paidInvoice;
  syncPaid();
  let remainingPaid = paidInvoice;
  const allocations = invoiceItems.map((item, idx) => {
    if(idx === invoiceItems.length - 1) return round2(remainingPaid);
    const proportional = totalInvoice > 0 ? round2((paidInvoice * Number(item.total_price || 0)) / totalInvoice) : 0;
    const safe = Math.min(round2(Number(item.total_price || 0)), proportional, remainingPaid);
    remainingPaid = round2(remainingPaid - safe);
    return safe;
  });
  for(let i = 0; i < invoiceItems.length; i++){
    const item = invoiceItems[i];
    const body = {
      purchase_date: p_date.value,
      supplier_id: p_supplier.value,
      item_type: item.item_type,
      item_name: item.item_name,
      quantity: item.quantity,
      unit: item.unit,
      unit_price: item.unit_price,
      total_price: item.total_price,
      paid_amount: allocations[i] || 0,
      due_date: p_due.value,
      notes: p_notes.value,
      stock_type: item.stock_type,
      stock_mode: item.stock_mode,
      stock_ref_id: (item.stock_type && item.stock_mode === 'existing') ? item.stock_ref_id : '',
      paper_length: item.paper_length,
      paper_width: item.paper_width,
      paper_grammage: item.paper_grammage,
      paper_color: item.paper_color,
      handle_color: item.handle_color
    };
    await authFetch('/save-purchase', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  }
  resetPurchaseForm();
  await reloadAll();
}

async function delPurchase(id){ if(!canManagePurchases){ alert('غير مصرح لك بحذف فاتورة شراء'); return; } if(!confirm('حذف فاتورة الشراء؟ لو كانت مضافة للمخزن سيتم عكسها تلقائيًا.')) return; await authFetch('/delete-purchase/' + id, { method:'DELETE' }); await reloadAll(); }
async function openPayments(id){ if(!canManagePurchases){ alert('غير مصرح لك بإدارة دفعات المشتريات'); return; } currentPurchaseId = id; const purchase = purchases.find(x => Number(x.id) === Number(id)); paymentsTitle.innerText = `فاتورة #${id} | ${purchase?.supplier_name || ''} | ${purchase?.item_name || ''}`; pp_amount.value=''; pp_date.value=today(); pp_note.value=''; await refreshPayments(); paymentsModal.style.display='block'; }
async function refreshPayments(){ if(!currentPurchaseId) return; const purchase = purchases.find(x => Number(x.id) === Number(currentPurchaseId)); paymentsSummary.innerHTML = `<b>الإجمالي:</b> ${money(purchase?.total_price || 0)} | <b>المدفوع:</b> ${money(purchase?.paid_amount || 0)} | <b>المتبقي:</b> ${money(purchase?.remaining_amount || 0)}`; const rows = await authFetch('/purchase-payments/' + currentPurchaseId); paymentsBox.innerHTML = rows.length ? rows.map(r => `<div style="padding:10px;border-bottom:1px solid #334155"><b>${money(r.amount)}</b><br><small>${esc(r.payment_date || '-')} | ${esc(r.created_by || '-')}</small><br><small>${esc(r.note || '-')}</small><div style="margin-top:8px"><button class="btn red" onclick="deletePurchasePayment(${r.id})">حذف الدفعة</button></div></div>`).join('') : 'لا توجد دفعات'; }
async function savePurchasePayment(){ if(!canManagePurchases){ alert('غير مصرح لك بإدارة الدفعات'); return; } await authFetch('/add-purchase-payment/' + currentPurchaseId, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ amount: pp_amount.value, payment_date: pp_date.value, note: pp_note.value }) }); await reloadAll(); await refreshPayments(); }
async function deletePurchasePayment(id){ if(!canManagePurchases){ alert('غير مصرح لك بحذف الدفعات'); return; } if(!confirm('حذف الدفعة؟')) return; await authFetch('/purchase-payment/' + id, { method:'DELETE' }); await reloadAll(); await refreshPayments(); }
function closePayments(){ paymentsModal.style.display='none'; currentPurchaseId=null; }
function exportPurchases(){ const headers=['رقم الفاتورة','التاريخ','المورد','النوع','الصنف','الكمية','الوحدة','الإجمالي','المدفوع','المتبقي','ميعاد السداد']; let csv=headers.join(';')+'\n'; purchases.forEach(r=>{ const row=[r.id,r.purchase_date,r.supplier_name,r.item_type,r.item_name,r.quantity,r.unit,r.total_price,r.paid_amount,r.remaining_amount,r.due_date||'']; csv += row.map(v=>`"${String(v??'').replace(/"/g,'""')}"`).join(';')+'\n'; }); const blob=new Blob(["﻿"+csv],{type:'text/csv;charset=utf-8;'}); const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='purchases.csv'; a.click(); setTimeout(()=>URL.revokeObjectURL(a.href),500); }

function supplierStats(){ supCount.innerText = suppliers.length; supPurchases.innerText = money(suppliers.reduce((s,r)=>s+Number(r.totalPurchases||0),0)); supPaid.innerText = money(suppliers.reduce((s,r)=>s+Number(r.totalPaid||0),0)); supRemaining.innerText = money(suppliers.reduce((s,r)=>s+Number(r.totalRemaining||0),0)); }
function resetSupplierForm(){ s_id.value=''; s_name.value=''; s_type.value='ورق'; s_phone.value=''; s_address.value=''; s_opening.value='0'; s_active.value='1'; s_notes.value=''; }
function fillSupplierForm(row){ s_id.value=row.id||''; s_name.value=row.name||''; s_type.value=row.supplier_type||'ورق'; s_phone.value=row.phone||''; s_address.value=row.address||''; s_opening.value=row.opening_balance||0; s_active.value=String(Number(row.is_active)!==0?1:0); s_notes.value=row.notes||''; }
function renderSuppliers(){ const s=(supplierSearch.value||'').trim().toLowerCase(); const rows=suppliers.filter(r=> String(r.name||'').toLowerCase().includes(s) || String(r.phone||'').toLowerCase().includes(s)); supBody.innerHTML=rows.length?rows.map(r=>`<tr><td><b>${esc(r.name||'')}</b><br><small class="muted">${esc(r.phone||'')} - ${esc(r.address||'')}</small></td><td>${esc(r.supplier_type||'-')}</td><td>${money(r.totalPurchases)}<br><small class="muted">${r.purchasesCount||0} فاتورة</small></td><td>${money(r.totalRemaining)}</td><td><div class="actions"><button class="btn dark" onclick="selectSupplier(${r.id})">تعديل</button><button class="btn green" onclick="openSupplierStatement(${r.id})">كشف حساب</button><button class="btn red" onclick="delSupplier(${r.id})">حذف</button></div></td></tr>`).join(''):'<tr><td colspan="5">لا يوجد موردين</td></tr>'; supplierStats(); }
function selectSupplier(id){ const row=suppliers.find(x=>Number(x.id)===Number(id)); if(row){ fillSupplierForm(row); goToSuppliers(); } }
async function saveSupplier(){ if(!canManageSuppliers){ alert('غير مصرح لك بإدارة الموردين'); return; } const body={ id:s_id.value, name:s_name.value, supplier_type:s_type.value, phone:s_phone.value, address:s_address.value, opening_balance:s_opening.value, is_active:s_active.value, notes:s_notes.value }; await authFetch('/save-supplier',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); resetSupplierForm(); await reloadAll(); }
async function delSupplier(id){ if(!canManageSuppliers){ alert('غير مصرح لك بإدارة الموردين'); return; } if(!confirm('حذف المورد؟')) return; await authFetch('/delete-supplier/' + id,{method:'DELETE'}); await reloadAll(); }
async function openSupplierStatement(id){ const d=await authFetch('/supplier-statement/' + id); statementTitle.innerText=`${d.supplier.name||''} | ${d.supplier.phone||''}`; statementChips.innerHTML=`<div class="chip">إجمالي المشتريات: ${money(d.summary.totalPurchases)}</div><div class="chip">المدفوع: ${money(d.summary.totalPaid)}</div><div class="chip">المتبقي: ${money(d.summary.totalRemaining)}</div><div class="chip">عدد الفواتير: ${d.summary.purchasesCount||0}</div><div class="chip">رصيد افتتاحي: ${money(d.summary.openingBalance||0)}</div>`; purchasesBox.innerHTML=d.purchases.length?d.purchases.map(p=>`<div style="padding:10px;border-bottom:1px solid #334155"><b>فاتورة #${p.id}</b> - ${esc(p.item_name||'')}<br><small>${esc(p.purchase_date||'')} | ${esc(p.item_type||'')} | كمية ${p.quantity||0} ${esc(p.unit||'')}</small><br><small>الإجمالي ${money(p.total_price)} | المدفوع ${money(p.paid_amount)} | المتبقي ${money(p.remaining_amount)}</small></div>`).join(''):'لا توجد مشتريات'; supplierPaymentsBox.innerHTML=d.payments.length?d.payments.map(p=>`<div style="padding:10px;border-bottom:1px solid #334155"><b>${money(p.amount)}</b><br><small>${esc(p.payment_date||'')} | فاتورة #${p.purchase_no||''}</small><br><small>${esc(p.note||'-')}</small></div>`).join(''):'لا توجد دفعات'; statementModal.style.display='block'; }
function closeStatement(){ statementModal.style.display='none'; }
function exportSuppliers(){ const headers=['الاسم','النوع','التليفون','العنوان','إجمالي المشتريات','المدفوع','المتبقي']; let csv=headers.join(';')+'\n'; suppliers.forEach(r=>{ const row=[r.name,r.supplier_type,r.phone,r.address,r.totalPurchases,r.totalPaid,r.totalRemaining]; csv += row.map(v=>`"${String(v??'').replace(/"/g,'""')}"`).join(';')+'\n'; }); const blob=new Blob(["﻿"+csv],{type:'text/csv;charset=utf-8;'}); const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='suppliers.csv'; a.click(); setTimeout(()=>URL.revokeObjectURL(a.href),500); }

async function reloadAll(){
  const [supplierRows, summary, purchaseRows, paper, handles, bags] = await Promise.all([
    canManageSuppliers ? authFetch('/suppliers').catch(()=>[]) : Promise.resolve([]),
    canManagePurchases ? authFetch('/purchases-summary').catch(()=>({totalPurchases:0,totalPaid:0,totalRemaining:0,purchasesCount:0})) : Promise.resolve({totalPurchases:0,totalPaid:0,totalRemaining:0,purchasesCount:0}),
    canManagePurchases ? authFetch('/purchases').catch(()=>[]) : Promise.resolve([]),
    canManagePurchases ? authFetch('/get-paper').catch(()=>[]) : Promise.resolve([]),
    canManagePurchases ? authFetch('/get-handles').catch(()=>[]) : Promise.resolve([]),
    canManagePurchases ? authFetch('/get-bags').catch(()=>[]) : Promise.resolve([])
  ]);
  suppliers = supplierRows || [];
  purchases = purchaseRows || [];
  stockCache.paper = paper || [];
  stockCache.handle = handles || [];
  stockCache.bag = bags || [];
  renderSupplierSelect();
  statsPurchases(summary || {});
  renderPurchases();
  renderSuppliers();
  toggleStockSection();
}

window.openPayments = openPayments;
window.delPurchase = delPurchase;
window.deletePurchasePayment = deletePurchasePayment;
window.selectSupplier = selectSupplier;
window.delSupplier = delSupplier;
window.openSupplierStatement = openSupplierStatement;
window.closeStatement = closeStatement;

if(!canManagePurchases){
  document.getElementById('purchaseSection').style.display='none';
  document.getElementById('purchaseAnchor').style.display='none';
}else{
  resetPurchaseForm();
  refreshPurchaseTypeUI();
}
if(!canManageSuppliers){
  document.getElementById('suppliersSection').style.display='none';
  document.getElementById('suppliersAnchor').style.display='none';
}else{
  resetSupplierForm();
}
reloadAll();
if(location.hash==='#suppliers' || location.hash==='#suppliersSection') setTimeout(()=>goToSuppliers(),120);
