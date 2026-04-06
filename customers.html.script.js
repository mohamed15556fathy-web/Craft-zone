
const token = localStorage.getItem('token');
const user = JSON.parse(localStorage.getItem('user') || 'null');
if(!token || !user) location.href = 'login.html';
if(user.role !== 'admin' && !Number(user.perm_customers || 0)) location.href = 'index.html';

let data = [];
let vipOnly = false;

function authFetch(url, opts = {}) {
  opts.headers = Object.assign({}, opts.headers || {}, { Authorization: 'Bearer ' + token });
  return fetch(url, opts).then(async r => {
    const d = await r.json().catch(() => ({}));
    if (r.status === 401) {
      localStorage.clear();
      location.href = 'login.html';
      throw new Error('unauthorized');
    }
    if (!r.ok) throw new Error(d.error || 'error');
    return d;
  });
}
function money(v){ return Number(v || 0).toLocaleString('en-US', { maximumFractionDigits: 2 }) + ' ج'; }
function esc(s){ return String(s || '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

function stats(){
  sCustomers.innerText = data.length;
  sVipCustomers.innerText = data.filter(r => Number(r.is_vip || 0) === 1).length;
  sSales.innerText = money(data.reduce((s, r) => s + Number(r.totalSales || 0), 0));
  sRemaining.innerText = money(data.reduce((s, r) => s + Number(r.totalRemaining || 0), 0));
}

function resetForm(){
  c_id.value = '';
  c_name.value = '';
  c_phone.value = '';
  c_email.value = '';
  c_address.value = '';
  c_governorate.value = '';
  c_zone.value = '';
  c_notes.value = '';
  c_is_vip.checked = false;
}

function fillForm(row){
  c_id.value = row.id || '';
  c_name.value = row.name || row.custName || '';
  c_phone.value = row.phone || row.custPhone || '';
  c_email.value = row.email || '';
  c_address.value = row.address || row.custAddress || '';
  c_governorate.value = row.governorate || '';
  c_zone.value = row.zone || '';
  c_notes.value = row.notes || '';
  c_is_vip.checked = Number(row.is_vip || 0) === 1;
}

async function saveCustomer(){
  const body = {
    id: c_id.value,
    name: c_name.value,
    phone: c_phone.value,
    email: c_email.value,
    address: c_address.value,
    governorate: c_governorate.value,
    zone: c_zone.value,
    notes: c_notes.value,
    is_vip: c_is_vip.checked ? 1 : 0
  };
  await authFetch('/save-customer', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  resetForm();
  await loadCustomers();
}

async function toggleVip(id){
  const row = data.find(x => Number(x.id) === Number(id));
  if (!row) return;
  const body = {
    id: row.id,
    name: row.name || row.custName || '',
    phone: row.phone || row.custPhone || '',
    email: row.email || '',
    address: row.address || row.custAddress || '',
    governorate: row.governorate || '',
    zone: row.zone || '',
    notes: row.notes || '',
    is_vip: Number(row.is_vip || 0) === 1 ? 0 : 1
  };
  await authFetch('/save-customer', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  await loadCustomers();
}

function toggleVipFilter(){
  vipOnly = !vipOnly;
  vipFilterBtn.innerText = vipOnly ? '⭐ Vip فقط' : '⭐ Vip';
  vipFilterBtn.classList.toggle('dark', !vipOnly);
  vipFilterBtn.classList.toggle('gold', true);
  render();
}

function render(){
  const s = (q.value || '').trim().toLowerCase();
  const rows = data.filter(c => {
    const matchSearch = String(c.custName || c.name || '').toLowerCase().includes(s) || String(c.custPhone || c.phone || '').toLowerCase().includes(s);
    const matchVip = !vipOnly || Number(c.is_vip || 0) === 1;
    return matchSearch && matchVip;
  });
  body.innerHTML = rows.length ? rows.map(c => `
    <tr>
      <td>
        ${Number(c.is_vip || 0) === 1 ? '<div class="vip-badge">⭐ Vip</div>' : ''}
        <b>${esc(c.custName || c.name || '')}</b><br>
        <small class="muted">${esc(c.custAddress || c.address || '')}</small>
      </td>
      <td>${esc(c.custPhone || c.phone || '')}</td>
      <td>${c.ordersCount || 0}<br><small class="muted">آخر أوردر: ${esc(c.lastOrderDate || '-')}</small></td>
      <td>${money(c.totalSales)}</td>
      <td>${money(c.totalRemaining)}</td>
      <td>
        <div class="actions">
          <button class="btn gold" onclick="toggleVip(${c.id})">${Number(c.is_vip || 0) === 1 ? '⭐ إزالة النجمة' : '☆ نجمة'}</button>
          <button class="btn dark" onclick="selectCustomer(${c.id})">تعديل</button>
          <button class="btn green" onclick="openStatement(${c.id})">كشف حساب</button>
        </div>
      </td>
    </tr>
  `).join('') : '<tr><td colspan="6">لا يوجد عملاء</td></tr>';
  stats();
}

function selectCustomer(id){ const row = data.find(x => Number(x.id) === Number(id)); if(row) fillForm(row); }

async function openStatement(id){
  const d = await authFetch('/customer-statement/' + id);
  statementTitle.innerText = `${d.customer.name || ''} | ${d.customer.phone || ''}`;
  statementChips.innerHTML = `
    <div class="chip">إجمالي البيع: ${money(d.summary.totalSales)}</div>
    <div class="chip">المدفوع: ${money(d.summary.totalPaid)}</div>
    <div class="chip">المتبقي: ${money(d.summary.totalRemaining)}</div>
    <div class="chip">عدد الأوردرات: ${d.orders.length}</div>
    ${Number(d.customer.is_vip || 0) === 1 ? '<div class="chip">⭐ عميل Vip</div>' : ''}
  `;
  ordersBox.innerHTML = d.orders.length ? d.orders.map(o => `<div style="padding:10px;border-bottom:1px solid #334155"><b>أوردر #${o.id}</b> - ${esc(o.status || '')}<br><small>${esc(o.orderDate || '')} | ${o.l}×${o.w}×${o.g} | كمية ${o.qty || 0}</small><br><small>البيع ${money(o.total_price)} | المدفوع ${money(o.paid_amount)} | المتبقي ${money(o.remaining_amount)}</small></div>`).join('') : 'لا توجد أوردرات';
  paymentsBox.innerHTML = d.payments.length ? d.payments.map(p => `<div style="padding:10px;border-bottom:1px solid #334155"><b>${money(p.amount)}</b> - ${esc(p.method || 'نقدي')}<br><small>${esc(p.payment_date || '')} | أوردر #${p.order_id || ''}</small><br><small>${esc(p.note || '-')}</small></div>`).join('') : 'لا توجد دفعات';
  statementModal.style.display = 'block';
}

function closeStatement(){ statementModal.style.display = 'none'; }
async function loadCustomers(){ data = await authFetch('/customers'); render(); }

window.selectCustomer = selectCustomer;
window.openStatement = openStatement;
window.closeStatement = closeStatement;
window.toggleVip = toggleVip;

loadCustomers();
