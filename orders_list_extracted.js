
const token=localStorage.getItem('token');
const user=JSON.parse(localStorage.getItem('user')||'null');
if(!token||!user) location.href='login.html';
if(user.username!=='admin'&&user.role!=='super_admin' && !Number(user.perm_view_orders||0)) location.href='index.html';
let ordersData=[], filteredOrders=[], currentOrderId=null, printMode=false, pendingStatusOrderId=null, activeOnlyFilter=false;
const groupChildrenOpen={};
let currentDirectionOrderId=null, currentDirectionData={order:null,steps:[],partners:[]};
let currentGroupStatusFlow=null;
let printSelectionSnapshot=[];
const ORDER_PAGE_OPTIONS=['El7amd Factory - مصنع الحمد','Craft Zone','Craft House - بــيت الكرافت'];

function normalizePhoneDigits(value=''){
  const arabicDigits='٠١٢٣٤٥٦٧٨٩';
  const persianDigits='۰۱۲۳۴۵۶۷۸۹';
  return String(value||'')
    .replace(/[٠-٩]/g,digit=>String(arabicDigits.indexOf(digit)))
    .replace(/[۰-۹]/g,digit=>String(persianDigits.indexOf(digit)))
    .replace(/[^0-9+]/g,'');
}
function enforceEnglishPhoneInput(input){
  if(!input) return '';
  const normalized=normalizePhoneDigits(input.value);
  if(input.value!==normalized) input.value=normalized;
  return normalized;
}

const STATUS_META={
  'أوردر جديد':{emoji:'🆕',bg:'rgba(148,163,184,.12)',border:'#64748b',color:'#e2e8f0'},
  'تحت الإنتاج':{emoji:'⚙️',bg:'rgba(100,116,139,.12)',border:'#64748b',color:'#e2e8f0'},
  'في القص':{emoji:'✂️',bg:'rgba(120,113,108,.12)',border:'#78716c',color:'#e7e5e4'},
  'مستني الزنكات':{emoji:'🧾',bg:'rgba(161,98,7,.10)',border:'#a16207',color:'#fde68a'},
  'تحت الطباعة':{emoji:'🖨️',bg:'rgba(8,145,178,.10)',border:'#0891b2',color:'#cffafe'},
  'تحت التصنيع':{emoji:'🏗️',bg:'rgba(107,114,128,.12)',border:'#6b7280',color:'#e5e7eb'},
  'جاهز للشحن':{emoji:'📦',bg:'rgba(21,128,61,.12)',border:'#15803d',color:'#dcfce7'},
  'تم الشحن':{emoji:'🚚',bg:'rgba(22,101,52,.14)',border:'#166534',color:'#dcfce7'},
  'تم التسليم':{emoji:'✅',bg:'rgba(20,83,45,.16)',border:'#14532d',color:'#dcfce7'},
  'مرتجع':{emoji:'↩️',bg:'rgba(180,83,9,.10)',border:'#b45309',color:'#fed7aa'},
  'في مشكله':{emoji:'⚠️',bg:'rgba(127,29,29,.14)',border:'#7f1d1d',color:'#fecaca'}
};
const ALL_ORDER_STATUSES=['أوردر جديد','تحت الإنتاج','مستني الزنكات','في القص','تحت الطباعة','تحت التصنيع','جاهز للشحن','تم الشحن','تم التسليم','مرتجع','في مشكله'];
const READY_STOCK_ONLY_STATUSES=['أوردر جديد','جاهز للشحن','تم الشحن','تم التسليم','مرتجع','في مشكله'];
let currentPaymentsOrderId=null;
let paymentActorUsers=[];
function orderById(id){ return ordersData.find(x=>Number(x.id)===Number(id)) || null; }
function paymentActorUsername(row){ return String(row?.username || row?.admin_username || row?.actor_username || '').trim(); }
function isCashboxActorValue(v){ const s=String(v||'').trim(); return s==='__cashbox__'||s==='cashbox'||s==='الخزنة'||s==='خزنة'; }
function paymentActorLabel(row){
  const username=paymentActorUsername(row);
  if(isCashboxActorValue(username) || String(row?.full_name||row?.actor_name||'').trim()==='الخزنة') return 'الخزنة';
  const name=String(row?.full_name || row?.display_name || row?.actor_name || username || '').trim();
  return name && name!==username ? `${name} (${username})` : (name || '-');
}
async function loadPaymentActorUsers(){
  try{
    const data=await authFetch('/active-users-lite?tracked_only=1');
    const rows=Array.isArray(data.transfer_users)&&data.transfer_users.length?data.transfer_users:(Array.isArray(data.users)?data.users:[]);
    const seen=new Set();
    paymentActorUsers=[];
    for(const row of rows){
      const username=paymentActorUsername(row);
      if(!username || seen.has(username.toLowerCase())) continue;
      seen.add(username.toLowerCase());
      paymentActorUsers.push(row);
    }
    if(!paymentActorUsers.some(u=>isCashboxActorValue(paymentActorUsername(u)))) paymentActorUsers.unshift({username:'__cashbox__',full_name:'الخزنة'});
  }catch(e){ paymentActorUsers=[{username:'__cashbox__',full_name:'الخزنة'}]; }
}
function fillPaymentActorSelect(selected='__cashbox__'){
  const sel=document.getElementById('pay_actor_username');
  if(!sel) return;
  if(!paymentActorUsers.length) paymentActorUsers=[{username:'__cashbox__',full_name:'الخزنة'}];
  sel.innerHTML=paymentActorUsers.map(row=>{const username=paymentActorUsername(row); return `<option value="${esc(username)}" ${String(selected)===username?'selected':''}>${esc(paymentActorLabel(row))}</option>`;}).join('');
  if(!sel.value) sel.value='__cashbox__';
}
function updatePaymentSummaryBox(order){
  const o=order||orderById(currentPaymentsOrderId)||{};
  const summary=document.getElementById('paymentsSummary');
  if(!summary) return;
  summary.innerHTML=`<b>الأوردر #${o.id||''}</b><br><small>إجمالي البيع: ${fmtMoney(o.total_price||0)} | المدفوع: ${fmtMoney(o.paid_amount||0)} | المتبقي: ${fmtMoney(o.remaining_amount||0)} | نوع الدفع: ${o.paymentType||'-'}</small>`;
}
async function loadOrderPayments(){
  const box=document.getElementById('paymentsBox');
  if(!box || !currentPaymentsOrderId) return;
  const rows=await authFetch(`/order-payments/${currentPaymentsOrderId}`);
  const human=rows.filter(r=>String(r.note||'').trim()!=='الرصيد الافتتاحي للأوردر');
  box.innerHTML=human.length?human.map(r=>`<div style="padding:10px;border-bottom:1px solid #334155"><b>${fmtMoney(r.amount)}</b> - ${r.method||'نقدي'}<br><small>${r.payment_date||''} | ${r.created_by||''} | دخلت في: ${esc(paymentActorLabel({username:r.actor_username,full_name:r.actor_name,actor_name:r.actor_name}))}</small><br><small>${r.note||'-'}</small><div style="margin-top:8px"><button class="btn red" onclick="deleteOrderPayment(${r.id})">حذف الدفعة</button></div></div>`).join(''):'لا توجد دفعات مسجلة بعد';
}
async function openPaymentsModal(id){
  currentPaymentsOrderId=Number(id);
  document.getElementById('pay_amount').value='';
  document.getElementById('pay_date').value=new Date().toISOString().slice(0,10);
  document.getElementById('pay_method').value='نقدي';
  document.getElementById('pay_note').value='';
  await loadPaymentActorUsers();
  fillPaymentActorSelect('__cashbox__');
  updatePaymentSummaryBox(orderById(id));
  await loadOrderPayments();
  document.getElementById('paymentsModal').style.display='block';
}
async function saveOrderPayment(){
  if(!currentPaymentsOrderId) return;
  const body={ amount: document.getElementById('pay_amount').value, payment_date: document.getElementById('pay_date').value, method: document.getElementById('pay_method').value, actor_username: document.getElementById('pay_actor_username')?.value || '__cashbox__', note: document.getElementById('pay_note').value };
  await authFetch(`/add-order-payment/${currentPaymentsOrderId}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  await load();
  updatePaymentSummaryBox(orderById(currentPaymentsOrderId));
  await loadOrderPayments();
  document.getElementById('pay_amount').value='';
  document.getElementById('pay_note').value='';
}
async function deleteOrderPayment(id){
  if(!confirm('حذف الدفعة؟')) return;
  await authFetch(`/order-payment/${id}`,{method:'DELETE'});
  await load();
  updatePaymentSummaryBox(orderById(currentPaymentsOrderId));
  await loadOrderPayments();
}

function authFetch(url,opts={}){
  opts.headers=Object.assign({},opts.headers||{},{Authorization:'Bearer '+token});
  return fetch(url,opts).then(async r=>{
    const data=await r.json().catch(()=>({}));
    if(r.status===401){localStorage.clear();location.href='login.html'; throw new Error('unauthorized')}
    if(!r.ok) throw new Error(data.error||'error');
    return data;
  });
}
function fmtMoney(v){return Number(v||0).toLocaleString('en-US',{maximumFractionDigits:2})+' ج';}
function numVal(v,fallback=0){ const n=Number(v); return Number.isFinite(n) ? n : fallback; }
function getGroupItemsByCode(groupCode){
  const code=String(groupCode||'').trim();
  if(!code) return [];
  return ordersData.filter(x=>String(x.group_code||'').trim()===code).sort((a,b)=>Number(a.item_no||1)-Number(b.item_no||1));
}
function getGroupDisplayOrderByCode(groupCode){
  const code=String(groupCode||'').trim();
  if(!code) return null;
  return buildDisplayOrders(ordersData).find(x=>x.is_group && String(x.group_code||'').trim()===code) || null;
}
function buildGroupBostaContext(groupCode){
  const group=getGroupDisplayOrderByCode(groupCode);
  if(!group) return null;
  const items=orderItems(group);
  const primary=items.find(item=>isBostaOrder(item) || String(item.bosta_business_reference||'').trim()) || items[0] || {};
  const sumField=field=>items.reduce((sum,item)=>sum+(Number(item?.[field])||0),0);
  return {
    ...primary,
    ...group,
    id:Number(primary.id||group.id||0),
    is_group:true,
    group_code:String(group.group_code||'').trim(),
    group_items:items,
    qty:Number(group.qty_total || sumField('qty') || 1),
    total_price:Number(group.total_price_display || sumField('total_price') || 0),
    paid_amount:Number(group.paid_total || sumField('paid_amount') || 0),
    remaining_amount:Number(group.remaining_total || sumField('remaining_amount') || 0),
    bosta_cod:Number(primary.bosta_cod || group.remaining_total || sumField('remaining_amount') || 0),
    bosta_product_value:Number(primary.bosta_product_value || group.total_price_display || sumField('total_price') || 0),
    bosta_business_reference:String(primary.bosta_business_reference || group.group_code || '').trim(),
    bosta_package_description:String(primary.bosta_package_description || '').trim(),
    customer_governorate:primary.customer_governorate || '',
    customer_zone:primary.customer_zone || '',
    customer_email:primary.customer_email || '',
    custName:primary.custName || group.custName || '',
    custPhone:primary.custPhone || group.custPhone || '',
    custAddress:primary.custAddress || group.custAddress || '',
    bosta_city_code:primary.bosta_city_code || primary.customer_governorate || '',
    bosta_zone:primary.bosta_zone || primary.customer_zone || ''
  };
}
function currentBostaOrder(){
  const groupCode=String(window.currentBostaGroupCode||'').trim();
  if(groupCode) return buildGroupBostaContext(groupCode);
  return ordersData.find(x=>Number(x.id)===Number(window.currentBostaOrderId||0)) || null;
}
function getBostaProductValueDefault(order){
  const saved = Number(order?.bosta_product_value || 0);
  if(saved > 0) return saved;
  if(order?.is_group){
    const grouped = Number(order?.total_price_display || order?.total_price || 0);
    if(grouped > 0) return grouped;
  }
  const displayed = Number(getDisplayedOrderTotal(order) || 0);
  if(displayed > 0) return displayed;
  const readySale = Number(order?.ready_stock_sale_total || 0);
  if(readySale > 0) return readySale;
  const total = Number(order?.total_price || 0);
  if(total > 0) return total;
  const savedCod = Number(order?.bosta_cod || 0);
  if(savedCod > 0) return savedCod;
  return Math.max(0, Number(order?.remaining_amount || 0));
}
function syncBostaProductValue(force=false){
  const productInput = document.getElementById('b_product_value');
  if(!productInput) return;
  const order = currentBostaOrder() || {};
  const fallbackValue = Math.max(0, getBostaProductValueDefault(order));
  const currentValue = String(productInput.value || '').trim();
  // القيمة بتتكتب تلقائيًا من إجمالي الأوردر عند فتح النافذة فقط، وبعدها تفضل قابلة للتعديل اليدوي.
  if(force || !currentValue){
    productInput.value = String(fallbackValue);
    window.currentBostaProductManual = false;
  }
}
function resetBostaProductValueToOrder(){
  syncBostaProductValue(true);
  renderBostaEstimateBox();
  requestBostaEstimate(true);
}
if(typeof window !== 'undefined'){
  window.addEventListener('DOMContentLoaded', ()=>{
    const codInput = document.getElementById('b_cod');
    if(codInput){
      codInput.addEventListener('input', ()=>{
        renderBostaEstimateBox();
        requestBostaEstimate(true);
      });
    }
  });
}
function setBostaAllowOpen(v){
  window.currentBostaAllowOpen = !!v;
  const btn = document.getElementById('b_allow_open_btn');
  if(btn){
    btn.className = `btn ${window.currentBostaAllowOpen ? 'green' : 'dark'} bosta-toggle`;
    btn.textContent = window.currentBostaAllowOpen ? '✅ السماح بفتح الشحنة' : '🚫 عدم السماح بفتح الشحنة';
  }
  renderBostaEstimateBox();
}
function toggleBostaAllowOpen(){ setBostaAllowOpen(!window.currentBostaAllowOpen); requestBostaEstimate(true); }
function toggleBostaDetails(force){
  if(typeof force==='boolean') window.currentBostaShowDetails = force;
  else window.currentBostaShowDetails = !window.currentBostaShowDetails;
  const btn = document.getElementById('b_shipping_details_btn');
  const box = document.getElementById('b_estimate_box');
  if(btn) {
    btn.className = `btn ${window.currentBostaShowDetails ? 'green' : 'dark'} bosta-toggle`;
    btn.textContent = window.currentBostaShowDetails ? '✅ إخفاء تفاصيل الشحن' : '📦 تفاصيل الشحن';
  }
  if(box) box.style.display = window.currentBostaShowDetails ? 'block' : 'none';
  if(window.currentBostaShowDetails) renderBostaEstimateBox();
}
function currentBostaEstimateInputs(){
  const order = currentBostaOrder() || {};
  return {
    city_code: String(document.getElementById('b_city_code')?.value || '').trim(),
    city_label: String((document.getElementById('b_city_code')?.options?.[document.getElementById('b_city_code')?.selectedIndex] || {}).getAttribute?.('data-label') || (document.getElementById('b_city_code')?.options?.[document.getElementById('b_city_code')?.selectedIndex] || {}).text || '').trim(),
    zone: String(document.getElementById('b_zone')?.value || '').trim(),
    zone_label: bostaSelectedZoneLabel(),
    package_type: String(document.getElementById('b_package_type')?.value || 'Parcel').trim() || 'Parcel',
    items_count: Math.max(1, numVal(document.getElementById('b_items_count')?.value, Number(order.qty||1))),
    cod: Math.max(0, numVal(document.getElementById('b_cod')?.value, Number(order.bosta_cod||order.remaining_amount||0))),
    product_value: Math.max(0, numVal(document.getElementById('b_product_value')?.value, getBostaProductValueDefault(order))),
    allow_open: window.currentBostaAllowOpen ? 1 : 0,
    second_line: String(document.getElementById('b_second_line')?.value || order.custAddress || '').trim(),
    package_description: String(document.getElementById('b_package_description')?.value || '').trim() || bostaPackageDescriptionFromOrder(order)
  };
}
async function requestBostaEstimate(force=false){
  const estimateBox = document.getElementById('b_estimate_box');
  const inputs = currentBostaEstimateInputs();
  const key = JSON.stringify(inputs);
  window.currentBostaEstimateKey = key;
  if(!inputs.city_code || !inputs.zone){
    window.currentBostaEstimate = null;
    renderBostaEstimateBox();
    return null;
  }
  if(!force && window.currentBostaEstimate && window.currentBostaEstimateKey === key) return window.currentBostaEstimate;
  window.currentBostaEstimateLoading = true;
  renderBostaEstimateBox();
  try{
    const data = await authFetch('/bosta-estimate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(inputs)});
    if(window.currentBostaEstimateKey !== key) return null;
    window.currentBostaEstimate = data || null;
    window.currentBostaEstimateLoading = false;
    renderBostaEstimateBox();
    return data;
  }catch(e){
    if(window.currentBostaEstimateKey === key){
      window.currentBostaEstimate = { error: e.message || 'تعذر حساب تكلفة الشحن الآن' };
      window.currentBostaEstimateLoading = false;
      renderBostaEstimateBox();
    }
    return null;
  }
}
function renderBostaEstimateBox(){
  const order = currentBostaOrder() || {};
  const productValue = Math.max(0, numVal(document.getElementById('b_product_value')?.value, getBostaProductValueDefault(order)));
  const codValue = Math.max(0, numVal(document.getElementById('b_cod')?.value, Number(order.bosta_cod||order.remaining_amount||0)));
  const estimateBox = document.getElementById('b_estimate_box');
  if(!estimateBox) return;
  estimateBox.style.display = window.currentBostaShowDetails ? 'block' : 'none';
  if(!window.currentBostaShowDetails){ estimateBox.innerHTML=''; return; }
  const estimate = window.currentBostaEstimate || {};
  const shippingFee = Math.max(0, numVal(estimate.shippingFee, Number(order.bosta_shipping_fee||0)));
  const rawShippingFee = Math.max(0, numVal(estimate.rawShippingFee, Number(order.bosta_raw_shipping_fee||0)));
  const insurance = Math.max(0, numVal(estimate.insuranceFees, Number(order.bosta_insurance_fees||+(productValue*0.01).toFixed(2)||0)));
  const openFee = Math.max(0, numVal(estimate.openPackageFees, window.currentBostaAllowOpen ? 7 : Number(order.bosta_open_package_fees||0)));
  const materialFee = Math.max(0, numVal(estimate.bostaMaterialFee, Number(order.bosta_material_fee||0)));
  const extraCodFee = Math.max(0, numVal(estimate.extraCodFee, Number(order.bosta_extra_cod_fee||0)));
  const vatAmount = Math.max(0, numVal(estimate.vatAmount, Number(order.bosta_vat_amount||0)));
  const priceBeforeVat = Math.max(0, numVal(estimate.priceBeforeVat, Number(order.bosta_price_before_vat||0)));
  const priceAfterVat = Math.max(0, numVal(estimate.priceAfterVat, Number(order.bosta_price_after_vat||0)));
  const estimated = Math.max(0, numVal(estimate.estimatedFees, Number(order.bosta_estimated_fees||0)));
  let html = `<div><b>قيمة المنتج:</b> ${fmtMoney(productValue)}</div>`;
  html += `<div><b>مبلغ التحصيل:</b> ${fmtMoney(codValue)}</div>`;
  html += `<div><b>فتح الشحنة:</b> ${window.currentBostaAllowOpen ? 'مسموح' : 'غير مسموح'}</div>`;
  if(!currentBostaEstimateInputs().city_code || !currentBostaEstimateInputs().zone){
    html += `<div><b>سعر الشحن حسب المحافظة:</b> اختَر المحافظة والمنطقة أولًا</div>`;
  }else if(window.currentBostaEstimateLoading){
    html += `<div><b>سعر الشحن حسب المحافظة:</b> جاري حسابه...</div>`;
  }else{
    html += `<div><b>سعر الشحن حسب المحافظة:</b> ${shippingFee>0 ? fmtMoney(shippingFee) : 'غير متاح الآن'}</div>`;
  }
  if(rawShippingFee>0) html += `<div><b>السعر الخام من رد بوسطة:</b> ${fmtMoney(rawShippingFee)}</div>`;
  html += `<div><b>مصاريف التأمين:</b> ${fmtMoney(insurance)}</div>`;
  if(extraCodFee>0) html += `<div><b>زيادة المبلغ على COD:</b> ${fmtMoney(extraCodFee)}</div>`;
  if(materialFee>0) html += `<div><b>خامة بوسطة:</b> ${fmtMoney(materialFee)}</div>`;
  if(openFee>0) html += `<div><b>رسوم فتح الشحنة:</b> ${fmtMoney(openFee)}</div>`;
  if(priceBeforeVat>0) html += `<div><b>الإجمالي قبل الضريبة:</b> ${fmtMoney(priceBeforeVat)}</div>`;
  if(vatAmount>0) html += `<div><b>الضريبة:</b> ${fmtMoney(vatAmount)}</div>`;
  if(priceAfterVat>0) html += `<div><b>الإجمالي بعد الضريبة:</b> ${fmtMoney(priceAfterVat)}</div>`;
  if(window.currentBostaEstimateLoading){
    html += `<div><b>تقدير مستحقات بوسطة:</b> جاري الحساب...</div>`;
  }else if(estimated>0){
    html += `<div><b>تقدير مستحقات بوسطة:</b> ${fmtMoney(estimated)}</div>`;
  }else{
    html += `<div><b>تقدير مستحقات بوسطة:</b> غير متاح الآن</div>`;
  }
  if(['local','manual_table'].includes(String(estimate.source||'').trim())) html += `<div style="color:#fbbf24"><small>التقدير الحالي داخل السيستم حسب جدول الشحن اللي اتحدد يدويًا: سعر أساسي للمحافظة + 10 ج لكل 1000 من مبلغ البيع + 1% تأمين من قيمة المنتج + رسوم فتح الشحنة لو مفعلة.</small></div>`;
  if(String(estimate.error||'').trim()) html += `<div style="color:#fca5a5"><small>${esc(estimate.error)}</small></div>`;
  if(String(estimate.text||order.bosta_estimated_fees_text||'').trim()) html += `<div style="color:#93c5fd"><small>${esc(String(estimate.text||order.bosta_estimated_fees_text||''))}</small></div>`;
  estimateBox.innerHTML = html;
}
function normalizePaperType(v){v=String(v||'').trim();return(v==='كوشيه'||v==='برستول كوشيه'||v==='ورق طبع')?v:'كرافت';}
function isCoatedPaperType(v){const t=normalizePaperType(v);return t==='كوشيه'||t==='برستول كوشيه';}
function isFixedWhitePaperType(v){const t=normalizePaperType(v);return isCoatedPaperType(t)||t==='ورق طبع';}
function normalizeHandleType(v,paperType){v=String(v||'').trim();const t=(v==='دوبار'||v==='دوبارة'||v==='دوباره')?'دوباره':(v==='ستان'||v==='ساتان'||v.toLowerCase()==='satin')?'ستان':(v==='مجدول'?'مجدول':'');if(isCoatedPaperType(paperType))return t==='ستان'?'ستان':'دوباره';return t||'مجدول';}
function rebuildEditPrintTypeOptions(){
  const type=normalizePaperType(e_paper_type?.value);
  const current=String(e_printType?.value||'سادة').trim();
  const useReady=String(window.e_useReadyStock?.value||'0')==='1';
  let list;
  if(useReady) list=isCoatedPaperType(type)?['سادة']:['سادة','سلك سكرين'];
  else list=isCoatedPaperType(type)?['سادة','أوفست']:['سادة','سلك سكرين','أوفست'];
  if(e_printType){e_printType.innerHTML=list.map(v=>`<option value="${v}" ${v===current?'selected':''}>${v}</option>`).join('');if(!list.includes(current))e_printType.value=list[0];}
}
function renderEditOffsetInputs(colors=[]){
  const count=Number(window.e_offsetCount?.value||0);
  if(!window.e_offsetInputsContainer) return;
  e_offsetInputsContainer.innerHTML='';
  for(let i=1;i<=count;i++) e_offsetInputsContainer.innerHTML += `<div><label>اللون ${i}</label><input class="edit-offset-color-input" value="${esc(colors[i-1]||'')}" placeholder="اسم اللون"></div>`;
}
function parseEditPrintDetails(text='', printType=''){
  const raw=String(text||'').trim();
  if(printType==='سلك سكرين') return { silk: raw.replace(/^سلك\s*سكرين\s*:\s*/,'').trim(), offsetCount:0, offsetColors:[] };
  const m=raw.match(/أوفست\s*\((\d+)\s*لون\)\s*:\s*(.*)$/);
  if(m){ const colors=String(m[2]||'').split('+').map(x=>x.trim()).filter(Boolean); return { silk:'', offsetCount:Number(m[1]||colors.length||0), offsetColors:colors }; }
  return { silk:'', offsetCount:0, offsetColors:[] };
}
function syncEditPrintDetailsUI(){
  const printType=String(window.e_printType?.value||'سادة').trim();
  if(window.e_silkBox) e_silkBox.style.display=printType==='سلك سكرين'?'block':'none';
  if(window.e_offsetBox) e_offsetBox.style.display=printType==='أوفست'?'block':'none';
  if(window.e_offsetInputsContainer) e_offsetInputsContainer.style.display=printType==='أوفست'?'grid':'none';
  if(printType!=='أوفست' && window.e_offsetInputsContainer) e_offsetInputsContainer.innerHTML='';
}
function hydrateEditPrintDetails(order){
  const printType=String(order?.printType||window.e_printType?.value||'سادة').trim();
  const parsed=parseEditPrintDetails(order?.colorSpecs||'', printType);
  if(window.e_silkColor) e_silkColor.value=parsed.silk||'';
  if(window.e_offsetCount){
    const count=printType==='أوفست' ? Math.max(0, Number(parsed.offsetCount||parsed.offsetColors.length||0)) : 0;
    e_offsetCount.value=String(Math.min(4,count));
    renderEditOffsetInputs(parsed.offsetColors||[]);
  }
  syncEditPrintDetailsUI();
}
function composeEditColorSpecs(){
  let printType=String(window.e_printType?.value||'سادة').trim();
  if(isCoatedPaperType(e_paper_type.value)&&printType==='سلك سكرين') printType='أوفست';
  if(printType==='سلك سكرين') return 'سلك سكرين: '+String(window.e_silkColor?.value||'').trim();
  if(printType==='أوفست'){
    const colors=Array.from(document.querySelectorAll('.edit-offset-color-input')).map(input=>String(input.value||'').trim()).filter(Boolean);
    const count=Number(window.e_offsetCount?.value||0)||colors.length||1;
    return `أوفست (${count} لون): ${colors.join(' + ')}`;
  }
  return String(window.e_colorSpecs?.value||'سادة').trim()||'سادة';
}
function syncEditMaterialUI(){const type=normalizePaperType(e_paper_type?.value); const coated=isCoatedPaperType(type); if(isFixedWhitePaperType(type)){e_color.value='أبيض';e_color.disabled=true;}else{e_color.disabled=false;} if(e_handle_type){Array.from(e_handle_type.options).forEach(opt=>{opt.disabled=coated&&opt.value==='مجدول';});e_handle_type.value=normalizeHandleType(e_handle_type.value,type);} rebuildEditPrintTypeOptions(); if(window.e_lamination_wrap)e_lamination_wrap.style.display=coated?'block':'none'; const on=!!window.e_has_lamination?.checked; if(window.e_lamination_type_wrap)e_lamination_type_wrap.style.display=(coated&&on)?'block':'none'; syncEditPrintDetailsUI();}
function totalCosts(o){ return ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix','cost_forme','cost_lamination'].reduce((s,k)=>s+(Number(o?.[k])||0),0)}
function isBostaOrder(o){
  return !!String(o?.bosta_delivery_id || o?.bosta_tracking_number || o?.bosta_sent_at || '').trim();
}
function baseOrderShippingCost(o){
  return Math.max(0, Number(o?.shipping_cost || o?.bosta_shipping_fee || o?.bosta_raw_shipping_fee || o?.bosta_estimated_fees || o?.bosta_price_after_vat || 0) || 0);
}
function orderShippingChargeBreakdown(o){
  const status = String(o?.status||'').trim();
  const shipping = baseOrderShippingCost(o);
  if(!isBostaOrder(o)) return { shipping, insurance:0, extraCodFee:0, total:shipping, mode:'manual' };
  const grossSale = Math.max(0, numVal(o?.total_price, 0));
  const productValue = Math.max(0, numVal(o?.bosta_product_value, grossSale));
  const insurance = +(productValue * 0.01).toFixed(2);
  const extraCodFee = grossSale > 0 ? Math.ceil(grossSale / 1000) * 10 : 0;
  const deliveredTotal = +(shipping + insurance + extraCodFee).toFixed(2);
  if(status === 'تم التسليم') return { shipping, insurance, extraCodFee, total: deliveredTotal, mode:'delivered' };
  if(status === 'مرتجع') return { shipping, insurance: 0, extraCodFee: 0, total: shipping, mode:'returned' };
  return { shipping, insurance, extraCodFee, total:0, mode:'pending' };
}
function orderShippingCost(o){ const b=orderShippingChargeBreakdown(o); return b.total || b.shipping || 0; }
function getDisplayedOrderTotal(o){
  const savedTotal = Number(o?.total_price || 0);
  const qty = Number(o?.qty || 0);
  const sellPrice = Number(o?.bag_sell_price || 0);
  if(Number(o?.useReadyStock || 0)===1 && qty>1 && sellPrice>0 && savedTotal <= sellPrice){
    return +(sellPrice * qty).toFixed(2);
  }
  return savedTotal;
}
function getStatusIndex(v){return STATUS_FLOW.indexOf(v);}
function shouldOpenCutPlan(order,newStatus){const currentStatus=order?.status||''; const cutDone=order?.paper_cut_done; if(Number(order?.useReadyStock||0)) return false; const cur=getStatusIndex(currentStatus), next=getStatusIndex(newStatus), cut=getStatusIndex('في القص'); if(next===-1) return false; return !Number(cutDone||0) && cur<cut && next>=cut;}
function calcCutDims(o, mode){const l=Number(o.l)||0,w=Number(o.w)||0,g=Number(o.g)||0; return {cutWidth: +(mode==='pieceByPiece' ? (w+g+2) : ((w*2)+(g*2)+2)).toFixed(2), cutLength:+(l+(g/2)+2).toFixed(2)};}
function getPlateTierFromDims(cutWidth,cutLength){const mxW=Math.max(Number(cutWidth)||0, Number(cutLength)||0), mxH=Math.min(Number(cutWidth)||0, Number(cutLength)||0); if(mxW<=50 && mxH<=35) return {zincCost:60, printCost:150}; if(mxW<=70 && mxH<=50) return {zincCost:150, printCost:200}; if(mxW<=100 && mxH<=70) return {zincCost:250, printCost:300}; return {zincCost:0, printCost:0};}
function getOrderPrintSheetClass(o){
  if(String(o?.printType||'').trim()!=='أوفست') return '';
  const mode=String(o?.last_cut_layout||'pieceByPiece');
  const dims=calcCutDims(o,mode);
  const mxW=Math.max(Number(dims.cutWidth)||0,Number(dims.cutLength)||0);
  const mxH=Math.min(Number(dims.cutWidth)||0,Number(dims.cutLength)||0);
  if(mxW<=50&&mxH<=35) return 'quarter';
  if(mxW<=70&&mxH<=50) return 'half';
  if(mxW<=100&&mxH<=70) return 'full';
  return 'oversize';
}
function getOrderPrintSheetLabel(o){
  return ({quarter:'ربع فرخ',half:'نص فرخ',full:'فرخ كامل',oversize:'أكبر من فرخ'})[getOrderPrintSheetClass(o)]||'';
}
function inferOffsetColorsCount(o){const txt=String(o.colorSpecs||''); const m=txt.match(/(\d+)/); return Math.max(1, Number(m?.[1]||0) || (txt.split('+').filter(Boolean).length||1));}
function buildAutoCosts(o){
  const qty=Number(o.qty)||0;
  const manual=(field, fallback=0)=>{ const raw=o['cost_'+field]; return raw!==undefined && raw!==null && raw!=='' ? Number(raw) : fallback; };
  if(Number(o.useReadyStock||0)===1){
    const purchase=Number(o.ready_stock_purchase_total)||((Number(o.bag_buy_price)||0)*qty);
    const ready={cut:0, print:0, zinc:0, design:0, make:manual('make', purchase), hand:0, paper:0, hand_fix:0, forme:manual('forme',0), lamination:manual('lamination',0)};
    if(String(o.printType||'')==='سلك سكرين'){ ready.design=manual('design', 100); ready.print=manual('print', qty*2); }
    return ready;
  }
  const unitMake=isCoatedPaperType(o.paper_type)?((Number(o.l)<=30&&Number(o.w)<=30)?1.5:2):1.2;const out={cut:manual('cut',50), print:manual('print',0), zinc:manual('zinc',0), design:manual('design',0), make:manual('make',qty*unitMake), hand:manual('hand',0), paper:manual('paper',0), hand_fix:manual('hand_fix',(String(o.handle||'')==='بيد'? +((qty/1000)*100).toFixed(2):0)), forme:manual('forme',0), lamination:manual('lamination',0)};
  if(String(o.printType||'')==='سادة') return out;
  out.design=manual('design',100);
  if(String(o.printType||'')==='سلك سكرين'){ out.print=manual('print', qty*2); return out; }
  if(String(o.printType||'')==='أوفست'){ const mode=String(o.last_cut_layout||'pieceByPiece'); const dims=calcCutDims(o, mode); const tier=getPlateTierFromDims(dims.cutWidth,dims.cutLength); const colorsCount=inferOffsetColorsCount(o); const piecesPerBag=mode==='pieceByPiece'?2:1; const printUnits=Math.ceil((qty*piecesPerBag)/1000); out.print=manual('print', tier.printCost*colorsCount*printUnits); out.zinc=manual('zinc', tier.zincCost*colorsCount); }
  return out;
}
function paymentBadge(o){
  const paid = Number(o?.is_group ? o?.paid_total : o?.paid_amount) || 0;
  const remaining = Number(o?.is_group ? o?.remaining_total : o?.remaining_amount) || 0;
  const type=o?.is_group ? (remaining<=0 ? 'مدفوع كامل' : (paid>0 ? 'عربون' : 'آجل')) : (o.paymentType||'غير محدد');
  let cls='late', icon='⏳';
  if(type==='مدفوع كامل'){ cls='paid'; icon='💵'; }
  else if(type==='عربون'){ cls='deposit'; icon='🪙'; }
  return `<div class="payment-wrap"><div class="tag ${cls}">${icon} ${type}</div><div>${fmtMoney(paid)}</div><small>باقي ${fmtMoney(remaining)}</small></div>`;
}
function normalizedStatusText(status){
  const value=String(status||'').trim();
  if(value==='يوجد مشكلة' || value==='في مشكلة') return 'في مشكله';
  return value || 'أوردر جديد';
}
function allowedStatusesForOrder(o){
  return Number(o?.useReadyStock||0)===1 ? READY_STOCK_ONLY_STATUSES : ALL_ORDER_STATUSES;
}
function orderItems(o){
  return o?.is_group ? (o.group_items||[]) : [o];
}
function selectionKeyForOrder(o){
  return o?.is_group ? `group:${String(o.group_code||'').trim()}` : `order:${Number(o.id)}`;
}
function rowDomIdFromSelectionKey(key){
  return `row-${String(key||'').replace(/[^a-zA-Z0-9_-]/g,'_')}`;
}
function rowDomIdForOrder(o){
  return rowDomIdFromSelectionKey(selectionKeyForOrder(o));
}
function safeGroupKey(groupCode){
  return String(groupCode||'').replace(/[^a-zA-Z0-9_-]/g,'_');
}
function jsAttrString(value){
  return JSON.stringify(String(value ?? '')).replace(/"/g,'&quot;');
}
function collectUniqueFiles(items){
  const map=new Map();
  items.forEach(item=>(item.files||[]).forEach(f=>{
    const key=String(f.filepath||f.filename||f.url||f.originalname||f.id||Math.random()).trim();
    if(!map.has(key)) map.set(key,f);
  }));
  return [...map.values()];
}
function highestPriority(items){
  const rank={عادي:1,مستعجل:2,عاجل:3};
  return [...items].sort((a,b)=>(rank[normalizePriority(b.priority)]||0)-(rank[normalizePriority(a.priority)]||0))[0]?.priority || 'عادي';
}
function sourceTypeForDisplay(o){
  const items=orderItems(o);
  const hasReady=items.some(item=>Number(item.useReadyStock||0)===1);
  const hasNew=items.some(item=>Number(item.useReadyStock||0)!==1);
  if(hasReady && hasNew) return 'mixed';
  return hasReady ? 'ready' : 'new';
}
function buildDisplayOrders(rows){
  const list=[];
  const groups=new Map();
  rows.forEach(raw=>{
    const row={...raw,status:normalizedStatusText(raw.status)};
    const groupCode=String(row.group_code||'').trim();
    if(groupCode && Number(row.item_count||1)>1){
      if(!groups.has(groupCode)){
        const entry={
          is_group:true,
          id:row.id,
          order_sort_id:Number(row.order_sort_id||row.id||0),
          order_display_no:Number(row.order_display_no||0),
          group_code:groupCode,
          group_items:[],
          custName:row.custName||'',
          custPhone:row.custPhone||'',
          custAddress:row.custAddress||'',
          created_by:row.created_by||'',
          order_page_name:row.order_page_name||'',
          orderDate:row.orderDate||'',
          due_date:row.due_date||'',
          priority:row.priority||'عادي',
          order_display_no:Number(row.order_display_no||0),
          display_order_no:Number(row.order_display_no||0),
          order_sort_id:Number(row.order_sort_id||row.id||0)
        };
        groups.set(groupCode, entry);
        list.push(entry);
      }
      groups.get(groupCode).group_items.push(row);
      return;
    }
    row.order_sort_id=Number(row.order_sort_id||row.id||0);
    row.order_display_no=Number(row.order_display_no||0);
    list.push(row);
  });
  // العرض في الجدول من الأحدث للأقدم، لكن رقم الأوردر المعروض محفوظ من السيرفر بحيث الأقدم = #1.
  list.sort((a,b)=>Number(b.order_sort_id||b.id||0)-Number(a.order_sort_id||a.id||0));
  return list.map(entry=>{
    if(!entry.is_group) return entry;
    entry.group_items=[...entry.group_items].sort((a,b)=>Number(a.item_no||1)-Number(b.item_no||1));
    entry.order_display_no=Number(entry.group_items[0]?.order_display_no||entry.order_display_no||0);
    entry.order_sort_id=Number(entry.group_items[0]?.order_sort_id||entry.order_sort_id||entry.id||0);
    entry.files=collectUniqueFiles(entry.group_items);
    entry.qty_total=entry.group_items.reduce((sum,item)=>sum+(Number(item.qty)||0),0);
    const finance=wholeGroupFinanceAmounts(entry.group_items);
    entry.total_price_display=finance?finance.total:entry.group_items.reduce((sum,item)=>sum+getDisplayedOrderTotal(item),0);
    entry.shipping_total=entry.group_items.reduce((sum,item)=>sum+orderShippingCost(item),0);
    entry.cost_total=entry.group_items.reduce((sum,item)=>sum+totalCosts(item),0);
    entry.paid_total=finance?finance.paid:entry.group_items.reduce((sum,item)=>sum+(Number(item.paid_amount)||0),0);
    entry.remaining_total=finance?finance.remaining:entry.group_items.reduce((sum,item)=>sum+(Number(item.remaining_amount)||0),0);
    entry.priority=highestPriority(entry.group_items);
    entry.status_list=[...new Set(entry.group_items.map(item=>normalizedStatusText(item.status)))];
    return entry;
  });
}
function statusBadgeOnly(status,label=''){
  const s=normalizedStatusText(status);
  const meta=STATUS_META[s]||{emoji:'📌',bg:'rgba(148,163,184,.12)',border:'#64748b',color:'#e2e8f0'};
  return `<div class="status-badge" style="background:${meta.bg};border-color:${meta.border};color:${meta.color}">${meta.emoji} ${s}${label?` - ${label}`:''}</div>`;
}
function statusOptions(o){
  const current=normalizedStatusText(o.status||'أوردر جديد');
  let list=[...allowedStatusesForOrder(o)];
  if(current && !list.includes(current)) list=[current,...list];
  if(user.username!=='admin'&&user.role!=='super_admin' && !Number(user.perm_change_status||0)) return `<div class="status-wrap"><div class="status-select">${current}</div></div>`;
  return `<div class="status-wrap"><select class="status-select" onchange="updateStatus(${o.id},this.value)">${list.map(s=>`<option value="${s}" ${current===s?'selected':''}>${(STATUS_META[s]?.emoji||'📌')} ${s}</option>`).join('')}</select></div>`;
}
function allowedStatusesForGroup(items){
  if(!items.length) return [...ALL_ORDER_STATUSES];
  const union=[];
  items.forEach(item=>{
    allowedStatusesForOrder(item).forEach(status=>{ if(!union.includes(status)) union.push(status); });
    const current=normalizedStatusText(item.status);
    if(current && !union.includes(current)) union.unshift(current);
  });
  return union.length ? union : [...ALL_ORDER_STATUSES];
}
function groupStatusOptions(o){
  const items=orderItems(o);
  const unique=[...new Set(items.map(item=>normalizedStatusText(item.status)))];
  const mixed=unique.length>1;
  const current=mixed ? '' : normalizedStatusText(unique[0]||'أوردر جديد');
  let list=allowedStatusesForGroup(items);
  if(current && !list.includes(current)) list=[current,...list];
  if(user.username!=='admin'&&user.role!=='super_admin' && !Number(user.perm_change_status||0)) return `<div class="status-wrap"><div class="status-select">${current || 'حالات مختلفة'}</div></div>`;
  return `<div class="status-wrap"><select class="status-select" onchange="updateGroupStatus(${jsAttrString(String(o.group_code||''))},this.value,this)">${mixed?`<option value="" selected disabled>📌 تغيير حالة الأوردر</option>`:''}${list.map(s=>`<option value="${s}" ${(!mixed && current===s)?'selected':''}>${(STATUS_META[s]?.emoji||'📌')} ${s}</option>`).join('')}</select></div>`;
}
function fileDisplayName(f){
  const rawName=String(f?.originalname||f?.filename||'design').trim()||'design';
  return /\.pdf$/i.test(rawName) ? rawName : `${rawName}.pdf`;
}
async function fetchOrderFileBlob(fileId, mode='download'){
  const endpoint = mode==='open' ? 'open' : 'download';
  const res = await fetch(`/order-file/${Number(fileId)}/${endpoint}?ts=${Date.now()}`, {
    headers:{ Authorization:'Bearer '+token, Accept:'application/pdf' },
    cache:'no-store'
  });
  if(res.status===401){ localStorage.clear(); location.href='login.html'; throw new Error('unauthorized'); }
  if(!res.ok){
    let msg = mode==='open' ? 'تعذر فتح الملف' : 'تعذر تحميل الملف';
    try{ const data=await res.json(); if(data?.error) msg=data.error; }catch(_){ }
    throw new Error(msg);
  }
  const blob = await res.blob();
  if(!blob || !blob.size) throw new Error('الملف فارغ أو غير متاح');
  return blob.type==='application/pdf' ? blob : new Blob([blob], { type:'application/pdf' });
}
async function downloadOrderFile(fileId, fileName){
  try{
    const pdfBlob = await fetchOrderFileBlob(fileId, 'download');
    const url = URL.createObjectURL(pdfBlob);
    const a = document.createElement('a');
    a.href = url;
    a.download = fileDisplayName({ originalname:fileName || 'design.pdf' });
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(()=>URL.revokeObjectURL(url), 1500);
  }catch(e){ alert(e.message || 'تعذر تحميل الملف'); }
}
async function openOrderFile(fileId){
  try{
    const pdfBlob = await fetchOrderFileBlob(fileId, 'open');
    const url = URL.createObjectURL(pdfBlob);
    const win = window.open(url, '_blank', 'noopener');
    if(!win) alert('المتصفح منع فتح الملف. اسمح بفتح Pop-up أو استخدم تحميل.');
    setTimeout(()=>URL.revokeObjectURL(url), 60000);
  }catch(e){ alert(e.message || 'تعذر فتح الملف'); }
}
function orderFileActionsHtml(f){
  const id=Number(f.id);
  const name=esc(fileDisplayName(f));
  return `<div class="file-link-block"><div>📎 ${name}</div><div class="file-actions"><button class="btn dark" type="button" onclick="openOrderFile(${id})">فتح</button><button class="btn cyan" type="button" onclick="downloadOrderFile(${id}, ${jsAttrString(fileDisplayName(f))})">تحميل</button></div></div>`;
}
function filesHtml(o){
  const files=(o.files||[]);
  return files.length ? files.map(f=>orderFileActionsHtml(f)).join('') : '-';
}
function traderInfoHtml(o){
  const scope = String(o?.order_scope || '').trim();
  const traderName = String(o?.trader_name || '').trim();
  const batchName = String(o?.trader_batch_name || '').trim();
  if(scope !== 'trader' && !traderName && !batchName) return '';
  return `<br><small style="color:#fbbf24">🏪 تاجر: ${esc(traderName || '-')}</small>${batchName ? `<br><small style="color:#fbbf24">طلبية: ${esc(batchName)}</small>` : ''}`;
}
function orderEntryMetaHtml(o){
  const createdBy = String(o?.created_by || '').trim();
  const pageName = String(o?.order_page_name || '').trim();
  const bits = [];
  if(createdBy) bits.push(`👤 أدخل بواسطة: ${esc(createdBy)}`);
  if(pageName) bits.push(`📣 اسم البيدج: ${esc(pageName)}`);
  return bits.length ? `<br><small style="color:#93c5fd">${bits.join(' | ')}</small>` : '';
}
function readyStockShortageRowsForOrder(o){
  return orderItems(o).filter(item=>Number(item?.useReadyStock||0)===1 && !Number(item?.ready_stock_deducted||0) && (Number(item?.ready_stock_missing_qty||0)||0)>0).map((item,idx)=>{
    const required=Number(item.ready_stock_required_qty||item.qty||0)||0;
    const available=Number(item.ready_stock_available_qty||0)||0;
    const missing=Number(item.ready_stock_missing_qty||0)||Math.max(0, required-available);
    const size=[Number(item.l||0),Number(item.w||0),Number(item.g||0)].filter(v=>v>0).join('×') || '-';
    const itemNo=Number(item.item_no||idx+1)||idx+1;
    return { id:Number(item.id||0), item_no:itemNo, size, color:String(item.color||'-'), handle:String(item.handle||'-'), required, available, missing, status:String(item.status||''), message:String(item.ready_stock_stock_message||'') };
  });
}
function getReadyShortageContext(kind,key){
  const k=String(kind||'').trim();
  if(k==='group'){
    const items=getGroupItemsByCode(key);
    const group=getGroupDisplayOrderByCode(key) || {is_group:true,group_code:key,group_items:items};
    return { title:`الأوردر المجمع ${String(key||'')}`, rows:readyStockShortageRowsForOrder(group), items };
  }
  const id=Number(key||0);
  const order=ordersData.find(x=>Number(x.id)===id) || {};
  const label=order?.order_display_no ? `#${order.order_display_no}` : `#${id}`;
  return { title:`الأوردر ${label}`, rows:readyStockShortageRowsForOrder(order), items:[order] };
}
function openReadyStockShortage(kind,key){
  const box=document.getElementById('readyStockShortageBox');
  if(!box) return;
  const ctx=getReadyShortageContext(kind,key);
  const rows=ctx.rows||[];
  const totalMissing=rows.reduce((sum,r)=>sum+(Number(r.missing)||0),0);
  const totalRequired=rows.reduce((sum,r)=>sum+(Number(r.required)||0),0);
  const totalAvailable=rows.reduce((sum,r)=>sum+(Number(r.available)||0),0);
  if(!rows.length){
    box.innerHTML=`<div class="shortage-modal-summary"><b>${esc(ctx.title)}</b><br>لا يوجد عجز ظاهر حاليًا في أصناف مخزن الشنط الجاهزة. اعمل تحديث للصفحة لو كنت لسه معدل المخزون.</div>`;
  }else{
    box.innerHTML=`<div class="shortage-modal-summary"><b>${esc(ctx.title)}</b><br>إجمالي المطلوب للأصناف الناقصة: <b class="shortage-required">${totalRequired.toLocaleString('en-US')}</b> | إجمالي المتاح حسب المخزن: <b class="shortage-available">${totalAvailable.toLocaleString('en-US')}</b> | إجمالي العجز: <b class="shortage-num">${totalMissing.toLocaleString('en-US')}</b><br><small>الأرقام هنا بتوضح الأصناف اللي مانقدرش نسحبها من مخزن الشنط الجاهزة قبل تجهيز الأوردر للشحن.</small></div>
    <table class="shortage-table"><thead><tr><th>الصنف</th><th>المقاس</th><th>اللون</th><th>اليد</th><th>المطلوب</th><th>المتاح</th><th>العجز</th></tr></thead><tbody>${rows.map(r=>`<tr><td>صنف ${esc(r.item_no)}</td><td>${esc(r.size)}</td><td>${esc(r.color)}</td><td>${esc(r.handle)}</td><td class="shortage-required">${Number(r.required||0).toLocaleString('en-US')}</td><td class="shortage-available">${Number(r.available||0).toLocaleString('en-US')}</td><td class="shortage-num">${Number(r.missing||0).toLocaleString('en-US')}</td></tr>`).join('')}</tbody></table>`;
  }
  document.getElementById('readyStockShortageModal').style.display='block';
}
function readyStockShortageHtml(o){
  if(Number(o?.useReadyStock||0)!==1) return '';
  const status=String(o?.ready_stock_stock_status||'').trim();
  const msg=String(o?.ready_stock_stock_message||'').trim();
  const missing=Number(o?.ready_stock_missing_qty||0)||0;
  const available=Number(o?.ready_stock_available_qty||0)||0;
  const required=Number(o?.ready_stock_required_qty||o?.qty||0)||0;
  if(status==='deducted' || Number(o?.ready_stock_deducted||0)===1) return '<div class="ready-stock-ok">✅ تم السحب من المخزن</div>';
  if(missing>0) return `<button class="ready-stock-missing ready-stock-click" type="button" onclick="openReadyStockShortage('order',${Number(o?.id||0)})">⚠️ ناقص: ${missing} | المطلوب ${required} / المتاح ${available}</button>`;
  if(status==='available' || available>=required) return `<div class="ready-stock-pending">🟡 متاح ولم يُسحب بعد: ${available}</div>`;
  return msg ? `<div class="ready-stock-missing">⚠️ ${esc(msg)}</div>` : '';
}
function readyStockLabel(o){
  if(o?.is_group){
    const items=orderItems(o);
    const readyItems=items.filter(item=>Number(item.useReadyStock||0)===1);
    const readyCount=readyItems.length;
    const newCount=items.length-readyCount;
    const tags=[];
    if(readyCount) tags.push(`<div class="ready-stock-tag">📦 من المخزن: ${readyCount}</div>`);
    if(newCount) tags.push(`<div class="order-source-empty">🛠️ تصنيع جديد: ${newCount}</div>`);
    const missingTotal=readyItems.reduce((sum,item)=>sum+(Number(item.ready_stock_missing_qty||0)||0),0);
    const pendingCount=readyItems.filter(item=>!Number(item.ready_stock_deducted||0) && !(Number(item.ready_stock_missing_qty||0)>0)).length;
    const deductedCount=readyItems.filter(item=>Number(item.ready_stock_deducted||0)===1).length;
    if(missingTotal>0) tags.push(`<button class="ready-stock-missing ready-stock-click" type="button" onclick="openReadyStockShortage('group',${jsAttrString(o.group_code)})">⚠️ ناقص بالمخزن: ${missingTotal}</button>`);
    else if(pendingCount>0) tags.push(`<div class="ready-stock-pending">🟡 متاح ولم يُسحب: ${pendingCount}</div>`);
    else if(deductedCount>0) tags.push(`<div class="ready-stock-ok">✅ تم السحب: ${deductedCount}</div>`);
    tags.push(`<div class="ready-stock-tag" style="margin-top:6px;background:rgba(74,222,128,.12);border-color:rgba(74,222,128,.45);color:#86efac">🧩 ${o.group_code}</div>`);
    return tags.join('');
  }
  const base = Number(o.useReadyStock||0)===1 ? '<div class="ready-stock-tag">📦 مخزن الشنط الجاهزة</div>' : '<div class="order-source-empty">تصنيع جديد</div>';
  return base + readyStockShortageHtml(o) + groupBadge(o);
}
function groupStatusHtml(o){
  const items=orderItems(o);
  const unique=[...new Set(items.map(item=>normalizedStatusText(item.status)))];
  if(unique.length===1) return `<div class="status-wrap">${statusBadgeOnly(unique[0],'كل الأصناف')}</div>`;
  return `<div class="group-status-list">${items.map(item=>statusBadgeOnly(item.status,`صنف ${Number(item.item_no||1)}`)).join('')}</div>`;
}

function normalizePriority(v){ return ['عادي','مستعجل','عاجل'].includes(String(v||'').trim()) ? String(v||'').trim() : 'عادي'; }
function isClosedStatus(status){ return ['تم التسليم','مرتجع'].includes(String(status||'').trim()); }
function isOverdueOrder(o){ return !!(o?.due_date && !isClosedStatus(o.status) && String(o.due_date) < new Date().toISOString().slice(0,10)); }
function bostaInfoHtml(o){
  if(!isBostaOrder(o)) return '';
  const tracking=String(o?.bosta_tracking_number||'').trim();
  const status = String(o?.status||'').trim();
  const shipping = orderShippingChargeBreakdown(o);
  const baseShipping = baseOrderShippingCost(o);
  const grossSale = Math.max(0, numVal(o?.total_price, 0));
  const productValue = Math.max(0, numVal(o?.bosta_product_value, grossSale));
  const insurance = +(productValue * 0.01).toFixed(2);
  const extraCodFee = grossSale > 0 ? Math.ceil(grossSale / 1000) * 10 : 0;
  const bits=[];
  bits.push('تم الإرسال إلى بوسطة');
  if(tracking) bits.push(`Tracking: ${tracking}`);
  if(Number(o?.bosta_product_value||0)>0) bits.push(`قيمة المنتج: ${fmtMoney(o.bosta_product_value)}`);
  bits.push(`فتح الشحنة: ${Number(o?.bosta_allow_open||0)===1 ? 'مسموح' : 'غير مسموح'}`);
  if(Number(baseShipping||0)>0) bits.push(`سعر الشحن الأساسي: ${fmtMoney(baseShipping)}`);
  if(Number(insurance||0)>0) bits.push(`1% قيمة المنتج: ${fmtMoney(insurance)}`);
  if(Number(extraCodFee||0)>0) bits.push(`10 ج لكل 1000 من البيع: ${fmtMoney(extraCodFee)}`);
  if(status === 'تم التسليم' && Number(shipping.total||0)>0) bits.push(`المبلغ المسجل على الأوردر بعد التسليم: ${fmtMoney(shipping.total)}`);
  if(status === 'مرتجع' && Number(shipping.total||0)>0) bits.push(`المبلغ المسجل على الأوردر عند المرتجع: ${fmtMoney(shipping.total)}`);
  if(!['تم التسليم','مرتجع'].includes(status)) bits.push('الخصم الفعلي لا يتم إلا عند تم التسليم أو مرتجع.');
  if(Number(o?.bosta_estimated_fees||0)>0) bits.push(`تقدير مستحقات بوسطة: ${fmtMoney(o.bosta_estimated_fees)}`);
  if(Number(o?.bosta_material_fee||0)>0) bits.push(`خامة بوسطة: ${fmtMoney(o.bosta_material_fee)}`);
  if(Number(o?.bosta_open_package_fees||0)>0) bits.push(`رسوم فتح الشحنة: ${fmtMoney(o.bosta_open_package_fees)}`);
  if(Number(o?.bosta_price_before_vat||0)>0) bits.push(`قبل الضريبة: ${fmtMoney(o.bosta_price_before_vat)}`);
  if(Number(o?.bosta_vat_amount||0)>0) bits.push(`الضريبة: ${fmtMoney(o.bosta_vat_amount)}`);
  if(Number(o?.bosta_price_after_vat||0)>0) bits.push(`بعد الضريبة: ${fmtMoney(o.bosta_price_after_vat)}`);
  return `<button class="btn dark" style="margin-top:8px;width:100%;justify-content:center" onclick="toggleOrderShippingDetails(${Number(o.id)})">📦 تفاصيل الشحن</button>${bits.length?`<div class="bosta-estimate" id="bosta-details-${Number(o.id)}" style="display:none">${bits.map(x=>`<small>${esc(x)}</small>`).join('')}</div>`:''}`;
}
function bostaActionButton(o){ const canSend = ((user.username==='admin'||user.role==='super_admin')||Number(user.perm_edit_order||0)); if(!canSend) return ''; if(String(o?.status||'').trim()!=='جاهز للشحن') return ''; if(isBostaOrder(o)) return ''; return `<button class="btn green" onclick="openBostaModal(${o.id})">🚚 إرسال إلى بوسطة</button>`; }
function groupRepresentativeItem(o){
  const items=orderItems(o);
  return items.find(item=>isBostaOrder(item) || String(item.bosta_business_reference||'').trim()) || items[0] || null;
}
function groupCanSendToBosta(o){
  const canSend = ((user.username==='admin'||user.role==='super_admin')||Number(user.perm_edit_order||0));
  if(!canSend) return false;
  const items=orderItems(o);
  if(!items.length) return false;
  const statuses=[...new Set(items.map(item=>normalizedStatusText(item.status)))];
  if(statuses.length!==1 || statuses[0]!=='جاهز للشحن') return false;
  return !items.some(item=>isBostaOrder(item));
}
function groupBostaActionButton(o){
  if(!groupCanSendToBosta(o)) return '';
  return `<button class="btn green" onclick="openGroupBostaModal(${jsAttrString(o.group_code)})">🚚 إرسال إلى بوسطة</button>`;
}
function priorityBadge(o){ const p=normalizePriority(o?.priority||'عادي'); const color=p==='عاجل'?'#fecaca':(p==='مستعجل'?'#fde68a':'#bfdbfe'); const text=p==='عاجل'?'#7f1d1d':(p==='مستعجل'?'#78350f':'#1d4ed8'); return `<div class="tag" style="background:${color};color:${text};border:none">${p}</div>`; }
function groupBadge(o){ if(!String(o?.group_code||'').trim()) return ''; return `<div class="ready-stock-tag" style="margin-top:6px;background:rgba(74,222,128,.12);border-color:rgba(74,222,128,.45);color:#86efac">🧩 ${o.group_code} | صنف ${Number(o.item_no||1)}/${Number(o.item_count||1)}</div>`; }
function dateInfo(o){ return `${o.orderDate||''}${o.due_date?`<br><small style="color:var(--muted)">تسليم: ${o.due_date}</small>`:''}${isOverdueOrder(o)?`<br><span class="tag late">متأخر</span>`:''}`; }
function formatPaperGramText(v){
  const gram=Number(v||0);
  return gram>0 ? `${gram} جم` : '';
}
function formatOrderSizeWithGram(item){
  const size=`${item.l}×${item.w}×${item.g}`;
  const gram=formatPaperGramText(item.paperGrammage);
  return gram ? `${size} - ${gram}` : size;
}
function singleOrderSizeHtml(o){
  return `${formatOrderSizeWithGram(o)}<br><small style="color:var(--muted)">${esc(normalizePaperType(o.paper_type))} | ${esc(o.color||'')}/${esc(o.handle||'')}${o.handle_type?`/${esc(o.handle_type)}`:''}${Number(o.has_lamination||0)?` | سلوفان ${esc(o.lamination_type||'')}`:''}</small>`;
}
function orderNumberHtml(o,index){
  const displayNo=Number(o?.order_display_no||index||0)||'-';
  if(o?.is_group){
    const count=orderItems(o).length || Number(o.item_count||1) || 1;
    return `<b>#${displayNo}</b>${count>1?`<br><small style="color:var(--muted)">${count} أصناف داخل نفس الأوردر</small>`:''}`;
  }
  return `<b>#${displayNo}</b>`;
}
function groupCompactPreview(items, type='sizes'){
  const total=items.length;
  const preview=items.slice(0,3).map(item=>{
    if(type==='print') return `صنف ${Number(item.item_no||1)}: ${esc(item.printType||'-')}`;
    return `صنف ${Number(item.item_no||1)}: ${esc(formatOrderSizeWithGram(item))}`;
  }).join('<br>');
  const remain=total>3 ? `<div class="summary-line">+ ${total-3} صنف آخر</div>` : '';
  return `${preview ? `<div class="summary-preview">${preview}</div>` : ''}${remain}`;
}
function groupSizesHtml(o){
  const items=orderItems(o);
  if(items.length<=4){
    return `<div class="group-inline-list">${items.map(item=>`<div><b>صنف ${Number(item.item_no||1)}:</b> ${formatOrderSizeWithGram(item)} <small>${esc(normalizePaperType(item.paper_type))} | ${esc(item.color||'')}/${esc(item.handle||'')}${item.handle_type?`/${esc(item.handle_type)}`:''}${Number(item.has_lamination||0)?` | سلوفان ${esc(item.lamination_type||'')}`:''}</small></div>`).join('')}</div>`;
  }
  const totalQty=items.reduce((sum,item)=>sum+(Number(item.qty)||0),0);
  return `<div class="group-compact-summary"><div class="summary-title">🧩 ${items.length} صنف</div><div class="summary-line">إجمالي الكمية: ${Number(totalQty||0).toLocaleString('en-US')}</div>${groupCompactPreview(items,'sizes')}<button class="mini-link" type="button" onclick="openGroupItemsSummary(${jsAttrString(o.group_code)})">عرض كل الأصناف</button></div>`;
}
function groupPrintHtml(o){
  const items=orderItems(o);
  if(items.length<=4){
    return `<div class="group-inline-list">${items.map(item=>{const sheet=getOrderPrintSheetLabel(item); return `<div><b>صنف ${Number(item.item_no||1)}:</b> ${esc(item.printType||'')}${sheet?` <small style="color:#fbbf24">(${esc(sheet)})</small>`:''}<br><small>${esc(item.colorSpecs||'')}</small></div>`;}).join('')}</div>`;
  }
  const counts={};
  items.forEach(item=>{ const key=String(item.printType||'بدون').trim()||'بدون'; counts[key]=(counts[key]||0)+1; });
  const line=Object.entries(counts).map(([k,v])=>`${esc(k)}: ${v}`).join(' | ');
  const sheetCounts={};
  items.forEach(item=>{const label=getOrderPrintSheetLabel(item); if(label) sheetCounts[label]=(sheetCounts[label]||0)+1;});
  const sheetLine=Object.entries(sheetCounts).map(([k,v])=>`${esc(k)}: ${v}`).join(' | ');
  return `<div class="group-compact-summary"><div class="summary-title">🖨️ طباعة الأصناف</div><div class="summary-line">${line}</div>${sheetLine?`<div class="summary-line" style="color:#fbbf24">${sheetLine}</div>`:''}${groupCompactPreview(items,'print')}<button class="mini-link" type="button" onclick="openGroupItemsSummary(${jsAttrString(o.group_code)})">عرض التفاصيل</button></div>`;
}
function openGroupItemsSummary(groupCode){
  const items=getGroupItemsByCode(groupCode);
  if(!items.length){ alert('الأوردر غير موجود'); return; }
  const title=document.getElementById('groupActionTitle');
  const hint=document.getElementById('groupActionHint');
  const box=document.getElementById('groupActionBox');
  const addBtn=document.getElementById('groupAddItemBtn');
  if(addBtn) addBtn.style.display='none';
  if(title) title.textContent='🧩 تفاصيل أصناف الأوردر';
  if(hint) hint.textContent='الأوردرات متعددة الأصناف تظهر مختصرة في القائمة علشان الصفحة تفضل خفيفة. التفاصيل الكاملة هنا.';
  if(box){
    box.innerHTML=`<table class="group-items-table"><thead><tr><th>الصنف</th><th>المقاس</th><th>الكمية</th><th>الورق/اللون/اليد</th><th>الطباعة</th><th>الحالة</th><th>البيع</th><th>التكلفة</th></tr></thead><tbody>${items.map(item=>`<tr><td>صنف ${Number(item.item_no||1)}</td><td>${esc(formatOrderSizeWithGram(item))}</td><td>${Number(item.qty||0).toLocaleString('en-US')}</td><td>${esc(normalizePaperType(item.paper_type))}<br><small>${esc(item.color||'')}/${esc(item.handle||'')}${item.handle_type?`/${esc(item.handle_type)}`:''}</small></td><td>${esc(item.printType||'-')}<br><small>${esc(item.colorSpecs||'')}</small>${getOrderPrintSheetLabel(item)?`<br><small style="color:#fbbf24">${esc(getOrderPrintSheetLabel(item))}</small>`:''}</td><td>${statusBadgeOnly(item.status)}</td><td>${fmtMoney(Number(item.total_price||0))}</td><td>${fmtMoney(totalCosts(item))}</td></tr>`).join('')}</tbody></table>`;
  }
  closeAllActionMenus();
  document.getElementById('groupActionModal').style.display='block';
}
function orderActionButtonsHtml(o){
  const canOpenCalculator = ((user.username==='admin'||user.role==='super_admin') || Number(user.perm_calculator||0));
  return `
    <button class="btn cyan" onclick="openDirectionModal(${o.id})">🧭 توجيه</button>
    <button class="btn cyan" onclick="openCosts(${o.id})">💰 التكاليف</button>
    <button class="btn cyan" onclick="openOrderFinance(${o.id})">📊 حسابات الأوردر</button>
    ${canOpenCalculator?`<button class="btn cyan" onclick="goToCalculatorFromOrder(${o.id})">🧮 احسب أوردر</button>`:''}
    ${Number(o.useReadyStock||0)?'':`<button class="btn cyan" onclick="showDetails(${o.id})">📐 تفاصيل</button>`}
    ${((user.username==='admin'||user.role==='super_admin')||Number(user.perm_edit_order||0))?`<button class="btn cyan" onclick="openEdit(${o.id})">✏️ تعديل</button>`:''}
    ${bostaActionButton(o)}
    ${((user.username==='admin'||user.role==='super_admin')||Number(user.perm_edit_order||0))?`<button class="btn green" onclick="openPaymentsModal(${o.id})">💵 دفعات</button>`:''}
    <button class="btn cyan" onclick="showHistory(${o.id})">📜 التاريخ</button>
    ${((user.username==='admin'||user.role==='super_admin')||Number(user.perm_delete_order||0))?`<button class="btn red" onclick="delOrder(${o.id})">🗑️ حذف</button>`:''}
  `;
}
function groupActionButtonsHtml(o){
  const canOpenCalculator = ((user.username==='admin'||user.role==='super_admin') || Number(user.perm_calculator||0));
  return `
    <button class="btn cyan" onclick="openGroupActionPicker(${jsAttrString(o.group_code)},'direction')">🧭 توجيه</button>
    <button class="btn cyan" onclick="openGroupActionPicker(${jsAttrString(o.group_code)},'costs')">💰 التكاليف</button>
    ${canOpenCalculator?`<button class="btn cyan" onclick="openGroupActionPicker(${jsAttrString(o.group_code)},'calculator')">🧮 احسب أوردر</button>`:''}
    <button class="btn cyan" onclick="openGroupActionPicker(${jsAttrString(o.group_code)},'details')">📐 تفاصيل</button>
    ${((user.username==='admin'||user.role==='super_admin')||Number(user.perm_edit_order||0))?`<button class="btn cyan" onclick="openGroupActionPicker(${jsAttrString(o.group_code)},'edit')">✏️ تعديل</button>`:''}
    ${groupBostaActionButton(o)}
    ${((user.username==='admin'||user.role==='super_admin')||Number(user.perm_edit_order||0))?`<button class="btn green" onclick="openGroupActionPicker(${jsAttrString(o.group_code)},'payments')">💵 دفعات</button>`:''}
    <button class="btn cyan" onclick="showGroupHistory(${jsAttrString(o.group_code)})">📜 التاريخ</button>
    ${((user.username==='admin'||user.role==='super_admin')||Number(user.perm_delete_order||0))?`<button class="btn red" onclick="delGroupOrder(${jsAttrString(o.group_code)})">🗑️ حذف</button>`:''}
  `;
}
const GROUP_ACTIONS={
  direction:{title:'🧭 توجيه',run:id=>openDirectionModal(id)},
  costs:{title:'💰 التكاليف',run:id=>openCosts(id)},
  calculator:{title:'🧮 احسب أوردر',run:id=>goToCalculatorFromOrder(id)},
  details:{title:'📐 تفاصيل',run:id=>showDetails(id)},
  edit:{title:'✏️ تعديل',run:id=>openEdit(id)},
  payments:{title:'💵 دفعات',run:id=>openPaymentsModal(id)}
};
function groupItemLabel(item){
  const bits=[`صنف ${Number(item.item_no||1)}`];
  const sizeParts=[Number(item.l||0),Number(item.w||0),Number(item.g||0)];
  if(sizeParts.some(v=>v>0)) bits.push(`${sizeParts[0]}×${sizeParts[1]}×${sizeParts[2]}`);
  if(String(item.color||'').trim()) bits.push(String(item.color).trim());
  if(String(item.printType||'').trim()) bits.push(String(item.printType).trim());
  if(Number(item.qty||0)>0) bits.push(`${Number(item.qty||0)} حبة`);
  return bits.filter(Boolean).join(' - ');
}
function runGroupActionOnItem(actionKey,itemId){
  const action=GROUP_ACTIONS[actionKey];
  if(!action){ alert('الإجراء غير متاح'); return; }
  closeModal('groupActionModal');
  closeAllActionMenus();
  action.run(Number(itemId));
}
function openGroupActionPicker(groupCode,actionKey){
  const items=getGroupItemsByCode(groupCode);
  const action=GROUP_ACTIONS[actionKey];
  if(!items.length || !action){ alert('الأوردر غير موجود'); return; }
  window.currentGroupActionCode=String(groupCode||'').trim();
  const addBtn=document.getElementById('groupAddItemBtn');
  if(addBtn) addBtn.style.display = actionKey==='edit' ? 'inline-flex' : 'none';
  if(items.length===1 && actionKey!=='edit'){ runGroupActionOnItem(actionKey, items[0].id); return; }
  const title=document.getElementById('groupActionTitle');
  const hint=document.getElementById('groupActionHint');
  const box=document.getElementById('groupActionBox');
  if(title) title.textContent=`${action.title} - اختر الصنف`;
  if(hint) hint.textContent= actionKey==='edit' ? 'اختَر تعديل لتعديل صنف معين، أو حذف لحذف الصنف فقط، أو إضافة صنف لإدخال صنف جديد لنفس الأوردر.' : 'الأوردر ظاهر كأوردر واحد، ولما الإجراء يخص صنف بعينه اختَر الصنف المطلوب من هنا فقط.';
  if(box){
    const canDeleteItem=((user.username==='admin'||user.role==='super_admin')||Number(user.perm_delete_order||0));
    box.innerHTML=items.map(item=>{
      const mainBtn=`<button class="btn cyan" onclick="runGroupActionOnItem(${jsAttrString(actionKey)},${Number(item.id)})">${action.title}</button>`;
      const deleteBtn=(actionKey==='edit'&&canDeleteItem)?`<button class="btn red" onclick="deleteGroupItem(${Number(item.id)}, ${jsAttrString(groupCode)})">حذف الصنف</button>`:'';
      return `<div style="padding:12px;border-bottom:1px solid #334155;display:flex;gap:10px;flex-wrap:wrap;align-items:center;justify-content:space-between"><div><b>${esc(groupItemLabel(item))}</b><br><small style="color:var(--muted)">${esc(normalizedStatusText(item.status))}</small></div><div style="display:flex;gap:8px;flex-wrap:wrap">${mainBtn}${deleteBtn}</div></div>`;
    }).join('');
  }
  closeAllActionMenus();
  document.getElementById('groupActionModal').style.display='block';
}
async function deleteGroupItem(itemId, groupCode=''){
  const id=Number(itemId||0);
  if(!id) return;
  if(!confirm('حذف الصنف ده فقط من الأوردر؟')) return;
  try{
    await authFetch('/delete-order/'+id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    closeModal('groupActionModal');
    await load();
  }catch(e){ alert(e.message||'تعذر حذف الصنف'); }
}
function openAddItemFromGroupPicker(){
  const code=String(window.currentGroupActionCode||'').trim();
  openAddItemForGroup(code);
}
function openAddItemForGroup(groupCode){
  const items=getGroupItemsByCode(groupCode);
  const parent=items[0];
  if(!parent){ alert('الأوردر غير موجود'); return; }
  closeModal('groupActionModal');
  closeAllActionMenus();
  openEdit(Number(parent.id));
  window.editAddOnlyMode=true;
  setEditFinanceModeLabel(false, null);
  if(window.editSaveBtn) editSaveBtn.style.display='none';
  if(window.editAddItemBtn){ editAddItemBtn.textContent='✅ حفظ الصنف الجديد'; editAddItemBtn.className='btn green'; }
  // نسيب بيانات العميل والشحن من الأوردر الأصلي، ونفضي بيانات الصنف الجديد.
  e_l.value=''; e_w.value=''; e_g.value=''; e_qty.value='';
  e_total_price.value=''; e_shipping_cost.value=''; e_paymentType.value='لم يتم الدفع'; e_paid_amount.value=0; e_remaining_amount.value=0;
  e_status.value='أوردر جديد';
  e_colorSpecs.value=''; if(window.e_silkColor)e_silkColor.value=''; if(window.e_offsetCount)e_offsetCount.value='0'; renderEditOffsetInputs();
  renderEditFilesList([]);
  if(window.e_designFiles) e_designFiles.value='';
  syncEditPayment();
}
function goToCalculatorFromOrder(id){
  const order = ordersData.find(x=>Number(x.id)===Number(id));
  if(!order){ alert('الأوردر غير موجود'); return; }
  const params = new URLSearchParams();
  params.set('order_id', String(Number(order.id)||0));
  params.set('l', String(Number(order.l)||0));
  params.set('w', String(Number(order.w)||0));
  params.set('g', String(Number(order.g)||0));
  params.set('qty', String(Number(order.qty)||0));
  params.set('color', String(order.color||''));
  params.set('handle', String(order.handle||''));
  params.set('printType', String(order.printType||''));
  params.set('colorSpecs', String(order.colorSpecs||''));
  location.href = `calculator.html?${params.toString()}`;
}
function renderSingleOrderRow(o,index,opts={}){
  const isChild=!!opts.child;
  const groupKey=isChild ? safeGroupKey(opts.groupCode) : '';
  const rowId=isChild ? `group-child-${groupKey}-${Number(o.id)}` : rowDomIdForOrder(o);
  const hiddenStyle=isChild && !groupChildrenOpen[groupKey] ? 'display:none' : '';
  return `<tr id="${rowId}" class="${Number(o.useReadyStock||0)===1?'ready-stock-row':''} ${isChild?'group-child-row group-child-'+groupKey:''}" style="${hiddenStyle}">
    <td class="select-col">${isChild?'':`<input type="checkbox" class="print-check row-print-check" value="${selectionKeyForOrder(o)}" onchange="syncPrintRow(${jsAttrString(selectionKeyForOrder(o))})">`}</td>
    <td>${orderNumberHtml(o,index)}</td>
    <td class="customer-cell"><b>${esc(o.custName||'')}</b><br><small>${esc(o.custPhone||'')}</small><br><small>${esc(o.custAddress||'')}</small>${traderInfoHtml(o)}${orderEntryMetaHtml(o)}${isChild?`<br><small style="color:#86efac">ضمن ${esc(o.group_code||'')}</small>`:''}</td>
    <td class="order-source-cell">${readyStockLabel(o)}</td>
    <td>${dateInfo(o)}</td>
    <td>${singleOrderSizeHtml(o)}</td>
    <td>${esc(o.printType||'')}<br><small style="color:var(--muted)">${esc(o.colorSpecs||'')}</small>${getOrderPrintSheetLabel(o)?`<br><small style="color:#fbbf24">${esc(getOrderPrintSheetLabel(o))}</small>`:''}</td>
    <td><b style="font-size:17px;color:#fde68a">${o.qty}</b></td>
    <td><b style="font-size:16px;color:#86efac">${fmtMoney(isChild?Number(o.total_price||0):getDisplayedOrderTotal(o))}</b></td>
    <td><b style="font-size:16px;color:#fca5a5">${fmtMoney(orderShippingCost(o))}</b><br><small style="color:var(--muted)">${!isBostaOrder(o) ? (baseOrderShippingCost(o)>0 ? 'سعر شحن يدوي' : 'لم يتم تحديد شحن') : (String(o.status||'').trim()==='تم التسليم' ? 'شامل الشحن + 1% المنتج + 10ج لكل 1000 بيع' : (String(o.status||'').trim()==='مرتجع' ? 'مرتجع: الشحن فقط' : 'سعر الشحن محفوظ ويظهر فورًا'))}</small></td>
    <td><b style="font-size:16px;color:#a5f3fc">${fmtMoney(totalCosts(o))}</b></td>
    <td>${paymentBadge(o)}</td>
    <td>${priorityBadge(o)}<div style="height:6px"></div>${statusOptions(o)}${bostaInfoHtml(o)}</td>
    <td class="files">${filesHtml(o)}</td>
    <td class="action-cell">
      <button class="btn dark action-toggle" onclick="toggleActions(${o.id})">⚙️ التحكم</button>
      <div class="action-menu" id="actions-${o.id}">${orderActionButtonsHtml(o)}</div>
    </td>
  </tr>`;
}
function renderGroupOrderRow(o,index){
  const key=safeGroupKey(o.group_code);
  const items=orderItems(o);
  const statusHtml=groupStatusOptions(o);
  const row=`<tr id="${rowDomIdForOrder(o)}" class="group-parent-row">
    <td class="select-col"><input type="checkbox" class="print-check row-print-check" value="${selectionKeyForOrder(o)}" onchange="syncPrintRow(${jsAttrString(selectionKeyForOrder(o))})"></td>
    <td>${orderNumberHtml(o,index)}</td>
    <td class="customer-cell"><b>${esc(o.custName||'')}</b><br><small>${esc(o.custPhone||'')}</small><br><small>${esc(o.custAddress||'')}</small>${traderInfoHtml(o)}${orderEntryMetaHtml(o)}<br><small style="color:#86efac">أوردر واحد</small></td>
    <td class="order-source-cell">${readyStockLabel(o)}</td>
    <td>${dateInfo(o)}</td>
    <td>${groupSizesHtml(o)}</td>
    <td>${groupPrintHtml(o)}</td>
    <td><b style="font-size:17px;color:#fde68a">${o.qty_total}</b><br><small style="color:var(--muted)">إجمالي الكمية</small></td>
    <td><b style="font-size:16px;color:#86efac">${fmtMoney(o.total_price_display)}</b><br><small style="color:var(--muted)">إجمالي البيع</small></td>
    <td><b style="font-size:16px;color:#fca5a5">${fmtMoney(o.shipping_total)}</b><br><small style="color:var(--muted)">إجمالي الشحن الحالي</small></td>
    <td><b style="font-size:16px;color:#a5f3fc">${fmtMoney(o.cost_total)}</b><br><small style="color:var(--muted)">إجمالي التكاليف</small></td>
    <td>${paymentBadge(o)}</td>
    <td>${priorityBadge(o)}<div style="height:6px"></div>${statusHtml}</td>
    <td class="files">${filesHtml(o)}</td>
    <td class="action-cell">
      <button class="btn dark action-toggle" onclick="toggleActions('group-${key}')">⚙️ التحكم</button>
      <div class="action-menu" id="actions-group-${key}">${groupActionButtonsHtml(o)}</div>
    </td>
  </tr>`;
  return row;
}
function renderOrders(data){
  const body=document.getElementById('ordersBody');
  body.innerHTML=data.map((o,i)=>o.is_group ? renderGroupOrderRow(o, i+1) : renderSingleOrderRow(o, i+1)).join('');
  document.getElementById('countBox').innerText='الظاهر: '+data.length;
  if(!printMode){
    document.querySelectorAll('.row-print-check').forEach(ch=>ch.checked=false);
    const all=document.getElementById('printSelectAll'); if(all) all.checked=false;
  }else{
    syncAllPrintedRows();
  }
}
function toggleGroupRows(groupCode){
  const key=safeGroupKey(groupCode);
  groupChildrenOpen[key]=!groupChildrenOpen[key];
  document.querySelectorAll(`.group-child-${key}`).forEach(row=>{ row.style.display=groupChildrenOpen[key] ? '' : 'none'; });
  const btn=document.getElementById(`group-toggle-${key}`);
  if(btn) btn.textContent=groupChildrenOpen[key] ? '🔼 إخفاء الأصناف' : '🔽 عرض الأصناف';
  closeAllActionMenus();
}
function closeAllActionMenus(exceptId=null){
  document.querySelectorAll('.action-menu').forEach(el=>{ if(el.id!==`actions-${exceptId}`) el.classList.remove('open'); });
}
function toggleActions(id){
  closeAllActionMenus(id);
  const box=document.getElementById(`actions-${id}`);
  if(box) box.classList.toggle('open');
}
function togglePanel(id){
  const panel=document.getElementById(id);
  panel.classList.toggle('open');
}
function updateActiveOnlyButton(){
  const btn=document.getElementById('activeOnlyBtn');
  if(!btn) return;
  btn.className = `btn ${activeOnlyFilter ? 'green' : 'dark'}`;
  btn.textContent = activeOnlyFilter ? '📂 إظهار كل الأوردرات' : '📌 إظهار الجاري فقط';
}
function toggleActiveOnly(){
  activeOnlyFilter=!activeOnlyFilter;
  updateActiveOnlyButton();
  applyFilters();
}
function populateOrderMetaFilters(){
  const adminSelect=document.getElementById('createdByFilter');
  if(adminSelect){
    const current=adminSelect.value;
    const admins=[...new Set(ordersData.map(order=>String(order.created_by||'').trim()).filter(Boolean))].sort((a,b)=>a.localeCompare(b,'ar'));
    adminSelect.innerHTML='<option value="">كل الأدمن</option>'+admins.map(name=>`<option value="${esc(name)}">${esc(name)}</option>`).join('');
    if(admins.includes(current)) adminSelect.value=current;
  }
  const pageSelect=document.getElementById('pageFilter');
  if(pageSelect){
    const current=pageSelect.value;
    const legacyPages=[...new Set(ordersData.map(order=>String(order.order_page_name||'').trim()).filter(name=>name&&!ORDER_PAGE_OPTIONS.includes(name)))].sort((a,b)=>a.localeCompare(b,'ar'));
    pageSelect.innerHTML='<option value="">كل الصفحات</option><option value="__none__">بدون صفحة</option>'+
      [...ORDER_PAGE_OPTIONS,...legacyPages].map(name=>`<option value="${esc(name)}">${esc(name)}</option>`).join('');
    if(current==='__none__'||[...ORDER_PAGE_OPTIONS,...legacyPages].includes(current)) pageSelect.value=current;
  }
}
function toggleOrderShippingDetails(id){
  const box=document.getElementById(`bosta-details-${id}`);
  if(!box) return;
  box.style.display = box.style.display==='none' || !box.style.display ? 'block' : 'none';
}
function applyFilters(){
  const name=document.getElementById('searchName').value.toLowerCase();
  const phone=document.getElementById('searchPhone').value.toLowerCase();
  const rawStatus=String(document.getElementById('statusFilter').value||'').trim();
  const status=rawStatus ? normalizedStatusText(rawStatus) : '';
  const payment=document.getElementById('paymentFilter').value;
  const printType=document.getElementById('printTypeFilter').value;
  const printSize=document.getElementById('printSizeFilter').value;
  const createdBy=document.getElementById('createdByFilter').value;
  const page=document.getElementById('pageFilter').value;
  const source=document.getElementById('sourceFilter').value;
  const from=document.getElementById('dateFrom').value;
  const to=document.getElementById('dateTo').value;
  const priority=document.getElementById('priorityFilter').value;
  filteredOrders=buildDisplayOrders(ordersData).filter(o=>{
    const items=orderItems(o);
    const a=!name||(o.custName||'').toLowerCase().includes(name);
    const b=!phone||(o.custPhone||'').toLowerCase().includes(phone);
    const c=!status||items.some(item=>normalizedStatusText(item.status)===status);
    const d=!payment||items.some(item=>(item.paymentType||'')===payment);
    const p=!printType||items.some(item=>(item.printType||'')===printType);
    const z=!printSize||items.some(item=>getOrderPrintSheetClass(item)===printSize);
    const adminOk=!createdBy||items.some(item=>String(item.created_by||'').trim()===createdBy);
    const pageOk=!page||items.some(item=>page==='__none__'?!String(item.order_page_name||'').trim():String(item.order_page_name||'').trim()===page);
    const s=!source || (source==='ready' ? items.some(item=>Number(item.useReadyStock||0)===1) : items.some(item=>Number(item.useReadyStock||0)!==1));
    const e=!from||((o.orderDate||'')>=from);
    const f=!to||((o.orderDate||'')<=to);
    const g=!priority||items.some(item=>normalizePriority(item.priority)===priority);
    const h=!activeOnlyFilter||items.some(item=>!isClosedStatus(normalizedStatusText(item.status)));
    return a&&b&&c&&d&&p&&z&&adminOk&&pageOk&&s&&e&&f&&g&&h;
  });
  renderOrders(filteredOrders);
}
function resetSearch(){ document.getElementById('searchName').value=''; document.getElementById('searchPhone').value=''; applyFilters(); }
function resetFilters(){ document.getElementById('statusFilter').value=''; document.getElementById('paymentFilter').value=''; document.getElementById('printTypeFilter').value=''; document.getElementById('printSizeFilter').value=''; document.getElementById('createdByFilter').value=''; document.getElementById('pageFilter').value=''; document.getElementById('sourceFilter').value=''; document.getElementById('dateFrom').value=''; document.getElementById('dateTo').value=''; document.getElementById('priorityFilter').value=''; activeOnlyFilter=false; updateActiveOnlyButton(); applyFilters(); }
async function load(){ ordersData=(await authFetch('/get-orders')).map(o=>({ ...o, custPhone:normalizePhoneDigits(o.custPhone||''), status: normalizedStatusText(o.status) })).sort((a,b)=>Number(b.order_sort_id||b.id||0)-Number(a.order_sort_id||a.id||0)); populateOrderMetaFilters(); updateActiveOnlyButton(); applyFilters(); }
const STATUS_FLOW=['أوردر جديد','تحت الإنتاج','مستني الزنكات','في القص','تحت الطباعة','تحت التصنيع','جاهز للشحن','تم الشحن','تم التسليم'];
function isBackwardFromCut(currentStatus,newStatus){ const cutIndex=STATUS_FLOW.indexOf('في القص'); const cur=STATUS_FLOW.indexOf(currentStatus); const next=STATUS_FLOW.indexOf(newStatus); if(cur===-1||next===-1) return false; return cur>=cutIndex && next < cutIndex; }
async function updateStatus(id,status){
  try{
    const order=ordersData.find(x=>x.id==id) || {};
    status=normalizedStatusText(status);
    if(Number(order.useReadyStock||0)===1 && ['جاهز للشحن','تم الشحن','تم التسليم'].includes(status) && !Number(order.ready_stock_deducted||0) && Number(order.ready_stock_missing_qty||0)>0){
      alert(`لا يمكن تغيير الحالة إلى ${status}: الكمية ناقصة في مخزن الشنط الجاهزة. ${order.ready_stock_stock_message||''}`);
      load();
      return;
    }
    if(!Number(order.useReadyStock||0) && isBackwardFromCut(normalizedStatusText(order.status),status)){ alert('لا يمكن الرجوع لحالة سابقة بعد القص'); load(); return; }
    if(!Number(order.useReadyStock||0) && !Number(order.paper_cut_done||0) && (status==='في القص' || shouldOpenCutPlan(order,status))){ pendingStatusOrderId=id; window.pendingTargetStatus=status; await showDetails(id,true,status); return; }
    const body={id,status};
    const afterManufacturing=getStatusIndex(status)>getStatusIndex('تحت التصنيع');
    if(afterManufacturing && !Number(order.useReadyStock||0) && String(order.handle||'')==='بيد' && !Number(order.handle_stock_deducted||0)){
      const askHandle=confirm('هل تم تركيب اليد؟\nاختيار لا سيُبقي الحالة على تحت التصنيع.');
      if(!askHandle){ load(); return; }
      body.confirmHandleInstall=true;
    }
    if(status==='تم التسليم' && Number(order.total_price||0)>0){
      const ask=confirm('هل العميل دفع المبلغ بالكامل؟\nاضغط OK للتأكيد أو Cancel لإلغاء تغيير الحالة.');
      if(!ask){ load(); return; }
      body.settlePayment=true;
    }
    await authFetch('/update-status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    load();
  }catch(e){ alert(e.message); load(); }
}
async function updateGroupStatus(groupCode,status,selectEl){
  const code=String(groupCode||'').trim();
  if(!code || !status){ if(selectEl) selectEl.value=''; return; }
  try{
    status=normalizedStatusText(status);
    const items=ordersData.filter(x=>String(x.group_code||'').trim()===code).sort((a,b)=>Number(a.item_no||1)-Number(b.item_no||1));
    if(!items.length) throw new Error('الأوردر المجمع غير موجود');
    const targetItems=items.filter(item=>{
      const allowed=allowedStatusesForOrder(item);
      return allowed.includes(status) || normalizedStatusText(item.status)===status;
    });
    if(!targetItems.length) throw new Error('الحالة المختارة غير متاحة لهذا الأوردر');
    const blockedReady=targetItems.find(item=>Number(item.useReadyStock||0)===1 && ['جاهز للشحن','تم الشحن','تم التسليم'].includes(status) && !Number(item.ready_stock_deducted||0) && Number(item.ready_stock_missing_qty||0)>0);
    if(blockedReady) throw new Error(`لا يمكن تغيير الحالة إلى ${status}: في صنف ناقص في مخزن الشنط الجاهزة. ${blockedReady.ready_stock_stock_message||''}`);
    for(const item of targetItems){
      if(!Number(item.useReadyStock||0) && isBackwardFromCut(normalizedStatusText(item.status),status)) throw new Error(`لا يمكن الرجوع لحالة سابقة بعد القص في صنف ${Number(item.item_no||1)}`);
    }
    const itemsNeedCut=targetItems.filter(item=>!Number(item.useReadyStock||0) && !Number(item.paper_cut_done||0) && (status==='في القص' || shouldOpenCutPlan(item,status)));
    const directItems=targetItems.filter(item=>!itemsNeedCut.some(cutItem=>Number(cutItem.id)===Number(item.id)));
    const afterManufacturing=getStatusIndex(status)>getStatusIndex('تحت التصنيع');
    const needsHandleConfirmation=targetItems.some(item=>afterManufacturing && !Number(item.useReadyStock||0) && String(item.handle||'')==='بيد' && !Number(item.handle_stock_deducted||0));
    const settlePayment=status==='تم التسليم' && targetItems.some(item=>Number(item.total_price||0)>0);
    let confirmHandleInstall=false;
    if(needsHandleConfirmation){
      confirmHandleInstall=confirm('هل تم تركيب اليد لكل الأصناف المطلوب تحديثها في الأوردر؟\nاختيار لا سيلغي تغيير الحالة.');
      if(!confirmHandleInstall){ load(); return; }
    }
    if(settlePayment){
      const ask=confirm('هل العميل دفع المبلغ بالكامل لكل الأصناف المطلوب تحديثها في الأوردر؟\nاضغط OK للتأكيد أو Cancel لإلغاء تغيير الحالة.');
      if(!ask){ load(); return; }
    }
    if(itemsNeedCut.length){
      currentGroupStatusFlow={
        groupCode:code,
        targetStatus:status,
        itemIds:itemsNeedCut.map(item=>Number(item.id)),
        postCutItemIds:directItems.map(item=>Number(item.id)),
        currentIndex:0,
        confirmHandleInstall,
        settlePayment
      };
      closeAllActionMenus();
      await openNextGroupCutItem();
      return;
    }
    await applyGroupStatusToItems(directItems,status,{confirmHandleInstall,settlePayment});
  }catch(e){
    if(selectEl){
      const currentItems=ordersData.filter(x=>String(x.group_code||'').trim()===code);
      const unique=[...new Set(currentItems.map(item=>normalizedStatusText(item.status)))];
      selectEl.value=unique.length===1 ? unique[0] : '';
    }
    alert(e.message);
    load();
  }
}
async function applyGroupStatusToItems(items,status,opts={}){
  for(const item of items){
    const body={id:item.id,status};
    if(opts.confirmHandleInstall) body.confirmHandleInstall=true;
    if(opts.settlePayment && Number(item.total_price||0)>0) body.settlePayment=true;
    await authFetch('/update-status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  }
  await load();
}
async function openNextGroupCutItem(){
  if(!currentGroupStatusFlow) return;
  const itemId=Number(currentGroupStatusFlow.itemIds[currentGroupStatusFlow.currentIndex]||0);
  if(!itemId){
    currentGroupStatusFlow=null;
    await load();
    return;
  }
  const item=ordersData.find(x=>Number(x.id)===Number(itemId));
  if(item && Number(item.paper_cut_done||0)){
    currentGroupStatusFlow.currentIndex += 1;
    await openNextGroupCutItem();
    return;
  }
  if(!item){
    currentGroupStatusFlow.currentIndex += 1;
    await openNextGroupCutItem();
    return;
  }
  await showDetails(itemId,true,currentGroupStatusFlow.targetStatus,{
    groupFlow:true,
    groupCode:currentGroupStatusFlow.groupCode,
    itemIndex:currentGroupStatusFlow.currentIndex,
    totalItems:currentGroupStatusFlow.itemIds.length
  });
}

function openCosts(id){
  currentOrderId=id;
  const o=ordersData.find(x=>x.id==id) || {};
  const auto=buildAutoCosts(o||{});
  ['cut','print','zinc','design','make','hand','paper','hand_fix','forme','lamination'].forEach(k=>{
    const saved=o['cost_'+k];
    document.getElementById('c_'+k).value=(saved!==undefined && saved!==null && saved!=='') ? saved : (auto[k] ?? 0);
  });
  document.getElementById('costModal').style.display='block';
}
async function saveCosts(){
  const val=id=>{ const raw=String(document.getElementById(id).value||'').trim(); return raw==='' ? 0 : Number(raw); };
  const body={id:currentOrderId,cost_cut:val('c_cut'),cost_print:val('c_print'),cost_zinc:val('c_zinc'),cost_design:val('c_design'),cost_make:val('c_make'),cost_hand:val('c_hand'),cost_paper:val('c_paper'),cost_hand_fix:val('c_hand_fix'),cost_forme:val('c_forme'),cost_lamination:val('c_lamination')};
  await authFetch('/update-costs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  closeModal('costModal');
  load();
}

function getOptionGramValue(opt){
  const direct = Number(opt?.paperGrammage ?? opt?.gram ?? opt?.grammage ?? 0);
  if(Number.isFinite(direct) && direct>0) return direct;
  const m = String(opt?.paperLabel || '').match(/(\d+(?:\.\d+)?)\s*جم/);
  return m ? Number(m[1]) : 0;
}
function getPlanDisplayPool(plan,includeAlternatives=false){
  const strict = Array.isArray(plan?.options) ? plan.options.slice() : [];
  const alternatives = includeAlternatives && Array.isArray(plan?.alternativeOptions) ? plan.alternativeOptions.slice() : [];
  const seen=new Set();
  return [...strict,...alternatives].filter(o=>{
    const enough = o?.enoughStock===true || Number(o?.enoughStock||0)===1 || Number(o?.availableSheets||0)>=Number(o?.neededSheets||0);
    const sig=`${o?.paperId}|${o?.layoutKey}|${o?.sheetWidth}|${o?.sheetHeight}`;
    if(!enough || seen.has(sig)) return false;
    seen.add(sig);
    return true;
  });
}
function chooseBestPlanOption(options){
  return (options || []).slice().sort((x,y)=>{
    const xe = Number(x?.availableSheets||0) >= Number(x?.neededSheets||0);
    const ye = Number(y?.availableSheets||0) >= Number(y?.neededSheets||0);
    if(xe !== ye) return xe ? -1 : 1;
    if(Number(x?.wastePercent||0) !== Number(y?.wastePercent||0)) return Number(x?.wastePercent||0) - Number(y?.wastePercent||0);
    return Number(x?.neededSheets||0) - Number(y?.neededSheets||0);
  })[0] || null;
}
function getCurrentPlanOptions(){
  if(!window.currentPlan) return [];
  const pool = window.currentPlan.poolOptions || [];
  if(window.currentPlan.selectedGram == null) return pool;
  return pool.filter(o => Number(getOptionGramValue(o)) === Number(window.currentPlan.selectedGram));
}
function planOrderDisplayNo(order={}){
  return order.order_display_no || order.display_order_no || order.order_sort_id || order.id || '';
}
function splitPlanSignature(plan={}){
  return (Array.isArray(plan?.allocations)?plan.allocations:[]).map(row=>`${Number(row.paperId||0)}:${String(row.layoutKey||'')}:${Number(row.sheetsToUse||row.neededSheets||0)}:${Number(row.allocatedBagQty||0)}`).sort().join('|');
}
function getSplitPlanOptions(plan={}){
  const raw=[plan.splitPlan,plan.alternativeSplitPlan,...(Array.isArray(plan.splitPlanOptions)?plan.splitPlanOptions:[])].filter(Boolean);
  const seen=new Set();
  return raw.filter(item=>{
    const sig=splitPlanSignature(item);
    if(!sig||seen.has(sig)) return false;
    seen.add(sig);
    return true;
  });
}
function getSplitPlanCandidate(plan={}){
  return getSplitPlanOptions(plan)[0] || null;
}
function splitPlanChoiceLabel(plan={},index=0){
  const rows=Array.isArray(plan.allocations)?plan.allocations:[];
  const parts=rows.map(row=>`${row.paperLabelFull||row.paperLabel||('فرخ #'+row.paperId)} — ${row.sheetsToUse||row.neededSheets} فرخ${Number(row.allocatedBagQty||0)>0?' / '+row.allocatedBagQty+' شنطة':''}`);
  return `${index===0?'⭐ اقتراح السيستم':'اختيار بديل'}: ${parts.join(' + ')}${Number(plan.usesAlternativeGram||0)===1?' — جرام بديل':''}`;
}
function manualSplitPaperOptions(choices=[],selectedId=0,placeholder='اختر صنف الورق'){
  return `<option value="">${esc(placeholder)}</option>`+(choices||[]).map(row=>`<option value="${Number(row.paperId||0)}" ${Number(selectedId)===Number(row.paperId)?'selected':''}>${esc(row.paperLabelFull||row.paperLabel)} | متاح ${esc(row.availableSheets)} فرخ | يغطي تقريبًا ${esc(row.capacityQty)} شنطة | ${esc(row.bestLayoutLabel||'')}</option>`).join('');
}
function renderSplitPlanSelection(){
  if(!window.currentPlan?.selectedSplit) return;
  const split=window.currentPlan.selectedSplit;
  const allocations=Array.isArray(split.allocations)?split.allocations:[];
  const alt=Number(split.usesAlternativeGram||0)===1;
  detailsMeta.innerHTML=`${renderGroupCutProgress()}<div><b>الأوردر:</b> #${esc(planOrderDisplayNo(window.currentPlan.order))} | <b>العميل:</b> ${esc(window.currentPlan.order.custName||'')} | <b>الكمية:</b> ${esc(window.currentPlan.order.qty)}</div>
  <div style="margin-top:8px;padding:10px 12px;border-radius:12px;background:rgba(${alt?'245,158,11':'20,184,166'},.10);border:1px solid ${alt?'#f59e0b':'#14b8a6'};color:${alt?'#fde68a':'#ccfbf1'}"><b>استكمال الناقص من ${allocations.length} مقاسات أفرخ:</b> ${esc(split.totalSheets)} فرخ • ${esc(Number(split.totalKg||0).toFixed(2))} كجم • تكلفة الورق ${esc(Number(split.totalCost||0).toFixed(2))} ج${alt?' • تشمل جرامًا بديلًا بعد موافقتك':''}</div>
  <div style="margin-top:8px">${allocations.map(row=>`<div>• ${esc(row.paperLabelFull||row.paperLabel)} | ${esc(row.paperGrammage||row.gram||'-')} جم | <b>${esc(row.sheetsToUse||row.neededSheets)} فرخ</b>${Number(row.allocatedBagQty||0)>0?` لتغطية ${esc(row.allocatedBagQty)} شنطة`:''}</div>`).join('')}</div>`;
  detailsBox.innerHTML=allocations.map(row=>`<div style="margin-bottom:14px;border:1px solid #cbd5e1;border-radius:12px;padding:8px">${drawSheetSvg(row,window.currentPlan.order,window.currentPlan.plan)}</div>`).join('');
}
function renderPlanSelectionMeta(opt){
  if(!window.currentPlan || !opt) return;
  const p1=window.currentPlan.plan.layouts?.pieceByPiece, p2=window.currentPlan.plan.layouts?.singlePiece;
  const gram = getOptionGramValue(opt);
  if(opt.isActualCut){
    const availability = Number(opt.missingPaper||0)===1 ? '<span style="color:#fca5a5">الفرخ المستخدم غير موجود حالياً بالمخزن لكنه محفوظ في السجل</span>' : `<b>المتاح الآن:</b> ${esc(opt.availableSheets)}`;
    detailsMeta.innerHTML=`${renderGroupCutProgress()}<div><b>الأوردر:</b> #${esc(planOrderDisplayNo(window.currentPlan.order))} | <b>العميل:</b> ${esc(window.currentPlan.order.custName||'')} | <b>المقاس:</b> ${esc(window.currentPlan.order.l)} × ${esc(window.currentPlan.order.w)} × ${esc(window.currentPlan.order.g)} | <b>الكمية المدخلة:</b> ${esc(window.currentPlan.order.qty)}</div>
    <div style="margin-top:8px;padding:10px 12px;border-radius:12px;background:rgba(74,222,128,.08);border:1px solid rgba(74,222,128,.35);color:#dcfce7"><b>✅ هذه هي القَصّة التي تم تنفيذها فعلاً على الأوردر</b></div>
    <div style="margin-top:8px"><b>الفرخ المستخدم:</b> ${esc(opt.paperColor||'')} | <b>المقاس:</b> ${esc(opt.paperLabelFull||opt.paperLabel)} | <b>الجرام:</b> ${esc(gram||'-')} جم | <b>الطريقة:</b> ${esc(opt.layoutLabel)} | <b>إنتاج الفرخ:</b> ${esc(opt.outputLabel||opt.piecesPerSheet)} | <b>المستهلك:</b> ${esc(opt.neededSheets)} فرخ | ${availability}</div>
    <div style="margin-top:8px"><b>حته ف حته:</b> نص شنطة × نص شنطة (${esc(p1?.cutWidth||0)} × ${esc(p1?.cutLength||0)}) ${opt.layoutKey==='pieceByPiece'?'⭐':''} &nbsp;&nbsp; <b>حته واحدة:</b> شنطة كاملة (${esc(p2?.cutWidth||0)} × ${esc(p2?.cutLength||0)}) ${opt.layoutKey==='singlePiece'?'⭐':''}</div>`;
    return;
  }
  const alternativeWarning=Number(opt.isAlternative||0)===1 ? `<div style="margin-top:8px;padding:10px 12px;border-radius:12px;background:rgba(245,158,11,.10);border:1px solid rgba(245,158,11,.55);color:#fde68a"><b>⚠️ ورق بديل عند العجز:</b> نفس نوع ولون الأوردر، لكن الجرام مختلف. لن يتم اعتماده إلا بعد تأكيدك.</div>` : '';
  detailsMeta.innerHTML=`${renderGroupCutProgress()}<div><b>الأوردر:</b> #${esc(planOrderDisplayNo(window.currentPlan.order))} | <b>العميل:</b> ${esc(window.currentPlan.order.custName||'')} | <b>المقاس:</b> ${esc(window.currentPlan.order.l)} × ${esc(window.currentPlan.order.w)} × ${esc(window.currentPlan.order.g)} | <b>الكمية المدخلة:</b> ${esc(window.currentPlan.order.qty)}</div>${alternativeWarning}
  <div style="margin-top:8px"><b>الفرخ المختار:</b> ${esc(opt.paperColor||'')} | <b>المقاس:</b> ${esc(opt.paperLabel)} | <b>الجرام:</b> ${esc(gram||'-')} جم | <b>الطريقة:</b> ${esc(opt.layoutLabel)} | <b>إنتاج الفرخ:</b> ${esc(opt.outputLabel||opt.piecesPerSheet)} | <b>المطلوب:</b> ${esc(opt.neededSheets)} فرخ | <b>المتاح:</b> ${esc(opt.availableSheets)} | <b>الهدر:</b> ${esc(opt.wastePercent)}%</div>
  <div style="margin-top:8px"><b>حته ف حته:</b> نص شنطة × نص شنطة (${esc(p1?.cutWidth||0)} × ${esc(p1?.cutLength||0)}) ${opt.layoutKey==='pieceByPiece'?'⭐':''} &nbsp;&nbsp; <b>حته واحدة:</b> شنطة كاملة (${esc(p2?.cutWidth||0)} × ${esc(p2?.cutLength||0)}) ${opt.layoutKey==='singlePiece'?'⭐':''}</div>`;
}
function renderGroupCutProgress(){
  if(!window.currentPlan?.flowMeta?.groupFlow) return '';
  const meta=window.currentPlan.flowMeta;
  return `<div style="margin-bottom:8px;padding:10px 12px;border-radius:12px;background:rgba(34,211,238,.08);border:1px solid rgba(34,211,238,.35);color:#cffafe"><b>أوردر مجمّع:</b> تجهيز القص للصنف ${Number(meta.itemIndex||0)+1} من ${Number(meta.totalItems||0)}</div>`;
}
function renderPlanChoices(){
  if(!window.currentPlan) return;
  if(window.currentPlan.showActualCutOnly){
    detailsChoices.innerHTML='<div style="padding:10px 12px;border-radius:12px;background:#0b1220;border:1px solid #334155;color:#cbd5e1">تم عرض تفاصيل القَصّة الفعلية المحفوظة لهذا الأوردر بعد تنفيذ القص.</div>';
    return;
  }
  const pool = window.currentPlan.poolOptions || [];
  const alternativeCount=Array.isArray(window.currentPlan.plan?.alternativeOptions) ? window.currentPlan.plan.alternativeOptions.length : 0;
  const alternativeButton=alternativeCount ? `<button class="btn ${window.currentPlan.showAlternatives?'green':'dark'}" onclick="toggleAlternativePlanChoices()">${window.currentPlan.showAlternatives?'إخفاء الورق البديل':'⚠️ الكمية مش كفاية؟ عرض مقاسات/جرامات بديلة'} (${alternativeCount})</button>` : '';
  const splitCandidate=getSplitPlanCandidate(window.currentPlan.plan);
  const activeSplitSig=splitPlanSignature(window.currentPlan.selectedSplit||{});
  const splitPaperChoices=Array.isArray(window.currentPlan.plan?.splitPaperChoices)?window.currentPlan.plan.splitPaperChoices:[];
  const suggestedPrimaryId=Number(splitCandidate?.allocations?.[0]?.paperId||splitPaperChoices[0]?.paperId||0);
  const manualSelected=Number(window.currentPlan.selectedSplit?.manualSelection||0)===1;
  const splitPicker=splitCandidate ? `<div style="margin:10px 0;padding:10px 12px;border:1px solid rgba(34,211,238,.35);border-radius:12px;background:rgba(34,211,238,.06)"><div style="font-weight:800;color:#cffafe;margin-bottom:8px">✂️ قدامك اقتراح مجمّع واحد من السيستم، أو اختار الأصناف بنفسك مانيول:</div><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn ${!manualSelected&&splitPlanSignature(splitCandidate)===activeSplitSig?'green':'cyan'}" onclick="selectSplitPlanByIndex(0)">${esc(splitPlanChoiceLabel(splitCandidate,0))}</button><button class="btn ${manualSelected?'green':'dark'}" onclick="toggleManualSplitBuilder()">🖐️ أحدد الصنف الثابت والاستكمال بنفسي</button>${window.currentPlan.selectedSplit?'<button class="btn red" onclick="clearSplitPlanSelection()">إلغاء خطة الاستكمال</button>':''}</div>${manualSelected?'<div style="margin-top:8px;color:#bbf7d0;font-weight:800">✅ الخطة المعروضة حاليًا هي خطة المانيول التي اخترتها.</div>':''}<div id="manualSplitBuilder" style="display:none;margin-top:12px;padding:12px;border-radius:10px;background:#0b1220;border:1px solid #334155"><div style="color:#cbd5e1;margin-bottom:8px">الصنف الثالث اختياري. أي صنف تختاره هنا سيدخل فعلًا في الخصم بفرخ واحد على الأقل.</div><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:8px"><label>الصنف الثابت<select id="manual_split_fixed">${manualSplitPaperOptions(splitPaperChoices,suggestedPrimaryId,'اختار الصنف الثابت')}</select></label><label>صنف الاستكمال الأول<select id="manual_split_second">${manualSplitPaperOptions(splitPaperChoices,0,'اختار صنف الاستكمال')}</select></label><label>صنف الاستكمال الثاني (اختياري)<select id="manual_split_third">${manualSplitPaperOptions(splitPaperChoices,0,'بدون صنف ثالث')}</select></label></div><button class="btn green" style="margin-top:10px" onclick="calculateManualSplitPlan()">احسب واعرض خطة الأصناف اللي اخترتها</button></div></div>` : '';
  if(!pool.length){
    detailsChoices.innerHTML=`<div style="padding:10px 12px;border-radius:12px;background:#0b1220;border:1px solid #334155;color:#fca5a5;margin-bottom:10px">لا يوجد صنف ورق واحد يكفي كمية الأوردر بالكامل.${splitCandidate?' يمكن استكمال الكمية من أكتر من مقاس فرخ بعد اختيارك للخطة.':' ولا توجد خطة مجمعة تغطي الكمية الحالية.'}</div>${splitPicker}<div style="display:flex;gap:8px;flex-wrap:wrap">${alternativeButton}</div>`;
    return;
  }
  const grams = [...new Set(pool.map(o => Number(getOptionGramValue(o))).filter(g => g>0))].sort((a,b)=>a-b);
  const options = getCurrentPlanOptions();
  const selectedSig = window.currentPlan.selected ? `${window.currentPlan.selected.paperId}|${window.currentPlan.selected.layoutKey}|${window.currentPlan.selected.sheetWidth}|${window.currentPlan.selected.sheetHeight}` : '';
  detailsChoices.innerHTML=`<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:10px"><span style="font-weight:800;color:#cbd5e1">الجرامات:</span><button class="btn ${window.currentPlan.selectedGram==null?'green':'dark'}" onclick="selectPlanGram('all')">كل الجرامات</button>${grams.map(g=>`<button class="btn ${Number(window.currentPlan.selectedGram)===Number(g)?'green':'dark'}" onclick="selectPlanGram(${g})">${g} جم</button>`).join('')}</div>${splitPicker}<div style="display:flex;gap:8px;flex-wrap:wrap">${alternativeButton}<button class="btn cyan" id="manualBtn" onclick="toggleManualChoices(this)">اختار صنف واحد بنفسك</button></div><div id="manualChoices" style="display:none;margin-top:10px;gap:8px;flex-wrap:wrap">${options.map(opt=>{ const sig=`${opt.paperId}|${opt.layoutKey}|${opt.sheetWidth}|${opt.sheetHeight}`; const gram=getOptionGramValue(opt); const isActive=sig===selectedSig; const alt=Number(opt.isAlternative||0)===1?' | ⚠️ بديل':''; return `<button class="btn ${isActive?'green':'cyan'}" data-opt='${encodeURIComponent(JSON.stringify(opt))}' onclick='selectPlanOption(this.dataset.opt)'>${esc(opt.paperColor||'')} | ${esc(opt.paperLabel)} | ${esc(gram||'-')} جم | ${esc(opt.layoutLabel)} | مطلوب ${esc(opt.neededSheets)} فرخ | متاح ${esc(opt.availableSheets)}${alt}</button>`;}).join('')}</div>`;
}
function selectSplitPlanByIndex(index){
  if(!window.currentPlan || window.currentPlan.showActualCutOnly) return;
  const plans=getSplitPlanOptions(window.currentPlan.plan);
  const selected=plans[Number(index)];
  if(!selected) return;
  window.currentPlan.selectedSplit=selected;
  window.currentPlan.selected=null;
  renderSplitPlanSelection();
  saveCutBtn.style.display=window.currentPlan.forCut?'inline-flex':'none';
  renderPlanChoices();
}
function toggleManualSplitBuilder(){
  const box=document.getElementById('manualSplitBuilder');
  if(!box) return;
  box.style.display=box.style.display==='none'?'block':'none';
}
async function calculateManualSplitPlan(){
  if(!window.currentPlan?.order?.id) return;
  const ids=['manual_split_fixed','manual_split_second','manual_split_third'].map(id=>Number(document.getElementById(id)?.value||0)).filter(Boolean);
  if(ids.length<2) return alert('اختار الصنف الثابت وصنف استكمال واحد على الأقل');
  if(new Set(ids).size!==ids.length) return alert('مينفعش تختار نفس صنف الورق أكثر من مرة');
  try{
    const data=await authFetch(`/order-paper-plan/${window.currentPlan.order.id}/custom-split`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({paper_ids:ids})});
    window.currentPlan.selectedSplit=data.plan;
    window.currentPlan.selected=null;
    renderSplitPlanSelection();
    saveCutBtn.style.display=window.currentPlan.forCut?'inline-flex':'none';
    renderPlanChoices();
  }catch(e){ alert(e.message||'تعذر حساب خطة الأصناف المختارة'); }
}
function clearSplitPlanSelection(){
  if(!window.currentPlan) return;
  window.currentPlan.selectedSplit=null;
  window.currentPlan.selected=window.currentPlan.best || chooseBestPlanOption(window.currentPlan.poolOptions||[]);
  if(window.currentPlan.selected){ renderPlanSelectionMeta(window.currentPlan.selected); detailsBox.innerHTML=drawSheetSvg(window.currentPlan.selected,window.currentPlan.order,window.currentPlan.plan); }
  else { detailsMeta.innerHTML='اختر خطة استكمال أو فرخًا مناسبًا.'; detailsBox.innerHTML=''; }
  saveCutBtn.style.display=window.currentPlan.forCut&&window.currentPlan.selected?'inline-flex':'none';
  renderPlanChoices();
}
function toggleSplitPlanChoice(){
  if(!window.currentPlan || window.currentPlan.showActualCutOnly) return;
  const candidate=getSplitPlanCandidate(window.currentPlan.plan);
  if(!candidate) return;
  if(window.currentPlan.selectedSplit){
    window.currentPlan.selectedSplit=null;
    window.currentPlan.selected=window.currentPlan.best || chooseBestPlanOption(window.currentPlan.poolOptions||[]);
    if(window.currentPlan.selected){ renderPlanSelectionMeta(window.currentPlan.selected); detailsBox.innerHTML=drawSheetSvg(window.currentPlan.selected,window.currentPlan.order,window.currentPlan.plan); }
    else { detailsMeta.innerHTML='اختر فرخًا مناسبًا أو خطة استكمال.'; detailsBox.innerHTML=''; }
  }else{
    window.currentPlan.selectedSplit=candidate;
    window.currentPlan.selected=null;
    renderSplitPlanSelection();
  }
  saveCutBtn.style.display=window.currentPlan.forCut&&(window.currentPlan.selected||window.currentPlan.selectedSplit)?'inline-flex':'none';
  renderPlanChoices();
}
function toggleAlternativePlanChoices(){
  if(!window.currentPlan || window.currentPlan.showActualCutOnly) return;
  window.currentPlan.selectedSplit=null;
  window.currentPlan.showAlternatives=!window.currentPlan.showAlternatives;
  window.currentPlan.poolOptions=getPlanDisplayPool(window.currentPlan.plan,window.currentPlan.showAlternatives);
  const current=window.currentPlan.selected;
  const pool=window.currentPlan.poolOptions;
  const currentSig=current?`${current.paperId}|${current.layoutKey}|${current.sheetWidth}|${current.sheetHeight}`:'';
  if(!pool.some(o=>`${o.paperId}|${o.layoutKey}|${o.sheetWidth}|${o.sheetHeight}`===currentSig)){
    const alternatives=pool.filter(o=>Number(o.isAlternative||0)===1);
    window.currentPlan.selected=chooseBestPlanOption(alternatives.length?alternatives:pool);
    window.currentPlan.selectedGram=window.currentPlan.selected?getOptionGramValue(window.currentPlan.selected)||null:null;
  }
  const opt=window.currentPlan.selected;
  if(opt){ renderPlanSelectionMeta(opt); detailsBox.innerHTML=drawSheetSvg(opt,window.currentPlan.order,window.currentPlan.plan); }
  else { detailsMeta.innerHTML='لا يوجد ورق كافٍ من نفس نوع ولون الأوردر.'; detailsBox.innerHTML=''; }
  saveCutBtn.style.display=window.currentPlan.forCut&&opt?'inline-flex':'none';
  renderPlanChoices();
}
function selectPlanGram(value){
  if(!window.currentPlan) return;
  window.currentPlan.selectedSplit=null;
  window.currentPlan.selectedGram = value==='all' ? null : Number(value||0);
  const options = getCurrentPlanOptions();
  const currentSig = window.currentPlan.selected ? `${window.currentPlan.selected.paperId}|${window.currentPlan.selected.layoutKey}|${window.currentPlan.selected.sheetWidth}|${window.currentPlan.selected.sheetHeight}` : '';
  if(!options.find(o => `${o.paperId}|${o.layoutKey}|${o.sheetWidth}|${o.sheetHeight}` === currentSig)){
    window.currentPlan.selected = chooseBestPlanOption(options);
  }
  const opt = window.currentPlan.selected || chooseBestPlanOption(options);
  if(opt){
    window.currentPlan.selected = opt;
    renderPlanSelectionMeta(opt);
    detailsBox.innerHTML = drawSheetSvg(opt, window.currentPlan.order, window.currentPlan.plan);
  }else{
    detailsMeta.innerHTML='لا يوجد فرخ مناسب بهذا الجرام حالياً.';
    detailsBox.innerHTML='';
  }
  saveCutBtn.style.display=window.currentPlan.forCut&&opt?'inline-flex':'none';
  renderPlanChoices();
}
function updateEditStatusOptions(useReady,currentStatus='أوردر جديد'){
  const current=normalizedStatusText(currentStatus);
  let list=[...(useReady ? READY_STOCK_ONLY_STATUSES : ALL_ORDER_STATUSES)];
  if(current && !list.includes(current)) list=[current,...list];
  e_status.innerHTML=list.map(s=>`<option value="${s}" ${current===s?'selected':''}>${s}</option>`).join('');
}
async function syncEditReadyStockGusset(){
  if(String(e_useReadyStock?.value||'0')!=='1') return null;
  const length=Number(e_l?.value||0), width=Number(e_w?.value||0);
  if(!length||!width) return null;
  const color=String(e_color?.value||'').trim(), handle=String(e_handle?.value||'').trim();
  try{
    const rows=await authFetch('/get-bags');
    let matches=(rows||[]).filter(row=>Number(row.length)===length&&Number(row.width)===width&&(!color||String(row.color||'').trim()===color)&&(!handle||String(row.handle||'').trim()===handle));
    if(!matches.length) matches=(rows||[]).filter(row=>Number(row.length)===length&&Number(row.width)===width);
    matches.sort((a,b)=>Number(b.total_qty||0)-Number(a.total_qty||0)||Number(b.id||0)-Number(a.id||0));
    if(matches.length){
      e_g.value=String(Number(matches[0].gusset||0)||0);
      return matches[0];
    }
  }catch(_){}
  return null;
}
function syncEditReadyStockUI(){
  const useReady = String(e_useReadyStock.value || '0') === '1';
  if(window.e_g_wrap) e_g_wrap.style.display = useReady ? 'none' : 'block';
  if(useReady) syncEditReadyStockGusset();
  if(useReady && e_printType.value === 'أوفست') e_printType.value = 'سادة';
  rebuildEditPrintTypeOptions();
  syncEditPrintDetailsUI();
  updateEditStatusOptions(useReady, e_status.value || 'أوردر جديد');
}
function renderEditFilesList(files=[]){
  const list = Array.isArray(files) ? files : [];
  if(!window.e_files_list) return;
  e_files_list.innerHTML = list.length
    ? list.map(f=>{ const id=Number(f.id); const name=esc(fileDisplayName(f)); return `<div class="edit-file-row"><span>📎 ${name}</span><div class="file-actions"><button type="button" class="btn dark file-delete-btn" onclick="openOrderFile(${id})">فتح</button><button type="button" class="btn cyan file-delete-btn" onclick="downloadOrderFile(${id}, ${jsAttrString(fileDisplayName(f))})">تحميل</button><button type="button" class="btn red file-delete-btn" onclick="deleteOrderFile(${id})">حذف</button></div></div>`; }).join('')
    : 'لا توجد ملفات PDF';
}
function wholeGroupFinanceAmounts(items=[]){
  const list=[...(items||[])].sort((a,b)=>(Number(a.item_no||1)-Number(b.item_no||1)) || (Number(a.id||0)-Number(b.id||0)));
  if(list.length<=1) return null;
  const sum = (arr, fn)=>arr.reduce((s,item)=>s+(Number(fn(item))||0),0);
  // Group finance must only use the values stored on the order rows.  The
  // ready-stock display fallback (stock selling price × quantity) is useful
  // for a standalone legacy row, but applying it to the zero-value child rows
  // makes a grouped order count its items for a second time (order #65 was
  // shown as 21,125 instead of its stored 10,500).
  const totalAll=sum(list, item=>item.total_price);
  const paidAll=sum(list, item=>item.paid_amount);
  const remainingAll=sum(list, item=>item.remaining_amount);
  const shippingAll=sum(list, item=>baseOrderShippingCost(item));
  const totalOther=sum(list.slice(1), item=>item.total_price);
  const paidOther=sum(list.slice(1), item=>item.paid_amount);
  const remainingOther=sum(list.slice(1), item=>item.remaining_amount);
  const shippingOther=sum(list.slice(1), item=>baseOrderShippingCost(item));
  // القيم القديمة قد تكون موزعة على الأصناف، لذلك نجمعها دائمًا. لا يجوز اعتبار
  // تقارب سعر صنفين تكرارًا؛ فهذا هو السبب الذي كان يحول 6100/1000 إلى 3000/500.
  const total = totalAll;
  const paid = paidAll;
  const shipping = shippingAll;
  const remaining = Math.max(0, total - paid);

  return { items:list, total:+total.toFixed(2), paid:+paid.toFixed(2), remaining:+remaining.toFixed(2), shipping:+shipping.toFixed(2), normalized: totalOther===0 && paidOther===0 && remainingOther===0 && shippingOther===0 };
}
function summarizeGroupFinancialsForEdit(order={}){
  const code=String(order.group_code||'').trim();
  const items=code ? getGroupItemsByCode(code) : [];
  if(items.length<=1) return null;
  return wholeGroupFinanceAmounts(items);
}
function setEditFinanceModeLabel(isGroup=false, summary=null){
  window.editGroupFinancialMode=!!isGroup;
  if(window.e_total_wrap){
    const label=e_total_wrap.querySelector('label');
    if(label) label.textContent=isGroup?'إجمالي سعر الأوردر كله':'إجمالي السعر';
  }
  if(window.e_paid_wrap){
    const label=e_paid_wrap.querySelector('label');
    if(label) label.textContent=isGroup?'المدفوع على الأوردر كله':'المدفوع';
  }
  if(window.e_remaining_wrap){
    const label=e_remaining_wrap.querySelector('label');
    if(label) label.textContent=isGroup?'المتبقي على الأوردر كله':'المتبقي';
  }
  let hint=document.getElementById('editGroupFinanceHint');
  if(isGroup){
    if(!hint && window.e_total_wrap && e_total_wrap.parentNode){
      hint=document.createElement('div');
      hint.id='editGroupFinanceHint';
      hint.className='full';
      hint.style.cssText='padding:10px 12px;border:1px solid rgba(34,211,238,.45);background:rgba(34,211,238,.08);border-radius:12px;color:#cffafe;font-size:13px;line-height:1.8';
      e_total_wrap.parentNode.insertBefore(hint, e_total_wrap);
    }
    if(hint){
      hint.style.display='block';
      const count=Number(summary?.items?.length||0);
      hint.innerHTML=`💰 <b>تعديل مالي للأوردر كله</b><br>الأوردر فيه ${count} أصناف. السعر/المدفوع/المتبقي هنا إجمالي على الأوردر كله، ومش هيتوزع كقيمة بيع على كل صنف لوحده.`;
    }
  }else if(hint){
    hint.style.display='none';
  }
}
async function deleteOrderFile(fileId){
  const id=Number(fileId||0);
  if(!id) return;
  if(!confirm('حذف ملف PDF من الأوردر؟')) return;
  try{
    await authFetch('/order-file/'+id+'/delete',{method:'DELETE'});
    await load();
    const refreshed=ordersData.find(x=>Number(x.id)===Number(currentOrderId)) || {};
    renderEditFilesList(refreshed.files || []);
  }catch(e){ alert(e.message||'تعذر حذف الملف'); }
}

function openEdit(id){
  window.editAddOnlyMode=false;
  window.editFinancialsTouched=false;
  if(window.editSaveBtn) editSaveBtn.style.display='inline-flex';
  if(window.editAddItemBtn){ editAddItemBtn.textContent='➕ إضافة صنف'; editAddItemBtn.className='btn cyan'; }
  currentOrderId=id; const o=ordersData.find(x=>x.id==id) || {};
  const groupFinance=summarizeGroupFinancialsForEdit(o);
  setEditFinanceModeLabel(!!groupFinance, groupFinance);
  e_name.value=o.custName||'';e_phone.value=normalizePhoneDigits(o.custPhone||'');e_address.value=o.custAddress||'';if(window.e_order_page_name)e_order_page_name.value=o.order_page_name||'';e_orderDate.value=o.orderDate||'';e_due_date.value=o.due_date||'';e_priority.value=normalizePriority(o.priority||'عادي');e_l.value=o.l||0;e_w.value=o.w||0;e_g.value=o.g||0;e_qty.value=o.qty||0;e_useReadyStock.value=String(o.useReadyStock||0);e_paper_type.value=normalizePaperType(o.paper_type);e_color.value=o.color||'';e_handle.value=o.handle||'';e_handle_type.value=normalizeHandleType(o.handle_type,o.paper_type);e_has_lamination.checked=Number(o.has_lamination||0)===1;e_lamination_type.value=o.lamination_type||'';syncEditMaterialUI();if(Array.from(e_printType.options).some(opt=>opt.value===String(o.printType||'')))e_printType.value=o.printType||'سادة';e_colorSpecs.value=o.colorSpecs||'';hydrateEditPrintDetails(o);e_total_price.value=groupFinance?groupFinance.total:(getDisplayedOrderTotal(o)||0);e_shipping_cost.value=groupFinance?groupFinance.shipping:(baseOrderShippingCost(o)||0);e_paperGrammage.value=o.paperGrammage||0;e_paymentType.value=groupFinance?(groupFinance.remaining<=0&&groupFinance.total>0?'مدفوع كامل':(groupFinance.paid>0?'عربون':(groupFinance.total>0?'آجل':'لم يتم الدفع'))):(o.paymentType||'لم يتم الدفع');e_paid_amount.value=groupFinance?groupFinance.paid:(o.paid_amount||0);e_remaining_amount.value=groupFinance?groupFinance.remaining:(o.remaining_amount||0);if(window.e_notes)e_notes.value=o.notes||'';updateEditStatusOptions(String(o.useReadyStock||0)==='1', o.status||'أوردر جديد');syncEditReadyStockUI();
  renderEditFilesList(o.files || []);
  if(window.e_designFiles) e_designFiles.value='';
  document.getElementById('editModal').style.display='block'; syncEditPayment();
}
function syncEditPayment(){ let total=Number(e_total_price.value)||0, paid=Number(e_paid_amount.value)||0; const status=e_paymentType.value; if(status==='مدفوع كامل') paid=total; if(status==='لم يتم الدفع') paid=0; if(paid>total) paid=total; if(paid<0) paid=0; e_paid_amount.value=paid; e_remaining_amount.value=Math.max(total-paid,0); e_total_wrap.style.display='block'; e_paid_wrap.style.display='block'; e_remaining_wrap.style.display='block'; if(status==='لم يتم الدفع'){ e_paid_wrap.style.display='none'; e_remaining_wrap.style.display='none'; } else if(status==='مدفوع كامل'){ e_paid_wrap.style.display='none'; e_remaining_wrap.style.display='none'; if(total>0) e_total_wrap.style.display='none'; } }
function markEditFinancialsTouched(){
  if(document.getElementById('editModal')?.style.display==='block') window.editFinancialsTouched=true;
}
['e_total_price','e_paid_amount','e_paymentType','e_shipping_cost']
  .map(id=>document.getElementById(id))
  .filter(Boolean)
  .forEach(input=>{
  input.addEventListener('input',markEditFinancialsTouched);
  input.addEventListener('change',markEditFinancialsTouched);
});
async function saveEdit(){ if(window.editAddOnlyMode){ return addEditAsNewItem(); } const current=ordersData.find(x=>x.id==currentOrderId) || {}; const useReady=String(e_useReadyStock.value||'0')==='1'; if(!useReady&&isBackwardFromCut(current.status,e_status.value)){ alert('لا يمكن الرجوع لحالة سابقة بعد القص'); return; } if(!useReady&&shouldOpenCutPlan(current,e_status.value)){ alert('غير الحالة من القائمة الرئيسية علشان تختار الفرخ وطريقة القص الأول.'); return; } if(useReady) await syncEditReadyStockGusset(); const formData=new FormData(); const body={id:currentOrderId,custName:e_name.value,custPhone:enforceEnglishPhoneInput(e_phone),custAddress:e_address.value,order_page_name:window.e_order_page_name?e_order_page_name.value:'',orderDate:e_orderDate.value,due_date:e_due_date.value,priority:e_priority.value,l:e_l.value,w:e_w.value,g:e_g.value,qty:e_qty.value,paper_type:normalizePaperType(e_paper_type.value),color:isFixedWhitePaperType(e_paper_type.value)?'أبيض':e_color.value,handle:e_handle.value,handle_type:normalizeHandleType(e_handle_type.value,e_paper_type.value),has_lamination:e_has_lamination.checked?'1':'0',lamination_type:e_lamination_type.value,printType:(isCoatedPaperType(e_paper_type.value)&&e_printType.value==='سلك سكرين'?'أوفست':e_printType.value),colorSpecs:composeEditColorSpecs(),total_price:e_total_price.value,shipping_cost:e_shipping_cost.value,paperGrammage:e_paperGrammage.value,paymentType:e_paymentType.value,paid_amount:e_paid_amount.value,status:e_status.value,useReadyStock:e_useReadyStock.value,notes:window.e_notes?e_notes.value:'',group_financial_whole:window.editGroupFinancialMode?'1':'0',financials_touched:window.editFinancialsTouched?'1':'0',preserve_group_financials:(window.editGroupFinancialMode&&!window.editFinancialsTouched)?'1':'0'}; Object.entries(body).forEach(([key,value])=>formData.append(key, value == null ? '' : String(value))); Array.from(e_designFiles?.files || []).forEach(file=>formData.append('designFiles', file)); try{ await authFetch('/update-order',{method:'POST',body:formData}); closeModal('editModal'); load(); }catch(e){ alert(e.message); } }

async function addEditAsNewItem(){
  const current=ordersData.find(x=>Number(x.id)===Number(currentOrderId)) || {};
  if(!currentOrderId){ alert('افتح أوردر للتعديل الأول'); return; }
  const addOnly=!!window.editAddOnlyMode;
  if(!addOnly && !confirm('إضافة البيانات الحالية كصنف جديد لنفس الأوردر؟\nالصنف الأصلي لن يتغير.')) return;
  if(String(e_useReadyStock.value||'0')==='1') await syncEditReadyStockGusset();
  const formData=new FormData();
  const body={
    custName:e_name.value,custPhone:enforceEnglishPhoneInput(e_phone),custAddress:e_address.value,order_page_name:window.e_order_page_name?e_order_page_name.value:'',orderDate:e_orderDate.value,due_date:e_due_date.value,priority:e_priority.value,
    l:e_l.value,w:e_w.value,g:e_g.value,qty:e_qty.value,
    paper_type:normalizePaperType(e_paper_type.value),
    color:isFixedWhitePaperType(e_paper_type.value)?'أبيض':e_color.value,
    handle:e_handle.value,handle_type:normalizeHandleType(e_handle_type.value,e_paper_type.value),
    has_lamination:e_has_lamination.checked?'1':'0',lamination_type:e_lamination_type.value,
    printType:(isCoatedPaperType(e_paper_type.value)&&e_printType.value==='سلك سكرين'?'أوفست':e_printType.value),
    colorSpecs:composeEditColorSpecs(),total_price:e_total_price.value,shipping_cost:e_shipping_cost.value,paperGrammage:e_paperGrammage.value,
    paymentType:e_paymentType.value,paid_amount:e_paid_amount.value,useReadyStock:e_useReadyStock.value,
    notes:window.e_notes?e_notes.value:(current.notes||'')
  };
  Object.entries(body).forEach(([key,value])=>formData.append(key, value == null ? '' : String(value)));
  Array.from(e_designFiles?.files || []).forEach(file=>formData.append('designFiles', file));
  try{
    await authFetch('/add-order-item/'+Number(currentOrderId),{method:'POST',body:formData});
    closeModal('editModal');
    window.editAddOnlyMode=false;
    if(window.editSaveBtn) editSaveBtn.style.display='inline-flex';
    if(window.editAddItemBtn){ editAddItemBtn.textContent='➕ إضافة صنف'; editAddItemBtn.className='btn cyan'; }
    await load();
    alert('تم إضافة الصنف الجديد لنفس الأوردر');
  }catch(e){ alert(e.message); }
}
async function showHistory(id){ const rows=await authFetch('/order-status-history/'+id); historyBox.innerHTML=rows.length?rows.map(r=>`<div style="padding:10px;border-bottom:1px solid #334155"><b>${r.from_status||'بداية'} ← ${r.to_status}</b><br><small>${r.changed_at} | ${r.changed_by||''}</small><br><small>${r.note||''}</small></div>`).join(''):'لا يوجد سجل'; historyModal.style.display='block'; }
async function showGroupHistory(groupCode){
  const items=getGroupItemsByCode(groupCode);
  if(!items.length){ alert('الأوردر غير موجود'); return; }
  const blocks=await Promise.all(items.map(async item=>{
    try{ return { item, rows: await authFetch('/order-status-history/'+item.id) }; }
    catch(_){ return { item, rows: [] }; }
  }));
  historyBox.innerHTML=blocks.map(({item,rows})=>`<div style="padding:12px;border-bottom:1px solid #334155"><b>${esc(groupItemLabel(item))}</b>${rows.length?rows.map(r=>`<div style="padding:10px 0;border-bottom:1px dashed #334155"><b>${esc(r.from_status||'بداية')} ← ${esc(r.to_status||'')}</b><br><small>${esc(r.changed_at||'')} | ${esc(r.changed_by||'')}</small><br><small>${esc(r.note||'')}</small></div>`).join(''):`<div style="margin-top:8px;color:var(--muted)">لا يوجد سجل</div>`}</div>`).join('');
  closeAllActionMenus();
  historyModal.style.display='block';
}
async function delGroupOrder(groupCode){
  const items=getGroupItemsByCode(groupCode);
  if(!items.length) return;
  if(!confirm(`حذف الأوردر بالكامل؟\nعدد الأصناف: ${items.length}`)) return;
  try{
    for(const item of items){
      await authFetch('/delete-order/'+Number(item.id),{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    }
    await load();
  }catch(e){
    alert(e.message||'فشل حذف الأوردر');
    await load();
  }
}
function esc(s){ return String(s==null?'':s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
function toNum(v){ const n=Number(v); return Number.isFinite(n)?n:0; }
function parseDimsFromLabel(label){
  const m=String(label||'').match(/(\d+(?:\.\d+)?)\s*[x×]\s*(\d+(?:\.\d+)?)/i);
  return m ? {w:toNum(m[1]), h:toNum(m[2])} : {w:0,h:0};
}
function getLayoutData(option, plan){
  const labelDims=parseDimsFromLabel(option.paperLabel);
  const layout = option.layoutKey==='pieceByPiece' ? (plan.layouts?.pieceByPiece||{}) : (plan.layouts?.singlePiece||{});
  const sheetW = toNum(option.sheetWidth||option.paperWidth||option.width||option.paper_w||option.sheet_w||option.w||labelDims.w);
  const sheetH = toNum(option.sheetHeight||option.sheetLength||option.paperHeight||option.paperLength||option.length||option.paper_h||option.sheet_h||option.h||labelDims.h);
  const cutW = toNum(option.cutWidth||option.cut_width||option.itemWidth||option.pieceWidth||layout.cutWidth||layout.cut_width||layout.itemWidth||layout.pieceWidth);
  const cutH = toNum(option.cutLength||option.cutHeight||option.cut_length||option.itemHeight||option.pieceHeight||layout.cutLength||layout.cutHeight||layout.cut_length||layout.itemHeight||layout.pieceHeight);
  let cols = toNum(option.cols||option.columns);
  let rows = toNum(option.rows);
  if(!cols && sheetW && cutW) cols = Math.floor(sheetW / cutW);
  if(!rows && sheetH && cutH) rows = Math.floor(sheetH / cutH);
  cols = Math.max(1, cols||1);
  rows = Math.max(1, rows||1);
  const maxByBounds = Math.max(1, Math.floor((sheetW/cutW)||1) * Math.floor((sheetH/cutH)||1));
  const piecesPerSheet = toNum(option.piecesPerSheet||option.pieces_per_sheet||option.countPerSheet||option.pieces) || Math.min(cols*rows, maxByBounds);
  return {sheetW,sheetH,cutW,cutH,cols,rows,piecesPerSheet};
}
function drawSheetSvg(option, order, plan){
  const data=getLayoutData(option, plan);
  const {sheetW,sheetH,cutW,cutH}=data;
  if(!sheetW || !sheetH || !cutW || !cutH) return `<div style="color:#111827;padding:20px;font-weight:700">تعذر رسم التخطيط لأن بيانات المقاس غير مكتملة.</div>`;
  const pad=40;
  const maxW=820, maxH=430;
  const scale=Math.min(maxW/sheetW, maxH/sheetH);
  const width=sheetW*scale, height=sheetH*scale;
  const svgW=width+pad*2+70, svgH=height+pad*2+50;
  const fitCols=Math.max(1, Math.floor(sheetW/cutW));
  const fitRows=Math.max(1, Math.floor(sheetH/cutH));
  const cols=Math.max(1, Math.min(data.cols||fitCols, fitCols));
  const rows=Math.max(1, Math.min(data.rows||fitRows, fitRows));
  const totalPieces=Math.max(1, data.piecesPerSheet || cols*rows);
  let cells='';
  let drawn=0;
  for(let r=0;r<rows && drawn<totalPieces;r++){
    for(let c=0;c<cols && drawn<totalPieces;c++){
      const x=pad+c*cutW*scale, y=pad+r*cutH*scale;
      if(x+cutW*scale<=pad+width+0.1 && y+cutH*scale<=pad+height+0.1){
        drawn++;
        const cx=x+(cutW*scale/2), cy=y+(cutH*scale/2);
        cells += `<rect x="${x}" y="${y}" width="${cutW*scale}" height="${cutH*scale}" fill="#cbd5e1" stroke="#111827" stroke-width="1.2"/>`;
        cells += `<text x="${x+10}" y="${y+18}" font-size="14" fill="#111827">${cutW}</text>`;
        cells += `<text x="${x+14}" y="${y+(cutH*scale/2)}" font-size="14" fill="#111827" transform="rotate(-90 ${x+14} ${y+(cutH*scale/2)})">${cutH}</text>`;
        cells += `<text x="${cx}" y="${cy}" font-size="16" text-anchor="middle" fill="#334155" font-weight="700">أوردر رقم ${esc(planOrderDisplayNo(order))}</text>`;
      }
    }
  }
  const usedW=Math.min(fitCols*cutW, sheetW), usedH=Math.min(fitRows*cutH, sheetH);
  const wasteRight=Math.max(sheetW-usedW,0), wasteBottom=Math.max(sheetH-usedH,0);
  let waste='';
  if(wasteRight>0){
    waste += `<rect x="${pad+usedW*scale}" y="${pad}" width="${wasteRight*scale}" height="${usedH*scale}" fill="#e5e7eb" stroke="#ef4444" stroke-width="1"/>`;
    waste += `<text x="${pad+usedW*scale + (wasteRight*scale/2)}" y="${pad+usedH*scale+20}" text-anchor="middle" font-size="12" fill="#ef4444">${wasteRight}</text>`;
  }
  if(wasteBottom>0){
    waste += `<rect x="${pad}" y="${pad+usedH*scale}" width="${sheetW*scale}" height="${wasteBottom*scale}" fill="#e5e7eb" stroke="#ef4444" stroke-width="1"/>`;
    waste += `<text x="${pad+sheetW*scale-10}" y="${pad+usedH*scale + (wasteBottom*scale/2)+5}" text-anchor="end" font-size="12" fill="#ef4444">${wasteBottom}</text>`;
  }
  return `<svg width="${svgW}" height="${svgH}" viewBox="0 0 ${svgW} ${svgH}" xmlns="http://www.w3.org/2000/svg">
    <rect x="0" y="0" width="${svgW}" height="${svgH}" fill="#ffffff"/>
    <text x="${pad}" y="22" font-size="22" fill="#111827" font-weight="700">${esc(option.paperLabel || ('فرخ '+sheetW+' × '+sheetH))}</text>
    <rect x="${pad}" y="${pad}" width="${width}" height="${height}" fill="#f8fafc" stroke="#111827" stroke-width="2"/>
    ${cells}
    ${waste}
    <line x1="${pad}" y1="${pad+height+12}" x2="${pad+width}" y2="${pad+height+12}" stroke="#ef4444"/>
    <line x1="${pad}" y1="${pad+height+8}" x2="${pad}" y2="${pad+height+16}" stroke="#ef4444"/>
    <line x1="${pad+width}" y1="${pad+height+8}" x2="${pad+width}" y2="${pad+height+16}" stroke="#ef4444"/>
    <text x="${pad+width/2}" y="${pad+height+30}" text-anchor="middle" font-size="18" fill="#ef4444">${sheetW}</text>
    <line x1="${pad+width+12}" y1="${pad}" x2="${pad+width+12}" y2="${pad+height}" stroke="#ef4444"/>
    <line x1="${pad+width+8}" y1="${pad}" x2="${pad+width+16}" y2="${pad}" stroke="#ef4444"/>
    <line x1="${pad+width+8}" y1="${pad+height}" x2="${pad+width+16}" y2="${pad+height}" stroke="#ef4444"/>
    <text x="${pad+width+32}" y="${pad+height/2}" text-anchor="middle" font-size="18" fill="#ef4444" transform="rotate(90 ${pad+width+32} ${pad+height/2})">${sheetH}</text>
  </svg>`;
}
async function showDetails(id,forCut=false,targetStatus='في القص',flowMeta=null){
  try{
    const order=ordersData.find(x=>x.id==id) || {};
    if(Number(order.useReadyStock||0)){
      alert('هذا الأوردر مسحوب من مخزن الشنط الجاهزة ولا يحتاج اختيار فرخ أو قص.');
      return;
    }
    const plan=await authFetch('/order-paper-plan/'+id);
    if(!plan || !plan.order) throw new Error('تعذر تحميل تفاصيل القص');
    const o=plan.order;
    const actualCut = (!forCut && plan.actualCut) ? plan.actualCut : null;
    const actualRows=(!forCut&&Array.isArray(plan.actualCutAllocations))?plan.actualCutAllocations:[];
    const actualSplit=actualRows.length>1?{layoutKey:actualRows[0]?.layoutKey,layoutLabel:actualRows[0]?.layoutLabel,totalSheets:actualRows.reduce((s,r)=>s+Number(r.sheetsToUse||r.neededSheets||0),0),totalKg:actualRows.reduce((s,r)=>s+Number(r.kgToUse||0),0),totalCost:actualRows.reduce((s,r)=>s+Number(r.costToUse||0),0),usesAlternativeGram:actualRows.some(r=>Number(r.isAlternative||0)===1),allocations:actualRows}:null;
    const pool=actualCut ? [actualCut] : getPlanDisplayPool(plan,false);
    const best=actualCut || chooseBestPlanOption(pool);
    window.currentPlan={plan,order:o,poolOptions:pool,best,selected:actualSplit?null:best,selectedSplit:actualSplit,selectedGram:actualCut?getOptionGramValue(actualCut)||null:null,forCut,targetStatus,flowMeta,showActualCutOnly:!!actualCut,showAlternatives:false};
    if(actualSplit) renderSplitPlanSelection();
    else if(best) renderPlanSelectionMeta(best);
    else detailsMeta.innerHTML=`<div><b>المطلوب:</b> ${esc(plan.requiredPaper?.paperType||o.paper_type||'')} | ${esc(plan.requiredPaper?.color||o.color||'')} | ${esc(plan.requiredPaper?.grammage||o.paperGrammage||'-')} جم</div><div style="margin-top:8px;color:#fca5a5">لا يوجد فرخ مطابق رصيده يكفي كمية الأوردر. يمكنك فتح الورق البديل لو متاح.</div>`;
    renderPlanChoices();
    if(!actualSplit) detailsBox.innerHTML=best?drawSheetSvg(best,o,plan):'';
    saveCutBtn.style.display=forCut && !actualCut && best?'inline-flex':'none';
    detailsModal.style.display='block';
  }catch(e){ alert(e.message); }
}
async function delOrder(id){ const order=ordersData.find(x=>x.id==id)||{}; if(!confirm('حذف الأوردر؟')) return; try{ const body={}; const canAskDeleteReturnedBagStock=!Number(order.useReadyStock||0) && (order.status||'')==='مرتجع' && Number(order.bag_returned_to_stock||0)===1; if(canAskDeleteReturnedBagStock){ body.deleteReturnedBagStock=confirm('الأوردر حالته مرتجع وتمت إضافة الكمية لمخزن الشنط الجاهز.\nهل تريد حذف نفس الكمية من مخزن الشنط الجاهز أيضًا؟'); } await authFetch('/delete-order/'+id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); load(); }catch(e){ alert(e.message); } }

function updatePrintUi(){
  document.body.classList.toggle('print-mode', printMode);
  const printBtn=document.getElementById('printBtn');
  const cancelBtn=document.getElementById('cancelPrintBtn');
  const all=document.getElementById('printSelectAll');
  if(printMode){
    printBtn.innerHTML='✅ اطبع المحدد';
    cancelBtn.style.display='inline-flex';
    if(all) all.checked=false;
  }else{
    printBtn.innerHTML='🖨️ طباعة';
    cancelBtn.style.display='none';
    if(all) all.checked=false;
    document.querySelectorAll('.row-print-check').forEach(ch=>ch.checked=false);
    document.querySelectorAll('tr[id^="row-"]').forEach(r=>r.classList.remove('print-row-selected'));
  }
}
function syncPrintRow(key){
  const row=document.getElementById(rowDomIdFromSelectionKey(key));
  const ch=row?.querySelector('.row-print-check');
  if(row && ch) row.classList.toggle('print-row-selected', ch.checked);
  syncSelectAllState();
}
function syncAllPrintedRows(){
  document.querySelectorAll('.row-print-check').forEach(ch=>{
    const row=ch.closest('tr');
    if(row) row.classList.toggle('print-row-selected', ch.checked);
  });
  syncSelectAllState();
}
function syncSelectAllState(){
  const boxes=[...document.querySelectorAll('.row-print-check')];
  const all=document.getElementById('printSelectAll');
  if(!all) return;
  all.checked = boxes.length>0 && boxes.every(b=>b.checked);
}
function toggleSelectAllForPrint(source){
  document.querySelectorAll('.row-print-check').forEach(ch=>{
    ch.checked=source.checked;
    const row=ch.closest('tr');
    if(row) row.classList.toggle('print-row-selected', ch.checked);
  });
}
function cancelPrintMode(){
  printMode=false;
  updatePrintUi();
}
function getSelectedLogicalOrders(){
  const selectedKeys=[...document.querySelectorAll('.row-print-check:checked')].map(ch=>String(ch.value));
  return filteredOrders.filter(o=>selectedKeys.includes(selectionKeyForOrder(o)));
}
function handlePrintOrders(){
  if(!printMode){
    printMode=true;
    updatePrintUi();
    alert('حدد الأوردرات اللي عايز تطبعها ثم اضغط زر ✅ اطبع المحدد');
    return;
  }
  const selectedOrders=getSelectedLogicalOrders();
  if(!selectedOrders.length){
    alert('حدد الأوردرات الأول');
    return;
  }
  printSelectionSnapshot=selectedOrders;
  document.getElementById('printChoiceModal').style.display='block';
}
function selectedOrdersForPrinting(){
  return Array.isArray(printSelectionSnapshot) && printSelectionSnapshot.length ? printSelectionSnapshot : getSelectedLogicalOrders();
}
function printSelectedAsList(){
  const selectedOrders=selectedOrdersForPrinting();
  if(!selectedOrders.length){ alert('حدد الأوردرات الأول'); return; }
  closeModal('printChoiceModal');
  printSelectedOrders(selectedOrders);
}
function printSelectedAsInvoices(){
  const selectedOrders=selectedOrdersForPrinting();
  if(!selectedOrders.length){ alert('حدد الأوردرات الأول'); return; }
  closeModal('printChoiceModal');
  printSelectedInvoices(selectedOrders);
}
function buildRequiredText(o){
  const bagType = (o.handle||'') === 'بيد' ? 'بيد' : 'بدون يد';
  const printType = o.printType || 'سادة';
  const isPrinted = printType === 'سادة' ? 'سادة' : 'مطبوع';
  let printColor = o.colorSpecs || '-';
  if(printType === 'سادة') printColor = '-';
  return {
    size: `${o.l||0} × ${o.w||0} × ${o.g||0}`,
    bagType,
    bagColor: o.color || '-',
    printType,
    printColor,
    isPrinted,
    qty: o.qty || 0
  };
}
function renderPrintItems(order){
  return orderItems(order).map((item,idx)=>{
    const req=buildRequiredText(item);
    return `<div class="req-grid" style="margin-bottom:${idx===orderItems(order).length-1?0:10}px"><div><span>الصنف</span><b>${order.is_group?`صنف ${idx+1}`:'الصنف الوحيد'}</b></div><div><span>المقاس</span><b>${esc(req.size)}</b></div><div><span>الشنطة</span><b>${esc(req.bagType)}</b></div><div><span>اللون</span><b>${esc(req.bagColor)}</b></div><div><span>الحالة</span><b>${esc(req.isPrinted)}</b></div><div><span>الطباعة</span><b>${esc(req.printType)}</b></div><div><span>لون الطباعة</span><b>${esc(req.printColor)}</b></div><div><span>الكمية</span><b>${esc(req.qty)}</b></div></div>`;
  }).join('');
}
function printSelectedOrders(selectedOrders){
  const now = new Date();
  const printDate = now.toLocaleDateString('ar-EG') + ' - ' + now.toLocaleTimeString('ar-EG');
  const rows = selectedOrders.map((o,idx)=>{
    const totalQty=orderItems(o).reduce((sum,item)=>sum+(Number(item.qty)||0),0);
    return `<tr>
      <td>${idx+1}</td>
      <td>
        <div class="customer-name">${esc(o.custName||'')}</div>
        <div class="sub-line">${esc(o.custAddress||'-')}</div>
        <div class="sub-line">${esc(o.custPhone||'-')}</div>
        ${o.is_group?`<div class="sub-line" style="color:#166534;font-weight:700">أوردر مجمّع - ${orderItems(o).length} أصناف</div>`:''}
      </td>
      <td>${renderPrintItems(o)}</td>
      <td><div class="qty-box">${esc(totalQty)}</div></td>
    </tr>`;
  }).join('');
  const html = `<!DOCTYPE html>
  <html dir="rtl">
  <head>
    <meta charset="UTF-8">
    <title>ليست الأوردرات</title>
    <style>
      body{font-family:'Segoe UI',Tahoma,sans-serif;background:#fff;color:#111827;direction:rtl;margin:0;padding:26px}
      .sheet{max-width:1100px;margin:0 auto}
      .top{display:flex;justify-content:space-between;align-items:flex-end;gap:20px;margin-bottom:22px;padding-bottom:14px;border-bottom:3px solid #0f172a}
      .title h1{margin:0;font-size:30px}
      .title p{margin:8px 0 0;color:#475569;font-size:14px}
      .meta{font-size:14px;color:#334155;text-align:left}
      table{width:100%;border-collapse:collapse}
      th,td{border:1.6px solid #cbd5e1;padding:12px;vertical-align:top}
      th{background:#0f172a;color:#fff;font-size:15px}
      td{font-size:14px}
      .customer-name{font-size:18px;font-weight:800;margin-bottom:8px;color:#0f172a}
      .sub-line{color:#334155;line-height:1.8}
      .req-grid{display:grid;grid-template-columns:repeat(2,minmax(160px,1fr));gap:8px 14px}
      .req-grid div{background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:8px 10px}
      .req-grid span{display:block;font-size:12px;color:#64748b;margin-bottom:4px}
      .req-grid b{font-size:14px;color:#111827}
      .qty-box{min-width:72px;text-align:center;font-size:22px;font-weight:800;background:#eff6ff;border:2px solid #93c5fd;border-radius:14px;padding:10px 8px}
      .footer{margin-top:18px;display:flex;justify-content:space-between;color:#475569;font-size:13px}
      @media print{ body{padding:0} .sheet{max-width:none} }
    </style>
  </head>
  <body>
    <div class="sheet">
      <div class="top">
        <div class="title">
          <h1>🖨️ ليست الأوردرات</h1>
          <p>بيان تشغيل وطباعة للأوردرات المحددة</p>
        </div>
        <div class="meta">
          <div><b>عدد الأوردرات:</b> ${selectedOrders.length}</div>
          <div><b>تاريخ الطباعة:</b> ${printDate}</div>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th style="width:52px">م</th>
            <th style="width:30%">العميل</th>
            <th>المطلوب</th>
            <th style="width:90px">الكمية</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
      <div class="footer">
        <div>ملاحظات التشغيل: ..............................................................</div>
        <div>توقيع المسؤول: __________________</div>
      </div>
    </div>
    <script>window.onload = function(){ window.print(); };<\/script>
  </body>
  </html>`;
  const win=window.open('', '_blank');
  win.document.open();
  win.document.write(html);
  win.document.close();
}

function hashStringSeed(text){
  let h=2166136261;
  String(text||'').split('').forEach(ch=>{ h ^= ch.charCodeAt(0); h = Math.imul(h, 16777619); });
  return h >>> 0;
}
function renderBarcodeSvg(text,width=520,height=82){
  const seed=hashStringSeed(text||'0');
  let x=10;
  const bars=[];
  const maxWidth=width-20;
  const pushBar=(w,fill=true)=>{ if(fill) bars.push(`<rect x="${x}" y="6" width="${w}" height="${height-12}" fill="#000"/>`); x+=w; };
  [2,1,2,1,2,1].forEach((w,i)=>pushBar(w,i%2===0));
  const raw=String(text||'0').replace(/\s+/g,'') || '0';
  raw.split('').forEach((ch,idx)=>{
    const code=ch.charCodeAt(0)+idx+(seed%7);
    const pattern=[1+(code%3),1+((code>>2)%2),2+((code>>3)%3),1+((code>>1)%2),1+((code>>4)%3),1+((code>>5)%2)];
    pattern.forEach((w,i)=>pushBar(w + (i%3===0?1:0), i%2===0));
    pushBar(2,false);
  });
  [2,1,2,1,2,1].forEach((w,i)=>pushBar(w,i%2===0));
  if(x<maxWidth){ while(x<maxWidth-12){ pushBar(1 + ((x+seed)%3), ((x+seed)%2)===0); } }
  return `<svg viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">${bars.join('')}</svg>`;
}
function renderPseudoQrSvg(text,size=108){
  const grid=21;
  const cell=Math.floor(size/grid);
  const seed=hashStringSeed(text||'0');
  const rects=[];
  const finder=(ox,oy)=>{
    rects.push(`<rect x="${ox*cell}" y="${oy*cell}" width="${cell*7}" height="${cell*7}" fill="#000"/>`);
    rects.push(`<rect x="${(ox+1)*cell}" y="${(oy+1)*cell}" width="${cell*5}" height="${cell*5}" fill="#fff"/>`);
    rects.push(`<rect x="${(ox+2)*cell}" y="${(oy+2)*cell}" width="${cell*3}" height="${cell*3}" fill="#000"/>`);
  };
  finder(0,0); finder(grid-7,0); finder(0,grid-7);
  for(let y=0;y<grid;y++){
    for(let x=0;x<grid;x++){
      const inFinder=(x<7&&y<7)||(x>=grid-7&&y<7)||(x<7&&y>=grid-7);
      if(inFinder) continue;
      const v=((seed + x*97 + y*57 + (x*y*13)) ^ (x<<4) ^ (y<<2)) & 1;
      if(v) rects.push(`<rect x="${x*cell}" y="${y*cell}" width="${cell}" height="${cell}" fill="#000"/>`);
    }
  }
  return `<svg viewBox="0 0 ${cell*grid} ${cell*grid}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect width="100%" height="100%" fill="#fff"/>${rects.join('')}</svg>`;
}
function buildLabelDescription(order){
  const items=orderItems(order);
  return items.map((item,idx)=>{
    const bits=[];
    if(order.is_group) bits.push(`صنف ${idx+1}`);
    bits.push(`${Number(item.qty||0)} شنطة`);
    bits.push(`مقاس ${Number(item.w||0)}*${Number(item.l||0)}${Number(item.g||0)?`*${Number(item.g||0)}`:''}`);
    if(String(item.color||'').trim()) bits.push(String(item.color).trim());
    if(String(item.printType||'').trim()) bits.push(String(item.printType).trim());
    return bits.join(' ');
  }).join(' | ');
}
function labelReference(order){
  return String(order?.bosta_business_reference || order?.group_code || `ORDER-${order?.id||''}`).trim() || `ORDER-${order?.id||''}`;
}
function labelTrackingNumber(order){
  return String(order?.bosta_tracking_number || order?.bosta_delivery_id || labelReference(order) || order?.id || '').trim();
}
function invoiceNumber(order){
  return order?.is_group ? String(order.group_code || `GROUP-${order?.id||''}`) : `INV-${order?.id||''}`;
}
function invoiceStatusText(order){
  const items = orderItems(order);
  const statuses = [...new Set(items.map(item=>normalizedStatusText(item.status)))];
  return statuses.length === 1 ? statuses[0] : 'حالات متعددة';
}
function uniqueInvoiceStrings(values=[]){
  return [...new Set((Array.isArray(values) ? values : [values]).map(v=>String(v == null ? '' : v).trim()).filter(Boolean))];
}
function invoicePaymentSummary(order){
  if(order?.is_group){
    return {
      total: Number(order.total_price_display || 0),
      paid: Number(order.paid_total || 0),
      remaining: Number(order.remaining_total || 0),
      shipping: Number(order.shipping_total || 0)
    };
  }
  return {
    total: Number(getDisplayedOrderTotal(order) || 0),
    paid: Number(order.paid_amount || 0),
    remaining: Number(order.remaining_amount || 0),
    shipping: Number(orderShippingCost(order) || 0)
  };
}
function invoicePaymentTypeText(order){
  if(!order?.is_group) return String(order?.paymentType || '').trim() || 'لم يتم الدفع';
  const summary = invoicePaymentSummary(order);
  if(summary.total > 0 && summary.remaining <= 0) return 'مدفوع كامل';
  if(summary.paid > 0) return 'عربون';
  return summary.total > 0 ? 'آجل' : 'لم يتم الدفع';
}
function invoiceSourceSummary(order){
  const source = sourceTypeForDisplay(order);
  if(source === 'mixed') return 'تصنيع جديد + مخزن جاهز';
  return source === 'ready' ? 'من المخزن الجاهز' : 'تصنيع جديد';
}
function invoiceFilesList(order){
  return collectUniqueFiles(orderItems(order));
}
function invoiceFilesText(order){
  const files = invoiceFilesList(order);
  return files.length ? files.map(fileDisplayName).join('، ') : 'لا توجد';
}
function invoiceTrackingText(order){
  const direct = String(order?.bosta_tracking_number || order?.bosta_delivery_id || '').trim();
  if(direct) return direct;
  const representative = groupRepresentativeItem(order) || orderItems(order)[0] || {};
  return String(representative?.bosta_tracking_number || representative?.bosta_delivery_id || '').trim() || '-';
}
function invoiceOrderDateText(order){
  return String(order?.orderDate || orderItems(order)[0]?.orderDate || '-').trim() || '-';
}
function invoiceDueDateText(order){
  return String(order?.due_date || orderItems(order)[0]?.due_date || '').trim() || '-';
}
function collectInvoiceNotes(order){
  const orderLevel = uniqueInvoiceStrings([order?.urgent_note, order?.notes, order?.bosta_notes]);
  const itemLevel = [];
  orderItems(order).forEach((item, idx)=>{
    uniqueInvoiceStrings([item?.urgent_note, item?.notes, item?.bosta_notes]).forEach(text=>{
      itemLevel.push(order?.is_group ? `صنف ${idx+1}: ${text}` : text);
    });
  });
  return uniqueInvoiceStrings([...orderLevel, ...itemLevel]);
}
function renderInvoiceItemSpecs(item){
  const specs = [
    ['نوع الورق', normalizePaperType(item?.paper_type)],
    ['اللون', String(item?.color || '').trim() || '-'],
    ['اليد', `${String(item?.handle || '').trim() || '-'}${item?.handle_type ? ' - '+String(item.handle_type).trim() : ''}`],
    ['السلوفان', Number(item?.has_lamination||0) ? (String(item?.lamination_type||'').trim() || 'نعم') : 'لا'],
    ['الطباعة', String(item?.printType || 'سادة').trim() || 'سادة'],
    ['تفاصيل الطباعة', String(item?.colorSpecs || '').trim() || '-'],
    ['جرام الورق', formatPaperGramText(item?.paperGrammage) || '-'],
    ['المصدر', invoiceSourceSummary(item)],
    ['الحالة', normalizedStatusText(item?.status)]
  ];
  return specs.map(([label, value])=>`<div><b>${esc(label)}:</b> ${esc(value)}</div>`).join('');
}
function renderInvoiceItemsRows(order){
  return orderItems(order).map((item, idx)=>{
    const lineTotal = order?.is_group ? Number(item.total_price || 0) : Number(getDisplayedOrderTotal(item) || 0);
    const qty = Number(item.qty || 0);
    const unitPrice = qty > 0 ? +(lineTotal / qty).toFixed(2) : 0;
    const size = `${Number(item.l||0)} × ${Number(item.w||0)} × ${Number(item.g||0)}`;
    return `<tr>
      <td>${idx+1}</td>
      <td>${order?.is_group ? `صنف ${idx+1}` : 'الشنطة'}</td>
      <td>${esc(size)}</td>
      <td class="details-cell">${renderInvoiceItemSpecs(item)}</td>
      <td>${qty}</td>
      <td>${fmtMoney(unitPrice)}</td>
      <td>${fmtMoney(lineTotal)}</td>
    </tr>`;
  }).join('');
}
function renderInvoiceCard(order, printedAt=''){
  const summary = invoicePaymentSummary(order);
  const filesText = invoiceFilesText(order);
  const notes = collectInvoiceNotes(order);
  const noteText = notes.length ? notes.join(' | ') : 'لا توجد ملاحظات';
  const itemCount = orderItems(order).length;
  const totalQty = orderItems(order).reduce((sum, item)=>sum + (Number(item.qty) || 0), 0);
  const shippingBlock = summary.shipping > 0 ? `<div class="summary-row"><span>تكلفة الشحن</span><b>${fmtMoney(summary.shipping)}</b></div>` : '';
  return `<div class="invoice-half">
    <div class="invoice-card">
      <div class="invoice-head">
        <div>
          <div class="brand">Ahmed</div>
          <div class="sub-brand">فاتورة أوردر / Order Invoice</div>
        </div>
        <div class="invoice-meta">
          <div><span>رقم الفاتورة</span><b>${esc(invoiceNumber(order))}</b></div>
          <div><span>تاريخ الطباعة</span><b>${esc(printedAt)}</b></div>
          <div><span>الحالة</span><b>${esc(invoiceStatusText(order))}</b></div>
        </div>
      </div>
      <div class="chips-row">
        <div class="pill">${order?.is_group ? `أوردر مجمّع (${itemCount} أصناف)` : 'أوردر فردي'}</div>
        <div class="pill">العميل: ${esc(order.custName || '-')}</div>
        <div class="pill">التليفون: ${esc(order.custPhone || '-')}</div>
        <div class="pill">إجمالي الكمية: ${esc(totalQty || 0)}</div>
      </div>
      <div class="customer-grid">
        <div class="box"><span>العنوان</span><b>${esc(order.custAddress || '-')}</b></div>
        <div class="box"><span>تاريخ الأوردر</span><b>${esc(invoiceOrderDateText(order))}</b></div>
        <div class="box"><span>ميعاد التسليم</span><b>${esc(invoiceDueDateText(order))}</b></div>
        <div class="box"><span>الأولوية</span><b>${esc(normalizePriority(order.priority || 'عادي'))}</b></div>
        <div class="box"><span>حالة الدفع</span><b>${esc(invoicePaymentTypeText(order))}</b></div>
        <div class="box"><span>مرجع الشحنة</span><b>${esc(labelReference(order))}</b></div>
        <div class="box"><span>رقم التتبع</span><b>${esc(invoiceTrackingText(order))}</b></div>
        <div class="box"><span>مصدر التنفيذ</span><b>${esc(invoiceSourceSummary(order))}</b></div>
      </div>
      <table class="invoice-table">
        <thead>
          <tr>
            <th>#</th>
            <th>البيان</th>
            <th>المقاس</th>
            <th>التفاصيل</th>
            <th>الكمية</th>
            <th>سعر الوحدة</th>
            <th>الإجمالي</th>
          </tr>
        </thead>
        <tbody>${renderInvoiceItemsRows(order)}</tbody>
      </table>
      <div class="invoice-bottom">
        <div class="notes-box">
          <div class="notes-title">تفاصيل إضافية</div>
          <div class="notes-value"><b>ملاحظات:</b> ${esc(noteText)}</div>
          <div class="notes-value" style="margin-top:6px"><b>ملفات التصميم:</b> ${esc(filesText)}</div>
        </div>
        <div class="summary-box">
          <div class="summary-row"><span>إجمالي البيع</span><b>${fmtMoney(summary.total)}</b></div>
          ${shippingBlock}
          <div class="summary-row"><span>المدفوع</span><b>${fmtMoney(summary.paid)}</b></div>
          <div class="summary-row total"><span>المتبقي</span><b>${fmtMoney(summary.remaining)}</b></div>
        </div>
      </div>
      <div class="invoice-foot">
        <span>شكراً لتعاملكم معنا</span>
        <span>توقيع المسؤول: __________________</span>
      </div>
    </div>
  </div>`;
}
function printSelectedInvoices(selectedOrders){
  const now = new Date();
  const printedAt = now.toLocaleDateString('ar-EG') + ' - ' + now.toLocaleTimeString('ar-EG');
  const pages = [];
  for(let i=0; i<selectedOrders.length; i+=2){
    const pair = selectedOrders.slice(i, i+2);
    const cards = pair.map(order => renderInvoiceCard(order, printedAt)).join('');
    pages.push(`<section class="invoice-sheet">${cards}${pair.length < 2 ? '<div class="invoice-half blank"></div>' : ''}</section>`);
  }
  const html = `<!DOCTYPE html>
  <html dir="rtl">
  <head>
    <meta charset="UTF-8">
    <title>فواتير الأوردرات</title>
    <style>
      @page{size:A4 portrait;margin:8mm}
      body{font-family:'Segoe UI',Tahoma,sans-serif;background:#f8fafc;color:#0f172a;direction:rtl;margin:0}
      .invoice-sheet{page-break-after:always;display:grid;grid-template-rows:1fr 1fr;gap:8mm;min-height:281mm}
      .invoice-sheet:last-child{page-break-after:auto}
      .invoice-half{min-height:0}
      .invoice-card{height:100%;background:#fff;border:1px solid #cbd5e1;border-radius:16px;padding:10px 12px;box-shadow:0 10px 30px rgba(15,23,42,.08);overflow:hidden}
      .invoice-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;padding-bottom:8px;border-bottom:2px solid #e2e8f0}
      .brand{font-size:20px;font-weight:900;letter-spacing:.5px}
      .sub-brand{color:#475569;font-size:10px;margin-top:4px}
      .invoice-meta{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:6px;min-width:50%}
      .invoice-meta div,.box,.pill,.notes-box,.summary-box{border:1px solid #e2e8f0;border-radius:10px;background:#f8fafc}
      .invoice-meta div{padding:6px 8px}
      .invoice-meta span,.box span{display:block;color:#64748b;font-size:10px;margin-bottom:4px}
      .invoice-meta b,.box b{font-size:11px;line-height:1.5}
      .chips-row{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
      .pill{padding:5px 8px;font-size:10px;font-weight:700}
      .customer-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px;margin-top:8px}
      .box{padding:7px 8px;min-height:48px}
      .invoice-table{width:100%;border-collapse:collapse;margin-top:8px}
      .invoice-table th,.invoice-table td{border:1px solid #cbd5e1;padding:5px 4px;text-align:center;vertical-align:top}
      .invoice-table th{background:#0f172a;color:#fff;font-size:10px}
      .invoice-table td{font-size:10px}
      .invoice-table .details-cell{text-align:right;line-height:1.55}
      .invoice-bottom{display:grid;grid-template-columns:1.35fr .65fr;gap:8px;margin-top:8px}
      .notes-box{padding:8px 10px;min-height:78px}
      .notes-title{font-size:11px;font-weight:900;margin-bottom:6px}
      .notes-value{line-height:1.6;font-size:10px;color:#334155;white-space:pre-wrap;word-break:break-word}
      .summary-box{padding:8px 10px;display:flex;flex-direction:column;gap:6px}
      .summary-row{display:flex;justify-content:space-between;gap:8px;font-size:10px}
      .summary-row.total{padding-top:6px;border-top:2px dashed #cbd5e1;font-size:12px;font-weight:900}
      .invoice-foot{margin-top:8px;padding-top:6px;border-top:1px dashed #cbd5e1;display:flex;justify-content:space-between;gap:8px;color:#475569;font-size:10px}
      .blank{background:#fff;border:1px dashed #cbd5e1;border-radius:16px}
      @media print{body{background:#fff}.invoice-card{box-shadow:none}.invoice-sheet{height:281mm}}
    </style>
  </head>
  <body>${pages.join('')}<script>window.onload=function(){window.print();};<\/script></body>
  </html>`;
  const win=window.open('', '_blank');
  win.document.open();
  win.document.write(html);
  win.document.close();
}

function handleExportExcel(){
  if(!printMode){
    printMode=true;
    updatePrintUi();
    alert('حدد الأوردرات اللي عايز تصدرها Excel ثم اضغط زر 📊 Excel مرة تانية');
    return;
  }
  const selectedOrders=getSelectedLogicalOrders();
  if(!selectedOrders.length){
    alert('حدد الأوردرات الأول');
    return;
  }
  exportOrdersExcel(selectedOrders);
}

function exportOrdersExcel(list){
  const headers=[
    'اسم العميل',
    'العنوان',
    'التليفون',
    'المقاس',
    'مصدر الشنطة',
    'نوع الشنطة',
    'لون الشنطة',
    'نوع الورق',
    'السلوفان',
    'نوع الطباعة',
    'لون الطباعة',
    'الكمية'
  ];

  const rows=list.map(o=>{
    const items=orderItems(o);
    const sourceText=sourceTypeForDisplay(o)==='mixed' ? 'مختلط (مخزن + تصنيع جديد)' : (sourceTypeForDisplay(o)==='ready' ? 'مخزن الشنط الجاهزة' : 'تصنيع جديد');
    return [
      o.custName||'',
      o.custAddress||'',
      o.custPhone||'',
      items.map(item=>`${item.l||0}x${item.w||0}x${item.g||0}`).join(' | '),
      sourceText,
      items.map(item=>(item.handle||'')==='بيد'?'بيد':'بدون يد').join(' | '),
      items.map(item=>item.color||'').join(' | '),
      items.map(item=>normalizePaperType(item.paper_type)).join(' | '),
      items.map(item=>Number(item.has_lamination||0)?(item.lamination_type||'نعم'):'لا').join(' | '),
      items.map(item=>item.printType||'سادة').join(' | '),
      items.map(item=>(item.printType||'سادة')==='سادة' ? '-' : (item.colorSpecs||'-')).join(' | '),
      items.reduce((sum,item)=>sum+(Number(item.qty)||0),0)
    ];
  });

  let csv=headers.join(';')+'\n';
  rows.forEach(r=>{ csv+=r.map(v=>`"${String(v).replace(/"/g,'""')}"`).join(';')+'\n'; });

  const blob=new Blob(['\uFEFF'+csv],{type:'text/csv;charset=utf-8;'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;
  a.download='orders_export.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function toggleManualChoices(btn){
  const box=document.getElementById('manualChoices');
  if(!box) return;
  const open = box.style.display==='flex';
  box.style.display = open ? 'none' : 'flex';
  btn.innerText = open ? 'اختار بنفسك' : 'إخفاء الاختيارات';
}
function selectPlanOption(encoded){
  if(!window.currentPlan || window.currentPlan.showActualCutOnly) return;
  const opt=JSON.parse(decodeURIComponent(encoded));
  window.currentPlan.selected=opt;
  window.currentPlan.selectedSplit=null;
  window.currentPlan.selectedGram=getOptionGramValue(opt)||null;
  renderPlanSelectionMeta(opt);
  detailsBox.innerHTML = drawSheetSvg(opt, window.currentPlan.order, window.currentPlan.plan);
  saveCutBtn.style.display=window.currentPlan.forCut?'inline-flex':'none';
  renderPlanChoices();
}
async function saveCutFromPlan(){
  if(!window.currentPlan||(!window.currentPlan.selected&&!window.currentPlan.selectedSplit)) return;
  try{
    const targetStatus=window.currentPlan.targetStatus||'في القص';
    const activePlan=window.currentPlan;
    if(activePlan.selectedSplit){
      const split=activePlan.selectedSplit;
      const allocations=Array.isArray(split.allocations)?split.allocations:[];
      const summary=allocations.map(row=>`${row.paperLabelFull||row.paperLabel}: ${row.sheetsToUse||row.neededSheets} فرخ`).join('\n');
      const alt=Number(split.usesAlternativeGram||0)===1;
      if(!confirm(`سيتم استكمال ورق الأوردر من ${allocations.length} مقاسات أفرخ:\n${summary}\n\nالإجمالي ${split.totalSheets} فرخ.${alt?'\nالخطة تشمل جرامًا بديلًا.':''}\nلن يتم الخصم إلا بالكامل، ولو فشلت حركة واحدة ستُلغى العملية كلها. هل توافق؟`)) return;
      await authFetch('/cut-paper',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({paper_id:allocations[0]?.paperId,order_id:activePlan.order.id,layoutKey:split.layoutKey,allow_alternative:alt?1:0,allocations:allocations.map(row=>({paper_id:row.paperId,layoutKey:row.layoutKey||split.layoutKey,sheetsToUse:row.sheetsToUse||row.neededSheets}))})});
    }else{
      const opt=activePlan.selected;
      const usingAlternative=Number(opt.isAlternative||0)===1;
      if(usingAlternative && !confirm(`الفرخ المختار بديل عن جرام الأوردر (${getOptionGramValue(opt)} جم).\nسيتم خصمه وتسجيل الجرام الفعلي على الأوردر. هل تريد المتابعة؟`)) return;
      await authFetch('/cut-paper',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({paper_id:opt.paperId,order_id:activePlan.order.id,layoutKey:opt.layoutKey,sheets:opt.neededSheets,allow_alternative:usingAlternative?1:0})});
    }
    const body={id:activePlan.order.id,status:targetStatus};
    if(activePlan.flowMeta?.groupFlow && currentGroupStatusFlow){
      if(currentGroupStatusFlow.confirmHandleInstall) body.confirmHandleInstall=true;
      if(currentGroupStatusFlow.settlePayment && Number(activePlan.order.total_price||0)>0) body.settlePayment=true;
    }else if(targetStatus==='تم التسليم' && Number(activePlan.order.total_price||0)>0){
      const paidOk=confirm(`هل العميل دفع المبلغ بالكامل؟\nاضغط OK للتأكيد أو Cancel لإلغاء تغيير الحالة.`);
      if(!paidOk){ load(); return; }
      body.settlePayment=true;
    }
    await authFetch('/update-status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    window.skipGroupFlowReset=true;
    closeModal('detailsModal');
    window.skipGroupFlowReset=false;
    if(activePlan.flowMeta?.groupFlow && currentGroupStatusFlow){
      currentGroupStatusFlow.currentIndex += 1;
      if(currentGroupStatusFlow.currentIndex < currentGroupStatusFlow.itemIds.length){
        await load();
        await openNextGroupCutItem();
        return;
      }
      const flowSnapshot=currentGroupStatusFlow;
      const postCutIds=(flowSnapshot?.postCutItemIds||[]).map(id=>Number(id));
      const flowOpts={confirmHandleInstall:!!flowSnapshot?.confirmHandleInstall,settlePayment:!!flowSnapshot?.settlePayment};
      currentGroupStatusFlow=null;
      if(postCutIds.length){
        await load();
        const remainingItems=ordersData.filter(item=>postCutIds.includes(Number(item.id)));
        if(remainingItems.length){
          await applyGroupStatusToItems(remainingItems,targetStatus,flowOpts);
          return;
        }
      }
    }
    await load();
  }catch(e){ alert(e.message); }
}
let bostaCitiesCache=null;
const LOCAL_BOSTA_ZONES = {
  'القاهرة':['مدينة نصر','مصر الجديدة','النزهة','السلام','المعادي','حلوان','الشروق','التجمع الخامس','عين شمس','المطرية','الزيتون','السيدة زينب','وسط البلد','المرج','مدينة بدر'],
  'الجيزة':['الدقي','العجوزة','إمبابة','الهرم','فيصل','بولاق الدكرور','الشيخ زايد','6 أكتوبر','الوراق','البدرشين','أبو النمرس','العياط'],
  'الإسكندرية':['سيدي جابر','سموحة','محرم بك','العصافرة','المندرة','العجمي','العامرية','برج العرب','ميامي','الرمل'],
  'القليوبية':['شبرا الخيمة','بنها','قليوب','العبور','الخانكة','طوخ','قها','كفر شكر'],
  'الشرقية':['الزقازيق','العاشر من رمضان','بلبيس','منيا القمح','أبو حماد','فاقوس','ههيا'],
  'الغربية':['طنطا','المحلة الكبرى','كفر الزيات','زفتى','السنطة','بسيون'],
  'المنوفية':['شبين الكوم','السادات','منوف','أشمون','قويسنا','تلا','الباجور'],
  'الدقهلية':['المنصورة','ميت غمر','طلخا','السنبلاوين','أجا','بلقاس','دكرنس'],
  'البحيرة':['دمنهور','كفر الدوار','إيتاي البارود','رشيد','أبو حمص','إدكو'],
  'كفر الشيخ':['كفر الشيخ','دسوق','فوه','بيلا','الحامول','بلطيم'],
  'دمياط':['دمياط','دمياط الجديدة','رأس البر','فارسكور','كفر سعد','الزرقا'],
  'بورسعيد':['بورفؤاد','حي العرب','الزهور','الضواحي','المناخ','شرق بورسعيد'],
  'الإسماعيلية':['الإسماعيلية','فايد','القنطرة شرق','القنطرة غرب','التل الكبير','أبو صوير'],
  'السويس':['الأربعين','فيصل','عتاقة','الجناين','السويس الجديدة'],
  'الفيوم':['الفيوم','سنورس','إطسا','طامية','أبشواي','يوسف الصديق'],
  'بني سويف':['بني سويف','الواسطى','ناصر','إهناسيا','ببا','سمسطا'],
  'المنيا':['المنيا','ملوي','أبو قرقاص','مغاغة','سمالوط','دير مواس'],
  'أسيوط':['أسيوط','ديروط','القوصية','منفلوط','أبو تيج','البداري'],
  'سوهاج':['سوهاج','أخميم','جرجا','البلينا','طهطا','طما'],
  'قنا':['قنا','نجع حمادي','دشنا','قفط','قوص','أبو تشت'],
  'الأقصر':['الأقصر','إسنا','أرمنت','الطود','القرنة'],
  'أسوان':['أسوان','دراو','كوم أمبو','إدفو','أبو سمبل'],
  'البحر الأحمر':['الغردقة','رأس غارب','سفاجا','القصير','مرسى علم'],
  'الوادي الجديد':['الخارجة','الداخلة','الفرافرة','باريس'],
  'مطروح':['مرسى مطروح','الحمام','العلمين','الضبعة','سيوة'],
  'شمال سيناء':['العريش','بئر العبد','الشيخ زويد','رفح','الحسنة'],
  'جنوب سيناء':['شرم الشيخ','الطور','دهب','نويبع','رأس سدر','سانت كاترين']
};
function normalizeBostaText(v=''){ return String(v||'').trim().replace(/^__label__:/,'').replace(/[ـ]/g,'').replace(/\s+/g,' '); }
function localBostaCityOptions(){
  return Object.keys(LOCAL_BOSTA_ZONES).map(label=>({ code:`__label__:${label}`, label, labels:[label], isFallback:true }));
}
function bostaCityOptionHasRealCodes(list=[]){
  return Array.isArray(list) && list.some(c=>String(c?.code||'').trim() && !String(c?.code||'').startsWith('__label__:'));
}
async function forceRefreshBostaCities(){
  try{
    const refreshed = await authFetch('/bosta-cities?refresh=1');
    if(Array.isArray(refreshed) && refreshed.length) bostaCitiesCache = refreshed;
  }catch(_){ }
}
const LOCAL_BOSTA_CITY_ALIASES = {
  'القاهرة':['cairo','al qahirah','القاهره'],
  'الجيزة':['giza','gizeh','الجيزه'],
  'الإسكندرية':['alexandria','iskandaria','اسكندرية','الاسكندريه','الإسكندريه'],
  'القليوبية':['qalyubia','qalyubiya','qalubia','kalubia','القليوبيه'],
  'الشرقية':['sharkia','sharqia','sharqeya','الشرقيه'],
  'الغربية':['gharbia','gharbeya','الغربيه'],
  'المنوفية':['monufia','menoufia','monofia','المنوفيه'],
  'الدقهلية':['dakahlia','dakahliya','daqahliyah','dakahleya','الدقهليه'],
  'البحيرة':['beheira','behaira','el beheira','البحيره'],
  'كفر الشيخ':['kafr el sheikh','kafr elsheikh','kafr ash shaykh'],
  'دمياط':['damietta','dumyat'],
  'بورسعيد':['port said','portsaid'],
  'الإسماعيلية':['ismailia','ismailiya','الاسماعيليه','الإسماعيليه'],
  'السويس':['suez'],
  'الفيوم':['fayoum','faiyum'],
  'بني سويف':['beni suef','bani sweif','beni sweif'],
  'المنيا':['minya','menia'],
  'أسيوط':['assiut','asyut','اسيوط'],
  'سوهاج':['sohag','suhaj'],
  'قنا':['qena','qina'],
  'الأقصر':['luxor','الاقصر'],
  'أسوان':['aswan','اسوان'],
  'البحر الأحمر':['red sea','البحر الاحمر'],
  'الوادي الجديد':['new valley'],
  'مطروح':['matrouh','marsa matrouh','مرسي مطروح'],
  'شمال سيناء':['north sinai'],
  'جنوب سيناء':['south sinai']
};
function detectLocalBostaCityLabel(raw=''){
  const source = normalizeBostaText(raw);
  if(!source) return '';
  const sourceCompact = source.replace(/\s+/g,'');
  let best = '';
  for(const label of Object.keys(LOCAL_BOSTA_ZONES)){
    const tokens = [label, ...(LOCAL_BOSTA_CITY_ALIASES[label]||[])].map(normalizeBostaText).filter(Boolean);
    for(const token of tokens){
      const tokenCompact = token.replace(/\s+/g,'');
      if(!tokenCompact) continue;
      const exact = source===token || sourceCompact===tokenCompact;
      const partial = token.length>=3 && (source.includes(token) || token.includes(source));
      const compactPartial = tokenCompact.length>=3 && (sourceCompact.includes(tokenCompact) || tokenCompact.includes(sourceCompact));
      if((exact || partial || compactPartial) && token.length > normalizeBostaText(best).length) best = label;
    }
  }
  return best;
}
function normalizeBostaCityToken(raw=''){
  const source = String(raw||'').trim();
  if(!source) return '';
  if(source.startsWith('__label__:')) return source;
  const detected = detectLocalBostaCityLabel(source);
  return detected ? `__label__:${detected}` : source;
}
function localZonesByCityToken(cityToken='', cityLabel=''){
  const label = detectLocalBostaCityLabel(cityLabel) || detectLocalBostaCityLabel(cityToken) || normalizeBostaText(cityLabel) || normalizeBostaText(cityToken);
  return LOCAL_BOSTA_ZONES[label] || [];
}
function bostaSelectedOptionLabel(selectEl){
  const opt = selectEl?.options?.[selectEl.selectedIndex] || {};
  return String(opt.getAttribute?.('data-label') || opt.text || opt.value || '').trim();
}
function bostaSelectedZoneLabel(){ return bostaSelectedOptionLabel(document.getElementById('b_zone')); }
function bostaOptionCode(item){ return String(item?.code || item?.id || item?.value || item?.label || '').trim(); }
function looksLikeBrokenZoneLabel(v=''){ const x=String(v||'').trim().toLowerCase(); return !x || /^f\d+$/i.test(x) || /^zone\s*\d+$/i.test(x) || /^district\s*\d+$/i.test(x); }
function localZonesForCurrentCity(){
  const selected = (b_city_code.options[b_city_code.selectedIndex]||{});
  const token = selected.getAttribute?.('data-label') || selected.text || b_city_code.value || '';
  return localZonesByCityToken(token);
}
function setBostaCitySelection(selectedCode=''){
  const direct = String(selectedCode||'').trim();
  const normalizedToken = normalizeBostaCityToken(direct);
  const wanted = normalizeBostaText(normalizedToken || direct);
  if(!wanted){ b_city_code.value=''; return; }
  if([...(b_city_code.options||[])].some(opt=>String(opt.value||'')===direct)){
    b_city_code.value = direct;
    return;
  }
  if(normalizedToken && [...(b_city_code.options||[])].some(opt=>String(opt.value||'')===normalizedToken)){
    b_city_code.value = normalizedToken;
    return;
  }
  const detectedLabel = detectLocalBostaCityLabel(normalizedToken || direct);
  const match = [...(b_city_code.options||[])].find(opt=>{
    const label = normalizeBostaText(opt.getAttribute?.('data-label') || opt.text || '');
    const value = normalizeBostaText(opt.value || '');
    return label===wanted || value===wanted || (!!detectedLabel && label===normalizeBostaText(detectedLabel));
  });
  b_city_code.value = match ? match.value : '';
}
function setBostaZoneSelection(selectedZone=''){
  const wanted = normalizeBostaText(selectedZone);
  if(!wanted){ b_zone.value=''; return; }
  if([...(b_zone.options||[])].some(opt=>String(opt.value||'')===String(selectedZone||''))){
    b_zone.value=String(selectedZone||'');
    return;
  }
  const match = [...(b_zone.options||[])].find(opt=>{
    const label = normalizeBostaText(opt.getAttribute?.('data-label') || opt.text || '');
    const value = normalizeBostaText(opt.value || '');
    return label===wanted || value===wanted || wanted.includes(label) || label.includes(wanted);
  });
  b_zone.value = match ? match.value : '';
}
async function loadBostaCities(selectedCode=''){
  if(!bostaCitiesCache){
    try{
      bostaCitiesCache = await authFetch('/bosta-cities');
      if(!bostaCityOptionHasRealCodes(bostaCitiesCache)) await forceRefreshBostaCities();
    }catch(_){
      bostaCitiesCache = null;
    }
  }
  let cityOptions = Array.isArray(bostaCitiesCache) ? bostaCitiesCache.filter(Boolean) : [];
  if(!cityOptions.length) cityOptions = localBostaCityOptions();
  for(const local of localBostaCityOptions()){
    if(!cityOptions.some(item=>normalizeBostaText(item?.label || item?.code || '')===normalizeBostaText(local.label))){
      cityOptions.push(local);
    }
  }
  b_city_code.innerHTML='<option value="">اختر المحافظة</option>' + cityOptions.map(c=>`<option value="${esc(c.code)}" data-label="${esc(c.label)}">${esc(c.label)}</option>`).join('');
  if(selectedCode) setBostaCitySelection(selectedCode);
}
async function loadBostaZones(cityCode, selectedZone=''){
  b_zone.innerHTML='<option value="">جاري تحميل المناطق...</option>';
  const requestedCityCode = String(cityCode||'').trim();
  const normalizedRequestedCode = normalizeBostaCityToken(requestedCityCode);
  const selectedOptionNow = b_city_code.options?.[b_city_code.selectedIndex] || null;
  const knownOption = [...(b_city_code.options||[])].find(opt=>String(opt.value||'')===requestedCityCode || String(opt.value||'')===normalizedRequestedCode) || selectedOptionNow;
  const effectiveCityCode = knownOption ? String(knownOption.value||'').trim() : String(b_city_code.value || normalizedRequestedCode || requestedCityCode || '').trim();
  const effectiveCityLabel = String(knownOption?.getAttribute?.('data-label') || knownOption?.text || detectLocalBostaCityLabel(requestedCityCode) || requestedCityCode || '').trim();
  if(!effectiveCityCode){
    b_zone.innerHTML='<option value="">اختر المنطقة</option>';
    return;
  }
  let zones = [];
  try{
    const url = `/bosta-zones/${encodeURIComponent(effectiveCityCode)}?city_label=${encodeURIComponent(effectiveCityLabel)}`;
    zones = await authFetch(url);
  }catch(_){ zones = []; }
  if(!Array.isArray(zones)) zones = [];
  const local = localZonesByCityToken(effectiveCityCode, effectiveCityLabel);
  const hasUsableZones = zones.length && !zones.every(z=>looksLikeBrokenZoneLabel(z?.label));
  if(!hasUsableZones){
    zones = local.map(label=>({label, code: label, isFallback:true}));
  }
  if(!zones.length){
    b_zone.innerHTML='<option value="">لا توجد مناطق لهذه المحافظة - راجع إعدادات بوسطة</option>';
    return;
  }
  b_zone.innerHTML='<option value="">اختر المنطقة</option>' + (zones||[]).map(z=>{const code=bostaOptionCode(z);const label=String(z?.label||code||'').trim();const fallback=Number(z?.isFallback||0)===1?'1':'0';return `<option value="${esc(code||label)}" data-label="${esc(label)}" data-fallback="${fallback}">${esc(label)}</option>`;}).join('');
  if(selectedZone) setBostaZoneSelection(selectedZone);
  if(!b_zone.value && zones && zones.length===1) b_zone.value=bostaOptionCode(zones[0]) || zones[0].label;
}
async function onBostaCityChange(){
  const selectedCode=b_city_code.value;
  await loadBostaZones(selectedCode, '');
  requestBostaEstimate(true);
}
function bostaPackageDescriptionFromOrder(order){
  if(order?.is_group){
    const groupedText=buildLabelDescription(order);
    if(groupedText && groupedText.length<=220) return groupedText;
    return [`أوردر مجمع ${String(order.group_code||order.id||'').trim()}`.trim(), `${orderItems(order).length} أصناف`].filter(Boolean).join(' - ');
  }
  const qty = Number(order?.qty||0);
  const sizeParts = [];
  const w = Number(order?.w||0);
  const l = Number(order?.l||0);
  if(w) sizeParts.push(w);
  if(l) sizeParts.push(l);
  const sizeText = sizeParts.length===2 ? `${sizeParts[0]}×${sizeParts[1]}` : (sizeParts[0]||'');
  const color = String(order?.color||'').trim();
  const printType = String(order?.printType||order?.print_type||'').trim();
  const bits = [];
  if(qty) bits.push(`${qty} شنطة`);
  if(color) bits.push(`لون ${color}`);
  if(printType) bits.push(printType === 'سادة' ? 'سادة' : 'مطبوعة');
  if(sizeText) bits.push(`مقاس ${sizeText}`);
  return bits.length ? bits.join(' - ') : `أوردر رقم ${order?.id||''}`.trim();
}
async function openBostaModalByContext(order,opts={}){
  if(!order) return;
  window.currentBostaOrderId=Number(opts.orderId || order.id || 0);
  window.currentBostaGroupCode=String(opts.groupCode || '').trim();
  window.currentBostaAllowOpen = Number(order.bosta_allow_open||0)===1;
  b_receiver_name.value=order.bosta_receiver_name||order.custName||'';
  b_receiver_phone.value=order.bosta_receiver_phone||order.custPhone||'';
  b_receiver_email.value=order.bosta_receiver_email||order.customer_email||'';
  b_second_line.value=order.bosta_second_line||order.custAddress||'';
  b_package_type.value=order.bosta_package_type||'Parcel';
  b_items_count.value=Number(order.qty||1);
  const defaultCod = Number(order.bosta_cod||order.remaining_amount||getBostaProductValueDefault(order)||0);
  const defaultProductValue = Number(order.bosta_product_value||getBostaProductValueDefault(order)||defaultCod||0);
  b_cod.value=defaultCod;
  b_product_value.value=defaultProductValue;
  window.currentBostaProductManual = false;
  syncBostaProductValue(true);
  b_business_reference.value=order.bosta_business_reference||(order.is_group?String(order.group_code||'').trim():`ORDER-${order.id}`);
  b_package_description.value=order.bosta_package_description||bostaPackageDescriptionFromOrder(order);
  b_notes.value=order.bosta_notes||'';
  setBostaAllowOpen(window.currentBostaAllowOpen);
  const preferredCityToken = normalizeBostaCityToken(order.customer_governorate || order.bosta_city_code || '');
  let guessed={ cityCode: preferredCityToken || order.customer_governorate || order.bosta_city_code || '', zone: order.customer_zone || order.bosta_zone || '', cityLabel: detectLocalBostaCityLabel(order.customer_governorate || order.bosta_city_code || '') || '' };
  try{
    guessed = await authFetch('/bosta-infer-location',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({address:b_second_line.value||order.custAddress||'',city_code:preferredCityToken || order.customer_governorate || order.bosta_city_code || '',zone:order.customer_zone||order.bosta_zone||''})});
  }catch(_){ }
  const selectedCityRaw = order.customer_governorate || guessed.cityLabel || order.bosta_city_code || guessed.cityCode || '';
  const selectedCity = normalizeBostaCityToken(selectedCityRaw) || selectedCityRaw;
  const selectedZone = order.customer_zone || order.bosta_zone_label || order.bosta_zone || order.bosta_zone_code || guessed.zone || '';
  try{
    await loadBostaCities(selectedCity);
    if(!b_city_code.value && guessed.cityLabel) setBostaCitySelection(guessed.cityLabel);
    await loadBostaZones(b_city_code.value || selectedCity, selectedZone);
    if(!b_zone.value && guessed.zone) setBostaZoneSelection(guessed.zone);
  }catch(e){
    b_auto_location_hint.textContent='تعذر تحميل محافظات ومناطق بوسطة الآن. تأكد من المفتاح أو جرّب مرة أخرى.';
  }
  const autoBits=[];
  if(order.custAddress) autoBits.push(`العنوان المسجل: ${order.custAddress}`);
  const cityText=(b_city_code.options[b_city_code.selectedIndex]||{}).text || detectLocalBostaCityLabel(selectedCity) || normalizeBostaText(selectedCity) || '-';
  if(selectedCity || selectedZone) autoBits.push(`الاختيار الحالي: ${cityText} / ${(selectedZone||'-')}`);
  autoBits.push(order.is_group ? 'الأوردر ده مجمع وبيتبعت كبوليصة واحدة إلى بوسطة. قيمة المنتج والـ COD محسوبين تلقائيًا على إجمالي الأوردر وتقدر تعدل قيمة المنتج يدويًا.' : 'بيانات الشحنة تتسحب تلقائيًا من الأوردر والعميل. المحافظة والمنطقة المحفوظين للعميل ليهم أولوية في بوسطة. قيمة المنتج بتتملأ من إجمالي الأوردر وتقدر تعدلها يدويًا قبل الإرسال.');
  b_auto_location_hint.textContent=autoBits.join(' | ');
  window.currentBostaEstimate = null;
  window.currentBostaEstimateKey = '';
  window.currentBostaEstimateLoading = false;
  window.currentBostaShowDetails = false;
  toggleBostaDetails(false);
  renderBostaEstimateBox();
  requestBostaEstimate(true);
  bostaModal.style.display='block';
}
async function openBostaModal(id){
  const order=ordersData.find(x=>Number(x.id)===Number(id));
  if(!order) return;
  await openBostaModalByContext(order,{orderId:id,groupCode:''});
}
async function openGroupBostaModal(groupCode){
  const order=buildGroupBostaContext(groupCode);
  if(!order) return;
  await openBostaModalByContext(order,{orderId:Number(order.id||0),groupCode:String(groupCode||'').trim()});
}
async function sendToBosta(){
  const id=window.currentBostaOrderId;
  const groupCode=String(window.currentBostaGroupCode||'').trim();
  if(!id && !groupCode) return;
  if(!b_city_code.value || !b_zone.value){ alert('اختَر المحافظة والمنطقة أولًا من قوائم بوسطة'); return; }
  if(String(b_city_code.value||'').startsWith('__label__:')){
    const currentLabel = (b_city_code.options[b_city_code.selectedIndex]||{}).text || b_city_code.value;
    await forceRefreshBostaCities();
    await loadBostaCities(currentLabel);
    await loadBostaZones(b_city_code.value || currentLabel, b_zone.value);
  }
  const selectedCityOption = (b_city_code.options[b_city_code.selectedIndex]||{});
  const selectedZoneOption = (b_zone.options[b_zone.selectedIndex]||{});
  const selectedZoneLabel = String(selectedZoneOption.getAttribute?.('data-label') || selectedZoneOption.text || b_zone.value || '').trim();
  const body={
    receiver_name:b_receiver_name.value,
    receiver_phone:b_receiver_phone.value,
    receiver_email:b_receiver_email.value,
    second_line:b_second_line.value,
    city_code:b_city_code.value,
    city_label:String(selectedCityOption.getAttribute?.('data-label') || selectedCityOption.text || '').trim(),
    zone:b_zone.value,
    zone_label:selectedZoneLabel,
    package_type:b_package_type.value,
    items_count:Math.max(1, Math.trunc(Number(b_items_count.value)||1)),
    cod:Math.max(0, Number(b_cod.value)||0),
    product_value:Math.max(0, Number(b_product_value.value)||0),
    allow_open: window.currentBostaAllowOpen ? 1 : 0,
    business_reference:b_business_reference.value,
    package_description:b_package_description.value,
    notes:b_notes.value
  };
  try{
    const endpoint=groupCode ? `/send-to-bosta-group/${encodeURIComponent(groupCode)}` : `/send-to-bosta/${id}`;
    const data=await authFetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const cityLabel = (b_city_code.options[b_city_code.selectedIndex]||{}).text || data.inferredCity || b_city_code.value;
    const parts=[groupCode ? 'تم إرسال الأوردر المجمّع إلى بوسطة بنجاح' : 'تم الإرسال إلى بوسطة بنجاح'];
    if(cityLabel || selectedZoneLabel || b_zone.value) parts.push(`العنوان: ${cityLabel||''}${(selectedZoneLabel||b_zone.value)?` / ${selectedZoneLabel||b_zone.value}`:''}`.trim());
    if(data.shippingFee) parts.push(`سعر الشحن حسب المحافظة: ${fmtMoney(data.shippingFee)}`);
    if(data.rawShippingFee) parts.push(`السعر الخام من رد بوسطة: ${fmtMoney(data.rawShippingFee)}`);
    if(data.estimatedFees) parts.push(`تقدير مستحقات بوسطة: ${fmtMoney(data.estimatedFees)}`);
    if(data.insuranceFees) parts.push(`التأمين: ${fmtMoney(data.insuranceFees)}`);
    if(data.extraCodFee) parts.push(`زيادة COD: ${fmtMoney(data.extraCodFee)}`);
    if(data.materialFee) parts.push(`خامة بوسطة: ${fmtMoney(data.materialFee)}`);
    if(data.priceAfterVat) parts.push(`بعد الضريبة: ${fmtMoney(data.priceAfterVat)}`);
    if(data.trackingNumber) parts.push(`رقم التتبع: ${data.trackingNumber}`);
    alert(parts.join('\n'));
    closeModal('bostaModal');
    load();
  }catch(e){ alert(e.message||'فشل الإرسال إلى بوسطة'); }
}

function dirEl(id){ return document.getElementById(id); }
function estimateDirectionAmount(order, stepType){
  const costs=buildAutoCosts(order||{});
  return ({ plate:Number(costs.zinc||0), print:Number(costs.print||0), make:Number(costs.make||0), handle:Number(costs.hand_fix||0) })[String(stepType||'').trim()] || 0;
}
function toggleDirectionPaid(prefix, forceValue=null){
  const checked = forceValue===null ? !!dirEl(`${prefix}_paid`)?.checked : !!forceValue;
  if(dirEl(`${prefix}_paid`)) dirEl(`${prefix}_paid`).checked = checked;
  const meta = dirEl(`${prefix}_paid_meta`);
  if(meta) meta.classList.toggle('show', checked && !dirEl(`${prefix}_paid`)?.disabled);
  if(checked && dirEl(`${prefix}_pay_date`) && !dirEl(`${prefix}_pay_date`).value) dirEl(`${prefix}_pay_date`).value = new Date().toISOString().slice(0,10);
}
function syncDirectionPaidAvailability(prefix){
  const hasPartner = Number(dirEl(`${prefix}_partner`)?.value||0)>0;
  const paid = dirEl(`${prefix}_paid`);
  if(paid){
    paid.disabled = !hasPartner;
    if(!hasPartner) toggleDirectionPaid(prefix, false);
    const label = paid.closest('label');
    if(label) label.style.opacity = hasPartner ? '1' : '.55';
  }
}
function directionStepRow(stepType){
  const steps=(currentDirectionData.steps||[]).filter(s=>String(s.step_type||'').trim()===stepType);
  return steps.length ? steps[steps.length-1] : null;
}
function directionPartnerOptions(type, selectedId=''){
  const allowedTypes = type==='تركيب يد' ? ['تركيب يد','صنايعي'] : [type];
  const rows=(currentDirectionData.partners||[]).filter(p=>Number(p.is_active||1)===1 && allowedTypes.includes(String(p.partner_type||'').trim()));
  return '<option value="">اختر الجهة</option>' + rows.map(p=>`<option value="${p.id}" ${String(selectedId)===String(p.id)?'selected':''}>${esc(p.name)}${p.phone?` - ${esc(p.phone)}`:''} (${esc(p.partner_type||'')})</option>`).join('');
}
function directionRowSettled(row){
  if(!row) return false;
  const amount = Number(row.amount||0);
  return amount>0 && Number(row.account_deducted_amount||0) >= amount;
}
function fillDirectionBlock(prefix, stepType, partnerType, defaultHint=''){
  const row=directionStepRow(stepType);
  dirEl(`${prefix}_id`).value=row?.id||'';
  dirEl(`${prefix}_status`).value=row?.status||'pending';
  dirEl(`${prefix}_partner`).innerHTML=directionPartnerOptions(partnerType, row?.partner_id||'');
  dirEl(`${prefix}_partner`).onchange=()=>syncDirectionPaidAvailability(prefix);
  dirEl(`${prefix}_amount`).value=row?.amount||'';
  if(dirEl(`${prefix}_pay_note`)) dirEl(`${prefix}_pay_note`).value='';
  const settled = directionRowSettled(row);
  toggleDirectionPaid(prefix, settled);
  syncDirectionPaidAvailability(prefix);
  const accountText = row ? (settled ? ' | الحسابات: مخصوم' : ' | الحسابات: غير مخصوم') : '';
  dirEl(`${prefix}_hint`).textContent=row ? `الحالة الحالية: ${String(row.status||'')==='done'?'تم التنفيذ':'مفتوح'}${row.partner_name?` | الجهة الحالية: ${row.partner_name}`:''}${accountText}` : defaultHint;
}
function syncDirectionModalUi(){
  const order=currentDirectionData.order||orderById(currentDirectionOrderId)||{};
  const cutDone=Number(order.paper_cut_done||0)===1 || Number(order.useReadyStock||0)===1;
  const printType=String(order.printType||'').trim();
  const isPrinted=printType && printType!=='سادة';
  const needHandle=String(order.handle||'').trim()==='بيد' && !Number(order.useReadyStock||0);
  dirEl('dir_plate_section').classList.toggle('hidden', printType!=='أوفست');
  dirEl('dir_print_section').classList.toggle('hidden', !isPrinted);
  dirEl('dir_handle_section').classList.toggle('hidden', !needHandle);
  const displayNo = order.order_display_no || order.display_order_no || order.order_sort_id || order.id || '';
  let note=`الأوردر #${displayNo} | ${order.custName||'-'} | الحالة الحالية ${order.status||'-'}`;
  if(!cutDone) note += ' | الزنكات قبل القص، والطباعة/التصنيع بعد اعتماد القص.';
  else note += ' | المبلغ المقترح ظاهر تلقائيًا وتقدر تعدله قبل الحفظ.';
  dirEl('dir_hint').textContent=note;
  [['dir_plate','plate','اختر جهة الزنكات وسجل تكلفة الزنكات.'],['dir_print','print','اختر المطبعة وسجل تكلفة الطباعة.'],['dir_make','make','اختر الصنايعي وسجل تكلفة التصنيع.'],['dir_handle','handle','اختر جهة تركيب اليد وسجل تكلفة التركيب.']].forEach(([prefix,stepType,baseHint])=>{
    const section = dirEl(`${prefix}_section`);
    if(!section || section.classList.contains('hidden')) return;
    const row = directionStepRow(stepType);
    const suggested = estimateDirectionAmount(order, stepType);
    const allowedBeforeCut = stepType === 'plate';
    const enabledNow = cutDone || allowedBeforeCut;
    if(!row && enabledNow && suggested>0 && !Number(dirEl(`${prefix}_amount`).value||0)) dirEl(`${prefix}_amount`).value = suggested;
    if(!row) dirEl(`${prefix}_hint`).textContent = enabledNow ? `${baseHint}${suggested>0 ? ` | المبلغ المقترح: ${fmtMoney(suggested)}` : ''}` : 'سيتم تفعيل هذه المرحلة بعد القص.';
  });
}
async function openDirectionModal(id){
  try{
    const data=await authFetch('/order-operations/'+id);
    currentDirectionOrderId=id;
    currentDirectionData=data||{order:null,steps:[],partners:[]};
    fillDirectionBlock('dir_plate','plate','زنكات','');
    fillDirectionBlock('dir_print','print','مطبعة','');
    fillDirectionBlock('dir_make','make','صنايعي','');
    fillDirectionBlock('dir_handle','handle','تركيب يد','');
    syncDirectionModalUi();
    dirEl('directionModal').style.display='block';
  }catch(e){
    alert(e.message||'تعذر فتح نافذة التوجيه');
  }
}
function collectDirectionPayload(prefix, stepType){
  const section=dirEl(`${prefix}_section`);
  if(!section || section.classList.contains('hidden')) return null;
  const id=Number(dirEl(`${prefix}_id`).value||0);
  const partnerId=Number(dirEl(`${prefix}_partner`).value||0);
  const amount=Number(dirEl(`${prefix}_amount`).value||0);
  const status=dirEl(`${prefix}_status`).value||'pending';
  const paidNow = !!dirEl(`${prefix}_paid`)?.checked;
  // لا نحفظ إلا المرحلة التي اخترت لها جهة فعلًا. لو علامة الدفع متعلمة بدون جهة، نتجاهل المرحلة بدل ما نوقف باقي التوجيه.
  if(!partnerId) { toggleDirectionPaid(prefix, false); return null; }
  if(paidNow && amount<=0) throw new Error('اكتب قيمة الشغل أولاً قبل تسجيل أنه تم الدفع');
  const order=currentDirectionData.order||orderById(currentDirectionOrderId)||{};
  return { id, order_id:Number(currentDirectionOrderId||0), partner_id:partnerId, step_type:stepType, status, qty:Number(order.qty||0), amount, record_payment: paidNow ? 1 : 0, payment_date: dirEl(`${prefix}_pay_date`)?.value || '', payment_note: dirEl(`${prefix}_pay_note`)?.value || '', actor_username: '__cashbox__' };
}
async function saveDirectionModal(){
  try{
    const payloads=[
      collectDirectionPayload('dir_plate','plate'),
      collectDirectionPayload('dir_print','print'),
      collectDirectionPayload('dir_make','make'),
      collectDirectionPayload('dir_handle','handle')
    ].filter(Boolean);
    if(!payloads.length) return alert('حدد على الأقل مرحلة واحدة في التوجيه');
    for(const body of payloads){
      await authFetch('/save-order-operation',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    }
    closeModal('directionModal');
    await load();
    alert('تم حفظ التوجيه وربط المراحل بالأوردر.');
  }catch(e){ alert(e.message||'تعذر حفظ التوجيه'); }
}

let currentOrderFinanceId=0;
function financeCostStatus(c){
  if(Number(c.paid||0)) return '<span class="tag ok">مدفوع</span>';
  if(Number(c.due||0)) return '<span class="tag late">مستحق</span>';
  return '<span class="tag">لم يصل مرحلته</span>';
}
async function openOrderFinance(id){
  currentOrderFinanceId=Number(id||0);
  orderFinanceBox.innerHTML='<div style="padding:12px;color:var(--muted)">جاري تحميل حسابات الأوردر...</div>';
  orderFinanceModal.style.display='block';
  try{ const d=await authFetch('/order-finance/'+currentOrderFinanceId); renderOrderFinance(d); }
  catch(e){ orderFinanceBox.innerHTML='<div style="padding:12px;color:#fecaca">'+esc(e.message||'فشل تحميل الحسابات')+'</div>'; }
}
function renderOrderFinance(d){
  const o=d.order||{}, t=d.totals||{}, costs=d.costs||[], pays=d.payments||[];
  orderFinanceBox.innerHTML=`<div class="bosta-preview" style="display:block"><b>${esc(o.custName||'')}</b> — أوردر #${esc(o.order_display_no||o.id||'')}<br>البيع: ${fmtMoney(t.total_price)} | صافي بعد الشحن: ${fmtMoney(t.sale_net)} | التكاليف: ${fmtMoney(t.costs_total)} | المدفوع من التكاليف: ${fmtMoney(t.paid_costs)} | المستحق للجهات: ${fmtMoney(t.unpaid_costs)} | الربح المتوقع: ${fmtMoney(t.expected_profit)}</div>`+
  `<div class="table-wrap" style="margin-top:10px"><table><thead><tr><th>البند</th><th>تكلفة الأوردر</th><th>المدفوع من الخزنة/العهدة</th><th>المتبقي</th><th>الجهة</th><th>الحالة</th></tr></thead><tbody>${costs.length?costs.map(c=>`<tr><td><b>${esc(c.label)}</b></td><td>${fmtMoney(c.amount)}</td><td>${fmtMoney(c.paid_amount)}</td><td>${fmtMoney(c.remaining_amount)}</td><td>${esc(c.execution_partner_name||c.actor_name||'-')}</td><td>${financeCostStatus(c)}</td></tr>`).join(''):'<tr><td colspan="6">لا توجد تكاليف مسجلة</td></tr>'}</tbody></table></div>`+
  `<h4>دفعات العميل</h4><div class="table-wrap"><table><thead><tr><th>التاريخ</th><th>المبلغ</th><th>الطريقة</th><th>ملاحظة</th></tr></thead><tbody>${pays.length?pays.map(p=>`<tr><td>${esc(p.payment_date||'')}</td><td>${fmtMoney(p.amount)}</td><td>${esc(p.method||'')}</td><td>${esc(p.note||'')}</td></tr>`).join(''):'<tr><td colspan="4">لا توجد دفعات عميل</td></tr>'}</tbody></table></div>`;
}
async function resyncCurrentOrderFinance(){
  if(!currentOrderFinanceId)return;
  if(!confirm('إعادة مزامنة حسابات هذا الأوردر؟'))return;
  const d=await authFetch('/resync-order-accounting/'+currentOrderFinanceId,{method:'POST'});
  renderOrderFinance(d.finance||{});
  await load();
}

function closeModal(id){ document.getElementById(id).style.display='none'; if(id==='detailsModal'){detailsChoices.innerHTML=''; detailsBox.innerHTML=''; saveCutBtn.style.display='none'; window.currentPlan=null; window.pendingTargetStatus=null; if(!window.skipGroupFlowReset) currentGroupStatusFlow=null;} if(id==='bostaModal'){ window.currentBostaOrderId=null; window.currentBostaGroupCode=null; window.currentBostaAllowOpen=false; window.currentBostaEstimate=null; window.currentBostaEstimateKey=''; window.currentBostaEstimateLoading=false; window.currentBostaShowDetails=false; } if(id==='paymentsModal'){ currentPaymentsOrderId=null; } if(id==='directionModal'){ currentDirectionOrderId=null; currentDirectionData={order:null,steps:[],partners:[]}; } if(id==='printChoiceModal'){ printSelectionSnapshot=[]; } if(id==='orderFinanceModal'){ currentOrderFinanceId=0; orderFinanceBox.innerHTML=''; } }
window.openEdit = openEdit;
window.saveEdit = saveEdit;
window.addEditAsNewItem = addEditAsNewItem;
window.updateStatus = updateStatus;
window.openDirectionModal = openDirectionModal;
window.saveDirectionModal = saveDirectionModal;
window.toggleDirectionPaid = toggleDirectionPaid;
window.openCosts = openCosts;
window.showDetails = showDetails;
window.showHistory = showHistory;
window.openBostaModal = openBostaModal;
window.onBostaCityChange = onBostaCityChange;
window.sendToBosta = sendToBosta;
window.toggleBostaAllowOpen = toggleBostaAllowOpen;
window.toggleBostaDetails = toggleBostaDetails;
window.renderBostaEstimateBox = renderBostaEstimateBox;
window.delOrder = delOrder;
window.toggleActions = toggleActions;
window.closeModal = closeModal;
document.addEventListener('click',e=>{ if(!e.target.closest('.action-cell')) closeAllActionMenus(); });
load();

