const token=localStorage.getItem('token');
const user=JSON.parse(localStorage.getItem('user')||'null');
if(!token||!user) location.href='login.html';
function hasPerm(key){return user.username==='admin'||user.role==='super_admin'||Number(user[key]||0)===1;}
if(!hasPerm('perm_view_artisans')) location.href='index.html';
function authFetch(url,opts={}){opts.headers=Object.assign({},opts.headers||{},{Authorization:'Bearer '+token});return fetch(url,opts).then(async r=>{const data=await r.json().catch(()=>({})); if(r.status===401){localStorage.clear();location.href='login.html';throw new Error('unauthorized')} if(!r.ok) throw new Error(data.error||'error'); return data;});}
const money=v=>Number(v||0).toLocaleString('en-US',{maximumFractionDigits:2})+' ج';
const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
let partners=[], partnersSummary=[], openOrders=[], productionJobs=[], cashUsers=[];
let editingPartnerId=0, partnersHidden=false, operationPartnerTypeFilter='';
let ordersRenderLimit=30;
let ordersRenderTimer=null;
const ORDERS_RENDER_STEP=30;
welcome.textContent=`${user.full_name||user.username} | متابعة المطابع والصنايعية`;
document.querySelector('a[href="orders_list.html"]')?.classList.toggle('hidden', !hasPerm('perm_view_orders'));
document.querySelector('a[href="accounts.html"]')?.classList.toggle('hidden', !hasPerm('perm_view_accounts'));
function closeModal(id){document.getElementById(id).classList.remove('show'); if(id==='operationModal') operationPartnerTypeFilter=''; }
function openModal(id){document.getElementById(id).classList.add('show')}
function roleColor(status){ return String(status||'').trim()==='done'?'done':'pending'; }
function stepLabel(step){ return ({plate:'زنكات',print:'طباعة',make:'تصنيع',handle:'تركيب يد'})[String(step||'').trim()] || step || '-'; }
function normalizeType(type){ const t=String(type||'').trim(); if(t==='مطبعة') return 'مطبعة'; if(['زنكات','زنك','جهة زنكات','بتاع زنكات','بتاع الزنكات'].includes(t)) return 'زنكات'; if(t==='صنايعي') return 'صنايعي'; if(t==='تركيب يد') return 'تركيب يد'; return 'أخرى'; }
function orderById(id){ return openOrders.find(o=>String(o.id)===String(id)); }
function estimateOperationAmount(order, step){
  const qty=Number(order?.qty)||0;
  const manual=(field, fallback=0)=>{ const raw=order?.['cost_'+field]; return raw!==undefined && raw!==null && raw!=='' ? Number(raw) : fallback; };
  const printType=String(order?.printType||'').trim();
  if(String(step)==='make') return manual('make', Number(order?.useReadyStock||0)===1 ? ((Number(order?.ready_stock_purchase_total)||0) || ((Number(order?.bag_buy_price)||0)*qty)) : qty*1.2);
  if(String(step)==='handle') return manual('hand_fix', (String(order?.handle||'').trim()==='بيد' ? +((qty/1000)*100).toFixed(2) : 0));
  if(String(step)==='plate') return manual('zinc', 0);
  if(String(step)!=='print') return 0;
  if(printType==='سلك سكرين') return manual('print', qty*2);
  if(printType==='أوفست'){
    const mode=String(order?.last_cut_layout||'pieceByPiece');
    const cutWidth = +(mode==='pieceByPiece' ? ((Number(order?.w)||0) + (Number(order?.g)||0) + 2) : (((Number(order?.w)||0)*2) + ((Number(order?.g)||0)*2) + 2)).toFixed(2);
    const cutLength = +((Number(order?.l)||0) + ((Number(order?.g)||0)/2) + 2).toFixed(2);
    const mxW=Math.max(cutWidth,cutLength), mxH=Math.min(cutWidth,cutLength);
    let base=0;
    if(mxW<=50 && mxH<=35) base=150; else if(mxW<=70 && mxH<=50) base=200; else if(mxW<=100 && mxH<=70) base=300;
    const txt=String(order?.colorSpecs||'');
    const m=txt.match(/(\d+)/);
    const colors=Math.max(1, Number(m?.[1]||0) || (txt.split('+').filter(Boolean).length||1));
    const piecesPerBag=mode==='pieceByPiece'?2:1;
    const printUnits=Math.ceil((qty*piecesPerBag)/1000);
    return manual('print', base*colors*printUnits);
  }
  return manual('print', 0);
}
function toggleOperationPaid(forceValue=null){
  const checked = forceValue===null ? !!op_paid_now.checked : !!forceValue;
  op_paid_now.checked = checked;
  op_paid_wrap.classList.toggle('show', checked);
  if(checked && !op_payment_date.value) op_payment_date.value = new Date().toISOString().slice(0,10);
}
async function reloadAll(){
  if(ordersRenderTimer) clearTimeout(ordersRenderTimer);
  ordersRenderLimit=30;
  ordersWrap.innerHTML='<div class="muted">جاري تحميل الأوردرات المفتوحة...</div>';
  await Promise.all([loadDashboard(),loadPartners(),loadOpenOrders(),loadProductionJobs()]);
}
async function loadDashboard(){ const d=await authFetch('/operations-dashboard'); sOrders.textContent=d.active_orders||0; sPending.textContent=d.pending_operations||0; sPartners.textContent=d.active_partners||0; sAssigned.textContent=money(d.total_assigned); sPaid.textContent=money(d.total_paid); sRemain.textContent=money(d.remaining); }
async function loadPartners(){ partners = await authFetch('/execution-partners'); partnersSummary = await authFetch('/operations-partners-summary'); renderPartners(); syncOperationForm(); }
async function loadOpenOrders(){ openOrders = await authFetch('/operations-orders'); renderOrders(); }
async function loadProductionJobs(){ productionJobs = await authFetch('/production-jobs-summary').catch(()=>[]); renderProductionJobs(); }
function stepPartnerType(step){ return ({plate:'زنكات',print:'مطبعة',make:'صنايعي',handle:'تركيب يد'})[String(step||'').trim()] || ''; }
function partnerCanDoStep(partnerType, step){
  const normalized = normalizeType(partnerType);
  const cleanStep = String(step||'').trim();
  if(cleanStep==='handle') return normalized==='تركيب يد' || normalized==='صنايعي';
  if(cleanStep==='make') return normalized==='صنايعي';
  if(cleanStep==='print') return normalized==='مطبعة';
  if(cleanStep==='plate') return normalized==='زنكات';
  return false;
}
function requiredOrderSteps(order){
  if(Number(order?.useReadyStock||0)===1) return [];
  const printType = String(order?.printType||'').trim();
  const isPrinted = printType && printType!=='سادة';
  const steps=[];
  if(printType==='أوفست') steps.push('plate');
  if(isPrinted) steps.push('print');
  steps.push('make');
  if(String(order?.handle||'').trim()==='بيد') steps.push('handle');
  return steps;
}
function availableOrderSteps(order, partnerType=''){
  const assigned = new Set((order?.operation_steps||[]).map(s=>String(s.step_type||'').trim()).filter(Boolean));
  const normalizedPartner = partnerType ? normalizeType(partnerType) : '';
  return requiredOrderSteps(order).filter(step=>!assigned.has(step)).filter(step=>!normalizedPartner || partnerCanDoStep(normalizedPartner, step));
}
function preferredPartnerStep(partnerType, steps=[]){
  const normalized = normalizeType(partnerType);
  if(normalized==='مطبعة') return steps.includes('print') ? 'print' : (steps[0]||'');
  if(normalized==='زنكات') return steps.includes('plate') ? 'plate' : (steps[0]||'');
  if(normalized==='صنايعي') return steps.includes('make') ? 'make' : (steps.includes('handle') ? 'handle' : (steps[0]||''));
  if(normalized==='تركيب يد') return steps.includes('handle') ? 'handle' : (steps[0]||'');
  return steps[0]||'';
}
function filteredPartnersByStep(step){ return partners.filter(p=>Number(p.is_active||1)===1 && partnerCanDoStep(p.partner_type, step)); }
function fillPartnerSelect(selected=''){ const step=op_step.value; const rows=filteredPartnersByStep(step); op_partner.innerHTML='<option value="">اختر الجهة</option>' + rows.map(p=>`<option value="${p.id}" ${String(selected)===String(p.id)?'selected':''}>${esc(p.name)} - ${esc(p.partner_type||'')}</option>`).join(''); if(selected && !rows.some(p=>String(p.id)===String(selected))) op_partner.value=''; }
function syncStepSelectForOrder(order, preferredStep=''){
  const options = availableOrderSteps(order, operationPartnerTypeFilter);
  if(!options.length){
    op_step.innerHTML='<option value="">لا توجد مراحل متاحة</option>';
    op_partner.innerHTML='<option value="">لا توجد جهات</option>';
    return [];
  }
  const selected = options.includes(preferredStep) ? preferredStep : preferredPartnerStep(operationPartnerTypeFilter, options);
  op_step.innerHTML = options.map(step=>`<option value="${step}" ${step===selected?'selected':''}>${stepLabel(step)}</option>`).join('');
  return options;
}
function partnerSummary(id){ return partnersSummary.find(p=>String(p.id)===String(id)); }
function operationById(id){ for(const o of openOrders){ const row=(o.operation_steps||[]).find(j=>String(j.id)===String(id)); if(row) return row; } for(const partner of partnersSummary){ const row=(partner.jobs||[]).find(j=>String(j.id)===String(id)); if(row) return row; } return null; }
function updateOperationEverywhere(id, patch={}){
  let updated=null;
  for(const o of openOrders){
    const row=(o.operation_steps||[]).find(j=>String(j.id)===String(id));
    if(row){ Object.assign(row, patch); updated=row; }
  }
  for(const partner of partnersSummary){
    const row=(partner.jobs||[]).find(j=>String(j.id)===String(id));
    if(row){ Object.assign(row, patch); updated=row; }
  }
  return updated;
}
function showTinyStatus(id, text='', ok=true){
  const el=document.getElementById('opSaveStatus_'+id);
  if(!el) return;
  el.textContent=text;
  el.classList.toggle('pending', !ok);
  if(text && ok) setTimeout(()=>{ if(el.textContent===text) el.textContent=''; }, 1400);
}
function refreshOperationRowUi(id){
  const row=operationById(id);
  if(!row) return;
  const partnerEl=document.getElementById('opPartnerCurrent_'+id);
  if(partnerEl){
    partnerEl.textContent = row.partner_name ? `${row.partner_name}${row.partner_type?' - '+row.partner_type:''}` : 'لم يتم تحديد جهة';
  }
  const statusEl=document.getElementById('opStatus_'+id);
  if(statusEl){
    const done=String(row.status||'').trim()==='done';
    statusEl.className='badge '+(done?'done':'pending');
    statusEl.textContent=done?'تم':'مفتوح';
  }
  const controls=document.getElementById('opControls_'+id);
  if(controls){
    controls.innerHTML = `<button class="warn mini" onclick="openEditOperation(${id})">تعديل</button> ${String(row.status||'').trim()==='done'?'':`<button class="success mini" onclick="markOperationDone(${id})">تم</button> `}<button class="danger mini" onclick="deleteOperation(${id})">حذف</button>`;
  }
}
function totalOrderExternal(steps=[]){ return steps.reduce((s,r)=>s+(Number(r.amount)||0),0); }
function togglePartnersSection(){ partnersHidden=!partnersHidden; partnersSection.classList.toggle('hidden', partnersHidden); partnersToggleBtn.textContent = partnersHidden ? '👁️ إظهار الجهات' : '🙈 إخفاء الجهات'; }
function operationPartnerOptions(step, selected=''){
  const rows=filteredPartnersByStep(step);
  return '<option value="">بدون جهة / افتح التكليف</option>' + rows.map(p=>`<option value="${p.id}" ${String(selected)===String(p.id)?'selected':''}>${esc(p.name)} - ${esc(p.partner_type||'')}</option>`).join('');
}
function operationPartnerCell(s){
  const id=Number(s.id||0);
  return `<div class="partner-cell"><select id="opPartner_${id}" onchange="autoSaveOperationPartner(${id}, this)">${operationPartnerOptions(s.step_type, s.partner_id||'')}</select><span class="tiny-save" id="opSaveStatus_${id}"></span></div><div class="partner-current" id="opPartnerCurrent_${id}">${s.partner_name?`${esc(s.partner_name)}${s.partner_type?' - '+esc(s.partner_type):''}`:'لم يتم تحديد جهة'}</div>`;
}
function renderOrderCard(o){
  const steps=o.operation_steps||[];
  const ext=totalOrderExternal(steps);
  const available=availableOrderSteps(o);
  const availableText = available.length ? available.map(stepLabel).join(' / ') : 'كل مراحل التكلفة ظاهرة/متسجلة';
  const chips=[
    ['تنفيذ خارجي', ext],
    ['تصنيع', o.cost_make],
    ['طباعة', o.cost_print],
    ['زنكات', o.cost_zinc],
    ['تركيب يد', o.cost_hand_fix]
  ].map(([label,val])=>`<span class="compact-pill">${esc(label)} <b>${money(val)}</b></span>`).join('');
  return `<div class="order-card order-card-compact"><div class="order-top"><div class="compact-order-main"><div class="compact-order-title"><b>${esc(o.order_display_label||('أوردر #'+o.id))}</b><span class="badge ${o.priority==='مستعجل'?'pending':''}">${esc(o.status||'')}</span></div><div class="compact-meta">${esc(o.custName||'-')} · ${Number(o.qty||0)} شنطة · ${esc(o.color||'-')} · المتاح: ${esc(availableText)}</div></div><div class="compact-actions"><button class="success mini" onclick="openOperationForOrder(${o.id})" ${available.length?'':'disabled'}>توجيه ناقص</button><a class="btn mini" href="orders_list.html">الأوردرات</a></div></div><div class="compact-costs">${chips}</div>${steps.length?`<div class="details table-wrap"><table><thead><tr><th>التكليف</th><th>الجهة</th><th>القيمة</th><th>الحالة</th><th>تحكم</th></tr></thead><tbody>${steps.map(s=>`<tr id="opRow_${s.id}"><td>${esc(stepLabel(s.step_type))}</td><td>${operationPartnerCell(s)}</td><td>${money(s.amount)}</td><td><span id="opStatus_${s.id}" class="badge ${roleColor(s.status)}">${String(s.status||'').trim()==='done'?'تم':'مفتوح'}</span></td><td id="opControls_${s.id}"><button class="warn mini" onclick="openEditOperation(${s.id})">تعديل</button> ${String(s.status||'').trim()==='done'?'':`<button class="success mini" onclick="markOperationDone(${s.id})">تم</button> `}<button class="danger mini" onclick="deleteOperation(${s.id})">حذف</button></td></tr>`).join('')}</tbody></table></div>`:'<div class="details muted" style="font-size:12px">لا توجد تكاليف تنفيذ خارجية مسجلة على هذا الأوردر حتى الآن</div>'}</div>`;
}
function renderOrders(){
  if(ordersRenderTimer) clearTimeout(ordersRenderTimer);
  if(!openOrders.length){ ordersWrap.innerHTML='<div class="muted">لا توجد أوردرات مفتوحة حاليًا</div>'; return; }
  const total=openOrders.length;
  const visible=openOrders.slice(0, ordersRenderLimit);
  ordersWrap.innerHTML = `<div class="muted" style="margin-bottom:8px">معروض ${Math.min(visible.length,total)} من ${total} أوردر مفتوح. الصفحة بتعرض أول دفعة بسرعة عشان التحميل مايبقاش تقيل.</div>`;
  let idx=0;
  const batch=10;
  function appendBatch(){
    const part=visible.slice(idx, idx+batch).map(renderOrderCard).join('');
    if(part) ordersWrap.insertAdjacentHTML('beforeend', part);
    idx+=batch;
    if(idx<visible.length){
      ordersRenderTimer=setTimeout(appendBatch, 0);
    } else if(ordersRenderLimit<total){
      ordersWrap.insertAdjacentHTML('beforeend', `<div class="flex" style="justify-content:center;margin-top:10px"><button class="success" onclick="showMoreOrders()">عرض ${Math.min(ORDERS_RENDER_STEP,total-ordersRenderLimit)} أوردر كمان</button><button onclick="showAllOrders()">عرض كل الأوردرات</button></div>`);
    }
  }
  appendBatch();
}
function showMoreOrders(){ ordersRenderLimit += ORDERS_RENDER_STEP; renderOrders(); }
function showAllOrders(){ ordersRenderLimit = openOrders.length; renderOrders(); }

function renderProductionJobs(){
  const wrap=document.getElementById('productionJobsWrap');
  if(!wrap) return;
  if(!productionJobs.length){ wrap.innerHTML='<div class="empty">لا توجد أوامر تشغيل مفتوحة حاليًا</div>'; return; }
  wrap.innerHTML=productionJobs.map(job=>{
    const planned=Number(job.qty||0);
    const operationExpense=Number(job.operation_expense||0);
    const handReserved=Number(job.handle_reserved_qty||0);
    const finalDefault=Number(job.final_qty||0)>0?Number(job.final_qty||0):planned;
    const waste=Math.max(0, planned-finalDefault);
    const extra=Math.max(0, finalDefault-planned);
    const isHandleInstall=String(job.source_type||'').trim()==='handle_install';
    const diffText=extra>0?(isHandleInstall?`زيادة ${extra} شنطة وسيتم طلب يد إضافية عند الإنهاء`:`زيادة ${extra} شنطة وسيتم خصم ورقها عند الإنهاء`):(waste>0?`هالك ${waste} شنطة`:'لا يوجد هالك');
    const noteText=isHandleInstall
      ? `تركيب يد فقط — سيتم خصم ${esc(job.handle_type||'مجدول')} من مخزن اليد وتحويل الكمية من صنف بدون يد إلى صنف بيد عند الضغط على تم.`
      : `${esc(job.source_type==='paper_cut'?'جاى من قص الورق':'جاى من صفحة الشنط الجاهزة')} — الورق اتخصم عند فتح أمر التشغيل${handReserved>0?` — اليد المخصومة ${handReserved}`:''}. اكتب الكمية الفعلية السليمة، والهالك أو الزيادة هيتحسبوا تلقائيًا.`;
    const buttonText=isHandleInstall?'تم وتركيب اليد':'تم وإدخال المخزن';
    return `<div class="order-card"><div class="order-top"><div><b>أمر تشغيل #${job.id}</b> <span class="badge pending">تحت التصنيع</span><div class="muted" style="margin-top:6px">مخطط ${planned} شنطة | ${esc(job.color||'-')} | ${esc(job.handle||'-')}${isHandleInstall?` | ${esc(job.handle_type||'مجدول')}`:''}</div></div><div class="flex"><button class="success" onclick="completeProductionJobUi(${job.id})">${buttonText}</button></div></div><div class="ops-note" style="margin-bottom:10px">${noteText}</div><div class="form-grid" style="margin-bottom:10px"><div><label>${isHandleInstall?'كمية تركيب اليد التي ستتحول إلى بيد':'التشغيل الفعلي / الكمية النهائية التي ستدخل المخزن'}</label><input id="final_qty_${job.id}" type="number" min="1" value="${finalDefault}" oninput="previewProductionFinalQty(${job.id},${planned})"></div><div><label>الهالك</label><input id="waste_qty_${job.id}" type="number" value="${waste}" disabled></div><div><label>الفرق</label><div id="extra_qty_${job.id}" class="muted" style="padding:13px;border:1px solid var(--border);border-radius:12px;background:#0b1220">${diffText}</div></div></div><div class="kv"><div><small>المقاس</small><b>${Number(job.length||0)} × ${Number(job.width||0)} × ${Number(job.gusset||0)}</b></div><div><small>نوع الأمر</small><b>${isHandleInstall?'تركيب يد فقط':esc(job.layout_label||'-')}</b></div><div><small>الورق</small><b>${esc(job.paper_label||'-')}</b></div><div><small>قص</small><b>${money(job.cost_cut||0)}</b></div><div><small>تصنيع</small><b>${money(job.cost_make||0)}</b></div><div><small>يد</small><b>${money(job.cost_hand||0)}</b></div><div><small>تركيب يد</small><b>${money(job.cost_hand_fix||0)}</b></div><div><small>مصاريف العهدة الحالية</small><b>${money(operationExpense)}</b></div><div><small>تكلفة وحدة محسوبة</small><b>${money(job.unit_cost||0)}</b></div><div><small>تحديث السعر؟</small><b>${Number(job.update_price_on_complete||0)===1?'نعم':'لا'}</b></div></div><div class="details muted">${esc(job.note||'-')}</div></div>`;
  }).join('');
}
function renderPartnerCard(p){
  const canAssign = openOrders.some(o=>availableOrderSteps(o, p.partner_type).length);
  const jobs = p.jobs || [];
  const unpaid = jobs.filter(j=>!operationSettled(j));
  const bulkControls = unpaid.length ? `<div class="ops-note" style="margin-bottom:8px"><b>إقفال حساب أوردرات محددة</b><div class="form-grid" style="margin-top:8px"><div><label>خصم من</label><select id="bulkActor_${p.id}">${cashUserOptions('__cashbox__')}</select></div><div><label>تاريخ الدفع</label><input id="bulkDate_${p.id}" type="date" value="${new Date().toISOString().slice(0,10)}"></div><div><label>ملاحظة</label><input id="bulkNote_${p.id}" placeholder="اختياري"></div><div style="display:flex;align-items:end"><button class="success" onclick="paySelectedPartnerJobs(${p.id})">إقفال المحدد</button></div></div><small class="muted">اختار الأوردرات غير المدفوعة من الجدول، وسيتم خصمها من الخزنة أو العهدة المحددة مرة واحدة.</small></div>` : `<div class="muted" style="margin-bottom:8px">لا توجد تكاليف مستحقة غير مدفوعة لهذه الجهة.</div>`;
  const rows = jobs.length ? jobs.map(j=>{
    const settled = operationSettled(j);
    return `<tr><td>${settled?'✅':`<input type="checkbox" class="partnerJobCheck_${p.id}" value="${j.id}">`}</td><td>${esc(j.order_display_label||('أوردر #'+(j.order_id||'')))}</td><td>${esc(j.custName||'-')}</td><td>${esc(stepLabel(j.step_type||j.step_label||'-'))}</td><td>${Number(j.qty||0)}</td><td>${money(j.amount)}</td><td><span class="badge ${roleColor(j.status)}">${String(j.status||'').trim()==='done'?'تم':'مفتوح'}</span></td><td>${operationAccountBadge(j)}</td><td>${String(j.status||'').trim()==='done'?'':`<button class="success" onclick="markOperationDone(${j.id})">تم</button> `}<button class="danger" onclick="deleteOperation(${j.id})">حذف</button></td></tr>`;
  }).join('') : '<tr><td colspan="9">لا يوجد شغل مسجل</td></tr>';
  return `<div class="card"><div class="flex" style="justify-content:space-between;align-items:start"><div><h3 style="margin:0">${esc(p.name||'')}</h3><div class="muted">${esc(p.partner_type||'أخرى')}</div></div><div class="flex"><button onclick="togglePartnerDetails(${p.id})">التفاصيل</button><button class="success" onclick="openPartnerPayment(${p.id})">دفعة مستقلة</button><button class="warn" onclick="editPartner(${p.id})">تعديل</button><button class="danger" onclick="deletePartner(${p.id})">حذف</button></div></div><div class="kv"><div><small>عدد الشغل</small><b>${Number(p.jobs_count||0)}</b></div><div><small>إجمالي المسند</small><b>${money(p.total_assigned)}</b></div><div><small>المدفوع فعليًا</small><b>${money(p.auto_paid)}</b></div><div><small>المتبقي</small><b>${money(p.remaining)}</b></div></div><div class="details hidden" id="partnerDetails_${p.id}"><div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:8px"><b>أوردرات وشغل الجهة</b><button class="success" onclick="openOperationForPartner(${p.id})" ${canAssign?'':'disabled'}>إضافة شغل</button></div>${bulkControls}<div class="table-wrap"><table><thead><tr><th>اختيار</th><th>الأوردر</th><th>العميل</th><th>المرحلة</th><th>الكمية</th><th>القيمة</th><th>الحالة</th><th>الحسابات</th><th>تحكم</th></tr></thead><tbody>${rows}</tbody></table></div><div id="partnerPayments_${p.id}" class="muted" style="margin-top:10px">جارٍ تحميل الدفعات...</div></div></div>`;
}
async function paySelectedPartnerJobs(partnerId){
  const ids=[...document.querySelectorAll(`.partnerJobCheck_${partnerId}:checked`)].map(x=>Number(x.value||0)).filter(Boolean);
  if(!ids.length) return alert('اختار أوردر واحد على الأقل');
  const actor=document.getElementById(`bulkActor_${partnerId}`)?.value || '__cashbox__';
  const date=document.getElementById(`bulkDate_${partnerId}`)?.value || new Date().toISOString().slice(0,10);
  const note=document.getElementById(`bulkNote_${partnerId}`)?.value || '';
  if(!confirm(`تأكيد إقفال ${ids.length} تكليف وخصمهم من الحسابات؟`)) return;
  const res=await authFetch('/pay-order-operations',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({operation_ids:ids,admin_username:actor,payment_date:date,note})});
  alert(`تم إقفال ${res.count||0} تكليف بإجمالي ${money(res.total||0)}`);
  await reloadAll();
}
function renderGroup(title,key,list){ return `<div class="group-block"><div class="group-head"><h3>${title}</h3><span class="muted">${list.length} جهة</span></div>${list.length?`<div class="cards" style="margin-top:12px">${list.map(renderPartnerCard).join('')}</div>`:'<div class="empty" style="margin-top:12px">لا توجد جهات في هذا القسم</div>'}</div>`; }
function renderPartners(){ if(!partnersSummary.length){ partnersWrap.innerHTML='<div class="muted">لا توجد جهات تنفيذ مسجلة</div>'; return; } const groups={ printers:[], zinc:[], artisans:[], handles:[], others:[] }; partnersSummary.forEach(p=>{ const t=normalizeType(p.partner_type); if(t==='مطبعة') groups.printers.push(p); else if(t==='زنكات') groups.zinc.push(p); else if(t==='صنايعي') groups.artisans.push(p); else if(t==='تركيب يد') groups.handles.push(p); else groups.others.push(p); }); partnersWrap.innerHTML = [ renderGroup('🖨️ المطابع','printers',groups.printers), renderGroup('🔩 جهات الزنكات','zinc',groups.zinc), renderGroup('🧑‍🏭 الصنايعية','artisans',groups.artisans), renderGroup('👜 تركيب اليد','handles',groups.handles), renderGroup('📦 جهات أخرى','others',groups.others) ].join(''); }
async function togglePartnerDetails(id){ const box=document.getElementById('partnerDetails_'+id); box.classList.toggle('hidden'); if(!box.classList.contains('hidden')) await loadPartnerPayments(id); }
async function loadPartnerPayments(id){ const rows=await authFetch('/partner-payments/'+id); const target=document.getElementById('partnerPayments_'+id); if(!target) return; target.innerHTML = rows.length?`<table><thead><tr><th>التاريخ</th><th>المبلغ</th><th>ملاحظة</th></tr></thead><tbody>${rows.map(r=>`<tr><td>${esc(r.payment_date||'')}</td><td>${money(r.amount)}</td><td>${esc(r.note||'-')}</td></tr>`).join('')}</tbody></table>`:'لا توجد دفعات بعد'; }
async function savePartner(){ const body={id:editingPartnerId,name:p_name.value.trim(),partner_type:p_type.value,phone:p_phone.value.trim(),address:p_address.value.trim(),notes:p_notes.value.trim(),is_active:1}; if(!body.name) return alert('اكتب اسم الجهة'); await authFetch('/save-execution-partner',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); cancelPartnerEdit(); await reloadAll(); }
function editPartner(id){ const row=partners.find(p=>String(p.id)===String(id)); if(!row) return; editingPartnerId=id; p_name.value=row.name||''; p_type.value=row.partner_type||'صنايعي'; p_phone.value=row.phone||''; p_address.value=row.address||''; p_notes.value=row.notes||''; partnerSaveBtn.textContent='حفظ التعديل'; partnerCancelBtn.classList.remove('hidden'); window.scrollTo({top:0,behavior:'smooth'}); }
function cancelPartnerEdit(){ editingPartnerId=0; p_name.value=''; p_type.value='صنايعي'; p_phone.value=''; p_address.value=''; p_notes.value=''; partnerSaveBtn.textContent='حفظ الجهة'; partnerCancelBtn.classList.add('hidden'); }
async function deletePartner(id){ if(!confirm('حذف الجهة؟')) return; await authFetch('/delete-execution-partner/'+id,{method:'DELETE'}); await reloadAll(); }
function syncOperationForm(){
  const order=orderById(Number(op_order_id.value||0));
  const currentSelected=op_partner.value;
  let note='';
  if(order){
    const available = syncStepSelectForOrder(order, op_step.value);
    fillPartnerSelect(currentSelected);
    if(!available.length) note='كل مراحل التوجيه المطلوبة لهذا الأوردر متسجلة بالفعل. تقدر تعدل الجهة من الصفوف الثابتة تحت كل أوردر.';
    else {
      const suggested = estimateOperationAmount(order, op_step.value);
      if(suggested>0 && !Number(op_amount.value||0)) op_amount.value=suggested;
      if(op_step.value==='plate') note=`اختَر جهة الزنكات فقط.${suggested>0 ? ` المبلغ المقترح: ${money(suggested)}` : ''}`;
      else if(op_step.value==='print') note=`اختَر مطبعة مسجلة وحدد تكلفة الطباعة.${suggested>0 ? ` المبلغ المقترح: ${money(suggested)}` : ''}`;
      else if(op_step.value==='make') note=`اختَر الصنايعي وحدد تكلفة التصنيع.${suggested>0 ? ` المبلغ المقترح: ${money(suggested)}` : ''}`;
      else if(op_step.value==='handle') note=`اختَر جهة تركيب اليد أو نفس الصنايعي لو هو اللي هيركب اليد.${suggested>0 ? ` المبلغ المقترح: ${money(suggested)}` : ''}`;
    }
  } else {
    op_step.innerHTML='<option value="">اختر المرحلة</option>';
    op_partner.innerHTML='<option value="">اختر الجهة</option>';
  }
  operationHintBox.textContent = note || 'اختر المرحلة والجهة والتكلفة.';
}
function openOperationForOrder(orderId){
  const order=orderById(orderId);
  if(!order) return alert('الأوردر غير موجود');
  operationPartnerTypeFilter='';
  const available=availableOrderSteps(order);
  if(!available.length) return alert('كل المراحل المطلوبة لهذا الأوردر متسجلة بالفعل. احذف التوجيه الحالي لو حابب تسجله من جديد.');
  op_id.value=''; op_order_id.value=orderId; op_status.value='pending'; op_qty.value=order?.qty||0; op_amount.value=''; op_ref.value=''; op_note.value=''; toggleOperationPaid(false); op_payment_note.value=''; op_payment_date.value='';
  operationOrderHint.textContent = `${order.order_display_label||('أوردر #'+order.id)} | ${order.custName||'-'} | الكمية ${order.qty||0} | الحالة الحالية ${order.status||'-'}`;
  syncStepSelectForOrder(order, available[0]);
  fillPartnerSelect('');
  openModal('operationModal');
  syncOperationForm();
}
function openOperationForPartner(partnerId){
  const partner = partners.find(p=>String(p.id)===String(partnerId));
  const partnerType = normalizeType(partner?.partner_type);
  const order = openOrders.find(o=>availableOrderSteps(o, partnerType).length);
  if(!order) return alert('لا يوجد أوردر متاح لهذه الجهة الآن. إمّا كل المراحل متسجلة أو لا توجد تكلفة مناسبة لهذه الجهة.');
  const available = availableOrderSteps(order, partnerType);
  const preferred = preferredPartnerStep(partnerType, available);
  operationPartnerTypeFilter=partnerType;
  op_id.value=''; op_order_id.value=order.id; op_qty.value=order.qty||0; op_status.value='pending'; op_ref.value=''; op_note.value=''; op_amount.value=''; toggleOperationPaid(false); op_payment_note.value=''; op_payment_date.value='';
  operationOrderHint.textContent = `تم فتح التوجيه على ${order.order_display_label||('أوردر #'+order.id)} | المتاح لهذه الجهة: ${available.map(stepLabel).join(' / ')}`;
  syncStepSelectForOrder(order, preferred);
  fillPartnerSelect(String(partnerId));
  openModal('operationModal');
  syncOperationForm();
}
async function saveOperation(){ const orderId=Number(op_order_id.value||0); if(!orderId) return alert('افتح الإسناد من أوردر محدد'); if(!op_step.value) return alert('لا توجد مرحلة متاحة الآن لهذا الأوردر'); const paidNow = !!op_paid_now.checked; const body={id:Number(op_id.value||0),order_id:orderId,partner_id:Number(op_partner.value||0),step_type:op_step.value,status:op_status.value,qty:Number(op_qty.value||0),amount:Number(op_amount.value||0),reference_code:op_ref.value.trim(),note:op_note.value.trim(),record_payment: paidNow ? 1 : 0,payment_date: op_payment_date.value,payment_note: op_payment_note.value.trim(), actor_username: '__cashbox__'}; if(!body.partner_id) return alert('اختر الجهة'); if(paidNow && body.amount<=0) return alert('اكتب قيمة الشغل أولاً قبل تسجيل أنه تم الدفع'); await authFetch('/save-order-operation',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); closeModal('operationModal'); await reloadAll(); alert('تم ربط الشغل بالأوردر وتحديث الحالة والتكلفة تلقائيًا.'); }
function openEditOperation(id){ const row=operationById(id); if(!row) return alert('لم أجد التكليف'); const order=orderById(row.order_id); op_id.value=Number(row.id||0); op_order_id.value=Number(row.order_id||0); op_status.value=String(row.status||'pending').trim()==='done'?'done':'pending'; op_qty.value=Number(row.qty||order?.qty||0); op_amount.value=Number(row.amount||0); op_ref.value=String(row.reference_code||''); op_note.value=String(row.note||''); op_step.innerHTML=`<option value="${esc(row.step_type||'')}">${esc(stepLabel(row.step_type))}</option>`; fillPartnerSelect(String(row.partner_id||'')); operationOrderHint.textContent = `${order?.order_display_label||('أوردر #'+(row.order_id||''))} | ${order?.custName||'-'} | تعديل ${stepLabel(row.step_type)}`; toggleOperationPaid(false); op_payment_note.value=''; op_payment_date.value=''; openModal('operationModal'); }
async function saveOperationPartner(id){ return autoSaveOperationPartner(id, document.getElementById('opPartner_'+id)); }
async function autoSaveOperationPartner(id, selectEl){
  const row=operationById(id);
  if(!row) return alert('لم أجد التكليف');
  const sel=selectEl || document.getElementById('opPartner_'+id);
  const partnerId=Number(sel?.value||0);
  const clearing=!partnerId;
  const chosen=clearing?null:partners.find(p=>String(p.id)===String(partnerId));
  const prev={partner_id:row.partner_id, partner_name:row.partner_name, partner_type:row.partner_type, status:row.status, account_deducted_amount:row.account_deducted_amount};
  const body={id:Number(row.id||0),order_id:Number(row.order_id||0),partner_id:partnerId,step_type:String(row.step_type||''),status:clearing?'pending':(String(row.status||'pending').trim()==='done'?'done':'pending'),qty:Number(row.qty||0),amount:Number(row.amount||0),reference_code:String(row.reference_code||''),note:String(row.note||'')};
  if(sel) sel.disabled=true;
  showTinyStatus(id,'جار الحفظ...',false);
  try{
    await authFetch('/save-order-operation',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    updateOperationEverywhere(id, clearing ? {partner_id:0,partner_name:'',partner_type:'',status:'pending',account_deducted_amount:0} : {partner_id:partnerId,partner_name:chosen?.name||row.partner_name||'',partner_type:chosen?.partner_type||row.partner_type||''});
    refreshOperationRowUi(id);
    showTinyStatus(id, clearing ? 'تم فتح التكليف وحذف الدفع المرتبط' : 'تم الحفظ', true);
  }catch(e){
    updateOperationEverywhere(id,prev);
    if(sel) sel.value=String(prev.partner_id||'');
    showTinyStatus(id,'فشل الحفظ',false);
    alert(e.message||'تعذر حفظ الجهة');
  }finally{
    if(sel) sel.disabled=false;
  }
}
async function markOperationDone(id){
  const row=operationById(id);
  if(!row) return alert('لم أجد عملية الشغل');
  if(!Number(row.partner_id||0)) return alert('حدد الجهة المسؤولة الأول');
  const body={id:Number(row.id||0),order_id:Number(row.order_id||0),partner_id:Number(row.partner_id||0),step_type:String(row.step_type||''),status:'done',qty:Number(row.qty||0),amount:Number(row.amount||0),reference_code:String(row.reference_code||''),note:String(row.note||'')};
  const btn=document.querySelector(`#opControls_${id} .success`);
  if(btn){ btn.disabled=true; btn.textContent='جارٍ...'; }
  try{
    await authFetch('/save-order-operation',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    updateOperationEverywhere(id,{status:'done',completed_at:new Date().toISOString()});
    refreshOperationRowUi(id);
    loadDashboard().catch(()=>{});
  }catch(e){
    if(btn){ btn.disabled=false; btn.textContent='تم'; }
    alert(e.message||'تعذر إنهاء التكليف');
  }
}
async function deleteOperation(id){ if(!confirm('حذف مرحلة الشغل؟')) return; await authFetch('/delete-order-operation/'+id,{method:'DELETE'}); await reloadAll(); }
function previewProductionFinalQty(id, plannedQty){
  const input=document.getElementById('final_qty_'+id);
  const waste=document.getElementById('waste_qty_'+id);
  const extraBox=document.getElementById('extra_qty_'+id);
  const finalQty=Math.max(0, Number(input?.value||0));
  const planned=Number(plannedQty||0);
  const wasteQty=Math.max(0, planned-finalQty);
  const extraQty=Math.max(0, finalQty-planned);
  if(waste) waste.value=wasteQty;
  if(extraBox) extraBox.textContent = extraQty>0 ? `زيادة ${extraQty} شنطة وسيتم خصم ورقها عند الإنهاء` : (wasteQty>0 ? `هالك ${wasteQty} شنطة` : 'لا يوجد هالك');
}
async function completeProductionJobUi(id){
  const job=productionJobs.find(j=>String(j.id)===String(id));
  const planned=Number(job?.qty||0);
  const finalEl=document.getElementById('final_qty_'+id);
  const finalQty=Math.round(Number(finalEl?.value||planned||0));
  if(finalQty<=0) return alert('اكتب الكمية النهائية السليمة');
  const waste=Math.max(0, planned-finalQty);
  const extra=Math.max(0, finalQty-planned);
  const isHandleInstall=String(job?.source_type||'').trim()==='handle_install';
  const extraLine=extra>0 ? (isHandleInstall ? `
زيادة عن المخطط: ${extra} شنطة — سيتم طلب يد إضافية لها من المخزن` : `
زيادة عن المخطط: ${extra} شنطة — سيتم خصم ورق إضافي لها من المخزن`) : '';
  if(!confirm(`تأكيد إنهاء أمر التشغيل؟
المخطط: ${planned} شنطة
${isHandleInstall?'كمية تركيب اليد التي ستتحول إلى بيد':'التشغيل الفعلي الذي سيدخل المخزن'}: ${finalQty} شنطة
الهالك: ${waste} شنطة${extraLine}`)) return;
  await authFetch('/complete-production-job/'+id,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({final_qty:finalQty,update_price_on_complete:Number(job?.update_price_on_complete||0)})});
  productionJobs = productionJobs.filter(j=>String(j.id)!==String(id));
  renderProductionJobs();
  loadDashboard().catch(()=>{});
}
function openPartnerPayment(id){ const row=partnerSummary(id) || partners.find(p=>String(p.id)===String(id)); if(!row) return; pay_partner_id.value=id; pay_amount.value=''; pay_date.value=new Date().toISOString().slice(0,10); pay_note.value=''; paymentPartnerHint.textContent=`${row.name||''} | المتبقي ${money(row.remaining||0)}`; openModal('paymentModal'); }
async function savePartnerPayment(){ const body={partner_id:Number(pay_partner_id.value||0),amount:Number(pay_amount.value||0),payment_date:pay_date.value,note:pay_note.value.trim()}; if(!body.partner_id) return alert('اختر الجهة'); if(body.amount<=0) return alert('اكتب مبلغ صحيح'); await authFetch('/save-partner-payment',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); closeModal('paymentModal'); await reloadAll(); }
window.markOperationDone=markOperationDone;
window.toggleOperationPaid=toggleOperationPaid;
window.paySelectedPartnerJobs=paySelectedPartnerJobs;
window.addEventListener('click',e=>{ if(e.target.classList.contains('modal')) closeModal(e.target.id); });
reloadAll();