
const token=localStorage.getItem('token');
const user=JSON.parse(localStorage.getItem('user')||'null');
if(!token||!user) location.href='login.html';
if(user.role!=='admin' && !Number(user.perm_activity_logs||0)) location.href='index.html';
const canEditActivity = user.role==='admin' || Number(user.perm_edit_activity_records||0)===1;
const canDeleteActivity = user.role==='admin' || Number(user.perm_delete_activity_records||0)===1;
function authFetch(url,opts={}){opts.headers=Object.assign({},opts.headers||{},{Authorization:'Bearer '+token});return fetch(url,opts).then(async r=>{const data=await r.json().catch(()=>({})); if(r.status===401){localStorage.clear();location.href='login.html';throw new Error('unauthorized')} if(!r.ok) throw new Error(data.error||'error'); return data;});}
function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}
function dur(s){ const n=Number(s||0); const h=Math.floor(n/3600), m=Math.floor((n%3600)/60), sec=n%60; return `${h} س ${m} د ${sec} ث`; }
function fmt(v){ if(!v) return '-'; try{return new Date(v).toLocaleString('ar-EG')}catch(e){return v} }
const state={auditPage:1,sessionsPage:1,pageSize:50};
let lastAuditRows=[]; let lastSessionRows=[];
function renderPager(targetId,current,total,cbName){ const target=document.getElementById(targetId); if(!target) return; if(total<=1){ target.innerHTML=''; return; } let html=`<button onclick="${cbName}(1)">الأولى</button><button onclick="${cbName}(${Math.max(1,current-1)})">السابق</button>`; const start=Math.max(1,current-2), end=Math.min(total,current+2); for(let i=start;i<=end;i++) html += `<button class="${i===current?'active':''}" onclick="${cbName}(${i})">${i}</button>`; html += `<button onclick="${cbName}(${Math.min(total,current+1)})">التالي</button><button onclick="${cbName}(${total})">الأخيرة</button>`; target.innerHTML=html; }
function toggleSessionsWrap(){ sessionsWrap.classList.toggle('hidden'); }
function toggleOpsWrap(){ opsWrap.classList.toggle('hidden'); }
function auditActionsHtml(r){ const parts=[]; if(canEditActivity) parts.push(`<button class="warn" onclick="editAudit(${Number(r.id)})">تعديل</button>`); if(canDeleteActivity) parts.push(`<button class="red" onclick="deleteAudit(${Number(r.id)})">حذف من السيستم</button>`); return parts.length ? `<div class="actions">${parts.join('')}</div>` : '-'; }
function sessionActionsHtml(r){ return canDeleteActivity ? `<div class="actions"><button class="red" onclick="deleteSession(${Number(r.id)})">حذف</button></div>` : '-'; }
async function loadAudit(page=state.auditPage){
  state.auditPage=page;
  const qv=encodeURIComponent(q.value||'');
  const res=await authFetch(`/activity-logs?q=${qv}&page=${state.auditPage}&pageSize=${state.pageSize}`);
  const rows=(res.rows||[]).filter(r=>!['login','logout'].includes(String(r.action||'').trim()));
  lastAuditRows=rows;
  auditCount.textContent=`${Number(res.total||rows.length)} سجل`;
  auditBody.innerHTML=rows.length?rows.map(r=>`<tr>
    <td>${fmt(r.created_at)}</td>
    <td><b>${esc(r.full_name||r.username||'-')}</b><br><small class="muted">${esc(r.username||'-')}</small></td>
    <td>${esc(r.action||'')}</td>
    <td>${esc(r.entity_type||'-')} ${r.entity_id?`#${r.entity_id}`:''}</td>
    <td>${esc(r.details||'-')}<div class="small">${r.edited_at?`آخر تعديل: ${esc(fmt(r.edited_at))}${r.edited_by?`<br>بواسطة: ${esc(r.edited_by)}`:''}`:'-'}</div></td>
    <td>${esc(r.device_label||'-')}<br><small class="muted">${esc(r.ip_address||'-')}</small><div class="small">${esc(r.user_agent||'')}</div></td>
    <td>${auditActionsHtml(r)}</td>
  </tr>`).join(''):'<tr><td colspan="7">لا توجد سجلات</td></tr>';
  renderPager('auditPager', Number(res.page||1), Number(res.totalPages||1), 'goAuditPage');
}
async function loadSessions(page=state.sessionsPage){
  state.sessionsPage=page;
  const qv=encodeURIComponent(q.value||'');
  const res=await authFetch(`/session-history?q=${qv}&page=${state.sessionsPage}&pageSize=${state.pageSize}`);
  const rows=res.rows||[];
  lastSessionRows=rows;
  sessionsCount.textContent=`${Number(res.total||0)} جلسة`;
  sessionsBody.innerHTML=rows.length?rows.map(r=>{ const active=Number(r.is_active||0)===1; const duration=active ? Math.max(0, Math.round((Date.now()-new Date(r.login_at).getTime())/1000)) : Number(r.duration_seconds||0); return `<tr><td>${fmt(r.login_at)}</td><td>${active?'ما زالت مفتوحة':fmt(r.logout_at)}</td><td><b>${esc(r.full_name||r.username||'-')}</b><br><small class="muted">${esc(r.username||'-')}</small></td><td>${active?'نشطة الآن':'انتهت'}</td><td>${esc(r.device_label||'-')}<div class="small">${esc(r.user_agent||'-')}</div></td><td>${dur(duration)}</td><td>${esc(r.ip_address||'-')}</td><td>${sessionActionsHtml(r)}</td></tr>`; }).join(''):'<tr><td colspan="8">لا توجد جلسات</td></tr>';
  renderPager('sessionsPager', Number(res.page||1), Number(res.totalPages||1), 'goSessionsPage');
}
function goAuditPage(page){ loadAudit(page); }
function goSessionsPage(page){ loadSessions(page); }
function findAuditRow(id){ return lastAuditRows.find(r=>Number(r.id)===Number(id))||null; }
async function editAudit(id){
  const row=findAuditRow(id);
  if(!row){ alert('السجل غير موجود'); return; }
  const action=prompt('اسم العملية', row.action||'');
  if(action===null) return;
  const details=prompt('التفاصيل', row.details||'');
  if(details===null) return;
  try{
    await authFetch(`/activity-logs/${Number(id)}/edit`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,details})});
    await loadAudit(state.auditPage);
    alert('تم تعديل السجل');
  }catch(e){ alert(e.message); }
}
async function deleteAudit(id){
  if(!confirm('حذف العملية من السيستم نفسه؟ ده هيحذف أثرها من قلب النظام لو العملية مدعومة.')) return;
  try{
    await authFetch(`/activity-logs/${Number(id)}`,{method:'DELETE'});
    await loadAudit(state.auditPage);
  }catch(e){ alert(e.message); }
}
async function deleteSession(id){
  if(!confirm('حذف جلسة الدخول من السجل؟')) return;
  try{
    await authFetch(`/session-history/${Number(id)}`,{method:'DELETE'});
    await loadSessions(state.sessionsPage);
  }catch(e){ alert(e.message); }
}
async function reloadAll(resetPages=false){ if(resetPages){ state.auditPage=1; state.sessionsPage=1; } await Promise.all([loadAudit(state.auditPage), loadSessions(state.sessionsPage)]); }
q.addEventListener('keydown',e=>{ if(e.key==='Enter') reloadAll(true); });
reloadAll(true);
