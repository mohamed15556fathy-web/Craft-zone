
const token=localStorage.getItem('token');
const user=JSON.parse(localStorage.getItem('user')||'null');
if(!token||!user) location.href='login.html';
if(user.role!=='admin' && !Number(user.perm_users||0)) location.href='index.html';

const permMeta={
  perm_view_orders:{title:'عرض الأوردرات',desc:'فتح صفحة متابعة الأوردرات فقط'},
  perm_add_order:{title:'إضافة أوردر',desc:'إنشاء أوردر جديد'},
  perm_edit_order:{title:'تعديل أوردر',desc:'فتح تعديل الأوردر وتعديل بياناته'},
  perm_delete_order:{title:'حذف أوردر',desc:'حذف الأوردرات من النظام'},
  perm_change_status:{title:'تغيير حالة أوردر',desc:'تحريك الأوردر بين الحالات'},
  perm_customers:{title:'صفحة العملاء',desc:'فتح صفحة العملاء وسجل تعاملهم'},
  perm_calculator:{title:'صفحة احسب أوردر',desc:'تشغيل الحاسبة واقتراح الأفرخ'},

  perm_view_inventory:{title:'عرض مخزن الورق',desc:'فتح صفحة مخزن الورق فقط'},
  perm_manage_paper:{title:'إضافة/تعديل الورق',desc:'إضافة أصناف الورق وتعديلها وحذفها وحدودها'},
  perm_cut_paper:{title:'قص الورق',desc:'تنفيذ القص وربطه بالأوردرات'},
  perm_paper_history:{title:'سجل الورق',desc:'عرض تاريخ حركات الورق'},

  perm_view_bags:{title:'عرض مخزن الشنط',desc:'فتح صفحة مخزن الشنط الجاهزة'},
  perm_manage_bags:{title:'إدارة الشنط',desc:'إضافة/تعديل/حذف الشنط وأوامر التشغيل'},
  perm_bags_history:{title:'سجل الشنط',desc:'عرض تاريخ حركات الشنط'},

  perm_view_handles:{title:'عرض مخزن اليد',desc:'فتح صفحة مخزن اليد'},
  perm_manage_handles:{title:'إدارة اليد',desc:'إضافة/تعديل رصيد اليد'},
  perm_handles_history:{title:'سجل اليد',desc:'عرض تاريخ حركات اليد'},

  perm_view_accounts:{title:'عرض الحسابات',desc:'فتح صفحة الحسابات والملخصات'},
  perm_view_financial_totals:{title:'عرض الإجماليات المالية',desc:'رؤية كروت إجمالي المخزون والموجودات والإجماليات المالية الحساسة'},
  perm_manage_expenses:{title:'إدارة المصاريف',desc:'إضافة وتعديل وحذف المصاريف'},
  perm_manage_current_cash:{title:'إدارة السيولة الحالية',desc:'فتح وتعديل سجل السيولة الحالية'},
  perm_use_admin_cash_on_expense:{title:'الخصم من عهدة الشغل',desc:'اختيار محمد أو بودا للخصم من عهدة الشغل عند تسجيل مصروف'},
  perm_edit_expense_records:{title:'تعديل سجلات المصاريف',desc:'تعديل المصاريف والتكاليف المرتبطة من السجلات'},
  perm_delete_expense_records:{title:'حذف سجلات المصاريف',desc:'حذف المصاريف والتكاليف المرتبطة من السجلات'},
  perm_edit_cash_records:{title:'تعديل سجل السيولة',desc:'تعديل حركات السيولة الحالية'},
  perm_delete_cash_records:{title:'حذف سجل السيولة',desc:'حذف حركات السيولة الحالية'},
  perm_edit_admin_cash_records:{title:'تعديل عهدة الشغل',desc:'تعديل عهدة محمد وبودا والتحويل بينهم'},
  perm_delete_admin_cash_records:{title:'حذف عهدة الشغل',desc:'حذف حركات عهدة الشغل اليدوية'},
  perm_view_cost_logs:{title:'سجل التكاليف',desc:'عرض سجل التكاليف على الأوردرات'},
  perm_view_sales_logs:{title:'سجل المبيعات',desc:'عرض سجل المبيعات والأرباح'},

  perm_view_debts:{title:'عرض المديونيات',desc:'فتح صفحة المديونيات وكشفها'},
  perm_manage_debts:{title:'إدارة المديونيات',desc:'إضافة المديونيات وسدادها وحذفها'},
  perm_view_reports:{title:'صفحة التقارير',desc:'عرض تقارير الإدارة والربحية والنواقص'},
  perm_suppliers:{title:'صفحة الموردين',desc:'إدارة الموردين وكشف حساب كل مورد'},
  perm_purchases:{title:'صفحة المشتريات',desc:'تسجيل فواتير الشراء وسدادها وربطها بالمخزون'},

  perm_users:{title:'إدارة المستخدمين',desc:'فتح صفحة المستخدمين وتغيير الصلاحيات'},
  perm_activity_logs:{title:'سجل الأدمن',desc:'عرض سجل النشاط والجلسات'},
  perm_edit_activity_records:{title:'تعديل عمليات السيستم',desc:'تعديل وصف العملية داخل سجل الأدمن'},
  perm_delete_activity_records:{title:'حذف عمليات السيستم',desc:'حذف العملية من السجل ومن قلب السيستم لو كانت مدعومة'},
  perm_backup_restore:{title:'النسخ الاحتياطي',desc:'تصدير واستيراد النسخ الاحتياطية'},
  perm_act_as_other_admin:{title:'العمل باسم أدمن آخر',desc:'يسمح بتسجيل أو تعديل العملية أو العهدة باسم مستخدم آخر'}
};
const permKeys=Object.keys(permMeta);
const roles={
  admin:{title:'مدير عام',desc:'كل الصلاحيات مفتوحة بالكامل.',perms:Object.fromEntries(permKeys.map(k=>[k,1]))},
  moderator:{title:'مودريتور',desc:'أوردرات كاملة بدون مخازن أو حسابات.',perms:{perm_view_orders:1,perm_add_order:1,perm_edit_order:1,perm_delete_order:1,perm_change_status:1,perm_customers:1,perm_calculator:1,perm_view_inventory:0,perm_manage_paper:0,perm_cut_paper:0,perm_paper_history:0,perm_view_bags:0,perm_manage_bags:0,perm_bags_history:0,perm_view_handles:0,perm_manage_handles:0,perm_handles_history:0,perm_view_accounts:0,perm_view_financial_totals:0,perm_manage_expenses:0,perm_manage_current_cash:0,perm_use_admin_cash_on_expense:0,perm_edit_expense_records:0,perm_delete_expense_records:0,perm_edit_cash_records:0,perm_delete_cash_records:0,perm_edit_admin_cash_records:0,perm_delete_admin_cash_records:0,perm_view_cost_logs:0,perm_view_sales_logs:0,perm_view_debts:0,perm_manage_debts:0,perm_view_reports:0,perm_suppliers:0,perm_purchases:0,perm_users:0,perm_activity_logs:0,perm_edit_activity_records:0,perm_delete_activity_records:0,perm_backup_restore:0,perm_act_as_other_admin:0}},
  operation:{title:'أوبريشن',desc:'متابعة تشغيل وحالات الأوردرات مع رؤية الحسابات.',perms:{perm_view_orders:1,perm_add_order:0,perm_edit_order:1,perm_delete_order:0,perm_change_status:1,perm_customers:1,perm_calculator:1,perm_view_inventory:0,perm_manage_paper:0,perm_cut_paper:0,perm_paper_history:0,perm_view_bags:0,perm_manage_bags:0,perm_bags_history:0,perm_view_handles:0,perm_manage_handles:0,perm_handles_history:0,perm_view_accounts:1,perm_view_financial_totals:0,perm_manage_expenses:1,perm_manage_current_cash:0,perm_use_admin_cash_on_expense:0,perm_edit_expense_records:0,perm_delete_expense_records:0,perm_edit_cash_records:0,perm_delete_cash_records:0,perm_edit_admin_cash_records:0,perm_delete_admin_cash_records:0,perm_view_cost_logs:1,perm_view_sales_logs:1,perm_view_debts:1,perm_manage_debts:0,perm_view_reports:1,perm_suppliers:0,perm_purchases:0,perm_users:0,perm_activity_logs:0,perm_edit_activity_records:0,perm_delete_activity_records:0,perm_backup_restore:0,perm_act_as_other_admin:0}},
  production:{title:'إنتاج',desc:'مخازن + قص + تشغيل + أوردرات بدون حسابات.',perms:{perm_view_orders:1,perm_add_order:1,perm_edit_order:1,perm_delete_order:0,perm_change_status:1,perm_customers:1,perm_calculator:1,perm_view_inventory:1,perm_manage_paper:1,perm_cut_paper:1,perm_paper_history:1,perm_view_bags:1,perm_manage_bags:1,perm_bags_history:1,perm_view_handles:1,perm_manage_handles:1,perm_handles_history:1,perm_view_accounts:0,perm_view_financial_totals:0,perm_manage_expenses:0,perm_manage_current_cash:0,perm_use_admin_cash_on_expense:0,perm_edit_expense_records:0,perm_delete_expense_records:0,perm_edit_cash_records:0,perm_delete_cash_records:0,perm_edit_admin_cash_records:0,perm_delete_admin_cash_records:0,perm_view_cost_logs:0,perm_view_sales_logs:0,perm_view_debts:0,perm_manage_debts:0,perm_view_reports:0,perm_suppliers:0,perm_purchases:0,perm_users:0,perm_activity_logs:0,perm_edit_activity_records:0,perm_delete_activity_records:0,perm_backup_restore:0,perm_act_as_other_admin:0}},
  store:{title:'مخزن',desc:'المخازن فقط بالتفصيل.',perms:{perm_view_orders:0,perm_add_order:0,perm_edit_order:0,perm_delete_order:0,perm_change_status:0,perm_customers:0,perm_calculator:0,perm_view_inventory:1,perm_manage_paper:1,perm_cut_paper:1,perm_paper_history:1,perm_view_bags:1,perm_manage_bags:1,perm_bags_history:1,perm_view_handles:1,perm_manage_handles:1,perm_handles_history:1,perm_view_accounts:0,perm_view_financial_totals:0,perm_manage_expenses:0,perm_manage_current_cash:0,perm_use_admin_cash_on_expense:0,perm_edit_expense_records:0,perm_delete_expense_records:0,perm_edit_cash_records:0,perm_delete_cash_records:0,perm_edit_admin_cash_records:0,perm_delete_admin_cash_records:0,perm_view_cost_logs:0,perm_view_sales_logs:0,perm_view_debts:0,perm_manage_debts:0,perm_view_reports:0,perm_suppliers:0,perm_purchases:0,perm_users:0,perm_activity_logs:0,perm_edit_activity_records:0,perm_delete_activity_records:0,perm_backup_restore:0,perm_act_as_other_admin:0}},
  accountant:{title:'محاسب',desc:'حسابات ومديونيات وسجلات مالية وموردين ومشتريات.',perms:{perm_view_orders:1,perm_add_order:0,perm_edit_order:0,perm_delete_order:0,perm_change_status:0,perm_customers:1,perm_calculator:0,perm_view_inventory:1,perm_manage_paper:0,perm_cut_paper:0,perm_paper_history:1,perm_view_bags:1,perm_manage_bags:0,perm_bags_history:1,perm_view_handles:1,perm_manage_handles:0,perm_handles_history:1,perm_view_accounts:1,perm_view_financial_totals:1,perm_manage_expenses:1,perm_manage_current_cash:0,perm_use_admin_cash_on_expense:0,perm_edit_expense_records:0,perm_delete_expense_records:0,perm_edit_cash_records:0,perm_delete_cash_records:0,perm_edit_admin_cash_records:0,perm_delete_admin_cash_records:0,perm_view_cost_logs:1,perm_view_sales_logs:1,perm_view_debts:1,perm_manage_debts:1,perm_view_reports:1,perm_suppliers:1,perm_purchases:1,perm_users:0,perm_activity_logs:0,perm_edit_activity_records:0,perm_delete_activity_records:0,perm_backup_restore:0,perm_act_as_other_admin:0}}
};
let usersData=[];
let selectedRole='moderator';
let manualMode=false;
let currentPerms={...roles.moderator.perms};

function authFetch(url,opts={}){opts.headers=Object.assign({},opts.headers||{},{Authorization:'Bearer '+token});return fetch(url,opts).then(async r=>{const data=await r.json().catch(()=>({})); if(r.status===401){logout();throw new Error('unauthorized')} if(!r.ok) throw new Error(data.error||'error'); return data;});}
function logout(){try{fetch('/logout',{method:'POST',headers:{Authorization:'Bearer '+token}})}catch(e){} localStorage.clear(); location.href='login.html';}
function roleTitle(key){return roles[key]?.title || key || '-';}
function activeTag(v){return Number(v)?'<span class="tag ok">نشط</span>':'<span class="tag stop">موقوف</span>';}
function currentAccessNames(perms){return permKeys.filter(k=>Number(perms[k]||0)).map(k=>permMeta[k].title);}
function accessList(u){const list=currentAccessNames(u);return list.length?list.join(' - '):'بدون صلاحيات';}
function isPresetMatch(role, perms){const preset=roles[role]?.perms||{};return permKeys.every(k=>Number(perms[k]||0)===Number(preset[k]||0));}
function clonePreset(role){return JSON.parse(JSON.stringify(roles[role].perms));}
function permGroupLabel(key){
  if(key.includes('order') || key==='perm_customers' || key==='perm_calculator') return 'الأوردرات';
  if(key.includes('paper') || key.includes('inventory')) return 'الورق';
  if(key.includes('bag')) return 'الشنط';
  if(key.includes('handle')) return 'اليد';
  if(key.includes('account') || key.includes('financial') || key.includes('cost') || key.includes('sales') || key.includes('report') || key.includes('expense') || key.includes('cash')) return 'الحسابات والتقارير';
  if(key.includes('debt')) return 'المديونيات';
  if(key.includes('supplier') || key.includes('purchase')) return 'الموردين والمشتريات';
  if(key.includes('user')) return 'المستخدمين';
  if(key.includes('activity') || key.includes('backup')) return 'الإدارة';
  return 'عام';
}

function renderPermissions(){
  permissionsGrid.innerHTML=permKeys.map(key=>`<label class="perm-item"><div><b>${permMeta[key].title}</b><small>${permGroupLabel(key)} - ${permMeta[key].desc}</small></div><span class="switch"><input type="checkbox" ${Number(currentPerms[key]||0)?'checked':''} ${manualMode?'':'disabled'} onchange="togglePerm('${key}',this.checked)"><span class="slider"></span></span></label>`).join('');
  const names=currentAccessNames(currentPerms);
  quickAccess.innerHTML=names.length?names.map(x=>`<span class="chip">${x}</span>`).join(''):'<span class="chip off">بدون صلاحيات</span>';
  const presetMatch=isPresetMatch(selectedRole,currentPerms);
  roleHint.innerHTML=`<b>الدور المختار:</b> ${roles[selectedRole].title}<br>${roles[selectedRole].desc}<br><span class="muted">${manualMode?(presetMatch?'أنت تعدل يدويًا ولكنها مساوية لصلاحيات الدور':'أنت تعدل الصلاحيات يدويًا حاليًا بالتفصيل'):'حاليًا الصلاحيات ماشية على صلاحيات الدور المختار'}</span>`;
  presetBtn.classList.toggle('hidden',!manualMode);
  manualBtn.textContent=manualMode?'✅ إنهاء تعديل الصلاحيات':'🎛️ تعديل الصلاحيات بالتفصيل';
}
function renderRoles(){
  rolesBox.innerHTML=Object.entries(roles).map(([key,meta])=>`<div class="role ${selectedRole===key?'active':''}" onclick="pickRole('${key}')"><h4>${meta.title}</h4><p>${meta.desc}</p><ul>${currentAccessNames(meta.perms).map(x=>`<li>${x}</li>`).join('')}</ul></div>`).join('');
  renderPermissions();
}
function pickRole(role){ selectedRole=role; if(!manualMode) currentPerms=clonePreset(role); renderRoles(); }
function toggleManualPerms(){ manualMode=!manualMode; if(!manualMode && !u_id.value) currentPerms=clonePreset(selectedRole); renderPermissions(); }
function applyRolePreset(){ currentPerms=clonePreset(selectedRole); renderPermissions(); }
function togglePerm(key,val){ currentPerms[key]=val?1:0; renderPermissions(); }
function resetForm(){ u_id.value=''; u_username.value=''; u_password.value=''; u_name.value=''; u_active.value='1'; selectedRole='moderator'; manualMode=false; currentPerms=clonePreset(selectedRole); renderRoles(); window.scrollTo({top:0,behavior:'smooth'}); }
async function saveUser(){
  if(!u_username.value.trim()) return alert('اكتب اسم الدخول');
  if(!u_name.value.trim()) return alert('اكتب الاسم الكامل');
  if(!u_id.value && !u_password.value.trim()) return alert('اكتب كلمة المرور للمستخدم الجديد');
  await authFetch('/save-user',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:u_id.value||undefined,username:u_username.value.trim(),password:u_password.value,full_name:u_name.value.trim(),role:selectedRole,is_active:u_active.value,...currentPerms})});
  alert('تم حفظ المستخدم');
  resetForm();
  await loadUsers();
}
function editUser(u){
  u_id.value=u.id; u_username.value=u.username||''; u_password.value=''; u_name.value=u.full_name||''; u_active.value=Number(u.is_active)?'1':'0';
  selectedRole=roles[u.role]?u.role:'moderator';
  currentPerms={}; permKeys.forEach(k=>currentPerms[k]=Number(u[k]||0));
  manualMode=!isPresetMatch(selectedRole,currentPerms);
  renderRoles();
  window.scrollTo({top:0,behavior:'smooth'});
}
async function deleteUser(id){ const target=usersData.find(x=>x.id===id); if(!target) return; if(!confirm(`حذف المستخدم ${target.full_name}؟`)) return; await authFetch('/delete-user/'+id,{method:'DELETE'}); await loadUsers(); }
function renderUsersTable(){
  const q=(searchInput.value||'').trim().toLowerCase();
  const rows=usersData.filter(u=>!q || String(u.full_name||'').toLowerCase().includes(q) || String(u.username||'').toLowerCase().includes(q));
  usersBody.innerHTML = rows.length ? rows.map(u=>`<tr><td>${u.full_name||''}</td><td>${u.username||''}</td><td><span class="tag role">${roleTitle(u.role)}</span></td><td>${activeTag(u.is_active)}</td><td style="white-space:normal;max-width:360px">${accessList(u)}</td><td><div class="actions"><button class="btn-info" onclick='editUser(${JSON.stringify(u).replace(/'/g,"&#39;")})'>تعديل</button>${u.username!=='admin'?`<button class="btn-danger" onclick='deleteUser(${u.id})'>حذف</button>`:''}</div></td></tr>`).join('') : '<tr><td colspan="6" class="empty">لا يوجد مستخدمون</td></tr>';
  statUsers.innerText=usersData.length;
  statActive.innerText=usersData.filter(u=>Number(u.is_active)).length;
  statInactive.innerText=usersData.filter(u=>!Number(u.is_active)).length;
  statCustomRoles.innerText=usersData.filter(u=>!isPresetMatch(u.role,Object.fromEntries(permKeys.map(k=>[k,Number(u[k]||0)])))).length;
}
async function loadUsers(){ usersData = await authFetch('/users'); renderUsersTable(); }
welcome.innerText=`${user.full_name} | ${roleTitle(user.role)}`;
resetForm();
loadUsers();
