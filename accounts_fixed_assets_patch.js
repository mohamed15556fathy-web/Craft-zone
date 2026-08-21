(function(){
  if (typeof authFetch !== 'function') return;

  const state = { rows: [], editingId: 0, canManage: false };
  const fullAdmin = user && (user.username === 'admin' || user.role === 'super_admin');
  const canView = !!(fullAdmin || Number(user?.perm_view_fixed_assets ?? user?.perm_view_assets_total ?? 0));
  const canManage = !!(fullAdmin || Number(user?.perm_manage_fixed_assets ?? user?.perm_manage_expenses ?? 0));
  const categories = ['مكن ومعدات','معدات المكان','أجهزة وشبكات','أثاث','عدد وأدوات','وسائل نقل','أخرى'];

  function byId(id){ return document.getElementById(id); }
  function safeQs(){ try { return typeof qs === 'function' ? qs() : ''; } catch (_) { return ''; } }
  function moneyLocal(v){ return typeof money === 'function' ? money(v) : Number(v || 0).toLocaleString('en-US',{maximumFractionDigits:2}) + ' ج'; }
  function escapeHtml(v){ return String(v ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
  function todayLocal(){ return new Date().toISOString().slice(0,10); }
  function currentEditRow(){ return state.rows.find(row => Number(row.id) === Number(state.editingId)) || null; }
  function openDetailsCard(type){
    const panel = byId('detailsPanel');
    const cash = byId('cashPanel');
    if (panel) panel.classList.remove('hidden');
    if (cash) cash.classList.add('hidden');
    if (typeof activateCard === 'function') activateCard(type);
  }
  function customCardToggle(type){
    const panel = byId('detailsPanel');
    if (typeof currentDetailsCard !== 'undefined' && currentDetailsCard === type && panel && !panel.classList.contains('hidden')) {
      if (typeof closeDetails === 'function') closeDetails();
      return true;
    }
    return false;
  }
  function categoryOptions(selected){
    const value = String(selected || '').trim();
    const list = categories.includes(value) || !value ? categories : [...categories, value];
    return list.map(item => `<option value="${escapeHtml(item)}" ${item === value ? 'selected' : ''}>${escapeHtml(item)}</option>`).join('');
  }
  function ensureStyles(){
    if (byId('fixedAssetsPatchStyles')) return;
    const style = document.createElement('style');
    style.id = 'fixedAssetsPatchStyles';
    style.textContent = `
      .asset-summary-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:14px}
      .asset-summary-item{background:#0b1220;border:1px solid var(--border);border-radius:12px;padding:12px}
      .asset-summary-item small{display:block;color:var(--muted);margin-bottom:6px}
      .asset-summary-item b{font-size:18px;color:var(--cyan)}
      .asset-form{background:#0b1220;border:1px solid var(--border);border-radius:14px;padding:14px;margin-bottom:14px}
      .asset-actions{display:flex;gap:8px;justify-content:center;flex-wrap:wrap}
      .asset-danger{border-color:#ef4444;color:#fecaca}
      .asset-section-title{margin:16px 0 6px;color:var(--cyan)}
    `;
    document.head.appendChild(style);
  }

  async function loadFixedAssetRows(){
    const data = await authFetch('/fixed-assets' + safeQs());
    state.rows = Array.isArray(data?.rows) ? data.rows : [];
    state.canManage = !!Number(data?.can_manage || 0);
    return data || {};
  }

  function fixedAssetForm(){
    if (!state.canManage) return '<div class="hint" style="margin-bottom:12px">عرض فقط — إضافة وتعديل الأصول تحتاج صلاحية شراء وتعديل الأصول.</div>';
    const row = currentEditRow() || {};
    return `
      <div class="asset-form">
        <h4 style="margin:0 0 12px">${state.editingId ? 'تعديل الأصل' : 'شراء أصل جديد'}</h4>
        <div class="form-grid">
          <div><label>اسم الأصل</label><input id="fixed_asset_name" value="${escapeHtml(row.asset_name || '')}" placeholder="مثال: مروحة / راوتر / ماكينة"></div>
          <div><label>التصنيف</label><select id="fixed_asset_category">${categoryOptions(row.category || 'معدات المكان')}</select></div>
          <div><label>سعر الشراء</label><input id="fixed_asset_price" type="number" min="0.01" step="0.01" value="${row.purchase_price ?? ''}"></div>
          <div><label>تاريخ الشراء</label><input id="fixed_asset_date" type="date" value="${escapeHtml(row.purchase_date || todayLocal())}"></div>
          <div style="grid-column:1/-1"><label>ملاحظات</label><textarea id="fixed_asset_notes" rows="2" placeholder="تفاصيل اختيارية">${escapeHtml(row.notes || '')}</textarea></div>
          <div style="grid-column:1/-1" class="hint">شراء الأصل بيتخصم من <b>السيولة الحالية</b> بس في تاريخ الشراء، وقيمته بتفضل ضمن <b>إجمالي الموجودات</b>. مابيأثرش على صافي الربح لأنه تحويل فلوس لأصل مش مصروف. مسموح أن تصبح الخزنة بالسالب أو يزيد الرصيد السالب.</div>
          <div style="display:flex;gap:8px;align-items:end;flex-wrap:wrap">
            <button type="button" class="filled" onclick="saveFixedAsset()">${state.editingId ? 'حفظ التعديل' : 'تسجيل شراء الأصل'}</button>
            ${state.editingId ? '<button type="button" onclick="cancelFixedAssetEdit()">إلغاء التعديل</button>' : ''}
          </div>
        </div>
      </div>`;
  }

  async function renderFixedAssetsDetails(){
    openDetailsCard('fixedAssets');
    byId('detailsTitle').textContent = 'الأصول الثابتة';
    byId('detailsNote').textContent = 'الأصل الثابت يخصم من السيولة الحالية فقط، وقيمته تظل ضمن إجمالي الموجودات، ولا يؤثر على صافي الربح';
    byId('detailsContent').innerHTML = '<div class="hint">جاري تحميل الأصول...</div>';
    try {
      const data = await loadFixedAssetRows();
      const rows = state.rows.length ? state.rows.map(row => `
        <tr>
          <td>${escapeHtml(row.purchase_date || '-')}</td>
          <td><b>${escapeHtml(row.asset_name || '-')}</b></td>
          <td>${escapeHtml(row.category || '-')}</td>
          <td>${moneyLocal(row.purchase_price)}</td>
          <td>${escapeHtml(row.notes || '-')}</td>
          <td>${state.canManage ? `<div class="asset-actions"><button type="button" onclick="editFixedAsset(${Number(row.id)})">تعديل</button><button type="button" class="asset-danger" onclick="deleteFixedAsset(${Number(row.id)})">حذف</button></div>` : '<span class="hint">عرض فقط</span>'}</td>
        </tr>`).join('') : '<tr><td colspan="6">لا توجد أصول مسجلة</td></tr>';
      byId('detailsContent').innerHTML = `
        <div class="asset-summary-grid">
          <div class="asset-summary-item"><small>إجمالي الأصول الثابتة</small><b>${moneyLocal(data.total_value)}</b></div>
          <div class="asset-summary-item"><small>شراء الأصول في الفترة</small><b>${moneyLocal(data.period_purchases)}</b></div>
          <div class="asset-summary-item"><small>عدد الأصول المعروضة</small><b>${state.rows.length}</b></div>
        </div>
        ${fixedAssetForm()}
        <div style="overflow:auto"><table><thead><tr><th>التاريخ</th><th>الأصل</th><th>التصنيف</th><th>سعر الشراء</th><th>ملاحظات</th><th>تحكم</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    } catch (e) {
      byId('detailsContent').innerHTML = `<div class="hint">${escapeHtml(e.message || 'تعذر تحميل الأصول')}</div>`;
    }
  }

  async function renderBusinessAssetsDetails(){
    openDetailsCard('assets');
    byId('detailsTitle').textContent = 'تفاصيل إجمالي الموجودات';
    byId('detailsNote').textContent = 'كل قيمة تنتقل بين المخزن والتشغيل والشحن بدون احتسابها مرتين';
    const c = cashSummary || {};
    const cashDisplay = canViewCurrentCashTotal ? moneyLocal(c.currentCash) : '<span class="hint">مخفي حسب الصلاحية</span>';
    const fixedDisplay = canView ? moneyLocal(c.totalFixedAssets) : '<span class="hint">مخفي حسب الصلاحية</span>';
    const countNote = (value, count) => `${moneyLocal(value)}${Number(count || 0) ? `<br><small>${Number(count)} أوردر/أمر</small>` : ''}`;
    byId('detailsContent').innerHTML = `
      <h4 class="asset-section-title">الموجودات</h4>
      ${detailsTable(['البند','القيمة'],[
        ['السيولة الحالية',cashDisplay],
        ['الخامات داخل المخازن',moneyLocal(c.totalInventoryValue)],
        ['الأصول الثابتة',fixedDisplay],
        ['شغل تحت التنفيذ',countNote(c.workInProgressValue,c.workInProgressOrders)],
        ['بضاعة جاهزة للشحن',countNote(c.finishedGoodsValue,c.finishedGoodsOrders)],
        ['بضاعة مشحونة وفي الطريق',countNote(c.goodsInTransitValue,c.goodsInTransitOrders)],
        ['مستحقات بعد التسليم وفلوس لينا برا',moneyLocal(c.receivablesAssetValue)],
        ['دفعات مقدمة للموردين والصنايعية',moneyLocal(c.supplierAdvancesValue)],
        ['إجمالي الموجودات',`<b>${moneyLocal(c.totalAssets)}</b>`]
      ])}
      <h4 class="asset-section-title">الالتزامات والديون</h4>
      ${detailsTable(['البند','القيمة'],[
        ['متبقي فواتير الموردين وأرصدة البداية',moneyLocal(c.supplierPayables)],
        ['الديون المسجلة',moneyLocal(c.recordedDebts)],
        ['تكاليف تشغيل مستحقة لم تُدفع',moneyLocal(c.operationalPayables)],
        ['عربون أوردرات مفتوحة لم تُسلّم',moneyLocal(c.customerAdvances)],
        ['إجمالي الالتزامات والديون',`<b>${moneyLocal(c.totalLiabilities)}</b>`],
        ['صافي الموجودات بعد الالتزامات',`<b>${moneyLocal(c.netAssets)}</b>`]
      ])}`;
  }

  function renderProfitDetails(){
    openDetailsCard('profit');
    byId('detailsTitle').textContent = 'تفاصيل صافي الربح';
    byId('detailsNote').textContent = 'صافي الربح الفعلي والربح المتوقع للأوردرات المفتوحة';
    const actualProfit = Number(accountsSummary.actualProfit ?? accountsSummary.totalProfit ?? 0);
    const expectedProfit = Number(accountsSummary.expectedProfit || 0);
    const totalWithExpected = Number(accountsSummary.totalProfitWithExpected ?? (actualProfit + expectedProfit));
    byId('detailsContent').innerHTML = detailsTable(['البند','القيمة'],[
      ['إجمالي المبيعات الفعلية',moneyLocal(accountsSummary.totalSales)],
      ['إجمالي التكاليف الفعلية/المستحقة',moneyLocal(accountsSummary.totalCosts)],
      ['إجمالي المصاريف',moneyLocal(accountsSummary.totalExpenses)],
      ['صافي الربح الفعلي',`<b>${moneyLocal(actualProfit)}</b>`],
      ['مبيعات متوقعة للأوردرات المفتوحة',moneyLocal(accountsSummary.expectedProfitSales)],
      ['تكاليف متوقعة للأوردرات المفتوحة',moneyLocal(accountsSummary.expectedProfitCosts)],
      ['الربح المتوقع',`${moneyLocal(expectedProfit)}<br><small>محسوب على ${Number(accountsSummary.expectedProfitOrdersCount || 0)} أوردر مفتوح قبل التسليم</small>`],
      ['صافي الربح بعد المتوقع',`<b>${moneyLocal(totalWithExpected)}</b>`]
    ]);
  }

  window.saveFixedAsset = async function(){
    if (!state.canManage) return alert('غير مصرح لك بشراء الأصول');
    const body = {
      asset_name: byId('fixed_asset_name')?.value?.trim() || '',
      category: byId('fixed_asset_category')?.value || 'معدات المكان',
      purchase_price: byId('fixed_asset_price')?.value || 0,
      purchase_date: byId('fixed_asset_date')?.value || todayLocal(),
      notes: byId('fixed_asset_notes')?.value?.trim() || ''
    };
    if (!body.asset_name) return alert('اكتب اسم الأصل');
    if (Number(body.purchase_price || 0) <= 0) return alert('اكتب سعر شراء صحيح');
    try {
      const wasEditing = !!state.editingId;
      const path = state.editingId ? `/fixed-assets/${Number(state.editingId)}` : '/fixed-assets';
      await authFetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
      state.editingId = 0;
      if (typeof loadSummary === 'function') await loadSummary();
      if (typeof loadCash === 'function') await loadCash();
      await renderFixedAssetsDetails();
      alert(wasEditing ? 'تم تعديل الأصل بنجاح' : 'تم تسجيل شراء الأصل وخصمه من السيولة الحالية');
    } catch (e) { alert(e.message || 'تعذر حفظ الأصل'); }
  };
  window.editFixedAsset = async function(id){
    state.editingId = Number(id);
    await renderFixedAssetsDetails();
    byId('fixed_asset_name')?.focus();
  };
  window.cancelFixedAssetEdit = async function(){
    state.editingId = 0;
    await renderFixedAssetsDetails();
  };
  window.deleteFixedAsset = async function(id){
    const row = state.rows.find(item => Number(item.id) === Number(id));
    if (!confirm(`حذف الأصل ${row?.asset_name || ''}؟ سيتم عكس خصمه من السيولة الحالية.`)) return;
    try {
      await authFetch('/fixed-assets/' + Number(id), {method:'DELETE'});
      state.editingId = 0;
      if (typeof loadSummary === 'function') await loadSummary();
      if (typeof loadCash === 'function') await loadCash();
      await renderFixedAssetsDetails();
    } catch (e) { alert(e.message || 'تعذر حذف الأصل'); }
  };

  function patchLoadCash(){
    const oldLoadCash = window.loadCash;
    if (typeof oldLoadCash !== 'function') return;
    window.loadCash = async function(){
      const result = await oldLoadCash.apply(this, arguments);
      const el = byId('fixedAssetsValue');
      if (el) el.innerText = canView ? moneyLocal(cashSummary?.totalFixedAssets) : 'مخفي';
      const breakdown = byId('cashBreakdown');
      if (breakdown && canView && Number(cashSummary?.fixedAssetPurchases || 0) > 0) {
        breakdown.insertAdjacentHTML('beforeend', ` | شراء أصول (من السيولة فقط): <b>${moneyLocal(cashSummary.fixedAssetPurchases)}</b>`);
      }
      return result;
    };
  }
  function patchDetails(){
    const oldShow = window.showDetails;
    window.showDetails = async function(type){
      if (type === 'fixedAssets') {
        if (!canView) return;
        if (customCardToggle(type)) return;
        return renderFixedAssetsDetails();
      }
      if (type === 'assets') {
        if (!canViewAssetsTotal) return;
        if (customCardToggle(type)) return;
        return renderBusinessAssetsDetails();
      }
      if (type === 'profit') {
        if (!canViewFinancialTotals) return;
        if (!canView) return oldShow ? oldShow.apply(this, arguments) : undefined;
        if (customCardToggle(type)) return;
        return renderProfitDetails();
      }
      return oldShow ? oldShow.apply(this, arguments) : undefined;
    };
  }
  function applyVisibility(){
    const card = document.querySelector('[data-card="fixedAssets"]');
    if (card) card.style.display = canView && !expensesOnly ? '' : 'none';
  }
  async function init(){
    ensureStyles();
    patchLoadCash();
    patchDetails();
    applyVisibility();
    if (typeof loadCash === 'function') await loadCash();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
