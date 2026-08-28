/* ============================================================================
   Craft Zone — إضافة حسابات التجار المعزولة لصفحة التجار (v9.9)
   ----------------------------------------------------------------------------
   بيضيف على الصفحة من غير ما يلمس الكود القديم:
     • كروت مالية معزولة (بيع، تكلفة، صافي ربح، محصّل، متبقي).
     • تكاليف مباشرة على الطلبية (نقل، عمولة، تغليف...).
     • كشف حساب التاجر برصيد جاري.
     • تفاصيل الطلبية: أوردراتها وتكاليفها وربحها.
     • اختيار الأوردرات بالبحث بدل ما تكتب أرقامها بإيدك.
     • رصيد افتتاحي وحد ائتمان لكل تاجر.
   الملف بيشتغل بنفس أسلوب باتشات صفحة الحسابات في المشروع.
   ========================================================================== */
(function () {
  'use strict';
  if (!/traders\.html$/i.test(location.pathname) && !document.getElementById('traderManager')) return;

  const token = localStorage.getItem('token');
  const user = JSON.parse(localStorage.getItem('user') || 'null');
  if (!token || !user) return;
  const isSuper = () => user.username === 'admin' || user.role === 'super_admin';
  const can = k => isSuper() || Number(user[k] || 0) === 1;
  const canManage = can('perm_manage_traders') || can('perm_customers');

  const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const money = v => (Number(v) || 0).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
  const $ = id => document.getElementById(id);
  const today = () => new Date().toISOString().slice(0, 10);

  function api(url, opts = {}) {
    opts.headers = Object.assign({}, opts.headers || {}, { Authorization: 'Bearer ' + token });
    return fetch(url, opts).then(async r => {
      const d = await r.json().catch(() => ({}));
      if (r.status === 401) { localStorage.clear(); location.href = 'login.html'; throw new Error('unauthorized'); }
      if (!r.ok) throw new Error(d.error || 'حصل خطأ');
      return d;
    });
  }
  const jpost = (u, b) => api(u, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b) });
  const jdel = u => api(u, { method: 'DELETE' });

  let FIN = { traders: [], totals: {} };
  let DASH = { batches: [], traders: [] };
  let pickerRows = [];

  /* ---------------- بناء الواجهة ---------------- */
  function buildUI() {
    const anchor = document.querySelector('.panel') || document.body;
    const host = document.createElement('div');
    host.id = 'traderFinanceHost';
    host.innerHTML = `
      <div class="panel" id="secTraderFinance">
        <div class="section-head">
          <h2>📊 حسابات التجار (معزولة عن حسابات المصنع)</h2>
          <div class="section-actions">
            <input type="date" id="tfFrom" style="width:auto" title="من تاريخ">
            <input type="date" id="tfTo" style="width:auto" title="إلى تاريخ">
            <button class="tab" onclick="tfReload()">🔄 تحديث</button>
          </div>
        </div>
        <div class="section-body">
          <div class="note-box">أرقام التجار دي مالهاش أي علاقة بصفحة الحسابات: مبيعات التجار وتكاليفهم وأرباحهم كلها محسوبة هنا لوحدها،
          ومش داخلة في مبيعات ولا أرباح ولا سيولة المصنع.</div>
          <div class="grid" id="tfStats"></div>
          <div class="table-wrap"><table><thead><tr>
            <th>التاجر</th><th>طلبيات</th><th>مفتوحة</th><th>رصيد افتتاحي</th><th>قيمة الشغل</th><th>التكلفة</th>
            <th>صافي الربح</th><th>الهامش</th><th>المحصّل</th><th>المتبقي علينا/عليه</th><th>حد الائتمان</th><th>تحكم</th>
          </tr></thead><tbody id="tfBody"></tbody></table></div>
        </div>
      </div>

      <div class="panel" id="secBatchCosts">
        <div class="section-head"><h2>🧾 تكاليف مباشرة على الطلبية</h2></div>
        <div class="section-body">
          <div class="note-box">التكاليف اللي مش جاية من الأوردرات نفسها: نقل للتاجر، عمولة، تغليف، عينات، رواجع... بتتخصم من ربح الطلبية.</div>
          ${canManage ? `<div class="form-grid">
            <div><label>الطلبية *</label><select id="bcBatch"></select></div>
            <div><label>نوع التكلفة</label><select id="bcType">
              <option>نقل وشحن</option><option>عمولة</option><option>تغليف</option><option>عينات</option><option>رواجع</option><option>أخرى</option>
            </select></div>
            <div><label>المبلغ *</label><input id="bcAmount" type="number" step="0.01"></div>
            <div><label>التاريخ</label><input id="bcDate" type="date"></div>
            <div><label>ملاحظة</label><input id="bcNote"></div>
            <div><label> </label><button class="filled" onclick="tfSaveBatchCost()">💾 حفظ التكلفة</button></div>
          </div>` : ''}
          <div id="bcList"></div>
        </div>
      </div>

      <div class="panel" id="secBatchDetail">
        <div class="section-head"><h2>🔍 تفاصيل الطلبية</h2>
          <div class="section-actions"><select id="bdBatch" onchange="tfLoadBatchDetail()" style="min-width:220px"></select></div>
        </div>
        <div class="section-body"><div id="bdBox" class="muted">اختار طلبية علشان تشوف أوردراتها وتكاليفها وربحها.</div></div>
      </div>

      ${canManage ? `<div class="panel" id="secOrderPicker">
        <div class="section-head"><h2>🔗 ربط أوردرات بالطلبية</h2></div>
        <div class="section-body">
          <div class="note-box">بدل ما تكتب أرقام الأوردرات بإيدك، دوّر واختار من هنا والسيستم هيربطهم ويعلّمهم أوردرات تاجر تلقائيًا.</div>
          <div class="form-grid">
            <div><label>الطلبية</label><select id="opBatch" onchange="tfLoadPicker()"></select></div>
            <div><label>بحث برقم الأوردر أو اسم العميل</label><input id="opSearch" oninput="tfLoadPickerDebounced()" placeholder="اكتب للبحث"></div>
            <div><label> </label><button class="filled" onclick="tfSaveLinks()">🔗 احفظ الربط</button></div>
          </div>
          <div class="table-wrap"><table><thead><tr><th>ربط</th><th>#</th><th>العميل</th><th>التاريخ</th><th>الحالة</th><th>الكمية</th><th>الإجمالي</th></tr></thead>
            <tbody id="opBody"></tbody></table></div>
        </div>
      </div>` : ''}

      <div class="panel" id="secStatement">
        <div class="section-head"><h2>📄 كشف حساب التاجر</h2>
          <div class="section-actions">
            <select id="stTrader" onchange="tfLoadStatement()" style="min-width:180px"></select>
            <input type="date" id="stFrom" onchange="tfLoadStatement()" style="width:auto">
            <input type="date" id="stTo" onchange="tfLoadStatement()" style="width:auto">
            <button class="tab" onclick="tfPrintStatement()">🖨️ طباعة</button>
          </div>
        </div>
        <div class="section-body"><div id="stBox" class="muted">اختار تاجر علشان تشوف كشف حسابه.</div></div>
      </div>

      ${canManage ? `<div class="panel" id="secTraderSettings">
        <div class="section-head"><h2>⚙️ رصيد افتتاحي وحد ائتمان</h2></div>
        <div class="section-body">
          <div class="form-grid">
            <div><label>التاجر *</label><select id="tsTrader" onchange="tfFillSettings()"></select></div>
            <div><label>رصيد افتتاحي عليه</label><input id="tsOpening" type="number" step="0.01"></div>
            <div><label>حد الائتمان</label><input id="tsLimit" type="number" step="0.01"></div>
            <div><label>مهلة السداد (يوم)</label><input id="tsTerms" type="number"></div>
            <div><label>خصم افتراضي %</label><input id="tsDiscount" type="number" step="0.01"></div>
            <div><label> </label><button class="filled" onclick="tfSaveSettings()">💾 حفظ</button></div>
          </div>
        </div>
      </div>` : ''}
    `;
    anchor.parentNode.insertBefore(host, anchor.nextSibling);
  }

  /* ---------------- الرسم ---------------- */
  function statCard(label, value, hint, cls) {
    return `<div class="stat"><small>${label}</small><b class="${cls || ''}">${value}</b>${hint ? `<div class="muted" style="margin-top:6px">${hint}</div>` : ''}</div>`;
  }

  function renderFinance() {
    const t = FIN.totals || {};
    $('tfStats').innerHTML = [
      statCard('قيمة شغل التجار', money(t.net_sale), 'من غير حسابات المصنع'),
      statCard('تكلفة الشغل', money(t.total_cost), 'تكاليف أوردرات + مباشرة ' + money(t.direct_costs)),
      statCard('صافي ربح التجار', money(t.net_profit), '', Number(t.net_profit) >= 0 ? 'positive' : 'negative'),
      statCard('المحصّل', money(t.collected), ''),
      statCard('المتبقي عند التجار', money(t.remaining), '', Number(t.remaining) > 0 ? 'negative' : 'positive'),
      statCard('الطلبيات', money(t.batches), money(t.open_batches) + ' مفتوحة'),
    ].join('');

    $('tfBody').innerHTML = (FIN.traders || []).map(r => {
      const over = Number(r.credit_limit) > 0 && Number(r.remaining) > Number(r.credit_limit);
      return `<tr>
        <td style="text-align:start"><b>${esc(r.name)}</b>${r.is_active ? '' : ' <span class="pill">موقوف</span>'}<br><small class="muted">${esc(r.phone || '')}</small></td>
        <td>${r.batches_count}</td><td>${r.open_batches}</td>
        <td>${money(r.opening_balance)}</td>
        <td><b>${money(r.net_sale)}</b></td><td>${money(r.total_cost)}</td>
        <td><b class="${Number(r.net_profit) >= 0 ? 'positive' : 'negative'}">${money(r.net_profit)}</b></td>
        <td>${money(r.margin_percent)}%</td>
        <td>${money(r.collected)}</td>
        <td><b class="${Number(r.remaining) > 0 ? 'negative' : 'positive'}">${money(r.remaining)}</b>${over ? '<br><span class="pill" style="border-color:#ef4444;color:#fecaca">عدّى الحد</span>' : ''}</td>
        <td>${Number(r.credit_limit) ? money(r.credit_limit) : '-'}</td>
        <td><button class="tab" onclick="tfOpenStatement(${r.id})">📄 كشف</button></td>
      </tr>`;
    }).join('') || '<tr><td colspan="12" class="muted">مفيش تجار</td></tr>';

    const opts = (FIN.traders || []).map(r => `<option value="${r.id}">${esc(r.name)}</option>`).join('');
    ['stTrader', 'tsTrader'].forEach(id => { const el = $(id); if (!el) return; const cur = el.value; el.innerHTML = opts; if (cur) el.value = cur; });
  }

  function batchOptions() {
    const src = DASH.batches || [];
    return src.map(b => `<option value="${b.id}">${esc(b.batch_name || ('طلبية #' + b.id))}${b.trader_name ? ' — ' + esc(b.trader_name) : ''}</option>`).join('');
  }
  function fillBatchSelects() {
    const opts = batchOptions();
    ['bcBatch', 'bdBatch', 'opBatch'].forEach(id => { const el = $(id); if (!el) return; const cur = el.value; el.innerHTML = '<option value="">— اختار —</option>' + opts; if (cur) el.value = cur; });
  }

  async function loadBatchCosts() {
    const box = $('bcList'); if (!box) return;
    const src = DASH.batches || [];
    if (!src.length) { box.innerHTML = '<div class="muted">مفيش طلبيات لسه.</div>'; return; }
    const rows = [];
    for (const b of src.slice(0, 40)) {
      try {
        const d = await api('/trader-batch-detail/' + b.id);
        (d.costs || []).forEach(c => rows.push({ ...c, batch_name: b.batch_name || ('طلبية #' + b.id) }));
      } catch (e) { /* تجاهل */ }
    }
    box.innerHTML = `<div class="table-wrap"><table><thead><tr><th>التاريخ</th><th>الطلبية</th><th>النوع</th><th>المبلغ</th><th>ملاحظة</th><th>تحكم</th></tr></thead>
      <tbody>${rows.length ? rows.map(c => `<tr><td>${esc(c.cost_date)}</td><td>${esc(c.batch_name)}</td><td>${esc(c.cost_type)}</td>
        <td><b>${money(c.amount)}</b></td><td>${esc(c.note || '')}</td>
        <td>${canManage ? `<button class="danger" onclick="tfDelBatchCost(${c.id})">حذف</button>` : ''}</td></tr>`).join('')
        : '<tr><td colspan="6" class="muted">مفيش تكاليف مباشرة</td></tr>'}</tbody></table></div>`;
  }

  /* ---------------- أفعال ---------------- */
  window.tfReload = async function () {
    const p = new URLSearchParams();
    if ($('tfFrom') && $('tfFrom').value) p.set('from', $('tfFrom').value);
    if ($('tfTo') && $('tfTo').value) p.set('to', $('tfTo').value);
    try {
      FIN = await api('/traders-finance-summary' + (p.toString() ? '?' + p.toString() : ''));
      try { DASH = await api('/traders-dashboard-data'); } catch (e) { DASH = { batches: [] }; }
      renderFinance(); fillBatchSelects(); loadBatchCosts();
    } catch (e) { console.error(e); }
  };

  window.tfSaveBatchCost = async function () {
    try {
      await jpost('/save-trader-batch-cost', {
        batch_id: Number($('bcBatch').value) || 0, cost_type: $('bcType').value,
        amount: Number($('bcAmount').value) || 0, cost_date: $('bcDate').value || today(), note: $('bcNote').value,
      });
      $('bcAmount').value = ''; $('bcNote').value = '';
      await window.tfReload();
    } catch (e) { alert(e.message); }
  };
  window.tfDelBatchCost = async function (id) {
    if (!confirm('تأكيد حذف التكلفة؟')) return;
    try { await jdel('/delete-trader-batch-cost/' + id); await window.tfReload(); } catch (e) { alert(e.message); }
  };

  window.tfLoadBatchDetail = async function () {
    const id = Number($('bdBatch').value) || 0;
    const box = $('bdBox');
    if (!id) { box.innerHTML = '<div class="muted">اختار طلبية.</div>'; return; }
    try {
      const d = await api('/trader-batch-detail/' + id);
      const f = d.finance || {};
      box.innerHTML = `
        <div class="cards-grid">
          <div class="card"><h3>ملخص الطلبية</h3>
            <div class="metric"><span>التاجر</span><b>${esc(d.trader?.name || '-')}</b></div>
            <div class="metric"><span>الحالة</span><b>${esc(d.batch?.status || '-')}</b></div>
            <div class="metric"><span>أوردرات</span><b>${f.orders_count} (مسلّم ${f.delivered_count})</b></div>
            <div class="metric"><span>قيمة البيع</span><b>${money(f.net_sale)}</b></div>
            <div class="metric"><span>تكلفة الأوردرات</span><b>${money(f.order_cost)}</b></div>
            <div class="metric"><span>تكاليف مباشرة</span><b>${money(f.direct_cost)}</b></div>
            <div class="metric"><span>إجمالي التكلفة</span><b>${money(f.total_cost)}</b></div>
            <div class="metric"><span>صافي الربح</span><b class="${f.net_profit >= 0 ? 'positive' : 'negative'}">${money(f.net_profit)}</b></div>
            <div class="metric"><span>الهامش</span><b>${money(f.margin_percent)}%</b></div>
            <div class="metric"><span>المحصّل</span><b>${money(f.collected)}</b></div>
            <div class="metric"><span>المتبقي</span><b class="${f.remaining > 0 ? 'negative' : 'positive'}">${money(f.remaining)}</b></div>
          </div>
        </div>
        <h3 class="mini-title">أوردرات الطلبية</h3>
        <div class="table-wrap"><table><thead><tr><th>#</th><th>العميل</th><th>التاريخ</th><th>الحالة</th><th>الكمية</th><th>الإجمالي</th><th>بيع محقق</th><th>تكلفة</th><th>ربح</th></tr></thead>
        <tbody>${(d.orders || []).map(o => `<tr><td>${o.id}</td><td>${esc(o.custName || '')}</td><td>${esc(o.orderDate || '')}</td>
          <td>${esc(o.status || '')}</td><td>${money(o.qty)}</td><td>${money(o.total_price)}</td>
          <td>${money(o.hist_sale)}</td><td>${money(o.hist_cost)}</td>
          <td class="${Number(o.hist_profit) >= 0 ? 'positive' : 'negative'}">${money(o.hist_profit)}</td></tr>`).join('')
          || '<tr><td colspan="9" class="muted">مفيش أوردرات مربوطة</td></tr>'}</tbody></table></div>`;
    } catch (e) { box.innerHTML = '<div class="note-box">' + esc(e.message) + '</div>'; }
  };

  let pickerTimer = null;
  window.tfLoadPickerDebounced = function () { clearTimeout(pickerTimer); pickerTimer = setTimeout(window.tfLoadPicker, 350); };
  window.tfLoadPicker = async function () {
    const batchId = Number($('opBatch').value) || 0;
    const batch = (DASH.batches || []).find(b => Number(b.id) === batchId);
    const p = new URLSearchParams({ batch_id: batchId });
    if (batch && batch.trader_id) p.set('trader_id', batch.trader_id);
    if ($('opSearch').value.trim()) p.set('q', $('opSearch').value.trim());
    try {
      const d = await api('/trader-linkable-orders?' + p.toString());
      pickerRows = d.orders || [];
      $('opBody').innerHTML = pickerRows.map(o => `<tr>
        <td><input type="checkbox" class="op-check" data-oid="${o.id}" ${Number(o.linked) ? 'checked' : ''} style="width:19px;height:19px"></td>
        <td>${o.id}</td><td>${esc(o.custName || '')}</td><td>${esc(o.orderDate || '')}</td>
        <td>${esc(o.status || '')}</td><td>${money(o.qty)}</td><td>${money(o.total_price)}</td></tr>`).join('')
        || '<tr><td colspan="7" class="muted">مفيش أوردرات مطابقة</td></tr>';
    } catch (e) { $('opBody').innerHTML = `<tr><td colspan="7">${esc(e.message)}</td></tr>`; }
  };
  window.tfSaveLinks = async function () {
    const batchId = Number($('opBatch').value) || 0;
    if (!batchId) return alert('اختار الطلبية الأول');
    const ids = [...document.querySelectorAll('.op-check:checked')].map(c => Number(c.dataset.oid));
    try {
      await jpost('/set-trader-batch-orders', { batch_id: batchId, order_ids: ids });
      alert('اتربط ' + ids.length + ' أوردر بالطلبية');
      if (typeof window.loadAll === 'function') await window.loadAll();
      await window.tfReload(); await window.tfLoadPicker();
    } catch (e) { alert(e.message); }
  };

  window.tfOpenStatement = function (traderId) {
    $('stTrader').value = String(traderId);
    window.tfLoadStatement();
    $('secStatement').scrollIntoView({ behavior: 'smooth' });
  };
  window.tfLoadStatement = async function () {
    const id = Number($('stTrader').value) || 0;
    const box = $('stBox');
    if (!id) { box.innerHTML = '<div class="muted">اختار تاجر.</div>'; return; }
    const p = new URLSearchParams();
    if ($('stFrom').value) p.set('from', $('stFrom').value);
    if ($('stTo').value) p.set('to', $('stTo').value);
    try {
      const d = await api('/trader-statement/' + id + (p.toString() ? '?' + p.toString() : ''));
      box.innerHTML = `<div class="note-box"><b>${esc(d.trader.name)}</b>${d.trader.phone ? ' · ' + esc(d.trader.phone) : ''}
        <br>رصيد أول المدة: <b>${money(d.opening_balance)}</b> · إجمالي الطلبيات: <b>${money(d.totals.sales)}</b> · المحصّل: <b>${money(d.totals.collected)}</b>
        <br>الرصيد النهائي: <b class="${d.closing_balance > 0 ? 'negative' : 'positive'}">${money(d.closing_balance)} ج</b> ${d.closing_balance > 0 ? '(عليه للشركة)' : '(متسدد)'}</div>
        <div class="table-wrap"><table><thead><tr><th>التاريخ</th><th>الحركة</th><th>المرجع</th><th>التفاصيل</th><th>عليه</th><th>له</th><th>الرصيد</th></tr></thead>
        <tbody>${d.rows.length ? d.rows.map(r => `<tr><td>${esc(r.date)}</td><td>${esc(r.kind)}</td><td>${esc(r.ref || '')}</td><td style="text-align:start">${esc(r.detail || '')}</td>
          <td class="negative">${r.debit ? money(r.debit) : ''}</td><td class="positive">${r.credit ? money(r.credit) : ''}</td>
          <td><b>${money(r.balance)}</b></td></tr>`).join('') : '<tr><td colspan="7" class="muted">مفيش حركات</td></tr>'}</tbody></table></div>`;
    } catch (e) { box.innerHTML = '<div class="note-box">' + esc(e.message) + '</div>'; }
  };
  window.tfPrintStatement = function () {
    const b = $('stBox').innerHTML;
    if (!b || $('stBox').classList.contains('muted')) return alert('اختار تاجر الأول');
    const w = window.open('', '_blank');
    w.document.write(`<html dir="rtl"><head><meta charset="utf-8"><title>كشف حساب تاجر</title>
      <style>body{font-family:Tahoma;padding:24px}table{width:100%;border-collapse:collapse}th,td{border:1px solid #999;padding:7px;text-align:center}
      .note-box{border:1px solid #999;padding:12px;margin-bottom:14px;line-height:1.9}</style></head><body>${b}</body></html>`);
    w.document.close(); w.print();
  };

  window.tfFillSettings = function () {
    const id = Number($('tsTrader').value) || 0;
    const r = (FIN.traders || []).find(x => x.id === id);
    if (!r) return;
    $('tsOpening').value = r.opening_balance || 0;
    $('tsLimit').value = r.credit_limit || 0;
  };
  window.tfSaveSettings = async function () {
    try {
      await jpost('/save-trader-finance-settings', {
        id: Number($('tsTrader').value) || 0,
        opening_balance: Number($('tsOpening').value) || 0,
        credit_limit: Number($('tsLimit').value) || 0,
        payment_terms_days: Number($('tsTerms').value) || 0,
        default_discount_percent: Number($('tsDiscount').value) || 0,
      });
      await window.tfReload(); alert('اتحفظ');
    } catch (e) { alert(e.message); }
  };

  /* ---------------- التشغيل ---------------- */
  function boot() {
    try { buildUI(); } catch (e) { console.error('trader finance UI failed', e); return; }
    if ($('bcDate')) $('bcDate').value = today();
    window.tfReload();
    setTimeout(fillBatchSelects, 1200);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
