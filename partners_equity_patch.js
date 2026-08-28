/* ============================================================================
   Craft Zone — حقوق الشركاء لصفحة الشركاء (v9.9)
   ----------------------------------------------------------------------------
   بيضيف على الصفحة:
     • جدول حقوق الشركاء الحقيقي: رأس مال + نصيب الأرباح − السحوبات = رصيده.
     • النسبة تلقائي من رأس المال، مع إمكانية تثبيت نسبة يدوية لأي شريك،
       وتحذير واضح لو المجموع مش 100%.
     • معاينة توزيع الأرباح قبل الترحيل، وترحيل بيمنع التوزيع المزدوج.
     • كشف حساب شريك برصيد جاري.
   ========================================================================== */
(function () {
  'use strict';
  const token = localStorage.getItem('token');
  const user = JSON.parse(localStorage.getItem('user') || 'null');
  if (!token || !user) return;
  if (!/partners\.html$/i.test(location.pathname)) return;

  const isSuper = () => user.username === 'admin' || user.role === 'super_admin';
  const can = k => isSuper() || Number(user[k] || 0) === 1;
  const canManage = can('perm_manage_financial_partners');
  const canTx = can('perm_manage_partner_transactions') || can('perm_manage_expenses');

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

  let EQ = { partners: [], totals: {}, shares: {} };

  function buildUI() {
    const anchor = document.querySelector('.panel, .card, section') || document.body;
    const host = document.createElement('div');
    host.id = 'partnerEquityHost';
    host.innerHTML = `
      <div class="panel" id="secEquity" style="margin-top:18px">
        <div class="section-head"><h2>🤝 حقوق الشركاء</h2>
          <div class="section-actions"><button class="tab" onclick="peReload()">🔄 تحديث</button></div>
        </div>
        <div class="section-body">
          <div id="peShareWarn"></div>
          <div class="grid" id="peStats"></div>
          <div class="table-wrap"><table><thead><tr>
            <th>الشريك</th><th>النوع</th><th>النسبة</th><th>طريقة النسبة</th><th>رأس المال</th>
            <th>نصيب أرباح مرحّل</th><th>السحوبات</th><th>رصيد الشريك</th><th>تحكم</th>
          </tr></thead><tbody id="peBody"></tbody></table></div>
          <div class="note-box" style="margin-top:14px">
            <b>رصيد الشريك</b> = رأس ماله + نصيبه من الأرباح المرحّلة − سحوباته.
            الرقم الموجب معناه الشركة مدينة له، والسالب معناه سحب أكتر من حقوقه.
            <br><b>النسبة التلقائية</b> بتتحسب من رأس مال كل شريك ÷ إجمالي رأس مال الشركاء التلقائيين، والباقي بعد النسب المثبتة يدويًا.
          </div>
        </div>
      </div>

      ${canManage ? `<div class="panel" id="secShareSettings">
        <div class="section-head"><h2>⚙️ نسبة الشريك ورأس ماله الافتتاحي</h2></div>
        <div class="section-body">
          <div class="form-grid">
            <div><label>الشريك *</label><select id="psPartner" onchange="peFillShare()"></select></div>
            <div><label>طريقة النسبة</label><select id="psMode" onchange="peToggleShareInput()">
              <option value="auto">تلقائي من رأس المال</option>
              <option value="manual">نسبة يدوية ثابتة</option>
            </select></div>
            <div><label>النسبة اليدوية %</label><input id="psShare" type="number" step="0.01" min="0" max="100"></div>
            <div><label>رأس مال افتتاحي</label><input id="psOpening" type="number" step="0.01"></div>
            <div><label>تاريخ الدخول</label><input id="psJoin" type="date"></div>
            <div><label> </label><button class="filled" onclick="peSaveShare()">💾 حفظ</button></div>
          </div>
        </div>
      </div>` : ''}

      ${canTx ? `<div class="panel" id="secSettlement">
        <div class="section-head"><h2>💹 توزيع الأرباح على الشركاء</h2></div>
        <div class="section-body">
          <div class="note-box">التوزيع بيتم على فترة محددة، وبيتسجّل في حقوق كل شريك. لو حاولت توزّع نفس الفترة تاني السيستم هيمنعك،
          علشان الربح ما يتوزّعش مرتين. أرباح التجار مش داخلة هنا — هي في صفحة التجار لوحدها.</div>
          <div class="form-grid">
            <div><label>من تاريخ *</label><input id="peFrom" type="date" onchange="pePreview()"></div>
            <div><label>إلى تاريخ *</label><input id="peTo" type="date" onchange="pePreview()"></div>
            <div><label>ملاحظة</label><input id="peNote"></div>
            <div><label> </label><button class="tab" onclick="pePreview()">👁️ معاينة التوزيع</button></div>
            <div><label> </label><button class="filled" onclick="pePost()">✅ رحّل التوزيع</button></div>
          </div>
          <div id="pePreviewBox"></div>
        </div>
      </div>` : ''}

      <div class="panel" id="secPartnerStatement">
        <div class="section-head"><h2>📄 كشف حساب شريك</h2>
          <div class="section-actions">
            <select id="pstPartner" onchange="peStatement()" style="min-width:170px"></select>
            <input type="date" id="pstFrom" onchange="peStatement()" style="width:auto">
            <input type="date" id="pstTo" onchange="peStatement()" style="width:auto">
            <button class="tab" onclick="pePrintStatement()">🖨️ طباعة</button>
          </div>
        </div>
        <div class="section-body"><div id="pstBox" class="muted">اختار شريك علشان تشوف كشف حسابه.</div></div>
      </div>
    `;
    anchor.parentNode.insertBefore(host, anchor.nextSibling);
  }

  function statCard(l, v, h, c) { return `<div class="stat"><small>${l}</small><b class="${c || ''}">${v}</b>${h ? `<div class="muted" style="margin-top:6px">${h}</div>` : ''}</div>`; }

  function render() {
    const t = EQ.totals || {}, sh = EQ.shares || {};
    $('peStats').innerHTML = [
      statCard('عدد الشركاء', money(t.partners), ''),
      statCard('صافي رأس المال', money(t.capital_net), 'اللي داخل ناقص المسترد'),
      statCard('أرباح مرحّلة للشركاء', money(t.profit_credited), ''),
      statCard('إجمالي السحوبات', money(t.drawings), ''),
      statCard('إجمالي حقوق الشركاء', money(t.equity_total), '', Number(t.equity_total) >= 0 ? 'positive' : 'negative'),
      statCard('مجموع النسب', money(sh.total) + '%', sh.balanced ? 'متظبط' : 'محتاج مراجعة', sh.balanced ? 'positive' : 'negative'),
    ].join('');

    $('peShareWarn').innerHTML = sh.balanced ? '' :
      `<div class="note-box" style="border-color:#ef4444;color:#fecaca">⚠️ مجموع نسب الشركاء ${money(sh.total)}% مش 100%. ${esc(sh.warning || '')}</div>`;

    $('peBody').innerHTML = (EQ.partners || []).map(p => `<tr>
      <td style="text-align:start"><b>${esc(p.name)}</b>${p.is_active ? '' : ' <span class="pill">موقوف</span>'}</td>
      <td>${p.partner_type === 'murabaha' ? 'ممول مرابحة' : 'شريك أساسي'}</td>
      <td><b>${money(p.share_percent)}%</b></td>
      <td>${p.share_mode === 'manual' ? 'يدوية ثابتة' : 'تلقائي من رأس المال'}</td>
      <td>${money(p.capital_net)}</td>
      <td>${money(p.profit_credited)}</td>
      <td>${money(p.drawings)}</td>
      <td><b class="${Number(p.equity_balance) >= 0 ? 'positive' : 'negative'}">${money(p.equity_balance)}</b></td>
      <td><button class="tab" onclick="peOpenStatement(${JSON.stringify(p.name)})">📄 كشف</button></td>
    </tr>`).join('') || '<tr><td colspan="9" class="muted">مفيش شركاء</td></tr>';

    const opts = (EQ.partners || []).map(p => `<option value="${esc(p.name)}" data-id="${p.id}">${esc(p.name)}</option>`).join('');
    ['psPartner', 'pstPartner'].forEach(id => { const el = $(id); if (!el) return; const cur = el.value; el.innerHTML = opts; if (cur) el.value = cur; });
    if ($('psPartner')) peFillShare();
  }

  window.peReload = async function () {
    try { EQ = await api('/partners-equity'); render(); } catch (e) { console.error(e); }
  };
  window.peToggleShareInput = function () {
    const manual = $('psMode').value === 'manual';
    $('psShare').disabled = !manual;
    $('psShare').style.opacity = manual ? '1' : '.45';
  };
  window.peFillShare = function () {
    const name = $('psPartner').value;
    const p = (EQ.partners || []).find(x => x.name === name);
    if (!p) return;
    $('psMode').value = p.share_mode || 'auto';
    $('psShare').value = p.manual_share_percent || 0;
    $('psOpening').value = p.opening_capital || 0;
    window.peToggleShareInput();
  };
  window.peSaveShare = async function () {
    const name = $('psPartner').value;
    const p = (EQ.partners || []).find(x => x.name === name);
    if (!p) return alert('اختار شريك');
    try {
      const r = await jpost('/save-partner-equity-settings', {
        id: p.id, share_mode: $('psMode').value,
        share_percent: Number($('psShare').value) || 0,
        opening_capital: Number($('psOpening').value) || 0,
        join_date: $('psJoin').value,
      });
      await window.peReload();
      if (r.shares && !r.shares.balanced) alert('اتحفظ، بس خد بالك: مجموع النسب دلوقتي ' + money(r.shares.total) + '% — ' + (r.shares.warning || ''));
      else alert('اتحفظ');
    } catch (e) { alert(e.message); }
  };

  window.pePreview = async function () {
    const from = $('peFrom').value, to = $('peTo').value;
    if (!from || !to) { $('pePreviewBox').innerHTML = '<div class="muted">حدد الفترة الأول.</div>'; return; }
    try {
      const d = await api(`/partner-settlement-preview?from=${from}&to=${to}`);
      $('pePreviewBox').innerHTML = `
        <div class="note-box">
          ربح الشغل في الفترة: <b>${money(d.gross_profit)}</b> − مصاريف عامة: <b>${money(d.general_expenses)}</b> = صافي: <b>${money(d.net_profit)}</b>
          <br>اتوزّع قبل كده في فترات متداخلة: <b>${money(d.already_allocated)}</b> ← <b>القابل للتوزيع دلوقتي: ${money(d.distributable)}</b>
          ${Math.abs(d.unallocated) > 0.5 ? `<br><span class="negative">فاضل من غير توزيع: ${money(d.unallocated)} — راجع النسب.</span>` : ''}
        </div>
        <div class="table-wrap"><table><thead><tr><th>الشريك</th><th>النوع</th><th>الطريقة</th><th>النسبة</th><th>الأساس</th><th>نصيبه</th></tr></thead>
        <tbody>${(d.lines || []).map(l => `<tr><td style="text-align:start"><b>${esc(l.name)}</b></td>
          <td>${l.partner_type === 'murabaha' ? 'ممول' : 'شريك'}</td><td>${esc(l.method)}</td>
          <td>${l.partner_type === 'murabaha' ? money(l.profit_rate_percent) + '% على رأس المال' : money(l.share_percent) + '%'}</td>
          <td>${money(l.basis)}</td><td><b class="positive">${money(l.amount)}</b></td></tr>`).join('')}</tbody></table></div>`;
    } catch (e) { $('pePreviewBox').innerHTML = '<div class="note-box">' + esc(e.message) + '</div>'; }
  };
  window.pePost = async function () {
    const from = $('peFrom').value, to = $('peTo').value;
    if (!from || !to) return alert('حدد الفترة');
    if (!confirm('هيتم ترحيل نصيب كل شريك من أرباح الفترة دي لحسابه. تأكيد؟')) return;
    try {
      const r = await jpost('/post-partner-settlement', { period_from: from, period_to: to, note: $('peNote').value });
      alert(`اترحّل التوزيع: صافي ${money(r.net_profit)} على ${r.lines.length} شريك`);
      await window.peReload(); await window.pePreview();
    } catch (e) {
      if (/تسوية مرحّلة|مجموع النسب/.test(e.message) && confirm(e.message + '\n\nتحب تكمل بالغصب؟')) {
        try {
          const r = await jpost('/post-partner-settlement', { period_from: from, period_to: to, note: $('peNote').value, force: 1 });
          alert('اترحّل التوزيع'); await window.peReload();
        } catch (e2) { alert(e2.message); }
      } else alert(e.message);
    }
  };

  window.peOpenStatement = function (name) { $('pstPartner').value = name; window.peStatement(); $('secPartnerStatement').scrollIntoView({ behavior: 'smooth' }); };
  window.peStatement = async function () {
    const name = $('pstPartner').value;
    const box = $('pstBox');
    if (!name) { box.innerHTML = '<div class="muted">اختار شريك.</div>'; return; }
    const p = new URLSearchParams();
    if ($('pstFrom').value) p.set('from', $('pstFrom').value);
    if ($('pstTo').value) p.set('to', $('pstTo').value);
    try {
      const d = await api('/partner-statement/' + encodeURIComponent(name) + (p.toString() ? '?' + p.toString() : ''));
      box.innerHTML = `<div class="note-box"><b>${esc(name)}</b>
        <br>رصيد أول المدة: <b>${money(d.opening_balance)}</b> · له في الفترة: <b>${money(d.totals.credit)}</b> · أخد: <b>${money(d.totals.debit)}</b>
        <br>الرصيد النهائي: <b class="${d.closing_balance >= 0 ? 'positive' : 'negative'}">${money(d.closing_balance)} ج</b>
        ${d.closing_balance >= 0 ? '(له عند الشركة)' : '(سحب بالزيادة)'}</div>
        <div class="table-wrap"><table><thead><tr><th>التاريخ</th><th>الحركة</th><th>البيان</th><th>له</th><th>عليه</th><th>الرصيد</th></tr></thead>
        <tbody>${d.rows.length ? d.rows.map(r => `<tr><td>${esc(r.date)}</td><td>${esc(r.kind)}</td><td style="text-align:start">${esc(r.note || '')}</td>
          <td class="positive">${r.credit ? money(r.credit) : ''}</td><td class="negative">${r.debit ? money(r.debit) : ''}</td>
          <td><b>${money(r.balance)}</b></td></tr>`).join('') : '<tr><td colspan="6" class="muted">مفيش حركات</td></tr>'}</tbody></table></div>`;
    } catch (e) { box.innerHTML = '<div class="note-box">' + esc(e.message) + '</div>'; }
  };
  window.pePrintStatement = function () {
    const b = $('pstBox').innerHTML;
    if (!b || $('pstBox').classList.contains('muted')) return alert('اختار شريك الأول');
    const w = window.open('', '_blank');
    w.document.write(`<html dir="rtl"><head><meta charset="utf-8"><title>كشف حساب شريك</title>
      <style>body{font-family:Tahoma;padding:24px}table{width:100%;border-collapse:collapse}th,td{border:1px solid #999;padding:7px;text-align:center}
      .note-box{border:1px solid #999;padding:12px;margin-bottom:14px;line-height:1.9}</style></head><body>${b}</body></html>`);
    w.document.close(); w.print();
  };

  function boot() {
    try { buildUI(); } catch (e) { console.error('partner equity UI failed', e); return; }
    if ($('peTo')) { $('peTo').value = today(); $('peFrom').value = today().slice(0, 8) + '01'; }
    window.peReload();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
