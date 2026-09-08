/*
 * Craft Zone — تسعير وأرباح الخامات (v10.1)
 * ------------------------------------------------------------------
 * بيضيف على صفحتَي مخزن الورق ومخزن اليد:
 *   1) كروت: استهلكت كام / بعت بكام / كسبت كام / قيمة المخزون بالشراء والبيع
 *   2) صندوق تدوير الخامات: المبلغ المحجوز لإعادة الشراء + تفاصيله + تصفير
 *   3) أسعار البيع: على مستوى النوع (البرستول كله بكذا) أو على مستوى الصنف
 *   4) سجل تغيّر الأسعار: مين غيّر إيه من كام لكام وامتى
 * الملف مستقل تمامًا وبيشتغل فوق الصفحة الموجودة من غير ما يلمس أي كود قديم.
 */
(function () {
  'use strict';

  const PAGE = document.getElementById('pBody') ? 'paper' : (document.getElementById('handlesBody') ? 'handle' : null);
  if (!PAGE) return;

  const ITEM_TYPE = PAGE;
  const IS_PAPER = ITEM_TYPE === 'paper';
  const UNIT = IS_PAPER ? 'كجم' : 'قطعة';
  const TITLE = IS_PAPER ? 'الورق' : 'اليد';
  const PAPER_TYPES = ['كرافت', 'ورق طبع', 'برستول كوشيه', 'كوشيه', 'ورق زبدة'];
  const HANDLE_TYPES = ['مجدول', 'دوباره', 'ستان'];
  const TYPE_OPTIONS = IS_PAPER ? PAPER_TYPES : HANDLE_TYPES;

  const token = localStorage.getItem('token');
  let currentUser = null;
  try { currentUser = JSON.parse(localStorage.getItem('user') || 'null'); } catch (_) { currentUser = null; }
  if (!token || !currentUser) return;

  const state = { summary: null, fund: null, prices: null, history: [], tab: 'prices', from: '', to: '' };

  // نفس منطق صفحة الأصناف: في الأنواع دي اللون أبيض إجباري.
  const WHITE_ONLY_TYPES = ['ورق طبع', 'ورق زبدة', 'كوشيه', 'برستول كوشيه'];
  const isWhiteOnly = t => IS_PAPER && WHITE_ONLY_TYPES.indexOf(String(t || '').trim()) >= 0;
  const BASE_COLORS = IS_PAPER ? ['بني', 'أبيض', 'أسود'] : ['بني', 'أبيض', 'أسود'];

  // ---------------------------------------------------------------- utils
  function api(url, opts) {
    opts = opts || {};
    opts.headers = Object.assign({}, opts.headers || {}, { Authorization: 'Bearer ' + token });
    return fetch(url, opts).then(async r => {
      const data = await r.json().catch(() => ({}));
      if (r.status === 401) { localStorage.clear(); location.href = 'login.html'; throw new Error('unauthorized'); }
      if (!r.ok) throw new Error(data.error || 'خطأ في الاتصال');
      return data;
    });
  }
  const n = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
  const eg = v => n(v).toLocaleString('en-US', { maximumFractionDigits: 2 }) + ' ج';
  const qty = v => n(v).toLocaleString('en-US', { maximumFractionDigits: 2 });
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  const dt = v => { try { return new Date(v).toLocaleString('ar-EG', { dateStyle: 'short', timeStyle: 'short' }); } catch (_) { return String(v || ''); } };
  function toast(msg, ok) {
    const el = document.createElement('div');
    el.textContent = msg;
    el.style.cssText = `position:fixed;bottom:24px;inset-inline-start:24px;z-index:99999;background:${ok === false ? '#7f1d1d' : '#065f46'};color:#fff;padding:12px 18px;border-radius:12px;font-weight:700;box-shadow:0 8px 24px rgba(0,0,0,.4);max-width:90vw`;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3800);
  }

  // ---------------------------------------------------------------- styles
  const css = document.createElement('style');
  css.textContent = `
  .mp-wrap{background:#1e293b;border:1px solid #334155;border-radius:16px;padding:18px;margin:18px 0}
  .mp-head{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px}
  .mp-head h3{margin:0;font-size:19px}
  .mp-tabs{display:flex;gap:8px;flex-wrap:wrap}
  .mp-tab{background:#0f172a;border:1px solid #334155;color:#cbd5e1;padding:8px 14px;border-radius:10px;cursor:pointer;font-weight:700;font-size:13px}
  .mp-tab.on{background:#22d3ee;color:#04222b;border-color:#22d3ee}
  .mp-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px}
  .mp-card{background:#0f172a;border:1px solid #334155;border-radius:14px;padding:14px}
  .mp-card small{display:block;color:#94a3b8;font-size:12px;font-weight:700;margin-bottom:7px}
  .mp-card b{font-size:21px;font-weight:900;display:block;line-height:1.3}
  .mp-card .sub{color:#64748b;font-size:11.5px;margin-top:6px;font-weight:700}
  .mp-card.click{cursor:pointer;transition:.15s}
  .mp-card.click:hover{border-color:#22d3ee;transform:translateY(-2px)}
  .mp-green b{color:#4ade80}.mp-red b{color:#f87171}.mp-cyan b{color:#22d3ee}.mp-amber b{color:#fbbf24}
  .mp-row{display:flex;gap:10px;flex-wrap:wrap;align-items:end;margin-bottom:14px}
  .mp-f{display:flex;flex-direction:column;gap:5px}
  .mp-f label{color:#94a3b8;font-size:12px;font-weight:700}
  .mp-wrap input,.mp-wrap select{background:#0f172a;color:#f1f5f9;border:1px solid #334155;padding:9px 11px;border-radius:9px;font-family:inherit;font-size:13px}
  .mp-btn{background:#22d3ee;color:#04222b;border:none;padding:9px 16px;border-radius:9px;cursor:pointer;font-weight:800;font-size:13px;font-family:inherit}
  .mp-btn.ghost{background:#0f172a;color:#cbd5e1;border:1px solid #334155}
  .mp-btn.warn{background:#fbbf24;color:#3b2708}
  .mp-btn.mini{padding:6px 11px;font-size:12px}
  .mp-tbl{width:100%;border-collapse:collapse;margin-top:12px;font-size:13px}
  .mp-tbl th{background:#111827;color:#94a3b8;padding:10px 8px;text-align:center;font-size:12px;position:sticky;top:0}
  .mp-tbl td{padding:9px 8px;border-bottom:1px solid #263449;text-align:center}
  .mp-tbl tr:hover td{background:#16233a}
  .mp-scroll{max-height:460px;overflow:auto;border-radius:12px;border:1px solid #334155}
  .mp-empty{text-align:center;color:#64748b;padding:26px;font-weight:700}
  .mp-note{color:#94a3b8;font-size:12.5px;line-height:1.8;margin-top:10px}
  .mp-modal{display:none;position:fixed;inset:0;background:rgba(2,6,23,.78);z-index:9999;padding:18px;overflow:auto}
  .mp-modal.on{display:block}
  .mp-box{max-width:1000px;margin:24px auto;background:#1e293b;border:1px solid #334155;border-radius:18px;padding:20px}
  .mp-pill{display:inline-block;padding:3px 10px;border-radius:999px;font-size:11px;font-weight:800}
  .mp-pill.res{background:#7f1d1d;color:#fecaca}
  .mp-pill.rst{background:#064e3b;color:#a7f3d0}
  .mp-warnbar{background:#7f1d1d;border:1px solid #f87171;color:#fecaca;padding:11px 14px;border-radius:12px;font-weight:700;font-size:13px;margin-bottom:14px}
  @media(max-width:640px){.mp-card b{font-size:18px}.mp-tbl{font-size:12px}.mp-tbl td,.mp-tbl th{padding:7px 5px}}
  `;
  document.head.appendChild(css);

  // ---------------------------------------------------------------- markup
  const wrap = document.createElement('div');
  wrap.className = 'mp-wrap';
  wrap.innerHTML = `
    <div class="mp-head">
      <h3>💰 تسعير وأرباح ${TITLE}</h3>
      <div class="mp-tabs">
        <button class="mp-tab on" data-tab="prices">🏷️ أسعار البيع</button>
        <button class="mp-tab" data-tab="fund">📉 ${TITLE} الناقص</button>
        <button class="mp-tab" data-tab="cards">📊 كسبت كام</button>
        <button class="mp-tab" data-tab="history">📜 سجل الأسعار</button>
      </div>
    </div>
    <div id="mpWarnings"></div>
    <div id="mpBody"><div class="mp-empty">جاري التحميل…</div></div>`;

  const modal = document.createElement('div');
  modal.className = 'mp-modal';
  modal.id = 'mpModal';
  modal.innerHTML = `<div class="mp-box"><div class="mp-head"><h3 id="mpModalTitle"></h3><button class="mp-btn ghost" id="mpModalClose">إغلاق</button></div><div id="mpModalBody"></div></div>`;

  function mount() {
    const container = document.querySelector('.container') || document.body;
    const table = container.querySelector('table');
    if (table && table.parentNode === container) container.insertBefore(wrap, table);
    else container.appendChild(wrap);
    document.body.appendChild(modal);
    wrap.querySelectorAll('.mp-tab').forEach(btn => btn.addEventListener('click', () => {
      state.tab = btn.dataset.tab;
      wrap.querySelectorAll('.mp-tab').forEach(b => b.classList.toggle('on', b === btn));
      render();
    }));
    modal.addEventListener('click', e => { if (e.target === modal) closeModal(); });
    document.getElementById('mpModalClose').addEventListener('click', closeModal);
  }
  function openModal(title, html) {
    document.getElementById('mpModalTitle').textContent = title;
    document.getElementById('mpModalBody').innerHTML = html;
    modal.classList.add('on');
  }
  function closeModal() { modal.classList.remove('on'); }

  // ---------------------------------------------------------------- data
  async function loadAll() {
    const q = new URLSearchParams({ item_type: ITEM_TYPE });
    if (state.from) q.set('from', state.from);
    if (state.to) q.set('to', state.to);
    const [summary, fund, prices, history] = await Promise.all([
      api('/material-profit-summary?' + q.toString()).catch(() => null),
      api('/material-replenishment?item_type=' + ITEM_TYPE).catch(() => null),
      api('/material-prices?item_type=' + ITEM_TYPE).catch(() => null),
      api('/material-price-history?item_type=' + ITEM_TYPE).catch(() => [])
    ]);
    state.summary = summary; state.fund = fund; state.prices = prices; state.history = history || [];
  }

  // ---------------------------------------------------------------- render
  function renderWarnings() {
    const box = document.getElementById('mpWarnings');
    if (!box) return;
    const warns = (state.prices && state.prices.warnings) || [];
    const mine = warns.filter(w => w.item_type === ITEM_TYPE);
    const due = state.fund ? n(state.fund.due) : 0;
    let html = '';
    if (mine.length) {
      html += `<div class="mp-warnbar">⚠️ ${mine.length} صنف سعر بيعه أقل من أو يساوي سعر شرائه: ${mine.slice(0, 3).map(w => esc(w.label)).join(' • ')}${mine.length > 3 ? ' …' : ''} — يعني بتبيعه بخسارة.</div>`;
    }
    if (due > 0) {
      html += `<div class="mp-warnbar" style="background:#78350f;border-color:#fbbf24;color:#fde68a">📉 ${TITLE} خرج من المخزن ولسه مرجعش: <b>−${eg(due)}</b> — لازم تجيب بدله.</div>`;
    }
    box.innerHTML = html;
  }

  function dateBar() {
    return `<div class="mp-row">
      <div class="mp-f"><label>من تاريخ</label><input type="date" id="mpFrom" value="${esc(state.from)}"></div>
      <div class="mp-f"><label>إلى تاريخ</label><input type="date" id="mpTo" value="${esc(state.to)}"></div>
      <button class="mp-btn" id="mpApply">تطبيق</button>
      <button class="mp-btn ghost" id="mpClear">كل الفترات</button>
      <button class="mp-btn ghost" id="mpMonth">آخر ٣٠ يوم</button>
    </div>`;
  }
  function bindDateBar() {
    const apply = document.getElementById('mpApply');
    if (apply) apply.onclick = () => { state.from = document.getElementById('mpFrom').value; state.to = document.getElementById('mpTo').value; refresh(); };
    const clr = document.getElementById('mpClear');
    if (clr) clr.onclick = () => { state.from = ''; state.to = ''; refresh(); };
    const mth = document.getElementById('mpMonth');
    if (mth) mth.onclick = () => {
      const to = new Date(); const from = new Date(Date.now() - 29 * 864e5);
      state.to = to.toISOString().slice(0, 10); state.from = from.toISOString().slice(0, 10); refresh();
    };
  }

  function renderCards() {
    const s = state.summary;
    if (!s) return '<div class="mp-empty">تعذر تحميل بيانات الأرباح</div>';
    const c = s.consumed || {}, p = s.purchased || {}, sv = s.stock_value || {};
    const ps = s.pricing_status || {};
    const profit = n(c.profit);
    const hasPrices = n(c.has_prices) === 1;
    const stockHasPrices = n(sv.has_prices) === 1;
    // الأصناف اللي لسه ملهاش سعر بيع بتتباع بسعر شرائها (ربح صفر) — مش خسارة.
    const setupBanner = n(ps.unpriced_items) > 0 ? `
      <div class="mp-warnbar" style="background:#78350f;border-color:#fbbf24;color:#fde68a;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
        <div>🏷️ <b>${n(ps.unpriced_items)}</b> صنف من ${n(ps.total_items)} بيتباع <b>بسعر الشراء</b> (ربح صفر) لإنك لسه محددتش سعر بيع.
          ${(ps.unpriced_sample || []).length ? `<br><small style="opacity:.85">${(ps.unpriced_sample || []).map(esc).join(' • ')}${n(ps.unpriced_items) > (ps.unpriced_sample || []).length ? ' …' : ''}</small>` : ''}</div>
        <button class="mp-btn" id="mpGoPrices">حدد أسعار البيع دلوقتي</button>
      </div>` : '';
    return `<div class="mp-note" style="margin:0 0 12px;background:#0f172a;border:1px solid #334155;border-radius:12px;padding:12px">
      <b style="color:#22d3ee">إيه اللي بيتحسب هنا؟</b><br>
      انت اشتريت الـ${UNIT} بسعر وبتحمّله على العميل بسعر أعلى. الفرق ده = <b>ربح ${TITLE}</b>.<br>
      مثال: استهلكت ١٠٠ ${UNIT} شاريهم بـ ٤٠ (يعني ٤٬٠٠٠ ج) وبايعهم بـ ٥٥ (يعني ٥٬٥٠٠ ج) ← كسبت <b style="color:#4ade80">١٬٥٠٠ ج</b> من ${TITLE} لوحده.<br>
      <span style="color:#64748b">ده مش ربح الأوردر — ربح القص والتصنيع والطباعة محسوب لوحده في صفحة الحسابات.</span>
    </div>` + setupBanner + dateBar() + `
    <div class="mp-cards">
      <div class="mp-card mp-cyan"><small>استهلكت من ${TITLE}</small><b>${qty(c.qty)} ${UNIT}</b>
        <div class="sub">${IS_PAPER ? Math.round(n(c.sheets)).toLocaleString('en-US') + ' فرخ • ' : ''}${n(c.moves)} حركة</div></div>
      <div class="mp-card"><small>تكلفتها الحقيقية (شراء)</small><b>${eg(c.cost)}</b>
        <div class="sub">ده اللي دفعته فعلاً</div></div>
      <div class="mp-card mp-cyan click" id="mpSoldCard"><small>بعت ${TITLE} بـ</small>
        <b>${eg(c.sold)}</b>
        <div class="sub">محمّلة على الأوردرات بسعر البيع ← اضغط للتفاصيل</div></div>
      <div class="mp-card ${profit > 0 ? 'mp-green' : (profit < 0 ? 'mp-red' : '')}"><small>ربح ${TITLE}</small>
        <b>${eg(profit)}</b>
        <div class="sub">${profit === 0 && n(ps.unpriced_items) > 0 ? 'بتبيع بسعر الشراء — حدد سعر بيع علشان تكسب' : `هامش ${qty(c.margin_percent)}% على التكلفة`}</div></div>
      <div class="mp-card"><small>اشتريت في نفس الفترة</small><b>${qty(p.qty)} ${UNIT}</b>
        <div class="sub">${eg(p.cost)} • ${n(p.moves)} فاتورة/حركة</div></div>
      <div class="mp-card ${n(s.net_stock_gap) > 0 ? 'mp-amber' : 'mp-green'}"><small>الفرق (استهلاك − شراء)</small><b>${eg(s.net_stock_gap)}</b>
        <div class="sub">${n(s.net_stock_gap) > 0 ? 'بتاكل من المخزن — لازم تشتري' : 'المخزن بيتعوّض أول بأول ✅'}</div></div>
      <div class="mp-card"><small>قيمة المخزون بسعر الشراء</small><b>${eg(sv.at_buy)}</b>
        <div class="sub">${n(ps.total_items)} صنف في المخزن</div></div>
      <div class="mp-card ${n(sv.unrealized_margin) > 0 ? 'mp-green' : ''}"><small>قيمته لو اتباع كله</small>
        <b>${eg(sv.at_sell)}</b>
        <div class="sub">ربح مخزّن في الرف: ${eg(sv.unrealized_margin)}${n(sv.unpriced_items) > 0 ? ` • ${n(sv.unpriced_items)} صنف بسعر الشراء` : ''}</div></div>
    </div>

    <h4 style="margin:22px 0 0">التفصيل حسب النوع</h4>
    <div class="mp-scroll"><table class="mp-tbl"><thead><tr>
      <th>النوع</th><th>المستهلك</th><th>التكلفة</th><th>المبيع</th><th>الربح</th><th>الهامش</th>
    </tr></thead><tbody>${(s.by_type || []).length ? s.by_type.map(r => `<tr>
      <td><b>${esc(r.type_key || '—')}</b></td>
      <td>${qty(r.qty)} ${UNIT}${IS_PAPER ? `<br><small style="color:#64748b">${Math.round(n(r.sheets))} فرخ</small>` : ''}</td>
      <td>${eg(r.cost)}</td><td>${eg(r.sold)}</td>
      <td style="color:${n(r.profit) > 0 ? '#4ade80' : (n(r.profit) < 0 ? '#f87171' : '#94a3b8')};font-weight:800">${eg(r.profit)}</td>
      <td>${n(r.cost) > 0 ? qty((n(r.profit) / n(r.cost)) * 100) + '%' : '—'}</td>
    </tr>`).join('') : `<tr><td colspan="6" class="mp-empty">مفيش حركات في الفترة دي</td></tr>`}</tbody></table></div>

    <h4 style="margin:22px 0 0">أعلى الأصناف ربحًا</h4>
    <div class="mp-scroll"><table class="mp-tbl"><thead><tr>
      <th>الصنف</th><th>المستهلك</th><th>التكلفة</th><th>المبيع</th><th>الربح</th><th>الباقي بالمخزن</th>
    </tr></thead><tbody>${(s.by_item || []).length ? s.by_item.map(r => `<tr>
      <td style="text-align:start">${esc(r.label || '—')}</td>
      <td>${qty(r.qty)} ${UNIT}</td><td>${eg(r.cost)}</td><td>${eg(r.sold)}</td>
      <td style="color:${n(r.profit) > 0 ? '#4ade80' : (n(r.profit) < 0 ? '#f87171' : '#94a3b8')};font-weight:800">${eg(r.profit)}</td>
      <td>${qty(r.remaining)} ${UNIT}</td>
    </tr>`).join('') : `<tr><td colspan="6" class="mp-empty">مفيش حركات في الفترة دي</td></tr>`}</tbody></table></div>

    <div class="mp-note">💡 <b>الربح ده مش ربح الأوردر.</b> ده فرق سعر الخامة بس: الفرق بين اللي دفعته للمورد واللي حمّلته على العميل. ربح التشغيل (قص/تصنيع/طباعة) محسوب على حدة في صفحة الحسابات.</div>`;
  }

  // جدول "محتاج تجيب إيه": بنعرض الناقص الفعلي بس، مش الأصناف المغطّاة.
  function shortageTable(title, rows, withType) {
    const cols = withType ? (IS_PAPER ? 5 : 4) : 3;
    return `<h4 style="margin:20px 0 0">محتاج تجيب إيه — ${title}</h4>
    <div class="mp-scroll" style="max-height:300px"><table class="mp-tbl"><thead><tr>
      ${withType ? `<th>النوع</th><th>اللون</th>${IS_PAPER ? '<th>الجرام</th>' : ''}` : '<th>الصنف</th>'}
      <th>الناقص</th><th>قيمته</th>
    </tr></thead><tbody>${rows.length ? rows.map(r => `<tr>
      ${withType
        ? `<td><b>${esc(r.type_key || '—')}</b></td><td>${esc(r.color || 'كل الألوان')}</td>${IS_PAPER ? `<td>${n(r.grammage) > 0 ? qty(r.grammage) + ' جم' : '—'}</td>` : ''}`
        : `<td style="text-align:start">${esc(r.label || '—')}</td>`}
      <td style="color:#fbbf24;font-weight:800">${qty(r.net_qty)} ${UNIT}${IS_PAPER && n(r.net_sheets) > 0 ? `<br><small style="color:#64748b">${Math.round(n(r.net_sheets))} فرخ</small>` : ''}</td>
      <td style="font-weight:800">${eg(r.net_amount)}</td>
    </tr>`).join('') : `<tr><td colspan="${cols}" class="mp-empty">مفيش نقص — كل حاجة متعوّضة ✅</td></tr>`}</tbody></table></div>`;
  }

  function renderFund() {
    const f = state.fund;
    if (!f) return '<div class="mp-empty">تعذر تحميل الصندوق</div>';
    const due = n(f.due);
    const netQty = n(f.reserved_qty) - n(f.restocked_qty);
    return `
    <div class="mp-note" style="margin:0 0 14px;background:#0f172a;border:1px solid #334155;border-radius:12px;padding:12px">
      <b style="color:#22d3ee">الكارت ده بيقولك إيه؟</b><br>
      كل ${UNIT} بيخرج من المخزن (قص لأوردر أو خصم) الرقم بينزل <b style="color:#f87171">بالسالب</b>.<br>
      وكل ${UNIT} بيدخل المخزن (فاتورة شراء أو إضافة يدوية) الرقم بيرجع <b style="color:#4ade80">لفوق</b> لوحده.<br>
      لما يوصل <b>صفر</b> يبقى كل اللي خرج رجع تاني ومحتاجش تشتري.
    </div>

    <div class="mp-cards">
      <div class="mp-card ${due > 0 ? 'mp-red' : 'mp-green'}">
        <small>${due > 0 ? `⚠️ ${TITLE} خرج ولسه مرجعش` : `✅ كل ${TITLE} اللي خرج رجع`}</small>
        <b>${due > 0 ? '−' + eg(due) : eg(0)}</b>
        <div class="sub">${due > 0 ? `${qty(Math.abs(netQty))} ${UNIT} لازم تجيبهم` : 'المخزن متعوّض بالكامل'}</div></div>
      <div class="mp-card mp-red"><small>خرج من المخزن</small><b>−${qty(f.reserved_qty)} ${UNIT}</b>
        <div class="sub">بقيمة ${eg(f.reserved)}</div></div>
      <div class="mp-card mp-green"><small>دخل المخزن</small><b>+${qty(f.restocked_qty)} ${UNIT}</b>
        <div class="sub">بقيمة ${eg(f.restocked)}</div></div>
      <div class="mp-card ${n(f.surplus) > 0 ? 'mp-green' : ''}"><small>زيادة عندك</small><b>${eg(f.surplus)}</b>
        <div class="sub">اشتريت أكتر من اللي استهلكته</div></div>
    </div>

    <div class="mp-row" style="margin-top:16px">
      <button class="mp-btn ghost" id="mpFundRows">📜 الرقم ده جاي منين؟ (${n(f.rows_count)} حركة)</button>
      <button class="mp-btn warn" id="mpSettle">🧹 تصفير والبدء من جديد</button>
    </div>

    ${shortageTable('حسب النوع', (f.by_type || []).filter(r => n(r.net_amount) > 0), true)}
    ${shortageTable('صنف صنف', (f.by_item || []).filter(r => n(r.net_amount) > 0), false)}
    ${(f.by_item || []).some(r => n(r.net_amount) < 0) ? `<div class="mp-note" style="color:#4ade80">✅ فيه أصناف اشتريت منها أكتر من اللي استهلكته — مغطّاة ومش محتاجة شراء دلوقتي.</div>` : ''}

    ${(f.settlements || []).length ? `<h4 style="margin:22px 0 0">دورات سابقة</h4>
    <div class="mp-scroll"><table class="mp-tbl"><thead><tr><th>التاريخ</th><th>المبلغ وقت التصفير</th><th>عدد الحركات</th><th>بواسطة</th></tr></thead><tbody>
    ${f.settlements.map(r => `<tr><td>${dt(r.created_at)}</td><td>${eg(r.amount)}</td><td>${n(r.rows_count)}</td><td>${esc(r.created_by)}</td></tr>`).join('')}
    </tbody></table></div>` : ''}

    <div class="mp-note">💡 القيمة بتتحسب بسعر الشراء <b>الحالي</b> مش القديم — عشان لو المورد غلّي عليك، الرقم يقولك تحتاج تدفع كام <b>النهاردة</b> علشان ترجّع نفس الكمية.<br>
    🧹 <b>زرار التصفير:</b> بيبدأ العدّ من الصفر من دلوقتي من غير ما يمسح أي حركة قديمة. استخدمه أول مرة بعد ما تدخل أرصدة المخزن، أو أول كل شهر لو حبيت تحسب الشهر لوحده.</div>`;
  }

  function renderPrices() {
    const p = state.prices;
    if (!p) return '<div class="mp-empty">تعذر تحميل الأسعار</div>';
    const rules = (p.rules || []).filter(r => (r.item_type === ITEM_TYPE));
    const items = (p.items || []).filter(r => r.item_type === ITEM_TYPE);
    const srcLabel = { custom: 'سعر خاص', type_fixed: 'سعر النوع', type_margin: 'هامش النوع', item_margin: 'هامش الصنف', legacy: 'سعر قديم', none: 'لسه متحددش' };
    // الألوان والجرامات بتتبني من نفس بيانات الأصناف الموجودة فعلاً + الاختيارات الثابتة.
    const dataColors = [...new Set(items.map(i => String(i.color || '').trim()).filter(Boolean))];
    const colorList = [...new Set([...BASE_COLORS, ...dataColors])];
    const gramList = [...new Set(items.map(i => n(i.grammage)).filter(g => g > 0))].sort((a, b) => a - b);
    return `
    <div class="mp-note" style="margin:0 0 12px;background:#0f172a;border:1px solid #334155;border-radius:12px;padding:12px">
      <b style="color:#22d3ee">إيه اللي بيحصل هنا؟</b><br>
      انت بتشتري الـ${UNIT} بسعر، وعايز تبيعه بسعر أعلى. الجزء الأول تحت بيخليك تحدد السعر <b>لنوع كامل مرة واحدة</b>
      (مثلاً: البرستول كله بـ ٧٠) وكل أصناف النوع ده هتاخد السعر تلقائي.
      والجزء التاني بيخليك تغيّر صنف واحد بعينه لو عايزه بسعر مختلف.
    </div>

    <h4 style="margin:0 0 4px">١) سعر النوع</h4>
    <div class="mp-row">
      <div class="mp-f"><label>النوع</label><select id="mpRuleType">${TYPE_OPTIONS.map(t => `<option>${esc(t)}</option>`).join('')}</select></div>
      <div class="mp-f"><label>اللون</label><select id="mpRuleColor"><option value="">كل الألوان</option>${colorList.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('')}</select></div>
      ${IS_PAPER ? `<div class="mp-f"><label>الجرام</label><select id="mpRuleGram"><option value="">كل الجرامات</option>${gramList.map(g => `<option value="${g}">${qty(g)} جم</option>`).join('')}</select></div>` : ''}
      <div class="mp-f"><label>طريقة التسعير</label><select id="mpRuleBasis"><option value="fixed">سعر ثابت</option><option value="margin">هامش % فوق الشراء</option></select></div>
      <div class="mp-f" id="mpPriceField"><label>سعر بيع الـ${UNIT}</label><input id="mpRulePrice" type="number" step="0.01" placeholder="0"></div>
      <div class="mp-f" id="mpMarginField" style="display:none"><label>الهامش %</label><input id="mpRuleMargin" type="number" step="0.5" placeholder="0"></div>
      <button class="mp-btn" id="mpSaveRule">حفظ سعر النوع</button>
    </div>
    <div class="mp-note" id="mpRuleHint" style="margin:-4px 0 12px"></div>
    <div class="mp-scroll" style="max-height:250px"><table class="mp-tbl"><thead><tr>
      <th>النوع</th><th>اللون</th>${IS_PAPER ? '<th>الجرام</th>' : ''}<th>الطريقة</th><th>السعر</th><th>الهامش</th><th>آخر تعديل</th>
    </tr></thead><tbody>${rules.length ? rules.map(r => `<tr>
      <td><b>${esc(r.type_key)}</b></td><td>${esc(r.color || 'الكل')}</td>
      ${IS_PAPER ? `<td>${n(r.grammage) > 0 ? qty(r.grammage) : 'الكل'}</td>` : ''}
      <td>${r.price_basis === 'margin' ? 'هامش %' : 'ثابت'}</td>
      <td style="font-weight:800;color:${n(r.sell_price) > 0 ? '#4ade80' : '#64748b'}">${n(r.sell_price) > 0 ? eg(r.sell_price) : '— لسه'}</td>
      <td>${n(r.margin_percent) > 0 ? qty(r.margin_percent) + '%' : '—'}</td>
      <td><small>${dt(r.updated_at)}<br>${esc(r.updated_by)}</small></td>
    </tr>`).join('') : `<tr><td colspan="${IS_PAPER ? 7 : 6}" class="mp-empty">مفيش قواعد أسعار</td></tr>`}</tbody></table></div>

    <h4 style="margin:24px 0 4px">٢) السعر الفعلي لكل صنف</h4>
    <div class="mp-note" style="margin:0 0 10px">لو عايز صنف معيّن بسعر مختلف عن نوعه، اكتب السعر جنبه واضغط حفظ. "رجوع للنوع" بيلغي التخصيص.</div>
    <div class="mp-scroll"><table class="mp-tbl"><thead><tr>
      <th>الصنف</th><th>شراء</th><th>بيع</th><th>الربح/${UNIT}</th><th>مصدر السعر</th><th>سعر خاص</th><th></th>
    </tr></thead><tbody>${items.length ? items.map(r => {
      const bad = n(r.resolved_sell_price) > 0 && n(r.resolved_sell_price) <= n(r.buy_price);
      return `<tr>
      <td style="text-align:start">${esc(r.label)}</td>
      <td>${eg(r.buy_price)}</td>
      <td style="font-weight:800;color:${n(r.resolved_sell_price) > 0 ? (bad ? '#f87171' : '#4ade80') : '#64748b'}">${n(r.resolved_sell_price) > 0 ? eg(r.resolved_sell_price) : '— لسه'}</td>
      <td style="color:${n(r.unit_margin) > 0 ? '#4ade80' : (n(r.unit_margin) < 0 ? '#f87171' : '#94a3b8')};font-weight:800">${eg(r.unit_margin)}</td>
      <td><small>${esc(srcLabel[r.price_source] || r.price_source)}</small></td>
      <td><input type="number" step="0.01" style="width:96px" id="mpItemPrice_${r.id}" value="${r.price_mode === 'custom' ? n(r.stored_sell_price) : ''}" placeholder="سعر النوع"></td>
      <td><button class="mp-btn mini" data-save-item="${r.id}">حفظ</button>
        ${r.price_mode === 'custom' ? `<button class="mp-btn mini ghost" data-reset-item="${r.id}" style="margin-inline-start:5px">رجوع للنوع</button>` : ''}</td>
    </tr>`;
    }).join('') : `<tr><td colspan="7" class="mp-empty">مفيش أصناف</td></tr>`}</tbody></table></div>`;
  }

  function renderHistory() {
    const rows = state.history || [];
    const fieldLabel = { sell_price: 'سعر البيع', sell_price_kg: 'سعر بيع الكيلو', buy_price: 'سعر الشراء', buy_price_kg: 'سعر شراء الكيلو', margin_percent: 'الهامش %' };
    return `<div class="mp-note" style="margin:0 0 12px">كل تغيير في سعر شراء أو بيع بيتسجل هنا — تعرف امتى رفعت السعر وإيه اللي حصل بعده.</div>
    <div class="mp-scroll"><table class="mp-tbl"><thead><tr>
      <th>التاريخ</th><th>المستوى</th><th>الصنف / النوع</th><th>البند</th><th>من</th><th>إلى</th><th>التغيير</th><th>بواسطة</th>
    </tr></thead><tbody>${rows.length ? rows.map(r => `<tr>
      <td><small>${dt(r.changed_at)}</small></td>
      <td><span class="mp-pill ${r.scope === 'type' ? 'rst' : 'res'}">${r.scope === 'type' ? 'نوع' : 'صنف'}</span></td>
      <td style="text-align:start">${esc(r.scope_label || '—')}</td>
      <td><small>${esc(fieldLabel[r.field] || r.field)}</small></td>
      <td>${eg(r.old_value)}</td><td style="font-weight:800">${eg(r.new_value)}</td>
      <td style="color:${n(r.change_percent) >= 0 ? '#4ade80' : '#f87171'};font-weight:800">${n(r.change_percent) > 0 ? '+' : ''}${qty(r.change_percent)}%</td>
      <td><small>${esc(r.changed_by)}</small></td>
    </tr>`).join('') : `<tr><td colspan="8" class="mp-empty">مفيش تغييرات أسعار لسه</td></tr>`}</tbody></table></div>`;
  }

  function render() {
    renderWarnings();
    const body = document.getElementById('mpBody');
    if (!body) return;
    if (state.tab === 'cards') body.innerHTML = renderCards();
    else if (state.tab === 'fund') body.innerHTML = renderFund();
    else if (state.tab === 'prices') body.innerHTML = renderPrices();
    else body.innerHTML = renderHistory();
    bindActions();
  }

  function bindActions() {
    bindDateBar();

    // زرار "حدد أسعار البيع دلوقتي" بينقل على تبويب الأسعار على طول
    const goPrices = document.getElementById('mpGoPrices');
    if (goPrices) goPrices.onclick = () => {
      state.tab = 'prices';
      wrap.querySelectorAll('.mp-tab').forEach(b => b.classList.toggle('on', b.dataset.tab === 'prices'));
      render();
      wrap.scrollIntoView({ behavior: 'smooth', block: 'start' });
    };

    const soldCard = document.getElementById('mpSoldCard');
    if (soldCard) soldCard.onclick = () => {
      const s = state.summary || {};
      openModal(`تفاصيل مبيعات ${TITLE}`, `<div class="mp-scroll"><table class="mp-tbl"><thead><tr>
        <th>الصنف</th><th>المستهلك</th><th>تكلفة</th><th>مبيع</th><th>ربح</th></tr></thead><tbody>
        ${(s.by_item || []).map(r => `<tr><td style="text-align:start">${esc(r.label)}</td><td>${qty(r.qty)} ${UNIT}</td>
        <td>${eg(r.cost)}</td><td>${eg(r.sold)}</td>
        <td style="color:${n(r.profit) > 0 ? '#4ade80' : (n(r.profit) < 0 ? '#f87171' : '#94a3b8')};font-weight:800">${eg(r.profit)}</td></tr>`).join('') || `<tr><td colspan="5" class="mp-empty">مفيش بيانات</td></tr>`}
        </tbody></table></div>`);
    };

    const settle = document.getElementById('mpSettle');
    if (settle) settle.onclick = async () => {
      const due = state.fund ? n(state.fund.due) : 0;
      if (!confirm(`تصفير عدّاد ${TITLE} الناقص؟\n\nالناقص دلوقتي: ${eg(due)}\nمفيش أي حركة هتتمسح — العدّاد بس هيبدأ من صفر من النهاردة.`)) return;
      try {
        const r = await api('/material-replenishment/settle', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ item_type: ITEM_TYPE }) });
        toast(`تم التصفير — ${n(r.settled_rows)} حركة بقيمة ${eg(r.amount)}`);
        await refresh();
      } catch (e) { toast('خطأ: ' + e.message, false); }
    };

    const fundRows = document.getElementById('mpFundRows');
    if (fundRows) fundRows.onclick = () => {
      const rows = (state.fund && state.fund.rows) || [];
      const typeLabel = { sub: 'خصم/استهلاك', add: 'إضافة', purchase: 'مشتريات', 'purchase-reverse': 'عكس مشتريات' };
      openModal(`الرقم جاي منين — حركات ${TITLE}`, `<div class="mp-note" style="margin:0 0 12px">الرقم اللي في الكارت مجموع الحركات دي بالظبط. <b style="color:#fbbf24">+ حجز</b> = ورق خرج، <b style="color:#4ade80">− توريد</b> = ورق دخل.</div>
      <div class="mp-scroll"><table class="mp-tbl"><thead><tr>
        <th>التاريخ</th><th>الاتجاه</th><th>نوع الحركة</th><th>الصنف</th><th>الكمية</th><th>سعر الوحدة</th><th>المبلغ</th><th>الأوردر</th></tr></thead><tbody>
        ${rows.length ? rows.map(r => `<tr>
          <td><small>${dt(r.created_at)}</small></td>
          <td><span class="mp-pill ${r.direction === 'reserve' ? 'res' : 'rst'}">${r.direction === 'reserve' ? '+ حجز' : '− توريد'}</span></td>
          <td><small>${esc(typeLabel[r.movement_type] || r.movement_type)}</small></td>
          <td style="text-align:start"><small>${esc(r.item_label)}</small></td>
          <td>${qty(r.qty)} ${esc(r.unit)}</td>
          <td>${eg(r.unit_cost)}</td>
          <td style="font-weight:800;color:${r.direction === 'reserve' ? '#fbbf24' : '#4ade80'}">${r.direction === 'reserve' ? '+' : '−'}${eg(r.amount)}</td>
          <td>${n(r.order_id) ? '#' + n(r.order_id) : '—'}</td></tr>`).join('') : `<tr><td colspan="8" class="mp-empty">مفيش حركات</td></tr>`}
        </tbody></table></div>`);
    };

    // تبديل الحقول حسب طريقة التسعير + قفل اللون على أبيض للأنواع اللي لونها ثابت.
    const basisSel = document.getElementById('mpRuleBasis');
    const typeSel = document.getElementById('mpRuleType');
    const colorSel = document.getElementById('mpRuleColor');
    function syncRuleForm() {
      if (!basisSel) return;
      const margin = basisSel.value === 'margin';
      const pf = document.getElementById('mpPriceField'), mf = document.getElementById('mpMarginField');
      if (pf) pf.style.display = margin ? 'none' : '';
      if (mf) mf.style.display = margin ? '' : 'none';
      if (colorSel && typeSel) {
        const locked = isWhiteOnly(typeSel.value);
        if (locked) { colorSel.value = 'أبيض'; colorSel.disabled = true; }
        else { colorSel.disabled = false; }
      }
      const hint = document.getElementById('mpRuleHint');
      if (hint && typeSel) {
        const c = colorSel && colorSel.value ? colorSel.value : 'كل الألوان';
        const g = IS_PAPER && document.getElementById('mpRuleGram') && document.getElementById('mpRuleGram').value
          ? `${document.getElementById('mpRuleGram').value} جم` : (IS_PAPER ? 'كل الجرامات' : '');
        const count = ((state.prices && state.prices.items) || []).filter(i => i.item_type === ITEM_TYPE
          && i.type_key === typeSel.value
          && (!colorSel.value || String(i.color || '').trim() === colorSel.value)
          && (!IS_PAPER || !document.getElementById('mpRuleGram').value || Math.round(n(i.grammage)) === Math.round(n(document.getElementById('mpRuleGram').value)))).length;
        hint.innerHTML = `هيتطبق على <b style="color:#22d3ee">${count}</b> صنف: ${esc(typeSel.value)} • ${esc(c)}${g ? ' • ' + esc(g) : ''}`;
      }
    }
    if (basisSel) basisSel.onchange = syncRuleForm;
    if (typeSel) typeSel.onchange = syncRuleForm;
    if (colorSel) colorSel.onchange = syncRuleForm;
    const gramSel = document.getElementById('mpRuleGram');
    if (gramSel) gramSel.onchange = syncRuleForm;
    syncRuleForm();

    const saveRule = document.getElementById('mpSaveRule');
    if (saveRule) saveRule.onclick = async () => {
      const basis = basisSel.value;
      const payload = {
        item_type: ITEM_TYPE,
        type_key: typeSel.value,
        color: String(colorSel.value || '').trim(),
        grammage: IS_PAPER ? n(gramSel && gramSel.value) : 0,
        price_basis: basis,
        sell_price: n(document.getElementById('mpRulePrice').value),
        margin_percent: n(document.getElementById('mpRuleMargin').value)
      };
      if (basis === 'fixed' && payload.sell_price <= 0) return toast('اكتب سعر البيع الأول', false);
      if (basis === 'margin' && payload.margin_percent <= 0) return toast('اكتب نسبة الهامش الأول', false);
      try {
        const r = await api('/material-price-rule', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const count = n(r.updated_items);
        toast(count > 0 ? `تم الحفظ — اتحدّث ${count} صنف` : 'تم الحفظ — الأصناف كانت بنفس السعر بالفعل');
        await refresh();
      } catch (e) { toast('خطأ: ' + e.message, false); }
    };

    document.querySelectorAll('[data-save-item]').forEach(btn => btn.onclick = async () => {
      const id = n(btn.dataset.saveItem);
      const price = n(document.getElementById('mpItemPrice_' + id).value);
      if (price <= 0) return toast('اكتب سعر أكبر من صفر، أو استخدم "رجوع للنوع"', false);
      try {
        await api('/material-item-price', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ item_type: ITEM_TYPE, item_id: id, price_mode: 'custom', sell_price: price }) });
        toast('تم حفظ السعر الخاص بالصنف');
        await refresh();
      } catch (e) { toast('خطأ: ' + e.message, false); }
    });

    document.querySelectorAll('[data-reset-item]').forEach(btn => btn.onclick = async () => {
      const id = n(btn.dataset.resetItem);
      try {
        await api('/material-item-price', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ item_type: ITEM_TYPE, item_id: id, price_mode: 'type' }) });
        toast('رجع لسعر النوع');
        await refresh();
      } catch (e) { toast('خطأ: ' + e.message, false); }
    });
  }

  async function refresh() {
    try {
      await loadAll();
      render();
      // نحدّث جدول الصفحة الأصلي كمان لو أسعار البيع اتغيرت
      if (IS_PAPER && typeof window.load === 'function') window.load();
      else if (!IS_PAPER && typeof window.loadHandles === 'function') window.loadHandles();
    } catch (e) {
      const body = document.getElementById('mpBody');
      if (body) body.innerHTML = `<div class="mp-empty">تعذر التحميل: ${esc(e.message)}</div>`;
    }
  }

  function boot() { mount(); refresh(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot, { once: true });
  else boot();
})();
