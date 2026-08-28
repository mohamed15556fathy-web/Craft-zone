/* ==========================================================================
   Craft Zone — طبقة الموبايل (Mobile Shell)  v1.0
   --------------------------------------------------------------------------
   الملف ده بيضيف على الموبايل بس:
     1) شريط علوي ثابت فيه عنوان الصفحة + زرار القائمة + زرار الرجوع
     2) شريط سفلي (Bottom Tabs) زي تطبيقات الموبايل، بيحترم صلاحيات المستخدم
     3) قائمة جانبية فيها كل صفحات السيستم + تسجيل الخروج
     4) تحويل الجداول لكروت علشان تتقرا على الشاشة الصغيرة
   على الكمبيوتر: الملف ده مش بيعمل أي حاجة خالص (بيقف عند أول شرط).
   الكود مكتوب ES5 علشان يشتغل حتى على WebView قديم داخل تطبيق الموبايل.
   ========================================================================== */
(function () {
  'use strict';

  if (window.__CZ_MOBILE_READY__) return;
  window.__CZ_MOBILE_READY__ = true;

  var MQ = window.matchMedia ? window.matchMedia('(max-width: 768px)') : null;

  /* ---------- تحديد الصفحة الحالية ---------- */
  var file = (location.pathname || '').toLowerCase().split('/').pop() || '';
  var isLogin = (file === '' || file === 'login.html');

  if (isLogin) {
    document.documentElement.classList.add('cz-noshell');
    return; // صفحة الدخول: تنسيق متجاوب بس من غير شل
  }

  /* ---------- بيانات المستخدم والصلاحيات (نفس منطق السيستم) ---------- */
  function currentUser() {
    try { return JSON.parse(localStorage.getItem('user') || 'null'); } catch (e) { return null; }
  }
  function hasPerm(u, key) {
    if (!u) return false;
    if (!key) return true;
    return u.username === 'admin' || u.role === 'super_admin' || Number(u[key] || 0) === 1;
  }
  function hasAnyPerm(u, list) {
    var arr = String(list || '').split(','), i, k;
    for (i = 0; i < arr.length; i++) {
      k = arr[i].replace(/^\s+|\s+$/g, '');
      if (k && hasPerm(u, k)) return true;
    }
    return false;
  }
  function roleLabel(role) {
    var map = {
      super_admin: 'مدير عام', admin: 'أدمن', moderator: 'مودريتور',
      operation: 'أوبريشن', production: 'إنتاج', store: 'مخزن', accountant: 'محاسب'
    };
    return map[role] || role || '';
  }

  /* ---------- كل صفحات السيستم ---------- */
  var PAGES = [
    { href: 'index.html',            ic: '🏠', lb: 'الرئيسية',      tab: 'الرئيسية' },
    { href: 'add_order.html',        ic: '➕', lb: 'أوردر جديد',    tab: 'جديد',     perm: 'perm_add_order' },
    { href: 'orders_list.html',      ic: '📋', lb: 'متابعة الأوردرات', tab: 'الأوردرات', perm: 'perm_view_orders' },
    { href: 'inventory.html',        ic: '📄', lb: 'مخزن الورق',    tab: 'الورق',    perm: 'perm_view_inventory' },
    { href: 'bags.html',             ic: '👜', lb: 'مخزن الشنط',    tab: 'الشنط',    perm: 'perm_view_bags' },
    { href: 'handles.html',          ic: '🧵', lb: 'مخزن اليد',     tab: 'اليد',     perm: 'perm_view_handles' },
    { href: 'customers.html',        ic: '👤', lb: 'العملاء',       tab: 'العملاء',  perm: 'perm_customers' },
    { href: 'accounts.html',         ic: '💰', lb: 'الحسابات',      tab: 'الحسابات', perm: 'perm_view_accounts' },
    { href: 'calculator.html',       ic: '🧮', lb: 'احسب أوردر',    tab: 'الحاسبة',  perm: 'perm_calculator' },
    { href: 'cutting_optimizer.html',ic: '🧠', lb: 'مخطط القص الذكي', tab: 'القص',   perm: 'perm_cutting_optimizer' },
    { href: 'forms.html',            ic: '🧰', lb: 'الفورم وملفات PDF', tab: 'الفورم', perm: 'perm_view_fixed_assets' },
    { href: 'partners.html',         ic: '🤝', lb: 'الشركاء',       tab: 'الشركاء',  perm: 'perm_view_partners' },
    { href: 'traders.html',          ic: '🏪', lb: 'التجار والطلبيات', tab: 'التجار', perm: 'perm_view_traders' },
    { href: 'debts.html',            ic: '📒', lb: 'المديونيات',    tab: 'المديونيات', perm: 'perm_view_debts' },
    { href: 'purchases.html',        ic: '🛒', lb: 'المشتريات والموردين', tab: 'المشتريات', permAny: 'perm_suppliers,perm_purchases' },
    { href: 'reports.html',          ic: '📊', lb: 'التقارير',      tab: 'التقارير', perm: 'perm_view_reports' },
    { href: 'artisans.html',         ic: '🧑‍🏭', lb: 'الصنايعية والتنفيذ الخارجي', tab: 'الصنايعية', perm: 'perm_view_artisans' },
    { href: 'workshop.html',         ic: '🛠️', lb: 'الورشة (يومية وسلف)', tab: 'الورشة',   permAny: 'perm_view_workshop,perm_manage_workshop' },
    { href: 'users.html',            ic: '👥', lb: 'المستخدمين',    tab: 'المستخدمين', perm: 'perm_users' },
    { href: 'activity_logs.html',    ic: '🛡️', lb: 'سجل الأدمن',    tab: 'السجل',    perm: 'perm_activity_logs' },
    { href: 'settings.html',         ic: '⚙️', lb: 'إعدادات السيستم', tab: 'الإعدادات', perm: 'perm_system_config' }
  ];

  /* صفحات فرعية بتتبع صفحة رئيسية علشان الشريط السفلي يفضّلها مظللة */
  var CHILD_OF = {
    'bags_history.html': 'bags.html',
    'history.html': 'inventory.html',
    'suppliers.html': 'purchases.html'
  };

  /* الترتيب المفضّل للشريط السفلي */
  var TAB_ORDER = ['index.html', 'orders_list.html', 'add_order.html', 'inventory.html', 'accounts.html'];

  function allowedPages(u) {
    var out = [], i, p;
    for (i = 0; i < PAGES.length; i++) {
      p = PAGES[i];
      if (p.permAny ? hasAnyPerm(u, p.permAny) : hasPerm(u, p.perm)) out.push(p);
    }
    return out;
  }
  function findPage(list, href) {
    for (var i = 0; i < list.length; i++) if (list[i].href === href) return list[i];
    return null;
  }
  function activeHref() {
    return CHILD_OF[file] || file || 'index.html';
  }

  /* ---------- أدوات مساعدة ---------- */
  function el(tag, cls, html) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (html != null) n.innerHTML = html;
    return n;
  }
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  /* ================= بناء الشل ================= */
  var drawerNode = null;

  function buildShell() {
    var u = currentUser();
    if (!u) return;                       // من غير تسجيل دخول ما نبنيش حاجة
    if (document.querySelector('.cz-appbar')) return;

    var allowed = allowedPages(u);
    var active = activeHref();

    /* ---- الشريط العلوي ---- */
    var bar = el('div', 'cz-appbar');

    var menuBtn = el('button', 'cz-ab-btn cz-menu-btn', '☰');
    menuBtn.type = 'button';
    menuBtn.setAttribute('aria-label', 'القائمة');
    menuBtn.onclick = function () { toggleDrawer(); };

    var title = (document.title || 'Craft Zone').replace(/\s*[|｜]\s*Craft\s*Zone\s*$/i, '');
    var who = (u.full_name || u.username || '') + (u.role ? ' • ' + roleLabel(u.role) : '');
    var titleBox = el('div', 'cz-ab-title',
      '<span class="cz-ab-t">' + esc(title) + '</span>' +
      '<span class="cz-ab-s">' + esc(who) + '</span>');
    titleBox.onclick = function () {
      try { window.scrollTo({ top: 0, behavior: 'smooth' }); } catch (e) { window.scrollTo(0, 0); }
    };

    bar.appendChild(menuBtn);
    bar.appendChild(titleBox);

    if (active !== 'index.html') {
      var backBtn = el('button', 'cz-ab-btn cz-back-btn', '↩');
      backBtn.type = 'button';
      backBtn.setAttribute('aria-label', 'رجوع');
      backBtn.onclick = function () {
        if (history.length > 1) history.back(); else location.href = 'index.html';
      };
      bar.appendChild(backBtn);
    } else {
      bar.appendChild(el('div', 'cz-ab-spacer'));
    }

    /* ---- الشريط السفلي ---- */
    var tabs = [], i, p;
    for (i = 0; i < TAB_ORDER.length; i++) {
      p = findPage(allowed, TAB_ORDER[i]);
      if (p) tabs.push(p);
    }
    for (i = 0; i < allowed.length && tabs.length < 5; i++) {
      if (!findPage(tabs, allowed[i].href)) tabs.push(allowed[i]);
    }
    /* زرار "أوردر جديد" يتحط في النص كزرار بارز */
    var fab = findPage(tabs, 'add_order.html');
    if (fab && tabs.length >= 3) {
      tabs.splice(tabs.indexOf(fab), 1);
      tabs.splice(Math.floor(tabs.length / 2), 0, fab);
    }

    var nav = el('nav', 'cz-tabbar');
    for (i = 0; i < tabs.length; i++) {
      p = tabs[i];
      var isFab = (p.href === 'add_order.html' && tabs.length >= 3);
      var a = el('a', 'cz-tab' + (isFab ? ' cz-fab' : '') + (p.href === active ? ' is-active' : ''),
        '<span class="cz-ic">' + p.ic + '</span><span class="cz-lb">' + esc(p.tab || p.lb) + '</span>');
      a.href = p.href;
      nav.appendChild(a);
    }

    /* ---- القائمة الجانبية ---- */
    var links = '';
    for (i = 0; i < allowed.length; i++) {
      p = allowed[i];
      links += '<a class="cz-dr-link' + (p.href === active ? ' is-active' : '') + '" href="' + p.href + '">' +
        '<span class="cz-ic">' + p.ic + '</span><span>' + esc(p.lb) + '</span></a>';
    }

    var initial = ((u.full_name || u.username || 'C') + '').charAt(0).toUpperCase();
    var dr = el('div', 'cz-drawer');
    dr.innerHTML =
      '<div class="cz-drawer-bd"></div>' +
      '<aside class="cz-drawer-panel">' +
        '<div class="cz-dr-head">' +
          '<div class="cz-dr-avatar">' + esc(initial) + '</div>' +
          '<div class="cz-dr-who">' +
            '<div class="cz-dr-name">' + esc(u.full_name || u.username || '') + '</div>' +
            '<div class="cz-dr-role">' + esc(roleLabel(u.role)) + '</div>' +
          '</div>' +
          '<button type="button" class="cz-dr-close" aria-label="إغلاق">✕</button>' +
        '</div>' +
        '<nav class="cz-dr-links">' + links + '</nav>' +
        '<div class="cz-dr-foot">' +
          '<button type="button" class="btn cz-dr-logout">🚪 تسجيل الخروج</button>' +
          '<div class="cz-dr-ver">Craft Zone — نسخة الموبايل</div>' +
        '</div>' +
      '</aside>';

    document.body.appendChild(bar);
    document.body.appendChild(nav);
    document.body.appendChild(dr);
    drawerNode = dr;

    dr.querySelector('.cz-drawer-bd').onclick = closeDrawer;
    dr.querySelector('.cz-dr-close').onclick = closeDrawer;
    dr.querySelector('.cz-dr-logout').onclick = function () {
      if (typeof window.logout === 'function') { try { window.logout(); return; } catch (e) {} }
      var t = localStorage.getItem('token');
      try {
        fetch('/logout', { method: 'POST', headers: { Authorization: 'Bearer ' + t } })
          .catch(function () {});
      } catch (e) {}
      setTimeout(function () { localStorage.clear(); location.href = 'login.html'; }, 200);
    };
  }

  function openDrawer() {
    if (!drawerNode) return;
    drawerNode.classList.add('is-open');
  }
  function closeDrawer() {
    if (!drawerNode) return;
    drawerNode.classList.remove('is-open');
  }
  function toggleDrawer() {
    if (!drawerNode) return;
    if (drawerNode.classList.contains('is-open')) closeDrawer(); else openDrawer();
  }
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') closeDrawer();
  });

  /* ================= تحويل الجداول لكروت ================= */

  function headerLabels(tb) {
    var head = tb.tHead, row, i, c, span, txt, out = [];
    if (!head || !head.rows.length) return null;
    row = head.rows[head.rows.length - 1];
    for (i = 0; i < row.cells.length; i++) {
      c = row.cells[i];
      span = c.colSpan || 1;
      txt = (c.textContent || '').replace(/\s+/g, ' ').replace(/^\s+|\s+$/g, '');
      /* عمود التحديد بيبقى فيه تشيك بوكس من غير عنوان */
      if (!txt && c.querySelector && c.querySelector('input[type="checkbox"]')) txt = 'تحديد';
      while (span--) out.push(txt);
    }
    return out.length ? out : null;
  }

  function cellIsEmpty(td) {
    if (td.querySelector('input,select,textarea,button,a,img,svg,canvas,table')) return false;
    return !(td.textContent || '').replace(/\s+/g, '').length;
  }
  function cellHasField(td) {
    return !!td.querySelector('input,select,textarea');
  }
  /* خلية أزرار = فيها زراير ومفيش نص تاني جنبها (من غير cloneNode علشان السرعة) */
  function cellIsActions(td) {
    var btns = td.querySelectorAll('button, .btn'), i, inside = 0, total;
    if (!btns.length) return false;
    total = (td.textContent || '').replace(/\s+/g, '').length;
    for (i = 0; i < btns.length; i++) inside += (btns[i].textContent || '').replace(/\s+/g, '').length;
    return total <= inside;
  }

  /* خلية فيها تشيك بوكس بس (زي عمود التحديد للطباعة) */
  function cellIsCheckOnly(td) {
    var f = td.querySelectorAll('input,select,textarea');
    if (f.length !== 1 || f[0].type !== 'checkbox') return false;
    return !(td.textContent || '').replace(/\s+/g, '').length;
  }

  /* أعمدة تصلح تبقى عنوان الكارت */
  var TITLE_LABELS = ['العميل', 'الاسم', 'اسم', 'الصنف', 'البند', 'المورد', 'التاجر',
    'الشريك', 'الموظف', 'المستخدم', 'الخامة', 'المنتج', 'الفورمة'];
  function isTitleLabel(label) {
    var i;
    if (!label) return false;
    for (i = 0; i < TITLE_LABELS.length; i++) if (label.indexOf(TITLE_LABELS[i]) > -1) return true;
    return false;
  }

  /* أهم الأعمدة اللي تفضل ظاهرة في الكارت المضغوط */
  var KEY_LABELS = ['م', 'رقم', 'كود', 'الحالة', 'الكمية', 'البيع', 'الإجمالي', 'التاريخ', 'الدفع',
    'المتبقي', 'المدفوع', 'المبلغ', 'الرصيد', 'العميل', 'الاسم', 'التليفون', 'الصنف', 'اللون'];
  var MAX_SUMMARY = 6;      /* أقصى عدد خانات تفضل ظاهرة */
  var COLLAPSE_AFTER = 6;   /* لو الأعمدة العادية أكتر من كده الكارت بيتضغط */

  function isKeyLabel(label) {
    var i, k;
    if (!label) return false;
    for (i = 0; i < KEY_LABELS.length; i++) {
      k = KEY_LABELS[i];
      if (label === k) return true;
      if (k.length > 3 && label.indexOf(k) > -1) return true;
    }
    return false;
  }

  function applyRow(tr, labels) {
    var cells = tr.cells, n = cells.length, i, j, td, span, total = 0, idx = 0;
    var roles = [], parts, label, normals = 0, cls;

    for (i = 0; i < n; i++) total += (cells[i].colSpan || 1);

    for (i = 0; i < n; i++) {
      td = cells[i];
      span = td.colSpan || 1;
      label = '';
      if (total === labels.length) {
        parts = [];
        for (j = 0; j < span; j++) if (labels[idx + j]) parts.push(labels[idx + j]);
        label = parts.join(' / ');
      }
      idx += span;
      td.setAttribute('data-label', label);

      if (span > 1 || !label) roles.push('full');
      else if (cellIsEmpty(td)) roles.push('empty');
      else if (cellIsCheckOnly(td)) roles.push('check');
      else if (cellHasField(td)) { roles.push('input'); if (td.querySelector('textarea')) td.setAttribute('data-cz-wide', '1'); }
      else if (cellIsActions(td)) roles.push('actions');
      else { roles.push('normal'); normals++; }
    }

    /* عنوان الكارت: نفضّل عمود الاسم/العميل، وإلا أول عمود عادي */
    var titleAt = -1;
    for (i = 0; i < n; i++) {
      if (roles[i] !== 'normal') continue;
      if (isTitleLabel(cells[i].getAttribute('data-label'))) { titleAt = i; break; }
    }
    if (titleAt < 0) {
      for (i = 0; i < n; i++) if (roles[i] === 'normal') { titleAt = i; break; }
    }
    if (titleAt > -1) { roles[titleAt] = 'title'; normals--; }

    /* جدول عريض؟ نخلي الكارت مضغوط: ملخص ظاهر + باقي التفاصيل بتتفتح باللمس */
    var collapse = normals > COLLAPSE_AFTER;
    var more = [], kept = 0;
    if (collapse) {
      /* الأول: الأعمدة المهمة */
      for (i = 0; i < n; i++) {
        if (roles[i] === 'check') { more.push(true); continue; }
        if (roles[i] !== 'normal') { more.push(false); continue; }
        if (kept < MAX_SUMMARY && isKeyLabel(cells[i].getAttribute('data-label'))) { more.push(false); kept++; }
        else more.push(true);
      }
      /* لو الأعمدة المهمة قليلة، نكمّل بالترتيب */
      if (kept < 3) {
        for (i = 0; i < n && kept < MAX_SUMMARY; i++) {
          if (roles[i] === 'normal' && more[i]) { more[i] = false; kept++; }
        }
      }
    }

    for (i = 0; i < n; i++) {
      td = cells[i];
      cls = (td.className || '')
        .replace(/\bcz-(title|actions|input|full|empty|long|open|more|check|wide)\b/g, '')
        .replace(/\s+/g, ' ').replace(/^\s+|\s+$/g, '');
      if (roles[i] !== 'normal') cls = (cls ? cls + ' ' : '') + 'cz-' + roles[i];
      if (td.getAttribute('data-cz-wide') === '1') cls += ' cz-wide';
      if (collapse && more[i]) cls = (cls ? cls + ' ' : '') + 'cz-more';
      td.className = cls;
      if (roles[i] === 'normal' || roles[i] === 'title') markLongCell(td);
    }

    if (collapse) tr.classList.add('cz-card');
    else { tr.classList.remove('cz-card'); tr.classList.remove('cz-open'); }
  }

  /* نص طويل جداً جوه خلية: نقصّه ونخليه يفتح باللمس */
  function markLongCell(td) {
    var txt = (td.textContent || '');
    if (txt.length < 230) return;
    if (td.getAttribute('onclick') || td.className.indexOf('clickable') > -1) return;
    if (td.querySelector('[onclick],button,input,select,textarea,a')) return;
    td.className = (td.className ? td.className + ' ' : '') + 'cz-long';
    if (td.getAttribute('data-cz-long') === '1') return;
    td.setAttribute('data-cz-long', '1');
    td.addEventListener('click', function (e) { e.stopPropagation(); td.classList.toggle('cz-open'); });
  }

  /* لمسة على الكارت = فتح/قفل التفاصيل (من غير ما نلمس أي DOM بتاع السيستم) */
  function bindCardTap(tb) {
    if (tb.getAttribute('data-cz-tap') === '1') return;
    tb.setAttribute('data-cz-tap', '1');
    tb.addEventListener('click', function (e) {
      var t = e.target;
      if (!t || !t.closest) return;
      if (t.closest('button, a, input, select, textarea, label, .btn, .action-menu, .cz-long')) return;
      var tr = t.closest('tr');
      if (!tr || !tr.classList.contains('cz-card')) return;
      if (tr.getAttribute('onclick')) return;
      var td = t.closest('td');
      if (td && td.getAttribute('onclick')) return;
      tr.classList.toggle('cz-open');
    });
  }

  function rowGroups(tb) {
    var g = [], i;
    for (i = 0; i < tb.tBodies.length; i++) g.push(tb.tBodies[i]);
    if (tb.tFoot) g.push(tb.tFoot);
    return g;
  }

  function cardifyTable(tb) {
    if (tb.getAttribute('data-cz') === 'off') return;

    var labels = headerLabels(tb);
    if (!labels) {
      tb.classList.remove('cz-cards');
      markCardWrapper(tb, false);
      return;
    }

    var groups = rowGroups(tb), g, r, rows, tr;
    var sig = labels.join('|');
    if (tb.getAttribute('data-cz-sig') !== sig) {
      tb.setAttribute('data-cz-sig', sig);
      for (g = 0; g < groups.length; g++) {
        rows = groups[g].rows;
        for (r = 0; r < rows.length; r++) rows[r].removeAttribute('data-cz-row');
      }
    }

    for (g = 0; g < groups.length; g++) {
      rows = groups[g].rows;
      for (r = 0; r < rows.length; r++) {
        tr = rows[r];
        if (tr.getAttribute('data-cz-row') === '1') continue;
        applyRow(tr, labels);
        tr.setAttribute('data-cz-row', '1');
      }
    }
    tb.classList.add('cz-cards');
    markCardWrapper(tb, true);
    bindCardTap(tb);
  }

  /* الجداول المتحوّلة لكروت لا تحتاج overflow داخلي. تعليم الحاوية يسمح
     لـ CSS بإرجاع السحب الرأسي إلى الصفحة بإصبع واحد على iOS وAndroid WebView. */
  function markCardWrapper(tb, enabled) {
    var p = tb.parentElement;
    if (!p || !p.classList) return;
    if (!(p.classList.contains('table-wrap') || p.classList.contains('tablewrap') ||
          p.classList.contains('table-scroll') || p.classList.contains('scroll-x') ||
          p.classList.contains('orders-table-shell'))) return;
    if (enabled) p.classList.add('cz-card-wrap');
    else p.classList.remove('cz-card-wrap');
  }

  /* كروت الأرقام: أي حاوية كل عناصرها stat/metric تبقى شبكة عمودين */
  function tagTiles() {
    var boxes = document.querySelectorAll('.grid, .summary, .metrics, .stats-grid');
    var i, k, box, kids, all, c;
    for (i = 0; i < boxes.length; i++) {
      box = boxes[i];
      kids = box.children;
      all = kids.length > 0;
      for (k = 0; k < kids.length; k++) {
        c = kids[k];
        if (!(c.classList.contains('stat') || c.classList.contains('metric') ||
              c.classList.contains('summary-card'))) { all = false; break; }
      }
      if (all) box.classList.add('cz-tiles'); else box.classList.remove('cz-tiles');
    }
  }

  function scan() {
    var tables = document.getElementsByTagName('table'), i, t;
    for (i = 0; i < tables.length; i++) {
      t = tables[i];
      if (t.closest && t.closest('.cz-drawer')) continue;
      try { cardifyTable(t); } catch (e) {}
    }
    try { tagTiles(); } catch (e) {}
  }

  var timer = null;
  function schedule() {
    if (timer) return;
    timer = setTimeout(function () { timer = null; scan(); }, 130);
  }

  function watch() {
    if (!window.MutationObserver) return;
    var mo = new MutationObserver(function (muts) {
      var i;
      for (i = 0; i < muts.length; i++) {
        if (muts[i].addedNodes.length || muts[i].removedNodes.length) { schedule(); return; }
      }
    });
    mo.observe(document.body, { childList: true, subtree: true });
  }

  /* ================= التشغيل ================= */
  var started = false;

  function start() {
    if (started) return;
    if (MQ && !MQ.matches) return;   // كمبيوتر: ما نعملش أي حاجة
    started = true;
    try { buildShell(); } catch (e) {}
    scan();
    watch();
  }

  function onReady() {
    start();
    if (MQ) {
      if (MQ.addEventListener) MQ.addEventListener('change', start);
      else if (MQ.addListener) MQ.addListener(start);
    }
    window.addEventListener('pageshow', function () {
      /* رجوع من كاش المتصفح: نتأكد إن القائمة مقفولة وقفل التمرير مرفوع */
      closeDrawer();
      recoverPageScroll();
      if (started) schedule();
    });
  }

  /* لو الصفحة رجعت من كاش المتصفح بعد مودال، ممكن inline overflow:hidden يفضل
     موجوداً رغم إن المودال مقفول. بنشيله فقط عندما لا توجد نافذة ظاهرة. */
  function recoverPageScroll() {
    if (!document.body) return;
    var nodes = document.querySelectorAll('.modal, .modal-wrap, .modal-backdrop, .health-modal');
    var hasVisibleModal = false, i, s;
    for (i = 0; i < nodes.length; i++) {
      s = window.getComputedStyle ? window.getComputedStyle(nodes[i]) : null;
      if (!s || (s.display !== 'none' && s.visibility !== 'hidden')) {
        hasVisibleModal = true;
        break;
      }
    }
    if (!hasVisibleModal && document.body.style.overflow === 'hidden') {
      document.body.style.removeProperty('overflow');
    }
    if (!hasVisibleModal && document.documentElement.style.overflow === 'hidden') {
      document.documentElement.style.removeProperty('overflow');
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady);
  } else {
    onReady();
  }
})();
