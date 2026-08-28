'use strict';
/* ============================================================================
   Craft Zone — وحدة الورشة (v10.0)
   ----------------------------------------------------------------------------
   ورشة الصنايعية. الوحدة دي معزولة تمامًا عن حسابات المصنع: ليها رأس مالها
   وخزنتها لوحدها، وكل حركة فلوس فيها بتتسجل في workshop_cash_ledger بس —
   ما بتلمسش الخزنة العامة ولا صفحة الحسابات ولا المخزون ولا الأوردرات.

   الجديد في v10.0:
     • الصنايعي بقى ليه «نظام حساب»: باليومية أو بالساعة.
       - باليومية → المستحق = اليومية × جزء اليوم (يوم/نص/ربع) + إضافي − خصم
       - بالساعة  → المستحق = سعر الساعة × عدد الساعات + إضافي − خصم
       في الحالتين القبض بيفضل آخر الأسبوع زي ما هو (يوميات مستحقة تتجمّع
       وتتقفل بالقبض).
     • الخامات والمصاريف اتوحّدوا في حاجة واحدة: «مصروفات الورشة»
       (workshop_costs). بتختار البند (خامة/كهربا/إيجار/نقل/...) وتحط الكمية
       والسعر والإجمالي. الجداول القديمة بتتنقل أوتوماتيك لمرة واحدة.
     • قسم جديد: «إنتاج الورشة» — بتسجل الشغلانة استلمت كام شنطة، اسمها،
       ورقم الأوردر لو ليها علاقة بصفحة الأوردرات (مرجع للقراءة بس، من غير أي
       ربط مالي)، وقيمة الشغلانة والمكسب فيها.
     • ربح وخسارة الورشة = إيراد الإنتاج − (يوميات + مصروفات).
     • رصيد خزنة الورشة مسموح يكون بالسالب عادي (عجز) — مفيش أي مانع.

   الفكرة المحاسبية:
     • الحضور (يومية/ساعات) → «مستحق» للصنايعي، مش صرف كاش.
     • السلفة              → كاش خارج فعلاً، وبتتخصم من مستحقاته.
     • القبض الأسبوعي      → كاش خارج فعلاً، وبيقفل المستحق.
       مستحق الصنايعي = إجمالي المستحق − السلف − المقبوض.
     • مصروفات الورشة      → كاش خارج + تكلفة تشغيل.
     • تحصيل إنتاج          → كاش داخل خزنة الورشة.
     • رأس المال           → كاش داخل خزنة الورشة.
   ========================================================================== */

const WORKSHOP_CASH_KINDS = {
  capital_in:    { sign: +1, label: 'إضافة رأس مال' },
  capital_out:   { sign: -1, label: 'سحب من رأس المال' },
  advance:       { sign: -1, label: 'سلفة صنايعي' },
  payout:        { sign: -1, label: 'قبض صنايعي' },
  cost:          { sign: -1, label: 'مصروف ورشة' },
  production_in: { sign: +1, label: 'تحصيل إنتاج' },
  // أنواع قديمة — بتفضل موجودة علشان الحركات المتسجلة قبل التوحيد ما تتكسرش
  material:      { sign: -1, label: 'شراء خامات' },
  expense:       { sign: -1, label: 'مصروف ورشة' },
  adjust_in:     { sign: +1, label: 'تسوية بالزيادة' },
  adjust_out:    { sign: -1, label: 'تسوية بالنقص' },
};

// البنود الجاهزة اللي بتظهر في قائمة المصروف. أي بند جديد بيتكتب مرة واحدة
// وبيتضاف للقائمة أوتوماتيك في المرات الجاية.
const WORKSHOP_DEFAULT_COST_TYPES = [
  'خامات', 'غراء ولزق', 'ورق وكرتون', 'كهربا', 'مياه', 'إيجار', 'نقل ومواصلات',
  'صيانة وإصلاح', 'عدد وأدوات', 'بوفيه وضيافة', 'تليفون وانترنت', 'نظافة', 'متنوع',
];

const PAY_TYPES = { daily: 'باليومية', hourly: 'بالساعة' };

function register(ctx) {
  const {
    app, runAsync, getAsync, allAsync, addColumnIfMissing,
    authRequired, requirePerm, requireAnyPerm,
    num, roundMoney, recordAudit, hasPerm, getCachedSetting,
  } = ctx;

  const actorName = (req) => String(req?.user?.full_name || req?.user?.username || 'system');
  const today = () => new Date().toISOString().slice(0, 10);
  const clean = (v) => String(v ?? '').trim();
  const payTypeOf = (v) => (clean(v) === 'hourly' ? 'hourly' : 'daily');

  /* ===================== 1) الجداول ===================== */
  async function ensureWorkshopSchema() {
    await runAsync(`CREATE TABLE IF NOT EXISTS workshop_workers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE,
      phone TEXT DEFAULT '',
      national_id TEXT DEFAULT '',
      job_title TEXT DEFAULT '',
      pay_type TEXT DEFAULT 'daily',
      daily_wage REAL DEFAULT 0,
      hourly_rate REAL DEFAULT 0,
      default_hours REAL DEFAULT 8,
      start_date TEXT DEFAULT CURRENT_DATE,
      is_active INTEGER DEFAULT 1,
      notes TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['phone', "TEXT DEFAULT ''"], ['national_id', "TEXT DEFAULT ''"], ['job_title', "TEXT DEFAULT ''"],
      ['pay_type', "TEXT DEFAULT 'daily'"], ['daily_wage', 'REAL DEFAULT 0'], ['hourly_rate', 'REAL DEFAULT 0'],
      ['default_hours', 'REAL DEFAULT 8'], ['start_date', 'TEXT DEFAULT CURRENT_DATE'], ['is_active', 'INTEGER DEFAULT 1'],
      ['notes', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('workshop_workers', n, d);
    await runAsync(`UPDATE workshop_workers SET pay_type='daily' WHERE pay_type IS NULL OR TRIM(pay_type)=''`);

    await runAsync(`CREATE TABLE IF NOT EXISTS workshop_attendance (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      worker_id INTEGER DEFAULT 0,
      worker_name TEXT DEFAULT '',
      work_date TEXT DEFAULT CURRENT_DATE,
      pay_type TEXT DEFAULT 'daily',
      day_fraction REAL DEFAULT 1,
      daily_wage REAL DEFAULT 0,
      hours REAL DEFAULT 0,
      hourly_rate REAL DEFAULT 0,
      amount REAL DEFAULT 0,
      extra_amount REAL DEFAULT 0,
      deduction_amount REAL DEFAULT 0,
      note TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['worker_name', "TEXT DEFAULT ''"], ['pay_type', "TEXT DEFAULT 'daily'"], ['day_fraction', 'REAL DEFAULT 1'],
      ['daily_wage', 'REAL DEFAULT 0'], ['hours', 'REAL DEFAULT 0'], ['hourly_rate', 'REAL DEFAULT 0'],
      ['amount', 'REAL DEFAULT 0'], ['extra_amount', 'REAL DEFAULT 0'], ['deduction_amount', 'REAL DEFAULT 0'],
      ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('workshop_attendance', n, d);
    await runAsync(`UPDATE workshop_attendance SET pay_type='daily' WHERE pay_type IS NULL OR TRIM(pay_type)=''`);
    await runAsync(`CREATE UNIQUE INDEX IF NOT EXISTS idx_workshop_attendance_day ON workshop_attendance(worker_id, work_date)`);

    await runAsync(`CREATE TABLE IF NOT EXISTS workshop_advances (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      worker_id INTEGER DEFAULT 0,
      worker_name TEXT DEFAULT '',
      advance_date TEXT DEFAULT CURRENT_DATE,
      amount REAL DEFAULT 0,
      note TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['worker_name', "TEXT DEFAULT ''"], ['advance_date', 'TEXT DEFAULT CURRENT_DATE'], ['amount', 'REAL DEFAULT 0'],
      ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('workshop_advances', n, d);

    await runAsync(`CREATE TABLE IF NOT EXISTS workshop_payouts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      worker_id INTEGER DEFAULT 0,
      worker_name TEXT DEFAULT '',
      period_from TEXT DEFAULT '',
      period_to TEXT DEFAULT '',
      payout_date TEXT DEFAULT CURRENT_DATE,
      earned_amount REAL DEFAULT 0,
      advances_amount REAL DEFAULT 0,
      bonus_amount REAL DEFAULT 0,
      deduction_amount REAL DEFAULT 0,
      net_amount REAL DEFAULT 0,
      note TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['worker_name', "TEXT DEFAULT ''"], ['period_from', "TEXT DEFAULT ''"], ['period_to', "TEXT DEFAULT ''"],
      ['payout_date', 'TEXT DEFAULT CURRENT_DATE'], ['earned_amount', 'REAL DEFAULT 0'], ['advances_amount', 'REAL DEFAULT 0'],
      ['bonus_amount', 'REAL DEFAULT 0'], ['deduction_amount', 'REAL DEFAULT 0'], ['net_amount', 'REAL DEFAULT 0'],
      ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('workshop_payouts', n, d);

    /* ---- الجدول الموحّد للخامات والمصاريف ---- */
    await runAsync(`CREATE TABLE IF NOT EXISTS workshop_costs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      cost_date TEXT DEFAULT CURRENT_DATE,
      cost_type TEXT DEFAULT 'خامات',
      item_name TEXT DEFAULT '',
      qty REAL DEFAULT 0,
      unit TEXT DEFAULT '',
      unit_price REAL DEFAULT 0,
      total_amount REAL DEFAULT 0,
      supplier_name TEXT DEFAULT '',
      note TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['cost_date', 'TEXT DEFAULT CURRENT_DATE'], ['cost_type', "TEXT DEFAULT 'خامات'"],
      ['item_name', "TEXT DEFAULT ''"], ['qty', 'REAL DEFAULT 0'], ['unit', "TEXT DEFAULT ''"],
      ['unit_price', 'REAL DEFAULT 0'], ['total_amount', 'REAL DEFAULT 0'], ['supplier_name', "TEXT DEFAULT ''"],
      ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('workshop_costs', n, d);
    await runAsync(`CREATE INDEX IF NOT EXISTS idx_workshop_costs_date ON workshop_costs(cost_date)`);

    /* ---- إنتاج الورشة ---- */
    await runAsync(`CREATE TABLE IF NOT EXISTS workshop_production (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      prod_date TEXT DEFAULT CURRENT_DATE,
      job_name TEXT DEFAULT '',
      order_id INTEGER DEFAULT 0,
      order_ref TEXT DEFAULT '',
      customer_name TEXT DEFAULT '',
      qty REAL DEFAULT 0,
      unit_price REAL DEFAULT 0,
      total_amount REAL DEFAULT 0,
      cost_amount REAL DEFAULT 0,
      profit_amount REAL DEFAULT 0,
      status TEXT DEFAULT 'تحت التنفيذ',
      collected INTEGER DEFAULT 0,
      note TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['prod_date', 'TEXT DEFAULT CURRENT_DATE'], ['job_name', "TEXT DEFAULT ''"],
      ['order_id', 'INTEGER DEFAULT 0'], ['order_ref', "TEXT DEFAULT ''"], ['customer_name', "TEXT DEFAULT ''"],
      ['qty', 'REAL DEFAULT 0'], ['unit_price', 'REAL DEFAULT 0'], ['total_amount', 'REAL DEFAULT 0'],
      ['cost_amount', 'REAL DEFAULT 0'], ['profit_amount', 'REAL DEFAULT 0'], ['status', "TEXT DEFAULT 'تحت التنفيذ'"],
      ['collected', 'INTEGER DEFAULT 0'], ['note', "TEXT DEFAULT ''"],
      ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('workshop_production', n, d);
    await runAsync(`CREATE INDEX IF NOT EXISTS idx_workshop_production_date ON workshop_production(prod_date)`);

    await runAsync(`CREATE TABLE IF NOT EXISTS workshop_cash_ledger (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entry_date TEXT DEFAULT CURRENT_DATE,
      entry_kind TEXT DEFAULT 'capital_in',
      amount REAL DEFAULT 0,
      delta REAL DEFAULT 0,
      balance_before REAL DEFAULT 0,
      balance_after REAL DEFAULT 0,
      note TEXT DEFAULT '',
      source_type TEXT DEFAULT 'manual',
      source_ref TEXT DEFAULT '',
      is_auto INTEGER DEFAULT 0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['entry_kind', "TEXT DEFAULT 'capital_in'"], ['amount', 'REAL DEFAULT 0'], ['delta', 'REAL DEFAULT 0'],
      ['balance_before', 'REAL DEFAULT 0'], ['balance_after', 'REAL DEFAULT 0'], ['note', "TEXT DEFAULT ''"],
      ['source_type', "TEXT DEFAULT 'manual'"], ['source_ref', "TEXT DEFAULT ''"], ['is_auto', 'INTEGER DEFAULT 0'],
      ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('workshop_cash_ledger', n, d);
    await runAsync(`CREATE INDEX IF NOT EXISTS idx_workshop_cash_source ON workshop_cash_ledger(source_type, source_ref)`);

    await migrateLegacyCosts();
  }

  /* ---- ترحيل الخامات والمصاريف القديمة لجدول المصروفات الموحّد ----
     بيشتغل مرة واحدة بس (لأن السطور بتتشال من الجدول القديم بعد النقل)،
     وبيحوّل كمان حركة الخزنة المرتبطة بيها علشان الرصيد ما يتغيّرش. */
  async function migrateLegacyCosts() {
    const hasTable = async (t) => !!(await getAsync(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, [t]));
    let migrated = 0;

    if (await hasTable('workshop_materials')) {
      const rows = await allAsync(`SELECT * FROM workshop_materials`);
      for (const r of rows) {
        const ins = await runAsync(
          `INSERT INTO workshop_costs (cost_date,cost_type,item_name,qty,unit,unit_price,total_amount,supplier_name,note,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?)`,
          [clean(r.purchase_date) || today(), clean(r.category) || 'خامات', clean(r.item_name), num(r.qty), clean(r.unit),
            roundMoney(num(r.unit_price)), roundMoney(num(r.total_amount)), clean(r.supplier_name), clean(r.note), clean(r.created_by)]);
        await runAsync(`UPDATE workshop_cash_ledger SET source_type='workshop_cost', source_ref=?, entry_kind='cost' WHERE source_type='workshop_material' AND source_ref=?`,
          [String(ins.lastID), String(r.id)]);
      }
      if (rows.length) await runAsync(`DELETE FROM workshop_materials`);
      migrated += rows.length;
    }

    if (await hasTable('workshop_expenses')) {
      const rows = await allAsync(`SELECT * FROM workshop_expenses`);
      for (const r of rows) {
        const ins = await runAsync(
          `INSERT INTO workshop_costs (cost_date,cost_type,item_name,qty,unit,unit_price,total_amount,supplier_name,note,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?)`,
          [clean(r.expense_date) || today(), clean(r.category) || 'متنوع', clean(r.category), 0, '', 0,
            roundMoney(num(r.amount)), '', clean(r.note), clean(r.created_by)]);
        await runAsync(`UPDATE workshop_cash_ledger SET source_type='workshop_cost', source_ref=?, entry_kind='cost' WHERE source_type='workshop_expense' AND source_ref=?`,
          [String(ins.lastID), String(r.id)]);
      }
      if (rows.length) await runAsync(`DELETE FROM workshop_expenses`);
      migrated += rows.length;
    }

    // بعد ما المراجع اتغيّرت نعيد بناء الأرصدة الجارية علشان الكشف يفضل مظبوط
    if (migrated) await rebuildWorkshopCashBalances();
  }

  /* ===================== 2) خزنة الورشة ===================== */
  /* ملاحظة مهمة: رصيد خزنة الورشة مسموح يكون بالسالب (عجز) — مفيش أي تحقق
     بيمنع الصرف لو الرصيد مش كفاية. ده مقصود. */
  async function workshopCashBalance() {
    const row = await getAsync(`SELECT COALESCE(SUM(delta),0) bal FROM workshop_cash_ledger`);
    return roundMoney(num(row?.bal));
  }

  async function addWorkshopCashEntry({ kind, amount, date, note = '', sourceType = 'manual', sourceRef = '', isAuto = 0, createdBy = 'system' }) {
    const def = WORKSHOP_CASH_KINDS[kind];
    if (!def) throw new Error('نوع حركة غير معروف في خزنة الورشة');
    const value = roundMoney(Math.abs(num(amount)));
    if (!(value > 0)) return null;
    const delta = roundMoney(def.sign * value);
    const before = await workshopCashBalance();
    const after = roundMoney(before + delta);   // ممكن يطلع سالب عادي
    const ins = await runAsync(
      `INSERT INTO workshop_cash_ledger (entry_date,entry_kind,amount,delta,balance_before,balance_after,note,source_type,source_ref,is_auto,created_by)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
      [clean(date) || today(), kind, value, delta, before, after, clean(note), clean(sourceType) || 'manual', clean(sourceRef), isAuto ? 1 : 0, clean(createdBy)]);
    return ins.lastID;
  }

  async function deleteWorkshopCashBySource(sourceType, sourceRef) {
    await runAsync(`DELETE FROM workshop_cash_ledger WHERE source_type=? AND source_ref=?`, [clean(sourceType), String(sourceRef)]);
    await rebuildWorkshopCashBalances();
  }

  /* إعادة بناء الأرصدة الجارية بالترتيب الزمني — بيتنادى بعد أي حذف أو تعديل. */
  async function rebuildWorkshopCashBalances() {
    const rows = await allAsync(`SELECT id, delta FROM workshop_cash_ledger ORDER BY date(entry_date) ASC, id ASC`);
    let running = 0;
    for (const row of rows) {
      const before = roundMoney(running);
      running = roundMoney(running + num(row.delta));
      await runAsync(`UPDATE workshop_cash_ledger SET balance_before=?, balance_after=? WHERE id=?`, [before, running, row.id]);
    }
    return roundMoney(running);
  }

  /* ===================== 3) حسابات الصنايعي ===================== */
  function periodWhere(field, from, to, params) {
    let sql = '';
    if (clean(from)) { sql += ` AND date(${field}) >= date(?)`; params.push(clean(from)); }
    if (clean(to)) { sql += ` AND date(${field}) <= date(?)`; params.push(clean(to)); }
    return sql;
  }

  async function workerTotals(workerId, { from = '', to = '' } = {}) {
    const p1 = [workerId]; const w1 = periodWhere('work_date', from, to, p1);
    const earnedRow = await getAsync(
      `SELECT COALESCE(SUM(amount),0) earned,
              COALESCE(SUM(CASE WHEN pay_type='hourly' THEN 0 ELSE day_fraction END),0) days,
              COALESCE(SUM(CASE WHEN pay_type='hourly' THEN hours ELSE 0 END),0) hours
       FROM workshop_attendance WHERE worker_id=?${w1}`, p1);
    const p2 = [workerId]; const w2 = periodWhere('advance_date', from, to, p2);
    const advRow = await getAsync(`SELECT COALESCE(SUM(amount),0) adv FROM workshop_advances WHERE worker_id=?${w2}`, p2);
    const p3 = [workerId]; const w3 = periodWhere('payout_date', from, to, p3);
    const payRow = await getAsync(`SELECT COALESCE(SUM(net_amount),0) paid FROM workshop_payouts WHERE worker_id=?${w3}`, p3);
    const earned = roundMoney(num(earnedRow?.earned));
    const advances = roundMoney(num(advRow?.adv));
    const paid = roundMoney(num(payRow?.paid));
    return {
      earned, advances, paid,
      days: roundMoney(num(earnedRow?.days)),
      hours: roundMoney(num(earnedRow?.hours)),
      // المستحق = اللي كسبه ناقص اللي أخده (سلف + قبض)
      balance: roundMoney(earned - advances - paid),
    };
  }

  /* ===================== 4) الصلاحيات ===================== */
  const canView = requireAnyPerm('perm_view_workshop', 'perm_manage_workshop');
  const canManage = requirePerm('perm_manage_workshop');
  const canPay = requireAnyPerm('perm_manage_workshop_payouts', 'perm_manage_workshop');
  const canCapital = requireAnyPerm('perm_manage_workshop_capital', 'perm_manage_workshop');

  /* ===================== 5) المسارات ===================== */

  app.get('/workshop-dashboard-data', authRequired, canView, async (req, res) => {
    try {
      const from = clean(req.query.from), to = clean(req.query.to);
      const showTotals = hasPerm(req.user, 'perm_view_workshop_totals') || hasPerm(req.user, 'perm_manage_workshop');

      const workers = await allAsync(`SELECT * FROM workshop_workers ORDER BY is_active DESC, name ASC`);

      const pA = []; const wA = periodWhere('work_date', from, to, pA).replace(/^ AND/, ' WHERE');
      const attendance = await allAsync(`SELECT * FROM workshop_attendance${wA} ORDER BY date(work_date) DESC, id DESC LIMIT 800`, pA);
      const pB = []; const wB = periodWhere('advance_date', from, to, pB).replace(/^ AND/, ' WHERE');
      const advances = await allAsync(`SELECT * FROM workshop_advances${wB} ORDER BY date(advance_date) DESC, id DESC LIMIT 500`, pB);
      const pC = []; const wC = periodWhere('payout_date', from, to, pC).replace(/^ AND/, ' WHERE');
      const payouts = await allAsync(`SELECT * FROM workshop_payouts${wC} ORDER BY date(payout_date) DESC, id DESC LIMIT 500`, pC);
      const pD = []; const wD = periodWhere('cost_date', from, to, pD).replace(/^ AND/, ' WHERE');
      const costs = await allAsync(`SELECT * FROM workshop_costs${wD} ORDER BY date(cost_date) DESC, id DESC LIMIT 700`, pD);
      const pP = []; const wP = periodWhere('prod_date', from, to, pP).replace(/^ AND/, ' WHERE');
      const production = await allAsync(`SELECT * FROM workshop_production${wP} ORDER BY date(prod_date) DESC, id DESC LIMIT 500`, pP);
      const pF = []; const wF = periodWhere('entry_date', from, to, pF).replace(/^ AND/, ' WHERE');
      const cashLog = await allAsync(`SELECT * FROM workshop_cash_ledger${wF} ORDER BY date(entry_date) DESC, id DESC LIMIT 500`, pF);

      // ملخص كل صنايعي: على الفترة + رصيده الكلي (من غير فلتر) علشان المستحق الحقيقي
      const summary = [];
      for (const w of workers) {
        const period = await workerTotals(w.id, { from, to });
        const lifetime = await workerTotals(w.id, {});
        summary.push({
          id: w.id, name: w.name, job_title: w.job_title,
          pay_type: payTypeOf(w.pay_type),
          daily_wage: roundMoney(num(w.daily_wage)),
          hourly_rate: roundMoney(num(w.hourly_rate)),
          default_hours: num(w.default_hours, 8),
          phone: w.phone, is_active: num(w.is_active, 1),
          period_days: period.days, period_hours: period.hours,
          period_earned: period.earned, period_advances: period.advances, period_paid: period.paid,
          total_earned: lifetime.earned, total_advances: lifetime.advances, total_paid: lifetime.paid,
          balance: lifetime.balance,
        });
      }

      const sum = (rows, field) => roundMoney((rows || []).reduce((a, r) => a + num(r[field]), 0));
      const capitalRow = await getAsync(
        `SELECT COALESCE(SUM(CASE WHEN entry_kind='capital_in' THEN amount ELSE 0 END),0) capital_in,
                COALESCE(SUM(CASE WHEN entry_kind='capital_out' THEN amount ELSE 0 END),0) capital_out
         FROM workshop_cash_ledger`);

      const earned = sum(attendance, 'amount');
      const costsTotal = sum(costs, 'total_amount');
      const revenue = sum(production, 'total_amount');
      const declaredProfit = sum(production, 'profit_amount');
      const collectedRow = await getAsync(
        `SELECT COALESCE(SUM(total_amount),0) v FROM workshop_production WHERE collected=1${from ? ' AND date(prod_date)>=date(?)' : ''}${to ? ' AND date(prod_date)<=date(?)' : ''}`,
        [from, to].filter(Boolean));
      const operatingCost = roundMoney(earned + costsTotal);

      // تجميع المصروفات حسب البند علشان أعرف الفلوس بتروح فين
      const costsByType = {};
      for (const c of costs) {
        const k = clean(c.cost_type) || 'متنوع';
        costsByType[k] = roundMoney(num(costsByType[k]) + num(c.total_amount));
      }

      const typeRows = await allAsync(`SELECT DISTINCT cost_type t FROM workshop_costs WHERE TRIM(COALESCE(cost_type,''))<>''`);
      const costTypes = Array.from(new Set([...WORKSHOP_DEFAULT_COST_TYPES, ...typeRows.map(r => clean(r.t))])).filter(Boolean);

      const totals = {
        workers_count: workers.filter(w => num(w.is_active, 1) === 1).length,
        workers_all: workers.length,
        workers_daily: workers.filter(w => payTypeOf(w.pay_type) === 'daily' && num(w.is_active, 1) === 1).length,
        workers_hourly: workers.filter(w => payTypeOf(w.pay_type) === 'hourly' && num(w.is_active, 1) === 1).length,
        period_days: roundMoney(summary.reduce((a, s) => a + num(s.period_days), 0)),
        period_hours: roundMoney(summary.reduce((a, s) => a + num(s.period_hours), 0)),
        earned,
        advances: sum(advances, 'amount'),
        payouts: sum(payouts, 'net_amount'),
        costs: costsTotal,
        costs_by_type: costsByType,
        // تكلفة تشغيل الورشة = مستحقات الصنايعية + المصروفات (السلف مش تكلفة)
        operating_cost: operatingCost,
        production_qty: roundMoney(production.reduce((a, r) => a + num(r.qty), 0)),
        production_jobs: production.length,
        revenue,
        revenue_collected: roundMoney(num(collectedRow?.v)),
        declared_profit: declaredProfit,
        // ربح/خسارة الورشة الحقيقي
        net_profit: roundMoney(revenue - operatingCost),
        cash_out: roundMoney(sum(advances, 'amount') + sum(payouts, 'net_amount') + costsTotal),
        capital_in: roundMoney(num(capitalRow?.capital_in)),
        capital_out: roundMoney(num(capitalRow?.capital_out)),
        cash_balance: await workshopCashBalance(),
        workers_due: roundMoney(summary.reduce((a, s) => a + Math.max(0, num(s.balance)), 0)),
        workers_overpaid: roundMoney(summary.reduce((a, s) => a + Math.min(0, num(s.balance)), 0)),
      };

      const payload = {
        workers, summary, attendance, advances, payouts, costs, production, cash_log: cashLog, totals,
        cost_types: costTypes,
        pay_types: PAY_TYPES,
        settings: {
          default_daily_wage: num(getCachedSetting('workshop_default_daily_wage', '0')),
          default_hourly_rate: num(getCachedSetting('workshop_default_hourly_rate', '0')),
          week_start: clean(getCachedSetting('workshop_week_start', 'السبت')),
          payout_cycle: clean(getCachedSetting('workshop_payout_cycle', 'أسبوعي')),
        },
        can: {
          manage: hasPerm(req.user, 'perm_manage_workshop'),
          pay: hasPerm(req.user, 'perm_manage_workshop_payouts') || hasPerm(req.user, 'perm_manage_workshop'),
          capital: hasPerm(req.user, 'perm_manage_workshop_capital') || hasPerm(req.user, 'perm_manage_workshop'),
          totals: showTotals,
        },
      };
      if (!showTotals) {
        payload.totals = { ...payload.totals, costs_by_type: {} };
        for (const k of ['earned', 'advances', 'payouts', 'costs', 'operating_cost', 'revenue', 'revenue_collected',
          'declared_profit', 'net_profit', 'cash_out', 'capital_in', 'capital_out', 'cash_balance',
          'workers_due', 'workers_overpaid']) payload.totals[k] = 0;
      }
      res.json(payload);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- الصنايعية ---------- */
  app.post('/save-workshop-worker', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.body?.id);
      const name = clean(req.body?.name);
      if (!name) return res.status(400).json({ error: 'اكتب اسم الصنايعي' });
      const dup = await getAsync(`SELECT id FROM workshop_workers WHERE LOWER(name)=LOWER(?) AND id<>?`, [name, id]);
      if (dup) return res.status(400).json({ error: 'في صنايعي بنفس الاسم' });
      const payType = payTypeOf(req.body?.pay_type);
      const dailyWage = roundMoney(num(req.body?.daily_wage));
      const hourlyRate = roundMoney(num(req.body?.hourly_rate));
      if (payType === 'daily' && !(dailyWage > 0)) return res.status(400).json({ error: 'اكتب اليومية للصنايعي اللي شغال باليومية' });
      if (payType === 'hourly' && !(hourlyRate > 0)) return res.status(400).json({ error: 'اكتب سعر الساعة للصنايعي اللي شغال بالساعة' });
      const vals = [name, clean(req.body?.phone), clean(req.body?.national_id), clean(req.body?.job_title),
        payType, dailyWage, hourlyRate, num(req.body?.default_hours, 8),
        clean(req.body?.start_date) || today(), num(req.body?.is_active, 1) ? 1 : 0, clean(req.body?.notes)];
      if (id) {
        await runAsync(`UPDATE workshop_workers SET name=?,phone=?,national_id=?,job_title=?,pay_type=?,daily_wage=?,hourly_rate=?,default_hours=?,start_date=?,is_active=?,notes=? WHERE id=?`, [...vals, id]);
        await runAsync(`UPDATE workshop_attendance SET worker_name=? WHERE worker_id=?`, [name, id]);
        await runAsync(`UPDATE workshop_advances SET worker_name=? WHERE worker_id=?`, [name, id]);
        await runAsync(`UPDATE workshop_payouts SET worker_name=? WHERE worker_id=?`, [name, id]);
        await recordAudit({ req, action: 'save-workshop-worker', entity_type: 'workshop_worker', entity_id: id, details: `تعديل صنايعي: ${name}` });
        return res.json({ success: true, id });
      }
      const ins = await runAsync(`INSERT INTO workshop_workers (name,phone,national_id,job_title,pay_type,daily_wage,hourly_rate,default_hours,start_date,is_active,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, [...vals, actorName(req)]);
      await recordAudit({ req, action: 'save-workshop-worker', entity_type: 'workshop_worker', entity_id: ins.lastID, details: `إضافة صنايعي: ${name} (${PAY_TYPES[payType]})` });
      res.json({ success: true, id: ins.lastID });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-workshop-worker/:id', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.params.id);
      const used = await getAsync(
        `SELECT (SELECT COUNT(*) FROM workshop_attendance WHERE worker_id=?) a,
                (SELECT COUNT(*) FROM workshop_advances WHERE worker_id=?) b,
                (SELECT COUNT(*) FROM workshop_payouts WHERE worker_id=?) c`, [id, id, id]);
      if (num(used?.a) + num(used?.b) + num(used?.c) > 0)
        return res.status(400).json({ error: 'مينفعش تحذف الصنايعي لأن له حركات مسجلة. وقّفه من التعديل بدل الحذف.' });
      await runAsync(`DELETE FROM workshop_workers WHERE id=?`, [id]);
      await recordAudit({ req, action: 'delete-workshop-worker', entity_type: 'workshop_worker', entity_id: id, details: 'حذف صنايعي' });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- الحضور / اليومية أو الساعات ---------- */
  function computeAttendanceAmount({ payType, fraction, wage, hours, rate, extra, deduction }) {
    const base = payType === 'hourly'
      ? roundMoney(num(rate) * num(hours))
      : roundMoney(num(wage) * num(fraction));
    return roundMoney(Math.max(0, base + num(extra) - num(deduction)));
  }

  async function upsertAttendance(req, row) {
    const workerId = num(row?.worker_id);
    const worker = await getAsync(`SELECT * FROM workshop_workers WHERE id=?`, [workerId]);
    if (!worker) throw new Error('اختار صنايعي صحيح');
    const workDate = clean(row?.work_date) || today();
    // نظام الحساب بيتاخد من ملف الصنايعي، إلا لو الصف بعت نظام مختلف صراحةً
    const payType = clean(row?.pay_type) ? payTypeOf(row.pay_type) : payTypeOf(worker.pay_type);

    const given = (v) => !(v === undefined || v === null || clean(v) === '');
    const fraction = payType === 'hourly' ? 0 : Math.max(0, num(row?.day_fraction, 1));
    const hours = payType === 'hourly' ? Math.max(0, num(row?.hours, num(worker.default_hours, 8))) : 0;
    const wage = payType === 'hourly' ? 0
      : (given(row?.daily_wage) ? roundMoney(num(row.daily_wage)) : roundMoney(num(worker.daily_wage)));
    const rate = payType === 'hourly'
      ? (given(row?.hourly_rate) ? roundMoney(num(row.hourly_rate)) : roundMoney(num(worker.hourly_rate)))
      : 0;
    const extra = roundMoney(num(row?.extra_amount));
    const deduction = roundMoney(num(row?.deduction_amount));
    const amount = computeAttendanceAmount({ payType, fraction, wage, hours, rate, extra, deduction });

    const existing = await getAsync(`SELECT id FROM workshop_attendance WHERE worker_id=? AND work_date=?`, [workerId, workDate]);
    if (existing) {
      await runAsync(`UPDATE workshop_attendance SET worker_name=?,pay_type=?,day_fraction=?,daily_wage=?,hours=?,hourly_rate=?,amount=?,extra_amount=?,deduction_amount=?,note=? WHERE id=?`,
        [worker.name, payType, fraction, wage, hours, rate, amount, extra, deduction, clean(row?.note), existing.id]);
      return { id: existing.id, updated: true, amount };
    }
    const ins = await runAsync(
      `INSERT INTO workshop_attendance (worker_id,worker_name,work_date,pay_type,day_fraction,daily_wage,hours,hourly_rate,amount,extra_amount,deduction_amount,note,created_by)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [workerId, worker.name, workDate, payType, fraction, wage, hours, rate, amount, extra, deduction, clean(row?.note), actorName(req)]);
    return { id: ins.lastID, updated: false, amount };
  }

  app.post('/save-workshop-attendance', authRequired, canManage, async (req, res) => {
    try {
      const rows = Array.isArray(req.body?.rows) ? req.body.rows : [req.body];
      const out = [];
      for (const row of rows) {
        if (!num(row?.worker_id)) continue;
        if (num(row?.absent) === 1) {   // غياب = مسح سطر اليوم لو موجود
          await runAsync(`DELETE FROM workshop_attendance WHERE worker_id=? AND work_date=?`, [num(row.worker_id), clean(row?.work_date) || today()]);
          continue;
        }
        out.push(await upsertAttendance(req, row));
      }
      await recordAudit({ req, action: 'save-workshop-attendance', entity_type: 'workshop_attendance', entity_id: 0, details: `تسجيل يومية لـ ${out.length} صنايعي` });
      res.json({ success: true, saved: out.length, rows: out });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-workshop-attendance/:id', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.params.id);
      await runAsync(`DELETE FROM workshop_attendance WHERE id=?`, [id]);
      await recordAudit({ req, action: 'delete-workshop-attendance', entity_type: 'workshop_attendance', entity_id: id, details: 'حذف يومية' });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- السلف ---------- */
  app.post('/save-workshop-advance', authRequired, canPay, async (req, res) => {
    try {
      const id = num(req.body?.id);
      const workerId = num(req.body?.worker_id);
      const worker = await getAsync(`SELECT * FROM workshop_workers WHERE id=?`, [workerId]);
      if (!worker) return res.status(400).json({ error: 'اختار صنايعي صحيح' });
      const amount = roundMoney(num(req.body?.amount));
      if (!(amount > 0)) return res.status(400).json({ error: 'اكتب مبلغ السلفة' });
      const date = clean(req.body?.advance_date) || today();
      const note = clean(req.body?.note);
      if (id) {
        await runAsync(`UPDATE workshop_advances SET worker_id=?,worker_name=?,advance_date=?,amount=?,note=? WHERE id=?`, [workerId, worker.name, date, amount, note, id]);
        await deleteWorkshopCashBySource('workshop_advance', id);
        await addWorkshopCashEntry({ kind: 'advance', amount, date, note: `سلفة ${worker.name}${note ? ' — ' + note : ''}`, sourceType: 'workshop_advance', sourceRef: id, isAuto: 1, createdBy: actorName(req) });
        await rebuildWorkshopCashBalances();
        await recordAudit({ req, action: 'save-workshop-advance', entity_type: 'workshop_advance', entity_id: id, details: `تعديل سلفة ${worker.name}: ${amount}` });
        return res.json({ success: true, id });
      }
      const ins = await runAsync(`INSERT INTO workshop_advances (worker_id,worker_name,advance_date,amount,note,created_by) VALUES (?,?,?,?,?,?)`, [workerId, worker.name, date, amount, note, actorName(req)]);
      await addWorkshopCashEntry({ kind: 'advance', amount, date, note: `سلفة ${worker.name}${note ? ' — ' + note : ''}`, sourceType: 'workshop_advance', sourceRef: ins.lastID, isAuto: 1, createdBy: actorName(req) });
      await rebuildWorkshopCashBalances();
      await recordAudit({ req, action: 'save-workshop-advance', entity_type: 'workshop_advance', entity_id: ins.lastID, details: `سلفة ${worker.name}: ${amount}` });
      res.json({ success: true, id: ins.lastID });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-workshop-advance/:id', authRequired, canPay, async (req, res) => {
    try {
      const id = num(req.params.id);
      await runAsync(`DELETE FROM workshop_advances WHERE id=?`, [id]);
      await deleteWorkshopCashBySource('workshop_advance', id);
      await recordAudit({ req, action: 'delete-workshop-advance', entity_type: 'workshop_advance', entity_id: id, details: 'حذف سلفة' });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- القبض ---------- */
  app.get('/workshop-payout-suggestion', authRequired, canPay, async (req, res) => {
    try {
      const workerId = num(req.query.worker_id);
      const from = clean(req.query.from), to = clean(req.query.to);
      const worker = await getAsync(`SELECT * FROM workshop_workers WHERE id=?`, [workerId]);
      if (!worker) return res.status(400).json({ error: 'اختار صنايعي صحيح' });
      const period = await workerTotals(workerId, { from, to });
      const lifetime = await workerTotals(workerId, {});
      res.json({
        worker: {
          id: worker.id, name: worker.name,
          pay_type: payTypeOf(worker.pay_type),
          pay_type_label: PAY_TYPES[payTypeOf(worker.pay_type)],
          daily_wage: roundMoney(num(worker.daily_wage)),
          hourly_rate: roundMoney(num(worker.hourly_rate)),
        },
        period_from: from, period_to: to,
        earned: period.earned, advances: period.advances, days: period.days, hours: period.hours, already_paid: period.paid,
        suggested_net: roundMoney(Math.max(0, period.earned - period.advances - period.paid)),
        lifetime_balance: lifetime.balance,
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/save-workshop-payout', authRequired, canPay, async (req, res) => {
    try {
      const id = num(req.body?.id);
      const workerId = num(req.body?.worker_id);
      const worker = await getAsync(`SELECT * FROM workshop_workers WHERE id=?`, [workerId]);
      if (!worker) return res.status(400).json({ error: 'اختار صنايعي صحيح' });
      const from = clean(req.body?.period_from), to = clean(req.body?.period_to);
      const earned = roundMoney(num(req.body?.earned_amount));
      const advancesAmount = roundMoney(num(req.body?.advances_amount));
      const bonus = roundMoney(num(req.body?.bonus_amount));
      const deduction = roundMoney(num(req.body?.deduction_amount));
      let net = roundMoney(num(req.body?.net_amount));
      if (!net) net = roundMoney(Math.max(0, earned - advancesAmount + bonus - deduction));
      if (!(net > 0)) return res.status(400).json({ error: 'صافي المقبوض لازم يكون أكبر من صفر' });
      const date = clean(req.body?.payout_date) || today();
      const note = clean(req.body?.note);
      const vals = [workerId, worker.name, from, to, date, earned, advancesAmount, bonus, deduction, net, note];
      if (id) {
        await runAsync(`UPDATE workshop_payouts SET worker_id=?,worker_name=?,period_from=?,period_to=?,payout_date=?,earned_amount=?,advances_amount=?,bonus_amount=?,deduction_amount=?,net_amount=?,note=? WHERE id=?`, [...vals, id]);
        await deleteWorkshopCashBySource('workshop_payout', id);
        await addWorkshopCashEntry({ kind: 'payout', amount: net, date, note: `قبض ${worker.name}`, sourceType: 'workshop_payout', sourceRef: id, isAuto: 1, createdBy: actorName(req) });
        await rebuildWorkshopCashBalances();
        await recordAudit({ req, action: 'save-workshop-payout', entity_type: 'workshop_payout', entity_id: id, details: `تعديل قبض ${worker.name}: ${net}` });
        return res.json({ success: true, id });
      }
      const ins = await runAsync(`INSERT INTO workshop_payouts (worker_id,worker_name,period_from,period_to,payout_date,earned_amount,advances_amount,bonus_amount,deduction_amount,net_amount,note,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, [...vals, actorName(req)]);
      await addWorkshopCashEntry({ kind: 'payout', amount: net, date, note: `قبض ${worker.name}`, sourceType: 'workshop_payout', sourceRef: ins.lastID, isAuto: 1, createdBy: actorName(req) });
      await rebuildWorkshopCashBalances();
      await recordAudit({ req, action: 'save-workshop-payout', entity_type: 'workshop_payout', entity_id: ins.lastID, details: `قبض ${worker.name}: ${net}` });
      res.json({ success: true, id: ins.lastID });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-workshop-payout/:id', authRequired, canPay, async (req, res) => {
    try {
      const id = num(req.params.id);
      await runAsync(`DELETE FROM workshop_payouts WHERE id=?`, [id]);
      await deleteWorkshopCashBySource('workshop_payout', id);
      await recordAudit({ req, action: 'delete-workshop-payout', entity_type: 'workshop_payout', entity_id: id, details: 'حذف قبض' });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- مصروفات الورشة (خامات + مصاريف في حاجة واحدة) ---------- */
  async function saveCostRow(req, body) {
    const id = num(body?.id);
    const costType = clean(body?.cost_type) || clean(body?.category) || 'متنوع';
    if (!costType) throw new Error('اختار البند');
    const itemName = clean(body?.item_name) || costType;
    const qty = num(body?.qty);
    const unitPrice = roundMoney(num(body?.unit_price));
    let total = roundMoney(num(body?.total_amount ?? body?.amount));
    if (!total) total = roundMoney(qty * unitPrice);
    if (!(total > 0)) throw new Error('اكتب المبلغ أو الكمية والسعر');
    const date = clean(body?.cost_date) || clean(body?.purchase_date) || clean(body?.expense_date) || today();
    const vals = [date, costType, itemName, qty, clean(body?.unit), unitPrice, total, clean(body?.supplier_name), clean(body?.note)];
    const label = `${costType}${itemName && itemName !== costType ? ' — ' + itemName : ''}`;
    if (id) {
      await runAsync(`UPDATE workshop_costs SET cost_date=?,cost_type=?,item_name=?,qty=?,unit=?,unit_price=?,total_amount=?,supplier_name=?,note=? WHERE id=?`, [...vals, id]);
      await deleteWorkshopCashBySource('workshop_cost', id);
      await addWorkshopCashEntry({ kind: 'cost', amount: total, date, note: label, sourceType: 'workshop_cost', sourceRef: id, isAuto: 1, createdBy: actorName(req) });
      await rebuildWorkshopCashBalances();
      await recordAudit({ req, action: 'save-workshop-cost', entity_type: 'workshop_cost', entity_id: id, details: `تعديل مصروف ورشة: ${label} بـ ${total}` });
      return { success: true, id };
    }
    const ins = await runAsync(`INSERT INTO workshop_costs (cost_date,cost_type,item_name,qty,unit,unit_price,total_amount,supplier_name,note,created_by) VALUES (?,?,?,?,?,?,?,?,?,?)`, [...vals, actorName(req)]);
    await addWorkshopCashEntry({ kind: 'cost', amount: total, date, note: label, sourceType: 'workshop_cost', sourceRef: ins.lastID, isAuto: 1, createdBy: actorName(req) });
    await rebuildWorkshopCashBalances();
    await recordAudit({ req, action: 'save-workshop-cost', entity_type: 'workshop_cost', entity_id: ins.lastID, details: `مصروف ورشة: ${label} بـ ${total}` });
    return { success: true, id: ins.lastID };
  }

  app.post('/save-workshop-cost', authRequired, canManage, async (req, res) => {
    try { res.json(await saveCostRow(req, req.body)); }
    catch (e) { res.status(400).json({ error: e.message }); }
  });

  app.delete('/delete-workshop-cost/:id', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.params.id);
      await runAsync(`DELETE FROM workshop_costs WHERE id=?`, [id]);
      await deleteWorkshopCashBySource('workshop_cost', id);
      await recordAudit({ req, action: 'delete-workshop-cost', entity_type: 'workshop_cost', entity_id: id, details: 'حذف مصروف ورشة' });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // مسارات قديمة — بتكتب في نفس الجدول الموحّد علشان أي نداء قديم ما يقعش
  app.post('/save-workshop-material', authRequired, canManage, async (req, res) => {
    try { res.json(await saveCostRow(req, { ...req.body, cost_type: clean(req.body?.category) || 'خامات' })); }
    catch (e) { res.status(400).json({ error: e.message }); }
  });
  app.post('/save-workshop-expense', authRequired, canManage, async (req, res) => {
    try { res.json(await saveCostRow(req, { ...req.body, cost_type: clean(req.body?.category) || 'متنوع' })); }
    catch (e) { res.status(400).json({ error: e.message }); }
  });

  /* ---------- إنتاج الورشة ---------- */
  /* مرجع الأوردر هنا للقراءة والتوثيق بس. مفيش أي حركة بتتكتب في جدول
     الأوردرات ولا في حسابات المصنع — الورشة فاضلة معزولة زي ما اتفقنا. */
  app.get('/workshop-orders-lookup', authRequired, canView, async (req, res) => {
    try {
      const q = clean(req.query.q);
      const hasOrders = await getAsync(`SELECT name FROM sqlite_master WHERE type='table' AND name='orders'`);
      if (!hasOrders) return res.json({ orders: [] });
      const params = [];
      let where = '';
      if (q) {
        where = ` WHERE (CAST(o.id AS TEXT) LIKE ? OR COALESCE(o.store_ref,'') LIKE ? OR COALESCE(o.custName,'') LIKE ?)`;
        params.push(`%${q}%`, `%${q}%`, `%${q}%`);
      }
      const rows = await allAsync(
        `SELECT o.id, COALESCE(o.store_ref,'') store_ref, COALESCE(o.custName,'') custName,
                COALESCE(o.orderDate,'') orderDate, COALESCE(o.qty,0) qty,
                COALESCE(o.product_type,'') product_type, COALESCE(o.status,'') status
         FROM orders o${where} ORDER BY o.id DESC LIMIT 300`, params);
      res.json({ orders: rows });
    } catch (e) { res.json({ orders: [], error: e.message }); }
  });

  app.post('/save-workshop-production', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.body?.id);
      const jobName = clean(req.body?.job_name);
      if (!jobName) return res.status(400).json({ error: 'اكتب اسم الشغلانة' });
      const qty = num(req.body?.qty);
      if (!(qty > 0)) return res.status(400).json({ error: 'اكتب عدد الشنط' });
      const unitPrice = roundMoney(num(req.body?.unit_price));
      let total = roundMoney(num(req.body?.total_amount));
      if (!total) total = roundMoney(qty * unitPrice);
      const costAmount = roundMoney(num(req.body?.cost_amount));
      const profit = req.body?.profit_amount === undefined || clean(req.body?.profit_amount) === ''
        ? roundMoney(total - costAmount)
        : roundMoney(num(req.body?.profit_amount));
      const orderId = num(req.body?.order_id);
      let orderRef = clean(req.body?.order_ref);
      let customer = clean(req.body?.customer_name);
      if (orderId) {
        const o = await getAsync(`SELECT id, COALESCE(store_ref,'') store_ref, COALESCE(custName,'') custName FROM orders WHERE id=?`, [orderId]).catch(() => null);
        if (o) { orderRef = orderRef || clean(o.store_ref) || String(o.id); customer = customer || clean(o.custName); }
      }
      const date = clean(req.body?.prod_date) || today();
      const status = clean(req.body?.status) || 'تحت التنفيذ';
      const collected = num(req.body?.collected) ? 1 : 0;
      const note = clean(req.body?.note);
      const vals = [date, jobName, orderId, orderRef, customer, qty, unitPrice, total, costAmount, profit, status, collected, note];

      const syncCash = async (rowId) => {
        await deleteWorkshopCashBySource('workshop_production', rowId);
        if (collected && total > 0) {
          await addWorkshopCashEntry({
            kind: 'production_in', amount: total, date,
            note: `تحصيل إنتاج: ${jobName}${orderRef ? ' — أوردر ' + orderRef : ''}`,
            sourceType: 'workshop_production', sourceRef: rowId, isAuto: 1, createdBy: actorName(req),
          });
        }
        await rebuildWorkshopCashBalances();
      };

      if (id) {
        await runAsync(`UPDATE workshop_production SET prod_date=?,job_name=?,order_id=?,order_ref=?,customer_name=?,qty=?,unit_price=?,total_amount=?,cost_amount=?,profit_amount=?,status=?,collected=?,note=? WHERE id=?`, [...vals, id]);
        await syncCash(id);
        await recordAudit({ req, action: 'save-workshop-production', entity_type: 'workshop_production', entity_id: id, details: `تعديل شغلانة ورشة: ${jobName} (${qty} شنطة)` });
        return res.json({ success: true, id });
      }
      const ins = await runAsync(`INSERT INTO workshop_production (prod_date,job_name,order_id,order_ref,customer_name,qty,unit_price,total_amount,cost_amount,profit_amount,status,collected,note,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [...vals, actorName(req)]);
      await syncCash(ins.lastID);
      await recordAudit({ req, action: 'save-workshop-production', entity_type: 'workshop_production', entity_id: ins.lastID, details: `شغلانة ورشة: ${jobName} (${qty} شنطة) بمكسب ${profit}` });
      res.json({ success: true, id: ins.lastID });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-workshop-production/:id', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.params.id);
      await runAsync(`DELETE FROM workshop_production WHERE id=?`, [id]);
      await deleteWorkshopCashBySource('workshop_production', id);
      await recordAudit({ req, action: 'delete-workshop-production', entity_type: 'workshop_production', entity_id: id, details: 'حذف شغلانة ورشة' });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- رأس مال / خزنة الورشة ---------- */
  app.post('/save-workshop-cash-entry', authRequired, canCapital, async (req, res) => {
    try {
      const kind = clean(req.body?.entry_kind) || 'capital_in';
      if (!['capital_in', 'capital_out', 'adjust_in', 'adjust_out'].includes(kind))
        return res.status(400).json({ error: 'الحركات التلقائية بتتظبط من مكانها مش من هنا' });
      const amount = roundMoney(num(req.body?.amount));
      if (!(amount > 0)) return res.status(400).json({ error: 'اكتب المبلغ' });
      const id = await addWorkshopCashEntry({
        kind, amount, date: clean(req.body?.entry_date) || today(),
        note: clean(req.body?.note), sourceType: 'manual', isAuto: 0, createdBy: actorName(req),
      });
      await rebuildWorkshopCashBalances();
      await recordAudit({ req, action: 'save-workshop-cash-entry', entity_type: 'workshop_cash', entity_id: id, details: `${WORKSHOP_CASH_KINDS[kind].label}: ${amount}` });
      res.json({ success: true, id, balance: await workshopCashBalance() });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-workshop-cash-entry/:id', authRequired, canCapital, async (req, res) => {
    try {
      const id = num(req.params.id);
      const row = await getAsync(`SELECT * FROM workshop_cash_ledger WHERE id=?`, [id]);
      if (!row) return res.status(404).json({ error: 'الحركة مش موجودة' });
      if (num(row.is_auto) === 1) return res.status(400).json({ error: 'الحركة دي تلقائية — امسحها من مكانها الأصلي (سلفة/قبض/مصروف/إنتاج).' });
      await runAsync(`DELETE FROM workshop_cash_ledger WHERE id=?`, [id]);
      await rebuildWorkshopCashBalances();
      res.json({ success: true, balance: await workshopCashBalance() });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- كشف حساب صنايعي ---------- */
  app.get('/workshop-worker-statement/:id', authRequired, canView, async (req, res) => {
    try {
      const id = num(req.params.id);
      const worker = await getAsync(`SELECT * FROM workshop_workers WHERE id=?`, [id]);
      if (!worker) return res.status(404).json({ error: 'الصنايعي مش موجود' });
      const from = clean(req.query.from), to = clean(req.query.to);

      const pOpen = [id];
      let openSql = '';
      if (from) { openSql = ` AND date(work_date) < date(?)`; pOpen.push(from); }
      const openEarned = from ? num((await getAsync(`SELECT COALESCE(SUM(amount),0) v FROM workshop_attendance WHERE worker_id=?${openSql}`, pOpen))?.v) : 0;
      const openAdv = from ? num((await getAsync(`SELECT COALESCE(SUM(amount),0) v FROM workshop_advances WHERE worker_id=? AND date(advance_date) < date(?)`, [id, from]))?.v) : 0;
      const openPaid = from ? num((await getAsync(`SELECT COALESCE(SUM(net_amount),0) v FROM workshop_payouts WHERE worker_id=? AND date(payout_date) < date(?)`, [id, from]))?.v) : 0;
      const opening = roundMoney(openEarned - openAdv - openPaid);

      const pA = [id]; const wA = periodWhere('work_date', from, to, pA);
      const att = await allAsync(`SELECT id, work_date d, amount, pay_type, day_fraction, hours, note FROM workshop_attendance WHERE worker_id=?${wA}`, pA);
      const pB = [id]; const wB = periodWhere('advance_date', from, to, pB);
      const adv = await allAsync(`SELECT id, advance_date d, amount, note FROM workshop_advances WHERE worker_id=?${wB}`, pB);
      const pC = [id]; const wC = periodWhere('payout_date', from, to, pC);
      const pay = await allAsync(`SELECT id, payout_date d, net_amount amount, note FROM workshop_payouts WHERE worker_id=?${wC}`, pC);

      const attDetail = (r) => (payTypeOf(r.pay_type) === 'hourly'
        ? `${roundMoney(num(r.hours))} ساعة`
        : `${num(r.day_fraction, 1)} يوم`) + (r.note ? ' — ' + r.note : '');

      const rows = [
        ...att.map(r => ({ sort: 1, date: r.d, kind: 'مستحق شغل', detail: attDetail(r), credit: roundMoney(num(r.amount)), debit: 0, id: r.id })),
        ...adv.map(r => ({ sort: 2, date: r.d, kind: 'سلفة', detail: r.note || '', credit: 0, debit: roundMoney(num(r.amount)), id: r.id })),
        ...pay.map(r => ({ sort: 3, date: r.d, kind: 'قبض', detail: r.note || '', credit: 0, debit: roundMoney(num(r.amount)), id: r.id })),
      ].sort((a, b) => String(a.date).localeCompare(String(b.date)) || num(a.sort) - num(b.sort) || num(a.id) - num(b.id));

      let running = opening;
      for (const r of rows) { running = roundMoney(running + num(r.credit) - num(r.debit)); r.balance = running; }

      res.json({
        worker: {
          id: worker.id, name: worker.name, job_title: worker.job_title,
          pay_type: payTypeOf(worker.pay_type), pay_type_label: PAY_TYPES[payTypeOf(worker.pay_type)],
          daily_wage: roundMoney(num(worker.daily_wage)), hourly_rate: roundMoney(num(worker.hourly_rate)), phone: worker.phone,
        },
        period_from: from, period_to: to, opening_balance: opening, rows, closing_balance: roundMoney(running),
        totals: {
          earned: roundMoney(rows.filter(r => r.kind === 'مستحق شغل').reduce((a, r) => a + num(r.credit), 0)),
          advances: roundMoney(rows.filter(r => r.kind === 'سلفة').reduce((a, r) => a + num(r.debit), 0)),
          paid: roundMoney(rows.filter(r => r.kind === 'قبض').reduce((a, r) => a + num(r.debit), 0)),
        },
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ---------- تقرير ربح وخسارة الورشة ---------- */
  app.get('/workshop-profit-report', authRequired, canView, async (req, res) => {
    try {
      if (!(hasPerm(req.user, 'perm_view_workshop_totals') || hasPerm(req.user, 'perm_manage_workshop')))
        return res.status(403).json({ error: 'مالكش صلاحية على إجماليات الورشة' });
      const from = clean(req.query.from), to = clean(req.query.to);
      const pA = []; const wA = periodWhere('work_date', from, to, pA).replace(/^ AND/, ' WHERE');
      const labour = num((await getAsync(`SELECT COALESCE(SUM(amount),0) v FROM workshop_attendance${wA}`, pA))?.v);
      const pB = []; const wB = periodWhere('cost_date', from, to, pB).replace(/^ AND/, ' WHERE');
      const costs = num((await getAsync(`SELECT COALESCE(SUM(total_amount),0) v FROM workshop_costs${wB}`, pB))?.v);
      const pC = []; const wC = periodWhere('prod_date', from, to, pC).replace(/^ AND/, ' WHERE');
      const prodRow = await getAsync(`SELECT COALESCE(SUM(total_amount),0) revenue, COALESCE(SUM(profit_amount),0) declared, COALESCE(SUM(qty),0) qty, COUNT(*) jobs FROM workshop_production${wC}`, pC);
      const byType = await allAsync(`SELECT cost_type t, COALESCE(SUM(total_amount),0) v, COUNT(*) c FROM workshop_costs${wB} GROUP BY cost_type ORDER BY v DESC`, pB);
      const revenue = roundMoney(num(prodRow?.revenue));
      const operating = roundMoney(labour + costs);
      res.json({
        period_from: from, period_to: to,
        revenue, jobs: num(prodRow?.jobs), bags: roundMoney(num(prodRow?.qty)),
        labour_cost: roundMoney(labour), costs_total: roundMoney(costs),
        operating_cost: operating,
        net_profit: roundMoney(revenue - operating),
        declared_profit: roundMoney(num(prodRow?.declared)),
        margin_pct: revenue > 0 ? roundMoney(((revenue - operating) / revenue) * 100) : 0,
        cost_breakdown: byType.map(r => ({ type: clean(r.t) || 'متنوع', total: roundMoney(num(r.v)), count: num(r.c) })),
        cash_balance: await workshopCashBalance(),
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ===================== 6) فحوصات الورشة للـ health check ===================== */
  async function workshopHealthChecks() {
    const issues = [];
    const push = (severity, title, details, data = {}) => issues.push({ severity, title, details, data: { module: 'workshop', ...data } });

    const orphanAtt = await allAsync(`SELECT a.id, a.worker_id FROM workshop_attendance a LEFT JOIN workshop_workers w ON w.id=a.worker_id WHERE w.id IS NULL LIMIT 200`);
    if (orphanAtt.length) push('error', 'يوميات ورشة مرتبطة بصنايعي محذوف', `${orphanAtt.length} سطر يومية بدون صنايعي.`, { repair_mode: 'auto', repair_key: 'workshop_orphan_attendance', affected_count: orphanAtt.length });

    const orphanAdv = await allAsync(`SELECT a.id FROM workshop_advances a LEFT JOIN workshop_workers w ON w.id=a.worker_id WHERE w.id IS NULL LIMIT 200`);
    if (orphanAdv.length) push('error', 'سلف ورشة مرتبطة بصنايعي محذوف', `${orphanAdv.length} سلفة بدون صنايعي.`, { repair_mode: 'auto', repair_key: 'workshop_orphan_advances', affected_count: orphanAdv.length });

    const orphanPay = await allAsync(`SELECT p.id FROM workshop_payouts p LEFT JOIN workshop_workers w ON w.id=p.worker_id WHERE w.id IS NULL LIMIT 200`);
    if (orphanPay.length) push('error', 'قبض ورشة مرتبط بصنايعي محذوف', `${orphanPay.length} عملية قبض بدون صنايعي.`, { repair_mode: 'auto', repair_key: 'workshop_orphan_payouts', affected_count: orphanPay.length });

    // حركات كاش تلقائية بدون مصدر
    const orphanCash = await allAsync(
      `SELECT c.id, c.source_type, c.source_ref FROM workshop_cash_ledger c WHERE c.is_auto=1 AND (
         (c.source_type='workshop_advance'    AND NOT EXISTS (SELECT 1 FROM workshop_advances   x WHERE x.id=CAST(c.source_ref AS INTEGER))) OR
         (c.source_type='workshop_payout'     AND NOT EXISTS (SELECT 1 FROM workshop_payouts    x WHERE x.id=CAST(c.source_ref AS INTEGER))) OR
         (c.source_type='workshop_cost'       AND NOT EXISTS (SELECT 1 FROM workshop_costs      x WHERE x.id=CAST(c.source_ref AS INTEGER))) OR
         (c.source_type='workshop_production' AND NOT EXISTS (SELECT 1 FROM workshop_production x WHERE x.id=CAST(c.source_ref AS INTEGER)))
       ) LIMIT 200`);
    if (orphanCash.length) push('error', 'حركات خزنة ورشة بدون مصدر', `${orphanCash.length} حركة تلقائية مصدرها اتمسح.`, { repair_mode: 'auto', repair_key: 'workshop_orphan_cash', affected_count: orphanCash.length });

    // حركة ناقصة في الخزنة لكل سلفة/قبض/مصروف
    for (const [table, srcType, amountField, label] of [
      ['workshop_advances', 'workshop_advance', 'amount', 'سلفة'],
      ['workshop_payouts', 'workshop_payout', 'net_amount', 'قبض'],
      ['workshop_costs', 'workshop_cost', 'total_amount', 'مصروف ورشة'],
    ]) {
      const missing = await allAsync(
        `SELECT x.id, x.${amountField} amount FROM ${table} x
         WHERE NOT EXISTS (SELECT 1 FROM workshop_cash_ledger c WHERE c.source_type=? AND c.source_ref=CAST(x.id AS TEXT))
         LIMIT 200`, [srcType]);
      if (missing.length) push('warn', `${label} مش متسجل في خزنة الورشة`, `${missing.length} حركة ${label} مش موجودة في الخزنة.`, { repair_mode: 'auto', repair_key: 'workshop_missing_cash', affected_count: missing.length });
    }

    // شغلانة متعلّم عليها «اتحصلت» ومفيش حركة تحصيل في الخزنة
    const missingProd = await allAsync(
      `SELECT p.id FROM workshop_production p WHERE p.collected=1 AND p.total_amount>0
       AND NOT EXISTS (SELECT 1 FROM workshop_cash_ledger c WHERE c.source_type='workshop_production' AND c.source_ref=CAST(p.id AS TEXT)) LIMIT 200`);
    if (missingProd.length) push('warn', 'تحصيل إنتاج مش متسجل في خزنة الورشة', `${missingProd.length} شغلانة متعلّم عليها إنها اتحصلت ومفيش حركة في الخزنة.`, { repair_mode: 'auto', repair_key: 'workshop_missing_cash', affected_count: missingProd.length });

    // شغلانة اتحصلت في الخزنة وهي مش متعلّم عليها
    const staleProd = await allAsync(
      `SELECT c.id FROM workshop_cash_ledger c WHERE c.source_type='workshop_production'
       AND EXISTS (SELECT 1 FROM workshop_production p WHERE p.id=CAST(c.source_ref AS INTEGER) AND p.collected=0) LIMIT 200`);
    if (staleProd.length) push('warn', 'تحصيل إنتاج في الخزنة لشغلانة مش متحصلة', `${staleProd.length} حركة تحصيل المفروض تتشال.`, { repair_mode: 'auto', repair_key: 'workshop_stale_production_cash', affected_count: staleProd.length });

    // الأرصدة الجارية غلط
    const ledger = await allAsync(`SELECT id, delta, balance_after FROM workshop_cash_ledger ORDER BY date(entry_date) ASC, id ASC`);
    let running = 0, drift = 0;
    for (const row of ledger) { running = roundMoney(running + num(row.delta)); if (Math.abs(running - num(row.balance_after)) > 0.01) drift++; }
    if (drift) push('warn', 'أرصدة خزنة الورشة محتاجة إعادة بناء', `${drift} سطر رصيده الجاري مش مظبوط.`, { repair_mode: 'auto', repair_key: 'workshop_rebuild_cash', affected_count: drift });

    // صنايعي مقبوض أكتر من المستحق
    const workers = await allAsync(`SELECT id, name FROM workshop_workers`);
    for (const w of workers) {
      const t = await workerTotals(w.id, {});
      if (t.balance < -0.5) push('warn', 'صنايعي أخد أكتر من مستحقاته', `${w.name}: أخد بزيادة ${Math.abs(t.balance).toLocaleString('en-US')} جنيه (مستحق ${t.earned} — سلف ${t.advances} — قبض ${t.paid}).`, { repair_mode: 'manual', action_hint: 'راجع اليوميات أو سجّل يومية ناقصة للصنايعي ده.', worker_id: w.id });
    }

    // صنايعي بالساعة من غير سعر ساعة (أو باليومية من غير يومية)
    const badRate = await allAsync(`SELECT id, name, pay_type FROM workshop_workers WHERE is_active=1 AND ((pay_type='hourly' AND COALESCE(hourly_rate,0)<=0) OR (pay_type<>'hourly' AND COALESCE(daily_wage,0)<=0)) LIMIT 100`);
    if (badRate.length) push('warn', 'صنايعية من غير سعر محدد', `${badRate.map(w => w.name).join('، ')} — محتاجين يومية أو سعر ساعة في ملفهم.`, { repair_mode: 'manual', action_hint: 'عدّل الصنايعي وحط اليومية أو سعر الساعة.' });

    // يومية بمبلغ صفر
    const zeroAtt = await allAsync(`SELECT id, worker_name, work_date FROM workshop_attendance WHERE amount<=0 LIMIT 100`);
    if (zeroAtt.length) push('warn', 'يوميات ورشة بمبلغ صفر', `${zeroAtt.length} يومية متسجلة بصفر — غالبًا السعر مش متحط على الصنايعي.`, { repair_mode: 'auto', repair_key: 'workshop_zero_attendance', affected_count: zeroAtt.length });

    return issues;
  }

  async function workshopRepair(repairKey) {
    const done = [];
    if (!repairKey || repairKey === 'workshop_orphan_attendance') {
      const r = await runAsync(`DELETE FROM workshop_attendance WHERE worker_id NOT IN (SELECT id FROM workshop_workers)`);
      if (r.changes) done.push(`مسح ${r.changes} يومية بدون صنايعي`);
    }
    if (!repairKey || repairKey === 'workshop_orphan_advances') {
      const r = await runAsync(`DELETE FROM workshop_advances WHERE worker_id NOT IN (SELECT id FROM workshop_workers)`);
      if (r.changes) done.push(`مسح ${r.changes} سلفة بدون صنايعي`);
    }
    if (!repairKey || repairKey === 'workshop_orphan_payouts') {
      const r = await runAsync(`DELETE FROM workshop_payouts WHERE worker_id NOT IN (SELECT id FROM workshop_workers)`);
      if (r.changes) done.push(`مسح ${r.changes} قبض بدون صنايعي`);
    }
    if (!repairKey || repairKey === 'workshop_orphan_cash') {
      const r = await runAsync(
        `DELETE FROM workshop_cash_ledger WHERE is_auto=1 AND (
           (source_type='workshop_advance'    AND CAST(source_ref AS INTEGER) NOT IN (SELECT id FROM workshop_advances)) OR
           (source_type='workshop_payout'     AND CAST(source_ref AS INTEGER) NOT IN (SELECT id FROM workshop_payouts)) OR
           (source_type='workshop_cost'       AND CAST(source_ref AS INTEGER) NOT IN (SELECT id FROM workshop_costs)) OR
           (source_type='workshop_production' AND CAST(source_ref AS INTEGER) NOT IN (SELECT id FROM workshop_production)))`);
      if (r.changes) done.push(`مسح ${r.changes} حركة خزنة ورشة بدون مصدر`);
    }
    if (!repairKey || repairKey === 'workshop_stale_production_cash') {
      const r = await runAsync(
        `DELETE FROM workshop_cash_ledger WHERE source_type='workshop_production'
         AND CAST(source_ref AS INTEGER) IN (SELECT id FROM workshop_production WHERE collected=0)`);
      if (r.changes) done.push(`مسح ${r.changes} حركة تحصيل إنتاج ملغية`);
    }
    if (!repairKey || repairKey === 'workshop_missing_cash') {
      let added = 0;
      for (const [table, srcType, amountField, dateField, kind, label] of [
        ['workshop_advances', 'workshop_advance', 'amount', 'advance_date', 'advance', 'سلفة'],
        ['workshop_payouts', 'workshop_payout', 'net_amount', 'payout_date', 'payout', 'قبض'],
        ['workshop_costs', 'workshop_cost', 'total_amount', 'cost_date', 'cost', 'مصروف ورشة'],
      ]) {
        const missing = await allAsync(
          `SELECT x.id, x.${amountField} amount, x.${dateField} d FROM ${table} x
           WHERE NOT EXISTS (SELECT 1 FROM workshop_cash_ledger c WHERE c.source_type=? AND c.source_ref=CAST(x.id AS TEXT))`, [srcType]);
        for (const row of missing) {
          if (!(num(row.amount) > 0)) continue;
          await addWorkshopCashEntry({ kind, amount: row.amount, date: row.d, note: `${label} (إصلاح تلقائي)`, sourceType: srcType, sourceRef: row.id, isAuto: 1, createdBy: 'system' });
          added++;
        }
      }
      const missingProd = await allAsync(
        `SELECT p.id, p.total_amount amount, p.prod_date d, p.job_name FROM workshop_production p
         WHERE p.collected=1 AND p.total_amount>0
         AND NOT EXISTS (SELECT 1 FROM workshop_cash_ledger c WHERE c.source_type='workshop_production' AND c.source_ref=CAST(p.id AS TEXT))`);
      for (const row of missingProd) {
        await addWorkshopCashEntry({ kind: 'production_in', amount: row.amount, date: row.d, note: `تحصيل إنتاج: ${row.job_name} (إصلاح تلقائي)`, sourceType: 'workshop_production', sourceRef: row.id, isAuto: 1, createdBy: 'system' });
        added++;
      }
      if (added) done.push(`تسجيل ${added} حركة ناقصة في خزنة الورشة`);
    }
    if (!repairKey || repairKey === 'workshop_zero_attendance') {
      const rows = await allAsync(`SELECT a.id, a.pay_type, a.day_fraction, a.hours, a.extra_amount, a.deduction_amount, w.daily_wage, w.hourly_rate FROM workshop_attendance a JOIN workshop_workers w ON w.id=a.worker_id WHERE a.amount<=0`);
      let fixed = 0;
      for (const row of rows) {
        const payType = payTypeOf(row.pay_type);
        const amount = computeAttendanceAmount({
          payType,
          fraction: num(row.day_fraction, 1), wage: num(row.daily_wage),
          hours: num(row.hours), rate: num(row.hourly_rate),
          extra: num(row.extra_amount), deduction: num(row.deduction_amount),
        });
        if (amount > 0) {
          await runAsync(`UPDATE workshop_attendance SET daily_wage=?, hourly_rate=?, amount=? WHERE id=?`,
            [roundMoney(num(row.daily_wage)), roundMoney(num(row.hourly_rate)), amount, row.id]);
          fixed++;
        }
      }
      if (fixed) done.push(`تصحيح ${fixed} يومية كانت بصفر`);
    }
    // دايمًا نعيد بناء الأرصدة في الآخر
    await rebuildWorkshopCashBalances();
    if (repairKey === 'workshop_rebuild_cash') done.push('إعادة بناء أرصدة خزنة الورشة');
    return done;
  }

  return { ensureWorkshopSchema, workshopHealthChecks, workshopRepair, workshopCashBalance, rebuildWorkshopCashBalances };
}

module.exports = { register, WORKSHOP_CASH_KINDS, WORKSHOP_DEFAULT_COST_TYPES, PAY_TYPES };
