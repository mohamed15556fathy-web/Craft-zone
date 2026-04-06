
const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const crypto = require('crypto');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const os = require('os');
const archiver = require('archiver');
const unzipper = require('unzipper');

const app = express();
const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, 'public');
const DEFAULT_DATA_DIR = process.env.RAILWAY_ENVIRONMENT ? '/app/data' : path.join(ROOT, 'data');
const DATA_DIR = path.resolve(process.env.APP_DATA_DIR || process.env.DATA_DIR || DEFAULT_DATA_DIR);
const DB_PATH = path.resolve(process.env.DATABASE_PATH || path.join(DATA_DIR, 'database.db'));
const UPLOAD_DIR = path.resolve(process.env.UPLOAD_DIR || path.join(DATA_DIR, 'uploads'));
const BACKUP_DIR = path.resolve(process.env.BACKUP_DIR || path.join(DATA_DIR, 'backups'));
const BOSTA_BASE_URL = String(process.env.BOSTA_BASE_URL || 'https://app.bosta.co/api/v2').replace(/\/+$/, '');
const BOSTA_API_KEY = String(process.env.BOSTA_API_KEY || '').trim();
const BOSTA_ENABLED = !!BOSTA_API_KEY;
const BOSTA_WEBHOOK_PATH = String(process.env.BOSTA_WEBHOOK_PATH || '/bosta-webhook').trim() || '/bosta-webhook';
const BOSTA_WEBHOOK_AUTH = String(process.env.BOSTA_WEBHOOK_AUTH || BOSTA_API_KEY || 'bosta-webhook').trim();
const BOSTA_WEBHOOK_TOKEN = crypto.createHash('sha256').update(String(BOSTA_WEBHOOK_AUTH || 'bosta-webhook')).digest('hex').slice(0, 24);
const SESSION_TTL_HOURS = Math.max(1, Number(process.env.SESSION_TTL_HOURS || 12) || 12);
const SESSION_TTL_MS = SESSION_TTL_HOURS * 60 * 60 * 1000;
const PASSWORD_HASH_PREFIX = 'scrypt$';

function appendUrlQueryParam(url='', key='', value='') {
  const rawUrl = String(url || '').trim();
  const queryKey = String(key || '').trim();
  const queryValue = String(value || '').trim();
  if (!rawUrl || !queryKey || !queryValue) return rawUrl;
  const matcher = new RegExp(`([?&])${queryKey}=`);
  if (matcher.test(rawUrl)) return rawUrl;
  const separator = rawUrl.includes('?') ? '&' : '?';
  return `${rawUrl}${separator}${encodeURIComponent(queryKey)}=${encodeURIComponent(queryValue)}`;
}
function resolveBaseUrl(req) {
  const protoHeader = String(req?.headers?.['x-forwarded-proto'] || req?.protocol || 'https').split(',')[0].trim();
  const hostHeader = String(req?.headers?.['x-forwarded-host'] || req?.headers?.host || '').split(',')[0].trim();
  if (!hostHeader) return '';
  const proto = protoHeader || 'https';
  return `${proto}://${hostHeader}`.replace(/\/+$/, '');
}
function resolveBostaWebhookUrl(req) {
  const configured = String(process.env.BOSTA_WEBHOOK_URL || '').trim();
  if (configured) return appendUrlQueryParam(configured, 'bosta_sig', BOSTA_WEBHOOK_TOKEN);
  const base = resolveBaseUrl(req);
  if (!base) return '';
  const suffix = String(BOSTA_WEBHOOK_PATH || '/bosta-webhook').startsWith('/') ? BOSTA_WEBHOOK_PATH : `/${BOSTA_WEBHOOK_PATH}`;
  return appendUrlQueryParam(`${base}${suffix}`, 'bosta_sig', BOSTA_WEBHOOK_TOKEN);
}
function bostaStateLabel(state) {
  const n = Number(state);
  return ({
    10:'جديد لدى بوسطة',
    20:'تم تعيين مندوب',
    21:'تم الاستلام من التاجر',
    24:'وصل لمخزن بوسطة',
    30:'في النقل بين المخازن',
    41:'خرج للتسليم',
    45:'تسليم ناجح',
    46:'مرتجع إلى التاجر',
    47:'استثناء',
    48:'تم الإنهاء',
    49:'تم الإلغاء',
    60:'مرتجع للمخزن',
    100:'مفقود',
    101:'تالف',
    102:'قيد التحقيق',
    103:'بانتظار إجراء منك',
    104:'مؤرشف',
    105:'معلّق'
  })[n] || (String(state || '').trim() || '');
}
function bostaWebhookTargetStatus(state) {
  const n = Number(state);
  if (n === 45) return 'تم التسليم';
  if (n === 46 || n === 60) return 'مرتجع';
  return '';
}

function ensureBostaConfigured() {
  if (!BOSTA_ENABLED) {
    const err = new Error('BOSTA_API_KEY غير مضبوط في Environment Variables');
    err.code = 'BOSTA_CONFIG_MISSING';
    throw err;
  }
}

function ensureDirSync(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
function copyDirSync(src, dest) {
  if (!fs.existsSync(src)) return;
  fs.cpSync(src, dest, { recursive: true, force: true });
}
function migrateLegacyData() {
  ensureDirSync(DATA_DIR);
  const legacyDb = path.join(ROOT, 'database.db');
  if (!fs.existsSync(DB_PATH) && fs.existsSync(legacyDb)) fs.copyFileSync(legacyDb, DB_PATH);
  const legacyUploads = path.join(ROOT, 'uploads');
  if (!fs.existsSync(UPLOAD_DIR) && fs.existsSync(legacyUploads)) copyDirSync(legacyUploads, UPLOAD_DIR);
  const legacyBackups = path.join(ROOT, 'backups');
  if (!fs.existsSync(BACKUP_DIR) && fs.existsSync(legacyBackups)) copyDirSync(legacyBackups, BACKUP_DIR);
}
function openDatabaseConnection() {
  return new sqlite3.Database(DB_PATH);
}

migrateLegacyData();
ensureDirSync(DATA_DIR);
ensureDirSync(UPLOAD_DIR);
ensureDirSync(BACKUP_DIR);

let db = openDatabaseConnection();
const upload = multer({ dest: UPLOAD_DIR });
const backupImportUpload = multer({ dest: BACKUP_DIR });

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
if (fs.existsSync(PUBLIC_DIR)) app.use(express.static(PUBLIC_DIR));

const SAFE_ROOT_FILE_EXTS = new Set(['.html', '.js', '.css', '.json']);
const SAFE_ROOT_FILES = new Set(
  fs.readdirSync(ROOT)
    .filter(name => {
      const ext = path.extname(name).toLowerCase();
      const lower = String(name || '').toLowerCase();
      if (!SAFE_ROOT_FILE_EXTS.has(ext)) return false;
      if (['server.js', 'package.json', '.env', '.env.example'].includes(lower)) return false;
      if (lower.endsWith('.bak') || lower.endsWith('.prepatch') || lower.endsWith('.jscheck.js')) return false;
      return fs.statSync(path.join(ROOT, name)).isFile();
    })
);
app.get('/', (req, res) => {
  const landing = fs.existsSync(path.join(ROOT, 'login.html')) ? 'login.html' : 'index.html';
  res.sendFile(path.join(ROOT, landing));
});
app.get('/:filename', (req, res, next) => {
  const filename = path.basename(String(req.params.filename || '').trim());
  if (!SAFE_ROOT_FILES.has(filename)) return next();
  res.sendFile(path.join(ROOT, filename));
});

function runAsync(sql, params = []) {
  return new Promise((resolve, reject) => db.run(sql, params, function (err) { err ? reject(err) : resolve(this); }));
}
function getAsync(sql, params = []) {
  return new Promise((resolve, reject) => db.get(sql, params, (err, row) => err ? reject(err) : resolve(row)));
}
function allAsync(sql, params = []) {
  return new Promise((resolve, reject) => db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows)));
}
async function tableColumns(table) {
  const rows = await allAsync(`PRAGMA table_info(${table})`);
  return rows.map(r => r.name);
}
async function addColumnIfMissing(table, name, def) {
  const exists = await getAsync(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, [table]);
  if (!exists) return;
  const cols = await tableColumns(table);
  if (!cols.includes(name)) await runAsync(`ALTER TABLE ${table} ADD COLUMN ${name} ${def}`);
}

function safeJsonParse(textValue, fallback = null) {
  try {
    if (textValue == null || textValue === '') return fallback;
    return JSON.parse(String(textValue));
  } catch (_) {
    return fallback;
  }
}
function safeJsonStringify(value, fallback = '[]') {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return fallback;
  }
}
function quoteIdent(name='') {
  return `"${String(name || '').replace(/"/g, '""')}"`;
}
function uniqueList(values = []) {
  return [...new Set((Array.isArray(values) ? values : [values]).map(v => typeof v === 'string' ? v.trim() : v).filter(v => !(v === undefined || v === null || v === '')) )];
}
function entityTouchRef(type='', id=null) {
  const kind = String(type || '').trim();
  const value = id == null ? '' : String(id).trim();
  return kind && value ? `${kind}:${value}` : '';
}
function normalizeTouchRefs(refs = [], entityType = '', entityId = null) {
  const extra = entityTouchRef(entityType, entityId);
  return uniqueList([...(Array.isArray(refs) ? refs : (safeJsonParse(refs, []) || [])), ...(extra ? [extra] : [])]);
}
function criteriaEq(column, value) {
  return [{ column: String(column || '').trim(), op: 'eq', value }];
}
function criteriaIn(column, values = []) {
  return [{ column: String(column || '').trim(), op: 'in', values: uniqueList(values) }];
}
function normalizeCriteria(criteria = []) {
  return (Array.isArray(criteria) ? criteria : []).map(cond => ({
    column: String(cond?.column || '').trim(),
    op: String(cond?.op || 'eq').trim() === 'in' ? 'in' : 'eq',
    value: cond?.value,
    values: uniqueList(cond?.values || [])
  })).filter(cond => cond.column);
}
function buildCriteriaWhere(criteria = []) {
  const normalized = normalizeCriteria(criteria);
  if (!normalized.length) return { sql: '1=0', params: [] };
  const parts = [];
  const params = [];
  for (const cond of normalized) {
    const col = quoteIdent(cond.column);
    if (cond.op === 'in') {
      if (!cond.values.length) {
        parts.push('1=0');
      } else {
        parts.push(`${col} IN (${cond.values.map(() => '?').join(',')})`);
        params.push(...cond.values);
      }
      continue;
    }
    parts.push(`${col} = ?`);
    params.push(cond.value);
  }
  return { sql: parts.join(' AND ') || '1=0', params };
}
async function snapshotTableSubset(table, criteria = []) {
  const name = String(table || '').trim();
  const normalized = normalizeCriteria(criteria);
  if (!name || !normalized.length) return null;
  const { sql, params } = buildCriteriaWhere(normalized);
  const rows = await allAsync(`SELECT * FROM ${quoteIdent(name)} WHERE ${sql}`, params);
  return { table: name, criteria: normalized, rows };
}
async function buildUndoPayloadFromDefs(defs = []) {
  const items = [];
  for (const def of (Array.isArray(defs) ? defs : [])) {
    const item = await snapshotTableSubset(def?.table, def?.criteria || []);
    if (item) items.push(item);
  }
  return items;
}
function emptyUndoPayloadFromDefs(defs = []) {
  return (Array.isArray(defs) ? defs : []).map(def => ({
    table: String(def?.table || '').trim(),
    criteria: normalizeCriteria(def?.criteria || []),
    rows: []
  })).filter(item => item.table && item.criteria.length);
}
async function insertSnapshotRow(table, row = {}) {
  const name = String(table || '').trim();
  if (!name || !row || typeof row !== 'object') return;
  const cols = await tableColumns(name);
  const keys = Object.keys(row).filter(key => cols.includes(key));
  if (!keys.length) return;
  const sql = `INSERT INTO ${quoteIdent(name)} (${keys.map(quoteIdent).join(',')}) VALUES (${keys.map(() => '?').join(',')})`;
  await runAsync(sql, keys.map(key => row[key]));
}
async function restoreUndoPayload(payload = []) {
  const items = Array.isArray(payload) ? payload.filter(item => String(item?.table || '').trim() && normalizeCriteria(item?.criteria || []).length) : [];
  for (const item of [...items].reverse()) {
    const { sql, params } = buildCriteriaWhere(item.criteria || []);
    await runAsync(`DELETE FROM ${quoteIdent(item.table)} WHERE ${sql}`, params);
  }
  for (const item of items) {
    for (const row of (Array.isArray(item.rows) ? item.rows : [])) {
      await insertSnapshotRow(item.table, row);
    }
  }
}
function orderSnapshotDefs(orderIds = []) {
  const ids = uniqueList(orderIds).filter(Boolean);
  if (!ids.length) return [];
  return [
    { table: 'orders', criteria: criteriaIn('id', ids) },
    { table: 'order_status_history', criteria: criteriaIn('order_id', ids) },
    { table: 'order_payments', criteria: criteriaIn('order_id', ids) },
    { table: 'cost_history', criteria: criteriaIn('order_id', ids) },
    { table: 'sales_history', criteria: criteriaIn('order_id', ids) },
    { table: 'order_files', criteria: criteriaIn('order_id', ids) },
    { table: 'order_operations', criteria: criteriaIn('order_id', ids) },
    { table: 'partner_payments', criteria: criteriaIn('order_id', ids) }
  ];
}
function paperSnapshotDefs(paperIds = []) {
  const ids = uniqueList(paperIds).filter(Boolean);
  if (!ids.length) return [];
  return [
    { table: 'paper', criteria: criteriaIn('id', ids) },
    { table: 'paper_history', criteria: criteriaIn('paper_id', ids) }
  ];
}
function bagSnapshotDefs(bagIds = []) {
  const ids = uniqueList(bagIds).filter(Boolean);
  if (!ids.length) return [];
  return [
    { table: 'bags', criteria: criteriaIn('id', ids) },
    { table: 'bags_history', criteria: criteriaIn('bag_id', ids) }
  ];
}
function handleSnapshotDefs(handleIds = []) {
  const ids = uniqueList(handleIds).filter(Boolean);
  if (!ids.length) return [];
  return [
    { table: 'handles', criteria: criteriaIn('id', ids) },
    { table: 'handles_history', criteria: criteriaIn('handle_id', ids) }
  ];
}
function supplierSnapshotDefs(supplierIds = []) {
  const ids = uniqueList(supplierIds).filter(Boolean);
  if (!ids.length) return [];
  return [{ table: 'suppliers', criteria: criteriaIn('id', ids) }];
}
function purchaseSnapshotDefs(purchaseIds = []) {
  const ids = uniqueList(purchaseIds).filter(Boolean);
  if (!ids.length) return [];
  return [
    { table: 'purchases', criteria: criteriaIn('id', ids) },
    { table: 'purchase_payments', criteria: criteriaIn('purchase_id', ids) }
  ];
}
function orderTouchRefs(orderIds = []) {
  return uniqueList(orderIds).filter(Boolean).map(id => entityTouchRef('order', id)).filter(Boolean);
}
function paperTouchRefs(paperIds = []) {
  return uniqueList(paperIds).filter(Boolean).map(id => entityTouchRef('paper', id)).filter(Boolean);
}
function bagTouchRefs(bagIds = []) {
  return uniqueList(bagIds).filter(Boolean).map(id => entityTouchRef('bag', id)).filter(Boolean);
}
function handleTouchRefs(handleIds = []) {
  return uniqueList(handleIds).filter(Boolean).map(id => entityTouchRef('handle', id)).filter(Boolean);
}
function supplierTouchRefs(supplierIds = []) {
  return uniqueList(supplierIds).filter(Boolean).map(id => entityTouchRef('supplier', id)).filter(Boolean);
}
function purchaseTouchRefs(purchaseIds = []) {
  return uniqueList(purchaseIds).filter(Boolean).map(id => entityTouchRef('purchase', id)).filter(Boolean);
}

function expenseSnapshotDefs(expenseIds = []) {
  const ids = uniqueList(expenseIds).filter(Boolean);
  if (!ids.length) return [];
  return [
    { table: 'expenses', criteria: criteriaIn('id', ids) },
    { table: 'partner_fund_ledger', criteria: criteriaIn('source_ref', ids.map(id => String(id)).filter(Boolean)).concat(criteriaEq('source_type', 'expense')) },
    { table: 'admin_cash_ledger', criteria: criteriaIn('source_ref', ids.map(id => String(id)).filter(Boolean)).concat(criteriaEq('source_type', 'expense')) },
    { table: 'cost_history', criteria: criteriaIn('source_ref', ids.map(id => String(id)).filter(Boolean)).concat(criteriaEq('source', 'accounts')) }
  ];
}
function cashAdjustmentSnapshotDefs(ids = []) {
  const values = uniqueList(ids).filter(Boolean);
  if (!values.length) return [];
  return [{ table: 'cash_adjustments', criteria: criteriaIn('id', values) }];
}
function adminCashSnapshotDefs(ids = []) {
  const values = uniqueList(ids).filter(Boolean);
  if (!values.length) return [];
  return [{ table: 'admin_cash_ledger', criteria: criteriaIn('id', values) }];
}
function adminCashSourceSnapshotDefs(sourceRef='') {
  const ref = String(sourceRef || '').trim();
  if (!ref) return [];
  return [{ table: 'admin_cash_ledger', criteria: criteriaEq('source_ref', ref) }];
}
function partnerFundTouchRefs(partners = []) {
  return uniqueList(partners).map(name => normalizePartnerName(name) || String(name || '').trim()).filter(Boolean).map(name => entityTouchRef('partnerfund', name)).filter(Boolean);
}
function adminCashTouchRefs(usernames = []) {
  return uniqueList(usernames).map(v => normalizeActorUsername(v)).filter(Boolean).map(v => entityTouchRef('admincash', v)).filter(Boolean);
}
function currentCashTouchRefs() {
  return [entityTouchRef('currentcash', 'main')];
}
async function firstBlockingAuditLog(logRow) {
  const refs = normalizeTouchRefs(logRow?.touch_refs || '[]');
  if (!refs.length) return null;
  const laterRows = await allAsync(`SELECT id, action, entity_type, entity_id, touch_refs, created_at FROM audit_logs WHERE id > ? AND COALESCE(reverted_at,'') = '' ORDER BY id ASC`, [num(logRow?.id)]);
  for (const row of laterRows) {
    const otherRefs = normalizeTouchRefs(row?.touch_refs || '[]');
    if (otherRefs.some(ref => refs.includes(ref))) return row;
  }
  return null;
}
function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}
function today() {
  return new Date().toISOString().slice(0, 10);
}
function normalizePartnerName(value='') {
  const name = String(value || '').trim().replace(/\s+/g, ' ');
  if (!name) return '';
  if (name.includes('عبدالقادر') || name.includes('عبد القادر')) return 'عبدالقادر';
  if (name.includes('محمد')) return 'محمد';
  return name;
}
const TRACKED_PARTNERS = ['محمد','عبدالقادر'];
function isTrackedPartner(value='') { return TRACKED_PARTNERS.includes(normalizePartnerName(value)); }
async function getPartnerFundBalance(partnerName='') {
  const name = normalizePartnerName(partnerName);
  if (!isTrackedPartner(name)) return 0;
  const row = await getAsync(`SELECT COALESCE(MAX(balance_after),0) bal FROM partner_fund_ledger WHERE partner_name=?`, [name]);
  if (row && Number.isFinite(Number(row.bal))) return roundMoney(num(row.bal));
  const sum = await getAsync(`SELECT COALESCE(SUM(delta),0) bal FROM partner_fund_ledger WHERE partner_name=?`, [name]);
  return roundMoney(num(sum?.bal));
}
async function deletePartnerFundEntriesBySource(sourceType='', sourceRef='') {
  const rows = await allAsync(`SELECT id FROM partner_fund_ledger WHERE source_type=? AND source_ref=? ORDER BY id DESC`, [String(sourceType||''), String(sourceRef||'')]);
  if (!rows.length) return;
  await runAsync(`DELETE FROM partner_fund_ledger WHERE source_type=? AND source_ref=?`, [String(sourceType||''), String(sourceRef||'')]);
  await rebuildPartnerFundBalances();
}
async function addPartnerFundEntry({ partner_name='', entry_date='', entry_kind='add', amount=0, delta=null, note='', source_type='', source_ref='', is_auto=0, created_by='' } = {}) {
  const partnerName = normalizePartnerName(partner_name);
  if (!isTrackedPartner(partnerName)) return null;
  const cleanAmount = roundMoney(Math.abs(num(amount)));
  let signedDelta = delta===null ? cleanAmount : roundMoney(num(delta));
  if (entry_kind === 'sub' || entry_kind === 'expense' || entry_kind === 'order_cost') signedDelta = -Math.abs(signedDelta || cleanAmount);
  if (entry_kind === 'add') signedDelta = Math.abs(signedDelta || cleanAmount);
  if (cleanAmount <= 0 && Math.abs(signedDelta) <= 0) return null;
  const balanceBefore = await getPartnerFundBalance(partnerName);
  const balanceAfter = roundMoney(balanceBefore + signedDelta);
  const ins = await runAsync(`INSERT INTO partner_fund_ledger (partner_name,entry_date,entry_kind,amount,delta,balance_before,balance_after,note,source_type,source_ref,is_auto,created_at,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, [partnerName, String(entry_date || today()).trim() || today(), String(entry_kind || 'add').trim() || 'add', cleanAmount || Math.abs(signedDelta), signedDelta, balanceBefore, balanceAfter, String(note || '').trim(), String(source_type || '').trim(), String(source_ref || '').trim(), num(is_auto,0)?1:0, new Date().toISOString(), String(created_by || '').trim()]);
  return ins.lastID;
}
async function rebuildPartnerFundBalances() {
  const rows = await allAsync(`SELECT * FROM partner_fund_ledger ORDER BY partner_name ASC, entry_date ASC, id ASC`);
  const balances = {};
  for (const row of rows) {
    const partnerName = normalizePartnerName(row.partner_name);
    const before = roundMoney(num(balances[partnerName]));
    const after = roundMoney(before + num(row.delta));
    balances[partnerName] = after;
    await runAsync(`UPDATE partner_fund_ledger SET balance_before=?, balance_after=? WHERE id=?`, [before, after, row.id]);
  }
}
async function syncExpensePartnerFund({ expenseId=0, partnerName='', amount=0, expenseDate='', linkedToOrder=0, notes='', createdBy='' } = {}) {
  await deletePartnerFundEntriesBySource('expense', String(num(expenseId)));
  const cleanPartner = normalizePartnerName(partnerName);
  const cleanAmount = roundMoney(Math.max(0, num(amount)));
  if (!isTrackedPartner(cleanPartner) || cleanAmount <= 0) return null;
  const kind = num(linkedToOrder) === 1 ? 'order_cost' : 'expense';
  const note = String(notes || '').trim() || (num(linkedToOrder)===1 ? `خصم تكلفة من عهدة ${cleanPartner}` : `خصم مصروف من عهدة ${cleanPartner}`);
  return await addPartnerFundEntry({ partner_name: cleanPartner, entry_date: String(expenseDate || today()).trim() || today(), entry_kind: kind, amount: cleanAmount, delta: -cleanAmount, note, source_type: 'expense', source_ref: String(num(expenseId)), is_auto: 1, created_by: createdBy || 'system' });
}

function normalizeActorUsername(value='') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const lower = raw.toLowerCase();
  if (lower === 'admin') return 'admin';
  if (lower === 'abdelrahman') return 'abdelrahman';
  return raw;
}
function actorDisplayName(rowOrName = {}) {
  const aliasByUsername = { admin: 'محمد', abdelrahman: 'بودا' };
  if (typeof rowOrName === 'string') {
    const raw = String(rowOrName || '').trim();
    const alias = aliasByUsername[raw.toLowerCase()] || '';
    return alias || raw;
  }
  const username = String(rowOrName?.username || rowOrName?.admin_username || '').trim();
  const alias = aliasByUsername[username.toLowerCase()] || '';
  if (alias) return alias;
  const full = String(rowOrName?.full_name || rowOrName?.admin_name || '').trim();
  return full || username;
}
async function getActiveUsersLite() {
  const rows = await allAsync(`SELECT id, username, full_name, role FROM users WHERE COALESCE(is_active,1)=1 ORDER BY CASE WHEN LOWER(username)='admin' THEN 0 WHEN LOWER(username)='abdelrahman' THEN 1 ELSE 2 END, full_name COLLATE NOCASE ASC, username COLLATE NOCASE ASC`);
  return rows.map(row => ({
    id: num(row.id),
    username: normalizeActorUsername(row.username),
    full_name: actorDisplayName(row),
    role: String(row.role || '').trim()
  })).filter(row => row.username);
}
function isTrackedAdminUserRow(row = {}) {
  const username = normalizeActorUsername(row?.username || row?.admin_username || '').toLowerCase();
  if (username === 'admin' || username === 'abdelrahman') return true;
  const display = actorDisplayName(row);
  if (isTrackedPartner(display)) return true;
  const full = String(row?.full_name || row?.admin_name || '').trim();
  return isTrackedPartner(full);
}
async function getTrackedAdminUsersLite() {
  const users = await getActiveUsersLite();
  const byUser = new Map();
  for (const row of users) {
    const key = normalizeActorUsername(row.username).toLowerCase();
    if (key && !byUser.has(key)) byUser.set(key, row);
  }
  const tracked = users.filter(isTrackedAdminUserRow);
  const forced = [];
  for (const key of ['admin','abdelrahman']) {
    if (byUser.has(key) && !tracked.some(row => normalizeActorUsername(row.username).toLowerCase() === key)) forced.push(byUser.get(key));
  }
  const finalRows = [...tracked, ...forced].filter(Boolean).sort((a,b)=>{
    const pa = normalizeActorUsername(a.username).toLowerCase()==='admin' ? 0 : (normalizeActorUsername(a.username).toLowerCase()==='abdelrahman' ? 1 : 2);
    const pb = normalizeActorUsername(b.username).toLowerCase()==='admin' ? 0 : (normalizeActorUsername(b.username).toLowerCase()==='abdelrahman' ? 1 : 2);
    if (pa !== pb) return pa - pb;
    return String(a.full_name || a.username || '').localeCompare(String(b.full_name || b.username || ''), 'ar');
  });
  if (finalRows.length) return finalRows;
  return users.slice(0, 2);
}
function canUseAdminCashOnExpense(user) {
  return !!user && hasPerm(user, 'perm_use_admin_cash_on_expense');
}
function canManageCurrentCash(user) {
  return !!user && hasPerm(user, 'perm_manage_current_cash');
}
function canEditExpenseRecords(user) {
  return !!user && hasPerm(user, 'perm_edit_expense_records');
}
function canDeleteExpenseRecords(user) {
  return !!user && hasPerm(user, 'perm_delete_expense_records');
}
function canEditCashRecords(user) {
  return !!user && hasPerm(user, 'perm_edit_cash_records');
}
function canDeleteCashRecords(user) {
  return !!user && hasPerm(user, 'perm_delete_cash_records');
}
function canEditAdminCashRecords(user) {
  return !!user && hasPerm(user, 'perm_edit_admin_cash_records');
}
function canDeleteAdminCashRecords(user) {
  return !!user && hasPerm(user, 'perm_delete_admin_cash_records');
}
function canEditActivityRecords(user) {
  return !!user && hasPerm(user, 'perm_edit_activity_records');
}
function canDeleteActivityRecords(user) {
  return !!user && hasPerm(user, 'perm_delete_activity_records');
}
function canDeleteSystemLogEntry(user, logRow = {}) {
  const entityType = String(logRow?.entity_type || '').trim();
  if (entityType === 'expense') return canDeleteExpenseRecords(user);
  if (entityType === 'cash_adjustment') return canDeleteCashRecords(user);
  if (entityType === 'admin_cash' || entityType === 'admin_cash_transfer') return canDeleteAdminCashRecords(user);
  return canDeleteActivityRecords(user);
}
async function resolveRequestedActor(req, requestedUsername='', { allowBlank = true } = {}) {
  const desired = normalizeActorUsername(requestedUsername);
  if (!desired) {
    if (allowBlank) return { username:'', full_name:'', display_name:'' };
    return { username: normalizeActorUsername(req.user?.username), full_name: actorDisplayName(req.user), display_name: actorDisplayName(req.user) };
  }
  const selfUsername = normalizeActorUsername(req.user?.username);
  if (desired !== selfUsername && !hasPerm(req.user, 'perm_act_as_other_admin')) {
    throw new Error('غير مسموح لك تعمل العملية باسم أدمن آخر');
  }
  const row = await getAsync(`SELECT id, username, full_name, role, is_active FROM users WHERE LOWER(username)=LOWER(?) LIMIT 1`, [desired]);
  if (!row || !num(row.is_active, 1)) throw new Error('الأدمن المحدد غير متاح');
  return { username: normalizeActorUsername(row.username), full_name: actorDisplayName(row), display_name: actorDisplayName(row) };
}
async function getAdminCashBalance(adminUsername='') {
  const username = normalizeActorUsername(adminUsername);
  if (!username) return 0;
  const row = await getAsync(`SELECT COALESCE(MAX(balance_after),0) bal FROM admin_cash_ledger WHERE admin_username=?`, [username]);
  if (row && Number.isFinite(Number(row.bal))) return roundMoney(num(row.bal));
  const sum = await getAsync(`SELECT COALESCE(SUM(delta),0) bal FROM admin_cash_ledger WHERE admin_username=?`, [username]);
  return roundMoney(num(sum?.bal));
}
async function ensureAdminCashAvailable(adminUsername='', amount=0, label='') {
  const username = normalizeActorUsername(adminUsername);
  const needed = roundMoney(Math.max(0, num(amount)));
  if (!username || needed <= 0) return;
  const bal = await getAdminCashBalance(username);
  if (roundMoney(bal) + 1e-9 < needed) {
    const display = actorDisplayName(username) || username;
    const extra = String(label || '').trim() ? ` لـ ${String(label || '').trim()}` : '';
    throw new Error(`لا يمكن تسجيل العملية${extra}: ${display} معاه ${bal.toFixed(2)} ج فقط`);
  }
}
async function deleteAdminCashEntriesBySource(sourceType='', sourceRef='') {
  const rows = await allAsync(`SELECT id FROM admin_cash_ledger WHERE source_type=? AND source_ref=? ORDER BY id DESC`, [String(sourceType||''), String(sourceRef||'')]);
  if (!rows.length) return;
  await runAsync(`DELETE FROM admin_cash_ledger WHERE source_type=? AND source_ref=?`, [String(sourceType||''), String(sourceRef||'')]);
  await rebuildAdminCashBalances();
}
async function addAdminCashEntry({ admin_username='', admin_name='', entry_date='', entry_kind='add', amount=0, delta=null, note='', source_type='', source_ref='', related_admin_username='', related_admin_name='', is_auto=0, created_by='' } = {}) {
  const adminUsername = normalizeActorUsername(admin_username);
  if (!adminUsername) return null;
  const cleanAmount = roundMoney(Math.abs(num(amount)));
  let signedDelta = delta===null ? cleanAmount : roundMoney(num(delta));
  const kind = String(entry_kind || 'add').trim() || 'add';
  if (['sub','expense','order_cost','transfer_out'].includes(kind)) signedDelta = -Math.abs(signedDelta || cleanAmount);
  if (['add','transfer_in'].includes(kind)) signedDelta = Math.abs(signedDelta || cleanAmount);
  if (cleanAmount <= 0 && Math.abs(signedDelta) <= 0) return null;
  const balanceBefore = await getAdminCashBalance(adminUsername);
  if (['sub','expense','order_cost','transfer_out'].includes(kind) && roundMoney(balanceBefore + signedDelta) < 0) {
    const display = actorDisplayName(adminUsername) || adminUsername;
    throw new Error(`لا يمكن الخصم من عهدة ${display}: المتاح ${balanceBefore.toFixed(2)} ج فقط`);
  }
  const balanceAfter = roundMoney(balanceBefore + signedDelta);
  const ins = await runAsync(`INSERT INTO admin_cash_ledger (admin_username,admin_name,entry_date,entry_kind,amount,delta,balance_before,balance_after,note,source_type,source_ref,related_admin_username,related_admin_name,is_auto,created_at,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [adminUsername, String(admin_name || '').trim() || adminUsername, String(entry_date || today()).trim() || today(), kind, cleanAmount || Math.abs(signedDelta), signedDelta, balanceBefore, balanceAfter, String(note || '').trim(), String(source_type || '').trim(), String(source_ref || '').trim(), normalizeActorUsername(related_admin_username), String(related_admin_name || '').trim(), num(is_auto,0)?1:0, new Date().toISOString(), String(created_by || '').trim()]);
  return ins.lastID;
}
async function rebuildAdminCashBalances() {
  const rows = await allAsync(`SELECT * FROM admin_cash_ledger ORDER BY admin_username ASC, entry_date ASC, id ASC`);
  const balances = {};
  for (const row of rows) {
    const adminUsername = normalizeActorUsername(row.admin_username);
    const before = roundMoney(num(balances[adminUsername]));
    const after = roundMoney(before + num(row.delta));
    balances[adminUsername] = after;
    await runAsync(`UPDATE admin_cash_ledger SET balance_before=?, balance_after=? WHERE id=?`, [before, after, row.id]);
  }
}
async function syncExpenseAdminCash({ expenseId=0, actorUsername='', actorName='', amount=0, expenseDate='', linkedToOrder=0, notes='', createdBy='' } = {}) {
  await deleteAdminCashEntriesBySource('expense', String(num(expenseId)));
  const username = normalizeActorUsername(actorUsername);
  const cleanAmount = roundMoney(Math.max(0, num(amount)));
  if (!username || cleanAmount <= 0) return null;
  const kind = num(linkedToOrder) === 1 ? 'order_cost' : 'expense';
  const display = String(actorName || username).trim() || username;
  const note = String(notes || '').trim() || (num(linkedToOrder)===1 ? `خصم تكلفة من عهدة ${display}` : `خصم مصروف من عهدة ${display}`);
  return await addAdminCashEntry({ admin_username: username, admin_name: display, entry_date: String(expenseDate || today()).trim() || today(), entry_kind: kind, amount: cleanAmount, delta: -cleanAmount, note, source_type: 'expense', source_ref: String(num(expenseId)), is_auto: 1, created_by: createdBy || 'system' });
}
async function syncOrderOperationAdminCash({ operationId=0 } = {}) {
  await deleteAdminCashEntriesBySource('order_operation', String(num(operationId)));
  return null;
}
async function getAdminCashSummaryPayload({ from='', to='' } = {}) {
  const users = await getTrackedAdminUsersLite();
  const filter = buildDateFilterParts('entry_date', from, to);
  const rows = await allAsync(`SELECT * FROM admin_cash_ledger WHERE 1=1${filter.sql} ORDER BY entry_date DESC, id DESC`, filter.params);
  const allRows = await allAsync(`SELECT * FROM admin_cash_ledger ORDER BY entry_date ASC, id ASC`);
  const summary = users.map(user => {
    const userAllRows = allRows.filter(row => normalizeActorUsername(row.admin_username) === user.username);
    const currentBalance = roundMoney(userAllRows.reduce((sum, row) => sum + num(row.delta), 0));
    const received = roundMoney(userAllRows.filter(row => num(row.delta) > 0).reduce((sum, row) => sum + num(row.delta), 0));
    const spent = roundMoney(userAllRows.filter(row => num(row.delta) < 0).reduce((sum, row) => sum + Math.abs(num(row.delta)), 0));
    const todaySpent = roundMoney(userAllRows.filter(row => String(row.entry_date || '').slice(0,10) === today() && num(row.delta) < 0).reduce((sum, row) => sum + Math.abs(num(row.delta)), 0));
    const last = userAllRows.slice().sort((a,b)=>String(b.entry_date||'').localeCompare(String(a.entry_date||'')) || num(b.id)-num(a.id))[0] || null;
    return { username: user.username, full_name: user.full_name, role: user.role, current_balance: currentBalance, received, spent, today_spent: todaySpent, entries_count: userAllRows.length, last_entry: last };
  });
  return { users, rows, summary };
}
function buildDateFilterParts(column, fromValue='', toValue='') {
  let sql = '';
  const params = [];
  const from = String(fromValue || '').trim();
  const to = String(toValue || '').trim();
  if (from) { sql += ` AND substr(COALESCE(${column},''),1,10) >= ?`; params.push(from); }
  if (to) { sql += ` AND substr(COALESCE(${column},''),1,10) <= ?`; params.push(to); }
  return { sql, params };
}
async function getInventoryValueSummary() {
  const [paper, handles, bags] = await Promise.all([
    getAsync(`SELECT COALESCE(SUM(COALESCE(total_kg,0) * COALESCE(buy_price_kg,0)),0) total FROM paper`),
    getAsync(`SELECT COALESCE(SUM(COALESCE(qty,0) * COALESCE(buy_price,0)),0) total FROM handles`),
    getAsync(`SELECT COALESCE(SUM(COALESCE(total_qty,0) * COALESCE(buy_price,0)),0) total FROM bags`)
  ]);
  const paperValue = roundMoney(num(paper?.total));
  const handlesValue = roundMoney(num(handles?.total));
  const bagsValue = roundMoney(num(bags?.total));
  const totalInventoryValue = roundMoney(paperValue + handlesValue + bagsValue);
  return { paperValue, handlesValue, bagsValue, totalInventoryValue };
}

async function getCashSummary({ from='', to='' } = {}) {
  const purchasesFilter = buildDateFilterParts('purchase_date', from, to);
  const expensesFilter = buildDateFilterParts('expense_date', from, to);
  const salesFilter = buildDateFilterParts('sale_date', from, to);
  const receivableFilter = buildDateFilterParts('payment_date', from, to);
  const debtFilter = buildDateFilterParts('payment_date', from, to);
  const adjustmentsFilter = buildDateFilterParts('adjustment_date', from, to);
  const orderFilter = buildDateFilterParts('orderDate', from, to);
  const partnerDrawsFilter = buildDateFilterParts('withdrawal_date', from, to);

  const [purchasesPaid, generalExpenses, sales, receivables, debtPayments, adjustments, inventory, orderCashCosts, partnerDraws] = await Promise.all([
    getAsync(`SELECT COALESCE(SUM(paid_amount),0) total FROM purchases WHERE 1=1${purchasesFilter.sql}`, purchasesFilter.params),
    getAsync(`SELECT COALESCE(SUM(amount),0) total FROM expenses WHERE COALESCE(linked_to_order,0)=0${expensesFilter.sql}`, expensesFilter.params),
    getAsync(`SELECT COALESCE(SUM(paid_amount),0) total FROM sales_history WHERE 1=1${salesFilter.sql}`, salesFilter.params),
    getAsync(`SELECT COALESCE(SUM(amount),0) total FROM manual_receivable_payments WHERE 1=1${receivableFilter.sql}`, receivableFilter.params),
    getAsync(`SELECT COALESCE(SUM(amount),0) total FROM debt_payments WHERE 1=1${debtFilter.sql}`, debtFilter.params),
    getAsync(`SELECT COALESCE(SUM(delta),0) total FROM cash_adjustments WHERE 1=1${adjustmentsFilter.sql}`, adjustmentsFilter.params),
    getInventoryValueSummary(),
    getAsync(`SELECT
      COALESCE(SUM(COALESCE(cost_cut,0)),0) AS totalCut,
      COALESCE(SUM(COALESCE(cost_print,0)),0) AS totalPrint,
      COALESCE(SUM(COALESCE(cost_zinc,0)),0) AS totalZinc,
      COALESCE(SUM(COALESCE(cost_design,0)),0) AS totalDesign,
      COALESCE(SUM(CASE WHEN COALESCE(useReadyStock,0)=1 THEN 0 ELSE COALESCE(cost_make,0) END),0) AS totalMake,
      COALESCE(SUM(COALESCE(cost_hand_fix,0)),0) AS totalHandFix
      FROM orders WHERE 1=1${orderFilter.sql}`, orderFilter.params),
    getAsync(`SELECT COALESCE(SUM(amount),0) total FROM partner_withdrawals WHERE 1=1${partnerDrawsFilter.sql}`, partnerDrawsFilter.params)
  ]);

  const totalPurchases = roundMoney(num(purchasesPaid?.total));
  const totalExpenses = roundMoney(num(generalExpenses?.total));
  const totalCostCutPaid = roundMoney(num(orderCashCosts?.totalCut));
  const totalCostPrintPaid = roundMoney(num(orderCashCosts?.totalPrint));
  const totalCostZincPaid = roundMoney(num(orderCashCosts?.totalZinc));
  const totalCostDesignPaid = roundMoney(num(orderCashCosts?.totalDesign));
  const totalCostMakePaid = roundMoney(num(orderCashCosts?.totalMake));
  const totalCostHandFixPaid = roundMoney(num(orderCashCosts?.totalHandFix));
  const totalOperationalPaid = roundMoney(totalCostCutPaid + totalCostPrintPaid + totalCostZincPaid + totalCostDesignPaid + totalCostMakePaid + totalCostHandFixPaid);
  const totalSalesPaid = roundMoney(num(sales?.total));
  const totalReceivablesCollected = roundMoney(num(receivables?.total));
  const totalDebtPaid = roundMoney(num(debtPayments?.total));
  const totalAdjustments = roundMoney(num(adjustments?.total));
  const totalPartnerDraws = roundMoney(num(partnerDraws?.total));
  const totalInflows = roundMoney(totalSalesPaid + totalReceivablesCollected);
  const totalOutflows = roundMoney(totalPurchases + totalExpenses + totalOperationalPaid + totalDebtPaid + totalPartnerDraws);
  const currentCash = roundMoney(totalAdjustments + totalInflows - totalOutflows);
  const totalAssets = roundMoney(currentCash + inventory.totalInventoryValue);

  return {
    openingCapital: 0,
    currentCapital: currentCash,
    currentCash,
    totalPurchases,
    totalExpenses,
    totalOrderExpensesPaid: totalOperationalPaid,
    totalPartnerPayments: 0,
    totalPartnerDraws,
    totalExecution: totalOperationalPaid,
    totalOperationalPaid,
    totalCostCutPaid,
    totalCostPrintPaid,
    totalCostZincPaid,
    totalCostDesignPaid,
    totalCostMakePaid,
    totalCostHandFixPaid,
    totalSalesPaid,
    totalReceivablesCollected,
    totalDebtPaid,
    totalAdjustments,
    totalInflows,
    totalOutflows,
    paperValue: inventory.paperValue,
    handlesValue: inventory.handlesValue,
    bagsValue: inventory.bagsValue,
    totalInventoryValue: inventory.totalInventoryValue,
    totalAssets
  };
}

async function getCapitalSummary({ from='', to='' } = {}) {
  return await getCashSummary({ from, to });
}
function readyBagCutDimensions(item = {}, layoutKey = 'pieceByPiece') {
  const width = num(item?.w);
  const length = num(item?.l);
  const gusset = num(item?.g);
  const mode = String(layoutKey || '').trim() === 'singlePiece' ? 'singlePiece' : 'pieceByPiece';
  const cutWidth = +(mode === 'pieceByPiece' ? (width + gusset + 2) : ((width * 2) + (gusset * 2) + 2)).toFixed(2);
  const cutLength = +(length + (gusset / 2) + 2).toFixed(2);
  const piecesNeededPerBag = mode === 'pieceByPiece' ? 2 : 1;
  return { cutWidth, cutLength, piecesNeededPerBag };
}
function normalizeProductionStatus(value='') {
  return String(value || '').trim() === 'done' ? 'done' : 'pending';
}

function compactObject(obj) {
  return Object.fromEntries(Object.entries(obj || {}).filter(([_, value]) => !(value === undefined || value === null || value === '')));
}
function splitFullName(fullName='') {
  const parts = String(fullName || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return { firstName: 'عميل', lastName: 'بوسطة' };
  if (parts.length === 1) return { firstName: parts[0], lastName: 'عميل' };
  return { firstName: parts[0], lastName: parts.slice(1).join(' ') };
}
function normalizePhone(phone='') {
  let value = String(phone || '').trim();
  if (!value) return '';
  value = value.replace(/[^\d+]/g, '');
  if (value.startsWith('00')) value = '+' + value.slice(2);
  return value;
}
function roundMoney(value) {
  return Math.round((Number(value) || 0) * 100) / 100;
}
function isPasswordHashed(value='') {
  return String(value || '').startsWith(PASSWORD_HASH_PREFIX);
}
function hashPassword(password='') {
  const plain = String(password || '');
  if (!plain) return '';
  if (isPasswordHashed(plain)) return plain;
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(plain, salt, 64).toString('hex');
  return `${PASSWORD_HASH_PREFIX}${salt}$${hash}`;
}
function verifyPassword(password='', stored='') {
  const plain = String(password || '');
  const saved = String(stored || '');
  if (!saved) return false;
  if (!isPasswordHashed(saved)) return plain === saved;
  const parts = saved.split('$');
  if (parts.length < 3) return false;
  const salt = parts[1] || '';
  const hash = parts[2] || '';
  const derived = crypto.scryptSync(plain, salt, 64).toString('hex');
  try {
    return crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(derived, 'hex'));
  } catch (_) {
    return false;
  }
}

const bostaLocationCache = {
  cities: null,
  citiesFetchedAt: 0,
  zonesByCity: new Map(),
  countries: null,
  countriesFetchedAt: 0
};

const BOSTA_FALLBACK_LOCATIONS = [
  { label: 'القاهرة', zones: ['مدينة نصر','مصر الجديدة','النزهة','السلام','المعادي','حلوان','الشروق','التجمع الخامس','عين شمس','المطرية','الزيتون','السيدة زينب','وسط البلد','المرج','مدينة بدر'] },
  { label: 'الجيزة', zones: ['الدقي','العجوزة','إمبابة','الهرم','فيصل','بولاق الدكرور','الشيخ زايد','6 أكتوبر','الوراق','البدرشين','أبو النمرس','العياط'] },
  { label: 'الإسكندرية', zones: ['سيدي جابر','سموحة','محرم بك','العصافرة','المندرة','العجمي','العامرية','برج العرب','ميامي','الرمل'] },
  { label: 'القليوبية', zones: ['شبرا الخيمة','بنها','قليوب','العبور','الخانكة','طوخ','قها','كفر شكر'] },
  { label: 'الشرقية', zones: ['الزقازيق','العاشر من رمضان','بلبيس','منيا القمح','أبو حماد','فاقوس','ههيا'] },
  { label: 'الغربية', zones: ['طنطا','المحلة الكبرى','كفر الزيات','زفتى','السنطة','بسيون'] },
  { label: 'المنوفية', zones: ['شبين الكوم','السادات','منوف','أشمون','قويسنا','تلا','الباجور'] },
  { label: 'الدقهلية', zones: ['المنصورة','ميت غمر','طلخا','السنبلاوين','أجا','بلقاس','دكرنس'] },
  { label: 'البحيرة', zones: ['دمنهور','كفر الدوار','إيتاي البارود','رشيد','أبو حمص','إدكو'] },
  { label: 'كفر الشيخ', zones: ['كفر الشيخ','دسوق','فوه','بيلا','الحامول','بلطيم'] },
  { label: 'دمياط', zones: ['دمياط','دمياط الجديدة','رأس البر','فارسكور','كفر سعد','الزرقا'] },
  { label: 'بورسعيد', zones: ['بورفؤاد','حي العرب','الزهور','الضواحي','المناخ','شرق بورسعيد'] },
  { label: 'الإسماعيلية', zones: ['الإسماعيلية','فايد','القنطرة شرق','القنطرة غرب','التل الكبير','أبو صوير'] },
  { label: 'السويس', zones: ['الأربعين','فيصل','عتاقة','الجناين','السويس الجديدة'] },
  { label: 'الفيوم', zones: ['الفيوم','سنورس','إطسا','طامية','أبشواي','يوسف الصديق'] },
  { label: 'بني سويف', zones: ['بني سويف','الواسطى','ناصر','إهناسيا','ببا','سمسطا'] },
  { label: 'المنيا', zones: ['المنيا','ملوي','أبو قرقاص','مغاغة','سمالوط','دير مواس'] },
  { label: 'أسيوط', zones: ['أسيوط','ديروط','القوصية','منفلوط','أبو تيج','البداري'] },
  { label: 'سوهاج', zones: ['سوهاج','أخميم','جرجا','البلينا','طهطا','طما'] },
  { label: 'قنا', zones: ['قنا','نجع حمادي','دشنا','قفط','قوص','أبو تشت'] },
  { label: 'الأقصر', zones: ['الأقصر','إسنا','أرمنت','الطود','القرنة'] },
  { label: 'أسوان', zones: ['أسوان','دراو','كوم أمبو','إدفو','أبو سمبل'] },
  { label: 'البحر الأحمر', zones: ['الغردقة','رأس غارب','سفاجا','القصير','مرسى علم'] },
  { label: 'الوادي الجديد', zones: ['الخارجة','الداخلة','الفرافرة','باريس'] },
  { label: 'مطروح', zones: ['مرسى مطروح','الحمام','العلمين','الضبعة','سيوة'] },
  { label: 'شمال سيناء', zones: ['العريش','بئر العبد','الشيخ زويد','رفح','الحسنة'] },
  { label: 'جنوب سيناء', zones: ['شرم الشيخ','الطور','دهب','نويبع','رأس سدر','سانت كاترين'] }
];

const BOSTA_BASE_FEES_BY_CITY = {
  'القاهرة': 100,
  'الجيزة': 100,
  'الإسكندرية': 110,
  'البحيرة': 110,
  'الدقهلية': 117,
  'الإسماعيلية': 120,
  'السويس': 120,
  'الشرقية': 120,
  'الغربية': 120,
  'القليوبية': 120,
  'المنوفية': 120,
  'بورسعيد': 120,
  'دمياط': 120,
  'كفر الشيخ': 120,
  'الفيوم': 135,
  'بني سويف': 135,
  'المنيا': 135,
  'أسيوط': 135,
  'سوهاج': 135,
  'الأقصر': 150,
  'مطروح': 150,
  'أسوان': 155,
  'قنا': 155,
  'البحر الأحمر': 155,
  'الوادي الجديد': 175,
  'شمال سيناء': 175,
  'جنوب سيناء': 175
};
const BOSTA_ZONE_BASE_OVERRIDES = {
  'مطروح': {
    'الساحل': 160,
    'الساحل الشمالي': 160,
    'العلمين': 160,
    'الضبعة': 160,
    'الحمام': 160,
    'مرسى مطروح': 150,
    'مطروح': 150
  }
};
const BOSTA_PACKAGE_TYPE_ADJUSTMENTS = {
  Document: 0,
  Parcel: 0,
  Bulky: 0
};
const BOSTA_OPEN_PACKAGE_FEE = 7;
const BOSTA_INSURANCE_RATE = 0.01;
const BOSTA_EXTRA_COD_RATE = 0.01;
const BOSTA_EXTRA_COD_PER_1000 = 10;
const BOSTA_VAT_RATE_DEFAULT = 0;
const BOSTA_CITY_LABEL_ALIASES = {
  'القاهرة': ['cairo'],
  'الجيزة': ['giza', 'gizeh'],
  'الإسكندرية': ['alexandria', 'iskandaria'],
  'القليوبية': ['qalyubia', 'qalyubiya', 'qalubia', 'kalubia'],
  'الشرقية': ['sharkia', 'sharqia', 'sharqeya'],
  'الغربية': ['gharbia', 'gharbeya'],
  'المنوفية': ['monufia', 'menoufia', 'monofia'],
  'الدقهلية': ['dakahlia', 'dakahliya', 'daqahliyah', 'dakahleya'],
  'البحيرة': ['beheira', 'behaira', 'el beheira'],
  'كفر الشيخ': ['kafr el sheikh', 'kafr elsheikh', 'kafr ash shaykh'],
  'دمياط': ['damietta', 'dumyat'],
  'بورسعيد': ['port said', 'portsaid'],
  'الإسماعيلية': ['ismailia', 'ismailiya'],
  'السويس': ['suez'],
  'الفيوم': ['fayoum', 'faiyum'],
  'بني سويف': ['beni suef', 'bani sweif', 'beni sweif'],
  'المنيا': ['minya', 'menia'],
  'أسيوط': ['assiut', 'asyut'],
  'سوهاج': ['sohag', 'suhaj'],
  'قنا': ['qena', 'qina'],
  'الأقصر': ['luxor'],
  'أسوان': ['aswan'],
  'البحر الأحمر': ['red sea'],
  'الوادي الجديد': ['new valley'],
  'مطروح': ['matrouh', 'marsa matrouh'],
  'شمال سيناء': ['north sinai'],
  'جنوب سيناء': ['south sinai']
};

function bostaGovernorateTokens(label='') {
  const raw = String(label || '').replace(/^__label__:/, '').trim();
  if (!raw) return [];
  const normalized = normalizeArabicLocationText(raw);
  const found = BOSTA_FALLBACK_LOCATIONS.find(item => normalizeArabicLocationText(item.label) === normalized);
  const labelKey = found?.label || raw;
  return [...new Set([
    labelKey,
    ...(BOSTA_CITY_LABEL_ALIASES[labelKey] || [])
  ].map(v => normalizeArabicLocationText(v)).filter(Boolean))];
}
function scoreBostaGovernorateRowMatch(row={}, governorateLabel='') {
  const tokens = bostaGovernorateTokens(governorateLabel).filter(token => token.length >= 3);
  if (!tokens.length) return 0;
  const info = cityInfoFromRow(row || {});
  const rowTokens = [info.code, ...(info.labels || [])].map(v => normalizeArabicLocationText(v)).filter(Boolean);
  let best = 0;
  for (const token of tokens) {
    for (const rowToken of rowTokens) {
      if (!rowToken) continue;
      if (rowToken === token) best = Math.max(best, 100 + token.length);
      else if (rowToken.replace(/\s+/g, '') === token.replace(/\s+/g, '')) best = Math.max(best, 95 + token.length);
      else if (token.length >= 4 && rowToken.includes(token)) best = Math.max(best, 80 + token.length);
      else if (rowToken.length >= 4 && token.includes(rowToken)) best = Math.max(best, 60 + rowToken.length);
    }
  }
  return best;
}
function rowMatchesBostaGovernorate(row={}, governorateLabel='') {
  return scoreBostaGovernorateRowMatch(row, governorateLabel) > 0;
}
function findBostaCityByGovernorateLabel(rows=[], governorateLabel='') {
  let bestRow = null;
  let bestScore = 0;
  for (const row of (rows || [])) {
    const score = scoreBostaGovernorateRowMatch(row, governorateLabel);
    if (score > bestScore) {
      bestScore = score;
      bestRow = row;
    }
  }
  return bestRow || null;
}
function detectGovernorateLabelFromText(rawText='') {
  const normalized = normalizeArabicLocationText(rawText);
  if (!normalized) return '';
  let best = { label:'', score:0 };
  for (const item of BOSTA_FALLBACK_LOCATIONS) {
    for (const token of bostaGovernorateTokens(item.label)) {
      if (!token || token.length < 3) continue;
      if (normalized.includes(token) && token.length > best.score) best = { label:item.label, score:token.length };
    }
  }
  return best.label || '';
}

function resolveConfiguredBostaGovernorateLabel(cityToken='', rows=[]) {
  const raw = String(cityToken || '').trim();
  if (!raw) return '';
  if (raw.startsWith('__label__:')) return raw.replace(/^__label__:/, '').trim();
  const normalizedRaw = normalizeArabicLocationText(raw);
  const direct = BOSTA_FALLBACK_LOCATIONS.find(item => normalizeArabicLocationText(item.label) === normalizedRaw || bostaGovernorateTokens(item.label).includes(normalizedRaw));
  if (direct) return direct.label;
  const matchedRow = resolveCityBySavedToken(rows, raw);
  const info = cityInfoFromRow(matchedRow || {});
  for (const item of BOSTA_FALLBACK_LOCATIONS) {
    if (rowMatchesBostaGovernorate(info, item.label)) return item.label;
  }
  return inferFallbackCityLabel(cityToken, rows);
}
function resolveBostaCityLabelFromToken(cityCode='', rows=[]) {
  return resolveConfiguredBostaGovernorateLabel(cityCode, rows);
}
function localBostaBaseFee(cityLabel='', zoneLabel='', packageType='Parcel') {
  const cityName = String(cityLabel || '').replace(/^__label__:/, '').trim();
  const zoneName = String(zoneLabel || '').trim();
  const cityKey = normalizeArabicLocationText(cityName);
  const zoneKey = normalizeArabicLocationText(zoneName);
  let base = 120;
  for (const [label, value] of Object.entries(BOSTA_BASE_FEES_BY_CITY)) {
    if (normalizeArabicLocationText(label) === cityKey) { base = Number(value) || base; break; }
  }
  const zoneMap = Object.entries(BOSTA_ZONE_BASE_OVERRIDES).find(([label]) => normalizeArabicLocationText(label) === cityKey);
  if (zoneMap) {
    for (const [zoneLabelAr, value] of Object.entries(zoneMap[1] || {})) {
      if (normalizeArabicLocationText(zoneLabelAr) === zoneKey) { base = Number(value) || base; break; }
    }
  }
  const typeAdjust = Number(BOSTA_PACKAGE_TYPE_ADJUSTMENTS[String(packageType || 'Parcel')] || 0);
  return roundMoney(base + typeAdjust);
}
function calcLocalBostaInsurance(productValue=0) {
  const val = Math.max(0, Number(productValue) || 0);
  if (!val) return 0;
  return roundMoney(val * BOSTA_INSURANCE_RATE);
}
function calcLocalBostaExtraCod(cod=0) {
  const val = Math.max(0, Number(cod) || 0);
  if (!val) return 0;
  return roundMoney(Math.ceil(val / 1000) * BOSTA_EXTRA_COD_PER_1000);
}
function buildBostaBreakdownText(details={}) {
  const bits = [];
  const pushMoney = (label, value) => {
    const n = roundMoney(Number(value) || 0);
    if (n > 0) bits.push(`${label}: ${n}`);
  };
  pushMoney('سعر الشحن', details.shippingFee);
  pushMoney('التأمين', details.insuranceFees);
  pushMoney('COD إضافي', details.extraCodFee);
  pushMoney('فتح الشحنة', details.openPackageFees);
  pushMoney('خامة بوسطة', details.bostaMaterialFee);
  pushMoney('قبل الضريبة', details.priceBeforeVat);
  pushMoney('بعد الضريبة', details.priceAfterVat);
  pushMoney('تقدير مستحقات بوسطة', details.estimatedFees);
  return bits.join(' | ');
}
function normalizeBostaBreakdown(details={}, source='local') {
  const shippingFee = roundMoney(Math.max(0, Number(details.shippingFee) || 0));
  const insuranceFees = roundMoney(Math.max(0, Number(details.insuranceFees) || 0));
  const openPackageFees = roundMoney(Math.max(0, Number(details.openPackageFees) || 0));
  const extraCodFee = roundMoney(Math.max(0, Number(details.extraCodFee) || 0));
  const bostaMaterialFee = roundMoney(Math.max(0, Number(details.bostaMaterialFee) || 0));
  const expediteFee = roundMoney(Math.max(0, Number(details.expediteFee) || 0));
  const vatRate = Math.max(0, Number(details.vatRate) || 0);
  const estimatedFees = roundMoney(Math.max(0, Number(details.estimatedFees) || (shippingFee + insuranceFees + openPackageFees + extraCodFee)));
  const priceBeforeVat = roundMoney(Math.max(0, Number(details.priceBeforeVat) || (shippingFee + insuranceFees + openPackageFees + extraCodFee + bostaMaterialFee + expediteFee)));
  const vatAmount = roundMoney(Math.max(0, Number(details.vatAmount) || (vatRate > 0 ? priceBeforeVat * vatRate : 0)));
  const priceAfterVat = roundMoney(Math.max(0, Number(details.priceAfterVat) || (priceBeforeVat + vatAmount)));
  const out = {
    shippingFee,
    insuranceFees,
    openPackageFees,
    extraCodFee,
    bostaMaterialFee,
    expediteFee,
    estimatedFees,
    priceBeforeVat,
    vatRate,
    vatAmount,
    priceAfterVat,
    rawShippingFee: roundMoney(Math.max(0, Number(details.rawShippingFee) || 0)),
    rawBody: details.rawBody || {},
    source: String(source || details.source || 'local').trim() || 'local'
  };
  out.text = String(details.text || '').trim() || buildBostaBreakdownText(out);
  return out;
}
function localBostaEstimateDetails({ cityLabel='', zone='', packageType='Parcel', productValue=0, allowOpen=false, cod=0 } = {}) {
  const shippingFee = localBostaBaseFee(cityLabel, zone, packageType);
  const insuranceFees = calcLocalBostaInsurance(productValue);
  const openPackageFees = allowOpen ? BOSTA_OPEN_PACKAGE_FEE : 0;
  const extraCodFee = calcLocalBostaExtraCod(cod);
  return normalizeBostaBreakdown({
    shippingFee,
    rawShippingFee: shippingFee,
    insuranceFees,
    openPackageFees,
    extraCodFee,
    estimatedFees: shippingFee + insuranceFees + openPackageFees + extraCodFee,
    priceBeforeVat: shippingFee + insuranceFees + openPackageFees + extraCodFee,
    vatRate: 0,
    vatAmount: 0,
    priceAfterVat: shippingFee + insuranceFees + openPackageFees + extraCodFee
  }, 'local');
}
async function callBostaEstimateEndpoint(method='POST', rel='', payload={}) {
  const url = `${BOSTA_BASE_URL}${rel}`;
  const options = {
    method,
    headers: {
      Authorization: BOSTA_API_KEY,
      'Content-Type': 'application/json'
    }
  };
  if (method !== 'GET') options.body = JSON.stringify(payload || {});
  const response = await fetch(url, options);
  const rawText = await response.text();
  let rawJson = {};
  try { rawJson = rawText ? JSON.parse(rawText) : {}; } catch (_) { rawJson = { message: rawText }; }
  if (!response.ok) throw new Error(rawJson?.message || rawJson?.error || rawText || `Bosta ${response.status}`);
  return rawJson;
}
function extractBostaPricingBreakdown(raw) {
  const body = raw?.data || raw?.message || raw || {};
  const shippingFee = pickFirstNumberDeep(body, ['shippingFee','shippingFees','shippingPrice','deliveryPrice','deliveryFee','deliveryFees','baseFee','fee']);
  const insuranceFees = pickFirstNumberDeep(body, ['insuranceFee','insuranceFees','insurance_fees','insurance','insurancePremium','insuranceAmount','insuranceFeesAmount','amount']);
  const extraCodFee = pickFirstNumberDeep(body, ['extraCodFee','extraCodFees','extra_cod_fee','codExtraFee','codExtraFees','amount']);
  const openPackageFees = pickFirstNumberDeep(body, ['openingPackageFee','openingPackageFees','openPackageFee','open_package_fee','amount']);
  const bostaMaterialFee = pickFirstNumberDeep(body, ['bostaMaterialFee','bosta_material_fee','materialFee','materialFees','amount']);
  const expediteFee = pickFirstNumberDeep(body, ['expediteFee','expediteFees','expedite_fee','amount']);
  const priceBeforeVat = pickFirstNumberDeep(body, ['priceBeforeVat','subtotal','subTotal','totalBeforeVat','price_before_vat']);
  const priceAfterVat = pickFirstNumberDeep(body, ['priceAfterVat','total','grandTotal','totalAfterVat','price_after_vat']);
  const vatRate = pickFirstNumberDeep(body, ['vat','vatRate','vat_rate']) ?? BOSTA_VAT_RATE_DEFAULT;
  const vatAmount = pickFirstNumberDeep(body, ['vatAmount','valueAddedTax','taxAmount']);
  return normalizeBostaBreakdown({
    shippingFee: shippingFee != null ? Number(shippingFee) : 0,
    rawShippingFee: shippingFee != null ? Number(shippingFee) : 0,
    insuranceFees: insuranceFees != null ? Number(insuranceFees) : 0,
    extraCodFee: extraCodFee != null ? Number(extraCodFee) : 0,
    openPackageFees: openPackageFees != null ? Number(openPackageFees) : 0,
    bostaMaterialFee: bostaMaterialFee != null ? Number(bostaMaterialFee) : 0,
    expediteFee: expediteFee != null ? Number(expediteFee) : 0,
    priceBeforeVat: priceBeforeVat != null ? Number(priceBeforeVat) : 0,
    priceAfterVat: priceAfterVat != null ? Number(priceAfterVat) : 0,
    vatRate: vatRate != null ? Number(vatRate) : 0,
    vatAmount: vatAmount != null ? Number(vatAmount) : 0,
    rawBody: raw || {}
  }, 'api');
}
function extractBostaShippingFee(raw) {
  return extractBostaPricingBreakdown(raw).shippingFee;
}
async function fetchBostaEstimateFromApi(params={}) {
  if (!BOSTA_API_KEY) return null;
  const body = compactObject({
    cityCode: params.cityCode,
    zone: params.zone,
    packageType: params.packageType || 'Parcel',
    cod: Math.max(0, Number(params.cod) || 0),
    productValue: Math.max(0, Number(params.productValue) || 0),
    allowToOpenPackage: !!params.allowOpen,
    itemsCount: Math.max(1, Number(params.itemsCount) || 1),
    specs: {
      packageType: params.packageType || 'Parcel',
      packageDetails: {
        description: String(params.description || 'Shipment').trim() || 'Shipment',
        itemsCount: Math.max(1, Number(params.itemsCount) || 1),
        productValue: Math.max(0, Number(params.productValue) || 0)
      }
    },
    dropOffAddress: compactObject({
      cityCode: params.cityCode,
      zone: params.zone,
      firstLine: String(params.firstLine || params.secondLine || params.zone || params.cityLabel || 'address').trim(),
      secondLine: String(params.secondLine || params.zone || params.cityLabel || 'address').trim()
    })
  });
  const qs = new URLSearchParams({
    cityCode: String(params.cityCode || ''),
    zone: String(params.zone || ''),
    packageType: String(params.packageType || 'Parcel'),
    cod: String(Math.max(0, Number(params.cod) || 0)),
    productValue: String(Math.max(0, Number(params.productValue) || 0)),
    allowToOpenPackage: params.allowOpen ? 'true' : 'false',
    itemsCount: String(Math.max(1, Number(params.itemsCount) || 1))
  }).toString();
  const candidates = [
    ['POST', '/deliveries/estimate'],
    ['POST', '/deliveries/fees/estimate'],
    ['POST', '/pricing/deliveries'],
    ['POST', '/pricing/shipments'],
    ['GET', `/pricing/deliveries?${qs}`],
    ['GET', `/pricing/shipments?${qs}`],
    ['GET', `/deliveries/estimate?${qs}`]
  ];
  for (const [method, rel] of candidates) {
    try {
      const raw = await callBostaEstimateEndpoint(method, rel, body);
      const parsed = extractBostaPricingBreakdown(raw);
      if (parsed.priceAfterVat > 0 || parsed.priceBeforeVat > 0 || parsed.rawShippingFee > 0 || parsed.insuranceFees > 0) {
        return parsed;
      }
    } catch (_) {}
  }
  return null;
}
async function getBostaEstimateDetails(params={}) {
  const rows = await getBostaCitiesCached();
  const cityLabel = String(params.cityLabel || resolveConfiguredBostaGovernorateLabel(params.cityCode || '', rows) || params.cityCode || '').replace(/^__label__:/, '').trim();
  const local = localBostaEstimateDetails({
    cityLabel,
    zone: String(params.zone || '').trim(),
    packageType: params.packageType || 'Parcel',
    productValue: Math.max(0, Number(params.productValue) || 0),
    allowOpen: !!params.allowOpen,
    cod: Math.max(0, Number(params.cod) || 0)
  });
  return {
    ...local,
    cityLabel,
    zone: String(params.zone || '').trim(),
    source: 'manual_table',
    rawShippingFee: local.shippingFee,
    text: buildBostaBreakdownText(local)
  };
}


function normalizeArabicLocationText(value='') {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[ً-ٰٟ]/g, '')
    .replace(/[أإآ]/g, 'ا')
    .replace(/ة/g, 'ه')
    .replace(/ى/g, 'ي')
    .replace(/ؤ/g, 'و')
    .replace(/ئ/g, 'ي')
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function pickBostaArray(raw) {
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw?.data)) return raw.data;
  if (Array.isArray(raw?.message)) return raw.message;
  if (Array.isArray(raw?.result)) return raw.result;
  if (Array.isArray(raw?.results)) return raw.results;
  if (Array.isArray(raw?.cities)) return raw.cities;
  if (Array.isArray(raw?.zones)) return raw.zones;
  return [];
}
async function fetchBostaList(paths=[]) {
  if (!BOSTA_API_KEY) return [];
  for (const rel of paths) {
    const url = `${BOSTA_BASE_URL}${rel}`;
    try {
      const response = await fetch(url, { headers: { Authorization: BOSTA_API_KEY } });
      if (!response.ok) continue;
      const text = await response.text();
      let raw = {};
      try { raw = text ? JSON.parse(text) : {}; } catch (_) { raw = {}; }
      const rows = pickBostaArray(raw);
      if (rows.length) return rows;
    } catch (_) {}
  }
  return [];
}
function countryInfoFromRow(row={}) {
  const id = String(row._id || row.id || row.countryId || row.country_id || row.code || row.countryCode || '').trim();
  const labels = [row.name, row.nameAr, row.arName, row.arabicName, row.displayName, row.displayNameAr, row.title].map(v => String(v || '').trim()).filter(Boolean);
  const code = String(row.code || row.countryCode || row.isoCode || '').trim();
  return { id, code, labels };
}
function fallbackBostaCityOptions() {
  return BOSTA_FALLBACK_LOCATIONS.map(item => ({ code: `__label__:${item.label}`, label: item.label, labels: [item.label], isFallback: true }));
}
function fallbackBostaZones(cityCode='') {
  const key = String(cityCode || '').replace(/^__label__:/, '').trim();
  const normalizedKey = normalizeArabicLocationText(key);
  const hit = BOSTA_FALLBACK_LOCATIONS.find(item => normalizeArabicLocationText(item.label) === normalizedKey);
  return (hit?.zones || []).map((label, idx) => ({ id: `fallback_${idx+1}`, label, labels: [label], isFallback: true }));
}
function fallbackBostaZonesByCityLabel(cityLabel='') {
  return fallbackBostaZones(`__label__:${String(cityLabel || '').trim()}`);
}
function inferFallbackCityLabel(cityToken='', rows=[]) {
  const raw = String(cityToken || '').trim();
  if (!raw) return '';
  if (raw.startsWith('__label__:')) return raw.replace(/^__label__:/, '').trim();
  const match = resolveCityBySavedToken(rows, raw);
  const info = cityInfoFromRow(match || {});
  for (const item of BOSTA_FALLBACK_LOCATIONS) {
    if (rowMatchesBostaGovernorate(info, item.label)) return item.label;
  }
  const normalized = normalizeArabicLocationText(raw);
  const fallback = BOSTA_FALLBACK_LOCATIONS.find(item => normalizeArabicLocationText(item.label) === normalized || bostaGovernorateTokens(item.label).includes(normalized));
  return fallback ? fallback.label : raw;
}
function looksLikeBrokenZoneOption(label='') {
  const v = String(label || '').trim().toLowerCase();
  if (!v) return true;
  return /^f\d+$/i.test(v) || /^zone\s*\d+$/i.test(v) || /^district\s*\d+$/i.test(v);
}
function resolveCityBySavedToken(rows=[], savedToken='') {
  const tokenRaw = String(savedToken || '').replace(/^__label__:/, '').trim();
  if (!tokenRaw || !rows.length) return null;
  const token = normalizeArabicLocationText(tokenRaw);
  const directMatch = rows.find(row => {
    const info = cityInfoFromRow(row);
    if (normalizeArabicLocationText(info.code) === token) return true;
    return info.labels.some(label => normalizeArabicLocationText(label) === token);
  });
  if (directMatch) return directMatch;
  const fallbackHit = BOSTA_FALLBACK_LOCATIONS.find(item => normalizeArabicLocationText(item.label) === token || bostaGovernorateTokens(item.label).includes(token));
  if (fallbackHit) return findBostaCityByGovernorateLabel(rows, fallbackHit.label);
  return null;
}
async function getBostaCountriesCached(force=false) {
  const fresh = bostaLocationCache.countries && (Date.now() - bostaLocationCache.countriesFetchedAt < 1000 * 60 * 60 * 12);
  if (!force && fresh) return bostaLocationCache.countries;
  const rows = await fetchBostaList(['/countries', '/countries?active=true', '/lookup/countries', '/lookups/countries']);
  bostaLocationCache.countries = rows;
  bostaLocationCache.countriesFetchedAt = Date.now();
  return rows;
}
async function discoverEgyptCountryParams() {
  const rows = await getBostaCountriesCached();
  if (!rows.length) return [];
  const found = rows.find(row => {
    const info = countryInfoFromRow(row);
    const labels = info.labels.map(normalizeArabicLocationText);
    return info.code.toUpperCase() === 'EG' || labels.includes('مصر') || labels.includes('egypt');
  });
  if (!found) return [];
  const info = countryInfoFromRow(found);
  const params = [];
  const pushUnique = (v) => { v = String(v || '').trim(); if (v && !params.includes(v)) params.push(v); };
  pushUnique(info.id);
  pushUnique(info.code);
  return params;
}

function cityInfoFromRow(row={}) {
  const code = String(row.cityCode || row.code || row._id || row.id || row.city_id || '').trim();
  const labels = [
    row.name, row.nameAr, row.arName, row.arabicName, row.displayName, row.displayNameAr,
    row.governorateName, row.governorateNameAr, row.cityName, row.cityNameAr, row.zoneName
  ].map(v => String(v || '').trim()).filter(Boolean);
  return { code, labels };
}
function zoneInfoFromRow(row={}) {
  const labels = [
    row.name, row.nameAr, row.districtName, row.districtNameAr, row.zoneName, row.zoneNameAr,
    row.displayName, row.displayNameAr, row.title
  ].map(v => String(v || '').trim()).filter(Boolean);
  return { id: String(row._id || row.id || '').trim(), labels };
}
function uniqueBy(items=[], keyFn=(x)=>x) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = String(keyFn(item) || '');
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}
function bostaCityOptions(rows=[]) {
  return uniqueBy(rows.map(row => {
    const info = cityInfoFromRow(row);
    return {
      code: info.code,
      label: info.labels[0] || info.code,
      labels: info.labels
    };
  }).filter(item => item.code && item.label), item => item.code).sort((a,b)=> String(a.label).localeCompare(String(b.label), 'ar'));
}
function bostaZoneOptions(rows=[]) {
  return uniqueBy(rows.map(row => {
    const info = zoneInfoFromRow(row);
    return {
      id: info.id,
      label: info.labels[0] || info.id,
      labels: info.labels
    };
  }).filter(item => item.label), item => normalizeArabicLocationText(item.label)).sort((a,b)=> String(a.label).localeCompare(String(b.label), 'ar'));
}
async function getBostaCitiesCached(force=false) {
  const fresh = bostaLocationCache.cities && (Date.now() - bostaLocationCache.citiesFetchedAt < 1000 * 60 * 60 * 12);
  if (!force && fresh) return bostaLocationCache.cities;
  let rows = await fetchBostaList(['/cities', '/cities?countryCode=EG', '/cities?countryCode=eg', '/cities?country=EG', '/cities/all']);
  if (!rows.length) {
    const egyptParams = await discoverEgyptCountryParams();
    const extraPaths = [];
    for (const token of egyptParams) {
      extraPaths.push(`/cities?countryId=${encodeURIComponent(token)}`);
      extraPaths.push(`/cities?country_id=${encodeURIComponent(token)}`);
      extraPaths.push(`/countries/${encodeURIComponent(token)}/cities`);
      extraPaths.push(`/cities/getAllCities?countryId=${encodeURIComponent(token)}`);
    }
    if (extraPaths.length) rows = await fetchBostaList(extraPaths);
  }
  bostaLocationCache.cities = rows;
  bostaLocationCache.citiesFetchedAt = Date.now();
  return rows;
}
async function getBostaZonesCached(cityCode='') {
  const key = String(cityCode || '').trim();
  if (!key) return [];
  if (bostaLocationCache.zonesByCity.has(key)) return bostaLocationCache.zonesByCity.get(key);
  const rawKey = key.replace(/^__label__:/, '').trim();
  let fetchKey = key;
  if (key.startsWith('__label__:')) {
    const cities = await getBostaCitiesCached();
    const match = resolveCityBySavedToken(cities, rawKey);
    const info = cityInfoFromRow(match || {});
    fetchKey = info.code || key;
  }
  let rows = await fetchBostaList([
    `/cities/${encodeURIComponent(fetchKey)}/zones`,
    `/zones?cityId=${encodeURIComponent(fetchKey)}`,
    `/zones?cityCode=${encodeURIComponent(fetchKey)}`,
    `/cities/${encodeURIComponent(fetchKey)}/districts`
  ]);
  if (!rows.length) rows = fallbackBostaZones(rawKey);
  bostaLocationCache.zonesByCity.set(key, rows);
  return rows;
}
function inferZoneFromAddressText(addressText='', cityLabel='') {
  const original = String(addressText || '').trim();
  if (!original) return '';
  const parts = original.split(/[\n،,\/-]+/).map(s => s.trim()).filter(Boolean);
  const normalizedCity = normalizeArabicLocationText(cityLabel);
  for (const part of parts) {
    const n = normalizeArabicLocationText(part);
    if (!n) continue;
    if (normalizedCity && n === normalizedCity) continue;
    if (normalizedCity && n.includes(normalizedCity)) continue;
    return part;
  }
  return parts[0] || '';
}
async function inferBostaLocation(address='', savedCode='', savedZone='') {
  const originalAddress = String(address || '').trim();
  const normalizedAddress = normalizeArabicLocationText(originalAddress);
  if (!normalizedAddress) return { cityCode: String(savedCode || '').trim(), zone: String(savedZone || '').trim(), cityLabel: '' };

  let chosenCity = null;
  const cities = await getBostaCitiesCached();
  const saved = String(savedCode || '').trim();
  if (saved) {
    chosenCity = resolveCityBySavedToken(cities, saved);
  }
  if (!chosenCity && cities.length) {
    const detectedGovernorate = detectGovernorateLabelFromText(originalAddress);
    if (detectedGovernorate) chosenCity = findBostaCityByGovernorateLabel(cities, detectedGovernorate);
  }
  if (!chosenCity && cities.length) {
    let best = null;
    for (const row of cities) {
      const info = cityInfoFromRow(row);
      for (const label of info.labels) {
        const n = normalizeArabicLocationText(label);
        if (!n || n.length < 3) continue;
        if (normalizedAddress.includes(n)) {
          const score = n.length + (n.includes(' ') ? 5 : 0);
          if (!best || score > best.score) best = { row, score, label };
        }
      }
    }
    chosenCity = best?.row || null;
  }

  const chosenInfo = cityInfoFromRow(chosenCity || {});
  let zone = String(savedZone || '').trim();
  if (!zone && chosenInfo.code) {
    const zones = await getBostaZonesCached(chosenInfo.code);
    let bestZone = null;
    for (const row of zones) {
      const info = zoneInfoFromRow(row);
      for (const label of info.labels) {
        const n = normalizeArabicLocationText(label);
        if (!n || n.length < 2) continue;
        if (normalizedAddress.includes(n)) {
          const score = n.length + (n.includes(' ') ? 5 : 0);
          if (!bestZone || score > bestZone.score) bestZone = { label, score };
        }
      }
    }
    zone = bestZone?.label || inferZoneFromAddressText(originalAddress, chosenInfo.labels[0] || resolveConfiguredBostaGovernorateLabel(savedCode, cities) || '');
  }
  if (!zone) zone = inferZoneFromAddressText(originalAddress, chosenInfo.labels[0] || resolveConfiguredBostaGovernorateLabel(savedCode, cities) || '');

  return {
    cityCode: chosenInfo.code || String(savedCode || '').trim(),
    zone: String(zone || '').trim(),
    cityLabel: resolveConfiguredBostaGovernorateLabel(chosenInfo.code || savedCode || '', cities) || chosenInfo.labels[0] || ''
  };
}

function resolvePaymentTypeFromAmounts(preferredType='', total=0, paid=0) {
  const totalValue = roundMoney(Math.max(0, Number(total) || 0));
  let paidValue = roundMoney(Math.max(0, Number(paid) || 0));
  if (paidValue > totalValue) paidValue = totalValue;
  const remainingValue = roundMoney(Math.max(0, totalValue - paidValue));
  const preferred = String(preferredType || '').trim();
  if (totalValue <= 0) {
    return { paymentType: preferred === 'آجل' ? 'آجل' : 'لم يتم الدفع', paid_amount: 0, remaining_amount: 0 };
  }
  if (remainingValue <= 0) {
    return { paymentType: 'مدفوع كامل', paid_amount: totalValue, remaining_amount: 0 };
  }
  if (preferred === 'لم يتم الدفع') {
    return { paymentType: 'لم يتم الدفع', paid_amount: 0, remaining_amount: totalValue };
  }
  if (paidValue > 0) {
    const paymentType = preferred && preferred !== 'لم يتم الدفع' ? preferred : 'عربون';
    return { paymentType, paid_amount: paidValue, remaining_amount: remainingValue };
  }
  return { paymentType: preferred || 'آجل', paid_amount: 0, remaining_amount: totalValue };
}
function distributeSharedOrderFinancials(items=[], groupTotal=0, groupPaid=0, groupPaymentType='') {
  const list = Array.isArray(items) ? items.map(item => ({ ...item })) : [];
  if (!list.length) return list;
  const normalizedGroup = resolvePaymentTypeFromAmounts(groupPaymentType, groupTotal, groupPaid);
  const totalValue = roundMoney(Math.max(0, Number(groupTotal) || 0));
  const paidValue = roundMoney(Math.max(0, Number(normalizedGroup.paid_amount) || 0));
  const weights = list.map(item => {
    const suggested = Math.max(0, num(item.total_price, 0));
    if (suggested > 0) return suggested;
    const qtyValue = Math.max(0, num(item.qty, 0));
    return qtyValue > 0 ? qtyValue : 1;
  });
  const totalWeight = weights.reduce((sum, value) => sum + value, 0) || list.length || 1;
  let usedTotal = 0;
  let usedPaid = 0;
  return list.map((item, idx) => {
    const isLast = idx === list.length - 1;
    let itemTotal = isLast ? roundMoney(totalValue - usedTotal) : roundMoney((totalValue * weights[idx]) / totalWeight);
    itemTotal = roundMoney(Math.max(0, itemTotal));
    usedTotal = roundMoney(usedTotal + itemTotal);

    let itemPaid = isLast ? roundMoney(paidValue - usedPaid) : roundMoney((paidValue * weights[idx]) / totalWeight);
    itemPaid = roundMoney(Math.max(0, Math.min(itemTotal, itemPaid)));
    usedPaid = roundMoney(usedPaid + itemPaid);

    const paymentState = resolvePaymentTypeFromAmounts(groupPaymentType, itemTotal, itemPaid);
    return {
      ...item,
      total_price: itemTotal,
      paid_amount: paymentState.paid_amount,
      remaining_amount: paymentState.remaining_amount,
      paymentType: paymentState.paymentType
    };
  });
}
function resolveBostaSelectedCityRow(cities=[], cityToken='', cityLabel='') {
  const token = String(cityToken || '').trim();
  const label = String(cityLabel || '').trim();
  let row = null;
  if (token) row = resolveCityBySavedToken(cities, token);
  if (!row && label) row = findBostaCityByGovernorateLabel(cities, label);
  if (!row && token.startsWith('__label__:')) row = findBostaCityByGovernorateLabel(cities, token.replace(/^__label__:/, '').trim());
  return row || null;
}
async function resolveBostaShipmentLocation({ address='', cityToken='', cityLabel='', zoneToken='' } = {}) {
  const cleanAddress = String(address || '').trim();
  const cleanCityToken = String(cityToken || '').trim();
  const cleanCityLabel = String(cityLabel || '').trim();
  const cleanZone = String(zoneToken || '').trim();
  const cities = await getBostaCitiesCached();
  const explicitRow = resolveBostaSelectedCityRow(cities, cleanCityToken, cleanCityLabel);
  const explicitInfo = cityInfoFromRow(explicitRow || {});
  let inferred = { cityCode: '', zone: '', cityLabel: '' };
  if ((!explicitInfo.code || !cleanZone) && cleanAddress) {
    inferred = await inferBostaLocation(cleanAddress, explicitInfo.code || cleanCityToken || cleanCityLabel, cleanZone);
  }
  const cityCode = String(explicitInfo.code || inferred.cityCode || '').trim();
  const cityLabelResolved = String(
    cleanCityLabel ||
    resolveConfiguredBostaGovernorateLabel(cityCode || cleanCityToken || '', cities) ||
    explicitInfo.labels[0] ||
    inferred.cityLabel ||
    cleanCityToken.replace(/^__label__:/, '').trim()
  ).trim();
  const zone = String(cleanZone || inferred.zone || '').trim();
  return { cityCode, cityLabel: cityLabelResolved, zone, inferred, cities };
}

function pickFirstNumberDeep(value, keys=[], allowPrimitive=false) {
  if (value == null) return null;
  if (allowPrimitive) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string') {
      const cleaned = value.replace(/[^0-9.\-]/g, '').trim();
      if (cleaned && !Number.isNaN(Number(cleaned))) return Number(cleaned);
    }
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = pickFirstNumberDeep(item, keys, allowPrimitive);
      if (found != null) return found;
    }
    return null;
  }
  if (typeof value === 'object') {
    const preferred = keys.map(k => String(k).toLowerCase());
    for (const [k, v] of Object.entries(value)) {
      if (preferred.includes(String(k).toLowerCase())) {
        const found = pickFirstNumberDeep(v, keys, true);
        if (found != null) return found;
      }
    }
    for (const v of Object.values(value)) {
      if (v && typeof v === 'object') {
        const found = pickFirstNumberDeep(v, keys, false);
        if (found != null) return found;
      }
    }
  }
  return null;
}
function extractBostaCharges(raw) {
  const details = extractBostaPricingBreakdown(raw);
  return {
    estimatedFees: details.estimatedFees,
    insuranceFees: details.insuranceFees,
    text: details.text,
    shippingFee: details.shippingFee,
    extraCodFee: details.extraCodFee,
    bostaMaterialFee: details.bostaMaterialFee,
    openPackageFees: details.openPackageFees,
    vatRate: details.vatRate,
    vatAmount: details.vatAmount,
    priceBeforeVat: details.priceBeforeVat,
    priceAfterVat: details.priceAfterVat,
    rawShippingFee: details.rawShippingFee
  };
}

function buildBostaPayload(order, extra={}, req=null) {
  const orderId = num(order?.id);
  const names = splitFullName(extra.receiver_name || order?.bosta_receiver_name || order?.custName || '');
  const receiverPhone = normalizePhone(extra.receiver_phone || order?.bosta_receiver_phone || order?.custPhone || '');
  const packageType = String(extra.package_type || order?.bosta_package_type || 'Parcel').trim() || 'Parcel';
  const itemsCount = Math.max(1, num(extra.items_count, order?.qty || 1));
  const packageDescription = String(extra.package_description || order?.bosta_package_description || `أوردر رقم ${orderId}`).trim();
  const addressFull = String(extra.second_line || order?.bosta_second_line || order?.custAddress || '').trim();
  const district = String(extra.district || order?.bosta_district || '').trim();
  const buildingNumber = String(extra.building_number || order?.bosta_building_number || '').trim();
  const floor = String(extra.floor || order?.bosta_floor || '').trim();
  const apartment = String(extra.apartment || order?.bosta_apartment || '').trim();
  const zone = String(extra.zone || order?.bosta_zone || '').trim();
  const cityCode = String(extra.city_code || order?.bosta_city_code || '').trim();
  const productValue = Math.max(0, num(extra.product_value, order?.bosta_product_value || order?.total_price || order?.remaining_amount || 0));
  const allowOpen = !!num(extra.allow_open, order?.bosta_allow_open || 0);
  const firstLine = [buildingNumber, district || zone, addressFull].filter(Boolean).join(' - ') || addressFull || district || zone || `ORDER-${orderId}`;
  const payload = compactObject({
    type: 'SEND',
    cod: Math.max(0, num(extra.cod, order?.bosta_cod || order?.remaining_amount || 0)),
    allowToOpenPackage: allowOpen,
    dropOffAddress: compactObject({
      cityCode,
      zone,
      firstLine,
      secondLine: addressFull || firstLine,
      district,
      buildingNumber,
      floor,
      apartment
    }),
    businessReference: String(extra.business_reference || order?.bosta_business_reference || `ORDER-${orderId}`).trim(),
    receiver: compactObject({
      firstName: names.firstName,
      lastName: names.lastName,
      email: String(extra.receiver_email || order?.bosta_receiver_email || '').trim(),
      phone: receiverPhone
    }),
    specs: compactObject({
      packageType,
      packageDetails: compactObject({
        description: packageDescription,
        itemsCount,
        itemCount: itemsCount,
        packageType,
        productValue
      }),
      description: packageDescription,
      numberOfItems: itemsCount,
      itemCount: itemsCount
    }),
    notes: String(extra.notes || order?.bosta_notes || '').trim() || `شحنة أوردر رقم ${orderId}`
  });
  const webhookUrl = resolveBostaWebhookUrl(req);
  if (webhookUrl) payload.webhookUrl = webhookUrl;
  if (BOSTA_WEBHOOK_AUTH) payload.webhookCustomHeaders = { Authorization: BOSTA_WEBHOOK_AUTH };
  return payload;
}
function extractBostaResponseInfo(raw) {
  const body = raw?.data || raw?.message || raw || {};
  const charges = extractBostaCharges(raw);
  return {
    deliveryId: String(body?._id || body?.id || body?.deliveryId || body?.delivery_id || '').trim(),
    trackingNumber: String(body?.trackingNumber || body?.tracking_number || body?.awb || '').trim(),
    statusText: String(body?.state || body?.status || 'تم الشحن').trim(),
    shippingFee: charges.shippingFee,
    estimatedFees: charges.estimatedFees,
    insuranceFees: charges.insuranceFees,
    extraCodFee: charges.extraCodFee,
    bostaMaterialFee: charges.bostaMaterialFee,
    openPackageFees: charges.openPackageFees,
    vatRate: charges.vatRate,
    vatAmount: charges.vatAmount,
    priceBeforeVat: charges.priceBeforeVat,
    priceAfterVat: charges.priceAfterVat,
    rawShippingFee: charges.rawShippingFee,
    feesText: charges.text,
    rawBody: raw || {}
  };
}
function filePath(name) {
  const p1 = path.join(PUBLIC_DIR, name);
  return fs.existsSync(p1) ? p1 : path.join(ROOT, name);
}
function permissionDefaults() {
  return {
    perm_inventory: 1,
    perm_bags: 1,
    perm_orders: 1,
    perm_add_order: 1,
    perm_edit_order: 1,
    perm_change_status: 1,
    perm_accounts: 1,
  };
}
const DETAILED_PERMISSIONS = [
  'perm_view_orders','perm_add_order','perm_edit_order','perm_delete_order','perm_change_status','perm_customers','perm_calculator',
  'perm_view_inventory','perm_manage_paper','perm_cut_paper','perm_paper_history',
  'perm_view_bags','perm_manage_bags','perm_bags_history',
  'perm_view_handles','perm_manage_handles','perm_handles_history',
  'perm_view_accounts','perm_view_financial_totals','perm_manage_expenses','perm_view_cost_logs','perm_view_sales_logs',
  'perm_manage_current_cash','perm_use_admin_cash_on_expense','perm_edit_expense_records','perm_delete_expense_records','perm_edit_cash_records','perm_delete_cash_records','perm_edit_admin_cash_records','perm_delete_admin_cash_records',
  'perm_view_debts','perm_manage_debts','perm_view_reports','perm_suppliers','perm_purchases',
  'perm_users','perm_activity_logs','perm_edit_activity_records','perm_delete_activity_records','perm_backup_restore','perm_act_as_other_admin'
];
function detailedPermissionFallback(row, key) {
  switch (key) {
    case 'perm_view_orders': return num(row?.perm_orders, 0);
    case 'perm_add_order': return num(row?.perm_add_order, 0);
    case 'perm_edit_order': return num(row?.perm_edit_order, 0);
    case 'perm_delete_order': return num(row?.perm_edit_order, 0);
    case 'perm_change_status': return num(row?.perm_change_status, 0);
    case 'perm_customers': return num(row?.perm_orders, 0);
    case 'perm_calculator': return num(row?.perm_orders, 0);
    case 'perm_view_inventory': return num(row?.perm_inventory, 0);
    case 'perm_manage_paper': return num(row?.perm_inventory, 0);
    case 'perm_cut_paper': return num(row?.perm_inventory, 0);
    case 'perm_paper_history': return num(row?.perm_inventory, 0);
    case 'perm_view_bags': return num(row?.perm_bags, 0);
    case 'perm_manage_bags': return num(row?.perm_bags, 0);
    case 'perm_bags_history': return num(row?.perm_bags, 0);
    case 'perm_view_handles': return num(row?.perm_bags, 0);
    case 'perm_manage_handles': return num(row?.perm_bags, 0);
    case 'perm_handles_history': return num(row?.perm_bags, 0);
    case 'perm_view_accounts': return num(row?.perm_accounts, 0);
    case 'perm_view_financial_totals': return num(row?.perm_accounts, 0);
    case 'perm_manage_expenses': return num(row?.perm_accounts, 0);
    case 'perm_view_cost_logs': return num(row?.perm_accounts, 0);
    case 'perm_view_sales_logs': return num(row?.perm_accounts, 0);
    case 'perm_manage_current_cash': return row?.username === 'admin' ? 1 : 0;
    case 'perm_use_admin_cash_on_expense': return row?.username === 'admin' ? 1 : 0;
    case 'perm_edit_expense_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_delete_expense_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_edit_cash_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_delete_cash_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_edit_admin_cash_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_delete_admin_cash_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_view_debts': return num(row?.perm_accounts, 0);
    case 'perm_manage_debts': return num(row?.perm_accounts, 0);
    case 'perm_view_reports': return num(row?.perm_accounts, 0);
    case 'perm_suppliers': return num(row?.perm_accounts, 0);
    case 'perm_purchases': return num(row?.perm_accounts, 0);
    case 'perm_users': return (row?.username === 'admin' || row?.role === 'admin') ? 1 : 0;
    case 'perm_activity_logs': return (row?.username === 'admin' || row?.role === 'admin') ? 1 : 0;
    case 'perm_edit_activity_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_delete_activity_records': return row?.username === 'admin' ? 1 : 0;
    case 'perm_backup_restore': return (row?.username === 'admin' || row?.role === 'admin') ? 1 : 0;
    case 'perm_act_as_other_admin': return (row?.username === 'admin' || row?.role === 'admin') ? 1 : 0;
    default: return num(row?.[key], 0);
  }
}
function getEffectivePerm(row, key) {
  if (!row) return 0;
  if (row.username === 'admin') return 1;
  if (Object.prototype.hasOwnProperty.call(row, key) && row[key] !== null && row[key] !== undefined && String(row[key]) !== '') return num(row[key], 0);
  return detailedPermissionFallback(row, key);
}
function computeLegacyPermsFromDetailed(perms) {
  return {
    perm_orders: Number(!!(num(perms.perm_view_orders) || num(perms.perm_add_order) || num(perms.perm_edit_order) || num(perms.perm_delete_order) || num(perms.perm_change_status) || num(perms.perm_customers) || num(perms.perm_calculator))),
    perm_add_order: Number(!!num(perms.perm_add_order)),
    perm_edit_order: Number(!!num(perms.perm_edit_order)),
    perm_change_status: Number(!!num(perms.perm_change_status)),
    perm_inventory: Number(!!(num(perms.perm_view_inventory) || num(perms.perm_manage_paper) || num(perms.perm_cut_paper) || num(perms.perm_paper_history))),
    perm_bags: Number(!!(num(perms.perm_view_bags) || num(perms.perm_manage_bags) || num(perms.perm_bags_history) || num(perms.perm_view_handles) || num(perms.perm_manage_handles) || num(perms.perm_handles_history))),
    perm_accounts: Number(!!(num(perms.perm_view_accounts) || num(perms.perm_manage_expenses) || num(perms.perm_view_cost_logs) || num(perms.perm_view_sales_logs) || num(perms.perm_manage_current_cash) || num(perms.perm_use_admin_cash_on_expense) || num(perms.perm_edit_expense_records) || num(perms.perm_delete_expense_records) || num(perms.perm_edit_cash_records) || num(perms.perm_delete_cash_records) || num(perms.perm_edit_admin_cash_records) || num(perms.perm_delete_admin_cash_records) || num(perms.perm_view_debts) || num(perms.perm_manage_debts) || num(perms.perm_view_reports) || num(perms.perm_suppliers) || num(perms.perm_purchases)))
  };
}
function userPermissionsPayload(row) {
  const detailed = Object.fromEntries(DETAILED_PERMISSIONS.map(key => [key, getEffectivePerm(row, key)]));
  return { ...computeLegacyPermsFromDetailed(detailed), ...detailed };
}
function collectIncomingDetailedPerms(body = {}, fallbackRow = null) {
  const base = fallbackRow ? userPermissionsPayload(fallbackRow) : userPermissionsPayload({ username:'', role:'' });
  const out = {};
  for (const key of DETAILED_PERMISSIONS) {
    if (body[key] === undefined || body[key] === null || body[key] === '') out[key] = num(base[key], 0);
    else out[key] = num(body[key], 0) === 1 ? 1 : 0;
  }
  return out;
}
function rolePreset(role) {
  const presets = {
    admin: { perm_view_orders:1, perm_add_order:1, perm_edit_order:1, perm_delete_order:1, perm_change_status:1, perm_customers:1, perm_calculator:1, perm_view_inventory:1, perm_manage_paper:1, perm_cut_paper:1, perm_paper_history:1, perm_view_bags:1, perm_manage_bags:1, perm_bags_history:1, perm_view_handles:1, perm_manage_handles:1, perm_handles_history:1, perm_view_accounts:1, perm_manage_expenses:1, perm_view_cost_logs:1, perm_view_sales_logs:1, perm_manage_current_cash:1, perm_use_admin_cash_on_expense:1, perm_edit_expense_records:1, perm_delete_expense_records:1, perm_edit_cash_records:1, perm_delete_cash_records:1, perm_edit_admin_cash_records:1, perm_delete_admin_cash_records:1, perm_view_debts:1, perm_manage_debts:1, perm_view_reports:1, perm_suppliers:1, perm_purchases:1, perm_users:1, perm_activity_logs:1, perm_edit_activity_records:1, perm_delete_activity_records:1, perm_backup_restore:1, perm_act_as_other_admin:1 },
    moderator: { perm_view_orders:1, perm_add_order:1, perm_edit_order:1, perm_delete_order:1, perm_change_status:1, perm_customers:1, perm_calculator:1, perm_view_inventory:0, perm_manage_paper:0, perm_cut_paper:0, perm_paper_history:0, perm_view_bags:0, perm_manage_bags:0, perm_bags_history:0, perm_view_handles:0, perm_manage_handles:0, perm_handles_history:0, perm_view_accounts:0, perm_manage_expenses:0, perm_view_cost_logs:0, perm_view_sales_logs:0, perm_manage_current_cash:0, perm_use_admin_cash_on_expense:0, perm_edit_expense_records:0, perm_delete_expense_records:0, perm_edit_cash_records:0, perm_delete_cash_records:0, perm_edit_admin_cash_records:0, perm_delete_admin_cash_records:0, perm_view_debts:0, perm_manage_debts:0, perm_view_reports:0, perm_suppliers:0, perm_purchases:0, perm_users:0, perm_activity_logs:0, perm_edit_activity_records:0, perm_delete_activity_records:0, perm_backup_restore:0, perm_act_as_other_admin:0 },
    operation: { perm_view_orders:1, perm_add_order:0, perm_edit_order:1, perm_delete_order:0, perm_change_status:1, perm_customers:1, perm_calculator:1, perm_view_inventory:0, perm_manage_paper:0, perm_cut_paper:0, perm_paper_history:0, perm_view_bags:0, perm_manage_bags:0, perm_bags_history:0, perm_view_handles:0, perm_manage_handles:0, perm_handles_history:0, perm_view_accounts:1, perm_manage_expenses:1, perm_view_cost_logs:1, perm_view_sales_logs:1, perm_manage_current_cash:0, perm_use_admin_cash_on_expense:0, perm_edit_expense_records:0, perm_delete_expense_records:0, perm_edit_cash_records:0, perm_delete_cash_records:0, perm_edit_admin_cash_records:0, perm_delete_admin_cash_records:0, perm_view_debts:1, perm_manage_debts:0, perm_view_reports:1, perm_suppliers:0, perm_purchases:0, perm_users:0, perm_activity_logs:0, perm_edit_activity_records:0, perm_delete_activity_records:0, perm_backup_restore:0, perm_act_as_other_admin:0 },
    production: { perm_view_orders:1, perm_add_order:1, perm_edit_order:1, perm_delete_order:0, perm_change_status:1, perm_customers:1, perm_calculator:1, perm_view_inventory:1, perm_manage_paper:1, perm_cut_paper:1, perm_paper_history:1, perm_view_bags:1, perm_manage_bags:1, perm_bags_history:1, perm_view_handles:1, perm_manage_handles:1, perm_handles_history:1, perm_view_accounts:0, perm_manage_expenses:0, perm_view_cost_logs:0, perm_view_sales_logs:0, perm_manage_current_cash:0, perm_use_admin_cash_on_expense:0, perm_edit_expense_records:0, perm_delete_expense_records:0, perm_edit_cash_records:0, perm_delete_cash_records:0, perm_edit_admin_cash_records:0, perm_delete_admin_cash_records:0, perm_view_debts:0, perm_manage_debts:0, perm_view_reports:0, perm_suppliers:0, perm_purchases:0, perm_users:0, perm_activity_logs:0, perm_edit_activity_records:0, perm_delete_activity_records:0, perm_backup_restore:0, perm_act_as_other_admin:0 },
    store: { perm_view_orders:0, perm_add_order:0, perm_edit_order:0, perm_delete_order:0, perm_change_status:0, perm_customers:0, perm_calculator:0, perm_view_inventory:1, perm_manage_paper:1, perm_cut_paper:1, perm_paper_history:1, perm_view_bags:1, perm_manage_bags:1, perm_bags_history:1, perm_view_handles:1, perm_manage_handles:1, perm_handles_history:1, perm_view_accounts:0, perm_manage_expenses:0, perm_view_cost_logs:0, perm_view_sales_logs:0, perm_manage_current_cash:0, perm_use_admin_cash_on_expense:0, perm_edit_expense_records:0, perm_delete_expense_records:0, perm_edit_cash_records:0, perm_delete_cash_records:0, perm_edit_admin_cash_records:0, perm_delete_admin_cash_records:0, perm_view_debts:0, perm_manage_debts:0, perm_view_reports:0, perm_suppliers:0, perm_purchases:0, perm_users:0, perm_activity_logs:0, perm_edit_activity_records:0, perm_delete_activity_records:0, perm_backup_restore:0, perm_act_as_other_admin:0 },
    accountant: { perm_view_orders:1, perm_add_order:0, perm_edit_order:0, perm_delete_order:0, perm_change_status:0, perm_customers:1, perm_calculator:0, perm_view_inventory:1, perm_manage_paper:0, perm_cut_paper:0, perm_paper_history:1, perm_view_bags:1, perm_manage_bags:0, perm_bags_history:1, perm_view_handles:1, perm_manage_handles:0, perm_handles_history:1, perm_view_accounts:1, perm_manage_expenses:1, perm_view_cost_logs:1, perm_view_sales_logs:1, perm_manage_current_cash:0, perm_use_admin_cash_on_expense:0, perm_edit_expense_records:0, perm_delete_expense_records:0, perm_edit_cash_records:0, perm_delete_cash_records:0, perm_edit_admin_cash_records:0, perm_delete_admin_cash_records:0, perm_view_debts:1, perm_manage_debts:1, perm_view_reports:1, perm_suppliers:1, perm_purchases:1, perm_users:0, perm_activity_logs:0, perm_edit_activity_records:0, perm_delete_activity_records:0, perm_backup_restore:0, perm_act_as_other_admin:0 }
  };
  const detailed = { ...(presets[String(role || '').trim()] || {}) };
  return { ...computeLegacyPermsFromDetailed(detailed), ...detailed };
}
function normalizeRoleName(role) {
  const value = String(role || '').trim();
  return value || 'moderator';
}
function bagColorMatchesPaper(bagColor, paperColor) {
  return String(bagColor || '').trim() === String(paperColor || '').trim();
}
function paperLabelBase(paper, { includeName = true } = {}) {
  const size = `${num(paper?.length)}×${num(paper?.width)}`;
  const gram = `${num(paper?.grammage)} جم`;
  const name = String(paper?.paper_name || '').trim();
  return `${size} - ${gram}${includeName && name ? ` - ${name}` : ''}`;
}
function paperLabelFull(paper, { includeName = true } = {}) {
  const color = String(paper?.color || '').trim();
  return `${color ? color + ' ' : ''}${paperLabelBase(paper, { includeName })}`.trim();
}
function userSafe(row) {
  return {
    id: row.id,
    username: row.username,
    full_name: row.full_name,
    role: row.role,
    is_active: row.is_active,
    ...userPermissionsPayload(row)
  };
}
function hasPerm(user, key) {
  return getEffectivePerm(user, key) === 1;
}
function canManageInventory(user) {
  return !!user && hasPerm(user, 'perm_manage_paper');
}
function canManageBags(user) {
  return !!user && hasPerm(user, 'perm_manage_bags');
}
function canSeeFullAccounts(user) {
  return !!user && hasPerm(user, 'perm_view_accounts');
}
function canViewFinancialTotals(user) {
  return !!user && hasPerm(user, 'perm_view_financial_totals');
}
function requirePerm(key) {
  return (req, res, next) => {
    if (hasPerm(req.user, key)) return next();
    return res.status(403).json({ error: 'غير مصرح' });
  };
}
function requireAnyPerm(...keys) {
  return (req, res, next) => {
    if (keys.some(key => hasPerm(req.user, key))) return next();
    return res.status(403).json({ error: 'غير مصرح' });
  };
}
function requireAdmin(req, res, next) {
  if (req.user?.role === 'admin' || req.user?.username === 'admin') return next();
  return res.status(403).json({ error: 'غير مصرح' });
}
async function closeDbAsync() {
  return new Promise((resolve, reject) => db.close(err => err ? reject(err) : resolve()));
}
function removeDirContentsSync(dir) {
  if (!fs.existsSync(dir)) return;
  for (const entry of fs.readdirSync(dir)) {
    fs.rmSync(path.join(dir, entry), { recursive: true, force: true });
  }
}
function streamZipFile(zipPath) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    output.on('close', resolve);
    archive.on('error', reject);
    archive.pipe(output);
    if (fs.existsSync(DB_PATH)) archive.file(DB_PATH, { name: 'database.db' });
    if (fs.existsSync(UPLOAD_DIR)) archive.directory(UPLOAD_DIR, 'uploads');
    archive.append(JSON.stringify({
      exportedAt: new Date().toISOString(),
      app: 'bag-orders-system',
      dataDir: DATA_DIR,
      dbFile: 'database.db',
      uploadDir: 'uploads'
    }, null, 2), { name: 'backup-meta.json' });
    archive.finalize();
  });
}
async function restoreFromBackupFile(filePath, originalName = '') {
  const ext = path.extname(originalName || filePath).toLowerCase();
  if (ext === '.db' || ext === '.sqlite' || ext === '.sqlite3') {
    await closeDbAsync();
    fs.copyFileSync(filePath, DB_PATH);
    db = openDatabaseConnection();
    return { type: 'db' };
  }
  if (ext !== '.zip') throw new Error('صيغة الملف غير مدعومة. ارفع ملف zip أو db أو sqlite');
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bag-backup-'));
  await fs.createReadStream(filePath).pipe(unzipper.Extract({ path: tempDir })).promise();
  const extractedDb = path.join(tempDir, 'database.db');
  if (!fs.existsSync(extractedDb)) throw new Error('ملف النسخة الاحتياطية لا يحتوي على database.db');
  const extractedUploads = path.join(tempDir, 'uploads');
  await closeDbAsync();
  fs.copyFileSync(extractedDb, DB_PATH);
  removeDirContentsSync(UPLOAD_DIR);
  ensureDirSync(UPLOAD_DIR);
  if (fs.existsSync(extractedUploads)) copyDirSync(extractedUploads, UPLOAD_DIR);
  db = openDatabaseConnection();
  return { type: 'zip' };
}


function clientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return forwarded || req.socket?.remoteAddress || req.ip || '';
}
function getUserAgent(req) {
  return String(req.headers['user-agent'] || '').trim();
}
function detectDeviceInfo(ua = '') {
  const source = String(ua || '').toLowerCase();
  let deviceType = 'كمبيوتر';
  if (/tablet|ipad/.test(source)) deviceType = 'تابلت';
  else if (/mobile|android|iphone/.test(source)) deviceType = 'موبايل';
  let os = 'غير معروف';
  if (/windows/.test(source)) os = 'Windows';
  else if (/android/.test(source)) os = 'Android';
  else if (/iphone|ipad|ios/.test(source)) os = 'iOS';
  else if (/mac os|macintosh/.test(source)) os = 'macOS';
  else if (/linux/.test(source)) os = 'Linux';
  let browser = 'غير معروف';
  if (/edg\//.test(source)) browser = 'Edge';
  else if (/chrome\//.test(source) && !/edg\//.test(source)) browser = 'Chrome';
  else if (/firefox\//.test(source)) browser = 'Firefox';
  else if (/safari\//.test(source) && !/chrome\//.test(source)) browser = 'Safari';
  return { deviceType, os, browser, label: `${deviceType} | ${os} | ${browser}` };
}
function normalizePriority(value) {
  const allowed = ['عادي', 'مستعجل', 'عاجل'];
  const v = String(value || '').trim();
  return allowed.includes(v) ? v : 'عادي';
}
function isClosedOrderStatus(status) {
  return ['تم التسليم', 'مرتجع'].includes(String(status || '').trim());
}
function normalizeOrderStatus(status='') {
  const value = String(status || '').trim();
  if (!value) return 'أوردر جديد';
  if (['يوجد مشكلة', 'في مشكلة'].includes(value)) return 'في مشكله';
  return value;
}
const ALL_ORDER_STATUSES = ['أوردر جديد','تحت الإنتاج','في القص','مستني الزنكات','تحت الطباعة','تحت التصنيع','جاهز للشحن','تم الشحن','تم التسليم','مرتجع','في مشكله'];
const READY_STOCK_ORDER_STATUSES = ['أوردر جديد','جاهز للشحن','تم الشحن','تم التسليم','مرتجع','في مشكله'];
function allowedOrderStatuses(order) {
  return num(order?.useReadyStock) === 1 ? READY_STOCK_ORDER_STATUSES : ALL_ORDER_STATUSES;
}
function ensureAllowedOrderStatus(order, status) {
  const normalized = normalizeOrderStatus(status);
  if (!allowedOrderStatuses(order).includes(normalized)) {
    throw new Error(num(order?.useReadyStock) === 1 ? 'حالات أوردرات مخزن الشنط الجاهزة محددة فقط' : 'حالة الأوردر غير صحيحة');
  }
  return normalized;
}
function isPrintedOrder(printType) {
  const v = String(printType || '').trim();
  return v === 'سلك سكرين' || v === 'أوفست';
}
function executionStepMeta(stepType) {
  const map = {
    stock: { label: 'صرف جاهز', partner_type: 'مخزن' },
    cut: { label: 'قص ورق', partner_type: 'قص' },
    plate: { label: 'تجهيز زنكة/تصميم', partner_type: 'مطبعة' },
    print: { label: 'طباعة', partner_type: 'مطبعة' },
    make: { label: 'تصنيع', partner_type: 'صنايعي' },
    handle: { label: 'تركيب يد', partner_type: 'تركيب يد' },
    deliver: { label: 'تجهيز/تسليم', partner_type: 'أخرى' }
  };
  return map[String(stepType || '').trim()] || { label: String(stepType || '').trim() || 'خطوة', partner_type: 'أخرى' };
}
async function recordAudit({ req=null, user=null, action='', entity_type='', entity_id=null, details='', can_undo=0, undo_type='', undo_payload='[]', touch_refs=[] } = {}) {
  try {
    const actor = user || req?.user || {};
    const ua = req ? getUserAgent(req) : '';
    const device = detectDeviceInfo(ua);
    const refs = normalizeTouchRefs(touch_refs, entity_type, entity_id);
    const payloadText = typeof undo_payload === 'string' ? undo_payload : safeJsonStringify(undo_payload, '[]');
    await runAsync(`INSERT INTO audit_logs (username,full_name,action,entity_type,entity_id,details,ip_address,user_agent,device_type,device_label,can_undo,undo_type,undo_payload,touch_refs) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [
      String(actor.username || '').trim(),
      String(actor.full_name || '').trim(),
      String(action || '').trim(),
      String(entity_type || '').trim(),
      entity_id == null ? null : num(entity_id),
      String(details || '').trim(),
      req ? clientIp(req) : '',
      ua,
      device.deviceType,
      device.label,
      num(can_undo) === 1 ? 1 : 0,
      String(undo_type || '').trim(),
      payloadText || '[]',
      safeJsonStringify(refs, '[]')
    ]);
  } catch (_) {}
}
async function closeSessionHistory(sessionRow, req = null) {
  try {
    if (!sessionRow?.session_login_id) return;
    const logoutAt = new Date();
    const started = sessionRow.session_started_at ? new Date(sessionRow.session_started_at) : null;
    const durationSeconds = started ? Math.max(0, Math.round((logoutAt - started) / 1000)) : 0;
    await runAsync(`UPDATE user_sessions_history SET logout_at=?, duration_seconds=?, is_active=0 WHERE id=?`, [logoutAt.toISOString(), durationSeconds, num(sessionRow.session_login_id)]);
    if (req) {
      await recordAudit({ req, user: sessionRow, action: 'logout', entity_type: 'session', entity_id: sessionRow.session_login_id, details: `مدة الجلسة ${durationSeconds} ثانية` });
    }
  } catch (_) {}
}

const sessions = new Map();
function clearExpiredSessions() {
  const now = Date.now();
  for (const [token, sessionRow] of sessions.entries()) {
    const startedAt = sessionRow?.session_started_at ? new Date(sessionRow.session_started_at).getTime() : 0;
    if (!startedAt || (now - startedAt) > SESSION_TTL_MS) sessions.delete(token);
  }
}
function authRequired(req, res, next) {
  clearExpiredSessions();
  const headerToken = String(req.headers.authorization || req.headers.Authorization || req.headers['x-access-token'] || '').replace('Bearer ', '').trim();
  const queryToken = String(req.query?.token || '').trim();
  const token = headerToken || queryToken;
  if (!token || !sessions.has(token)) return res.status(401).json({ error: 'unauthorized' });
  const sessionRow = sessions.get(token);
  const startedAt = sessionRow?.session_started_at ? new Date(sessionRow.session_started_at).getTime() : 0;
  if (!startedAt || (Date.now() - startedAt) > SESSION_TTL_MS) {
    sessions.delete(token);
    return res.status(401).json({ error: 'انتهت الجلسة، سجل دخول مرة أخرى' });
  }
  req.user = sessionRow;
  req.token = token;
  next();
}

function safeDownloadFilename(name='') {
  return path.basename(String(name || '').trim());
}
app.get('/protected-file/:bucket/:name', authRequired, (req, res) => {
  try {
    const bucket = String(req.params.bucket || '').trim().toLowerCase();
    const fileName = safeDownloadFilename(req.params.name);
    if (!fileName) return res.status(404).json({ error: 'الملف غير موجود' });
    let baseDir = '';
    if (bucket === 'uploads') baseDir = UPLOAD_DIR;
    else if (bucket === 'backups') baseDir = BACKUP_DIR;
    else return res.status(404).json({ error: 'المجلد غير مدعوم' });
    const target = path.resolve(path.join(baseDir, fileName));
    if (!target.startsWith(baseDir) || !fs.existsSync(target) || !fs.statSync(target).isFile()) {
      return res.status(404).json({ error: 'الملف غير موجود' });
    }
    return res.sendFile(target);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

async function ensureSchema() {
  await runAsync(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    password TEXT,
    full_name TEXT,
    role TEXT DEFAULT 'admin',
    is_active INTEGER DEFAULT 1
  )`);
  for (const [name, def] of Object.entries({
    perm_inventory: 'INTEGER DEFAULT 1',
    perm_bags: 'INTEGER DEFAULT 1',
    perm_orders: 'INTEGER DEFAULT 1',
    perm_add_order: 'INTEGER DEFAULT 1',
    perm_edit_order: 'INTEGER DEFAULT 1',
    perm_change_status: 'INTEGER DEFAULT 1',
    perm_accounts: 'INTEGER DEFAULT 1'
  })) await addColumnIfMissing('users', name, def);

  for (const name of DETAILED_PERMISSIONS) await addColumnIfMissing('users', name, 'INTEGER');

  await runAsync(`CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    custName TEXT,
    custPhone TEXT,
    custAddress TEXT,
    orderDate TEXT DEFAULT CURRENT_DATE,
    l REAL,w REAL,g REAL,qty INTEGER,
    color TEXT,handle TEXT,printType TEXT,colorSpecs TEXT,
    total_price REAL,paid_amount REAL,remaining_amount REAL,paymentType TEXT,
    status TEXT DEFAULT 'أوردر جديد'
  )`);
  await addColumnIfMissing('orders', 'customer_id', 'INTEGER DEFAULT 0');
  for (const [name, def] of [
    ['cost_cut', 'REAL DEFAULT 0'],['cost_print', 'REAL DEFAULT 0'],['cost_zinc', 'REAL DEFAULT 0'],['cost_design', 'REAL DEFAULT 0'],
    ['cost_make', 'REAL DEFAULT 0'],['cost_hand', 'REAL DEFAULT 0'],['cost_paper', 'REAL DEFAULT 0'],['cost_hand_fix', 'REAL DEFAULT 0'],
    ['paperGrammage', 'REAL DEFAULT 0'],['useReadyStock', 'INTEGER DEFAULT 0'],['handle_stock_deducted', 'INTEGER DEFAULT 0'],
    ['bag_returned_to_stock', 'INTEGER DEFAULT 0'],
    ['paper_cut_done', 'INTEGER DEFAULT 0'],['last_cut_layout', "TEXT DEFAULT 'pieceByPiece'"],['last_cut_paper_label', 'TEXT'],['last_cut_paper_id', 'INTEGER DEFAULT 0'],
    ['priority', "TEXT DEFAULT 'عادي'"],['due_date', 'TEXT'],['notes', "TEXT DEFAULT ''"],['shipping_cost', 'REAL DEFAULT 0'],['group_code', "TEXT DEFAULT ''"],['item_no', 'INTEGER DEFAULT 1'],['item_count', 'INTEGER DEFAULT 1'],
    ['bosta_delivery_id', "TEXT DEFAULT ''"],['bosta_tracking_number', "TEXT DEFAULT ''"],['bosta_status', "TEXT DEFAULT ''"],['bosta_sent_at', 'TEXT'],['bosta_city_code', "TEXT DEFAULT ''"],['bosta_zone', "TEXT DEFAULT ''"],['bosta_district', "TEXT DEFAULT ''"],['bosta_building_number', "TEXT DEFAULT ''"],['bosta_floor', "TEXT DEFAULT ''"],['bosta_apartment', "TEXT DEFAULT ''"],['bosta_second_line', "TEXT DEFAULT ''"],['bosta_receiver_name', "TEXT DEFAULT ''"],['bosta_receiver_phone', "TEXT DEFAULT ''"],['bosta_receiver_email', "TEXT DEFAULT ''"],['bosta_business_reference', "TEXT DEFAULT ''"],['bosta_package_type', "TEXT DEFAULT 'Parcel'"],['bosta_package_description', "TEXT DEFAULT ''"],['bosta_product_value', 'REAL DEFAULT 0'],['bosta_allow_open', 'INTEGER DEFAULT 0'],['bosta_shipping_fee', 'REAL DEFAULT 0'],['bosta_raw_shipping_fee', 'REAL DEFAULT 0'],['bosta_open_package_fees', 'REAL DEFAULT 0'],['bosta_material_fee', 'REAL DEFAULT 0'],['bosta_extra_cod_fee', 'REAL DEFAULT 0'],['bosta_expedite_fee', 'REAL DEFAULT 0'],['bosta_vat_rate', 'REAL DEFAULT 0'],['bosta_vat_amount', 'REAL DEFAULT 0'],['bosta_price_before_vat', 'REAL DEFAULT 0'],['bosta_price_after_vat', 'REAL DEFAULT 0'],['bosta_estimate_source', "TEXT DEFAULT ''"],['bosta_estimated_fees', 'REAL DEFAULT 0'],['bosta_insurance_fees', 'REAL DEFAULT 0'],['bosta_estimated_fees_text', "TEXT DEFAULT ''"],['bosta_cod', 'REAL DEFAULT 0'],['bosta_notes', "TEXT DEFAULT ''"],['bosta_last_response', "TEXT DEFAULT ''"]
  ]) await addColumnIfMissing('orders', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS order_status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    from_status TEXT,
    to_status TEXT,
    changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    note TEXT
  )`);
  await runAsync(`CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    phone TEXT,
    phone_normalized TEXT,
    address TEXT,
    governorate TEXT DEFAULT '',
    zone TEXT DEFAULT '',
    email TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    is_active INTEGER DEFAULT 1,
    is_vip INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_order_date TEXT,
    last_order_id INTEGER DEFAULT 0
  )`);
  for (const [name, def] of [['phone_normalized', "TEXT DEFAULT ''"], ['governorate', "TEXT DEFAULT ''"], ['zone', "TEXT DEFAULT ''"], ['email', "TEXT DEFAULT ''"], ['notes', "TEXT DEFAULT ''"], ['is_active', 'INTEGER DEFAULT 1'], ['is_vip', 'INTEGER DEFAULT 0'], ['updated_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['last_order_date', 'TEXT'], ['last_order_id', 'INTEGER DEFAULT 0']]) await addColumnIfMissing('customers', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS order_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    customer_id INTEGER DEFAULT 0,
    amount REAL DEFAULT 0,
    payment_date TEXT DEFAULT CURRENT_DATE,
    method TEXT DEFAULT 'نقدي',
    note TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT ''
  )`);
  for (const [name, def] of [['customer_id', 'INTEGER DEFAULT 0'], ['method', "TEXT DEFAULT 'نقدي'"], ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('order_payments', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS bags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    length REAL,width REAL,gusset REAL,color TEXT,handle TEXT,
    total_qty INTEGER DEFAULT 0,min_qty INTEGER DEFAULT 0,buy_price REAL DEFAULT 0,sell_price REAL DEFAULT 0
  )`);
  for (const [name, def] of [['min_qty', 'INTEGER DEFAULT 0'],['buy_price', 'REAL DEFAULT 0'],['sell_price', 'REAL DEFAULT 0']]) await addColumnIfMissing('bags', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS bags_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bag_id INTEGER,
    order_id INTEGER,
    type TEXT,
    qty INTEGER,
    date TEXT DEFAULT CURRENT_TIMESTAMP,
    color TEXT,handle TEXT,length REAL,width REAL,gusset REAL,note TEXT
  )`);

  await runAsync(`CREATE TABLE IF NOT EXISTS handles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    color TEXT UNIQUE,
    qty INTEGER DEFAULT 0,
    buy_price REAL DEFAULT 0,
    min_qty INTEGER DEFAULT 0
  )`);
  for (const [name, def] of [['min_qty', 'INTEGER DEFAULT 0']]) await addColumnIfMissing('handles', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS handles_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    handle_id INTEGER,
    order_id INTEGER,
    type TEXT,
    qty INTEGER DEFAULT 0,
    color TEXT,
    note TEXT,
    date TEXT DEFAULT CURRENT_TIMESTAMP
  )`);
  await addColumnIfMissing('handles_history', 'order_id', 'INTEGER');

  await runAsync(`CREATE TABLE IF NOT EXISTS cost_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cost_date TEXT DEFAULT CURRENT_DATE,
    order_id INTEGER,
    cost_field TEXT,
    amount REAL DEFAULT 0,
    source TEXT,
    source_ref TEXT,
    notes TEXT,
    created_by TEXT
  )`);

  await runAsync(`CREATE TABLE IF NOT EXISTS sales_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date TEXT DEFAULT CURRENT_DATE,
    order_id INTEGER UNIQUE,
    total_sale REAL DEFAULT 0,
    total_cost REAL DEFAULT 0,
    net_profit REAL DEFAULT 0,
    paid_amount REAL DEFAULT 0,
    remaining_amount REAL DEFAULT 0,
    customer_name TEXT,
    status TEXT,
    notes TEXT,
    created_by TEXT
  )`);
  for (const [name, def] of [['gross_sale', 'REAL DEFAULT 0'], ['shipping_cost', 'REAL DEFAULT 0'], ['insurance_fee', 'REAL DEFAULT 0'], ['extra_cod_fee', 'REAL DEFAULT 0'], ['total_deductions', 'REAL DEFAULT 0']]) await addColumnIfMissing('sales_history', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS paper (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    length REAL,width REAL,grammage REAL,color TEXT,
    total_kg REAL DEFAULT 0,total_sheets REAL DEFAULT 0,min_kg REAL DEFAULT 0,min_sheets REAL DEFAULT 0
  )`);
  for (const [name, def] of [['paper_name', "TEXT DEFAULT ''"],['min_kg', 'REAL DEFAULT 0'],['min_sheets', 'REAL DEFAULT 0'],['buy_price_kg', 'REAL DEFAULT 0'],['buy_price_sheet', 'REAL DEFAULT 0']]) await addColumnIfMissing('paper', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS paper_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    paper_id INTEGER,type TEXT,kg REAL DEFAULT 0,sheets REAL DEFAULT 0,date TEXT DEFAULT CURRENT_TIMESTAMP,
    color TEXT,length REAL,width REAL,grammage REAL,paper_name TEXT DEFAULT '',note TEXT
  )`);
  await addColumnIfMissing('paper_history', 'paper_name', "TEXT DEFAULT ''");

  await runAsync(`CREATE TABLE IF NOT EXISTS production_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    production_date TEXT DEFAULT CURRENT_TIMESTAMP,
    paper_id INTEGER,
    bag_id INTEGER,
    qty INTEGER DEFAULT 0,
    color TEXT,
    handle TEXT,
    length REAL,
    width REAL,
    gusset REAL,
    layout_key TEXT,
    layout_label TEXT,
    paper_label TEXT,
    paper_kg REAL DEFAULT 0,
    paper_sheets REAL DEFAULT 0,
    cost_paper REAL DEFAULT 0,
    cost_cut REAL DEFAULT 0,
    cost_make REAL DEFAULT 0,
    cost_hand REAL DEFAULT 0,
    cost_hand_fix REAL DEFAULT 0,
    total_cost REAL DEFAULT 0,
    unit_cost REAL DEFAULT 0,
    note TEXT,
    created_by TEXT
  )`);
  for (const [name, def] of [
    ['cost_hand', 'REAL DEFAULT 0'],
    ['status', "TEXT DEFAULT 'done'"],
    ['completed_at', 'TEXT'],
    ['completed_by', "TEXT DEFAULT ''"],
    ['source_type', "TEXT DEFAULT 'ready_bags'"],
    ['source_ref_id', 'INTEGER DEFAULT 0'],
    ['pricing_mode', "TEXT DEFAULT 'auto_from_margin'"],
    ['profit_margin', 'REAL DEFAULT 0'],
    ['bag_buy_price', 'REAL DEFAULT 0'],
    ['bag_sell_price', 'REAL DEFAULT 0'],
    ['reserve_handle_id', 'INTEGER DEFAULT 0']
  ]) await addColumnIfMissing('production_orders', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS capital_settings (
    id INTEGER PRIMARY KEY CHECK (id=1),
    opening_capital REAL DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT DEFAULT ''
  )`);
  await runAsync(`INSERT OR IGNORE INTO capital_settings (id, opening_capital, updated_at, updated_by) VALUES (1,0,CURRENT_TIMESTAMP,'system')`);

  await runAsync(`CREATE TABLE IF NOT EXISTS cash_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    adjustment_date TEXT DEFAULT CURRENT_DATE,
    action_type TEXT DEFAULT 'set',
    amount REAL DEFAULT 0,
    delta REAL DEFAULT 0,
    previous_balance REAL DEFAULT 0,
    new_balance REAL DEFAULT 0,
    reason TEXT DEFAULT '',
    note TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT ''
  )`);
  for (const [name, def] of [['adjustment_date', "TEXT DEFAULT CURRENT_DATE"], ['action_type', "TEXT DEFAULT 'set'"], ['amount', 'REAL DEFAULT 0'], ['delta', 'REAL DEFAULT 0'], ['previous_balance', 'REAL DEFAULT 0'], ['new_balance', 'REAL DEFAULT 0'], ['reason', "TEXT DEFAULT ''"], ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('cash_adjustments', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS order_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    originalname TEXT,
    filename TEXT,
    filepath TEXT,
    mimetype TEXT
  )`);

  await runAsync(`CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    expense_date TEXT DEFAULT CURRENT_DATE,
    amount REAL DEFAULT 0,
    category TEXT,
    custom_category TEXT,
    notes TEXT,
    linked_to_order INTEGER DEFAULT 0,
    order_id INTEGER,
    order_cost_field TEXT,
    expense_partner_name TEXT DEFAULT '',
    actor_username TEXT DEFAULT '',
    actor_name TEXT DEFAULT '',
    created_by TEXT
  )`);
  for (const [name, def] of [['expense_date', "TEXT DEFAULT CURRENT_DATE"], ['amount', 'REAL DEFAULT 0'], ['category', "TEXT DEFAULT ''"], ['custom_category', "TEXT DEFAULT ''"], ['notes', "TEXT DEFAULT ''"], ['linked_to_order', 'INTEGER DEFAULT 0'], ['order_id', 'INTEGER'], ['order_cost_field', "TEXT DEFAULT ''"], ['expense_partner_name', "TEXT DEFAULT ''"], ['actor_username', "TEXT DEFAULT ''"], ['actor_name', "TEXT DEFAULT ''"], ['execution_partner_id', 'INTEGER DEFAULT 0'], ['execution_partner_name', "TEXT DEFAULT ''"], ['execution_partner_type', "TEXT DEFAULT ''"], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('expenses', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS partner_withdrawals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partner_name TEXT DEFAULT '',
    amount REAL DEFAULT 0,
    withdrawal_date TEXT DEFAULT CURRENT_DATE,
    note TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT ''
  )`);
  for (const [name, def] of [['partner_name', "TEXT DEFAULT ''"], ['amount', 'REAL DEFAULT 0'], ['withdrawal_date', "TEXT DEFAULT CURRENT_DATE"], ['note', "TEXT DEFAULT ''"], ['created_at', "TEXT DEFAULT CURRENT_TIMESTAMP"], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('partner_withdrawals', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS partner_fund_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partner_name TEXT DEFAULT '',
    entry_date TEXT DEFAULT CURRENT_DATE,
    entry_kind TEXT DEFAULT 'add',
    amount REAL DEFAULT 0,
    delta REAL DEFAULT 0,
    balance_before REAL DEFAULT 0,
    balance_after REAL DEFAULT 0,
    note TEXT DEFAULT '',
    source_type TEXT DEFAULT '',
    source_ref TEXT DEFAULT '',
    is_auto INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT ''
  )`);
  for (const [name, def] of [['partner_name', "TEXT DEFAULT ''"], ['entry_date', "TEXT DEFAULT CURRENT_DATE"], ['entry_kind', "TEXT DEFAULT 'add'"], ['amount', 'REAL DEFAULT 0'], ['delta', 'REAL DEFAULT 0'], ['balance_before', 'REAL DEFAULT 0'], ['balance_after', 'REAL DEFAULT 0'], ['note', "TEXT DEFAULT ''"], ['source_type', "TEXT DEFAULT ''"], ['source_ref', "TEXT DEFAULT ''"], ['is_auto', 'INTEGER DEFAULT 0'], ['created_at', "TEXT DEFAULT CURRENT_TIMESTAMP"], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('partner_fund_ledger', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS admin_cash_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_username TEXT DEFAULT '',
    admin_name TEXT DEFAULT '',
    entry_date TEXT DEFAULT CURRENT_DATE,
    entry_kind TEXT DEFAULT 'add',
    amount REAL DEFAULT 0,
    delta REAL DEFAULT 0,
    balance_before REAL DEFAULT 0,
    balance_after REAL DEFAULT 0,
    note TEXT DEFAULT '',
    source_type TEXT DEFAULT '',
    source_ref TEXT DEFAULT '',
    related_admin_username TEXT DEFAULT '',
    related_admin_name TEXT DEFAULT '',
    is_auto INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT ''
  )`);
  for (const [name, def] of [['admin_username', "TEXT DEFAULT ''"], ['admin_name', "TEXT DEFAULT ''"], ['entry_date', "TEXT DEFAULT CURRENT_DATE"], ['entry_kind', "TEXT DEFAULT 'add'"], ['amount', 'REAL DEFAULT 0'], ['delta', 'REAL DEFAULT 0'], ['balance_before', 'REAL DEFAULT 0'], ['balance_after', 'REAL DEFAULT 0'], ['note', "TEXT DEFAULT ''"], ['source_type', "TEXT DEFAULT ''"], ['source_ref', "TEXT DEFAULT ''"], ['related_admin_username', "TEXT DEFAULT ''"], ['related_admin_name', "TEXT DEFAULT ''"], ['is_auto', 'INTEGER DEFAULT 0'], ['created_at', "TEXT DEFAULT CURRENT_TIMESTAMP"], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('admin_cash_ledger', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    username TEXT,
    full_name TEXT,
    action TEXT,
    entity_type TEXT,
    entity_id INTEGER,
    details TEXT,
    ip_address TEXT,
    user_agent TEXT,
    device_type TEXT,
    device_label TEXT
  )`);
  for (const [name, def] of [
    ['can_undo', 'INTEGER DEFAULT 0'],
    ['undo_type', 'TEXT'],
    ['undo_payload', 'TEXT'],
    ['touch_refs', 'TEXT'],
    ['reverted_at', 'TEXT'],
    ['reverted_by', 'TEXT'],
    ['edited_at', 'TEXT'],
    ['edited_by', 'TEXT']
  ]) await addColumnIfMissing('audit_logs', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS user_sessions_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    full_name TEXT,
    login_at TEXT DEFAULT CURRENT_TIMESTAMP,
    logout_at TEXT,
    duration_seconds INTEGER DEFAULT 0,
    ip_address TEXT,
    user_agent TEXT,
    device_type TEXT,
    device_label TEXT,
    is_active INTEGER DEFAULT 1
  )`);

  await runAsync(`CREATE TABLE IF NOT EXISTS debts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    creditor_name TEXT,
    debt_type TEXT,
    subject TEXT,
    total_amount REAL DEFAULT 0,
    paid_amount REAL DEFAULT 0,
    remaining_amount REAL DEFAULT 0,
    due_date TEXT,
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT
  )`);

  await runAsync(`CREATE TABLE IF NOT EXISTS debt_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    debt_id INTEGER,
    amount REAL DEFAULT 0,
    payment_date TEXT DEFAULT CURRENT_DATE,
    note TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT
  )`);

  await runAsync(`CREATE TABLE IF NOT EXISTS manual_receivables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    debtor_name TEXT,
    receivable_type TEXT,
    subject TEXT,
    total_amount REAL DEFAULT 0,
    paid_amount REAL DEFAULT 0,
    remaining_amount REAL DEFAULT 0,
    due_date TEXT,
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT
  )`);

  await runAsync(`CREATE TABLE IF NOT EXISTS manual_receivable_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    receivable_id INTEGER,
    amount REAL DEFAULT 0,
    payment_date TEXT DEFAULT CURRENT_DATE,
    note TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT
  )`);


  await runAsync(`CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    supplier_type TEXT DEFAULT 'ورق',
    phone TEXT DEFAULT '',
    address TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    opening_balance REAL DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);
  for (const [name, def] of [['supplier_type', "TEXT DEFAULT 'ورق'"], ['phone', "TEXT DEFAULT ''"], ['address', "TEXT DEFAULT ''"], ['notes', "TEXT DEFAULT ''"], ['opening_balance', 'REAL DEFAULT 0'], ['is_active', 'INTEGER DEFAULT 1'], ['updated_at', 'TEXT DEFAULT CURRENT_TIMESTAMP']]) await addColumnIfMissing('suppliers', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS purchases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_date TEXT DEFAULT CURRENT_DATE,
    supplier_id INTEGER DEFAULT 0,
    supplier_name TEXT DEFAULT '',
    item_type TEXT DEFAULT 'خامة أخرى',
    item_name TEXT DEFAULT '',
    quantity REAL DEFAULT 0,
    unit TEXT DEFAULT 'وحدة',
    unit_price REAL DEFAULT 0,
    total_price REAL DEFAULT 0,
    paid_amount REAL DEFAULT 0,
    remaining_amount REAL DEFAULT 0,
    due_date TEXT,
    notes TEXT DEFAULT '',
    stock_type TEXT DEFAULT '',
    stock_ref_id INTEGER DEFAULT 0,
    stock_applied INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT ''
  )`);
  for (const [name, def] of [['supplier_name', "TEXT DEFAULT ''"], ['item_type', "TEXT DEFAULT 'خامة أخرى'"], ['item_name', "TEXT DEFAULT ''"], ['quantity', 'REAL DEFAULT 0'], ['unit', "TEXT DEFAULT 'وحدة'"], ['unit_price', 'REAL DEFAULT 0'], ['total_price', 'REAL DEFAULT 0'], ['paid_amount', 'REAL DEFAULT 0'], ['remaining_amount', 'REAL DEFAULT 0'], ['notes', "TEXT DEFAULT ''"], ['stock_type', "TEXT DEFAULT ''"], ['stock_ref_id', 'INTEGER DEFAULT 0'], ['stock_applied', 'INTEGER DEFAULT 0'], ['stock_mode', "TEXT DEFAULT 'existing'"], ['invoice_group_no', "TEXT DEFAULT ''"], ['paper_length', 'REAL DEFAULT 0'], ['paper_width', 'REAL DEFAULT 0'], ['paper_grammage', 'REAL DEFAULT 0'], ['paper_color', "TEXT DEFAULT ''"], ['handle_color', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('purchases', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS purchase_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_id INTEGER,
    supplier_id INTEGER DEFAULT 0,
    amount REAL DEFAULT 0,
    payment_date TEXT DEFAULT CURRENT_DATE,
    note TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT ''
  )`);
  for (const [name, def] of [['supplier_id', 'INTEGER DEFAULT 0'], ['amount', 'REAL DEFAULT 0'], ['payment_date', 'TEXT DEFAULT CURRENT_DATE'], ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('purchase_payments', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS execution_partners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    partner_type TEXT,
    phone TEXT,
    address TEXT,
    notes TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);
  for (const [name, def] of [['phone', "TEXT DEFAULT ''"], ['address', "TEXT DEFAULT ''"], ['notes', "TEXT DEFAULT ''"], ['is_active', 'INTEGER DEFAULT 1']]) await addColumnIfMissing('execution_partners', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS order_operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    step_type TEXT,
    step_label TEXT,
    partner_id INTEGER,
    partner_name TEXT,
    partner_type TEXT,
    reference_code TEXT,
    qty INTEGER DEFAULT 0,
    paper_sheets REAL DEFAULT 0,
    paper_kg REAL DEFAULT 0,
    amount REAL DEFAULT 0,
    status TEXT DEFAULT 'pending',
    note TEXT,
    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT
  )`);
  for (const [name, def] of [['step_label', "TEXT DEFAULT ''"], ['partner_name', "TEXT DEFAULT ''"], ['partner_type', "TEXT DEFAULT ''"], ['reference_code', "TEXT DEFAULT ''"], ['qty', 'INTEGER DEFAULT 0'], ['paper_sheets', 'REAL DEFAULT 0'], ['paper_kg', 'REAL DEFAULT 0'], ['amount', 'REAL DEFAULT 0'], ['status', "TEXT DEFAULT 'pending'"], ['note', "TEXT DEFAULT ''"], ['started_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['completed_at', 'TEXT'], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"], ['actor_username', "TEXT DEFAULT ''"], ['actor_name', "TEXT DEFAULT ''"]]) await addColumnIfMissing('order_operations', name, def);

  await runAsync(`CREATE TABLE IF NOT EXISTS partner_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partner_id INTEGER,
    partner_name TEXT,
    amount REAL DEFAULT 0,
    payment_date TEXT DEFAULT CURRENT_DATE,
    note TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT
  )`);
  for (const [name, def] of [['partner_name', "TEXT DEFAULT ''"], ['amount', 'REAL DEFAULT 0'], ['payment_date', 'TEXT DEFAULT CURRENT_DATE'], ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('partner_payments', name, def);
  for (const [name, def] of [['operation_id', 'INTEGER'], ['order_id', 'INTEGER'], ['step_type', "TEXT DEFAULT ''"], ['payment_scope', "TEXT DEFAULT ''"], ['auto_created', 'INTEGER DEFAULT 0']]) await addColumnIfMissing('partner_payments', name, def);

  const admin = await getAsync(`SELECT * FROM users WHERE username='admin'`);
  if (!admin) {
    await runAsync(`INSERT INTO users (username,password,full_name,role,is_active,perm_inventory,perm_bags,perm_orders,perm_add_order,perm_edit_order,perm_change_status,perm_accounts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, ['admin', hashPassword('1234'), 'Admin', 'admin', 1,1,1,1,1,1,1,1]);
  }
  const plainUsers = await allAsync(`SELECT id,password FROM users WHERE password IS NOT NULL AND password!=''`);
  for (const row of plainUsers) if (!isPasswordHashed(row.password)) await runAsync(`UPDATE users SET password=? WHERE id=?`, [hashPassword(row.password), row.id]);
  await syncAllCustomersFromOrders();
  await syncAllOpeningOrderPayments();
}

async function findMatchingCustomerRecord({ name='', phone='' } = {}) {
  const normalizedPhone = normalizePhone(phone || '');
  if (normalizedPhone) {
    const byPhone = await getAsync(`SELECT * FROM customers WHERE phone_normalized=? ORDER BY id DESC LIMIT 1`, [normalizedPhone]);
    if (byPhone) return byPhone;
  }
  const cleanedName = String(name || '').trim();
  if (cleanedName) {
    const byName = await getAsync(`SELECT * FROM customers WHERE TRIM(COALESCE(name,''))=? ORDER BY id DESC LIMIT 1`, [cleanedName]);
    if (byName) return byName;
  }
  return null;
}
async function ensureCustomerRecord(payload = {}) {
  const name = String(payload.name || payload.custName || '').trim();
  const phone = String(payload.phone || payload.custPhone || '').trim();
  const address = String(payload.address || payload.custAddress || '').trim();
  const governorate = String(payload.governorate || payload.bosta_city_code || '').trim();
  const zone = String(payload.zone || payload.bosta_zone || '').trim();
  const email = String(payload.email || payload.bosta_receiver_email || '').trim();
  const notes = String(payload.notes || '').trim();
  if (!name && !phone) return null;
  const existing = await findMatchingCustomerRecord({ name, phone });
  const normalizedPhone = normalizePhone(phone);
  const values = [name || existing?.name || '', phone || existing?.phone || '', normalizedPhone || existing?.phone_normalized || '', address || existing?.address || '', governorate || existing?.governorate || '', zone || existing?.zone || '', email || existing?.email || '', notes || existing?.notes || '', today()];
  if (existing) {
    await runAsync(`UPDATE customers SET name=?,phone=?,phone_normalized=?,address=?,governorate=?,zone=?,email=?,notes=?,updated_at=? WHERE id=?`, values.concat([existing.id]));
    return await getAsync(`SELECT * FROM customers WHERE id=?`, [existing.id]);
  }
  const ins = await runAsync(`INSERT INTO customers (name,phone,phone_normalized,address,governorate,zone,email,notes,updated_at) VALUES (?,?,?,?,?,?,?,?,?)`, values);
  return await getAsync(`SELECT * FROM customers WHERE id=?`, [ins.lastID]);
}
async function syncCustomerForOrder(orderId) {
  const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  if (!order) return null;
  const customer = await ensureCustomerRecord({
    name: order.custName,
    phone: order.custPhone,
    address: order.custAddress,
    governorate: order.bosta_city_code,
    zone: order.bosta_zone,
    email: order.bosta_receiver_email
  });
  if (!customer) return null;
  await runAsync(`UPDATE orders SET customer_id=? WHERE id=?`, [customer.id, order.id]);
  await runAsync(`UPDATE customers SET last_order_date=?, last_order_id=?, updated_at=? WHERE id=?`, [order.orderDate || today(), order.id, today(), customer.id]);
  return customer;
}
async function syncAllCustomersFromOrders() {
  const rows = await allAsync(`SELECT * FROM orders ORDER BY id ASC`);
  for (const row of rows) {
    try { await syncCustomerForOrder(row.id); } catch (_) {}
  }
}
async function syncAllOpeningOrderPayments() {
  const rows = await allAsync(`SELECT id,paid_amount FROM orders ORDER BY id ASC`);
  for (const row of rows) {
    try { await syncOpeningOrderPayment(row.id, row.paid_amount, 'system'); } catch (_) {}
  }
}
async function syncOpeningOrderPayment(orderId, targetPaid, createdBy='system') {
  const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  if (!order) return;
  const openingNote = 'الرصيد الافتتاحي للأوردر';
  const opening = await getAsync(`SELECT * FROM order_payments WHERE order_id=? AND note=? ORDER BY id ASC LIMIT 1`, [order.id, openingNote]);
  const nonOpening = await getAsync(`SELECT COALESCE(SUM(amount),0) total FROM order_payments WHERE order_id=? AND (note IS NULL OR note!=?)`, [order.id, openingNote]);
  const openingAmount = Math.max(0, roundMoney(num(targetPaid) - num(nonOpening?.total)));
  if (opening) {
    if (openingAmount > 0) await runAsync(`UPDATE order_payments SET amount=?, payment_date=?, created_by=? WHERE id=?`, [openingAmount, order.orderDate || today(), createdBy, opening.id]);
    else await runAsync(`DELETE FROM order_payments WHERE id=?`, [opening.id]);
  } else if (openingAmount > 0) {
    await runAsync(`INSERT INTO order_payments (order_id,customer_id,amount,payment_date,method,note,created_by) VALUES (?,?,?,?,?,?,?)`, [order.id, num(order.customer_id), openingAmount, order.orderDate || today(), 'افتتاحي', openingNote, createdBy]);
  }
}
async function refreshOrderPaymentSummary(orderId, userName='system') {
  const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  if (!order) return null;
  const payRow = await getAsync(`SELECT COALESCE(SUM(amount),0) total FROM order_payments WHERE order_id=?`, [order.id]);
  const paid = Math.max(0, roundMoney(payRow?.total));
  const total = Math.max(0, num(order.total_price));
  const remaining = Math.max(0, roundMoney(total - paid));
  const paymentType = remaining <= 0 ? 'مدفوع كامل' : (paid > 0 ? 'عربون' : 'لم يتم الدفع');
  await runAsync(`UPDATE orders SET paid_amount=?, remaining_amount=?, paymentType=? WHERE id=?`, [paid, remaining, paymentType, order.id]);
  const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [order.id]);
  await syncSaleForOrder(refreshed, userName);
  return refreshed;
}

async function addBagStock(order, note) {
  let bag = await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(order.l), num(order.w), num(order.g), order.color || '', order.handle || '']);
  let bagId;
  if (bag) {
    await runAsync(`UPDATE bags SET total_qty=total_qty+? WHERE id=?`, [num(order.qty), bag.id]);
    bagId = bag.id;
    bag = await getAsync(`SELECT * FROM bags WHERE id=?`, [bag.id]);
  } else {
    const r = await runAsync(`INSERT INTO bags (length,width,gusset,color,handle,total_qty,min_qty,buy_price,sell_price) VALUES (?,?,?,?,?,?,?,?,?)`, [num(order.l), num(order.w), num(order.g), order.color || '', order.handle || '', num(order.qty), 0, 0, 0]);
    bagId = r.lastID;
    bag = await getAsync(`SELECT * FROM bags WHERE id=?`, [bagId]);
  }
  await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [bagId, order.id, 'add', num(order.qty), bag.color, bag.handle, bag.length, bag.width, bag.gusset, note]);
}

async function subtractBagStock(order, note) {
  const bag = await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(order.l), num(order.w), num(order.g), order.color || '', order.handle || '']);
  if (!bag || num(bag.total_qty) < num(order.qty)) throw new Error('كمية الشنط غير متاحة في مخزن الشنط');
  await runAsync(`UPDATE bags SET total_qty=total_qty-? WHERE id=?`, [num(order.qty), bag.id]);
  await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [bag.id, order.id, 'sub', num(order.qty), bag.color, bag.handle, bag.length, bag.width, bag.gusset, note]);
}

async function checkHandlesAvailable(order) {
  if (order.handle !== 'بيد') return true;
  const h = await getAsync(`SELECT * FROM handles WHERE color=?`, [order.color || '']);
  return h && num(h.qty) >= num(order.qty);
}
async function deductHandles(order, note) {
  if (order.handle !== 'بيد' || num(order.handle_stock_deducted)) return;
  const h = await getAsync(`SELECT * FROM handles WHERE color=?`, [order.color || '']);
  if (!h || num(h.qty) < num(order.qty)) throw new Error('كمية اليد غير كافية في المخزن');
  await runAsync(`UPDATE handles SET qty=qty-? WHERE id=?`, [num(order.qty), h.id]);
  await runAsync(`INSERT INTO handles_history (handle_id,order_id,type,qty,color,note) VALUES (?,?,?,?,?,?)`, [h.id, order.id, 'sub', num(order.qty), h.color, note || `خصم يد للأوردر #${order.id}`]);
  const handCost = num(order.qty) * num(h.buy_price);
  await runAsync(`UPDATE orders SET handle_stock_deducted=1, cost_hand=COALESCE(cost_hand,0)+? WHERE id=?`, [handCost, order.id]);
  await addCostLog({ order_id: order.id, cost_field:'cost_hand', amount: handCost, source:'handles', source_ref:String(h.id), notes: note || `خصم يد للأوردر #${order.id}`, created_by:'system' });
}
async function maybeReturnBagOnReturn(order) {
  if (num(order.bag_returned_to_stock)) return;
  if (num(order.useReadyStock)) {
    const printType = String(order.printType || 'سادة').trim() || 'سادة';
    await addBagStock(order, `رجوع أوردر كان مسحوب من مخزن الشنط - رقم الأوردر ${order.id} (${printType})`);
    await runAsync(`UPDATE orders SET bag_returned_to_stock=1 WHERE id=?`, [order.id]);
    return;
  }
  if ((String(order.printType || 'سادة').trim() || 'سادة') === 'سادة') {
    await addBagStock(order, `مرتجع أوردر سادة دخل مخزن الشنط - رقم الأوردر ${order.id}`);
    await runAsync(`UPDATE orders SET bag_returned_to_stock=1 WHERE id=?`, [order.id]);
  }
}
async function maybeDeductBagAgain(order) {
  if (!num(order.bag_returned_to_stock)) return;
  await subtractBagStock(order, 'خصم من مخزن الشنط عند إعادة التسليم');
  await runAsync(`UPDATE orders SET bag_returned_to_stock=0 WHERE id=?`, [order.id]);
}


async function getOrderFiles(orderId) {
  const rows = await allAsync(`SELECT * FROM order_files WHERE order_id=? ORDER BY id ASC`, [orderId]);
  return rows.map(r => ({ ...r, url: `/protected-file/uploads/${encodeURIComponent(path.basename(r.filepath || r.filename || ''))}` }));
}
async function getOrdersWithFiles(sql, params=[]) {
  const rows = await allAsync(sql, params);
  for (const row of rows) {
    row.status = normalizeOrderStatus(row.status || 'أوردر جديد');
    row.files = await getOrderFiles(row.id);
    const info = await getReadyStockPurchaseTotal(row);
    row.bag_buy_price = num(info.bag?.buy_price);
    row.bag_sell_price = num(info.bag?.sell_price);
    row.ready_stock_purchase_total = num(info.total);
    row.ready_stock_sale_total = +(num(info.bag?.sell_price) * num(row.qty)).toFixed(2);
    const customer = num(row.customer_id) ? await getAsync(`SELECT governorate, zone, email FROM customers WHERE id=?`, [num(row.customer_id)]) : await findMatchingCustomerRecord({ name: row.custName, phone: row.custPhone });
    row.customer_governorate = String(customer?.governorate || '').trim();
    row.customer_zone = String(customer?.zone || '').trim();
    row.customer_email = String(customer?.email || '').trim();
  }
  return rows;
}
function calcHandFixCost(handle, qty, useReadyStock=0) { return num(useReadyStock) === 1 ? 0 : (handle === 'بيد' ? +((num(qty) / 1000) * 100).toFixed(2) : 0); }
async function getMatchingBag(order) {
  return await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(order.l), num(order.w), num(order.g), order.color || '', order.handle || '']);
}
async function getReadyStockPurchaseTotal(order) {
  const bag = await getMatchingBag(order);
  return { bag, total: +(num(bag?.buy_price) * num(order?.qty)).toFixed(2) };
}
async function getOrderBostaProductValue(order) {
  if (!order) return 0;
  let total = num(order.total_price);
  if (num(order.useReadyStock) === 1) {
    const bag = await getMatchingBag(order);
    const readyStockSale = +(num(bag?.sell_price) * num(order?.qty)).toFixed(2);
    if (readyStockSale > 0) total = readyStockSale;
  }
  return roundMoney(total);
}
async function getGroupOrderBostaProductValue(items = []) {
  let total = 0;
  for (const item of items) total += await getOrderBostaProductValue(item);
  return roundMoney(total);
}
function inferOffsetColorsCount(order) {
  const txt = String(order?.colorSpecs || '');
  const m = txt.match(/(\d+)/);
  return Math.max(1, Number(m?.[1] || 0) || (txt.split('+').filter(Boolean).length || 1));
}
function getOrderCutDimensions(order, mode = null) {
  const layoutMode = String(mode || order?.last_cut_layout || 'pieceByPiece').trim() || 'pieceByPiece';
  const width = num(order?.w);
  const length = num(order?.l);
  const gusset = num(order?.g);
  const cutWidth = +(layoutMode === 'pieceByPiece' ? (width + gusset + 2) : ((width * 2) + (gusset * 2) + 2)).toFixed(2);
  const cutLength = +(length + (gusset / 2) + 2).toFixed(2);
  return {
    mode: layoutMode,
    cutWidth,
    cutLength,
    piecesNeededPerBag: layoutMode === 'pieceByPiece' ? 2 : 1
  };
}
function getPlateTierFromDims(cutWidth, cutLength) {
  const mxW = Math.max(num(cutWidth), num(cutLength)), mxH = Math.min(num(cutWidth), num(cutLength));
  if (mxW <= 50 && mxH <= 35) return { zincCost: 60, printCost: 150 };
  if (mxW <= 70 && mxH <= 50) return { zincCost: 150, printCost: 200 };
  if (mxW <= 100 && mxH <= 70) return { zincCost: 250, printCost: 300 };
  return { zincCost: 0, printCost: 0 };
}

function estimateOrderPlateCost(order) {
  if (String(order?.printType || '').trim() !== 'أوفست') return 0;
  const { cutWidth, cutLength } = getOrderCutDimensions(order);
  const tier = getPlateTierFromDims(cutWidth, cutLength);
  const colorsCount = inferOffsetColorsCount(order);
  return roundMoney(tier.zincCost * colorsCount);
}
async function syncOrderAutoPlateCost(orderId, actor='system') {
  const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  if (!order) return null;
  if (String(order.printType || '').trim() !== 'أوفست' || !num(order.paper_cut_done)) return order;
  const manualPlate = await getAsync(`SELECT id FROM order_operations WHERE order_id=? AND step_type='plate' ORDER BY id DESC LIMIT 1`, [num(orderId)]);
  if (manualPlate) return order;
  const nextValue = estimateOrderPlateCost(order);
  const prevValue = roundMoney(num(order.cost_zinc));
  if (nextValue === prevValue) return order;
  await runAsync(`UPDATE orders SET cost_zinc=? WHERE id=?`, [nextValue, num(orderId)]);
  const diff = roundMoney(nextValue - prevValue);
  if (diff > 0) {
    await addCostLog({ order_id: num(orderId), cost_field:'cost_zinc', amount: diff, source:'paper-cut-auto-zinc', source_ref:String(num(orderId)), notes:'احتساب تلقائي لتكلفة الزنكات بعد القص', created_by: actor || 'system' });
  }
  const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  await syncSaleForOrder(refreshed, actor || 'system');
  return refreshed;
}
function calcAutoOrderCosts(order) {
  const qty = num(order?.qty);
  const out = { cost_cut: 0, cost_print: 0, cost_zinc: 0, cost_design: 0, cost_make: 0, cost_hand: 0, cost_paper: 0, cost_hand_fix: 0 };
  if (num(order?.useReadyStock) === 1) {
    const readyPurchase = num(order?.ready_stock_purchase_total) || (num(order?.bag_buy_price) * qty);
    out.cost_make = readyPurchase;
    if (String(order?.printType || '').trim() === 'سلك سكرين') {
      out.cost_design = 100;
      out.cost_print = qty * 2;
    }
    return out;
  }
  out.cost_cut = 50;
  out.cost_make = qty * 1;
  out.cost_hand_fix = calcHandFixCost(order?.handle || '', qty, 0);
  const printType = String(order?.printType || '').trim();
  if (printType === 'سادة' || !printType) return out;
  out.cost_design = 100;
  if (printType === 'سلك سكرين') {
    out.cost_print = qty * 2;
    return out;
  }
  if (printType === 'أوفست') {
    const { cutWidth, cutLength, piecesNeededPerBag } = getOrderCutDimensions(order);
    const tier = getPlateTierFromDims(cutWidth, cutLength);
    const colorsCount = inferOffsetColorsCount(order);
    const unitsForPrint = Math.max(0, qty) * piecesNeededPerBag;
    const printUnits = Math.ceil(unitsForPrint / 1000);
    out.cost_print = tier.printCost * colorsCount * printUnits;
    out.cost_zinc = tier.zincCost * colorsCount;
  }
  return out;
}
async function applyAutoOrderCosts(orderId, baseOrder, createdBy, source, beforeOrder=null) {
  const auto = calcAutoOrderCosts(baseOrder || {});
  await runAsync(`UPDATE orders SET cost_cut=?,cost_print=?,cost_zinc=?,cost_design=?,cost_make=?,cost_hand=?,cost_paper=?,cost_hand_fix=? WHERE id=?`, [num(auto.cost_cut), num(auto.cost_print), num(auto.cost_zinc), num(auto.cost_design), num(auto.cost_make), num(auto.cost_hand), num(auto.cost_paper), num(auto.cost_hand_fix), num(orderId)]);
  const labels = { cost_cut:'قص', cost_print:'طباعة', cost_zinc:'زنكات', cost_design:'تصميم', cost_make:'تصنيع', cost_hand:'يد', cost_paper:'ورق', cost_hand_fix:'تركيب يد' };
  for (const field of Object.keys(auto)) {
    const diff = num(auto[field]) - num(beforeOrder?.[field]);
    if (diff > 0) await addCostLog({ order_id:num(orderId), cost_field:field, amount:diff, source:source || 'auto-order-costs', source_ref:String(num(orderId)), notes:`تحديد تلقائي لتكلفة ${labels[field]}`, created_by:createdBy || 'system' });
  }
  return auto;
}

function collectOrderCostRows(order, linkedExpenseMap = {}) {
  if (!order || !num(order.id)) return [];
  const fields = ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix'];
  return fields
    .map(field => ({ field, amount: num(order[field]) }))
    .filter(row => row.amount > 0)
    .map(row => {
      const linked = linkedExpenseMap[`${num(order.id)}::${row.field}`] || null;
      return {
        id: `order-${order.id}-${row.field}`,
        expense_date: order.orderDate || today(),
        order_id: order.id,
        order_cost_field: row.field,
        amount: row.amount,
        notes: `تكلفة ${costFieldLabel(row.field)} للأوردر #${order.id}`,
        source: 'order-current',
        source_ref: String(order.id),
        custName: order.custName || '',
        linked_expense_id: num(linked?.id),
        actor_username: String(linked?.actor_username || '').trim(),
        actor_name: String(linked?.actor_name || '').trim(),
        expense_partner_name: normalizePartnerName(linked?.expense_partner_name || '')
      };
    });
}
async function getCurrentCostLogRows({ from='', to='' }={}) {
  let sql = `SELECT * FROM orders WHERE 1=1`;
  const params = [];
  if (from) { sql += ` AND orderDate>=?`; params.push(from); }
  if (to) { sql += ` AND orderDate<=?`; params.push(to); }
  sql += ` ORDER BY id DESC`;
  const [orders, linkedExpenseRows] = await Promise.all([
    allAsync(sql, params),
    allAsync(`SELECT * FROM expenses WHERE linked_to_order=1 ORDER BY id DESC`)
  ]);
  const linkedExpenseMap = {};
  for (const row of linkedExpenseRows) {
    const key = `${num(row.order_id)}::${String(row.order_cost_field || '').trim()}`;
    if (!key || linkedExpenseMap[key]) continue;
    linkedExpenseMap[key] = row;
  }
  return orders.flatMap(order => collectOrderCostRows(order, linkedExpenseMap)).sort((a,b)=>{
    if (String(a.expense_date) === String(b.expense_date)) return Number(String(b.order_id||0)) - Number(String(a.order_id||0));
    return String(b.expense_date).localeCompare(String(a.expense_date));
  });
}
function isAfterManufacturing(status) {
  return STATUS_FLOW.indexOf(String(status || '').trim()) > STATUS_FLOW.indexOf('تحت التصنيع');
}
const STATUS_FLOW = ['أوردر جديد','تحت الإنتاج','في القص','مستني الزنكات','تحت الطباعة','تحت التصنيع','جاهز للشحن','تم الشحن','تم التسليم'];
function isBackwardFromCut(currentStatus, newStatus) { const cutIndex = STATUS_FLOW.indexOf('في القص'); const cur = STATUS_FLOW.indexOf(currentStatus); const next = STATUS_FLOW.indexOf(newStatus); if (cur === -1 || next === -1) return false; return cur >= cutIndex && next < cutIndex; }
async function syncOrderOperationalStatus(orderId, actor='system', req=null) {
  const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  if (!order) return null;
  const currentStatus = String(order.status || '').trim();
  if (['تم الشحن','تم التسليم','مرتجع'].includes(currentStatus)) return order;
  const rows = await allAsync(`SELECT * FROM order_operations WHERE order_id=? ORDER BY id ASC`, [num(orderId)]);
  const hasAny = step => rows.some(r => String(r.step_type || '').trim() === step);
  const hasDone = step => rows.some(r => String(r.step_type || '').trim() === step && String(r.status || '').trim() === 'done');
  const needsHandle = String(order.handle || '').trim() === 'بيد' && !num(order.useReadyStock);
  let target = '';

  if ((num(order.paper_cut_done) || num(order.useReadyStock))) {
    if (hasAny('handle')) {
      target = (hasDone('handle') && hasDone('make')) ? 'جاهز للشحن' : 'تحت التصنيع';
    } else if (hasAny('make')) {
      target = (hasDone('make') && !needsHandle) ? 'جاهز للشحن' : 'تحت التصنيع';
    } else if (hasAny('print')) {
      target = 'تحت الطباعة';
    } else if (hasAny('plate')) {
      target = 'مستني الزنكات';
    } else if (num(order.paper_cut_done)) {
      target = 'في القص';
    }
  }

  if (!target || target === currentStatus) return order;
  let paid = num(order.paid_amount), remaining = num(order.remaining_amount), paymentType = order.paymentType || 'لم يتم الدفع';
  let orderForSideEffects = order;
  if (isAfterManufacturing(target) && needsHandle && !num(order.handle_stock_deducted)) {
    const ok = await checkHandlesAvailable(order);
    if (!ok) {
      target = 'تحت التصنيع';
    } else {
      if (num(order.bag_returned_to_stock)) await maybeDeductBagAgain(order);
      await deductHandles(order, 'خصم اليد تلقائياً بعد اعتماد مرحلة تركيب اليد');
      orderForSideEffects = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]) || order;
    }
  }
  if (target === currentStatus) return orderForSideEffects;
  await runAsync(`UPDATE orders SET status=?, paid_amount=?, remaining_amount=?, paymentType=? WHERE id=?`, [target, paid, remaining, paymentType, num(orderId)]);
  const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  await syncSaleForOrder(refreshed, actor || 'system');
  await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [num(orderId), currentStatus, target, actor || 'system', 'تحديث تلقائي من توجيه الأوردر والتنفيذ الخارجي']);
  if (req) {
    await recordAudit({ req, action: 'auto-update-status', entity_type: 'order', entity_id: num(orderId), details: `تحديث تلقائي للحالة من ${currentStatus || '-'} إلى ${target}` });
  }
  return refreshed;
}


async function upsertAutoPaymentForOperation({ operationId=null, orderId=null, partnerId=null, partnerName='', stepType='', amount=0, paymentDate='', paymentNote='', createdBy='' } = {}) {
  operationId = num(operationId);
  partnerId = num(partnerId);
  amount = Math.max(0, num(amount));
  if (!operationId || !partnerId || amount <= 0) return null;
  const payDate = String(paymentDate || '').trim() || today();
  const note = String(paymentNote || '').trim() || `دفعة مسجلة مع توجيه ${executionStepMeta(stepType).label} للأوردر #${num(orderId)}`;
  const existing = await getAsync(`SELECT * FROM partner_payments WHERE operation_id=? AND COALESCE(auto_created,0)=1 ORDER BY id DESC LIMIT 1`, [operationId]);
  if (existing) {
    await runAsync(`UPDATE partner_payments SET partner_id=?, partner_name=?, amount=?, payment_date=?, note=?, created_by=?, order_id=?, step_type=?, payment_scope=?, auto_created=? WHERE id=?`, [partnerId, partnerName || existing.partner_name || '', amount, payDate, note, createdBy || existing.created_by || '', num(orderId), String(stepType || '').trim(), 'operation-direction', 1, existing.id]);
    return existing.id;
  }
  const ins = await runAsync(`INSERT INTO partner_payments (partner_id,partner_name,amount,payment_date,note,created_by,operation_id,order_id,step_type,payment_scope,auto_created) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [partnerId, partnerName || '', amount, payDate, note, createdBy || '', operationId, num(orderId), String(stepType || '').trim(), 'operation-direction', 1]);
  return ins.lastID;
}

async function refreshPaperPriceSheet(paperId) {
  const p = await getAsync(`SELECT * FROM paper WHERE id=?`, [paperId]);
  if (!p) return;
  const weightSheet = (num(p.length) * num(p.width) * num(p.grammage)) / 10000000;
  const priceSheet = weightSheet > 0 ? num(p.buy_price_kg) * weightSheet : 0;
  await runAsync(`UPDATE paper SET buy_price_sheet=? WHERE id=?`, [priceSheet, paperId]);
}
function describeDiff(before, after) {
  const labels = {length:'الطول',width:'العرض',gusset:'الجنب',color:'اللون',handle:'اليد',qty:'الرصيد',total_qty:'الرصيد',min_qty:'الحد الأدنى',buy_price:'سعر الشراء',sell_price:'سعر البيع',grammage:'الجرام',paper_name:'مسمى الورق',total_kg:'الرصيد كجم',total_sheets:'الرصيد فرخ',min_kg:'حد أدنى كجم',min_sheets:'حد أدنى فرخ',buy_price_kg:'سعر الكيلو',buy_price_sheet:'سعر الفرخ',shipping_cost:'سعر الشحن'};
  const out=[];
  for (const k of Object.keys(after||{})) if (String(before?.[k] ?? '') !== String(after?.[k] ?? '')) out.push(`${labels[k]||k}: ${before?.[k] ?? '-'} → ${after?.[k] ?? '-'}`);
  return out.join(' | ');
}

async function addCostLog({order_id=null,cost_field='',amount=0,source='',source_ref='',notes='',created_by=''}) {
  amount = num(amount);
  if (amount <= 0) return;
  await runAsync(`INSERT INTO cost_history (cost_date,order_id,cost_field,amount,source,source_ref,notes,created_by) VALUES (?,?,?,?,?,?,?,?)`, [today(), order_id || null, cost_field || '', amount, source || '', source_ref || '', notes || '', created_by || '']);
}

function calcOrderTotalCosts(order) {
  return ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix'].reduce((s, k) => s + num(order?.[k]), 0);
}

function executionCostField(stepType) {
  return ({
    cut: 'cost_cut',
    plate: 'cost_zinc',
    print: 'cost_print',
    make: 'cost_make',
    handle: 'cost_hand_fix'
  })[String(stepType || '').trim()] || '';
}

async function recalculateOrderExecutionCosts(orderId, actor='system', forcedFields=[]) {
  const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  if (!order) return null;
  const fields = ['cost_cut','cost_print','cost_zinc','cost_make','cost_hand_fix'];
  const forced = new Set((forcedFields || []).filter(Boolean));
  const rows = await allAsync(`SELECT step_type, COUNT(*) count_rows, COALESCE(SUM(amount),0) total_amount FROM order_operations WHERE order_id=? GROUP BY step_type`, [num(orderId)]);
  const before = Object.fromEntries(fields.map(f => [f, num(order[f])]));
  const after = { ...before };
  const touched = new Set();
  for (const row of rows) {
    const field = executionCostField(row.step_type);
    if (!field) continue;
    after[field] = roundMoney(num(row.total_amount));
    touched.add(field);
  }
  for (const field of forced) {
    if (!touched.has(field)) after[field] = 0;
  }
  const changed = fields.filter(field => num(before[field]) !== num(after[field]));
  if (!changed.length) return order;
  await runAsync(`UPDATE orders SET cost_cut=?, cost_print=?, cost_zinc=?, cost_make=?, cost_hand_fix=? WHERE id=?`, [num(after.cost_cut), num(after.cost_print), num(after.cost_zinc), num(after.cost_make), num(after.cost_hand_fix), num(orderId)]);
  for (const field of changed) {
    const nextValue = roundMoney(num(after[field]));
    const prevValue = roundMoney(num(before[field]));
    const delta = roundMoney(nextValue - prevValue);
    if (nextValue > 0) {
      await addCostLog({ order_id: num(orderId), cost_field: field, amount: nextValue, source: 'execution', source_ref: String(num(orderId)), notes: `تحديث تكلفة ${costFieldLabel(field)} من حسابات التنفيذ الخارجي`, created_by: actor || 'system' });
    }
  }
  const updated = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  await syncSaleForOrder(updated, actor || 'system');
  return updated;
}

function costFieldLabel(field) {
  return ({
    cost_cut:'قص',
    cost_print:'طباعة',
    cost_zinc:'زنكات',
    cost_design:'تصميم',
    cost_make:'تصنيع',
    cost_hand:'يد',
    cost_paper:'ورق',
    cost_hand_fix:'تركيب يد'
  })[String(field || '').trim()] || String(field || '').trim();
}

function isBostaOrderRecord(order) {
  return !!String(order?.bosta_delivery_id || order?.bosta_tracking_number || order?.bosta_sent_at || '').trim();
}

function getEffectiveOrderShippingFee(order) {
  if (!isBostaOrderRecord(order)) return 0;
  const explicit = Math.max(0, num(order?.shipping_cost, 0));
  const apiShipping = Math.max(0, num(order?.bosta_shipping_fee, 0));
  const legacyEstimate = Math.max(0, num(order?.bosta_estimated_fees, num(order?.bosta_price_after_vat, 0)));
  return roundMoney(explicit || apiShipping || legacyEstimate || 0);
}

function calcDeliveredSalesDeductions(order) {
  const shippingFee = getEffectiveOrderShippingFee(order);
  const grossSale = Math.max(0, num(order?.total_price, 0));
  const productValueBase = Math.max(0, num(order?.bosta_product_value, grossSale));
  const insuranceFee = roundMoney(productValueBase * 0.01);
  const extraCodFee = calcLocalBostaExtraCod(grossSale);
  const totalDeductions = roundMoney(shippingFee + insuranceFee + extraCodFee);
  return { shippingFee, insuranceFee, extraCodFee, totalDeductions };
}

async function syncSaleForOrder(order, created_by='') {
  if (!order || !num(order.id)) return;
  const status = String(order.status || '').trim();
  if (!['تم التسليم', 'مرتجع'].includes(status)) {
    await runAsync(`DELETE FROM sales_history WHERE order_id=?`, [num(order.id)]);
    return;
  }

  const gross_sale = Math.max(0, num(order.total_price));
  const shipping_cost = getEffectiveOrderShippingFee(order);
  let insurance_fee = 0;
  let extra_cod_fee = 0;
  let total_deductions = shipping_cost;
  let total_sale = 0;
  let total_cost = 0;
  let net_profit = 0;
  let paid_amount = 0;
  let remaining_amount = 0;
  let notes = '';

  if (status === 'تم التسليم') {
    const d = calcDeliveredSalesDeductions(order);
    insurance_fee = d.insuranceFee;
    extra_cod_fee = d.extraCodFee;
    total_deductions = d.totalDeductions;
    total_sale = roundMoney(Math.max(0, gross_sale - total_deductions));
    total_cost = calcOrderTotalCosts(order);
    net_profit = roundMoney(total_sale - total_cost);
    paid_amount = num(order.paid_amount);
    remaining_amount = num(order.remaining_amount);
    notes = 'مبيعات أوردر تم تسليمه';
  } else {
    total_deductions = shipping_cost;
    total_sale = roundMoney(-shipping_cost);
    total_cost = 0;
    net_profit = total_sale;
    paid_amount = 0;
    remaining_amount = 0;
    notes = 'أوردر مرتجع - خصم الشحن فقط';
  }

  const existing = await getAsync(`SELECT id FROM sales_history WHERE order_id=?`, [num(order.id)]);
  const values = [today(), num(order.id), total_sale, gross_sale, shipping_cost, insurance_fee, extra_cod_fee, total_deductions, total_cost, net_profit, paid_amount, remaining_amount, order.custName || '', status, notes, created_by || ''];
  if (existing) {
    await runAsync(`UPDATE sales_history SET sale_date=?,order_id=?,total_sale=?,gross_sale=?,shipping_cost=?,insurance_fee=?,extra_cod_fee=?,total_deductions=?,total_cost=?,net_profit=?,paid_amount=?,remaining_amount=?,customer_name=?,status=?,notes=?,created_by=? WHERE order_id=?`, [...values, num(order.id)]);
  } else {
    await runAsync(`INSERT INTO sales_history (sale_date,order_id,total_sale,gross_sale,shipping_cost,insurance_fee,extra_cod_fee,total_deductions,total_cost,net_profit,paid_amount,remaining_amount,customer_name,status,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, values);
  }
}


function paperSheetWeightKg(paperLike = {}) {
  const length = num(paperLike.length ?? paperLike.paper_length);
  const width = num(paperLike.width ?? paperLike.paper_width);
  const grammage = num(paperLike.grammage ?? paperLike.paper_grammage);
  const weight = (length * width * grammage) / 10000000;
  return weight > 0 ? weight : 0;
}

function purchasePaperStockQuantities(purchase = {}, paperRow = null) {
  const quantity = Math.max(0, num(purchase?.quantity));
  const unit = String(purchase?.unit || '').trim();
  const ref = paperRow || purchase || {};
  const perSheetKg = paperSheetWeightKg(ref);
  let kg = 0, sheets = 0;
  if (['فرخ', 'sheet', 'sheets'].includes(unit)) {
    sheets = quantity;
    kg = perSheetKg > 0 ? roundMoney(quantity * perSheetKg) : 0;
  } else {
    kg = quantity;
    sheets = perSheetKg > 0 ? roundMoney(quantity / perSheetKg) : 0;
  }
  return { kg, sheets, perSheetKg };
}

function partnerTypeAllowedForStep(stepType, partnerType) {
  const normalized = String(partnerType || '').trim();
  if (!normalized) return true;
  if (stepType === 'handle') return ['تركيب يد', 'صنايعي'].includes(normalized);
  if (stepType === 'make') return normalized === 'صنايعي';
  if (stepType === 'print' || stepType === 'plate') return normalized === 'مطبعة';
  return true;
}

async function refreshPurchaseRemaining(purchaseId) {
  const purchase = await getAsync(`SELECT * FROM purchases WHERE id=?`, [num(purchaseId)]);
  if (!purchase) return null;
  const row = await getAsync(`SELECT COALESCE(SUM(amount),0) paid FROM purchase_payments WHERE purchase_id=?`, [num(purchaseId)]);
  const paid = roundMoney(num(row?.paid));
  const total = roundMoney(num(purchase.total_price));
  const remaining = roundMoney(Math.max(0, total - paid));
  await runAsync(`UPDATE purchases SET paid_amount=?, remaining_amount=? WHERE id=?`, [paid, remaining, num(purchaseId)]);
  return await getAsync(`SELECT * FROM purchases WHERE id=?`, [num(purchaseId)]);
}

function normalizePurchaseGroupNo(v = '') {
  return String(v || '').trim();
}
async function getNextPurchaseInvoiceNo() {
  const row = await getAsync(`SELECT invoice_group_no, id FROM purchases WHERE TRIM(COALESCE(invoice_group_no,''))<>'' ORDER BY id DESC LIMIT 1`);
  const current = String(row?.invoice_group_no || '').trim();
  const match = current.match(/^(?:PUR|INV)-?(\d+)$/i);
  let next = match ? (parseInt(match[1], 10) + 1) : 0;
  if (!next) {
    const countRow = await getAsync(`SELECT COUNT(DISTINCT COALESCE(NULLIF(invoice_group_no,''), 'single:' || id)) c FROM purchases`);
    next = num(countRow?.c) + 1;
  }
  return `PUR-${String(Math.max(1, next)).padStart(6, '0')}`;
}
function purchaseGroupKeyForRow(row = {}) {
  const groupNo = normalizePurchaseGroupNo(row?.invoice_group_no);
  return groupNo ? `group:${groupNo}` : `single:${num(row?.id)}`;
}
function purchaseGroupLabelForRow(row = {}) {
  return normalizePurchaseGroupNo(row?.invoice_group_no) || String(num(row?.id) || '');
}
async function getPurchaseGroupRowsByKey(groupKey) {
  const key = String(groupKey || '').trim();
  if (!key) return [];
  if (key.startsWith('group:')) {
    const code = key.slice(6).trim();
    if (!code) return [];
    return await allAsync(`SELECT * FROM purchases WHERE invoice_group_no=? ORDER BY id ASC`, [code]);
  }
  const singleId = num(key.replace(/^single:/, ''));
  if (!singleId) return [];
  const row = await getAsync(`SELECT * FROM purchases WHERE id=?`, [singleId]);
  return row ? [row] : [];
}
async function getPurchaseGroupPayments(rows = []) {
  const ids = (rows || []).map(r => num(r.id)).filter(Boolean);
  if (!ids.length) return [];
  const placeholders = ids.map(() => '?').join(',');
  return await allAsync(`SELECT pp.*, p.item_name, p.purchase_date, p.id AS purchase_no, p.invoice_group_no
    FROM purchase_payments pp
    LEFT JOIN purchases p ON p.id=pp.purchase_id
    WHERE pp.purchase_id IN (${placeholders})
    ORDER BY pp.payment_date DESC, pp.id DESC`, ids);
}
async function getPurchaseGroupPayload(groupKey) {
  const rows = await getPurchaseGroupRowsByKey(groupKey);
  if (!rows.length) return null;
  const payments = await getPurchaseGroupPayments(rows);
  const summary = rows.reduce((acc, row) => {
    acc.total_price += num(row.total_price);
    acc.paid_amount += num(row.paid_amount);
    acc.remaining_amount += num(row.remaining_amount);
    return acc;
  }, { total_price: 0, paid_amount: 0, remaining_amount: 0 });
  return {
    group_key: purchaseGroupKeyForRow(rows[0]),
    invoice_group_no: normalizePurchaseGroupNo(rows[0].invoice_group_no),
    invoice_no: purchaseGroupLabelForRow(rows[0]),
    supplier_name: rows[0].supplier_name || '',
    purchase_date: rows[0].purchase_date || '',
    rows,
    payments,
    summary: {
      total_price: roundMoney(summary.total_price),
      paid_amount: roundMoney(summary.paid_amount),
      remaining_amount: roundMoney(summary.remaining_amount),
      items_count: rows.length
    }
  };
}
async function addPaymentToPurchaseGroup(groupKey, { amount = 0, paymentDate = '', note = '', createdBy = '' } = {}) {
  const rows = await getPurchaseGroupRowsByKey(groupKey);
  if (!rows.length) throw new Error('الفاتورة غير موجودة');
  let remainingToAllocate = roundMoney(num(amount));
  if (remainingToAllocate <= 0) throw new Error('المبلغ غير صحيح');
  const paymentIds = [];
  const ordered = rows.slice().sort((a, b) => num(a.id) - num(b.id));
  for (const row of ordered) {
    if (remainingToAllocate <= 0) break;
    const currentRemaining = roundMoney(num(row.remaining_amount));
    if (currentRemaining <= 0) continue;
    const part = roundMoney(Math.min(currentRemaining, remainingToAllocate));
    if (part <= 0) continue;
    const ins = await runAsync(`INSERT INTO purchase_payments (purchase_id,supplier_id,amount,payment_date,note,created_by) VALUES (?,?,?,?,?,?)`, [num(row.id), num(row.supplier_id), part, String(paymentDate || today()).trim() || today(), String(note || '').trim(), String(createdBy || '').trim() || 'system']);
    paymentIds.push(num(ins.lastID));
    remainingToAllocate = roundMoney(remainingToAllocate - part);
  }
  if (!paymentIds.length) throw new Error('لا يوجد رصيد مستحق على هذه الفاتورة');
  for (const row of ordered) await refreshPurchaseRemaining(row.id);
  return { paymentIds, allocated: roundMoney(num(amount) - remainingToAllocate), group: await getPurchaseGroupPayload(groupKey) };
}
async function deleteSinglePurchaseRecord(purchase, req, { auditAction = 'delete-purchase' } = {}) {
  const id = num(purchase?.id);
  if (!id) return null;
  const linkedPayments = await allAsync(`SELECT id FROM purchase_payments WHERE purchase_id=?`, [id]);
  const paymentIds = linkedPayments.map(r => num(r.id)).filter(Boolean);
  const stockUndoPayload = num(purchase.stock_applied) === 1 ? await buildUndoPayloadFromDefs(String(purchase.stock_type || '').trim() === 'paper' ? [
    { table:'paper', criteria: criteriaEq('id', num(purchase.stock_ref_id)) },
    { table:'paper_history', criteria: criteriaEq('paper_id', num(purchase.stock_ref_id)) }
  ] : String(purchase.stock_type || '').trim() === 'handle' ? [
    { table:'handles', criteria: criteriaEq('id', num(purchase.stock_ref_id)) },
    { table:'handles_history', criteria: criteriaEq('handle_id', num(purchase.stock_ref_id)) }
  ] : String(purchase.stock_type || '').trim() === 'bag' ? [
    { table:'bags', criteria: criteriaEq('id', num(purchase.stock_ref_id)) },
    { table:'bags_history', criteria: criteriaEq('bag_id', num(purchase.stock_ref_id)) }
  ] : []) : [];
  const undoPayload = [
    ...(await buildUndoPayloadFromDefs(purchaseSnapshotDefs([id]))),
    ...stockUndoPayload
  ];
  if (num(purchase.stock_applied) === 1) await reversePurchaseStockSilently(purchase);
  await deletePurchaseLinkedLogs(purchase, paymentIds);
  await runAsync(`DELETE FROM purchase_payments WHERE purchase_id=?`, [id]);
  await runAsync(`DELETE FROM purchases WHERE id=?`, [id]);
  await recordAudit({ req, action: auditAction, entity_type: 'purchase', entity_id: id, details: `حذف مشتريات ${purchase.item_name || ''}${normalizePurchaseGroupNo(purchase.invoice_group_no) ? ` | فاتورة ${purchase.invoice_group_no}` : ''}`, can_undo: 1, undo_type: 'delete-purchase', undo_payload: undoPayload, touch_refs: [...purchaseTouchRefs([id]), ...(num(purchase.stock_applied) === 1 && String(purchase.stock_type || '').trim() === 'paper' ? paperTouchRefs([num(purchase.stock_ref_id)]) : []), ...(num(purchase.stock_applied) === 1 && String(purchase.stock_type || '').trim() === 'handle' ? handleTouchRefs([num(purchase.stock_ref_id)]) : []), ...(num(purchase.stock_applied) === 1 && String(purchase.stock_type || '').trim() === 'bag' ? bagTouchRefs([num(purchase.stock_ref_id)]) : [])] });
  return { success: true, id };
}
async function syncExecutionOperationFromExpense({ orderId = 0, costField = '', executionPartner = null, amount = 0, actorInfo = {}, note = '', expenseDate = '', createdBy = '' } = {}) {
  const stepType = ({ cost_print:'print', cost_make:'make', cost_hand_fix:'handle' })[String(costField || '').trim()] || '';
  if (!stepType || !executionPartner || !num(orderId)) return null;
  const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(orderId)]);
  if (!order) return null;
  if (!num(order.useReadyStock) && !num(order.paper_cut_done)) return null;
  if (stepType === 'print' && ['', 'سادة'].includes(String(order.printType || '').trim())) return null;
  const partnerId = num(executionPartner.id);
  const partnerName = String(executionPartner.name || '').trim();
  const partnerType = String(executionPartner.partner_type || '').trim();
  if (!partnerId || !partnerName || !partnerTypeAllowedForStep(stepType, partnerType)) return null;
  const meta = executionStepMeta(stepType);
  const linkedField = executionCostField(stepType);
  const payload = [num(orderId), stepType, meta.label, partnerId, partnerName, partnerType, '', Math.max(0, Math.round(num(order.qty))), 0, 0, roundMoney(num(amount)), 'pending', String(note || '').trim() || 'مزامنة من الحسابات', String(expenseDate || today()).trim() || today(), null, String(createdBy || '').trim() || 'system', String(actorInfo?.username || '').trim(), String(actorInfo?.full_name || '').trim()];
  const existing = await getAsync(`SELECT * FROM order_operations WHERE order_id=? AND step_type=? ORDER BY id DESC LIMIT 1`, [num(orderId), stepType]);
  let operationId = 0;
  if (existing) {
    operationId = num(existing.id);
    await runAsync(`UPDATE order_operations SET order_id=?, step_type=?, step_label=?, partner_id=?, partner_name=?, partner_type=?, reference_code=?, qty=?, paper_sheets=?, paper_kg=?, amount=?, status=?, note=?, started_at=?, completed_at=?, created_by=?, actor_username=?, actor_name=? WHERE id=?`, [...payload, operationId]);
    await runAsync(`UPDATE partner_payments SET partner_id=?, partner_name=? WHERE operation_id=? AND COALESCE(auto_created,0)=1`, [partnerId, partnerName, operationId]);
  } else {
    const ins = await runAsync(`INSERT INTO order_operations (order_id,step_type,step_label,partner_id,partner_name,partner_type,reference_code,qty,paper_sheets,paper_kg,amount,status,note,started_at,completed_at,created_by,actor_username,actor_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, payload);
    operationId = num(ins.lastID);
  }
  await syncOrderOperationAdminCash({ operationId, actorUsername: String(actorInfo?.username || '').trim(), actorName: String(actorInfo?.full_name || '').trim(), amount: roundMoney(num(amount)), entryDate: String(expenseDate || today()).trim() || today(), note: String(note || '').trim() || `تكلفة ${meta.label} للأوردر #${num(orderId)}`, stepType, createdBy: String(createdBy || '').trim() || 'system' });
  await recalculateOrderExecutionCosts(orderId, String(createdBy || '').trim() || 'system', linkedField ? [linkedField] : []);
  return operationId ? await getAsync(`SELECT * FROM order_operations WHERE id=?`, [operationId]) : null;
}
async function applyPurchaseToStock(purchase, direction = 1) {
  const stockType = String(purchase?.stock_type || '').trim();
  const stockRefId = num(purchase?.stock_ref_id);
  if (!stockType || !stockRefId) return;
  const sign = direction >= 0 ? 1 : -1;
  const actor = String(purchase?.created_by || 'system').trim();
  if (stockType === 'paper') {
    const paper = await getAsync(`SELECT * FROM paper WHERE id=?`, [stockRefId]);
    const q = purchasePaperStockQuantities(purchase, paper || purchase || {});
    const kg = roundMoney(q.kg * sign);
    const sheets = roundMoney(q.sheets * sign);
    await runAsync(`UPDATE paper SET total_kg=COALESCE(total_kg,0)+?, total_sheets=COALESCE(total_sheets,0)+? WHERE id=?`, [kg, sheets, stockRefId]);
    const afterPaper = await getAsync(`SELECT * FROM paper WHERE id=?`, [stockRefId]);
    const historyPaper = afterPaper || paper || purchase || {};
    if (historyPaper) await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,date,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [stockRefId, sign>0?'purchase':'purchase-reverse', Math.abs(kg), Math.abs(sheets), new Date().toISOString(), historyPaper.color || purchase.paper_color || '', num(historyPaper.length || purchase.paper_length), num(historyPaper.width || purchase.paper_width), num(historyPaper.grammage || purchase.paper_grammage), historyPaper.paper_name || purchase.item_name || '', `${sign>0?'إضافة':'عكس'} مشتريات #${num(purchase.id)} بواسطة ${actor} | ${Math.abs(num(purchase.quantity))} ${String(purchase.unit || '').trim() || 'وحدة'}`]);
  } else if (stockType === 'handle') {
    const qty = Math.round(num(purchase.quantity) * sign);
    await runAsync(`UPDATE handles SET qty=COALESCE(qty,0)+? WHERE id=?`, [qty, stockRefId]);
    const handle = await getAsync(`SELECT * FROM handles WHERE id=?`, [stockRefId]);
    if (handle) await runAsync(`INSERT INTO handles_history (handle_id,order_id,type,qty,color,note,date) VALUES (?,?,?,?,?,?,?)`, [stockRefId, 0, sign>0?'purchase':'purchase-reverse', Math.abs(qty), handle.color || '', `${sign>0?'إضافة':'عكس'} مشتريات #${num(purchase.id)} بواسطة ${actor}`, new Date().toISOString()]);
  } else if (stockType === 'bag') {
    const qty = Math.round(num(purchase.quantity) * sign);
    await runAsync(`UPDATE bags SET total_qty=COALESCE(total_qty,0)+? WHERE id=?`, [qty, stockRefId]);
    const bag = await getAsync(`SELECT * FROM bags WHERE id=?`, [stockRefId]);
    if (bag) await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,date,color,handle,length,width,gusset,note) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [stockRefId, 0, sign>0?'purchase':'purchase-reverse', Math.abs(qty), new Date().toISOString(), bag.color || '', bag.handle || '', num(bag.length), num(bag.width), num(bag.gusset), `${sign>0?'إضافة':'عكس'} مشتريات #${num(purchase.id)} بواسطة ${actor}`]);
  }
}
async function reversePurchaseStockSilently(purchase) {
  const stockType = String(purchase?.stock_type || '').trim();
  const stockRefId = num(purchase?.stock_ref_id);
  if (!stockType || !stockRefId) return;
  if (stockType === 'paper') {
    const paper = await getAsync(`SELECT * FROM paper WHERE id=?`, [stockRefId]);
    const q = purchasePaperStockQuantities(purchase, paper || purchase || {});
    await runAsync(`UPDATE paper SET total_kg=COALESCE(total_kg,0)-?, total_sheets=COALESCE(total_sheets,0)-? WHERE id=?`, [roundMoney(q.kg), roundMoney(q.sheets), stockRefId]);
  } else if (stockType === 'handle') {
    await runAsync(`UPDATE handles SET qty=COALESCE(qty,0)-? WHERE id=?`, [Math.round(num(purchase.quantity)), stockRefId]);
  } else if (stockType === 'bag') {
    await runAsync(`UPDATE bags SET total_qty=COALESCE(total_qty,0)-? WHERE id=?`, [Math.round(num(purchase.quantity)), stockRefId]);
  }
}
async function deletePurchaseLinkedLogs(purchase, paymentIds = []) {
  const purchaseId = num(purchase?.id);
  if (!purchaseId) return;
  const noteLike = `%مشتريات #${purchaseId}%`;
  const stockType = String(purchase?.stock_type || '').trim();
  const stockRefId = num(purchase?.stock_ref_id);

  if (stockType === 'paper' && stockRefId) {
    await runAsync(`DELETE FROM paper_history WHERE paper_id=? AND (type IN ('purchase','purchase-reverse') OR note LIKE ?) AND note LIKE ?`, [stockRefId, noteLike, noteLike]);
  } else if (stockType === 'handle' && stockRefId) {
    await runAsync(`DELETE FROM handles_history WHERE handle_id=? AND (type IN ('purchase','purchase-reverse') OR note LIKE ?) AND note LIKE ?`, [stockRefId, noteLike, noteLike]);
  } else if (stockType === 'bag' && stockRefId) {
    await runAsync(`DELETE FROM bags_history WHERE bag_id=? AND (type IN ('purchase','purchase-reverse') OR note LIKE ?) AND note LIKE ?`, [stockRefId, noteLike, noteLike]);
  }

  await runAsync(`DELETE FROM audit_logs WHERE entity_type='purchase' AND entity_id=?`, [purchaseId]);
  const uniquePaymentIds = uniqueList(paymentIds).filter(Boolean);
  if (uniquePaymentIds.length) {
    await runAsync(`DELETE FROM audit_logs WHERE entity_type='purchase-payment' AND entity_id IN (${uniquePaymentIds.map(()=>'?').join(',')})`, uniquePaymentIds);
  }
}
async function supplierStatementData(supplierId) {
  const supplier = await getAsync(`SELECT * FROM suppliers WHERE id=?`, [num(supplierId)]);
  if (!supplier) return null;
  const purchases = await allAsync(`SELECT *, COALESCE(NULLIF(invoice_group_no,''), CAST(id AS TEXT)) AS invoice_no FROM purchases WHERE supplier_id=? ORDER BY purchase_date DESC, id DESC`, [num(supplierId)]);
  const payments = await allAsync(`SELECT pp.*, p.item_name, p.purchase_date, p.id AS purchase_no, COALESCE(NULLIF(p.invoice_group_no,''), CAST(p.id AS TEXT)) AS invoice_no FROM purchase_payments pp LEFT JOIN purchases p ON p.id=pp.purchase_id WHERE pp.supplier_id=? ORDER BY pp.payment_date DESC, pp.id DESC`, [num(supplierId)]);
  const summary = await getAsync(`SELECT COALESCE(SUM(total_price),0) totalPurchases, COALESCE(SUM(paid_amount),0) totalPaid, COALESCE(SUM(remaining_amount),0) totalRemaining, COUNT(DISTINCT COALESCE(NULLIF(invoice_group_no,''), 'single:' || id)) purchasesCount FROM purchases WHERE supplier_id=?`, [num(supplierId)]);
  const opening = roundMoney(num(supplier.opening_balance));
  return {
    supplier,
    purchases,
    payments,
    summary: {
      totalPurchases: roundMoney(num(summary?.totalPurchases) + opening),
      totalPaid: roundMoney(num(summary?.totalPaid)),
      totalRemaining: roundMoney(Math.max(0, num(summary?.totalRemaining) + opening)),
      purchasesCount: num(summary?.purchasesCount),
      openingBalance: opening
    }
  };
}


async function syncAllSalesHistory() {
  await runAsync(`DELETE FROM sales_history WHERE order_id IN (SELECT id FROM orders WHERE TRIM(COALESCE(status,'')) NOT IN ('تم التسليم','مرتجع'))`);
  const targetOrders = await allAsync(`SELECT * FROM orders WHERE TRIM(COALESCE(status,'')) IN ('تم التسليم','مرتجع')`);
  for (const order of targetOrders) await syncSaleForOrder(order, 'system-sync');
}

function computeLayout(order, paper, mode) {
  const qty = Math.max(1, num(order.qty));
  const isPieceByPiece = mode === 'pieceByPiece';
  const { cutWidth, cutLength, piecesNeededPerBag } = getOrderCutDimensions(order, mode);
  const layoutLabel = isPieceByPiece ? 'حته ف حته' : 'حته واحدة';
  const orientations = [
    { sheetWidth: num(paper.length), sheetHeight: num(paper.width), rotated: false },
    { sheetWidth: num(paper.width), sheetHeight: num(paper.length), rotated: true }
  ];
  let best = null;
  for (const ori of orientations) {
    const cols = Math.floor(ori.sheetWidth / cutWidth);
    const rows = Math.floor(ori.sheetHeight / cutLength);
    const piecesPerSheet = cols * rows;
    if (piecesPerSheet <= 0) continue;
    const usedArea = piecesPerSheet * cutWidth * cutLength;
    const totalArea = ori.sheetWidth * ori.sheetHeight;
    const wastePercent = Math.max(0, ((totalArea - usedArea) / totalArea) * 100);
    const c = {
      paperId: paper.id,
      paperLabel: paperLabelBase(paper),
      paperColor: paper.color,
      layoutKey: mode,
      layoutLabel,
      sheetWidth: ori.sheetWidth,
      sheetHeight: ori.sheetHeight,
      cutWidth: +cutWidth.toFixed(2),
      cutLength: +cutLength.toFixed(2),
      cols, rows, rotated: ori.rotated,
      piecesPerSheet,
      piecesNeededPerBag,
      bagsPerSheet: +(piecesPerSheet / piecesNeededPerBag).toFixed(2),
      neededSheets: Math.ceil((qty * piecesNeededPerBag) / piecesPerSheet),
      availableSheets: Math.round(num(paper.total_sheets)),
      wastePercent: +wastePercent.toFixed(2),
      colorMatch: bagColorMatchesPaper(order.color, paper.color),
      outputLabel: isPieceByPiece ? `${piecesPerSheet} حتة = ${(piecesPerSheet / 2).toFixed(2).replace(/\.00$/, '')} شنطة` : `${piecesPerSheet} شنطة`,
      paperGrammage: num(paper.grammage),
      gram: num(paper.grammage),
      paperLabelFull: paperLabelFull(paper)
    };
    if (!best || c.wastePercent < best.wastePercent || (c.wastePercent === best.wastePercent && c.neededSheets < best.neededSheets)) best = c;
  }
  return best;
}

function paperSheetWeightKg(paper) {
  return +(((num(paper?.length) * num(paper?.width) * num(paper?.grammage)) / 10000000)).toFixed(6);
}
function calcProductionOrderCosts({ qty=0, handle='', neededSheets=0, paper=null, handleBuyPrice=0 } = {}) {
  const safeQty = Math.max(0, num(qty));
  const safeSheets = Math.max(0, Math.ceil(num(neededSheets)));
  const sheetWeight = paperSheetWeightKg(paper);
  const neededKg = +(sheetWeight * safeSheets).toFixed(4);
  const hasHandle = String(handle || '').trim() === 'بيد';
  const cost_paper = +(neededKg * num(paper?.buy_price_kg)).toFixed(2);
  const cost_cut = safeQty > 0 ? 50 : 0;
  const cost_make = +(safeQty * 1).toFixed(2);
  const cost_hand = hasHandle ? +(safeQty * num(handleBuyPrice)).toFixed(2) : 0;
  const cost_hand_fix = hasHandle ? +calcHandFixCost(handle || '', safeQty, 0).toFixed(2) : 0;
  const total_cost = +(cost_paper + cost_cut + cost_make + cost_hand + cost_hand_fix).toFixed(2);
  const unit_cost = safeQty > 0 ? +(total_cost / safeQty).toFixed(4) : 0;
  return { neededKg, neededSheets: safeSheets, cost_paper, cost_cut, cost_make, cost_hand, cost_hand_fix, total_cost, unit_cost };
}
async function createPendingProductionJob({
  sourceType='ready_bags',
  sourceRefId=0,
  paper=null,
  paperId=0,
  bagId=0,
  qty=0,
  color='',
  handle='',
  l=0,
  w=0,
  g=0,
  layoutKey='pieceByPiece',
  layoutLabel='',
  paperLabel='',
  paperKg=0,
  paperSheets=0,
  costPaper=0,
  costCut=0,
  costMake=0,
  costHand=0,
  costHandFix=0,
  totalCost=0,
  unitCost=0,
  note='',
  createdBy='system',
  pricingMode='auto_from_margin',
  profitMargin=0,
  bagBuyPrice=0,
  bagSellPrice=0,
  reserveHandleId=0
} = {}) {
  const ins = await runAsync(`INSERT INTO production_orders (
    production_date,paper_id,bag_id,qty,color,handle,length,width,gusset,layout_key,layout_label,paper_label,paper_kg,paper_sheets,cost_paper,cost_cut,cost_make,cost_hand,cost_hand_fix,total_cost,unit_cost,note,created_by,status,completed_at,completed_by,source_type,source_ref_id,pricing_mode,profit_margin,bag_buy_price,bag_sell_price,reserve_handle_id
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [
    new Date().toISOString(),
    num(paperId || paper?.id),
    num(bagId),
    Math.max(0, Math.round(num(qty))),
    String(color || '').trim(),
    String(handle || '').trim(),
    num(l),
    num(w),
    num(g),
    String(layoutKey || '').trim() || 'pieceByPiece',
    String(layoutLabel || '').trim(),
    String(paperLabel || paperLabelBase(paper || {})).trim(),
    num(paperKg),
    num(paperSheets),
    num(costPaper),
    num(costCut),
    num(costMake),
    num(costHand),
    num(costHandFix),
    num(totalCost),
    num(unitCost),
    String(note || '').trim(),
    String(createdBy || 'system').trim(),
    'pending',
    null,
    '',
    String(sourceType || 'ready_bags').trim(),
    num(sourceRefId),
    String(pricingMode || 'auto_from_margin').trim(),
    num(profitMargin),
    num(bagBuyPrice),
    num(bagSellPrice),
    num(reserveHandleId)
  ]);
  return ins.lastID;
}
async function completeProductionJob(jobId, actor='system') {
  const job = await getAsync(`SELECT * FROM production_orders WHERE id=?`, [num(jobId)]);
  if (!job) throw new Error('أمر التشغيل غير موجود');
  if (normalizeProductionStatus(job.status) === 'done') throw new Error('أمر التشغيل تم إنهاؤه بالفعل');

  const qty = Math.max(0, Math.round(num(job.qty)));
  if (qty <= 0) throw new Error('كمية أمر التشغيل غير صحيحة');

  await runAsync('BEGIN TRANSACTION');
  try {
    let handleRow = null;
    if (String(job.handle || '').trim() === 'بيد') {
      if (num(job.reserve_handle_id) > 0) handleRow = await getAsync(`SELECT * FROM handles WHERE id=?`, [num(job.reserve_handle_id)]);
      if (!handleRow) handleRow = await getAsync(`SELECT * FROM handles WHERE color=? ORDER BY id DESC LIMIT 1`, [String(job.color || '').trim()]);
      if (!handleRow) throw new Error('لا يوجد يد مطابقة لهذا اللون في المخزن');
      if (num(handleRow.qty) < qty) throw new Error('كمية اليد غير كافية في المخزن لإتمام أمر التشغيل');
      await runAsync(`UPDATE handles SET qty=qty-? WHERE id=?`, [qty, handleRow.id]);
      await runAsync(`INSERT INTO handles_history (handle_id,order_id,type,qty,color,note,date) VALUES (?,?,?,?,?,?,?)`, [handleRow.id, null, 'sub', qty, String(job.color || '').trim(), `خصم يد لإتمام أمر التشغيل #${num(job.id)}`, new Date().toISOString()]);
    }

    const existingBag = await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(job.length), num(job.width), num(job.gusset), String(job.color || '').trim(), String(job.handle || '').trim()]);
    const buyPrice = num(job.bag_buy_price) > 0 ? num(job.bag_buy_price) : num(job.unit_cost);
    const sellPrice = num(job.bag_sell_price);
    let bagId = num(existingBag?.id);
    if (existingBag) {
      await runAsync(`UPDATE bags SET total_qty=COALESCE(total_qty,0)+?, buy_price=?, sell_price=? WHERE id=?`, [qty, buyPrice, sellPrice > 0 ? sellPrice : num(existingBag.sell_price), existingBag.id]);
    } else {
      const ins = await runAsync(`INSERT INTO bags (length,width,gusset,color,handle,total_qty,min_qty,buy_price,sell_price) VALUES (?,?,?,?,?,?,?,?,?)`, [num(job.length), num(job.width), num(job.gusset), String(job.color || '').trim(), String(job.handle || '').trim(), qty, 0, buyPrice, sellPrice]);
      bagId = ins.lastID;
    }
    const bagRow = await getAsync(`SELECT * FROM bags WHERE id=?`, [bagId]);
    const note = `إضافة من أمر تشغيل #${num(job.id)} | ${String(job.note || '').trim()}`;
    await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [bagId, null, 'add', qty, bagRow.color, bagRow.handle, bagRow.length, bagRow.width, bagRow.gusset, note]);
    await runAsync(`UPDATE production_orders SET status='done', completed_at=?, completed_by=?, bag_id=? WHERE id=?`, [new Date().toISOString(), String(actor || 'system').trim(), bagId, num(job.id)]);
    await runAsync('COMMIT');
    return await getAsync(`SELECT * FROM production_orders WHERE id=?`, [num(job.id)]);
  } catch (err) {
    await runAsync('ROLLBACK');
    throw err;
  }
}

async function getProductionPlanOptions({ l=0, w=0, g=0, qty=0, color='', handle='', paperId=0 } = {}) {
  const safeQty = Math.max(1, num(qty));
  const params = [];
  let sql = `SELECT * FROM paper WHERE total_sheets > 0 AND total_kg > 0`;
  if (num(paperId) > 0) { sql += ` AND id=?`; params.push(num(paperId)); }
  sql += ` ORDER BY id DESC`;
  const papers = await allAsync(sql, params);
  const base = { l:num(l), w:num(w), g:num(g), qty:safeQty, color:String(color || '').trim(), handle:String(handle || '').trim() };
  const needsHandleStock = base.handle === 'بيد';
  const handleRow = needsHandleStock ? await getAsync(`SELECT * FROM handles WHERE color=?`, [base.color]) : null;
  let options = [];
  for (const paper of papers) {
    for (const layoutKey of ['pieceByPiece', 'singlePiece']) {
      const plan = computeLayout(base, paper, layoutKey);
      if (!plan) continue;
      const costs = calcProductionOrderCosts({ qty:safeQty, handle:base.handle, neededSheets:plan.neededSheets, paper, handleBuyPrice:num(handleRow?.buy_price) });
      const enoughPaperStock = num(plan.availableSheets) >= num(plan.neededSheets) && num(paper.total_kg) >= num(costs.neededKg);
      const enoughHandleStock = !needsHandleStock || (handleRow && num(handleRow.qty) >= safeQty);
      options.push({
        ...plan,
        paperLabelFull: paperLabelFull(paper),
        paperGrammage: num(paper.grammage),
        buy_price_kg: num(paper.buy_price_kg),
        enoughStock: !!(enoughPaperStock && enoughHandleStock),
        enoughPaperStock: !!enoughPaperStock,
        enoughHandleStock: !!enoughHandleStock,
        handleQtyAvailable: num(handleRow?.qty),
        handleBuyPrice: num(handleRow?.buy_price),
        colorMatch: bagColorMatchesPaper(base.color, paper.color),
        ...costs
      });
    }
  }
  const result = options.sort((a, b) => {
    if (a.enoughStock !== b.enoughStock) return a.enoughStock ? -1 : 1;
    if (a.colorMatch !== b.colorMatch) return a.colorMatch ? -1 : 1;
    if (a.wastePercent !== b.wastePercent) return a.wastePercent - b.wastePercent;
    if (a.total_cost !== b.total_cost) return a.total_cost - b.total_cost;
    return a.neededSheets - b.neededSheets;
  });
  return result;
}


const READY_BAG_SIZES = [
  '10 * 10 * 6','15 * 10 * 6','15 * 15 * 6','20 * 15 * 6','20 * 20 * 7','25 * 20 * 8','30 * 25 * 8','30 * 30 * 10','35 * 30 * 10','35 * 35 * 10','40 * 35 * 12','40 * 30 * 12','40 * 40 * 12','45 * 40 * 12','45 * 45 * 12','40 * 50 * 12','40 * 60 * 12'
].map(s => {
  const [length,width,gusset] = s.split('*').map(v => num(v));
  return { length, width, gusset, label: `${length} × ${width} × ${gusset}` };
});
function readyBagSizeKey(v) {
  return [num(v?.length), num(v?.width), num(v?.gusset)].join('|');
}
async function getReadyBagSizeRows() {
  const colors = ['بني', 'أبيض'];
  const handles = ['بيد', 'بدون يد'];
  const rows = await allAsync(`SELECT length,width,gusset,color,handle,total_qty FROM bags ORDER BY length,width,gusset,id`);
  const sizesMap = new Map(READY_BAG_SIZES.map(s => [readyBagSizeKey(s), { ...s, cells: Object.fromEntries(colors.flatMap(color => handles.map(handle => [`${color}_${handle}`, 0]))) }]));
  for (const row of rows) {
    const key = readyBagSizeKey(row);
    if (!sizesMap.has(key)) {
      sizesMap.set(key, {
        length: num(row.length),
        width: num(row.width),
        gusset: num(row.gusset),
        label: `${num(row.length)} × ${num(row.width)} × ${num(row.gusset)}`,
        cells: Object.fromEntries(colors.flatMap(color => handles.map(handle => [`${color}_${handle}`, 0])))
      });
    }
    if (!colors.includes(String(row.color || '').trim()) || !handles.includes(String(row.handle || '').trim())) continue;
    sizesMap.get(key).cells[`${row.color}_${row.handle}`] = num(row.total_qty);
  }
  return [...sizesMap.values()].sort((a, b) => num(a.length) - num(b.length) || num(a.width) - num(b.width) || num(a.gusset) - num(b.gusset));
}

async function findReadyBagMatch({ l=0, w=0, g=null, color='', handle='' }) {
  const params = [num(l), num(w), String(color || '').trim(), String(handle || '').trim()];
  if (g !== null && g !== undefined && String(g).trim() !== '') {
    return await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=? ORDER BY id DESC LIMIT 1`, [num(l), num(w), num(g), ...params.slice(2)]);
  }
  return await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND color=? AND handle=? ORDER BY total_qty DESC, id DESC LIMIT 1`, params);
}

// routes
app.get('/', (req, res) => res.sendFile(filePath('login.html')));

app.post('/login', async (req, res) => {
  try {
    const username = String(req.body.username || '').trim();
    const password = String(req.body.password || '');
    const row = await getAsync(`SELECT * FROM users WHERE username=? AND is_active=1`, [username]);
    if (!row || !verifyPassword(password, row.password)) return res.status(400).json({ error: 'بيانات غلط' });
    const token = crypto.randomBytes(24).toString('hex');
    const safe = userSafe(row);
    const ua = getUserAgent(req);
    const device = detectDeviceInfo(ua);
    const loginAt = new Date();
    const loginRow = await runAsync(`INSERT INTO user_sessions_history (username,full_name,login_at,ip_address,user_agent,device_type,device_label,is_active) VALUES (?,?,?,?,?,?,?,1)`, [safe.username || '', safe.full_name || '', loginAt.toISOString(), clientIp(req), ua, device.deviceType, device.label]);
    const sessionData = { ...safe, session_login_id: loginRow.lastID, session_started_at: loginAt.toISOString() };
    sessions.set(token, sessionData);
    await recordAudit({ req, user: sessionData, action: 'login', entity_type: 'session', entity_id: loginRow.lastID, details: `تسجيل دخول من ${device.label}` });
    res.json({ token, user: safe });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/logout', authRequired, async (req, res) => {
  const sessionRow = sessions.get(req.token);
  await closeSessionHistory(sessionRow, req);
  sessions.delete(req.token);
  res.json({ success: true });
});

app.get('/get-orders', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try { res.json(await getOrdersWithFiles(`SELECT * FROM orders ORDER BY id DESC`)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/order-file/:id/download', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const file = await getAsync(`SELECT * FROM order_files WHERE id=?`, [num(req.params.id)]);
    if (!file || !file.filepath || !fs.existsSync(file.filepath)) return res.status(404).json({ error: 'الملف غير موجود' });
    const rawName = String(file.originalname || file.filename || 'design').trim() || 'design';
    const downloadName = /\.pdf$/i.test(rawName) ? rawName : `${rawName}.pdf`;
    const encodedName = encodeURIComponent(downloadName).replace(/['()]/g, escape).replace(/\*/g, '%2A');
    const stats = fs.statSync(file.filepath);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodedName}`);
    res.setHeader('Content-Length', String(stats.size || 0));
    return fs.createReadStream(file.filepath).pipe(res);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/save-order', authRequired, requirePerm('perm_add_order'), upload.array('designFiles'), async (req, res) => {
  try {
    const b = req.body || {};
    for (const f of (req.files || [])) {
      const name = String(f.originalname || '').trim();
      const mime = String(f.mimetype || '').trim().toLowerCase();
      if (mime !== 'application/pdf' && !/\.pdf$/i.test(name)) {
        throw new Error('ملفات التصميم يجب أن تكون PDF فقط');
      }
    }
    let items = [];
    if (String(b.items_json || '').trim()) {
      try { items = JSON.parse(String(b.items_json)); } catch (_) { items = []; }
    }
    if (!Array.isArray(items) || !items.length) {
      items = [{
        l: b.l, w: b.w, g: b.g, qty: b.qty, color: b.color, handle: b.handle,
        total_price: b.total_price, paymentType: b.paymentType, paid_amount: b.paid_amount,
        printType: b.printType, colorSpecs: b.colorSpecs, paperGrammage: b.paperGrammage,
        useReadyStock: b.useReadyStock
      }];
    }
    items = items.map((item, idx) => ({
      ...item,
      printType: item.printType || 'سادة',
      colorSpecs: item.colorSpecs || 'سادة',
      useReadyStock: String(item.useReadyStock) === '1' || num(item.useReadyStock) === 1 ? 1 : 0,
      item_no: idx + 1
    }));
    const selectedCustomerId = num(b.selected_customer_id, 0);
    const selectedCustomerGovernorate = String(b.customer_governorate || '').trim();
    const selectedCustomerZone = String(b.customer_zone || '').trim();
    const selectedCustomerEmail = String(b.customer_email || '').trim();
    const useSharedFinancials = items.length > 1 && (
      String(b.group_total_price || '').trim() !== '' ||
      String(b.group_paymentType || '').trim() !== '' ||
      String(b.group_paid_amount || '').trim() !== ''
    );
    if (useSharedFinancials) {
      items = distributeSharedOrderFinancials(items, num(b.group_total_price, 0), num(b.group_paid_amount, 0), String(b.group_paymentType || '').trim() || 'لم يتم الدفع');
    }
    const bundleCode = items.length > 1 ? `GRP-${Date.now()}` : '';
    const dueDate = String(b.due_date || '').trim();
    const priority = normalizePriority(b.priority || 'عادي');
    const orderNotes = String(b.urgent_note || b.notes || '').trim();
    const createdIds = [];
    const bagSnapshots = new Map();
    const bagHistorySnapshots = new Map();

    await runAsync('BEGIN TRANSACTION');
    try {
      for (const item of items) {
        if (num(item.useReadyStock) === 1 && String(item.printType || '').trim() === 'أوفست') {
          throw new Error('السحب من المخزن الجاهز يدعم سادة أو سلك سكرين فقط');
        }
        if (num(item.useReadyStock) === 1 && (!String(item.g || '').trim() || num(item.g) <= 0)) {
          const readyBag = await findReadyBagMatch({ l:item.l, w:item.w, color:item.color, handle:item.handle });
          if (!readyBag) throw new Error('لا يوجد صنف مطابق في مخزن الشنط الجاهزة');
          item.g = readyBag.gusset;
        }
        const total = num(item.total_price);
        let paid = Math.min(num(item.paid_amount), total);
        if (item.paymentType === 'لم يتم الدفع') paid = 0;
        if (item.paymentType === 'مدفوع كامل') paid = total;
        const remaining = Math.max(total - paid, 0);
        const paymentType = remaining <= 0 ? 'مدفوع كامل' : (item.paymentType || 'لم يتم الدفع');
        const handFixCost = calcHandFixCost(item.handle || '', item.qty, num(item.useReadyStock));
        const r = await runAsync(`INSERT INTO orders (custName,custPhone,custAddress,orderDate,l,w,g,qty,color,handle,printType,colorSpecs,total_price,paid_amount,remaining_amount,paymentType,status,paperGrammage,useReadyStock,handle_stock_deducted,bag_returned_to_stock,cost_hand_fix,priority,due_date,notes,shipping_cost,group_code,item_no,item_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [
          b.custName || '', b.custPhone || '', b.custAddress || '', b.orderDate || today(),
          num(item.l), num(item.w), num(item.g), num(item.qty), item.color || '', item.handle || '', item.printType || 'سادة', item.colorSpecs || 'سادة',
          total, paid, remaining, paymentType, 'أوردر جديد', num(item.paperGrammage), num(item.useReadyStock), 0, 0, handFixCost,
          priority, dueDate || null, orderNotes, num(item.shipping_cost, 0), bundleCode, num(item.item_no), items.length
        ]);
        createdIds.push(r.lastID);
        if (selectedCustomerId || selectedCustomerGovernorate || selectedCustomerZone || selectedCustomerEmail) {
          await runAsync(`UPDATE orders SET customer_id=?, bosta_city_code=CASE WHEN TRIM(COALESCE(bosta_city_code,''))='' THEN ? ELSE bosta_city_code END, bosta_zone=CASE WHEN TRIM(COALESCE(bosta_zone,''))='' THEN ? ELSE bosta_zone END, bosta_receiver_email=CASE WHEN TRIM(COALESCE(bosta_receiver_email,''))='' THEN ? ELSE bosta_receiver_email END WHERE id=?`, [selectedCustomerId || 0, selectedCustomerGovernorate, selectedCustomerZone, selectedCustomerEmail, r.lastID]);
        }
        await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [r.lastID, null, 'أوردر جديد', req.user.full_name || req.user.username, items.length > 1 ? `إنشاء الأوردر ضمن مجموعة ${bundleCode}` : 'إنشاء الأوردر']);
        for (const f of (req.files || [])) {
          await runAsync(`INSERT INTO order_files (order_id,originalname,filename,filepath,mimetype) VALUES (?,?,?,?,?)`, [r.lastID, f.originalname, f.filename, f.path, f.mimetype || 'application/octet-stream']);
        }
        const readyPurchase = num(item.useReadyStock) === 1 ? await getReadyStockPurchaseTotal({ l:item.l, w:item.w, g:item.g, color:item.color, handle:item.handle, qty:item.qty }) : { bag:null, total:0 };
        await applyAutoOrderCosts(r.lastID, { ...item, bag_buy_price:num(readyPurchase?.bag?.buy_price), ready_stock_purchase_total:num(readyPurchase?.total), last_cut_layout:'pieceByPiece' }, req.user.full_name || req.user.username, 'order-create-auto');
        await syncCustomerForOrder(r.lastID);
        await syncOpeningOrderPayment(r.lastID, paid, req.user.full_name || req.user.username);
        await refreshOrderPaymentSummary(r.lastID, req.user.full_name || req.user.username);
        if (num(item.useReadyStock) === 1) {
          const bag = await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(item.l), num(item.w), num(item.g), item.color || '', item.handle || '']);
          if (!bag || num(bag.total_qty) < num(item.qty)) throw new Error('كمية الشنط الجاهزة غير كافية');
          if (!bagSnapshots.has(String(bag.id))) {
            bagSnapshots.set(String(bag.id), await snapshotTableSubset('bags', criteriaEq('id', bag.id)));
            bagHistorySnapshots.set(String(bag.id), await snapshotTableSubset('bags_history', criteriaEq('bag_id', bag.id)));
          }
          await runAsync(`UPDATE bags SET total_qty=total_qty-? WHERE id=?`, [num(item.qty), bag.id]);
          await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [bag.id, r.lastID, 'sub', num(item.qty), bag.color, bag.handle, bag.length, bag.width, bag.gusset, 'خصم من مخزن الشنط عند إنشاء الأوردر']);
        }
      }
      await runAsync('COMMIT');
    } catch (err) {
      await runAsync('ROLLBACK');
      throw err;
    }

    const undoPayload = [
      ...emptyUndoPayloadFromDefs(orderSnapshotDefs(createdIds)),
      ...[...bagSnapshots.values()].filter(Boolean),
      ...[...bagHistorySnapshots.values()].filter(Boolean)
    ];
    const touchedBagIds = [...bagSnapshots.keys()].map(v => num(v)).filter(Boolean);
    await recordAudit({
      req,
      action: 'create-order',
      entity_type: 'order',
      entity_id: createdIds[0],
      details: `عميل: ${String(b.custName || '').trim() || '-'} | ${items.length > 1 ? `إنشاء ${items.length} أصناف في مجموعة ${bundleCode}` : `إنشاء أوردر #${createdIds[0]}`} | أول صنف ${num(items[0]?.l)}×${num(items[0]?.w)}×${num(items[0]?.g)} | كمية ${num(items[0]?.qty)}` + (orderNotes ? ` | ملاحظة: ${orderNotes}` : ''),
      can_undo: 1,
      undo_type: 'create-order',
      undo_payload: undoPayload,
      touch_refs: [...orderTouchRefs(createdIds), ...bagTouchRefs(touchedBagIds)]
    });
    res.json({ success: true, id: createdIds[0], ids: createdIds, group_code: bundleCode, items_count: items.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.post('/update-status', authRequired, requirePerm('perm_change_status'), async (req, res) => {
  try {
    const id = num(req.body.id);
    const before = await getAsync(`SELECT * FROM orders WHERE id=?`, [id]);
    if (!before) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const status = ensureAllowedOrderStatus(before, req.body.status || before.status || 'أوردر جديد');
    if (isBackwardFromCut(normalizeOrderStatus(before.status), status)) return res.status(400).json({ error: 'لا يمكن الرجوع لحالة سابقة بعد القص' });
    const shippingStates = ['جاهز للشحن', 'تم الشحن', 'تم التسليم'];
    const movingAfterManufacturing = isAfterManufacturing(status);
    const needsHandleConfirmation = movingAfterManufacturing && !num(before.useReadyStock) && before.handle === 'بيد' && !num(before.handle_stock_deducted);
    if (needsHandleConfirmation) {
      const ok = await checkHandlesAvailable(before);
      if (!ok) return res.status(400).json({ error: 'كمية اليد غير كافية في المخزن' });
      if (!req.body.confirmHandleInstall) return res.status(400).json({ error: 'لازم تأكيد تركيب اليد أولاً' });
    }

    let paid = num(before.paid_amount), remaining = num(before.remaining_amount), paymentType = before.paymentType || 'لم يتم الدفع';
    let note = 'تغيير حالة';
    if (status === 'تم التسليم' && num(before.total_price) > 0) {
      if (req.body.settlePayment) {
        paid = num(before.total_price);
        remaining = 0;
        paymentType = 'مدفوع كامل';
        note = 'تم التسليم + دفع كامل';
      }
    }
    if (status === 'مرتجع') {
      paid = 0;
      remaining = num(before.total_price);
      paymentType = 'لم يتم الدفع';
      note = 'مرتجع';
    }

    const bagCouldChange = status === 'مرتجع' || (shippingStates.includes(status) && num(before.bag_returned_to_stock));
    const bagCriteria = [
      { column:'length', op:'eq', value:num(before.l) },
      { column:'width', op:'eq', value:num(before.w) },
      { column:'gusset', op:'eq', value:num(before.g) },
      { column:'color', op:'eq', value:before.color || '' },
      { column:'handle', op:'eq', value:before.handle || '' }
    ];
    const undoPayload = [
      ...(await buildUndoPayloadFromDefs(orderSnapshotDefs([id]))),
      ...(bagCouldChange ? [
        await snapshotTableSubset('bags', bagCriteria),
        await snapshotTableSubset('bags_history', criteriaEq('order_id', id))
      ].filter(Boolean) : []),
      ...(needsHandleConfirmation ? [
        await snapshotTableSubset('handles', criteriaEq('color', before.color || '')),
        await snapshotTableSubset('handles_history', criteriaEq('order_id', id))
      ].filter(Boolean) : [])
    ];

    if (status === 'مرتجع') {
      await maybeReturnBagOnReturn(before);
    } else if (shippingStates.includes(status) || needsHandleConfirmation) {
      if (num(before.bag_returned_to_stock)) await maybeDeductBagAgain(before);
      if (needsHandleConfirmation) await deductHandles(before, 'خصم اليد بعد تأكيد التركيب');
    }

    await runAsync(`UPDATE orders SET status=?, paid_amount=?, remaining_amount=?, paymentType=? WHERE id=?`, [status, paid, remaining, paymentType, id]);
    const afterOrder = await getAsync(`SELECT * FROM orders WHERE id=?`, [id]);
    await syncSaleForOrder(afterOrder, req.user.full_name || req.user.username);
    await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [id, before.status, status, req.user.full_name || req.user.username, note]);

    const bagAfter = bagCouldChange ? await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(before.l), num(before.w), num(before.g), before.color || '', before.handle || '']) : null;
    const handleAfter = needsHandleConfirmation ? await getAsync(`SELECT * FROM handles WHERE color=?`, [before.color || '']) : null;
    await recordAudit({
      req,
      action: 'update-status',
      entity_type: 'order',
      entity_id: id,
      details: `تغيير حالة الأوردر من ${before.status || '-'} إلى ${status}`,
      can_undo: 1,
      undo_type: 'update-status',
      undo_payload: undoPayload,
      touch_refs: [
        ...orderTouchRefs([id]),
        ...(bagAfter ? bagTouchRefs([bagAfter.id]) : []),
        ...(handleAfter ? handleTouchRefs([handleAfter.id]) : [])
      ]
    });
    res.json({ success: true, paymentType, paid_amount: paid, remaining_amount: remaining });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/update-costs', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const b = req.body || {};
    const before = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(b.id)]);
    if (!before) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const manualHandFix = num(b.cost_hand_fix);
    await runAsync(`UPDATE orders SET cost_cut=?,cost_print=?,cost_zinc=?,cost_design=?,cost_make=?,cost_hand=?,cost_paper=?,cost_hand_fix=? WHERE id=?`, [num(b.cost_cut), num(b.cost_print), num(b.cost_zinc), num(b.cost_design), num(b.cost_make), num(b.cost_hand), num(b.cost_paper), manualHandFix, num(b.id)]);
    const after = { cost_cut:num(b.cost_cut), cost_print:num(b.cost_print), cost_zinc:num(b.cost_zinc), cost_design:num(b.cost_design), cost_make:num(b.cost_make), cost_hand:num(b.cost_hand), cost_paper:num(b.cost_paper), cost_hand_fix:manualHandFix };
    for (const field of ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix']) {
      const diff = num(after[field]) - num(before?.[field]);
      if (diff > 0) await addCostLog({ order_id:num(b.id), cost_field:field, amount:diff, source:'order-costs', source_ref:String(num(b.id)), notes:`إضافة تكلفة ${costFieldLabel(field)}`, created_by:req.user.full_name || req.user.username });
    }
    const refreshed = await refreshOrderPaymentSummary(num(b.id), req.user.full_name || req.user.username);
    await recordAudit({ req, action: 'update-costs', entity_type: 'order', entity_id: num(b.id), details: 'تعديل تكاليف الأوردر' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


function orderBagSpecKey(order = {}) {
  return [num(order.l), num(order.w), num(order.g), String(order.color || '').trim(), String(order.handle || '').trim()].join('||');
}
function isReadyBagReserved(order = {}) {
  return num(order.useReadyStock) === 1 && !num(order.bag_returned_to_stock);
}
async function moveBagStockForEdit(order = {}, qtyDelta = 0, note = '') {
  const delta = Math.trunc(num(qtyDelta));
  if (!delta || !num(order.qty)) return;
  const criteria = [num(order.l), num(order.w), num(order.g), String(order.color || '').trim(), String(order.handle || '').trim()];
  let bag = await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, criteria);
  if (delta > 0) {
    if (!bag || num(bag.total_qty) < delta) throw new Error('كمية الشنط الجاهزة غير كافية بعد تعديل الأوردر');
    await runAsync(`UPDATE bags SET total_qty=COALESCE(total_qty,0)-? WHERE id=?`, [delta, bag.id]);
    await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note,date) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [bag.id, num(order.id), 'sub', delta, bag.color, bag.handle, num(bag.length), num(bag.width), num(bag.gusset), note || `خصم فرق تعديل أوردر #${num(order.id)}`, new Date().toISOString()]);
    return;
  }
  const restoreQty = Math.abs(delta);
  if (!bag) {
    const ins = await runAsync(`INSERT INTO bags (length,width,gusset,color,handle,total_qty,min_qty,buy_price,sell_price) VALUES (?,?,?,?,?,?,?,?,?)`, [num(order.l), num(order.w), num(order.g), String(order.color || '').trim(), String(order.handle || '').trim(), restoreQty, 0, 0, 0]);
    bag = await getAsync(`SELECT * FROM bags WHERE id=?`, [ins.lastID]);
  } else {
    await runAsync(`UPDATE bags SET total_qty=COALESCE(total_qty,0)+? WHERE id=?`, [restoreQty, bag.id]);
    bag = await getAsync(`SELECT * FROM bags WHERE id=?`, [bag.id]);
  }
  await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note,date) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [bag.id, num(order.id), 'add', restoreQty, bag.color, bag.handle, num(bag.length), num(bag.width), num(bag.gusset), note || `استرجاع فرق تعديل أوردر #${num(order.id)}`, new Date().toISOString()]);
}
async function getOrderPaperConsumptionSnapshot(order = {}) {
  if (!num(order.paper_cut_done) || !num(order.last_cut_paper_id)) return null;
  const paper = await getAsync(`SELECT * FROM paper WHERE id=?`, [num(order.last_cut_paper_id)]);
  if (!paper) return null;
  const chosen = computeLayout(order, paper, String(order.last_cut_layout || 'pieceByPiece').trim() || 'pieceByPiece');
  const sheets = Math.max(0, Math.ceil(num(chosen?.neededSheets)));
  const kg = roundMoney(((num(paper.length) * num(paper.width) * num(paper.grammage) * sheets) / 10000000));
  return { paper, paperId: num(paper.id), sheets, kg };
}
async function applyPaperConsumptionDelta(snapshot, deltaSheets = 0, deltaKg = 0, orderId = 0, note = '') {
  if (!snapshot || (!deltaSheets && !deltaKg)) return;
  const paper = snapshot.paper;
  if (!paper) return;
  if (deltaSheets > 0 || deltaKg > 0) {
    const freshPaper = await getAsync(`SELECT * FROM paper WHERE id=?`, [snapshot.paperId]);
    if (!freshPaper || num(freshPaper.total_sheets) < deltaSheets || num(freshPaper.total_kg) < deltaKg) throw new Error('كمية الورق غير كافية بعد تعديل الأوردر');
    await runAsync(`UPDATE paper SET total_kg=COALESCE(total_kg,0)-?, total_sheets=COALESCE(total_sheets,0)-? WHERE id=?`, [roundMoney(deltaKg), roundMoney(deltaSheets), snapshot.paperId]);
    await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note,date) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [snapshot.paperId, 'sub', -roundMoney(deltaKg), -roundMoney(deltaSheets), freshPaper.color || '', num(freshPaper.length), num(freshPaper.width), num(freshPaper.grammage), freshPaper.paper_name || '', note || `خصم فرق ورق تعديل الأوردر #${num(orderId)}`, new Date().toISOString()]);
    return;
  }
  const restoreSheets = Math.abs(roundMoney(deltaSheets));
  const restoreKg = Math.abs(roundMoney(deltaKg));
  await runAsync(`UPDATE paper SET total_kg=COALESCE(total_kg,0)+?, total_sheets=COALESCE(total_sheets,0)+? WHERE id=?`, [restoreKg, restoreSheets, snapshot.paperId]);
  await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note,date) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [snapshot.paperId, 'add', restoreKg, restoreSheets, paper.color || '', num(paper.length), num(paper.width), num(paper.grammage), paper.paper_name || '', note || `استرجاع فرق ورق تعديل الأوردر #${num(orderId)}`, new Date().toISOString()]);
}
async function moveHandleStockForEdit(color = '', qtyDelta = 0, orderId = 0, note = '') {
  const delta = Math.trunc(num(qtyDelta));
  const handleColor = String(color || '').trim();
  if (!delta || !handleColor) return;
  let handle = await getAsync(`SELECT * FROM handles WHERE color=?`, [handleColor]);
  if (delta > 0) {
    if (!handle || num(handle.qty) < delta) throw new Error('كمية اليد غير كافية بعد تعديل الأوردر');
    await runAsync(`UPDATE handles SET qty=COALESCE(qty,0)-? WHERE id=?`, [delta, handle.id]);
    await runAsync(`INSERT INTO handles_history (handle_id,order_id,type,qty,color,note,date) VALUES (?,?,?,?,?,?,?)`, [handle.id, num(orderId), 'sub', delta, handle.color || handleColor, note || `خصم فرق يد تعديل الأوردر #${num(orderId)}`, new Date().toISOString()]);
    return;
  }
  const restoreQty = Math.abs(delta);
  if (!handle) {
    const ins = await runAsync(`INSERT INTO handles (color,qty,buy_price,min_qty) VALUES (?,?,?,?)`, [handleColor, restoreQty, 0, 0]);
    handle = await getAsync(`SELECT * FROM handles WHERE id=?`, [ins.lastID]);
  } else {
    await runAsync(`UPDATE handles SET qty=COALESCE(qty,0)+? WHERE id=?`, [restoreQty, handle.id]);
    handle = await getAsync(`SELECT * FROM handles WHERE id=?`, [handle.id]);
  }
  await runAsync(`INSERT INTO handles_history (handle_id,order_id,type,qty,color,note,date) VALUES (?,?,?,?,?,?,?)`, [handle.id, num(orderId), 'add', restoreQty, handle.color || handleColor, note || `استرجاع فرق يد تعديل الأوردر #${num(orderId)}`, new Date().toISOString()]);
}
async function applyOrderInventoryDelta({ before = {}, after = {}, actor = 'system' } = {}) {
  const orderId = num(before.id || after.id);
  const notes = {
    bagSub: `خصم فرق تعديل أوردر #${orderId} بواسطة ${actor}`,
    bagAdd: `استرجاع فرق تعديل أوردر #${orderId} بواسطة ${actor}`,
    paperSub: `خصم فرق ورق تعديل الأوردر #${orderId} بواسطة ${actor}`,
    paperAdd: `استرجاع فرق ورق تعديل الأوردر #${orderId} بواسطة ${actor}`,
    handleSub: `خصم فرق يد تعديل الأوردر #${orderId} بواسطة ${actor}`,
    handleAdd: `استرجاع فرق يد تعديل الأوردر #${orderId} بواسطة ${actor}`
  };

  const beforeBagReserved = isReadyBagReserved(before);
  const afterBagReserved = isReadyBagReserved(after);
  if (beforeBagReserved && afterBagReserved) {
    if (orderBagSpecKey(before) === orderBagSpecKey(after)) {
      const deltaQty = Math.trunc(num(after.qty) - num(before.qty));
      if (deltaQty > 0) await moveBagStockForEdit(after, deltaQty, notes.bagSub);
      else if (deltaQty < 0) await moveBagStockForEdit(before, deltaQty, notes.bagAdd);
    } else {
      await moveBagStockForEdit(before, -Math.trunc(num(before.qty)), notes.bagAdd);
      await moveBagStockForEdit(after, Math.trunc(num(after.qty)), notes.bagSub);
    }
  } else if (beforeBagReserved && !afterBagReserved) {
    await moveBagStockForEdit(before, -Math.trunc(num(before.qty)), notes.bagAdd);
  } else if (!beforeBagReserved && afterBagReserved) {
    await moveBagStockForEdit(after, Math.trunc(num(after.qty)), notes.bagSub);
  }

  const beforePaper = await getOrderPaperConsumptionSnapshot(before);
  const afterPaper = await getOrderPaperConsumptionSnapshot(after);
  if (beforePaper && afterPaper && beforePaper.paperId === afterPaper.paperId) {
    const deltaSheets = roundMoney(num(afterPaper.sheets) - num(beforePaper.sheets));
    const deltaKg = roundMoney(num(afterPaper.kg) - num(beforePaper.kg));
    if (deltaSheets > 0 || deltaKg > 0) await applyPaperConsumptionDelta(afterPaper, deltaSheets, deltaKg, orderId, notes.paperSub);
    else if (deltaSheets < 0 || deltaKg < 0) await applyPaperConsumptionDelta(beforePaper, deltaSheets, deltaKg, orderId, notes.paperAdd);
  } else {
    if (beforePaper) await applyPaperConsumptionDelta(beforePaper, -num(beforePaper.sheets), -num(beforePaper.kg), orderId, notes.paperAdd);
    if (afterPaper) await applyPaperConsumptionDelta(afterPaper, num(afterPaper.sheets), num(afterPaper.kg), orderId, notes.paperSub);
  }

  const beforeHandleConsumed = num(before.handle_stock_deducted) === 1 && String(before.handle || '').trim() === 'بيد' && num(before.useReadyStock) !== 1;
  const afterHandleDesired = beforeHandleConsumed && String(after.handle || '').trim() === 'بيد' && num(after.useReadyStock) !== 1;
  let nextHandleStockDeducted = beforeHandleConsumed ? 1 : num(before.handle_stock_deducted);
  if (beforeHandleConsumed && afterHandleDesired) {
    if (String(before.color || '').trim() === String(after.color || '').trim()) {
      const deltaQty = Math.trunc(num(after.qty) - num(before.qty));
      if (deltaQty > 0) await moveHandleStockForEdit(after.color, deltaQty, orderId, notes.handleSub);
      else if (deltaQty < 0) await moveHandleStockForEdit(before.color, deltaQty, orderId, notes.handleAdd);
    } else {
      await moveHandleStockForEdit(before.color, -Math.trunc(num(before.qty)), orderId, notes.handleAdd);
      await moveHandleStockForEdit(after.color, Math.trunc(num(after.qty)), orderId, notes.handleSub);
    }
  } else if (beforeHandleConsumed && !afterHandleDesired) {
    await moveHandleStockForEdit(before.color, -Math.trunc(num(before.qty)), orderId, notes.handleAdd);
    nextHandleStockDeducted = 0;
  }
  return { nextHandleStockDeducted };
}

app.post('/update-order', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const b = req.body || {};
    const before = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(b.id)]);
    if (!before) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const undoPayload = await buildUndoPayloadFromDefs(orderSnapshotDefs([num(b.id)]));
    const nextStatus = ensureAllowedOrderStatus({ ...before, useReadyStock: num(b.useReadyStock, before.useReadyStock) }, b.status || before.status || 'أوردر جديد');
    if (isBackwardFromCut(normalizeOrderStatus(before.status), nextStatus)) return res.status(400).json({ error: 'لا يمكن الرجوع لحالة سابقة بعد القص' });
    const total = num(b.total_price);
    let paid = Math.min(num(b.paid_amount), total);
    if (b.paymentType === 'لم يتم الدفع') paid = 0;
    if (b.paymentType === 'مدفوع كامل') paid = total;
    const remaining = Math.max(total - paid, 0);
    const paymentType = remaining <= 0 ? 'مدفوع كامل' : (b.paymentType || before.paymentType || 'لم يتم الدفع');
    const handFixCost = calcHandFixCost(b.handle || before.handle || '', num(b.qty), num(b.useReadyStock));
    if (num(b.useReadyStock) === 1 && String(b.printType || '').trim() === 'أوفست') return res.status(400).json({ error: 'السحب من المخزن الجاهز يدعم سادة أو سلك سكرين فقط' });
    if (num(b.useReadyStock) === 1 && (!String(b.g || '').trim() || num(b.g) <= 0)) {
      const readyBag = await findReadyBagMatch({ l:b.l || before.l, w:b.w || before.w, color:b.color || before.color, handle:b.handle || before.handle });
      if (!readyBag) return res.status(400).json({ error: 'لا يوجد صنف مطابق في مخزن الشنط الجاهزة' });
      b.g = readyBag.gusset;
    }
    const priority = normalizePriority(b.priority || before.priority || 'عادي');
    const dueDate = String(b.due_date || before.due_date || '').trim();
    const shippingCost = Math.max(0, num(b.shipping_cost, before.shipping_cost || before.bosta_price_after_vat || before.bosta_estimated_fees || before.bosta_shipping_fee || 0));
    const afterPreview = {
      ...before,
      id: num(b.id),
      custName: b.custName || '', custPhone: b.custPhone || '', custAddress: b.custAddress || '',
      orderDate: b.orderDate || today(), l: num(b.l), w: num(b.w), g: num(b.g), qty: num(b.qty),
      color: b.color || '', handle: b.handle || '', printType: b.printType || '', colorSpecs: b.colorSpecs || '',
      total_price: total, paid_amount: paid, remaining_amount: remaining, paymentType, status: nextStatus,
      paperGrammage: num(b.paperGrammage), useReadyStock: num(b.useReadyStock), cost_hand_fix: handFixCost,
      priority, due_date: dueDate || null, shipping_cost: shippingCost
    };
    await runAsync('BEGIN TRANSACTION');
    try {
      const inventoryDelta = await applyOrderInventoryDelta({ before, after: afterPreview, actor: req.user.full_name || req.user.username });
      const nextHandleStockDeducted = Number.isFinite(Number(inventoryDelta?.nextHandleStockDeducted)) ? Number(inventoryDelta.nextHandleStockDeducted) : num(before.handle_stock_deducted);
      const nextBagReturned = num(afterPreview.useReadyStock) === 1 ? num(before.bag_returned_to_stock) : 0;
      await runAsync(`UPDATE orders SET custName=?,custPhone=?,custAddress=?,orderDate=?,l=?,w=?,g=?,qty=?,color=?,handle=?,printType=?,colorSpecs=?,total_price=?,paid_amount=?,remaining_amount=?,paymentType=?,status=?,paperGrammage=?,useReadyStock=?,handle_stock_deducted=?,bag_returned_to_stock=?,cost_hand_fix=?,priority=?,due_date=?,shipping_cost=? WHERE id=?`, [b.custName || '', b.custPhone || '', b.custAddress || '', b.orderDate || today(), num(b.l), num(b.w), num(b.g), num(b.qty), b.color || '', b.handle || '', b.printType || '', b.colorSpecs || '', total, paid, remaining, paymentType, nextStatus, num(b.paperGrammage), num(b.useReadyStock), nextHandleStockDeducted, nextBagReturned, handFixCost, priority, dueDate || null, shippingCost, num(b.id)]);
      await syncCustomerForOrder(num(b.id));
      await syncOpeningOrderPayment(num(b.id), paid, req.user.full_name || req.user.username);
      const readyPurchase = num(b.useReadyStock) === 1 ? await getReadyStockPurchaseTotal({ l:b.l, w:b.w, g:b.g, color:b.color, handle:b.handle, qty:b.qty }) : { bag:null, total:0 };
      await applyAutoOrderCosts(num(b.id), { ...before, ...b, useReadyStock:num(b.useReadyStock), bag_buy_price:num(readyPurchase?.bag?.buy_price), ready_stock_purchase_total:num(readyPurchase?.total), last_cut_layout: before.last_cut_layout || 'pieceByPiece' }, req.user.full_name || req.user.username, 'order-edit-auto', before);
      if (normalizeOrderStatus(before.status) !== nextStatus) {
        await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [num(b.id), normalizeOrderStatus(before.status), nextStatus, req.user.full_name || req.user.username, 'تعديل الأوردر']);
      }
      const handFixDiff = handFixCost - num(before.cost_hand_fix);
      if (handFixDiff > 0) await addCostLog({ order_id:num(b.id), cost_field:'cost_hand_fix', amount: handFixDiff, source:'order-edit', source_ref:String(num(b.id)), notes:'تحديث تركيب اليد', created_by:req.user.full_name || req.user.username });
      const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(b.id)]);
      await syncSaleForOrder(refreshed, req.user.full_name || req.user.username);
      const orderDiff = describeDiff(before, { custName: b.custName || '', custPhone: b.custPhone || '', custAddress: b.custAddress || '', l: num(b.l), w: num(b.w), g: num(b.g), qty: num(b.qty), color: b.color || '', handle: b.handle || '', printType: b.printType || '', colorSpecs: b.colorSpecs || '', total_price: total, paid_amount: paid, remaining_amount: remaining, paymentType, status: nextStatus, paperGrammage: num(b.paperGrammage), useReadyStock: num(b.useReadyStock), priority, due_date: dueDate || null, shipping_cost: shippingCost });
      await recordAudit({
        req,
        action: 'update-order',
        entity_type: 'order',
        entity_id: num(b.id),
        details: `تعديل أوردر العميل ${b.custName || before.custName || ''} | ${orderDiff || 'بدون تفاصيل إضافية'}`,
        can_undo: 1,
        undo_type: 'update-order',
        undo_payload: undoPayload,
        touch_refs: orderTouchRefs([num(b.id)])
      });
      await runAsync('COMMIT');
      res.json({ success: true });
    } catch (err) {
      await runAsync('ROLLBACK');
      throw err;
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/bosta-cities', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const rows = await getBostaCitiesCached();
    const fallbackItems = fallbackBostaCityOptions();
    const options = fallbackItems.map(item => {
      const fallbackLabel = String(item.label || '').trim();
      const fallbackKey = normalizeArabicLocationText(fallbackLabel);
      const match = findBostaCityByGovernorateLabel(rows, fallbackLabel);
      const matchedInfo = cityInfoFromRow(match || {});
      return {
        code: matchedInfo.code || item.code,
        label: fallbackLabel,
        labels: [fallbackLabel, ...(matchedInfo.labels || []).filter(v => normalizeArabicLocationText(v) !== fallbackKey)],
        isFallback: !matchedInfo.code
      };
    });
    res.json(options);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/bosta-zones/:cityCode', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const cityCode = String(req.params.cityCode || '').trim();
    if (!cityCode) return res.json([]);
    const cities = await getBostaCitiesCached();
    const cityLabel = inferFallbackCityLabel(cityCode, cities);
    const fallbackOptions = fallbackBostaZonesByCityLabel(cityLabel || cityCode);
    if (fallbackOptions.length) return res.json(fallbackOptions);
    const rows = await getBostaZonesCached(cityCode);
    let options = bostaZoneOptions(rows);
    if (!options.length || options.every(item => looksLikeBrokenZoneOption(item.label))) {
      options = fallbackOptions;
    }
    res.json(options.length ? options : []);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/bosta-infer-location', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const body = req.body || {};
    const inferred = await inferBostaLocation(body.address || '', body.city_code || '', body.zone || '');
    res.json(inferred);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


app.post('/bosta-estimate', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const body = req.body || {};
    const cityCode = String(body.city_code || '').trim();
    const zone = String(body.zone || '').trim();
    if (!cityCode || !zone) return res.json({ shippingFee: 0, insuranceFees: 0, openPackageFees: 0, estimatedFees: 0, source: 'none', text: '' });
    const estimate = await getBostaEstimateDetails({
      cityCode,
      zone,
      packageType: String(body.package_type || 'Parcel').trim() || 'Parcel',
      cod: Math.max(0, num(body.cod, 0)),
      productValue: Math.max(0, num(body.product_value, 0)),
      allowOpen: !!num(body.allow_open, 0),
      itemsCount: Math.max(1, num(body.items_count, 1)),
      secondLine: String(body.second_line || '').trim(),
      description: String(body.package_description || '').trim() || 'Shipment'
    });
    res.json(estimate);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/send-to-bosta/:id', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    ensureBostaConfigured();
    const id = num(req.params.id);
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [id]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
    if (String(order.status || '').trim() !== 'جاهز للشحن') return res.status(400).json({ error: 'الأوردر لازم يكون جاهز للشحن أولاً' });
    if (String(order.bosta_delivery_id || '').trim()) return res.status(400).json({ error: 'تم إرسال هذا الأوردر إلى بوسطة بالفعل' });

    const customer = num(order.customer_id)
      ? await getAsync(`SELECT governorate, zone, email FROM customers WHERE id=?`, [num(order.customer_id)])
      : await findMatchingCustomerRecord({ name: order.custName, phone: order.custPhone });
    const customerGovernorate = String(customer?.governorate || '').trim();
    const customerZone = String(customer?.zone || '').trim();
    const customerEmail = String(customer?.email || '').trim();
    const orderSaleValue = await getOrderBostaProductValue(order);

    const body = req.body || {};
    const bostaCities = await getBostaCitiesCached();
    const selectedCityToken = String(body.city_code || order.bosta_city_code || customerGovernorate || '').trim();
    const selectedCityLabel = String(body.city_label || resolveConfiguredBostaGovernorateLabel(selectedCityToken, bostaCities) || customerGovernorate || '').trim();
    const selectedZoneToken = String(body.zone || order.bosta_zone || customerZone || '').trim();
    const secondLine = String(body.second_line || order.bosta_second_line || order.custAddress || '').trim();
    const receiverPhone = normalizePhone(body.receiver_phone || order.bosta_receiver_phone || order.custPhone || '');
    const receiverName = String(body.receiver_name || order.bosta_receiver_name || order.custName || '').trim();
    const productValue = Math.max(0, num(body.product_value, order.bosta_product_value || orderSaleValue || order.total_price || 0));
    const allowOpen = !!num(body.allow_open, order.bosta_allow_open || 0);
    const location = await resolveBostaShipmentLocation({
      address: secondLine || order.custAddress || '',
      cityToken: selectedCityToken || order.bosta_city_code || customerGovernorate || '',
      cityLabel: selectedCityLabel || customerGovernorate || '',
      zoneToken: selectedZoneToken || order.bosta_zone || customerZone || ''
    });
    const cityCode = String(location.cityCode || '').trim();
    const zone = String(location.zone || '').trim();
    const district = String(body.district || order.bosta_district || selectedZoneToken || customerZone || zone || '').trim();
    const resolvedBody = {
      ...body,
      city_code: cityCode,
      city_label: location.cityLabel || selectedCityLabel || customerGovernorate || '',
      zone,
      district,
      second_line: secondLine,
      receiver_phone: receiverPhone,
      receiver_name: receiverName,
      receiver_email: String(body.receiver_email || order.bosta_receiver_email || customerEmail || '').trim(),
      product_value: productValue,
      allow_open: allowOpen ? 1 : 0
    };

    if (!receiverName) return res.status(400).json({ error: 'اسم المستلم مطلوب' });
    if (!receiverPhone) return res.status(400).json({ error: 'رقم الهاتف مطلوب' });
    if (!secondLine) return res.status(400).json({ error: 'العنوان مطلوب' });
    if (!cityCode) {
      return res.status(400).json({ error: 'تعذر تحويل المحافظة المختارة إلى كود بوسطة صحيح. راجع المحافظة في بيانات العميل أو اخترها يدويًا من نافذة بوسطة.' });
    }
    if (!zone) {
      return res.status(400).json({ error: 'تعذر تحديد المنطقة الخاصة بالمحافظة المختارة. اختر المنطقة يدويًا من نافذة بوسطة.' });
    }

    const estimateInfo = await getBostaEstimateDetails({
      cityCode,
      cityLabel: resolvedBody.city_label || selectedCityLabel || cityCode,
      zone,
      packageType: String(resolvedBody.package_type || order.bosta_package_type || 'Parcel').trim() || 'Parcel',
      cod: Math.max(0, num(resolvedBody.cod, order.remaining_amount || 0)),
      productValue,
      allowOpen,
      itemsCount: Math.max(1, num(resolvedBody.items_count, order.qty || 1)),
      secondLine,
      description: String(resolvedBody.package_description || order.bosta_package_description || `أوردر رقم ${id}`).trim()
    });

    const payload = buildBostaPayload(order, resolvedBody, req);
    const response = await fetch(`${BOSTA_BASE_URL}/deliveries`, {
      method: 'POST',
      headers: {
        Authorization: BOSTA_API_KEY,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const rawText = await response.text();
    let rawJson = {};
    try { rawJson = rawText ? JSON.parse(rawText) : {}; } catch (_) { rawJson = { message: rawText }; }
    if (!response.ok) {
      return res.status(response.status >= 500 ? 502 : 400).json({ error: rawJson?.message || rawJson?.error || rawText || 'فشل الإرسال إلى بوسطة' });
    }

    const info = extractBostaResponseInfo(rawJson);
    const sentAt = new Date().toISOString();
    const shippingStatus = 'تم الشحن';
    const businessReference = String(resolvedBody.business_reference || order.bosta_business_reference || `ORDER-${id}`).trim();
    const packageType = String(resolvedBody.package_type || order.bosta_package_type || 'Parcel').trim() || 'Parcel';
    const packageDescription = String(resolvedBody.package_description || order.bosta_package_description || `أوردر رقم ${id}`).trim();
    const codAmount = Math.max(0, num(resolvedBody.cod, order.remaining_amount || 0));
    const orderNote = String(resolvedBody.notes || order.bosta_notes || '').trim();
    const productValueSaved = Math.max(0, num(resolvedBody.product_value, order.bosta_product_value || orderSaleValue || 0));
    const allowOpenSaved = !!num(resolvedBody.allow_open, order.bosta_allow_open || 0);

    const finalShippingFee = roundMoney(estimateInfo.shippingFee || info.shippingFee || 0);
    const finalRawShippingFee = roundMoney(estimateInfo.rawShippingFee || info.rawShippingFee || info.shippingFee || 0);
    const finalInsuranceFees = roundMoney(estimateInfo.insuranceFees || info.insuranceFees || 0);
    const finalOpenPackageFees = roundMoney(allowOpenSaved ? (estimateInfo.openPackageFees || info.openPackageFees || BOSTA_OPEN_PACKAGE_FEE) : 0);
    const finalExtraCodFee = roundMoney(estimateInfo.extraCodFee || info.extraCodFee || 0);
    const finalBostaMaterialFee = roundMoney(estimateInfo.bostaMaterialFee || info.bostaMaterialFee || 0);
    const finalExpediteFee = roundMoney(estimateInfo.expediteFee || info.expediteFee || 0);
    const finalVatRate = Number(estimateInfo.vatRate || info.vatRate || 0) || 0;
    const finalVatAmount = roundMoney(estimateInfo.vatAmount || info.vatAmount || 0);
    const finalPriceBeforeVat = roundMoney(estimateInfo.priceBeforeVat || info.priceBeforeVat || (finalShippingFee + finalInsuranceFees + finalOpenPackageFees + finalExtraCodFee + finalBostaMaterialFee + finalExpediteFee));
    const finalPriceAfterVat = roundMoney(estimateInfo.priceAfterVat || info.priceAfterVat || (finalPriceBeforeVat + finalVatAmount));
    const finalEstimatedFees = roundMoney(estimateInfo.estimatedFees || info.estimatedFees || (finalShippingFee + finalInsuranceFees + finalOpenPackageFees));
    const finalFeesText = String(info.feesText || estimateInfo.text || order.bosta_estimated_fees_text || '').trim();
    const finalEstimateSource = String(estimateInfo.source || (info.estimatedFees ? 'api' : 'local')).trim();
    const effectiveShippingCost = roundMoney(finalShippingFee || finalRawShippingFee || 0);

    await runAsync(`UPDATE orders SET status=?, bosta_delivery_id=?, bosta_tracking_number=?, bosta_status=?, bosta_sent_at=?, bosta_city_code=?, bosta_zone=?, bosta_district=?, bosta_building_number=?, bosta_floor=?, bosta_apartment=?, bosta_second_line=?, bosta_receiver_name=?, bosta_receiver_phone=?, bosta_receiver_email=?, bosta_business_reference=?, bosta_package_type=?, bosta_package_description=?, bosta_product_value=?, bosta_allow_open=?, bosta_shipping_fee=?, bosta_raw_shipping_fee=?, bosta_open_package_fees=?, bosta_material_fee=?, bosta_extra_cod_fee=?, bosta_expedite_fee=?, bosta_vat_rate=?, bosta_vat_amount=?, bosta_price_before_vat=?, bosta_price_after_vat=?, bosta_estimate_source=?, bosta_estimated_fees=?, bosta_insurance_fees=?, bosta_estimated_fees_text=?, bosta_cod=?, bosta_notes=?, bosta_last_response=?, shipping_cost=? WHERE id=?`, [
      shippingStatus,
      info.deliveryId || '',
      info.trackingNumber || '',
      info.statusText || shippingStatus,
      sentAt,
      cityCode,
      zone,
      district,
      String(resolvedBody.building_number || order.bosta_building_number || '').trim(),
      String(resolvedBody.floor || order.bosta_floor || '').trim(),
      String(resolvedBody.apartment || order.bosta_apartment || '').trim(),
      secondLine,
      receiverName,
      receiverPhone,
      String(resolvedBody.receiver_email || order.bosta_receiver_email || '').trim(),
      businessReference,
      packageType,
      packageDescription,
      productValueSaved,
      allowOpenSaved ? 1 : 0,
      finalShippingFee,
      finalRawShippingFee,
      finalOpenPackageFees,
      finalBostaMaterialFee,
      finalExtraCodFee,
      finalExpediteFee,
      finalVatRate,
      finalVatAmount,
      finalPriceBeforeVat,
      finalPriceAfterVat,
      finalEstimateSource,
      finalEstimatedFees,
      finalInsuranceFees,
      finalFeesText,
      codAmount,
      orderNote,
      JSON.stringify(info.rawBody || estimateInfo.rawBody || {}),
      effectiveShippingCost,
      id
    ]);

    await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [id, order.status || 'جاهز للشحن', shippingStatus, req.user.full_name || req.user.username, `إرسال إلى بوسطة${info.trackingNumber ? ` | رقم التتبع ${info.trackingNumber}` : ''}`]);
    await recordAudit({ req, action: 'send-to-bosta', entity_type: 'order', entity_id: id, details: `إرسال أوردر #${id} إلى بوسطة | ${receiverName} | ${cityCode}/${zone}${allowOpenSaved ? ' | فتح مسموح' : ' | فتح غير مسموح'}${productValueSaved ? ` | قيمة المنتج ${productValueSaved}` : ''}${finalEstimatedFees ? ` | مستحقات متوقعة ${finalEstimatedFees}` : ''}${info.trackingNumber ? ` | تتبع ${info.trackingNumber}` : ''}` });
    res.json({ success: true, deliveryId: info.deliveryId || '', trackingNumber: info.trackingNumber || '', status: shippingStatus, shippingFee: finalShippingFee, rawShippingFee: finalRawShippingFee, openPackageFees: finalOpenPackageFees, materialFee: finalBostaMaterialFee, extraCodFee: finalExtraCodFee, expediteFee: finalExpediteFee, vatRate: finalVatRate, vatAmount: finalVatAmount, priceBeforeVat: finalPriceBeforeVat, priceAfterVat: finalPriceAfterVat, estimateSource: finalEstimateSource, estimatedFees: finalEstimatedFees, insuranceFees: finalInsuranceFees, feesText: finalFeesText, raw: info.rawBody || estimateInfo.rawBody || {}, inferredCity: resolvedBody.city_label || '', inferredZone: zone || '' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/send-to-bosta-group/:groupCode', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    ensureBostaConfigured();
    const groupCode = String(req.params.groupCode || '').trim();
    if (!groupCode) return res.status(400).json({ error: 'كود الأوردر المجمع غير موجود' });

    const items = await allAsync(`SELECT * FROM orders WHERE group_code=? ORDER BY item_no ASC, id ASC`, [groupCode]);
    if (!items.length) return res.status(404).json({ error: 'الأوردر المجمع غير موجود' });
    if (items.some(item => String(normalizeOrderStatus(item.status || '')).trim() !== 'جاهز للشحن')) {
      return res.status(400).json({ error: 'كل أصناف الأوردر لازم تكون جاهزة للشحن أولاً' });
    }
    if (items.some(item => String(item.bosta_delivery_id || '').trim())) {
      return res.status(400).json({ error: 'تم إرسال هذا الأوردر إلى بوسطة بالفعل' });
    }

    const baseOrder = items[0];
    const customer = num(baseOrder.customer_id)
      ? await getAsync(`SELECT governorate, zone, email FROM customers WHERE id=?`, [num(baseOrder.customer_id)])
      : await findMatchingCustomerRecord({ name: baseOrder.custName, phone: baseOrder.custPhone });
    const customerGovernorate = String(customer?.governorate || '').trim();
    const customerZone = String(customer?.zone || '').trim();
    const customerEmail = String(customer?.email || '').trim();
    const groupSaleValue = await getGroupOrderBostaProductValue(items);
    const groupRemainingValue = roundMoney(items.reduce((sum, item) => sum + num(item.remaining_amount, 0), 0));
    const groupQty = Math.max(1, items.reduce((sum, item) => sum + Math.max(0, num(item.qty, 0)), 0));

    const body = req.body || {};
    const bostaCities = await getBostaCitiesCached();
    const selectedCityToken = String(body.city_code || baseOrder.bosta_city_code || customerGovernorate || '').trim();
    const selectedCityLabel = String(body.city_label || resolveConfiguredBostaGovernorateLabel(selectedCityToken, bostaCities) || customerGovernorate || '').trim();
    const selectedZoneToken = String(body.zone || baseOrder.bosta_zone || customerZone || '').trim();
    const secondLine = String(body.second_line || baseOrder.bosta_second_line || baseOrder.custAddress || '').trim();
    const receiverPhone = normalizePhone(body.receiver_phone || baseOrder.bosta_receiver_phone || baseOrder.custPhone || '');
    const receiverName = String(body.receiver_name || baseOrder.bosta_receiver_name || baseOrder.custName || '').trim();
    const productValue = Math.max(0, num(body.product_value, baseOrder.bosta_product_value || groupSaleValue || baseOrder.total_price || 0));
    const allowOpen = !!num(body.allow_open, baseOrder.bosta_allow_open || 0);
    const location = await resolveBostaShipmentLocation({
      address: secondLine || baseOrder.custAddress || '',
      cityToken: selectedCityToken || baseOrder.bosta_city_code || customerGovernorate || '',
      cityLabel: selectedCityLabel || customerGovernorate || '',
      zoneToken: selectedZoneToken || baseOrder.bosta_zone || customerZone || ''
    });
    const cityCode = String(location.cityCode || '').trim();
    const zone = String(location.zone || '').trim();
    const district = String(body.district || baseOrder.bosta_district || selectedZoneToken || customerZone || zone || '').trim();
    const resolvedBody = {
      ...body,
      city_code: cityCode,
      city_label: location.cityLabel || selectedCityLabel || customerGovernorate || '',
      zone,
      district,
      second_line: secondLine,
      receiver_phone: receiverPhone,
      receiver_name: receiverName,
      receiver_email: String(body.receiver_email || baseOrder.bosta_receiver_email || customerEmail || '').trim(),
      product_value: productValue,
      allow_open: allowOpen ? 1 : 0
    };

    if (!receiverName) return res.status(400).json({ error: 'اسم المستلم مطلوب' });
    if (!receiverPhone) return res.status(400).json({ error: 'رقم الهاتف مطلوب' });
    if (!secondLine) return res.status(400).json({ error: 'العنوان مطلوب' });
    if (!cityCode) {
      return res.status(400).json({ error: 'تعذر تحويل المحافظة المختارة إلى كود بوسطة صحيح. راجع المحافظة في بيانات العميل أو اخترها يدويًا من نافذة بوسطة.' });
    }
    if (!zone) {
      return res.status(400).json({ error: 'تعذر تحديد المنطقة الخاصة بالمحافظة المختارة. اختر المنطقة يدويًا من نافذة بوسطة.' });
    }

    const estimateInfo = await getBostaEstimateDetails({
      cityCode,
      cityLabel: resolvedBody.city_label || selectedCityLabel || cityCode,
      zone,
      packageType: String(resolvedBody.package_type || baseOrder.bosta_package_type || 'Parcel').trim() || 'Parcel',
      cod: Math.max(0, num(resolvedBody.cod, groupRemainingValue || 0)),
      productValue,
      allowOpen,
      itemsCount: Math.max(1, num(resolvedBody.items_count, groupQty || 1)),
      secondLine,
      description: String(resolvedBody.package_description || baseOrder.bosta_package_description || `أوردر مجمع ${groupCode}`).trim()
    });

    const payloadOrder = {
      ...baseOrder,
      id: num(baseOrder.id),
      qty: groupQty,
      total_price: groupSaleValue,
      remaining_amount: groupRemainingValue,
      bosta_business_reference: groupCode,
      bosta_package_description: String(resolvedBody.package_description || baseOrder.bosta_package_description || `أوردر مجمع ${groupCode}`).trim()
    };
    const payload = buildBostaPayload(payloadOrder, resolvedBody, req);
    const response = await fetch(`${BOSTA_BASE_URL}/deliveries`, {
      method: 'POST',
      headers: {
        Authorization: BOSTA_API_KEY,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const rawText = await response.text();
    let rawJson = {};
    try { rawJson = rawText ? JSON.parse(rawText) : {}; } catch (_) { rawJson = { message: rawText }; }
    if (!response.ok) {
      return res.status(response.status >= 500 ? 502 : 400).json({ error: rawJson?.message || rawJson?.error || rawText || 'فشل الإرسال إلى بوسطة' });
    }

    const info = extractBostaResponseInfo(rawJson);
    const sentAt = new Date().toISOString();
    const shippingStatus = 'تم الشحن';
    const businessReference = String(resolvedBody.business_reference || baseOrder.bosta_business_reference || groupCode || `GROUP-${baseOrder.id}`).trim();
    const packageType = String(resolvedBody.package_type || baseOrder.bosta_package_type || 'Parcel').trim() || 'Parcel';
    const packageDescription = String(resolvedBody.package_description || baseOrder.bosta_package_description || `أوردر مجمع ${groupCode}`).trim();
    const codAmount = Math.max(0, num(resolvedBody.cod, groupRemainingValue || 0));
    const orderNote = String(resolvedBody.notes || baseOrder.bosta_notes || '').trim();
    const productValueSaved = Math.max(0, num(resolvedBody.product_value, baseOrder.bosta_product_value || groupSaleValue || 0));
    const allowOpenSaved = !!num(resolvedBody.allow_open, baseOrder.bosta_allow_open || 0);

    const finalShippingFee = roundMoney(estimateInfo.shippingFee || info.shippingFee || 0);
    const finalRawShippingFee = roundMoney(estimateInfo.rawShippingFee || info.rawShippingFee || info.shippingFee || 0);
    const finalInsuranceFees = roundMoney(estimateInfo.insuranceFees || info.insuranceFees || 0);
    const finalOpenPackageFees = roundMoney(allowOpenSaved ? (estimateInfo.openPackageFees || info.openPackageFees || BOSTA_OPEN_PACKAGE_FEE) : 0);
    const finalExtraCodFee = roundMoney(estimateInfo.extraCodFee || info.extraCodFee || 0);
    const finalBostaMaterialFee = roundMoney(estimateInfo.bostaMaterialFee || info.bostaMaterialFee || 0);
    const finalExpediteFee = roundMoney(estimateInfo.expediteFee || info.expediteFee || 0);
    const finalVatRate = Number(estimateInfo.vatRate || info.vatRate || 0) || 0;
    const finalVatAmount = roundMoney(estimateInfo.vatAmount || info.vatAmount || 0);
    const finalPriceBeforeVat = roundMoney(estimateInfo.priceBeforeVat || info.priceBeforeVat || (finalShippingFee + finalInsuranceFees + finalOpenPackageFees + finalExtraCodFee + finalBostaMaterialFee + finalExpediteFee));
    const finalPriceAfterVat = roundMoney(estimateInfo.priceAfterVat || info.priceAfterVat || (finalPriceBeforeVat + finalVatAmount));
    const finalEstimatedFees = roundMoney(estimateInfo.estimatedFees || info.estimatedFees || (finalShippingFee + finalInsuranceFees + finalOpenPackageFees));
    const finalFeesText = String(info.feesText || estimateInfo.text || baseOrder.bosta_estimated_fees_text || '').trim();
    const finalEstimateSource = String(estimateInfo.source || (info.estimatedFees ? 'api' : 'local')).trim();
    const effectiveShippingCost = roundMoney(finalShippingFee || finalRawShippingFee || 0);

    for (let idx = 0; idx < items.length; idx += 1) {
      const item = items[idx];
      const isPrimaryItem = idx === 0;
      await runAsync(`UPDATE orders SET status=?, bosta_delivery_id=?, bosta_tracking_number=?, bosta_status=?, bosta_sent_at=?, bosta_city_code=?, bosta_zone=?, bosta_district=?, bosta_building_number=?, bosta_floor=?, bosta_apartment=?, bosta_second_line=?, bosta_receiver_name=?, bosta_receiver_phone=?, bosta_receiver_email=?, bosta_business_reference=?, bosta_package_type=?, bosta_package_description=?, bosta_product_value=?, bosta_allow_open=?, bosta_shipping_fee=?, bosta_raw_shipping_fee=?, bosta_open_package_fees=?, bosta_material_fee=?, bosta_extra_cod_fee=?, bosta_expedite_fee=?, bosta_vat_rate=?, bosta_vat_amount=?, bosta_price_before_vat=?, bosta_price_after_vat=?, bosta_estimate_source=?, bosta_estimated_fees=?, bosta_insurance_fees=?, bosta_estimated_fees_text=?, bosta_cod=?, bosta_notes=?, bosta_last_response=?, shipping_cost=? WHERE id=?`, [
        shippingStatus,
        info.deliveryId || '',
        info.trackingNumber || '',
        info.statusText || shippingStatus,
        sentAt,
        cityCode,
        zone,
        district,
        String(resolvedBody.building_number || item.bosta_building_number || '').trim(),
        String(resolvedBody.floor || item.bosta_floor || '').trim(),
        String(resolvedBody.apartment || item.bosta_apartment || '').trim(),
        secondLine,
        receiverName,
        receiverPhone,
        String(resolvedBody.receiver_email || item.bosta_receiver_email || '').trim(),
        businessReference,
        packageType,
        packageDescription,
        isPrimaryItem ? productValueSaved : 0,
        allowOpenSaved ? 1 : 0,
        isPrimaryItem ? finalShippingFee : 0,
        isPrimaryItem ? finalRawShippingFee : 0,
        isPrimaryItem ? finalOpenPackageFees : 0,
        isPrimaryItem ? finalBostaMaterialFee : 0,
        isPrimaryItem ? finalExtraCodFee : 0,
        isPrimaryItem ? finalExpediteFee : 0,
        isPrimaryItem ? finalVatRate : 0,
        isPrimaryItem ? finalVatAmount : 0,
        isPrimaryItem ? finalPriceBeforeVat : 0,
        isPrimaryItem ? finalPriceAfterVat : 0,
        finalEstimateSource,
        isPrimaryItem ? finalEstimatedFees : 0,
        isPrimaryItem ? finalInsuranceFees : 0,
        finalFeesText,
        isPrimaryItem ? codAmount : 0,
        orderNote,
        JSON.stringify(info.rawBody || estimateInfo.rawBody || {}),
        isPrimaryItem ? effectiveShippingCost : 0,
        num(item.id)
      ]);
      await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [num(item.id), item.status || 'جاهز للشحن', shippingStatus, req.user.full_name || req.user.username, `إرسال إلى بوسطة | أوردر مجمع ${groupCode}${info.trackingNumber ? ` | رقم التتبع ${info.trackingNumber}` : ''}`]);
    }

    await recordAudit({ req, action: 'send-to-bosta', entity_type: 'order', entity_id: num(baseOrder.id), details: `إرسال أوردر مجمع ${groupCode} إلى بوسطة | ${receiverName} | ${cityCode}/${zone}${allowOpenSaved ? ' | فتح مسموح' : ' | فتح غير مسموح'}${productValueSaved ? ` | قيمة المنتج ${productValueSaved}` : ''}${finalEstimatedFees ? ` | مستحقات متوقعة ${finalEstimatedFees}` : ''}${info.trackingNumber ? ` | تتبع ${info.trackingNumber}` : ''}` });
    res.json({ success: true, deliveryId: info.deliveryId || '', trackingNumber: info.trackingNumber || '', status: shippingStatus, shippingFee: finalShippingFee, rawShippingFee: finalRawShippingFee, openPackageFees: finalOpenPackageFees, materialFee: finalBostaMaterialFee, extraCodFee: finalExtraCodFee, expediteFee: finalExpediteFee, vatRate: finalVatRate, vatAmount: finalVatAmount, priceBeforeVat: finalPriceBeforeVat, priceAfterVat: finalPriceAfterVat, estimateSource: finalEstimateSource, estimatedFees: finalEstimatedFees, insuranceFees: finalInsuranceFees, feesText: finalFeesText, raw: info.rawBody || estimateInfo.rawBody || {}, inferredCity: resolvedBody.city_label || '', inferredZone: zone || '', groupCode, itemIds: items.map(item => num(item.id)) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post(BOSTA_WEBHOOK_PATH, async (req, res) => {
  try {
    const providedAuth = String(req.headers.authorization || req.headers.Authorization || '').trim();
    const providedSig = String(req.query?.bosta_sig || req.query?.sig || '').trim();
    const validHeader = !!BOSTA_WEBHOOK_AUTH && providedAuth === BOSTA_WEBHOOK_AUTH;
    const validSig = providedSig === BOSTA_WEBHOOK_TOKEN;
    if (!validHeader && !validSig) {
      return res.status(401).json({ error: 'unauthorized webhook' });
    }

    const payload = req.body || {};
    const trackingNumber = String(payload.trackingNumber || payload.tracking_number || '').trim();
    const deliveryId = String(payload._id || payload.id || payload.deliveryId || '').trim();
    const businessReference = String(payload.businessReference || payload.business_reference || '').trim();
    const stateCode = Number(payload.state);
    const stateLabel = bostaStateLabel(stateCode);
    const targetStatus = bostaWebhookTargetStatus(stateCode);

    let orders = [];
    if (deliveryId) orders = await allAsync(`SELECT * FROM orders WHERE bosta_delivery_id=? ORDER BY item_no ASC, id ASC`, [deliveryId]);
    if (!orders.length && trackingNumber) orders = await allAsync(`SELECT * FROM orders WHERE bosta_tracking_number=? ORDER BY item_no ASC, id ASC`, [trackingNumber]);
    if (!orders.length && businessReference) {
      orders = await allAsync(`SELECT * FROM orders WHERE bosta_business_reference=? ORDER BY item_no ASC, id ASC`, [businessReference]);
      if (!orders.length) {
        const m = businessReference.match(/ORDER-(\d+)/i);
        if (m) {
          const one = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(m[1])]);
          if (one) orders = [one];
        }
      }
    }

    if (!orders.length) {
      return res.json({ success: true, ignored: true, reason: 'order_not_found', trackingNumber, deliveryId, businessReference });
    }

    const noteBits = [
      stateLabel ? `بوسطة: ${stateLabel}` : '',
      trackingNumber ? `تتبع ${trackingNumber}` : '',
      businessReference ? `مرجع ${businessReference}` : '',
      payload.exceptionReason ? `سبب ${String(payload.exceptionReason).trim()}` : '',
      Number.isFinite(Number(payload.numberOfAttempts)) ? `محاولات ${Number(payload.numberOfAttempts)}` : ''
    ].filter(Boolean);
    const webhookNote = noteBits.join(' | ');

    const refreshedOrders = [];
    for (const order of orders) {
      const fromStatus = String(order.status || '').trim();
      const nextStatus = targetStatus || fromStatus;
      await runAsync(`UPDATE orders SET status=?, bosta_status=?, bosta_tracking_number=COALESCE(NULLIF(?,''), bosta_tracking_number), bosta_delivery_id=COALESCE(NULLIF(?,''), bosta_delivery_id), bosta_last_response=? WHERE id=?`, [
        nextStatus,
        stateLabel || String(stateCode || '').trim(),
        trackingNumber,
        deliveryId,
        JSON.stringify(payload || {}),
        num(order.id)
      ]);
      const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(order.id)]);
      refreshedOrders.push(refreshed);
      if (targetStatus && targetStatus !== fromStatus) {
        await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [num(order.id), fromStatus || '', targetStatus, 'Bosta Webhook', webhookNote || `تحديث من بوسطة للحالة ${targetStatus}`]);
      } else if (webhookNote) {
        await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [num(order.id), fromStatus || '', nextStatus || fromStatus || '', 'Bosta Webhook', webhookNote]);
      }
      await syncSaleForOrder(refreshed, 'Bosta Webhook');
      await recordAudit({ req, user: { username: 'bosta-webhook', full_name: 'Bosta Webhook' }, action: 'bosta-webhook', entity_type: 'order', entity_id: num(order.id), details: webhookNote || `Webhook state ${stateCode}` });
    }

    const firstOrder = refreshedOrders[0] || orders[0] || null;
    res.json({ success: true, orderIds: refreshedOrders.map(row => num(row?.id)), status: firstOrder?.status || targetStatus || '', bostaStatus: stateLabel || String(stateCode || '').trim() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/order-status-history/:id', authRequired, requirePerm('perm_view_orders'), async (req, res) => { try { res.json(await allAsync(`SELECT * FROM order_status_history WHERE order_id=? ORDER BY id DESC`, [num(req.params.id)])); } catch (e) { res.status(500).json({ error: e.message }); } });
app.delete('/delete-order/:id', authRequired, requirePerm('perm_delete_order'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [id]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });

    const shouldDeleteReturnedBagStock = num(req.body?.deleteReturnedBagStock) === 1;
    const canAskDeleteReturnedBagStock = num(order.useReadyStock) !== 1 && String(order.status || '').trim() === 'مرتجع' && num(order.bag_returned_to_stock) === 1;
    const bagCouldChange = (num(order.useReadyStock) === 1 && !num(order.bag_returned_to_stock)) || (canAskDeleteReturnedBagStock && shouldDeleteReturnedBagStock);
    const bagCriteria = [
      { column:'length', op:'eq', value:num(order.l) },
      { column:'width', op:'eq', value:num(order.w) },
      { column:'gusset', op:'eq', value:num(order.g) },
      { column:'color', op:'eq', value:order.color || '' },
      { column:'handle', op:'eq', value:order.handle || '' }
    ];
    const handleCouldChange = order.handle === 'بيد' && num(order.handle_stock_deducted);
    const undoPayload = [
      ...(await buildUndoPayloadFromDefs(orderSnapshotDefs([id]))),
      ...(num(order.paper_cut_done) && num(order.last_cut_paper_id) ? [
        await snapshotTableSubset('paper', criteriaEq('id', num(order.last_cut_paper_id))),
        await snapshotTableSubset('paper_history', criteriaEq('paper_id', num(order.last_cut_paper_id)))
      ].filter(Boolean) : []),
      ...(bagCouldChange ? [
        await snapshotTableSubset('bags', bagCriteria),
        await snapshotTableSubset('bags_history', criteriaEq('order_id', id))
      ].filter(Boolean) : []),
      ...(handleCouldChange ? [
        await snapshotTableSubset('handles', criteriaEq('color', order.color || '')),
        await snapshotTableSubset('handles_history', criteriaEq('order_id', id))
      ].filter(Boolean) : [])
    ];

    await runAsync('BEGIN TRANSACTION');
    try {
      if (num(order.useReadyStock) === 1 && !num(order.bag_returned_to_stock)) {
        const printType = String(order.printType || 'سادة').trim() || 'سادة';
        await addBagStock(order, `مرتجع للمخزن بسبب حذف الأوردر رقم ${order.id} (${printType})`);
      }
      if (canAskDeleteReturnedBagStock && shouldDeleteReturnedBagStock) {
        await subtractBagStock(order, `تم خصم الكمية بسبب حذف أوردر رقم ${order.id}`);
      }
      if (num(order.paper_cut_done) && num(order.last_cut_paper_id)) {
        const paper = await getAsync(`SELECT * FROM paper WHERE id=?`, [num(order.last_cut_paper_id)]);
        if (paper) {
          let sheets = 0, kg = 0;
          const mode = String(order.last_cut_layout || 'pieceByPiece');
          const chosen = computeLayout(order, paper, mode);
          if (chosen) {
            sheets = num(chosen.neededSheets);
            kg = ((num(paper.length) * num(paper.width) * num(paper.grammage) * sheets) / 10000000);
          }
          if (kg > 0 || sheets > 0) {
            await runAsync(`UPDATE paper SET total_kg=total_kg+?, total_sheets=total_sheets+? WHERE id=?`, [kg, sheets, paper.id]);
            await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [paper.id, 'add', kg, sheets, paper.color, paper.length, paper.width, paper.grammage, paper.paper_name || '', `استرجاع ورق خام بسبب حذف الأوردر #${order.id}`]);
          }
        }
      }
      if (handleCouldChange) {
        const h = await getAsync(`SELECT * FROM handles WHERE color=?`, [order.color || '']);
        let handleId = h?.id;
        if (h) {
          await runAsync(`UPDATE handles SET qty=qty+? WHERE id=?`, [num(order.qty), h.id]);
        } else {
          const ins = await runAsync(`INSERT INTO handles (color,qty,buy_price,min_qty) VALUES (?,?,?,?)`, [order.color || '', num(order.qty), 0, 0]);
          handleId = ins.lastID;
        }
        await runAsync(`INSERT INTO handles_history (handle_id,order_id,type,qty,color,note) VALUES (?,?,?,?,?,?)`, [handleId, order.id, 'add', num(order.qty), order.color || '', `استرجاع يد بسبب حذف الأوردر #${order.id}`]);
      }
      await runAsync(`DELETE FROM order_files WHERE order_id=?`, [id]);
      await runAsync(`DELETE FROM order_status_history WHERE order_id=?`, [id]);
      await runAsync(`DELETE FROM cost_history WHERE order_id=?`, [id]);
      await runAsync(`DELETE FROM sales_history WHERE order_id=?`, [id]);
      await runAsync(`DELETE FROM order_payments WHERE order_id=?`, [id]);
      await runAsync(`DELETE FROM partner_payments WHERE order_id=?`, [id]);
      await runAsync(`DELETE FROM order_operations WHERE order_id=?`, [id]);
      await runAsync(`DELETE FROM orders WHERE id=?`, [id]);
      await runAsync('COMMIT');
    } catch (err) {
      await runAsync('ROLLBACK');
      throw err;
    }

    const bagAfter = bagCouldChange ? await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(order.l), num(order.w), num(order.g), order.color || '', order.handle || '']) : null;
    const handleAfter = handleCouldChange ? await getAsync(`SELECT * FROM handles WHERE color=?`, [order.color || '']) : null;
    await recordAudit({
      req,
      action: 'delete-order',
      entity_type: 'order',
      entity_id: id,
      details: `حذف أوردر للعميل ${order.custName || ''}`,
      can_undo: 1,
      undo_type: 'delete-order',
      undo_payload: undoPayload,
      touch_refs: [
        ...orderTouchRefs([id]),
        ...(num(order.last_cut_paper_id) ? paperTouchRefs([num(order.last_cut_paper_id)]) : []),
        ...(bagAfter ? bagTouchRefs([bagAfter.id]) : []),
        ...(handleAfter ? handleTouchRefs([handleAfter.id]) : [])
      ]
    });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/customers', authRequired, requirePerm('perm_customers'), async (req, res) => {
  try {
    await syncAllCustomersFromOrders();
    const rows = await allAsync(`SELECT c.*, COUNT(o.id) AS ordersCount, COALESCE(SUM(CASE WHEN TRIM(COALESCE(o.status,''))='مرتجع' THEN 0 ELSE o.total_price END),0) AS totalSales, COALESCE(SUM(o.paid_amount),0) AS totalPaid, COALESCE(SUM(o.remaining_amount),0) AS totalRemaining, MAX(o.orderDate) AS lastOrderDate FROM customers c LEFT JOIN orders o ON o.customer_id=c.id WHERE c.is_active=1 GROUP BY c.id ORDER BY COALESCE(c.is_vip,0) DESC, COALESCE(MAX(o.id), c.id) DESC`);
    res.json(rows.map(r => ({ ...r, custName: r.name, custPhone: r.phone, custAddress: r.address })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/customers-list', authRequired, async (req, res) => {
  try {
    if (!(hasPerm(req.user, 'perm_customers') || hasPerm(req.user, 'perm_add_order') || hasPerm(req.user, 'perm_view_orders'))) return res.status(403).json({ error: 'غير مصرح' });
    await syncAllCustomersFromOrders();
    const rows = await allAsync(`SELECT id,name,phone,address,governorate,zone,email,notes,is_vip FROM customers WHERE is_active=1 AND TRIM(COALESCE(name,''))!='' ORDER BY COALESCE(is_vip,0) DESC, id DESC`);
    res.json(rows.map(r => ({ id:r.id, custName:r.name, custPhone:r.phone, custAddress:r.address, governorate:r.governorate, zone:r.zone, email:r.email, notes:r.notes, is_vip:num(r.is_vip) })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-customer', authRequired, requirePerm('perm_customers'), async (req, res) => {
  try {
    const b = req.body || {};
    let current = null;
    if (b.id) current = await getAsync(`SELECT * FROM customers WHERE id=?`, [num(b.id)]);
    const values = {
      name: String(b.name || current?.name || '').trim(),
      phone: String(b.phone || current?.phone || '').trim(),
      address: String(b.address || current?.address || '').trim(),
      governorate: String(b.governorate || current?.governorate || '').trim(),
      zone: String(b.zone || current?.zone || '').trim(),
      email: String(b.email || current?.email || '').trim(),
      notes: String(b.notes || current?.notes || '').trim(),
      is_active: num(b.is_active, current ? num(current.is_active,1) : 1),
      is_vip: num(b.is_vip, current ? num(current.is_vip,0) : 0),
      phone_normalized: normalizePhone(String(b.phone || current?.phone || '').trim()),
      updated_at: today()
    };
    if (!values.name) return res.status(400).json({ error: 'اسم العميل مطلوب' });
    if (current) {
      await runAsync(`UPDATE customers SET name=?,phone=?,phone_normalized=?,address=?,governorate=?,zone=?,email=?,notes=?,is_active=?,is_vip=?,updated_at=? WHERE id=?`, [values.name, values.phone, values.phone_normalized, values.address, values.governorate, values.zone, values.email, values.notes, values.is_active, values.is_vip, values.updated_at, current.id]);
      await runAsync(`UPDATE orders SET custName=?,custPhone=?,custAddress=? WHERE customer_id=?`, [values.name, values.phone, values.address, current.id]);
      await recordAudit({ req, action: 'update-customer', entity_type: 'customer', entity_id: current.id, details: `تعديل العميل ${values.name}` });
      return res.json({ success: true, id: current.id });
    }
    const ins = await runAsync(`INSERT INTO customers (name,phone,phone_normalized,address,governorate,zone,email,notes,is_active,is_vip,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [values.name, values.phone, values.phone_normalized, values.address, values.governorate, values.zone, values.email, values.notes, values.is_active, values.is_vip, values.updated_at]);
    await recordAudit({ req, action: 'create-customer', entity_type: 'customer', entity_id: ins.lastID, details: `إضافة عميل ${values.name}` });
    res.json({ success: true, id: ins.lastID });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/customer-statement/:id', authRequired, requirePerm('perm_customers'), async (req, res) => {
  try {
    const customer = await getAsync(`SELECT * FROM customers WHERE id=?`, [num(req.params.id)]);
    if (!customer) return res.status(404).json({ error: 'العميل غير موجود' });
    const orders = await allAsync(`SELECT * FROM orders WHERE customer_id=? ORDER BY id DESC`, [customer.id]);
    const payments = await allAsync(`SELECT p.*, o.total_price, o.status FROM order_payments p LEFT JOIN orders o ON o.id=p.order_id WHERE p.customer_id=? ORDER BY p.payment_date DESC, p.id DESC`, [customer.id]);
    const summary = await getAsync(`SELECT COALESCE(SUM(total_price),0) totalSales, COALESCE(SUM(paid_amount),0) totalPaid, COALESCE(SUM(remaining_amount),0) totalRemaining FROM orders WHERE customer_id=?`, [customer.id]);
    res.json({ customer, summary, orders, payments });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/cuttable-orders', authRequired, requirePerm('perm_cut_paper'), async (req, res) => {
  try {
    if (!canManageInventory(req.user)) return res.status(403).json({ error: 'غير مصرح' });
    const rows = await allAsync(`SELECT id,custName,status,orderDate,l,w,g,qty,color,handle,printType,useReadyStock,paper_cut_done FROM orders WHERE TRIM(COALESCE(status,'')) IN ('أوردر جديد','تحت الإنتاج') AND COALESCE(useReadyStock,0)=0 ORDER BY id DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/order-paper-plan/:id', authRequired, requirePerm('perm_calculator'), async (req, res) => {
  try {
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(req.params.id)]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const papers = await allAsync(`SELECT * FROM paper WHERE total_sheets > 0 ORDER BY id DESC`);
    let options = [];
    for (const p of papers) {
      const a = computeLayout(order, p, 'pieceByPiece');
      const b = computeLayout(order, p, 'singlePiece');
      if (a) options.push(a);
      if (b) options.push(b);
    }
    const matching = options.filter(o => o.colorMatch);
    const displayOptions = (matching.length ? matching : options).sort((x, y) => {
      const xe = x.availableSheets >= x.neededSheets, ye = y.availableSheets >= y.neededSheets;
      if (xe !== ye) return xe ? -1 : 1;
      if (x.wastePercent !== y.wastePercent) return x.wastePercent - y.wastePercent;
      return x.neededSheets - y.neededSheets;
    });

    let actualCut = null;
    if (num(order.paper_cut_done) && num(order.last_cut_paper_id)) {
      const actualPaper = await getAsync(`SELECT * FROM paper WHERE id=?`, [num(order.last_cut_paper_id)]);
      const layoutKey = String(order.last_cut_layout || 'pieceByPiece').trim() || 'pieceByPiece';
      if (actualPaper) {
        actualCut = computeLayout(order, actualPaper, layoutKey);
        if (actualCut) {
          actualCut = { ...actualCut, isActualCut: true, paperLabel: paperLabelBase(actualPaper), paperLabelFull: paperLabelFull(actualPaper) };
        } else {
          actualCut = {
            isActualCut: true,
            paperId: actualPaper.id,
            paperColor: actualPaper.color,
            paperLabel: paperLabelBase(actualPaper),
            paperLabelFull: paperLabelFull(actualPaper),
            layoutKey,
            layoutLabel: layoutKey === 'singlePiece' ? 'حته واحدة' : 'حته ف حته',
            colorMatch: true,
            neededSheets: 0,
            neededKg: 0,
            availableSheets: num(actualPaper.total_sheets),
            availableKg: num(actualPaper.total_kg),
            enoughStock: true,
            wastePercent: 0
          };
        }
      } else if (String(order.last_cut_paper_label || '').trim()) {
        actualCut = {
          isActualCut: true,
          paperId: num(order.last_cut_paper_id),
          paperColor: '',
          paperLabel: String(order.last_cut_paper_label || '').trim(),
          paperLabelFull: String(order.last_cut_paper_label || '').trim(),
          layoutKey,
          layoutLabel: layoutKey === 'singlePiece' ? 'حته واحدة' : 'حته ف حته',
          colorMatch: true,
          neededSheets: 0,
          neededKg: 0,
          availableSheets: 0,
          availableKg: 0,
          enoughStock: false,
          wastePercent: 0,
          missingPaper: true
        };
      }
    }

    res.json({
      order,
      layouts: {
        pieceByPiece: (() => { const d = getOrderCutDimensions(order, 'pieceByPiece'); return { cutWidth: d.cutWidth, cutLength: d.cutLength, piecesNeededPerBag: d.piecesNeededPerBag }; })(),
        singlePiece: (() => { const d = getOrderCutDimensions(order, 'singlePiece'); return { cutWidth: d.cutWidth, cutLength: d.cutLength, piecesNeededPerBag: d.piecesNeededPerBag }; })()
      },
      options: displayOptions,
      bestOption: displayOptions[0] || null,
      actualCut
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/production-plan', authRequired, requirePerm('perm_manage_bags'), async (req, res) => {
  try {
    const b = req.body || {};
    const safeQty = Math.max(1, num(b.qty));
    if (num(b.l) <= 0 || num(b.w) <= 0 || num(b.g) < 0) return res.status(400).json({ error: 'حدد مقاس الشنطة بشكل صحيح' });
    if (!String(b.color || '').trim()) return res.status(400).json({ error: 'حدد لون الشنطة' });
    const options = await getProductionPlanOptions({ l:b.l, w:b.w, g:b.g, qty:safeQty, color:b.color, handle:b.handle, paperId:b.paper_id });
    res.json({ options, bestOption: options[0] || null });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/create-production-order', authRequired, requirePerm('perm_manage_bags'), async (req, res) => {
  try {
    const b = req.body || {};
    const qty = Math.max(1, num(b.qty));
    const l = num(b.l), w = num(b.w), g = num(b.g);
    const color = String(b.color || '').trim();
    const handle = String(b.handle || '').trim();
    const paperId = num(b.paper_id);
    const layoutKey = String(b.layoutKey || '').trim();
    const pricingMode = String(b.pricing_mode || '').trim() === 'keep_existing' ? 'keep_existing' : 'auto_from_margin';
    const profitMargin = Math.max(0, num(b.profit_margin));
    if (l <= 0 || w <= 0 || g < 0) return res.status(400).json({ error: 'حدد المقاس بالكامل' });
    if (!color) return res.status(400).json({ error: 'حدد لون الشنطة' });
    if (!paperId) return res.status(400).json({ error: 'اختر الورق المستخدم' });
    if (!['pieceByPiece', 'singlePiece'].includes(layoutKey)) return res.status(400).json({ error: 'اختر نوع القصة' });

    const paper = await getAsync(`SELECT * FROM paper WHERE id=?`, [paperId]);
    if (!paper) return res.status(404).json({ error: 'الورق المطلوب غير موجود' });
    const existingBag = await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [l, w, g, color, handle]);

    const options = await getProductionPlanOptions({ l, w, g, qty, color, handle, paperId });
    const selected = options.find(o => o.layoutKey === layoutKey);
    if (!selected) return res.status(400).json({ error: 'القصة المختارة لا تناسب هذا المقاس' });
    if (!selected.enoughStock) return res.status(400).json({ error: 'مخزن الورق غير كافٍ لتنفيذ أمر التشغيل' });

    let finalBuyPrice = selected.unit_cost;
    let finalSellPrice = +(num(selected.unit_cost) * (1 + (profitMargin / 100))).toFixed(4);
    const usingExistingPrices = pricingMode === 'keep_existing' && existingBag;
    if (usingExistingPrices) {
      finalBuyPrice = num(existingBag.buy_price);
      finalSellPrice = num(existingBag.sell_price);
    }

    await runAsync('BEGIN TRANSACTION');
    try {
      await runAsync(`UPDATE paper SET total_kg=total_kg-?, total_sheets=total_sheets-? WHERE id=?`, [selected.neededKg, selected.neededSheets, paperId]);
      const note = `أمر تشغيل مفتوح | الكمية ${qty} | القصة ${selected.layoutLabel} | الورق ${selected.paperLabelFull} | تكلفة الوحدة ${selected.unit_cost} ج | إجمالي التكلفة ${selected.total_cost} ج | ${usingExistingPrices ? 'بنفس أسعار الصنف الحالي' : `تسعير تلقائي بهامش ربح ${profitMargin}%`}`;
      await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [paperId, 'sub', -selected.neededKg, -selected.neededSheets, paper.color, paper.length, paper.width, paper.grammage, paper.paper_name || '', note]);
      const jobId = await createPendingProductionJob({
        sourceType: 'ready_bags',
        sourceRefId: 0,
        paper,
        paperId,
        bagId: num(existingBag?.id),
        qty,
        color,
        handle,
        l, w, g,
        layoutKey: selected.layoutKey,
        layoutLabel: selected.layoutLabel,
        paperLabel: selected.paperLabelFull,
        paperKg: selected.neededKg,
        paperSheets: selected.neededSheets,
        costPaper: selected.cost_paper,
        costCut: selected.cost_cut,
        costMake: selected.cost_make,
        costHand: selected.cost_hand,
        costHandFix: selected.cost_hand_fix,
        totalCost: selected.total_cost,
        unitCost: selected.unit_cost,
        note,
        createdBy: req.user.full_name || req.user.username,
        pricingMode: usingExistingPrices ? 'keep_existing' : 'auto_from_margin',
        profitMargin: usingExistingPrices ? 0 : profitMargin,
        bagBuyPrice: finalBuyPrice,
        bagSellPrice: finalSellPrice,
        reserveHandleId: 0
      });
      await runAsync('COMMIT');
      await recordAudit({ req, action: 'create-production-order', entity_type: 'production_order', entity_id: jobId, details: `إنشاء أمر تشغيل مفتوح للمقاس ${l}×${w}×${g} | ${color} | كمية ${qty}` });
      return res.json({
        success: true,
        id: jobId,
        qty,
        bag_buy_price: finalBuyPrice,
        bag_sell_price: finalSellPrice,
        pricing_mode: usingExistingPrices ? 'keep_existing' : 'auto_from_margin',
        profit_margin: usingExistingPrices ? 0 : profitMargin,
        plan: selected
      });
    } catch (err) {
      await runAsync('ROLLBACK');
      throw err;
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Bags

app.get('/ready-bags-summary', authRequired, requirePerm('perm_view_bags'), async (req, res) => {
  try {
    res.json(await getReadyBagSizeRows());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/get-bags', authRequired, requirePerm('perm_view_bags'), async (req, res) => { try { res.json(await allAsync(`SELECT * FROM bags ORDER BY id DESC`)); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/set-bags-min-all', authRequired, requirePerm('perm_manage_bags'), async (req, res) => { try { if (!canManageBags(req.user)) return res.status(403).json({ error: 'غير مصرح' }); const minQty = Math.max(0, num(req.body?.min_qty)); await runAsync(`UPDATE bags SET min_qty=?`, [minQty]); await recordAudit({ req, action: 'set-bags-min-all', entity_type: 'bags', details: `تعيين حد أدنى عام للشنط = ${minQty}` }); res.json({ success: true, min_qty: minQty }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/add-bag', authRequired, requirePerm('perm_manage_bags'), async (req, res) => {
  try {
    if (!canManageBags(req.user)) return res.status(403).json({ error: 'غير مصرح' });
    const b = req.body, editId = num(b.edit_id);
    if (editId) {
      const old = await getAsync(`SELECT * FROM bags WHERE id=?`, [editId]);
      const undoPayload = await buildUndoPayloadFromDefs([
        { table:'bags', criteria: criteriaEq('id', editId) },
        { table:'bags_history', criteria: criteriaEq('bag_id', editId) }
      ]);
      await runAsync(`UPDATE bags SET length=?,width=?,gusset=?,color=?,handle=?,total_qty=?,min_qty=?,buy_price=?,sell_price=? WHERE id=?`, [num(b.l), num(b.w), num(b.g), b.color || '', b.handle || '', num(b.qty), num(b.min_qty), num(b.buy_price), num(b.sell_price), editId]);
      const diffNote = describeDiff(old, { length: num(b.l), width: num(b.w), gusset: num(b.g), color: b.color || '', handle: b.handle || '', total_qty: num(b.qty), min_qty: num(b.min_qty), buy_price: num(b.buy_price), sell_price: num(b.sell_price) });
      await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [editId, null, 'edit', 0, b.color || '', b.handle || '', num(b.l), num(b.w), num(b.g), diffNote]);
      await recordAudit({ req, action: 'update-bag', entity_type: 'bag', entity_id: editId, details: `تعديل صنف شنط ${num(b.l)}×${num(b.w)}×${num(b.g)} | ${b.color || ''} | ${b.handle || ''} | ${diffNote || 'بدون تغييرات'}`, can_undo: 1, undo_type: 'update-bag', undo_payload: undoPayload, touch_refs: bagTouchRefs([editId]) });
      return res.json({ success: true, updated: true });
    }
    const row = await getAsync(`SELECT * FROM bags WHERE length=? AND width=? AND gusset=? AND color=? AND handle=?`, [num(b.l), num(b.w), num(b.g), b.color || '', b.handle || '']);
    const qty = num(b.qty); let bagId;
    let undoPayload = row ? await buildUndoPayloadFromDefs([
      { table:'bags', criteria: criteriaEq('id', row.id) },
      { table:'bags_history', criteria: criteriaEq('bag_id', row.id) }
    ]) : [];
    if (row) {
      bagId = row.id;
      await runAsync(`UPDATE bags SET total_qty=total_qty+?,min_qty=?,buy_price=?,sell_price=? WHERE id=?`, [qty, b.min_qty === undefined || b.min_qty === '' ? num(row.min_qty) : num(b.min_qty), b.buy_price === undefined || b.buy_price === '' ? num(row.buy_price) : num(b.buy_price), b.sell_price === undefined || b.sell_price === '' ? num(row.sell_price) : num(b.sell_price), row.id]);
    } else {
      const r = await runAsync(`INSERT INTO bags (length,width,gusset,color,handle,total_qty,min_qty,buy_price,sell_price) VALUES (?,?,?,?,?,?,?,?,?)`, [num(b.l), num(b.w), num(b.g), b.color || '', b.handle || '', qty, num(b.min_qty), num(b.buy_price), num(b.sell_price)]);
      bagId = r.lastID;
      undoPayload = emptyUndoPayloadFromDefs([
        { table:'bags', criteria: criteriaEq('id', bagId) },
        { table:'bags_history', criteria: criteriaEq('bag_id', bagId) }
      ]);
    }
    const bag = await getAsync(`SELECT * FROM bags WHERE id=?`, [bagId]);
    if (qty !== 0) await runAsync(`INSERT INTO bags_history (bag_id,order_id,type,qty,color,handle,length,width,gusset,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [bagId, null, qty >= 0 ? 'add' : 'sub', Math.abs(qty), bag.color, bag.handle, bag.length, bag.width, bag.gusset, 'حركة يدوية']);
    await recordAudit({ req, action: row ? 'add-bag-stock' : 'create-bag', entity_type: 'bag', entity_id: bagId, details: `${row ? 'زيادة رصيد' : 'إضافة صنف'} شنط ${bag.length}×${bag.width}×${bag.gusset} | ${bag.color} | ${bag.handle} | كمية ${qty}`, can_undo: 1, undo_type: row ? 'add-bag-stock' : 'create-bag', undo_payload: undoPayload, touch_refs: bagTouchRefs([bagId]) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-bag/:id', authRequired, requirePerm('perm_manage_bags'), async (req, res) => { try { if (!canManageBags(req.user)) return res.status(403).json({ error: 'غير مصرح' }); const bag = await getAsync(`SELECT * FROM bags WHERE id=?`, [num(req.params.id)]); if (!bag) return res.status(404).json({ error: 'الصنف غير موجود' }); const undoPayload = await buildUndoPayloadFromDefs([{ table:'bags', criteria: criteriaEq('id', num(req.params.id)) }, { table:'bags_history', criteria: criteriaEq('bag_id', num(req.params.id)) }]); await runAsync(`DELETE FROM bags WHERE id=?`, [num(req.params.id)]); await recordAudit({ req, action: 'delete-bag', entity_type: 'bag', entity_id: num(req.params.id), details: bag ? `حذف صنف شنط ${bag.length}×${bag.width}×${bag.gusset} | ${bag.color} | ${bag.handle}` : 'حذف صنف شنط', can_undo: 1, undo_type: 'delete-bag', undo_payload: undoPayload, touch_refs: bagTouchRefs([num(req.params.id)]) }); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.get('/get-bags-history', authRequired, requirePerm('perm_bags_history'), async (req, res) => { try { res.json(await allAsync(`SELECT * FROM bags_history ORDER BY id DESC LIMIT 500`)); } catch (e) { res.status(500).json({ error: e.message }); } });

// Handles
app.get('/get-handles', authRequired, requirePerm('perm_view_handles'), async (req, res) => { try { res.json(await allAsync(`SELECT * FROM handles ORDER BY id DESC`)); } catch (e) { res.status(500).json({ error: e.message }); } });
app.get('/get-handles-history', authRequired, requirePerm('perm_handles_history'), async (req, res) => { try { res.json(await allAsync(`SELECT * FROM handles_history ORDER BY id DESC LIMIT 500`)); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/add-handle', authRequired, requirePerm('perm_manage_handles'), async (req, res) => {
  try {
    if (!hasPerm(req.user, 'perm_manage_handles')) return res.status(403).json({ error: 'غير مصرح' });
    const color = String(req.body.color || '').trim(), qty = num(req.body.qty), buy = num(req.body.buy_price), minQty = num(req.body.min_qty), editId = num(req.body.edit_id);
    if (editId) {
      const old = await getAsync(`SELECT * FROM handles WHERE id=?`, [editId]);
      const undoPayload = await buildUndoPayloadFromDefs([
        { table:'handles', criteria: criteriaEq('id', editId) },
        { table:'handles_history', criteria: criteriaEq('handle_id', editId) }
      ]);
      await runAsync(`UPDATE handles SET color=?,qty=?,buy_price=?,min_qty=? WHERE id=?`, [color, qty, buy, minQty, editId]);
      const diffNote = describeDiff(old, { color, qty, buy_price: buy, min_qty: minQty });
      await runAsync(`INSERT INTO handles_history (handle_id,type,qty,color,note) VALUES (?,?,?,?,?)`, [editId, 'edit', 0, color, diffNote]);
      await recordAudit({ req, action: 'update-handle', entity_type: 'handle', entity_id: editId, details: `تعديل صنف يد ${color} | ${diffNote || 'بدون تغييرات'}`, can_undo: 1, undo_type: 'update-handle', undo_payload: undoPayload, touch_refs: handleTouchRefs([editId]) });
      return res.json({ success: true, updated: true });
    }
    const row = await getAsync(`SELECT * FROM handles WHERE color=?`, [color]);
    let handleId = row?.id || 0;
    let undoPayload = row ? await buildUndoPayloadFromDefs([
      { table:'handles', criteria: criteriaEq('id', row.id) },
      { table:'handles_history', criteria: criteriaEq('handle_id', row.id) }
    ]) : [];
    if (row) {
      await runAsync(`UPDATE handles SET qty=qty+?,buy_price=?,min_qty=? WHERE id=?`, [qty, buy || row.buy_price, minQty || row.min_qty, row.id]);
      if (qty) await runAsync(`INSERT INTO handles_history (handle_id,type,qty,color,note) VALUES (?,?,?,?,?)`, [row.id, qty>=0 ? 'add' : 'sub', Math.abs(qty), color, 'حركة يدوية']);
    } else {
      const ins = await runAsync(`INSERT INTO handles (color,qty,buy_price,min_qty) VALUES (?,?,?,?)`, [color, qty, buy, minQty]);
      handleId = ins.lastID;
      undoPayload = emptyUndoPayloadFromDefs([
        { table:'handles', criteria: criteriaEq('id', handleId) },
        { table:'handles_history', criteria: criteriaEq('handle_id', handleId) }
      ]);
      if (qty) await runAsync(`INSERT INTO handles_history (handle_id,type,qty,color,note) VALUES (?,?,?,?,?)`, [ins.lastID, qty>=0 ? 'add' : 'sub', Math.abs(qty), color, 'حركة يدوية']);
    }
    await recordAudit({ req, action: row ? 'add-handle-stock' : 'create-handle', entity_type: 'handle', entity_id: handleId, details: `${row ? 'زيادة رصيد' : 'إضافة صنف'} يد ${color} | كمية ${qty}`, can_undo: 1, undo_type: row ? 'add-handle-stock' : 'create-handle', undo_payload: undoPayload, touch_refs: handleTouchRefs([handleId]) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Paper
app.get('/get-paper', authRequired, requirePerm('perm_view_inventory'), async (req, res) => { try { const rows=await allAsync(`SELECT * FROM paper ORDER BY id DESC`); for (const r of rows) { const weight=((num(r.length)*num(r.width)*num(r.grammage))/10000000); r.buy_price_sheet = weight>0 ? num(r.buy_price_kg)*weight : num(r.buy_price_sheet); } res.json(rows); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/add-paper', authRequired, requirePerm('perm_manage_paper'), async (req, res) => {
  try {
    if (!canManageInventory(req.user)) return res.status(403).json({ error: 'غير مصرح' });
    const b = req.body, editId = num(b.edit_id);
    const paperName = String(b.paper_name || '').trim();
    const buyPriceKg = num(b.buy_price_kg) || ((num(b.total_price_input) && num(b.kg)) ? (num(b.total_price_input)/num(b.kg)) : 0);
    if (editId) {
      const old = await getAsync(`SELECT * FROM paper WHERE id=?`, [editId]);
      const undoPayload = await buildUndoPayloadFromDefs([
        { table:'paper', criteria: criteriaEq('id', editId) },
        { table:'paper_history', criteria: criteriaEq('paper_id', editId) }
      ]);
      await runAsync(`UPDATE paper SET length=?,width=?,grammage=?,color=?,paper_name=?,total_kg=?,total_sheets=?,min_kg=?,min_sheets=?,buy_price_kg=? WHERE id=?`, [num(b.length), num(b.width), num(b.grammage), b.color || '', paperName, num(b.kg), num(b.sheets), num(b.min_kg), num(b.min_sheets), buyPriceKg, editId]);
      await refreshPaperPriceSheet(editId);
      const editNote = describeDiff(old, { length: num(b.length), width: num(b.width), grammage: num(b.grammage), color: b.color || '', paper_name: paperName, total_kg: num(b.kg), total_sheets: num(b.sheets), min_kg: num(b.min_kg), min_sheets: num(b.min_sheets), buy_price_kg: buyPriceKg }) || 'تم حفظ الصنف بدون تغييرات';
      await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [editId, 'edit', 0, 0, b.color || '', num(b.length), num(b.width), num(b.grammage), paperName, editNote]);
      await recordAudit({ req, action: 'update-paper', entity_type: 'paper', entity_id: editId, details: `تعديل صنف ورق ${b.color || ''} ${num(b.length)}×${num(b.width)} - ${num(b.grammage)} جم${paperName ? ' - ' + paperName : ''} | ${editNote}`, can_undo: 1, undo_type: 'update-paper', undo_payload: undoPayload, touch_refs: paperTouchRefs([editId]) });
      return res.json({ success: true, updated: true });
    }
    const row = await getAsync(`SELECT * FROM paper WHERE length=? AND width=? AND grammage=? AND color=? AND TRIM(COALESCE(paper_name,''))=?`, [num(b.length), num(b.width), num(b.grammage), b.color || '', paperName]);
    const kg = num(b.kg), sheets = num(b.sheets);
    let paperId;
    let undoPayload = row ? await buildUndoPayloadFromDefs([
      { table:'paper', criteria: criteriaEq('id', row.id) },
      { table:'paper_history', criteria: criteriaEq('paper_id', row.id) }
    ]) : [];
    if (row) {
      await runAsync(`UPDATE paper SET total_kg=total_kg+?,total_sheets=total_sheets+?,min_kg=?,min_sheets=?,buy_price_kg=? WHERE id=?`, [kg, sheets, b.min_kg === undefined || b.min_kg === '' ? num(row.min_kg) : num(b.min_kg), b.min_sheets === undefined || b.min_sheets === '' ? num(row.min_sheets) : num(b.min_sheets), buyPriceKg || num(row.buy_price_kg), row.id]);
      paperId = row.id;
    } else {
      const r = await runAsync(`INSERT INTO paper (length,width,grammage,color,paper_name,total_kg,total_sheets,min_kg,min_sheets,buy_price_kg) VALUES (?,?,?,?,?,?,?,?,?,?)`, [num(b.length), num(b.width), num(b.grammage), b.color || '', paperName, kg, sheets, num(b.min_kg), num(b.min_sheets), buyPriceKg]);
      paperId = r.lastID;
      undoPayload = emptyUndoPayloadFromDefs([
        { table:'paper', criteria: criteriaEq('id', paperId) },
        { table:'paper_history', criteria: criteriaEq('paper_id', paperId) }
      ]);
    }
    await refreshPaperPriceSheet(paperId);
    const paper = await getAsync(`SELECT * FROM paper WHERE id=?`, [paperId]);
    if (kg !== 0 || sheets !== 0) await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [paperId, (kg < 0 || sheets < 0) ? 'sub' : 'add', kg, sheets, paper.color, paper.length, paper.width, paper.grammage, paper.paper_name || '', 'حركة يدوية']);
    await recordAudit({ req, action: row ? 'add-paper-stock' : 'create-paper', entity_type: 'paper', entity_id: paperId, details: `${row ? 'زيادة رصيد' : 'إضافة صنف'} ورق ${paper.color} ${paper.length}×${paper.width} - ${paper.grammage} جم${paper.paper_name ? ' - ' + paper.paper_name : ''} | ${kg.toFixed(2)} كجم | ${Math.round(sheets)} فرخ`, can_undo: 1, undo_type: row ? 'add-paper-stock' : 'create-paper', undo_payload: undoPayload, touch_refs: paperTouchRefs([paperId]) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/cut-paper', authRequired, requirePerm('perm_cut_paper'), async (req, res) => {
  try {
    if (!canManageInventory(req.user)) return res.status(403).json({ error: 'غير مصرح' });
    const body = req.body || {};
    const paperId = num(body.paper_id);
    const cutMode = String(body.cut_mode || 'current_order').trim() || 'current_order';
    let kg = num(body.kg), sheets = num(body.sheets);
    const layoutKey = String(body.layoutKey || '').trim();
    const paper = await getAsync(`SELECT * FROM paper WHERE id=?`, [paperId]);
    if (!paper) return res.status(404).json({ error: 'الورق غير موجود' });

    const baseUndoPayload = await buildUndoPayloadFromDefs([
      { table:'paper', criteria: criteriaEq('id', paperId) },
      { table:'paper_history', criteria: criteriaEq('paper_id', paperId) }
    ]);

    if (cutMode === 'ready_bags') {
      const itemsRaw = Array.isArray(body.ready_bag_items) ? body.ready_bag_items : [];
      const selectedLayout = String(body.layoutKey || 'pieceByPiece').trim() === 'singlePiece' ? 'singlePiece' : 'pieceByPiece';
      const items = itemsRaw
        .map((item, idx) => {
          const dims = readyBagCutDimensions(item, selectedLayout);
          return {
            index: idx + 1,
            l: num(item?.l),
            w: num(item?.w),
            g: num(item?.g),
            qty: Math.max(0, num(item?.qty)),
            handle: String(item?.handle || '').trim(),
            color: String(item?.color || '').trim(),
            cutWidth: dims.cutWidth,
            cutLength: dims.cutLength,
            piecesNeededPerBag: dims.piecesNeededPerBag,
            areaWeight: +(Math.max(0, dims.cutWidth * dims.cutLength * dims.piecesNeededPerBag * Math.max(0, num(item?.qty)))).toFixed(4)
          };
        })
        .filter(item => item.l > 0 && item.w > 0 && item.qty > 0);
      if (!items.length) return res.status(400).json({ error: 'أدخل مقاس واحد على الأقل لأمر التشغيل' });
      if (kg <= 0 && sheets <= 0) return res.status(400).json({ error: 'أدخل كمية القص كجم أو فرخ' });
      if (kg <= 0 && sheets > 0) kg = ((num(paper.length) * num(paper.width) * num(paper.grammage) * sheets) / 10000000);
      if (sheets <= 0 && kg > 0) sheets = Math.round((kg * 10000000) / (num(paper.length) * num(paper.width) * num(paper.grammage)));
      if (num(paper.total_kg) < kg || num(paper.total_sheets) < sheets) return res.status(400).json({ error: 'الكمية غير كافية' });

      const itemsText = items.map(item => `مقاس ${item.index}: ${item.l}×${item.w}${item.g > 0 ? `×${item.g}` : ''} | كمية ${item.qty}${item.color ? ` | لون ${item.color}` : ''}${item.handle ? ` | ${item.handle}` : ''}`).join(' || ');
      const note = `قص لأمر تشغيل شنط جاهزة | ${itemsText}${String(body.ready_bag_note || '').trim() ? ` | ملاحظة: ${String(body.ready_bag_note || '').trim()}` : ''}`;
      const totalAreaWeight = items.reduce((s, item) => s + num(item.areaWeight), 0) || items.length;

      await runAsync('BEGIN TRANSACTION');
      try {
        await runAsync(`UPDATE paper SET total_kg=total_kg-?, total_sheets=total_sheets-? WHERE id=?`, [kg, sheets, paperId]);
        await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [paperId, 'sub', -kg, -sheets, paper.color, paper.length, paper.width, paper.grammage, paper.paper_name || '', note]);

        const jobIds = [];
        for (const item of items) {
          const ratio = totalAreaWeight > 0 ? (num(item.areaWeight) / totalAreaWeight) : (1 / items.length);
          const allocatedKg = +(kg * ratio).toFixed(4);
          const allocatedSheets = +(sheets * ratio).toFixed(4);
          const estimatedCosts = calcProductionOrderCosts({ qty: item.qty, handle: item.handle, neededSheets: Math.max(1, Math.ceil(allocatedSheets || 0)), paper, handleBuyPrice: 0 });
          const unitPaperCost = allocatedKg * num(paper.buy_price_kg);
          const totalCost = +(unitPaperCost + num(estimatedCosts.cost_cut) + num(estimatedCosts.cost_make) + num(estimatedCosts.cost_hand_fix)).toFixed(2);
          const unitCost = item.qty > 0 ? +(totalCost / item.qty).toFixed(4) : 0;
          const jobId = await createPendingProductionJob({
            sourceType: 'paper_cut',
            sourceRefId: paperId,
            paper,
            paperId,
            qty: item.qty,
            color: item.color || String(paper.color || '').trim(),
            handle: item.handle,
            l: item.l,
            w: item.w,
            g: item.g,
            layoutKey: selectedLayout,
            layoutLabel: selectedLayout === 'singlePiece' ? 'حته واحدة' : 'حته ف حته',
            paperLabel: paperLabelFull(paper),
            paperKg: allocatedKg,
            paperSheets: allocatedSheets,
            costPaper: +(unitPaperCost).toFixed(2),
            costCut: num(estimatedCosts.cost_cut),
            costMake: num(estimatedCosts.cost_make),
            costHand: 0,
            costHandFix: num(estimatedCosts.cost_hand_fix),
            totalCost,
            unitCost,
            note: `أمر تشغيل مفتوح من قص الورق | ${note}`,
            createdBy: req.user.full_name || req.user.username,
            pricingMode: 'auto_from_margin',
            profitMargin: 0,
            bagBuyPrice: unitCost,
            bagSellPrice: 0,
            reserveHandleId: 0
          });
          jobIds.push(jobId);
        }

        await runAsync('COMMIT');
        await recordAudit({ req, action: 'cut-paper-ready-bags', entity_type: 'paper', entity_id: paperId, details: `${note} | ${Math.round(sheets)} فرخ | ${kg.toFixed(2)} كجم | أوامر تشغيل ${jobIds.length}`, can_undo: 1, undo_type: 'cut-paper', undo_payload: baseUndoPayload, touch_refs: paperTouchRefs([paperId]) });
        return res.json({ success: true, kg, sheets, cut_mode: 'ready_bags', note, production_jobs_count: jobIds.length, production_job_ids: jobIds });
      } catch (err) {
        await runAsync('ROLLBACK');
        throw err;
      }
    }

    const orderId = num(body.order_id);
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [orderId]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
    if (num(order.paper_cut_done)) return res.status(400).json({ error: 'تم تنفيذ القص بالفعل لهذا الأوردر' });
    const undoPayload = [
      ...(await buildUndoPayloadFromDefs(orderSnapshotDefs([orderId]))),
      ...baseUndoPayload
    ];

    if ((kg <= 0 && sheets <= 0) && layoutKey) {
      const chosen = computeLayout(order, paper, layoutKey);
      if (!chosen) return res.status(400).json({ error: 'الفرخ لا يقبل هذا القص' });
      sheets = num(chosen.neededSheets);
      kg = ((num(paper.length) * num(paper.width) * num(paper.grammage) * sheets) / 10000000);
    }

    if (kg <= 0 && sheets <= 0) return res.status(400).json({ error: 'أدخل كمية' });
    if (kg <= 0 && sheets > 0) kg = ((num(paper.length)*num(paper.width)*num(paper.grammage)*sheets)/10000000);
    if (sheets <= 0 && kg > 0) sheets = Math.round((kg*10000000)/(num(paper.length)*num(paper.width)*num(paper.grammage)));
    if (num(paper.total_kg) < kg || num(paper.total_sheets) < sheets) return res.status(400).json({ error: 'الكمية غير كافية' });

    await runAsync(`UPDATE paper SET total_kg=total_kg-?, total_sheets=total_sheets-? WHERE id=?`, [kg, sheets, paperId]);
    const note = `قص للأوردر #${orderId}${layoutKey ? ` - ${layoutKey==='pieceByPiece' ? 'حته ف حته' : 'حته واحدة'}` : ''}`;
    await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [paperId, 'sub', -kg, -sheets, paper.color, paper.length, paper.width, paper.grammage, paper.paper_name || '', note]);
    const cost = kg * num(paper.buy_price_kg);
    await runAsync(`UPDATE orders SET cost_paper=COALESCE(cost_paper,0)+?, paperGrammage=?, paper_cut_done=1, last_cut_layout=?, last_cut_paper_label=?, last_cut_paper_id=? WHERE id=?`, [cost, num(paper.grammage), layoutKey || 'pieceByPiece', paperLabelBase(paper), paperId, orderId]);
    await addCostLog({ order_id: orderId, cost_field:'cost_paper', amount:cost, source:'paper-cut', source_ref:String(paperId), notes: note, created_by:req.user.full_name || req.user.username });
    await syncOrderAutoPlateCost(orderId, req.user.full_name || req.user.username);
    const afterCut = await getAsync(`SELECT * FROM orders WHERE id=?`, [orderId]);
    if (afterCut && !['تم الشحن','تم التسليم','مرتجع'].includes(String(afterCut.status || '').trim()) && String(afterCut.status || '').trim() !== 'في القص') {
      await runAsync(`UPDATE orders SET status=? WHERE id=?`, ['في القص', orderId]);
      await runAsync(`INSERT INTO order_status_history (order_id,from_status,to_status,changed_by,note) VALUES (?,?,?,?,?)`, [orderId, afterCut.status || '', 'في القص', req.user.full_name || req.user.username, 'تغيير تلقائي للحالة بعد تنفيذ القص']);
    }
    await recordAudit({ req, action: 'cut-paper', entity_type: 'order', entity_id: orderId, details: `${note} | ${Math.round(sheets)} فرخ | ${kg.toFixed(2)} كجم`, can_undo: 1, undo_type: 'cut-paper', undo_payload: undoPayload, touch_refs: [...orderTouchRefs([orderId]), ...paperTouchRefs([paperId])] });
    res.json({ success: true, cost_paper: cost, kg, sheets, layoutKey, status: 'في القص', cut_mode: 'current_order' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/update-paper-min', authRequired, requirePerm('perm_manage_paper'), async (req, res) => { try { if (!canManageInventory(req.user)) return res.status(403).json({ error: 'غير مصرح' }); await runAsync(`UPDATE paper SET min_kg=?,min_sheets=? WHERE id=?`, [num(req.body.min_kg), num(req.body.min_sheets), num(req.body.id)]); await recordAudit({ req, action: 'update-paper-min', entity_type: 'paper', entity_id: num(req.body.id), details: `تعديل الحد الأدنى للورق إلى ${num(req.body.min_kg)} كجم / ${num(req.body.min_sheets)} فرخ` }); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.delete('/delete-paper/:id', authRequired, requirePerm('perm_manage_paper'), async (req, res) => {
  if (!canManageInventory(req.user)) return res.status(403).json({ error: 'غير مصرح' });
  try {
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM paper WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'الصنف غير موجود' });
    const undoPayload = await buildUndoPayloadFromDefs([
      { table:'paper', criteria: criteriaEq('id', id) },
      { table:'paper_history', criteria: criteriaEq('paper_id', id) }
    ]);
    await runAsync(`INSERT INTO paper_history (paper_id,type,kg,sheets,color,length,width,grammage,paper_name,note) VALUES (?,?,?,?,?,?,?,?,?,?)`, [id, 'delete', 0, 0, row.color, row.length, row.width, row.grammage, row.paper_name || '', `تم حذف الصنف وكان رصيده ${num(row.total_kg).toFixed(2)} كجم / ${Math.round(num(row.total_sheets))} فرخ`]);
    await runAsync(`DELETE FROM paper WHERE id=?`, [id]);
    await recordAudit({ req, action: 'delete-paper', entity_type: 'paper', entity_id: id, details: `حذف صنف ورق ${row.color} ${row.length}×${row.width} - ${row.grammage} جم${row.paper_name ? ' - ' + row.paper_name : ''}`, can_undo: 1, undo_type: 'delete-paper', undo_payload: undoPayload, touch_refs: paperTouchRefs([id]) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/get-paper-history', authRequired, requirePerm('perm_paper_history'), async (req, res) => { try { res.json(await allAsync(`SELECT * FROM paper_history ORDER BY id DESC LIMIT 500`)); } catch (e) { res.status(500).json({ error: e.message }); } });


app.get('/get-expenses', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    await syncAllSalesHistory();
    const from = String(req.query.from || '').trim();
    const to = String(req.query.to || '').trim();
    let sql = `SELECT e.*, o.custName FROM expenses e LEFT JOIN orders o ON o.id=e.order_id WHERE linked_to_order=0`;
    const params = [];
    if (from) { sql += ` AND expense_date>=?`; params.push(from); }
    if (to) { sql += ` AND expense_date<=?`; params.push(to); }
    sql += ` ORDER BY expense_date DESC, id DESC`;
    res.json(await allAsync(sql, params));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/get-cost-logs', authRequired, requirePerm('perm_view_cost_logs'), async (req, res) => {
  try {
    if (!canSeeFullAccounts(req.user)) return res.status(403).json({ error: 'غير مصرح' });
    const from = String(req.query.from || '').trim();
    const to = String(req.query.to || '').trim();
    res.json(await getCurrentCostLogRows({ from, to }));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/save-expense', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canEditExpenseRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بإضافة أو تعديل المصاريف' });
    const b = req.body || {};
    const amount = num(b.amount);
    if (amount <= 0) return res.status(400).json({ error: 'أدخل مبلغ صحيح' });
    const linked = num(b.linked_to_order) === 1;
    const expense_date = String(b.expense_date || today());
    const notes = String(b.notes || '');
    const category = String(b.category || '');
    const custom_category = String(b.custom_category || '');
    const expensePartnerName = isTrackedPartner(b.expense_partner_name) ? normalizePartnerName(b.expense_partner_name) : '';
    const actorInfo = await resolveRequestedActor(req, b.actor_username, { allowBlank: true });
    if (actorInfo.username && !isTrackedAdminUserRow(actorInfo)) return res.status(400).json({ error: 'الخصم من عهدة الشغل متاح لمحمد أو بودا فقط' });
    if (actorInfo.username && !canUseAdminCashOnExpense(req.user)) return res.status(403).json({ error: 'غير مصرح لك بالخصم من عهدة الشغل' });
    if (expensePartnerName && actorInfo.username) return res.status(400).json({ error: 'اختار يا إما عهدة الشريك أو عهدة الشغل، مش الاتنين مع بعض' });
    if (actorInfo.username) await ensureAdminCashAvailable(actorInfo.username, amount, linked ? 'تكلفة الأوردر' : 'المصروف');
    const requestedExecPartnerId = num(b.execution_partner_id || 0);
    let executionPartner = null;
    if (requestedExecPartnerId > 0) executionPartner = await getAsync(`SELECT * FROM execution_partners WHERE id=?`, [requestedExecPartnerId]);

    if (linked) {
      const orderId = num(b.order_id);
      const field = String(b.order_cost_field || '').trim();
      const allowed = ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix'];
      if (!allowed.includes(field)) return res.status(400).json({ error: 'بند التكلفة غير صحيح' });
      const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [orderId]);
      if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
      const execTypeByField = { cost_print: 'مطبعة', cost_make: 'صنايعي', cost_hand_fix: 'تركيب يد' };
      const requiredExecType = execTypeByField[field] || '';
      if (requiredExecType) {
        if (!executionPartner) return res.status(400).json({ error: `اختَر الجهة المسؤولة عن ${field === 'cost_print' ? 'الطباعة' : (field === 'cost_make' ? 'التصنيع' : 'تركيب اليد')}` });
        const execType = String(executionPartner.partner_type || '').trim();
        const typeOk = requiredExecType === 'تركيب يد' ? (execType === 'تركيب يد' || execType === 'صنايعي') : execType === requiredExecType;
        if (!typeOk) return res.status(400).json({ error: 'الجهة المختارة لا تناسب نوع المرحلة' });
      } else {
        executionPartner = null;
      }

      const existing = await getAsync(`SELECT * FROM expenses WHERE linked_to_order=1 AND order_id=? AND order_cost_field=? ORDER BY id DESC LIMIT 1`, [orderId, field]);
      const duplicates = existing ? await allAsync(`SELECT id FROM expenses WHERE linked_to_order=1 AND order_id=? AND order_cost_field=? AND id<>?`, [orderId, field, existing.id]) : [];
      const duplicateIds = duplicates.map(x => num(x.id)).filter(Boolean);

      let undoPayload = await buildUndoPayloadFromDefs([
        ...orderSnapshotDefs([orderId]),
        ...(existing ? expenseSnapshotDefs([existing.id]) : []),
        ...(duplicateIds.length ? expenseSnapshotDefs(duplicateIds) : [])
      ]);

      await runAsync(`UPDATE orders SET ${field}=? WHERE id=?`, [amount, orderId]);

      let expenseId = 0;
      if (existing) {
        expenseId = existing.id;
        await runAsync(`UPDATE expenses SET expense_date=?,amount=?,category=?,custom_category=?,notes=?,linked_to_order=1,order_id=?,order_cost_field=?,expense_partner_name=?,actor_username=?,actor_name=?,execution_partner_id=?,execution_partner_name=?,execution_partner_type=? WHERE id=?`, [expense_date, amount, category, custom_category, notes, orderId, field, expensePartnerName, actorInfo.username, actorInfo.full_name, num(executionPartner?.id), String(executionPartner?.name || '').trim(), String(executionPartner?.partner_type || '').trim(), expenseId]);
      } else {
        const ins = await runAsync(`INSERT INTO expenses (expense_date,amount,category,custom_category,notes,linked_to_order,order_id,order_cost_field,expense_partner_name,actor_username,actor_name,execution_partner_id,execution_partner_name,execution_partner_type,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [expense_date, amount, category, custom_category, notes, 1, orderId, field, expensePartnerName, actorInfo.username, actorInfo.full_name, num(executionPartner?.id), String(executionPartner?.name || '').trim(), String(executionPartner?.partner_type || '').trim(), req.user.full_name || req.user.username]);
        expenseId = ins.lastID;
        undoPayload = [
          ...undoPayload,
          ...emptyUndoPayloadFromDefs(expenseSnapshotDefs([expenseId]))
        ];
      }

      for (const dup of duplicates) {
        await deletePartnerFundEntriesBySource('expense', String(num(dup.id)));
        await deleteAdminCashEntriesBySource('expense', String(num(dup.id)));
        await runAsync(`DELETE FROM cost_history WHERE source='accounts' AND source_ref=?`, [String(num(dup.id))]);
        await runAsync(`DELETE FROM expenses WHERE id=?`, [num(dup.id)]);
      }

      await syncExpensePartnerFund({ expenseId, partnerName: expensePartnerName, amount, expenseDate: expense_date, linkedToOrder: 1, notes, createdBy: req.user.full_name || req.user.username });
      await syncExpenseAdminCash({ expenseId, actorUsername: actorInfo.username, actorName: actorInfo.full_name, amount, expenseDate: expense_date, linkedToOrder: 1, notes, createdBy: req.user.full_name || req.user.username });
      await runAsync(`DELETE FROM cost_history WHERE source='accounts' AND source_ref=?`, [String(expenseId)]);
      await addCostLog({ order_id: orderId, cost_field: field, amount, source:'accounts', source_ref:String(expenseId), notes: notes || `تحديث تكلفة من الحسابات`, created_by:req.user.full_name || req.user.username });
      const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [orderId]);
      await syncSaleForOrder(refreshed, req.user.full_name || req.user.username);
      if (executionPartner && ['cost_print','cost_make','cost_hand_fix'].includes(field)) {
        await syncExecutionOperationFromExpense({ orderId, costField: field, executionPartner, amount, actorInfo, note: notes, expenseDate: expense_date, createdBy: req.user.full_name || req.user.username });
      }
      await recordAudit({
        req,
        action: existing ? 'update-expense' : 'save-expense',
        entity_type: 'expense',
        entity_id: expenseId,
        details: `${existing ? 'تحديث' : 'إضافة'} تكلفة على أوردر #${orderId} بقيمة ${amount.toFixed(2)} ج${expensePartnerName ? ` | من عهدة ${expensePartnerName}` : ''}${actorInfo.full_name ? ` | من عهدة الشغل ${actorInfo.full_name}` : ''}`,
        can_undo: 1,
        undo_type: existing ? 'update-expense' : 'save-expense',
        undo_payload: undoPayload,
        touch_refs: [
          ...orderTouchRefs([orderId]),
          ...partnerFundTouchRefs(expensePartnerName ? [expensePartnerName] : []),
          ...adminCashTouchRefs(actorInfo.username ? [actorInfo.username] : [])
        ]
      });
      return res.json({ success: true, linked_to_order: 1, id: expenseId, updated: existing ? 1 : 0 });
    }

    const expIns = await runAsync(`INSERT INTO expenses (expense_date,amount,category,custom_category,notes,linked_to_order,expense_partner_name,actor_username,actor_name,execution_partner_id,execution_partner_name,execution_partner_type,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, [expense_date, amount, category, custom_category, notes, 0, expensePartnerName, actorInfo.username, actorInfo.full_name, 0, '', '', req.user.full_name || req.user.username]);
    const undoPayload = emptyUndoPayloadFromDefs(expenseSnapshotDefs([expIns.lastID]));
    await syncExpensePartnerFund({ expenseId: expIns.lastID, partnerName: expensePartnerName, amount, expenseDate: expense_date, linkedToOrder: 0, notes, createdBy: req.user.full_name || req.user.username });
    await syncExpenseAdminCash({ expenseId: expIns.lastID, actorUsername: actorInfo.username, actorName: actorInfo.full_name, amount, expenseDate: expense_date, linkedToOrder: 0, notes, createdBy: req.user.full_name || req.user.username });
    await recordAudit({
      req,
      action: 'save-expense',
      entity_type: 'expense',
      entity_id: expIns.lastID,
      details: `إضافة مصروف بقيمة ${amount.toFixed(2)} ج${expensePartnerName ? ` من عهدة ${expensePartnerName}` : ''}${actorInfo.full_name ? ` | من عهدة الشغل ${actorInfo.full_name}` : ''}`,
      can_undo: 1,
      undo_type: 'save-expense',
      undo_payload: undoPayload,
      touch_refs: [
        ...partnerFundTouchRefs(expensePartnerName ? [expensePartnerName] : []),
        ...adminCashTouchRefs(actorInfo.username ? [actorInfo.username] : [])
      ]
    });
    res.json({ success: true, linked_to_order: 0, id: expIns.lastID });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/update-expense/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canEditExpenseRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بتعديل المصاريف' });
    const id = num(req.params.id);
    const old = await getAsync(`SELECT * FROM expenses WHERE id=?`, [id]);
    if (!old) return res.status(404).json({ error: 'غير موجود' });

    const b = req.body || {};
    const amount = num(b.amount);
    if (amount <= 0) return res.status(400).json({ error: 'أدخل مبلغ صحيح' });
    const linked = num(b.linked_to_order) === 1;
    const expense_date = String(b.expense_date || today());
    const notes = String(b.notes || '');
    const category = String(b.category || '');
    const custom_category = String(b.custom_category || '');
    const expensePartnerName = isTrackedPartner(b.expense_partner_name) ? normalizePartnerName(b.expense_partner_name) : '';
    const actorInfo = await resolveRequestedActor(req, b.actor_username, { allowBlank: true });
    if (actorInfo.username && !isTrackedAdminUserRow(actorInfo)) return res.status(400).json({ error: 'الخصم من عهدة الشغل متاح لمحمد أو بودا فقط' });
    if (actorInfo.username && !canUseAdminCashOnExpense(req.user)) return res.status(403).json({ error: 'غير مصرح لك بالخصم من عهدة الشغل' });
    if (expensePartnerName && actorInfo.username) return res.status(400).json({ error: 'اختار يا إما عهدة الشريك أو عهدة الشغل، مش الاتنين مع بعض' });
    const oldActorUsernameForFunds = normalizeActorUsername(old.actor_username);
    if (actorInfo.username) {
      const extraNeeded = oldActorUsernameForFunds === actorInfo.username ? Math.max(0, roundMoney(amount - num(old.amount))) : amount;
      await ensureAdminCashAvailable(actorInfo.username, extraNeeded, linked ? 'تكلفة الأوردر' : 'المصروف');
    }
    const requestedExecPartnerId = num(b.execution_partner_id || 0);
    let executionPartner = null;
    if (requestedExecPartnerId > 0) executionPartner = await getAsync(`SELECT * FROM execution_partners WHERE id=?`, [requestedExecPartnerId]);

    let order_id = null, order_cost_field = null;
    const oldOrderId = num(old.order_id);
    const oldField = String(old.order_cost_field || '').trim();

    const baseUndoDefs = [
      ...expenseSnapshotDefs([id]),
      ...(oldOrderId ? orderSnapshotDefs([oldOrderId]) : [])
    ];

    if (linked) {
      order_id = num(b.order_id);
      order_cost_field = String(b.order_cost_field || '').trim();
      const allowed = ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix'];
      if (!allowed.includes(order_cost_field)) return res.status(400).json({ error: 'بند التكلفة غير صحيح' });
      const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [order_id]);
      if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
      const execTypeByField = { cost_print: 'مطبعة', cost_make: 'صنايعي', cost_hand_fix: 'تركيب يد' };
      const requiredExecType = execTypeByField[order_cost_field] || '';
      if (requiredExecType) {
        if (!executionPartner) return res.status(400).json({ error: `اختَر الجهة المسؤولة عن ${order_cost_field === 'cost_print' ? 'الطباعة' : (order_cost_field === 'cost_make' ? 'التصنيع' : 'تركيب اليد')}` });
        const execType = String(executionPartner.partner_type || '').trim();
        const typeOk = requiredExecType === 'تركيب يد' ? (execType === 'تركيب يد' || execType === 'صنايعي') : execType === requiredExecType;
        if (!typeOk) return res.status(400).json({ error: 'الجهة المختارة لا تناسب نوع المرحلة' });
      } else {
        executionPartner = null;
      }

      const existingOther = await getAsync(`SELECT * FROM expenses WHERE linked_to_order=1 AND order_id=? AND order_cost_field=? AND id<>? ORDER BY id DESC LIMIT 1`, [order_id, order_cost_field, id]);
      const undoPayload = await buildUndoPayloadFromDefs([
        ...baseUndoDefs,
        ...(order_id && order_id !== oldOrderId ? orderSnapshotDefs([order_id]) : []),
        ...(existingOther ? expenseSnapshotDefs([existingOther.id]) : [])
      ]);

      if (existingOther) {
        await deletePartnerFundEntriesBySource('expense', String(num(existingOther.id)));
        await deleteAdminCashEntriesBySource('expense', String(num(existingOther.id)));
        await runAsync(`DELETE FROM cost_history WHERE source='accounts' AND source_ref=?`, [String(num(existingOther.id))]);
        await runAsync(`DELETE FROM expenses WHERE id=?`, [num(existingOther.id)]);
      }

      await runAsync(`UPDATE orders SET ${order_cost_field}=? WHERE id=?`, [amount, order_id]);
      if (num(old.linked_to_order) === 1 && oldOrderId && oldField && (oldOrderId != order_id || oldField !== order_cost_field)) {
        await runAsync(`UPDATE orders SET ${oldField}=0 WHERE id=?`, [oldOrderId]);
      }

      await runAsync(`UPDATE expenses SET expense_date=?,amount=?,category=?,custom_category=?,notes=?,linked_to_order=?,order_id=?,order_cost_field=?,expense_partner_name=?,actor_username=?,actor_name=?,execution_partner_id=?,execution_partner_name=?,execution_partner_type=? WHERE id=?`, [expense_date, amount, category, custom_category, notes, 1, order_id, order_cost_field, expensePartnerName, actorInfo.username, actorInfo.full_name, num(executionPartner?.id), String(executionPartner?.name || '').trim(), String(executionPartner?.partner_type || '').trim(), id]);
      await syncExpensePartnerFund({ expenseId: id, partnerName: expensePartnerName, amount, expenseDate: expense_date, linkedToOrder: 1, notes, createdBy: req.user.full_name || req.user.username });
      await syncExpenseAdminCash({ expenseId: id, actorUsername: actorInfo.username, actorName: actorInfo.full_name, amount, expenseDate: expense_date, linkedToOrder: 1, notes, createdBy: req.user.full_name || req.user.username });
      await runAsync(`DELETE FROM cost_history WHERE source='accounts' AND source_ref=?`, [String(id)]);
      await addCostLog({ order_id, cost_field: order_cost_field, amount, source:'accounts', source_ref:String(id), notes: notes || `تعديل تكلفة من الحسابات`, created_by:req.user.full_name || req.user.username });

      if (oldOrderId && oldOrderId != num(order_id || 0)) {
        const oldRefreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [oldOrderId]);
        if (oldRefreshed) await syncSaleForOrder(oldRefreshed, req.user.full_name || req.user.username);
      }
      const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [order_id]);
      if (refreshed) await syncSaleForOrder(refreshed, req.user.full_name || req.user.username);
      if (executionPartner && ['cost_print','cost_make','cost_hand_fix'].includes(order_cost_field)) {
        await syncExecutionOperationFromExpense({ orderId: order_id, costField: order_cost_field, executionPartner, amount, actorInfo, note: notes, expenseDate: expense_date, createdBy: req.user.full_name || req.user.username });
      }

      await recordAudit({
        req,
        action: 'update-expense',
        entity_type: 'expense',
        entity_id: id,
        details: `تعديل مصروف/تكلفة ${amount.toFixed(2)} ج${expensePartnerName ? ` | من عهدة ${expensePartnerName}` : ''}${actorInfo.full_name ? ` | من عهدة الشغل ${actorInfo.full_name}` : ''}`,
        can_undo: 1,
        undo_type: 'update-expense',
        undo_payload: undoPayload,
        touch_refs: [
          ...orderTouchRefs(uniqueList([oldOrderId, order_id]).filter(Boolean)),
          ...partnerFundTouchRefs(uniqueList([old.expense_partner_name, expensePartnerName]).filter(Boolean)),
          ...adminCashTouchRefs(uniqueList([old.actor_username, actorInfo.username]).filter(Boolean))
        ]
      });
      return res.json({ success: true });
    }

    const undoPayload = await buildUndoPayloadFromDefs(baseUndoDefs);
    if (num(old.linked_to_order) === 1 && oldOrderId && oldField) {
      await runAsync(`UPDATE orders SET ${oldField}=0 WHERE id=?`, [oldOrderId]);
    }

    await runAsync(`UPDATE expenses SET expense_date=?,amount=?,category=?,custom_category=?,notes=?,linked_to_order=?,order_id=?,order_cost_field=?,expense_partner_name=?,actor_username=?,actor_name=?,execution_partner_id=?,execution_partner_name=?,execution_partner_type=? WHERE id=?`, [expense_date, amount, category, custom_category, notes, 0, null, null, expensePartnerName, actorInfo.username, actorInfo.full_name, 0, '', '', id]);
    await syncExpensePartnerFund({ expenseId: id, partnerName: expensePartnerName, amount, expenseDate: expense_date, linkedToOrder: 0, notes, createdBy: req.user.full_name || req.user.username });
    await syncExpenseAdminCash({ expenseId: id, actorUsername: actorInfo.username, actorName: actorInfo.full_name, amount, expenseDate: expense_date, linkedToOrder: 0, notes, createdBy: req.user.full_name || req.user.username });
    await runAsync(`DELETE FROM cost_history WHERE source='accounts' AND source_ref=?`, [String(id)]);

    if (oldOrderId) {
      const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [oldOrderId]);
      if (refreshed) await syncSaleForOrder(refreshed, req.user.full_name || req.user.username);
    }

    await recordAudit({
      req,
      action: 'update-expense',
      entity_type: 'expense',
      entity_id: id,
      details: `تعديل مصروف ${amount.toFixed(2)} ج${expensePartnerName ? ` | من عهدة ${expensePartnerName}` : ''}${actorInfo.full_name ? ` | من عهدة الشغل ${actorInfo.full_name}` : ''}`,
      can_undo: 1,
      undo_type: 'update-expense',
      undo_payload: undoPayload,
      touch_refs: [
        ...orderTouchRefs(oldOrderId ? [oldOrderId] : []),
        ...partnerFundTouchRefs(uniqueList([old.expense_partner_name, expensePartnerName]).filter(Boolean)),
        ...adminCashTouchRefs(uniqueList([old.actor_username, actorInfo.username]).filter(Boolean))
      ]
    });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/delete-expense/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canDeleteExpenseRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بحذف المصاريف' });
    const row = await getAsync(`SELECT * FROM expenses WHERE id=?`, [num(req.params.id)]);
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    const id = num(req.params.id);
    const undoPayload = await buildUndoPayloadFromDefs([
      ...expenseSnapshotDefs([id]),
      ...(num(row.linked_to_order) === 1 && row.order_id ? orderSnapshotDefs([num(row.order_id)]) : [])
    ]);
    if (num(row.linked_to_order) === 1 && row.order_id && row.order_cost_field) {
      const allowed = ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix'];
      if (allowed.includes(row.order_cost_field)) {
        await runAsync(`UPDATE orders SET ${row.order_cost_field}=0 WHERE id=?`, [num(row.order_id)]);
      }
      await runAsync(`DELETE FROM cost_history WHERE source='accounts' AND source_ref=?`, [String(id)]);
    }
    await deletePartnerFundEntriesBySource('expense', String(id));
    await deleteAdminCashEntriesBySource('expense', String(id));
    await runAsync(`DELETE FROM expenses WHERE id=?`, [id]);
    if (num(row.linked_to_order) === 1 && row.order_id) {
      const refreshed = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(row.order_id)]);
      if (refreshed) await syncSaleForOrder(refreshed, req.user.full_name || req.user.username);
    }
    await recordAudit({
      req,
      action: 'delete-expense',
      entity_type: 'expense',
      entity_id: id,
      details: `حذف مصروف ${num(row.amount).toFixed(2)} ج`,
      can_undo: 1,
      undo_type: 'delete-expense',
      undo_payload: undoPayload,
      touch_refs: [
        ...orderTouchRefs(num(row.linked_to_order) === 1 && row.order_id ? [num(row.order_id)] : []),
        ...partnerFundTouchRefs(row.expense_partner_name ? [row.expense_partner_name] : []),
        ...adminCashTouchRefs(row.actor_username ? [row.actor_username] : [])
      ]
    });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/partner-withdrawals', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    const filter = buildDateFilterParts('withdrawal_date', req.query.from, req.query.to);
    const rows = await allAsync(`SELECT * FROM partner_withdrawals WHERE 1=1${filter.sql} ORDER BY withdrawal_date DESC, id DESC`, filter.params);
    res.json(rows.map(row => ({ ...row, partner_name: normalizePartnerName(row.partner_name) || String(row.partner_name || '').trim() })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/save-partner-withdrawal', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    const body = req.body || {};
    const partnerName = normalizePartnerName(body.partner_name);
    const amount = roundMoney(Math.max(0, num(body.amount)));
    const withdrawalDate = String(body.withdrawal_date || today()).trim() || today();
    const note = String(body.note || '').trim();
    if (!partnerName) return res.status(400).json({ error: 'اكتب اسم الشريك' });
    if (!['محمد','عبدالقادر'].includes(partnerName)) return res.status(400).json({ error: 'الشريك لازم يكون محمد أو عبدالقادر' });
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const ins = await runAsync(`INSERT INTO partner_withdrawals (partner_name,amount,withdrawal_date,note,created_at,created_by) VALUES (?,?,?,?,?,?)`, [partnerName, amount, withdrawalDate, note, new Date().toISOString(), req.user.full_name || req.user.username]);
    await recordAudit({ req, action: 'save-partner-withdrawal', entity_type: 'partner_withdrawal', entity_id: ins.lastID, details: `سحب شريك: ${partnerName} | ${amount.toFixed(2)} ج` });
    res.json({ success: true, id: ins.lastID });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/update-partner-withdrawal/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM partner_withdrawals WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    const body = req.body || {};
    const partnerName = normalizePartnerName(body.partner_name);
    const amount = roundMoney(Math.max(0, num(body.amount)));
    const withdrawalDate = String(body.withdrawal_date || today()).trim() || today();
    const note = String(body.note || '').trim();
    if (!partnerName) return res.status(400).json({ error: 'اكتب اسم الشريك' });
    if (!['محمد','عبدالقادر'].includes(partnerName)) return res.status(400).json({ error: 'الشريك لازم يكون محمد أو عبدالقادر' });
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    await runAsync(`UPDATE partner_withdrawals SET partner_name=?, amount=?, withdrawal_date=?, note=? WHERE id=?`, [partnerName, amount, withdrawalDate, note, id]);
    await recordAudit({ req, action: 'update-partner-withdrawal', entity_type: 'partner_withdrawal', entity_id: id, details: `تعديل سحب شريك: ${partnerName}` });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/delete-partner-withdrawal/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM partner_withdrawals WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    await runAsync(`DELETE FROM partner_withdrawals WHERE id=?`, [id]);
    await recordAudit({ req, action: 'delete-partner-withdrawal', entity_type: 'partner_withdrawal', entity_id: id, details: `حذف سحب شريك: ${row.partner_name || ''}`.trim() || 'حذف سحب شريك' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/partner-fund-log', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    const filter = buildDateFilterParts('entry_date', req.query.from, req.query.to);
    const rows = await allAsync(`SELECT * FROM partner_fund_ledger WHERE 1=1${filter.sql} ORDER BY entry_date DESC, id DESC`, filter.params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/save-partner-fund-entry', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    const body = req.body || {};
    const partnerName = normalizePartnerName(body.partner_name);
    if (!isTrackedPartner(partnerName)) return res.status(400).json({ error: 'اختار محمد أو عبدالقادر' });
    const amount = roundMoney(Math.max(0, num(body.amount)));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const entryDate = String(body.entry_date || today()).trim() || today();
    const mode = String(body.entry_mode || 'add').trim();
    const note = String(body.note || '').trim();
    let delta = amount;
    if (mode === 'sub') delta = -amount;
    if (mode === 'set') {
      const current = await getPartnerFundBalance(partnerName);
      delta = roundMoney(amount - current);
    }
    const validMode = ['add','sub','set'].includes(mode) ? mode : 'add';
    const id = await addPartnerFundEntry({ partner_name: partnerName, entry_date: entryDate, entry_kind: validMode, amount, delta, note, source_type: 'manual', source_ref: '', is_auto: 0, created_by: req.user.full_name || req.user.username });
    await recordAudit({ req, action: 'partner-fund-entry', entity_type: 'partner_fund', entity_id: id, details: `حركة عهدة ${partnerName} | ${validMode} | ${amount.toFixed(2)} ج` });
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/update-partner-fund-entry/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM partner_fund_ledger WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    if (num(row.is_auto) === 1) return res.status(400).json({ error: 'الحركات التلقائية تعدل من المصروف نفسه' });
    const body = req.body || {};
    const partnerName = normalizePartnerName(body.partner_name);
    if (!isTrackedPartner(partnerName)) return res.status(400).json({ error: 'اختار محمد أو عبدالقادر' });
    const amount = roundMoney(Math.max(0, num(body.amount)));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const entryDate = String(body.entry_date || today()).trim() || today();
    const mode = ['add','sub','set'].includes(String(body.entry_mode || '').trim()) ? String(body.entry_mode).trim() : 'add';
    let delta = amount;
    if (mode === 'sub') delta = -amount;
    if (mode === 'set') {
      const currentWithout = roundMoney((await getPartnerFundBalance(partnerName)) - (partnerName === normalizePartnerName(row.partner_name) ? num(row.delta) : 0));
      delta = roundMoney(amount - currentWithout);
    }
    await runAsync(`UPDATE partner_fund_ledger SET partner_name=?, entry_date=?, entry_kind=?, amount=?, delta=?, note=? WHERE id=?`, [partnerName, entryDate, mode, amount, delta, String(body.note || '').trim(), id]);
    await rebuildPartnerFundBalances();
    await recordAudit({ req, action: 'update-partner-fund-entry', entity_type: 'partner_fund', entity_id: id, details: `تعديل حركة عهدة ${partnerName}` });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/delete-partner-fund-entry/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM partner_fund_ledger WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    if (num(row.is_auto) === 1) return res.status(400).json({ error: 'الحركات التلقائية تتحذف من المصروف نفسه' });
    await runAsync(`DELETE FROM partner_fund_ledger WHERE id=?`, [id]);
    await rebuildPartnerFundBalances();
    await recordAudit({ req, action: 'delete-partner-fund-entry', entity_type: 'partner_fund', entity_id: id, details: `حذف حركة عهدة ${row.partner_name || ''}`.trim() || 'حذف حركة عهدة' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/get-sales-logs', authRequired, requirePerm('perm_view_sales_logs'), async (req, res) => {
  try {
    if (!canSeeFullAccounts(req.user)) return res.status(403).json({ error: 'غير مصرح' });
    await syncAllSalesHistory();
    const from = String(req.query.from || '').trim();
    const to = String(req.query.to || '').trim();
    let sql = `SELECT s.*, o.printType, o.colorSpecs FROM sales_history s LEFT JOIN orders o ON o.id=s.order_id WHERE 1=1`;
    const params = [];
    if (from) { sql += ` AND s.sale_date>=?`; params.push(from); }
    if (to) { sql += ` AND s.sale_date<=?`; params.push(to); }
    sql += ` ORDER BY s.sale_date DESC, s.id DESC`;
    res.json(await allAsync(sql, params));
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.get('/accounts-purchases', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    if (!canSeeFullAccounts(req.user)) return res.json([]);
    const from = String(req.query.from || '').trim();
    const to = String(req.query.to || '').trim();
    let sql = `SELECT * FROM purchases WHERE 1=1`;
    const params = [];
    if (from) { sql += ` AND purchase_date>=?`; params.push(from); }
    if (to) { sql += ` AND purchase_date<=?`; params.push(to); }
    sql += ` ORDER BY purchase_date DESC, id DESC`;
    res.json(await allAsync(sql, params));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/accounts-summary', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    await syncAllSalesHistory();
    if (!canSeeFullAccounts(req.user)) {
      const params = [];
      let expenseWhere = ` WHERE linked_to_order=0`;
      if (req.query.from) { expenseWhere += ` AND expense_date>=?`; params.push(req.query.from); }
      if (req.query.to) { expenseWhere += ` AND expense_date<=?`; params.push(req.query.to); }
      const exp = await getAsync(`SELECT COALESCE(SUM(amount),0) totalExpenses FROM expenses` + expenseWhere, params);
      return res.json({ totalSales: 0, totalCosts: 0, totalExpenses: num(exp.totalExpenses), totalProfit: 0, totalRemaining: 0, totalPurchases: 0, totalPurchasesRemaining: 0 });
    }
    const from = String(req.query.from || '').trim();
    const to = String(req.query.to || '').trim();
    let orderWhere = ` WHERE 1=1`;
    const op = [];
    if (from) { orderWhere += ` AND orderDate>=?`; op.push(from); }
    if (to) { orderWhere += ` AND orderDate<=?`; op.push(to); }
    let salesWhere = ` WHERE 1=1`;
    const sp = [];
    if (from) { salesWhere += ` AND sale_date>=?`; sp.push(from); }
    if (to) { salesWhere += ` AND sale_date<=?`; sp.push(to); }
    const sales = await getAsync(`SELECT COALESCE(SUM(total_sale),0) totalSales, COALESCE(SUM(total_cost),0) totalCosts, COALESCE(SUM(net_profit),0) totalProfit FROM sales_history` + salesWhere, sp);
    const receivablesSummary = await getAsync(`SELECT COALESCE(SUM(remaining_amount),0) remaining_amount FROM orders WHERE COALESCE(remaining_amount,0) > 0 AND TRIM(COALESCE(status,'')) != 'مرتجع'`);
    const manualReceivablesSummary = await getAsync(`SELECT COALESCE(SUM(remaining_amount),0) remaining_amount FROM manual_receivables`);
    const costs = await getAsync(`SELECT COALESCE(SUM(cost_cut+cost_print+cost_zinc+cost_design+cost_make+cost_hand+cost_paper+cost_hand_fix),0) totalCosts FROM orders` + orderWhere, op);
    const returned = await getAsync(`SELECT COALESCE(SUM(cost_cut+cost_print+cost_zinc+cost_design+cost_make+cost_hand+cost_paper+cost_hand_fix),0) returnedCosts FROM orders` + orderWhere + ` AND TRIM(COALESCE(status,''))='مرتجع'`, op);
    let expWhere = ` WHERE linked_to_order=0`;
    const ep = [];
    if (from) { expWhere += ` AND expense_date>=?`; ep.push(from); }
    if (to) { expWhere += ` AND expense_date<=?`; ep.push(to); }
    const exp = await getAsync(`SELECT COALESCE(SUM(amount),0) totalExpenses FROM expenses` + expWhere, ep);
    let purchasesWhere = ` WHERE 1=1`;
    const pp = [];
    if (from) { purchasesWhere += ` AND purchase_date>=?`; pp.push(from); }
    if (to) { purchasesWhere += ` AND purchase_date<=?`; pp.push(to); }
    const purchasesSummary = await getAsync(`SELECT COALESCE(SUM(total_price),0) totalPurchases, COALESCE(SUM(remaining_amount),0) totalPurchasesRemaining FROM purchases` + purchasesWhere, pp);
    const payload = {
      totalSales: num(sales.totalSales),
      totalCosts: num(costs.totalCosts),
      totalExpenses: num(exp.totalExpenses),
      totalProfit: num(sales.totalProfit) - num(returned.returnedCosts) - num(exp.totalExpenses),
      totalRemaining: roundMoney(num(receivablesSummary?.remaining_amount) + num(manualReceivablesSummary?.remaining_amount)),
      totalPurchases: num(purchasesSummary?.totalPurchases),
      totalPurchasesRemaining: num(purchasesSummary?.totalPurchasesRemaining)
    };
    if (!canViewFinancialTotals(req.user)) {
      payload.totalSales = 0;
      payload.totalCosts = 0;
      payload.totalExpenses = 0;
      payload.totalProfit = 0;
      payload.totalRemaining = 0;
      payload.totalPurchases = 0;
      payload.totalPurchasesRemaining = 0;
    }
    res.json(payload);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/production-jobs-summary', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT po.*, p.color AS paper_color, p.length AS paper_length, p.width AS paper_width, p.grammage AS paper_grammage, p.paper_name AS paper_name
      FROM production_orders po
      LEFT JOIN paper p ON p.id=po.paper_id
      WHERE TRIM(COALESCE(po.status,'pending'))!='done'
      ORDER BY po.production_date DESC, po.id DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/complete-production-job/:id', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const done = await completeProductionJob(num(req.params.id), req.user.full_name || req.user.username);
    await recordAudit({ req, action: 'complete-production-job', entity_type: 'production_order', entity_id: num(req.params.id), details: `إغلاق أمر التشغيل #${num(req.params.id)} وإضافة ${num(done.qty)} شنطة للمخزن` });
    res.json({ success: true, job: done });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/cash-summary', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    const summary = await getCashSummary({ from: req.query.from, to: req.query.to });
    if (!canViewFinancialTotals(req.user)) {
      return res.json({
        currentCash: 0,
        totalInflows: 0,
        totalOutflows: 0,
        totalInventoryValue: 0,
        totalAssets: 0,
        totalPurchases: 0,
        totalExpenses: 0,
        totalOrderExpensesPaid: 0,
        totalPartnerPayments: 0,
        totalPartnerDraws: 0,
        totalExecution: 0,
        totalOperationalPaid: 0,
        totalCostCutPaid: 0,
        totalCostPrintPaid: 0,
        totalCostZincPaid: 0,
        totalCostDesignPaid: 0,
        totalCostMakePaid: 0,
        totalCostHandFixPaid: 0,
        totalDebtPaid: 0,
        totalSalesPaid: 0,
        totalReceivablesCollected: 0,
        totalAdjustments: 0,
        paperValue: 0,
        handlesValue: 0,
        bagsValue: 0
      });
    }
    res.json(summary);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/active-users-lite', authRequired, async (req, res) => {
  try {
    const trackedOnly = num(req.query?.tracked_only) === 1;
    const users = trackedOnly ? await getTrackedAdminUsersLite() : await getActiveUsersLite();
    res.json({ users, current_user: normalizeActorUsername(req.user?.username), can_act_as_other: hasPerm(req.user, 'perm_act_as_other_admin') ? 1 : 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/admin-cash-log', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    if (!canViewFinancialTotals(req.user)) return res.json({ users: await getTrackedAdminUsersLite(), rows: [], summary: [] });
    res.json(await getAdminCashSummaryPayload({ from: req.query.from, to: req.query.to }));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/save-admin-cash-entry', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canEditAdminCashRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بإدارة عهدة الشغل' });
    const body = req.body || {};
    const actor = await resolveRequestedActor(req, body.admin_username, { allowBlank: false });
    if (!isTrackedAdminUserRow(actor)) return res.status(400).json({ error: 'العهدة متاحة لمحمد أو بودا فقط' });
    const mode = ['add','sub','set'].includes(String(body.entry_mode || '').trim()) ? String(body.entry_mode || '').trim() : 'add';
    const amount = roundMoney(Math.max(0, num(body.amount)));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    let delta = amount;
    if (mode === 'sub') delta = -amount;
    if (mode === 'set') delta = roundMoney(amount - await getAdminCashBalance(actor.username));
    const id = await addAdminCashEntry({ admin_username: actor.username, admin_name: actor.full_name, entry_date: String(body.entry_date || today()).trim() || today(), entry_kind: mode, amount, delta, note: String(body.note || '').trim(), source_type: 'manual', source_ref: '', is_auto: 0, created_by: req.user.full_name || req.user.username });
    const undoPayload = emptyUndoPayloadFromDefs(adminCashSnapshotDefs([id]));
    await recordAudit({ req, action: 'admin-cash-entry', entity_type: 'admin_cash', entity_id: id, details: `حركة عهدة شغل ${actor.full_name || actor.username} | ${mode} | ${amount.toFixed(2)} ج`, can_undo: 1, undo_type: 'admin-cash-entry', undo_payload: undoPayload, touch_refs: adminCashTouchRefs([actor.username]) });
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/update-admin-cash-entry/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canEditAdminCashRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بتعديل عهدة الشغل' });
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM admin_cash_ledger WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'الحركة غير موجودة' });
    if (num(row.is_auto) === 1) return res.status(400).json({ error: 'الحركات التلقائية تعدل من مصدرها' });
    const body = req.body || {};
    const actor = await resolveRequestedActor(req, body.admin_username, { allowBlank: false });
    if (!isTrackedAdminUserRow(actor)) return res.status(400).json({ error: 'العهدة متاحة لمحمد أو بودا فقط' });
    const mode = ['add','sub','set'].includes(String(body.entry_mode || '').trim()) ? String(body.entry_mode || '').trim() : 'add';
    const amount = roundMoney(Math.max(0, num(body.amount)));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const oldActorUsername = normalizeActorUsername(row.admin_username);
    const oldDelta = roundMoney(num(row.delta));
    let baseBalance = await getAdminCashBalance(actor.username);
    if (oldActorUsername && oldActorUsername === actor.username) baseBalance = roundMoney(baseBalance - oldDelta);
    let delta = amount;
    if (mode === 'sub') {
      if (roundMoney(baseBalance) + 1e-9 < amount) return res.status(400).json({ error: `لا يمكن الخصم من عهدة ${(actor.full_name || actor.username)}: المتاح ${baseBalance.toFixed(2)} ج فقط` });
      delta = -amount;
    }
    if (mode === 'set') delta = roundMoney(amount - baseBalance);
    const undoPayload = await buildUndoPayloadFromDefs(adminCashSnapshotDefs([id]));
    await runAsync(`UPDATE admin_cash_ledger SET admin_username=?, admin_name=?, entry_date=?, entry_kind=?, amount=?, delta=?, note=? WHERE id=?`, [actor.username, actor.full_name, String(body.entry_date || today()).trim() || today(), mode, amount, delta, String(body.note || '').trim(), id]);
    await rebuildAdminCashBalances();
    await recordAudit({ req, action: 'update-admin-cash-entry', entity_type: 'admin_cash', entity_id: id, details: `تعديل حركة عهدة شغل ${actor.full_name || actor.username}`, can_undo: 1, undo_type: 'update-admin-cash-entry', undo_payload: undoPayload, touch_refs: adminCashTouchRefs(uniqueList([oldActorUsername, actor.username]).filter(Boolean)) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/delete-admin-cash-entry/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canDeleteAdminCashRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بحذف عهدة الشغل' });
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM admin_cash_ledger WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'الحركة غير موجودة' });
    if (num(row.is_auto) === 1) return res.status(400).json({ error: 'الحركات التلقائية تتحذف من مصدرها' });
    const undoPayload = await buildUndoPayloadFromDefs(adminCashSnapshotDefs([id]));
    await runAsync(`DELETE FROM admin_cash_ledger WHERE id=?`, [id]);
    await rebuildAdminCashBalances();
    await recordAudit({ req, action: 'delete-admin-cash-entry', entity_type: 'admin_cash', entity_id: id, details: `حذف حركة عهدة شغل ${row.admin_name || row.admin_username || ''}`.trim() || 'حذف حركة عهدة شغل', can_undo: 1, undo_type: 'delete-admin-cash-entry', undo_payload: undoPayload, touch_refs: adminCashTouchRefs(row.admin_username ? [row.admin_username] : []) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/transfer-admin-cash', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canEditAdminCashRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بتحويل عهدة الشغل' });
    const body = req.body || {};
    const fromActor = await resolveRequestedActor(req, body.from_admin_username, { allowBlank: false });
    const toActor = await resolveRequestedActor(req, body.to_admin_username, { allowBlank: false });
    if (!isTrackedAdminUserRow(fromActor) || !isTrackedAdminUserRow(toActor)) return res.status(400).json({ error: 'التحويل متاح لمحمد أو بودا فقط' });
    const amount = roundMoney(Math.max(0, num(body.amount)));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    if (fromActor.username === toActor.username) return res.status(400).json({ error: 'اختار اسمين مختلفين للتحويل' });
    const entryDate = String(body.entry_date || today()).trim() || today();
    const note = String(body.note || '').trim() || `تحويل عهدة من ${fromActor.full_name || fromActor.username} إلى ${toActor.full_name || toActor.username}`;
    const sourceRef = `transfer-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
    const undoPayload = emptyUndoPayloadFromDefs(adminCashSourceSnapshotDefs(sourceRef));
    await addAdminCashEntry({ admin_username: fromActor.username, admin_name: fromActor.full_name, entry_date: entryDate, entry_kind: 'transfer_out', amount, delta: -amount, note, source_type: 'admin_transfer', source_ref: sourceRef, related_admin_username: toActor.username, related_admin_name: toActor.full_name, is_auto: 1, created_by: req.user.full_name || req.user.username });
    await addAdminCashEntry({ admin_username: toActor.username, admin_name: toActor.full_name, entry_date: entryDate, entry_kind: 'transfer_in', amount, delta: amount, note, source_type: 'admin_transfer', source_ref: sourceRef, related_admin_username: fromActor.username, related_admin_name: fromActor.full_name, is_auto: 1, created_by: req.user.full_name || req.user.username });
    await recordAudit({ req, action: 'transfer-admin-cash', entity_type: 'admin_cash_transfer', entity_id: 0, details: `تحويل ${amount.toFixed(2)} ج من ${fromActor.full_name || fromActor.username} إلى ${toActor.full_name || toActor.username}`, can_undo: 1, undo_type: 'transfer-admin-cash', undo_payload: undoPayload, touch_refs: adminCashTouchRefs([fromActor.username, toActor.username]) });
    res.json({ success: true, source_ref: sourceRef });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/cash-adjustments', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    const filter = buildDateFilterParts('adjustment_date', req.query.from, req.query.to);
    const rows = await allAsync(`SELECT * FROM cash_adjustments WHERE 1=1${filter.sql} ORDER BY adjustment_date DESC, id DESC`, filter.params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/cash-adjustments', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canManageCurrentCash(req.user) || !canEditCashRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بتعديل السيولة الحالية' });
    const body = req.body || {};
    const mode = ['set', 'add', 'sub'].includes(String(body.mode || '').trim()) ? String(body.mode || '').trim() : 'set';
    const rawAmount = roundMoney(Math.max(0, num(body.amount)));
    const adjustmentDate = String(body.adjustment_date || today()).trim() || today();
    const reason = String(body.reason || '').trim();
    const note = String(body.note || '').trim();
    if (rawAmount <= 0 && mode !== 'set') return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const currentSummary = await getCashSummary({});
    const previousBalance = roundMoney(num(currentSummary.currentCash));
    let delta = 0;
    let effectiveAmount = rawAmount;
    if (mode === 'set') delta = roundMoney(rawAmount - previousBalance);
    else if (mode === 'add') delta = roundMoney(rawAmount);
    else delta = roundMoney(-rawAmount);
    const newBalance = roundMoney(previousBalance + delta);
    const result = await runAsync(`INSERT INTO cash_adjustments (adjustment_date,action_type,amount,delta,previous_balance,new_balance,reason,note,created_at,created_by) VALUES (?,?,?,?,?,?,?,?,?,?)`, [adjustmentDate, mode, effectiveAmount, delta, previousBalance, newBalance, reason, note, new Date().toISOString(), req.user.full_name || req.user.username]);
    const undoPayload = emptyUndoPayloadFromDefs(cashAdjustmentSnapshotDefs([result.lastID]));
    const actionLabel = mode === 'set' ? 'تعيين الرصيد' : (mode === 'add' ? 'زيادة رصيد' : 'نقصان رصيد');
    await recordAudit({ req, action: 'cash-adjustment', entity_type: 'cash_adjustment', entity_id: result.lastID, details: `${actionLabel} | قبل ${previousBalance.toFixed(2)} | بعد ${newBalance.toFixed(2)} | فرق ${delta.toFixed(2)}${reason ? ` | السبب: ${reason}` : ''}`, can_undo: 1, undo_type: 'cash-adjustment', undo_payload: undoPayload, touch_refs: currentCashTouchRefs() });
    res.json({ success: true, id: result.lastID, previous_balance: previousBalance, new_balance: newBalance, delta, mode });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/cash-adjustments/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canManageCurrentCash(req.user) || !canEditCashRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بتعديل السيولة الحالية' });
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM cash_adjustments WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'التعديل غير موجود' });
    const body = req.body || {};
    const mode = ['set', 'add', 'sub'].includes(String(body.mode || '').trim()) ? String(body.mode || '').trim() : 'set';
    const rawAmount = roundMoney(Math.max(0, num(body.amount)));
    const adjustmentDate = String(body.adjustment_date || today()).trim() || today();
    const reason = String(body.reason || '').trim();
    const note = String(body.note || '').trim();
    if (rawAmount <= 0 && mode !== 'set') return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const currentSummary = await getCashSummary({});
    const baseBalance = roundMoney(num(currentSummary.currentCash) - num(row.delta));
    let delta = 0;
    let effectiveAmount = rawAmount;
    if (mode === 'set') delta = roundMoney(rawAmount - baseBalance);
    else if (mode === 'add') delta = roundMoney(rawAmount);
    else delta = roundMoney(-rawAmount);
    const newBalance = roundMoney(baseBalance + delta);
    const undoPayload = await buildUndoPayloadFromDefs(cashAdjustmentSnapshotDefs([id]));
    await runAsync(`UPDATE cash_adjustments SET adjustment_date=?,action_type=?,amount=?,delta=?,previous_balance=?,new_balance=?,reason=?,note=?,created_by=? WHERE id=?`, [adjustmentDate, mode, effectiveAmount, delta, baseBalance, newBalance, reason, note, req.user.full_name || req.user.username, id]);
    await recordAudit({ req, action: 'update-cash-adjustment', entity_type: 'cash_adjustment', entity_id: id, details: `تعديل حركة سيولة | ${mode} | ${effectiveAmount.toFixed(2)} ج`, can_undo: 1, undo_type: 'update-cash-adjustment', undo_payload: undoPayload, touch_refs: currentCashTouchRefs() });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/cash-adjustments/:id', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    if (!canManageCurrentCash(req.user) || !canDeleteCashRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بتعديل السيولة الحالية' });
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM cash_adjustments WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'التعديل غير موجود' });
    const undoPayload = await buildUndoPayloadFromDefs(cashAdjustmentSnapshotDefs([id]));
    await runAsync(`DELETE FROM cash_adjustments WHERE id=?`, [id]);
    await recordAudit({ req, action: 'delete-cash-adjustment', entity_type: 'cash_adjustment', entity_id: id, details: `حذف حركة سيولة ${num(row.amount).toFixed(2)} ج`, can_undo: 1, undo_type: 'delete-cash-adjustment', undo_payload: undoPayload, touch_refs: currentCashTouchRefs() });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/capital-settings', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    const row = await getAsync(`SELECT opening_capital, updated_at, updated_by FROM capital_settings WHERE id=1`);
    res.json({ opening_capital: num(row?.opening_capital), updated_at: row?.updated_at || '', updated_by: row?.updated_by || '' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/capital-settings', authRequired, requirePerm('perm_manage_expenses'), async (req, res) => {
  try {
    const openingCapital = roundMoney(Math.max(0, num(req.body?.opening_capital)));
    await runAsync(`INSERT INTO capital_settings (id, opening_capital, updated_at, updated_by) VALUES (1,?,?,?) ON CONFLICT(id) DO UPDATE SET opening_capital=excluded.opening_capital, updated_at=excluded.updated_at, updated_by=excluded.updated_by`, [openingCapital, new Date().toISOString(), req.user.full_name || req.user.username]);
    await recordAudit({ req, action: 'update-capital-settings', entity_type: 'capital_settings', entity_id: 1, details: `تحديد رأس المال إلى ${openingCapital.toFixed(2)} ج` });
    res.json({ success: true, opening_capital: openingCapital });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/capital-summary', authRequired, requirePerm('perm_view_accounts'), async (req, res) => {
  try {
    const summary = await getCapitalSummary({ from: req.query.from, to: req.query.to });
    res.json(summary);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/dashboard-data', authRequired, async (req, res) => {
  try {
    await syncAllSalesHistory();
    const sales = await getAsync(`SELECT COALESCE(SUM(total_sale),0) totalSales, COALESCE(SUM(remaining_amount),0) totalRemaining, COALESCE(SUM(totalProfit),0) bad FROM (SELECT total_sale, remaining_amount, net_profit as totalProfit FROM sales_history)`);
    const costs = await getAsync(`SELECT COALESCE(SUM(cost_cut+cost_print+cost_zinc+cost_design+cost_make+cost_hand+cost_paper+cost_hand_fix),0) totalCosts FROM orders`);
    const returned = await getAsync(`SELECT COALESCE(SUM(cost_cut+cost_print+cost_zinc+cost_design+cost_make+cost_hand+cost_paper+cost_hand_fix),0) returnedCosts FROM orders WHERE TRIM(COALESCE(status,''))='مرتجع'`);
    const exp = await getAsync(`SELECT COALESCE(SUM(amount),0) totalExpenses FROM expenses WHERE linked_to_order=0`);
    const paper = await getAsync(`SELECT COALESCE(SUM(total_sheets),0) paperSheetsTotal FROM paper`);
    const bags = await getAsync(`SELECT COALESCE(SUM(total_qty),0) bagsQtyTotal FROM bags`);
    const handles = await getAsync(`SELECT COALESCE(SUM(qty),0) handlesQtyTotal FROM handles`);
    res.json({ totalSales: num(sales.totalSales), totalCosts: num(costs.totalCosts), totalExpenses: num(exp.totalExpenses), totalProfit: num(sales.bad) - num(returned.returnedCosts) - num(exp.totalExpenses), totalRemaining: num(sales.totalRemaining), paperSheetsTotal: num(paper.paperSheetsTotal), bagsQtyTotal: num(bags.bagsQtyTotal), handlesQtyTotal: num(handles.handlesQtyTotal) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/low-stock', authRequired, async (req, res) => {
  try {
    res.json({
      paper: await allAsync(`SELECT * FROM paper WHERE (min_kg>0 AND total_kg<=min_kg) OR (min_sheets>0 AND total_sheets<=min_sheets) ORDER BY id DESC`),
      bags: await allAsync(`SELECT * FROM bags WHERE min_qty>0 AND total_qty<=min_qty ORDER BY id DESC`),
      handles: await allAsync(`SELECT * FROM handles WHERE min_qty>0 AND qty<=min_qty ORDER BY id DESC`)
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.get('/manual-receivables', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT * FROM manual_receivables ORDER BY COALESCE(due_date,'9999-12-31') ASC, id DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/manual-receivable-payments/:id', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT * FROM manual_receivable_payments WHERE receivable_id=? ORDER BY id DESC`, [num(req.params.id)]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/manual-receivables-summary', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    const s = await getAsync(`SELECT COALESCE(SUM(total_amount),0) total_amount, COALESCE(SUM(paid_amount),0) paid_amount, COALESCE(SUM(remaining_amount),0) remaining_amount, COUNT(*) rows_count FROM manual_receivables`);
    res.json(s || { total_amount: 0, paid_amount: 0, remaining_amount: 0, rows_count: 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-manual-receivable', authRequired, requirePerm('perm_manage_debts'), async (req, res) => {
  try {
    const b = req.body || {};
    const debtorName = String(b.debtor_name || '').trim();
    if (!debtorName) return res.status(400).json({ error: 'اكتب اسم الشخص أو الجهة' });
    const total = Math.max(0, num(b.total_amount));
    const paid = Math.max(0, num(b.paid_amount));
    const remaining = Math.max(total - paid, 0);
    const data = [debtorName, String(b.receivable_type || '').trim() || 'أخرى', String(b.subject || '').trim(), total, paid, remaining, String(b.due_date || '').trim() || null, String(b.notes || '').trim()];
    if (num(b.id) > 0) {
      await runAsync(`UPDATE manual_receivables SET debtor_name=?, receivable_type=?, subject=?, total_amount=?, paid_amount=?, remaining_amount=?, due_date=?, notes=? WHERE id=?`, [...data, num(b.id)]);
      await recordAudit({ req, action: 'update-manual-receivable', entity_type: 'manual_receivable', entity_id: num(b.id), details: `تعديل فلوس لينا عند ${debtorName} | ${total.toFixed(2)} ج` });
      return res.json({ success: true, id: num(b.id) });
    }
    const ins = await runAsync(`INSERT INTO manual_receivables (debtor_name,receivable_type,subject,total_amount,paid_amount,remaining_amount,due_date,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?)`, [...data, req.user.full_name || req.user.username]);
    await recordAudit({ req, action: 'create-manual-receivable', entity_type: 'manual_receivable', entity_id: ins.lastID, details: `إضافة فلوس لينا عند ${debtorName} | ${total.toFixed(2)} ج` });
    res.json({ success: true, id: ins.lastID });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/pay-manual-receivable/:id', authRequired, requirePerm('perm_manage_debts'), async (req, res) => {
  try {
    const receivable = await getAsync(`SELECT * FROM manual_receivables WHERE id=?`, [num(req.params.id)]);
    if (!receivable) return res.status(404).json({ error: 'السجل غير موجود' });
    const paid = Math.max(0, num(req.body?.amount));
    if (paid <= 0) return res.status(400).json({ error: 'اكتب المبلغ' });
    const paymentDate = String(req.body?.payment_date || '').trim() || today();
    const note = String(req.body?.note || '').trim();
    const ins = await runAsync(`INSERT INTO manual_receivable_payments (receivable_id,amount,payment_date,note,created_by) VALUES (?,?,?,?,?)`, [receivable.id, paid, paymentDate, note, req.user.full_name || req.user.username]);
    await runAsync(`UPDATE manual_receivables SET paid_amount=paid_amount+?, remaining_amount=MAX(total_amount-(paid_amount+?),0) WHERE id=?`, [paid, paid, receivable.id]);
    await recordAudit({ req, action: 'manual-receivable-payment', entity_type: 'manual_receivable', entity_id: receivable.id, details: `تحصيل ${paid.toFixed(2)} ج من ${receivable.debtor_name}` });
    res.json({ success: true, id: ins.lastID });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-manual-receivable/:id', authRequired, requirePerm('perm_manage_debts'), async (req, res) => {
  try {
    const row = await getAsync(`SELECT * FROM manual_receivables WHERE id=?`, [num(req.params.id)]);
    await runAsync(`DELETE FROM manual_receivable_payments WHERE receivable_id=?`, [num(req.params.id)]);
    await runAsync(`DELETE FROM manual_receivables WHERE id=?`, [num(req.params.id)]);
    await recordAudit({ req, action: 'delete-manual-receivable', entity_type: 'manual_receivable', entity_id: num(req.params.id), details: row ? `حذف فلوس لينا عند ${row.debtor_name}` : 'حذف فلوس لينا' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.get('/suppliers', authRequired, requireAnyPerm('perm_suppliers','perm_purchases'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT s.*, COALESCE(p.totalPurchases,0) AS totalPurchases, COALESCE(p.totalPaid,0) AS totalPaid, COALESCE(p.totalRemaining,0) AS totalRemaining, COALESCE(p.purchasesCount,0) AS purchasesCount
      FROM suppliers s
      LEFT JOIN (
        SELECT supplier_id, COUNT(DISTINCT COALESCE(NULLIF(invoice_group_no,''), 'single:' || id)) purchasesCount, COALESCE(SUM(total_price),0) totalPurchases, COALESCE(SUM(paid_amount),0) totalPaid, COALESCE(SUM(remaining_amount),0) totalRemaining
        FROM purchases GROUP BY supplier_id
      ) p ON p.supplier_id=s.id
      ORDER BY s.name COLLATE NOCASE ASC, s.id DESC`);
    res.json(rows.map(r=>({ ...r, totalPurchases: roundMoney(num(r.totalPurchases)+num(r.opening_balance)), totalRemaining: roundMoney(Math.max(0, num(r.totalRemaining)+num(r.opening_balance)))})));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/supplier-statement/:id', authRequired, requireAnyPerm('perm_suppliers','perm_purchases'), async (req, res) => {
  try {
    const data = await supplierStatementData(req.params.id);
    if (!data) return res.status(404).json({ error: 'المورد غير موجود' });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-supplier', authRequired, requireAnyPerm('perm_suppliers','perm_purchases'), async (req, res) => {
  try {
    const b = req.body || {};
    const name = String(b.name || '').trim();
    if (!name) return res.status(400).json({ error: 'اسم المورد مطلوب' });
    const data = [name, String(b.supplier_type || 'ورق').trim() || 'ورق', String(b.phone || '').trim(), String(b.address || '').trim(), String(b.notes || '').trim(), roundMoney(num(b.opening_balance)), num(b.is_active) === 0 ? 0 : 1, new Date().toISOString()];
    let id = num(b.id);
    let undoPayload = [];
    if (id) {
      undoPayload = await buildUndoPayloadFromDefs([{ table:'suppliers', criteria: criteriaEq('id', id) }]);
      await runAsync(`UPDATE suppliers SET name=?, supplier_type=?, phone=?, address=?, notes=?, opening_balance=?, is_active=?, updated_at=? WHERE id=?`, [...data, id]);
      await recordAudit({ req, action: 'update-supplier', entity_type: 'supplier', entity_id: id, details: `تعديل المورد ${name}`, can_undo: 1, undo_type: 'update-supplier', undo_payload: undoPayload, touch_refs: supplierTouchRefs([id]) });
    } else {
      const ins = await runAsync(`INSERT INTO suppliers (name,supplier_type,phone,address,notes,opening_balance,is_active,updated_at) VALUES (?,?,?,?,?,?,?,?)`, data);
      id = ins.lastID;
      undoPayload = emptyUndoPayloadFromDefs([{ table:'suppliers', criteria: criteriaEq('id', id) }]);
      await recordAudit({ req, action: 'save-supplier', entity_type: 'supplier', entity_id: id, details: `إضافة مورد ${name}`, can_undo: 1, undo_type: 'save-supplier', undo_payload: undoPayload, touch_refs: supplierTouchRefs([id]) });
    }
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-supplier/:id', authRequired, requireAnyPerm('perm_suppliers','perm_purchases'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const purchasesCount = await getAsync(`SELECT COUNT(*) c FROM purchases WHERE supplier_id=?`, [id]);
    if (num(purchasesCount?.c) > 0) return res.status(400).json({ error: 'لا يمكن حذف مورد عليه مشتريات مسجلة' });
    const row = await getAsync(`SELECT * FROM suppliers WHERE id=?`, [id]);
    const undoPayload = await buildUndoPayloadFromDefs([{ table:'suppliers', criteria: criteriaEq('id', id) }]);
    await runAsync(`DELETE FROM suppliers WHERE id=?`, [id]);
    await recordAudit({ req, action: 'delete-supplier', entity_type: 'supplier', entity_id: id, details: `حذف المورد ${row?.name || ''}`, can_undo: 1, undo_type: 'delete-supplier', undo_payload: undoPayload, touch_refs: supplierTouchRefs([id]) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/purchases', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT * FROM purchases ORDER BY purchase_date DESC, id DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/purchases-summary', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const row = await getAsync(`SELECT COALESCE(SUM(total_price),0) totalPurchases, COALESCE(SUM(paid_amount),0) totalPaid, COALESCE(SUM(remaining_amount),0) totalRemaining, COUNT(DISTINCT COALESCE(NULLIF(invoice_group_no,''), 'single:' || id)) purchasesCount FROM purchases`);
    res.json({ totalPurchases: num(row?.totalPurchases), totalPaid: num(row?.totalPaid), totalRemaining: num(row?.totalRemaining), purchasesCount: num(row?.purchasesCount) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/next-purchase-invoice-no', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    res.json({ invoice_group_no: await getNextPurchaseInvoiceNo() });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-purchase', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const b = req.body || {};
    const id = num(b.id);
    if (id) return res.status(400).json({ error: 'تعديل المشتريات غير مدعوم حاليًا. احذف العملية وأضفها من جديد.' });
    const supplierId = num(b.supplier_id);
    const supplier = supplierId ? await getAsync(`SELECT * FROM suppliers WHERE id=?`, [supplierId]) : null;
    const purchaseDate = String(b.purchase_date || today()).trim() || today();
    const itemType = String(b.item_type || 'خامة أخرى').trim() || 'خامة أخرى';
    const rawItemName = String(b.item_name || '').trim();
    const quantity = Math.max(0, num(b.quantity));
    const unit = String(b.unit || 'وحدة').trim() || 'وحدة';
    const unitPrice = roundMoney(num(b.unit_price));
    const totalPrice = roundMoney(num(b.total_price || (quantity * unitPrice)));
    const paidAmount = roundMoney(Math.max(0, Math.min(totalPrice, num(b.paid_amount))));
    const remainingAmount = roundMoney(Math.max(0, totalPrice - paidAmount));
    const stockType = String(b.stock_type || '').trim();
    let stockRefId = num(b.stock_ref_id);
    const stockMode = String(b.stock_mode || (stockRefId ? 'existing' : '')).trim() || (stockRefId ? 'existing' : '');
    const paperLength = num(b.paper_length);
    const paperWidth = num(b.paper_width);
    const paperGrammage = num(b.paper_grammage);
    const paperColor = String(b.paper_color || '').trim();
    const handleColor = String(b.handle_color || '').trim();
    let invoiceGroupNo = normalizePurchaseGroupNo(b.invoice_group_no);
    if (!invoiceGroupNo) invoiceGroupNo = await getNextPurchaseInvoiceNo();
    const itemName = rawItemName || (itemType === 'ورق' ? `ورق${paperColor ? ` ${paperColor}` : ''}${paperLength && paperWidth ? ` ${paperLength}×${paperWidth}` : ''}${paperGrammage ? ` - ${paperGrammage} جم` : ''}`.trim() : itemType === 'يد' ? `يد${handleColor ? ` ${handleColor}` : ''}`.trim() : '');
    if (!supplier) return res.status(400).json({ error: 'اختر المورد' });
    if (!itemName) return res.status(400).json({ error: 'اسم الصنف مطلوب' });
    if (quantity <= 0) return res.status(400).json({ error: 'الكمية المطلوبة غير صحيحة' });
    if (totalPrice <= 0) return res.status(400).json({ error: 'إجمالي الشراء مطلوب' });
    if (itemType === 'ورق' && !['كجم','فرخ'].includes(unit)) return res.status(400).json({ error: 'وحدة الورق لازم تكون كجم أو فرخ' });
    if (itemType === 'ورق' && paperGrammage <= 0) return res.status(400).json({ error: 'حدد جرام الورق' });
    if (itemType === 'يد' && unit !== 'عدد') return res.status(400).json({ error: 'وحدة اليد لازم تكون عدد' });

    if (stockType === 'paper' && !stockRefId && stockMode === 'new') {
      const existingPaper = await getAsync(`SELECT * FROM paper WHERE length=? AND width=? AND grammage=? AND color=? AND TRIM(COALESCE(paper_name,''))=?`, [paperLength, paperWidth, paperGrammage, paperColor, itemName]);
      if (existingPaper) {
        stockRefId = existingPaper.id;
      } else {
        const derivedBuyPriceKg = unit === 'كجم' ? unitPrice : (() => {
          const perSheetKg = paperSheetWeightKg({ length: paperLength, width: paperWidth, grammage: paperGrammage });
          return perSheetKg > 0 ? roundMoney(unitPrice / perSheetKg) : 0;
        })();
        const insPaper = await runAsync(`INSERT INTO paper (length,width,grammage,color,paper_name,total_kg,total_sheets,min_kg,min_sheets,buy_price_kg,buy_price_sheet) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [paperLength, paperWidth, paperGrammage, paperColor, itemName, 0, 0, 0, 0, derivedBuyPriceKg, unit === 'فرخ' ? unitPrice : 0]);
        stockRefId = insPaper.lastID;
        await refreshPaperPriceSheet(stockRefId);
      }
    }

    if (stockType === 'handle' && !stockRefId && stockMode === 'new') {
      const existingHandle = await getAsync(`SELECT * FROM handles WHERE color=?`, [handleColor]);
      if (existingHandle) {
        stockRefId = existingHandle.id;
      } else {
        const insHandle = await runAsync(`INSERT INTO handles (color,qty,buy_price,min_qty) VALUES (?,?,?,?)`, [handleColor, 0, unitPrice, 0]);
        stockRefId = insHandle.lastID;
      }
    }

    if (stockType && stockMode === 'existing' && !stockRefId) return res.status(400).json({ error: 'اختر الصنف الموجود بالمخزن أو غيّر الوضع إلى صنف جديد' });
    const stockApplied = stockType && stockRefId ? 1 : 0;
    const stockUndoPayload = stockApplied ? await buildUndoPayloadFromDefs(stockType === 'paper' ? [
      { table:'paper', criteria: criteriaEq('id', stockRefId) },
      { table:'paper_history', criteria: criteriaEq('paper_id', stockRefId) }
    ] : stockType === 'handle' ? [
      { table:'handles', criteria: criteriaEq('id', stockRefId) },
      { table:'handles_history', criteria: criteriaEq('handle_id', stockRefId) }
    ] : stockType === 'bag' ? [
      { table:'bags', criteria: criteriaEq('id', stockRefId) },
      { table:'bags_history', criteria: criteriaEq('bag_id', stockRefId) }
    ] : []) : [];
    const ins = await runAsync(`INSERT INTO purchases (purchase_date,supplier_id,supplier_name,item_type,item_name,quantity,unit,unit_price,total_price,paid_amount,remaining_amount,due_date,notes,stock_type,stock_ref_id,stock_applied,stock_mode,invoice_group_no,paper_length,paper_width,paper_grammage,paper_color,handle_color,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [purchaseDate, supplierId, supplier.name || '', itemType, itemName, quantity, unit, unitPrice, totalPrice, paidAmount, remainingAmount, String(b.due_date || '').trim(), String(b.notes || '').trim(), stockType, stockRefId, stockApplied, stockMode || '', invoiceGroupNo, paperLength, paperWidth, paperGrammage, paperColor, handleColor, req.user.full_name || req.user.username]);
    const purchase = await getAsync(`SELECT * FROM purchases WHERE id=?`, [ins.lastID]);
    if (stockApplied) await applyPurchaseToStock(purchase, 1);
    if (paidAmount > 0) await runAsync(`INSERT INTO purchase_payments (purchase_id,supplier_id,amount,payment_date,note,created_by) VALUES (?,?,?,?,?,?)`, [purchase.id, supplierId, paidAmount, purchaseDate, 'دفعة أولية مع فاتورة الشراء', req.user.full_name || req.user.username]);
    await refreshPurchaseRemaining(purchase.id);
    const undoPayload = [
      ...emptyUndoPayloadFromDefs(purchaseSnapshotDefs([purchase.id])),
      ...stockUndoPayload
    ];
    await recordAudit({ req, action: 'save-purchase', entity_type: 'purchase', entity_id: purchase.id, details: `إضافة مشتريات ${itemName} من المورد ${supplier.name}${stockApplied ? ` | ربط مخزن ${stockType}${stockMode === 'new' ? ' | صنف جديد' : ' | صنف موجود'}` : ''}`, can_undo: 1, undo_type: 'save-purchase', undo_payload: undoPayload, touch_refs: [...purchaseTouchRefs([purchase.id]), ...(stockApplied && stockType === 'paper' ? paperTouchRefs([stockRefId]) : []), ...(stockApplied && stockType === 'handle' ? handleTouchRefs([stockRefId]) : []), ...(stockApplied && stockType === 'bag' ? bagTouchRefs([stockRefId]) : [])] });
    res.json({ success: true, id: purchase.id, stock_ref_id: stockRefId, stock_applied: stockApplied });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-purchase/:id', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const purchase = await getAsync(`SELECT * FROM purchases WHERE id=?`, [id]);
    if (!purchase) return res.status(404).json({ error: 'المشتريات غير موجودة' });
    await deleteSinglePurchaseRecord(purchase, req, { auditAction: 'delete-purchase' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/purchase-group/:groupKey', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const payload = await getPurchaseGroupPayload(decodeURIComponent(String(req.params.groupKey || '')));
    if (!payload) return res.status(404).json({ error: 'الفاتورة غير موجودة' });
    res.json(payload);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-purchase-group/:groupKey', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const groupKey = decodeURIComponent(String(req.params.groupKey || ''));
    const rows = await getPurchaseGroupRowsByKey(groupKey);
    if (!rows.length) return res.status(404).json({ error: 'الفاتورة غير موجودة' });
    for (const purchase of rows) await deleteSinglePurchaseRecord(purchase, req, { auditAction: 'delete-purchase-group' });
    res.json({ success: true, deleted: rows.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/purchase-payments/:id', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try { res.json(await allAsync(`SELECT * FROM purchase_payments WHERE purchase_id=? ORDER BY payment_date DESC, id DESC`, [num(req.params.id)])); } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/add-purchase-group-payment/:groupKey', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const groupKey = decodeURIComponent(String(req.params.groupKey || ''));
    const amount = roundMoney(num(req.body?.amount));
    if (amount <= 0) return res.status(400).json({ error: 'المبلغ غير صحيح' });
    const result = await addPaymentToPurchaseGroup(groupKey, {
      amount,
      paymentDate: String(req.body?.payment_date || today()).trim() || today(),
      note: String(req.body?.note || '').trim(),
      createdBy: req.user.full_name || req.user.username
    });
    await recordAudit({ req, action: 'add-purchase-group-payment', entity_type: 'purchase', entity_id: 0, details: `تسجيل دفعة على فاتورة شراء مجمعة بقيمة ${amount.toFixed(2)} ج` });
    res.json({ success: true, ...result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/add-purchase-payment/:id', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const purchaseId = num(req.params.id);
    const purchase = await getAsync(`SELECT * FROM purchases WHERE id=?`, [purchaseId]);
    if (!purchase) return res.status(404).json({ error: 'المشتريات غير موجودة' });
    const amount = roundMoney(num(req.body?.amount));
    if (amount <= 0) return res.status(400).json({ error: 'المبلغ غير صحيح' });
    const undoPayload = await buildUndoPayloadFromDefs(purchaseSnapshotDefs([purchaseId]));
    await runAsync(`INSERT INTO purchase_payments (purchase_id,supplier_id,amount,payment_date,note,created_by) VALUES (?,?,?,?,?,?)`, [purchaseId, num(purchase.supplier_id), amount, String(req.body?.payment_date || today()).trim() || today(), String(req.body?.note || '').trim(), req.user.full_name || req.user.username]);
    const refreshed = await refreshPurchaseRemaining(purchaseId);
    await recordAudit({ req, action: 'add-purchase-payment', entity_type: 'purchase', entity_id: purchaseId, details: `تسجيل دفعة مشتريات بقيمة ${amount.toFixed(2)} ج`, can_undo: 1, undo_type: 'add-purchase-payment', undo_payload: undoPayload, touch_refs: purchaseTouchRefs([purchaseId]) });
    res.json({ success: true, purchase: refreshed });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/purchase-payment/:id', authRequired, requirePerm('perm_purchases'), async (req, res) => {
  try {
    const payment = await getAsync(`SELECT * FROM purchase_payments WHERE id=?`, [num(req.params.id)]);
    if (!payment) return res.status(404).json({ error: 'الدفعة غير موجودة' });
    const undoPayload = await buildUndoPayloadFromDefs(purchaseSnapshotDefs([num(payment.purchase_id)]));
    await runAsync(`DELETE FROM purchase_payments WHERE id=?`, [num(req.params.id)]);
    await refreshPurchaseRemaining(payment.purchase_id);
    await recordAudit({ req, action: 'delete-purchase-payment', entity_type: 'purchase-payment', entity_id: num(req.params.id), details: `حذف دفعة مشتريات بقيمة ${num(payment.amount).toFixed(2)} ج`, can_undo: 1, undo_type: 'delete-purchase-payment', undo_payload: undoPayload, touch_refs: purchaseTouchRefs([num(payment.purchase_id)]) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/reports-dashboard', authRequired, requirePerm('perm_view_reports'), async (req, res) => {
  try {
    await syncAllSalesHistory();
    const [sales, purchases, expenses, receivables, payables, delivered, pending, lateOrders, topCustomers, topSizes, monthlySales, lowPaper, lowBags, lowHandles, topSuppliers] = await Promise.all([
      getAsync(`SELECT COALESCE(SUM(total_sale),0) totalSales, COALESCE(SUM(total_cost),0) totalCosts, COALESCE(SUM(net_profit),0) totalProfit FROM sales_history`),
      getAsync(`SELECT COALESCE(SUM(total_price),0) totalPurchases, COALESCE(SUM(remaining_amount),0) totalPurchasesRemaining FROM purchases`),
      getAsync(`SELECT COALESCE(SUM(amount),0) totalExpenses FROM expenses WHERE linked_to_order=0`),
      getAsync(`SELECT COALESCE(SUM(remaining_amount),0) totalReceivables FROM orders WHERE remaining_amount>0`),
      getAsync(`SELECT COALESCE(SUM(remaining_amount),0) totalPayables FROM debts WHERE remaining_amount>0`),
      getAsync(`SELECT COUNT(*) c FROM orders WHERE TRIM(COALESCE(status,''))='تم التسليم'`),
      getAsync(`SELECT COUNT(*) c FROM orders WHERE TRIM(COALESCE(status,'')) IN ('أوردر جديد','تحت الإنتاج','في القص','مستني الزنكات','تحت الطباعة','تحت التصنيع','جاهز للشحن','تم الشحن')`),
      allAsync(`SELECT id,custName,status,due_date,total_price,remaining_amount FROM orders WHERE COALESCE(TRIM(due_date),'')!='' AND date(due_date)<date('now') AND TRIM(COALESCE(status,'')) NOT IN ('تم التسليم','مرتجع','ملغي') ORDER BY due_date ASC LIMIT 20`),
      allAsync(`SELECT customer_name, COUNT(*) ordersCount, COALESCE(SUM(gross_sale),0) grossSale, COALESCE(SUM(total_sale),0) netSales FROM sales_history GROUP BY customer_name ORDER BY netSales DESC LIMIT 10`),
      allAsync(`SELECT (CAST(l AS TEXT)||'×'||CAST(w AS TEXT)||CASE WHEN COALESCE(g,0)>0 THEN '×'||CAST(g AS TEXT) ELSE '' END) size_label, COUNT(*) ordersCount, COALESCE(SUM(qty),0) totalQty FROM orders GROUP BY size_label ORDER BY totalQty DESC LIMIT 10`),
      allAsync(`SELECT substr(sale_date,1,7) month, COALESCE(SUM(gross_sale),0) grossSale, COALESCE(SUM(total_sale),0) netSales, COALESCE(SUM(net_profit),0) netProfit, COUNT(*) ordersCount FROM sales_history GROUP BY substr(sale_date,1,7) ORDER BY month DESC LIMIT 12`),
      allAsync(`SELECT id,paper_name,color,length,width,grammage,total_kg,min_kg,total_sheets,min_sheets FROM paper WHERE (min_kg>0 AND total_kg<=min_kg) OR (min_sheets>0 AND total_sheets<=min_sheets) ORDER BY id DESC LIMIT 15`),
      allAsync(`SELECT id,length,width,gusset,color,handle,total_qty,min_qty FROM bags WHERE min_qty>0 AND total_qty<=min_qty ORDER BY id DESC LIMIT 15`),
      allAsync(`SELECT id,color,qty,min_qty FROM handles WHERE min_qty>0 AND qty<=min_qty ORDER BY id DESC LIMIT 15`),
      allAsync(`SELECT supplier_name, COUNT(*) purchasesCount, COALESCE(SUM(total_price),0) totalPurchases, COALESCE(SUM(remaining_amount),0) totalRemaining FROM purchases GROUP BY supplier_name ORDER BY totalPurchases DESC LIMIT 10`)
    ]);
    res.json({
      summary: {
        totalSales: num(sales?.totalSales),
        totalCosts: num(sales?.totalCosts),
        totalProfit: num(sales?.totalProfit),
        totalExpenses: num(expenses?.totalExpenses),
        totalPurchases: num(purchases?.totalPurchases),
        totalPurchasesRemaining: num(purchases?.totalPurchasesRemaining),
        totalReceivables: num(receivables?.totalReceivables),
        totalPayables: num(payables?.totalPayables),
        deliveredOrders: num(delivered?.c),
        pendingOrders: num(pending?.c),
        lateOrders: lateOrders.length
      },
      topCustomers, topSizes, monthlySales,
      lowStock: { paper: lowPaper, bags: lowBags, handles: lowHandles },
      lateOrders,
      topSuppliers
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/execution-partners', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT * FROM execution_partners ORDER BY partner_type ASC, name ASC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-execution-partner', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const b = req.body || {};
    const name = String(b.name || '').trim();
    const partnerType = String(b.partner_type || '').trim() || 'أخرى';
    if (!name) return res.status(400).json({ error: 'اكتب اسم الجهة' });
    const data = [name, partnerType, String(b.phone || '').trim(), String(b.address || '').trim(), String(b.notes || '').trim(), num(b.is_active, 1) ? 1 : 0];
    if (num(b.id) > 0) {
      await runAsync(`UPDATE execution_partners SET name=?, partner_type=?, phone=?, address=?, notes=?, is_active=? WHERE id=?`, [...data, num(b.id)]);
      await recordAudit({ req, action: 'update-partner', entity_type: 'partner', entity_id: num(b.id), details: `تعديل جهة تنفيذ ${name} | ${partnerType}` });
      return res.json({ success: true, id: num(b.id) });
    }
    const ins = await runAsync(`INSERT INTO execution_partners (name,partner_type,phone,address,notes,is_active) VALUES (?,?,?,?,?,?)`, data);
    await recordAudit({ req, action: 'create-partner', entity_type: 'partner', entity_id: ins.lastID, details: `إضافة جهة تنفيذ ${name} | ${partnerType}` });
    res.json({ success: true, id: ins.lastID });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-execution-partner/:id', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM execution_partners WHERE id=?`, [id]);
    await runAsync(`DELETE FROM execution_partners WHERE id=?`, [id]);
    await recordAudit({ req, action: 'delete-partner', entity_type: 'partner', entity_id: id, details: row ? `حذف جهة تنفيذ ${row.name}` : 'حذف جهة تنفيذ' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/operations-orders', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const rows = await getOrdersWithFiles(`SELECT * FROM orders WHERE TRIM(COALESCE(status,'')) NOT IN ('تم الشحن','تم التسليم','مرتجع') AND COALESCE(useReadyStock,0)=0 ORDER BY id DESC`);
    for (const row of rows) {
      const ops = await allAsync(`SELECT id,step_type,status,partner_name,partner_type,amount,completed_at,created_at FROM order_operations WHERE order_id=? ORDER BY id ASC`, [row.id]);
      row.operation_steps = ops;
      row.operation_counts = {
        total: ops.length,
        done: ops.filter(x => String(x.status || '').trim() === 'done').length,
        pending: ops.filter(x => String(x.status || '').trim() !== 'done').length
      };
    }
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/order-operations/:id', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const orderId = num(req.params.id);
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [orderId]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const steps = await allAsync(`SELECT oo.*, COALESCE((SELECT SUM(pp.amount) FROM partner_payments pp WHERE pp.operation_id=oo.id AND COALESCE(pp.auto_created,0)=1),0) paid_amount FROM order_operations oo WHERE oo.order_id=? ORDER BY oo.id ASC`, [orderId]);
    const partners = await allAsync(`SELECT * FROM execution_partners WHERE is_active=1 ORDER BY partner_type ASC, name ASC`);
    res.json({ order, steps, partners });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-order-operation', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const b = req.body || {};
    const orderId = num(b.order_id);
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [orderId]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const stepType = String(b.step_type || '').trim();
    const allowedSteps = ['plate','print','make','handle'];
    if (!allowedSteps.includes(stepType)) return res.status(400).json({ error: 'نوع المرحلة غير متاح' });
    if (!num(order.useReadyStock) && !num(order.paper_cut_done)) return res.status(400).json({ error: 'لازم قص الورق أولاً قبل توجيه الطباعة أو التصنيع أو الزنكات' });
    if (stepType === 'plate' && String(order.printType || '').trim() !== 'أوفست') return res.status(400).json({ error: 'الزنكات متاحة فقط لأوردرات الأوفست' });
    if (stepType === 'print' && ['','سادة'].includes(String(order.printType || '').trim())) return res.status(400).json({ error: 'هذا الأوردر سادة ولا يحتاج مرحلة طباعة خارجية' });
    const meta = executionStepMeta(stepType);
    const currentOperationId = num(b.id);
    const existingSameStep = await getAsync(`SELECT * FROM order_operations WHERE order_id=? AND step_type=? AND id<>? ORDER BY id DESC LIMIT 1`, [orderId, stepType, currentOperationId || 0]);
    if (existingSameStep) return res.status(400).json({ error: `تم توجيه ${meta.label} لهذا الأوردر بالفعل. احذف التوجيه الحالي أولاً لو عايز تسجله من جديد.` });
    const partnerId = num(b.partner_id);
    const partner = partnerId ? await getAsync(`SELECT * FROM execution_partners WHERE id=?`, [partnerId]) : null;
    const partnerName = String((partner && partner.name) || b.partner_name || '').trim();
    const partnerType = String((partner && partner.partner_type) || b.partner_type || meta.partner_type || '').trim();
    if (!partnerName) return res.status(400).json({ error: 'اختر الجهة المنفذة' });
    const expectedPartnerType = ({ plate:'مطبعة', print:'مطبعة', make:'صنايعي', handle:'تركيب يد أو صنايعي' })[stepType] || meta.partner_type;
    if (!partnerTypeAllowedForStep(stepType, partnerType)) return res.status(400).json({ error: `نوع الجهة لازم يكون ${expectedPartnerType}` });
    const stepLabel = String(b.step_label || '').trim() || meta.label;
    const status = String(b.status || '').trim() === 'done' ? 'done' : 'pending';
    const completedAt = status === 'done' ? (String(b.completed_at || '').trim() || new Date().toISOString()) : null;
    let amount = Math.max(0, num(b.amount));
    if (stepType === 'plate' && amount <= 0) amount = estimateOrderPlateCost(order);
    const actorInfo = await resolveRequestedActor(req, b.actor_username, { allowBlank: true });
    const payload = [orderId, stepType, stepLabel, partnerId || null, partnerName, partnerType, String(b.reference_code || '').trim(), Math.max(0, Math.round(num(b.qty) || num(order.qty))), Math.max(0, num(b.paper_sheets)), Math.max(0, num(b.paper_kg)), amount, status, String(b.note || '').trim(), String(b.started_at || '').trim() || new Date().toISOString(), completedAt, req.user.full_name || req.user.username, actorInfo.username, actorInfo.full_name];
    const linkedField = executionCostField(stepType);
    const paymentRequested = num(b.record_payment) === 1 || num(b.paid_now) === 1;
    const paymentDate = String(b.payment_date || '').trim() || today();
    const paymentNote = String(b.payment_note || '').trim();
    if (num(b.id) > 0) {
      const operationId = num(b.id);
      await runAsync(`UPDATE order_operations SET order_id=?, step_type=?, step_label=?, partner_id=?, partner_name=?, partner_type=?, reference_code=?, qty=?, paper_sheets=?, paper_kg=?, amount=?, status=?, note=?, started_at=?, completed_at=?, created_by=?, actor_username=?, actor_name=? WHERE id=?`, [...payload, operationId]);
      if (paymentRequested && amount > 0) await upsertAutoPaymentForOperation({ operationId, orderId, partnerId: partnerId || null, partnerName, stepType, amount, paymentDate, paymentNote, createdBy: req.user.full_name || req.user.username });
      await syncOrderOperationAdminCash({ operationId, actorUsername: actorInfo.username, actorName: actorInfo.full_name, amount, entryDate: paymentDate || today(), note: String(b.note || '').trim() || `تكلفة ${stepLabel} للأوردر #${orderId}`, stepType, createdBy: req.user.full_name || req.user.username });
      await recalculateOrderExecutionCosts(orderId, req.user.full_name || req.user.username, linkedField ? [linkedField] : []);
      await syncOrderOperationalStatus(orderId, req.user.full_name || req.user.username, req);
      await recordAudit({ req, action: 'update-order-operation', entity_type: 'order_operation', entity_id: operationId, details: `تعديل مرحلة ${stepLabel} للأوردر #${orderId} | ${partnerName || partnerType || '-'} | قيمة ${amount.toFixed(2)} ج${actorInfo.full_name ? ` | على عهدة ${actorInfo.full_name}` : ''}${paymentRequested && amount > 0 ? ' | تم تسجيل دفعة' : ''}` });
      return res.json({ success: true, id: operationId, auto_amount: amount, payment_recorded: paymentRequested && amount > 0 });
    }
    const ins = await runAsync(`INSERT INTO order_operations (order_id,step_type,step_label,partner_id,partner_name,partner_type,reference_code,qty,paper_sheets,paper_kg,amount,status,note,started_at,completed_at,created_by,actor_username,actor_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, payload);
    if (paymentRequested && amount > 0) await upsertAutoPaymentForOperation({ operationId: ins.lastID, orderId, partnerId: partnerId || null, partnerName, stepType, amount, paymentDate, paymentNote, createdBy: req.user.full_name || req.user.username });
    await syncOrderOperationAdminCash({ operationId: ins.lastID, actorUsername: actorInfo.username, actorName: actorInfo.full_name, amount, entryDate: paymentDate || today(), note: String(b.note || '').trim() || `تكلفة ${stepLabel} للأوردر #${orderId}`, stepType, createdBy: req.user.full_name || req.user.username });
    await recalculateOrderExecutionCosts(orderId, req.user.full_name || req.user.username, linkedField ? [linkedField] : []);
    await syncOrderOperationalStatus(orderId, req.user.full_name || req.user.username, req);
    await recordAudit({ req, action: 'create-order-operation', entity_type: 'order_operation', entity_id: ins.lastID, details: `إضافة مرحلة ${stepLabel} للأوردر #${orderId} | ${partnerName || partnerType || '-'} | كمية ${Math.max(0, Math.round(num(b.qty) || num(order.qty)))} | قيمة ${amount.toFixed(2)} ج${actorInfo.full_name ? ` | على عهدة ${actorInfo.full_name}` : ''}${paymentRequested && amount > 0 ? ' | تم تسجيل دفعة' : ''}` });
    res.json({ success: true, id: ins.lastID, auto_amount: amount, payment_recorded: paymentRequested && amount > 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-order-operation/:id', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM order_operations WHERE id=?`, [id]);
    const linkedPayments = await allAsync(`SELECT id FROM partner_payments WHERE operation_id=? AND COALESCE(auto_created,0)=1`, [id]);
    await runAsync(`DELETE FROM partner_payments WHERE operation_id=? AND COALESCE(auto_created,0)=1`, [id]);
    await deleteAdminCashEntriesBySource('order_operation', String(id));
    await runAsync(`DELETE FROM order_operations WHERE id=?`, [id]);
    if (row?.order_id) {
      const linkedField = executionCostField(row.step_type);
      await recalculateOrderExecutionCosts(row.order_id, req.user.full_name || req.user.username, linkedField ? [linkedField] : []);
      await syncOrderOperationalStatus(row.order_id, req.user.full_name || req.user.username, req);
    }
    await recordAudit({ req, action: 'delete-order-operation', entity_type: 'order_operation', entity_id: id, details: row ? `حذف مرحلة ${row.step_label || row.step_type} من الأوردر #${row.order_id}${linkedPayments.length ? ' | تم حذف الدفعة المرتبطة تلقائيًا' : ''}` : 'حذف مرحلة تشغيل' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/operations-partners-summary', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const partners = await allAsync(`SELECT * FROM execution_partners ORDER BY partner_type ASC, name ASC`);
    const out = [];
    for (const partner of partners) {
      const jobs = await allAsync(`SELECT oo.*, o.custName, o.status AS order_status, COALESCE((SELECT SUM(pp.amount) FROM partner_payments pp WHERE pp.operation_id=oo.id AND COALESCE(pp.auto_created,0)=1),0) paid_amount FROM order_operations oo LEFT JOIN orders o ON o.id=oo.order_id WHERE oo.partner_id=? AND COALESCE(o.useReadyStock,0)=0 AND TRIM(COALESCE(o.status,'')) NOT IN ('تم الشحن','تم التسليم','مرتجع') ORDER BY oo.id DESC`, [partner.id]);
      const pay = await getAsync(`SELECT COALESCE(SUM(amount),0) total_paid FROM partner_payments WHERE partner_id=?`, [partner.id]);
      const totalAssigned = jobs.reduce((s, r) => s + num(r.amount), 0);
      const totalPaid = num(pay?.total_paid);
      out.push({
        ...partner,
        jobs_count: jobs.length,
        done_jobs: jobs.filter(r => String(r.status || '').trim() === 'done').length,
        total_assigned: +totalAssigned.toFixed(2),
        total_paid: +totalPaid.toFixed(2),
        remaining: +(Math.max(totalAssigned - totalPaid, 0)).toFixed(2),
        jobs
      });
    }
    res.json(out);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/partner-payments/:id', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT * FROM partner_payments WHERE partner_id=? ORDER BY id DESC`, [num(req.params.id)]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-partner-payment', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const b = req.body || {};
    const partnerId = num(b.partner_id);
    const partner = await getAsync(`SELECT * FROM execution_partners WHERE id=?`, [partnerId]);
    if (!partner) return res.status(404).json({ error: 'الجهة غير موجودة' });
    const amount = Math.max(0, num(b.amount));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب المبلغ' });
    const paymentDate = String(b.payment_date || '').trim() || today();
    const paymentNote = String(b.note || '').trim();
    const actor = await resolveRequestedActor(req, b.admin_username, { allowBlank: false });
    await ensureAdminCashAvailable(actor.username, amount, 'دفعة الجهة');
    await runAsync('BEGIN TRANSACTION');
    try {
      const ins = await runAsync(`INSERT INTO partner_payments (partner_id,partner_name,amount,payment_date,note,created_by) VALUES (?,?,?,?,?,?)`, [partner.id, partner.name, amount, paymentDate, paymentNote, req.user.full_name || req.user.username]);
      await addAdminCashEntry({
        admin_username: actor.username,
        admin_name: actor.full_name,
        entry_date: paymentDate,
        entry_kind: 'expense',
        amount,
        delta: -amount,
        note: paymentNote || `دفعة مالية إلى ${partner.name}`,
        source_type: 'partner_payment',
        source_ref: String(ins.lastID),
        is_auto: 1,
        created_by: req.user.full_name || req.user.username
      });
      await recordAudit({ req, action: 'partner-payment', entity_type: 'partner_payment', entity_id: ins.lastID, details: `دفع ${amount.toFixed(2)} ج إلى ${partner.name} | خصم من عهدة ${actor.full_name || actor.username}` });
      await runAsync('COMMIT');
      res.json({ success: true, id: ins.lastID });
    } catch (err) {
      await runAsync('ROLLBACK');
      throw err;
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/operations-dashboard', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const activeOrders = await getAsync(`SELECT COUNT(*) c FROM orders WHERE TRIM(COALESCE(status,'')) NOT IN ('تم الشحن','تم التسليم','مرتجع') AND COALESCE(useReadyStock,0)=0`);
    const pendingOps = await getAsync(`SELECT COUNT(*) c FROM order_operations oo LEFT JOIN orders o ON o.id=oo.order_id WHERE TRIM(COALESCE(oo.status,''))!='done' AND COALESCE(o.useReadyStock,0)=0`);
    const partners = await getAsync(`SELECT COUNT(*) c FROM execution_partners WHERE COALESCE(is_active,1)=1`);
    const finances = await getAsync(`SELECT COALESCE(SUM(oo.amount),0) assigned FROM order_operations oo LEFT JOIN orders o ON o.id=oo.order_id WHERE COALESCE(o.useReadyStock,0)=0`);
    const paid = await getAsync(`SELECT COALESCE(SUM(pp.amount),0) paid FROM partner_payments pp LEFT JOIN order_operations oo ON oo.id=pp.operation_id LEFT JOIN orders o ON o.id=oo.order_id WHERE COALESCE(o.useReadyStock,0)=0 OR pp.operation_id IS NULL`);
    res.json({ active_orders: num(activeOrders?.c), pending_operations: num(pendingOps?.c), active_partners: num(partners?.c), total_assigned: +num(finances?.assigned).toFixed(2), total_paid: +num(paid?.paid).toFixed(2), remaining: +(Math.max(num(finances?.assigned) - num(paid?.paid), 0)).toFixed(2) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/order-payments/:id', authRequired, requirePerm('perm_view_orders'), async (req, res) => {
  try {
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(req.params.id)]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const rows = await allAsync(`SELECT * FROM order_payments WHERE order_id=? ORDER BY payment_date DESC, id DESC`, [order.id]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/add-order-payment/:id', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const order = await getAsync(`SELECT * FROM orders WHERE id=?`, [num(req.params.id)]);
    if (!order) return res.status(404).json({ error: 'الأوردر غير موجود' });
    const amount = Math.max(0, num(req.body.amount));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const customer = order.customer_id ? await getAsync(`SELECT * FROM customers WHERE id=?`, [num(order.customer_id)]) : null;
    await runAsync(`INSERT INTO order_payments (order_id,customer_id,amount,payment_date,method,note,created_by) VALUES (?,?,?,?,?,?,?)`, [order.id, num(customer?.id), amount, String(req.body.payment_date || today()).trim() || today(), String(req.body.method || 'نقدي').trim() || 'نقدي', String(req.body.note || '').trim(), req.user.full_name || req.user.username]);
    const refreshed = await refreshOrderPaymentSummary(order.id, req.user.full_name || req.user.username);
    await recordAudit({ req, action: 'add-order-payment', entity_type: 'order', entity_id: order.id, details: `إضافة دفعة ${amount.toFixed(2)} ج للأوردر #${order.id}` });
    res.json({ success: true, order: refreshed });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/order-payment/:id', authRequired, requirePerm('perm_edit_order'), async (req, res) => {
  try {
    const row = await getAsync(`SELECT * FROM order_payments WHERE id=?`, [num(req.params.id)]);
    if (!row) return res.status(404).json({ error: 'الدفعة غير موجودة' });
    if (String(row.note||'').trim() === 'الرصيد الافتتاحي للأوردر') return res.status(400).json({ error: 'لا يمكن حذف الرصيد الافتتاحي من هنا' });
    await runAsync(`DELETE FROM order_payments WHERE id=?`, [row.id]);
    await refreshOrderPaymentSummary(row.order_id, req.user.full_name || req.user.username);
    await recordAudit({ req, action: 'delete-order-payment', entity_type: 'order', entity_id: row.order_id, details: `حذف دفعة ${num(row.amount).toFixed(2)} ج من الأوردر #${row.order_id}` });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/users', authRequired, async (req, res) => {
  try {
    if (!hasPerm(req.user, 'perm_users')) return res.status(403).json({ error: 'غير مصرح' });
    res.json((await allAsync(`SELECT * FROM users ORDER BY id DESC`)).map(userSafe));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-user', authRequired, async (req, res) => {
  try {
    if (!hasPerm(req.user, 'perm_users')) return res.status(403).json({ error: 'غير مصرح' });
    const b = req.body || {};
    const role = normalizeRoleName(b.role);
    let current = null;
    if (b.id) current = await getAsync(`SELECT * FROM users WHERE id=?`, [num(b.id)]);
    const roleBase = rolePreset(role);
    const detailedPerms = collectIncomingDetailedPerms({ ...roleBase, ...b }, current || roleBase);
    const legacyPerms = computeLegacyPermsFromDetailed(detailedPerms);
    const data = {
      username: String(b.username || current?.username || '').trim(),
      password: (() => {
        const incoming = String(b.password || '').trim();
        if (incoming) return hashPassword(incoming);
        return String(current?.password || hashPassword('1234'));
      })(),
      full_name: String(b.full_name || current?.full_name || b.username || '').trim(),
      role: role || current?.role || 'moderator',
      is_active: num(b.is_active, current ? num(current.is_active, 1) : 1),
      ...legacyPerms,
      ...detailedPerms,
    };
    if (!data.username) return res.status(400).json({ error: 'اسم الدخول مطلوب' });
    const userColumns = ['username','password','full_name','role','is_active','perm_inventory','perm_bags','perm_orders','perm_add_order','perm_edit_order','perm_change_status','perm_accounts', ...DETAILED_PERMISSIONS];
    if (current) {
      const params = userColumns.map(col => data[col]).concat([num(b.id)]);
      await runAsync(`UPDATE users SET ${userColumns.map(col => `${col}=?`).join(',')} WHERE id=?`, params);
    } else {
      const params = userColumns.map(col => data[col]);
      await runAsync(`INSERT INTO users (${userColumns.join(',')}) VALUES (${userColumns.map(()=>'?').join(',')})`, params);
    }
    const permissionsSummary = DETAILED_PERMISSIONS.filter(key => num(detailedPerms[key]) === 1).join(', ');
    await recordAudit({ req, action: 'save-user', entity_type: 'user', entity_id: num(b.id || 0), details: (b.id ? `تعديل مستخدم ${data.username}` : `إضافة مستخدم ${data.username}`) + ` | الدور ${data.role} | صلاحيات: ${permissionsSummary || 'بدون صلاحيات'}` });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-user/:id', authRequired, async (req, res) => {
  try {
    if (!hasPerm(req.user, 'perm_users')) return res.status(403).json({ error: 'غير مصرح' });
    const id = num(req.params.id);
    const user = await getAsync(`SELECT * FROM users WHERE id=?`, [id]);
    if (!user) return res.status(404).json({ error: 'غير موجود' });
    if (user.username === 'admin') return res.status(400).json({ error: 'لا يمكن حذف admin' });
    await runAsync(`DELETE FROM users WHERE id=?`, [id]);
    await recordAudit({ req, action: 'delete-user', entity_type: 'user', entity_id: id, details: `حذف المستخدم ${user.username || ''}` });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/backup-db', authRequired, requirePerm('perm_backup_restore'), async (req, res) => {
  try {
    const name = `system_backup_${Date.now()}.zip`;
    const zipPath = path.join(BACKUP_DIR, name);
    await streamZipFile(zipPath);
    await recordAudit({ req, action: 'backup', entity_type: 'system', entity_id: null, details: `تصدير نسخة احتياطية ZIP: ${name}` });
    res.json({ success: true, url: `/protected-file/backups/${encodeURIComponent(name)}?token=${encodeURIComponent(req.token || '')}`, fileName: name, mode: 'full-system-backup' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/backup-db-file', authRequired, requirePerm('perm_backup_restore'), async (req, res) => {
  try {
    const name = `system_backup_${Date.now()}.db`;
    const dbCopyPath = path.join(BACKUP_DIR, name);
    if (!fs.existsSync(DB_PATH)) return res.status(404).json({ error: 'ملف قاعدة البيانات غير موجود' });
    fs.copyFileSync(DB_PATH, dbCopyPath);
    await recordAudit({ req, action: 'backup-db', entity_type: 'system', entity_id: null, details: `تصدير قاعدة البيانات: ${name}` });
    res.json({ success: true, url: `/protected-file/backups/${encodeURIComponent(name)}?token=${encodeURIComponent(req.token || '')}`, fileName: name, mode: 'database-only-backup' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/import-backup', authRequired, requirePerm('perm_backup_restore'), backupImportUpload.single('backupFile'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'ارفع ملف النسخة الاحتياطية أولاً' });
    const result = await restoreFromBackupFile(req.file.path, req.file.originalname);
    await ensureSchema();
    await syncAllSalesHistory();
    await recordAudit({ req, action: 'import-backup', entity_type: 'system', entity_id: null, details: `استيراد نسخة احتياطية من نوع ${result.type}` });
    res.json({ success: true, mode: result.type });
  } catch (e) {
    res.status(500).json({ error: e.message });
  } finally {
    try {
      if (req.file?.path && fs.existsSync(req.file.path)) fs.rmSync(req.file.path, { force: true });
    } catch (_) {}
  }
});

app.get('/activity-logs', authRequired, requirePerm('perm_activity_logs'), async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    const pageSize = Math.max(1, Math.min(200, num(req.query.pageSize, 50)));
    const page = Math.max(1, num(req.query.page, 1));
    let where = ` WHERE COALESCE(action,'') NOT IN ('login','logout')`;
    const params = [];
    if (q) {
      where += ` AND (username LIKE ? OR full_name LIKE ? OR action LIKE ? OR entity_type LIKE ? OR details LIKE ? OR ip_address LIKE ? OR device_label LIKE ?)`;
      for (let i = 0; i < 7; i++) params.push(`%${q}%`);
    }
    const countRow = await getAsync(`SELECT COUNT(*) c FROM audit_logs${where}`, params);
    const total = num(countRow?.c);
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage = Math.min(page, totalPages);
    const rows = await allAsync(`SELECT * FROM audit_logs${where} ORDER BY id DESC LIMIT ? OFFSET ?`, params.concat([pageSize, (safePage - 1) * pageSize]));
    res.json({ rows, total, page: safePage, totalPages, pageSize });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/activity-logs/:id/undo', authRequired, requirePerm('perm_activity_logs'), async (req, res) => {
  try {
    const id = num(req.params.id);
    const log = await getAsync(`SELECT * FROM audit_logs WHERE id=?`, [id]);
    if (!log) return res.status(404).json({ error: 'السجل غير موجود' });
    if (String(log.reverted_at || '').trim()) return res.status(400).json({ error: 'تم التراجع عن هذا السجل بالفعل' });
    if (num(log.can_undo) !== 1) return res.status(400).json({ error: 'هذا السجل لا يدعم التراجع التلقائي' });
    const payload = safeJsonParse(log.undo_payload, []);
    if (!Array.isArray(payload) || !payload.length) return res.status(400).json({ error: 'لا توجد بيانات كافية للتراجع عن العملية' });
    const blocker = await firstBlockingAuditLog(log);
    if (blocker) return res.status(400).json({ error: `لا يمكن التراجع الآن لأن هناك عملية أحدث على نفس البيانات: ${String(blocker.action || '').trim() || 'عملية'} (#${num(blocker.id)})` });

    await runAsync('BEGIN TRANSACTION');
    try {
      await restoreUndoPayload(payload);
      try { await syncAllCustomersFromOrders(); } catch (_) {}
      try { await syncAllSalesHistory(); } catch (_) {}
      await runAsync(`UPDATE audit_logs SET reverted_at=?, reverted_by=?, can_undo=0 WHERE id=?`, [new Date().toISOString(), req.user.full_name || req.user.username, id]);
      await recordAudit({ req, action: 'undo-audit-log', entity_type: 'audit-log', entity_id: id, details: `تراجع عن العملية ${String(log.action || '').trim() || '-'} رقم ${id}`, touch_refs: normalizeTouchRefs(log.touch_refs || '[]') });
      await runAsync('COMMIT');
    } catch (err) {
      await runAsync('ROLLBACK');
      throw err;
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/activity-logs/:id/edit', authRequired, requirePerm('perm_activity_logs'), async (req, res) => {
  try {
    if (!canEditActivityRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بتعديل سجل العمليات' });
    const id = num(req.params.id);
    const log = await getAsync(`SELECT * FROM audit_logs WHERE id=?`, [id]);
    if (!log) return res.status(404).json({ error: 'السجل غير موجود' });
    const nextAction = String(req.body?.action || log.action || '').trim();
    const nextDetails = String(req.body?.details || log.details || '').trim();
    await runAsync(`UPDATE audit_logs SET action=?, details=?, edited_at=?, edited_by=? WHERE id=?`, [nextAction, nextDetails, new Date().toISOString(), req.user.full_name || req.user.username, id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/activity-logs/:id', authRequired, requirePerm('perm_activity_logs'), async (req, res) => {
  try {
    if (!canDeleteActivityRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بحذف سجل العمليات' });
    const id = num(req.params.id);
    const log = await getAsync(`SELECT * FROM audit_logs WHERE id=?`, [id]);
    if (!log) return res.status(404).json({ error: 'السجل غير موجود' });
    if (!canDeleteSystemLogEntry(req.user, log)) return res.status(403).json({ error: 'غير مصرح لك بحذف هذا النوع من السجلات' });
    if (num(log.can_undo) !== 1) return res.status(400).json({ error: 'هذا السجل لا يمكن حذفه من قلب السيستم تلقائيًا لأنه غير مربوط بخريطة تراجع كاملة' });
    const payload = safeJsonParse(log.undo_payload, []);
    if (!Array.isArray(payload)) return res.status(400).json({ error: 'لا توجد بيانات كافية للتراجع عن العملية' });
    const blocker = await firstBlockingAuditLog(log);
    if (blocker) return res.status(400).json({ error: `لا يمكن حذف العملية من السيستم الآن لأن هناك عملية أحدث مرتبطة بها: ${String(blocker.action || '').trim() || 'عملية'} (#${num(blocker.id)})` });
    await runAsync('BEGIN TRANSACTION');
    try {
      await restoreUndoPayload(payload);
      try { await rebuildPartnerFundBalances(); } catch (_) {}
      try { await rebuildAdminCashBalances(); } catch (_) {}
      try { await syncAllCustomersFromOrders(); } catch (_) {}
      try { await syncAllSalesHistory(); } catch (_) {}
      await runAsync(`DELETE FROM audit_logs WHERE id=?`, [id]);
      await runAsync('COMMIT');
    } catch (err) {
      await runAsync('ROLLBACK');
      throw err;
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/session-history/:id', authRequired, requirePerm('perm_activity_logs'), async (req, res) => {
  try {
    if (!canDeleteActivityRecords(req.user)) return res.status(403).json({ error: 'غير مصرح لك بحذف جلسات الدخول' });
    const id = num(req.params.id);
    const row = await getAsync(`SELECT * FROM user_sessions_history WHERE id=?`, [id]);
    if (!row) return res.status(404).json({ error: 'جلسة الدخول غير موجودة' });
    await runAsync(`DELETE FROM user_sessions_history WHERE id=?`, [id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/session-history', authRequired, requirePerm('perm_activity_logs'), async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    const pageSize = Math.max(1, Math.min(200, num(req.query.pageSize, 50)));
    const page = Math.max(1, num(req.query.page, 1));
    let where = ` WHERE 1=1`;
    const params = [];
    if (q) {
      where += ` AND (username LIKE ? OR full_name LIKE ? OR ip_address LIKE ? OR device_label LIKE ?)`;
      for (let i = 0; i < 4; i++) params.push(`%${q}%`);
    }
    const countRow = await getAsync(`SELECT COUNT(*) c FROM user_sessions_history${where}`, params);
    const total = num(countRow?.c);
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage = Math.min(page, totalPages);
    const rows = await allAsync(`SELECT * FROM user_sessions_history${where} ORDER BY id DESC LIMIT ? OFFSET ?`, params.concat([pageSize, (safePage - 1) * pageSize]));
    res.json({ rows, total, page: safePage, totalPages, pageSize });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/debts', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT * FROM debts ORDER BY COALESCE(due_date,'9999-12-31') ASC, id DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/debt-payments/:id', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    res.json(await allAsync(`SELECT * FROM debt_payments WHERE debt_id=? ORDER BY payment_date DESC, id DESC`, [num(req.params.id)]));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/debts-summary', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    const s = await getAsync(`SELECT COALESCE(SUM(total_amount),0) total_amount, COALESCE(SUM(paid_amount),0) paid_amount, COALESCE(SUM(remaining_amount),0) remaining_amount FROM debts`);
    res.json({ total_amount: num(s?.total_amount), paid_amount: num(s?.paid_amount), remaining_amount: num(s?.remaining_amount) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/receivables', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    const rows = await allAsync(`SELECT id,custName,custPhone,custAddress,orderDate,status,total_price,paid_amount,remaining_amount,paymentType,priority,notes FROM orders WHERE COALESCE(remaining_amount,0) > 0 AND TRIM(COALESCE(status,'')) != 'مرتجع' ORDER BY COALESCE(orderDate,'') DESC, id DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/receivables-summary', authRequired, requirePerm('perm_view_debts'), async (req, res) => {
  try {
    const s = await getAsync(`SELECT COUNT(*) orders_count, COALESCE(SUM(total_price),0) total_amount, COALESCE(SUM(paid_amount),0) paid_amount, COALESCE(SUM(remaining_amount),0) remaining_amount FROM orders WHERE COALESCE(remaining_amount,0) > 0 AND TRIM(COALESCE(status,'')) != 'مرتجع'`);
    res.json({ orders_count: num(s?.orders_count), total_amount: num(s?.total_amount), paid_amount: num(s?.paid_amount), remaining_amount: num(s?.remaining_amount) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/save-debt', authRequired, requirePerm('perm_manage_debts'), async (req, res) => {
  try {
    const b = req.body || {};
    const total = Math.max(0, num(b.total_amount));
    const paid = Math.max(0, Math.min(total, num(b.paid_amount)));
    const remaining = Math.max(0, total - paid);
    if (b.id) {
      await runAsync(`UPDATE debts SET creditor_name=?, debt_type=?, subject=?, total_amount=?, paid_amount=?, remaining_amount=?, due_date=?, notes=? WHERE id=?`, [String(b.creditor_name || '').trim(), String(b.debt_type || '').trim(), String(b.subject || '').trim(), total, paid, remaining, String(b.due_date || '').trim() || null, String(b.notes || '').trim(), num(b.id)]);
      await recordAudit({ req, action: 'update-debt', entity_type: 'debt', entity_id: num(b.id), details: `تعديل مديونية ${b.creditor_name || ''}` });
      return res.json({ success: true, id: num(b.id) });
    }
    const r = await runAsync(`INSERT INTO debts (creditor_name,debt_type,subject,total_amount,paid_amount,remaining_amount,due_date,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?)`, [String(b.creditor_name || '').trim(), String(b.debt_type || '').trim(), String(b.subject || '').trim(), total, paid, remaining, String(b.due_date || '').trim() || null, String(b.notes || '').trim(), req.user.full_name || req.user.username]);
    if (paid > 0) {
      await runAsync(`INSERT INTO debt_payments (debt_id,amount,payment_date,note,created_by) VALUES (?,?,?,?,?)`, [r.lastID, paid, String(b.payment_date || today()).trim() || today(), 'دفعة أولية عند التسجيل', req.user.full_name || req.user.username]);
    }
    await recordAudit({ req, action: 'create-debt', entity_type: 'debt', entity_id: r.lastID, details: `إضافة مديونية على ${b.creditor_name || ''} بقيمة ${total.toFixed(2)} ج` });
    res.json({ success: true, id: r.lastID });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/pay-debt/:id', authRequired, requirePerm('perm_manage_debts'), async (req, res) => {
  try {
    const debt = await getAsync(`SELECT * FROM debts WHERE id=?`, [num(req.params.id)]);
    if (!debt) return res.status(404).json({ error: 'المديونية غير موجودة' });
    const amount = Math.max(0, num(req.body.amount));
    if (amount <= 0) return res.status(400).json({ error: 'اكتب مبلغ صحيح' });
    const paid = Math.min(num(debt.remaining_amount), amount);
    if (paid <= 0) return res.status(400).json({ error: 'لا يوجد رصيد مستحق على هذه المديونية' });
    await runAsync(`INSERT INTO debt_payments (debt_id,amount,payment_date,note,created_by) VALUES (?,?,?,?,?)`, [debt.id, paid, String(req.body.payment_date || today()).trim() || today(), String(req.body.note || '').trim(), req.user.full_name || req.user.username]);
    await runAsync(`UPDATE debts SET paid_amount=paid_amount+?, remaining_amount=MAX(total_amount-(paid_amount+?),0) WHERE id=?`, [paid, paid, debt.id]);
    await recordAudit({ req, action: 'pay-debt', entity_type: 'debt', entity_id: debt.id, details: `سداد ${paid.toFixed(2)} ج إلى ${debt.creditor_name || ''}` });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/delete-debt/:id', authRequired, requirePerm('perm_manage_debts'), async (req, res) => {
  try {
    const debt = await getAsync(`SELECT * FROM debts WHERE id=?`, [num(req.params.id)]);
    if (!debt) return res.status(404).json({ error: 'المديونية غير موجودة' });
    await runAsync(`DELETE FROM debt_payments WHERE debt_id=?`, [debt.id]);
    await runAsync(`DELETE FROM debts WHERE id=?`, [debt.id]);
    await recordAudit({ req, action: 'delete-debt', entity_type: 'debt', entity_id: debt.id, details: `حذف مديونية ${debt.creditor_name || ''}` });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/storage-config', authRequired, requireAdmin, async (req, res) => {
  try {
    res.json({
      dataDir: DATA_DIR,
      databasePath: DB_PATH,
      uploadsDir: UPLOAD_DIR,
      backupsDir: BACKUP_DIR,
      railwayDetected: !!process.env.RAILWAY_ENVIRONMENT,
      recommendedRailwayMountPath: '/app/data'
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 3000;
ensureSchema().then(async () => { await syncAllSalesHistory(); app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`)); }).catch(err => { console.error(err); process.exit(1); });
