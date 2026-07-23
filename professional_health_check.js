#!/usr/bin/env node
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

const dbPath = path.resolve(process.argv[2] || process.env.DATABASE_PATH || process.env.DB_PATH || path.join(__dirname, 'data', 'database.db'));
if (!fs.existsSync(dbPath)) {
  console.error('DB not found:', dbPath);
  process.exit(1);
}
const db = new sqlite3.Database(dbPath);
const all = (sql, p=[]) => new Promise((res, rej) => db.all(sql, p, (e, r) => e ? rej(e) : res(r)));
const get = (sql, p=[]) => new Promise((res, rej) => db.get(sql, p, (e, r) => e ? rej(e) : res(r)));
(async () => {
  const checks = [];
  const orphanExpenses = await get(`SELECT COUNT(*) c FROM expenses e LEFT JOIN orders o ON o.id=e.order_id WHERE COALESCE(e.order_id,0)>0 AND o.id IS NULL`).catch(()=>({c:0}));
  const duplicateAutoExpenses = await all(`SELECT order_id,order_cost_field,COUNT(*) c FROM expenses WHERE COALESCE(linked_to_order,0)=1 AND COALESCE(is_auto,0)=1 AND TRIM(COALESCE(order_cost_field,''))<>'' GROUP BY order_id,order_cost_field HAVING COUNT(*)>1`).catch(()=>[]);
  const duplicateBagSubs = await all(`SELECT order_id,COUNT(*) c FROM bags_history WHERE TRIM(COALESCE(type,''))='sub' AND COALESCE(order_id,0)>0 GROUP BY order_id HAVING COUNT(*)>1`).catch(()=>[]);
  const duplicateHandleSubs = await all(`SELECT order_id,COUNT(*) c FROM handles_history WHERE TRIM(COALESCE(type,''))='sub' AND COALESCE(order_id,0)>0 GROUP BY order_id HAVING COUNT(*)>1`).catch(()=>[]);
  if (Number(orphanExpenses.c || 0)) checks.push({severity:'error', key:'orphan_expenses', count:Number(orphanExpenses.c)});
  if (duplicateAutoExpenses.length) checks.push({severity:'warning', key:'duplicate_auto_order_costs', rows:duplicateAutoExpenses});
  if (duplicateBagSubs.length) checks.push({severity:'warning', key:'duplicate_ready_bag_deductions', rows:duplicateBagSubs});
  if (duplicateHandleSubs.length) checks.push({severity:'warning', key:'duplicate_handle_deductions', rows:duplicateHandleSubs});
  console.log(JSON.stringify({ok: checks.length === 0, checks}, null, 2));
  db.close();
  process.exit(checks.some(c => c.severity === 'error') ? 2 : 0);
})().catch(err => { console.error(err); db.close(); process.exit(1); });
