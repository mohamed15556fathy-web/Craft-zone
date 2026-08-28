'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const rules = require('../lib/form_rules');

test('تصنيف الفرخ الصريح محفوظ ولا يتحول تلقائيًا', () => {
  assert.equal(rules.requireExplicitSheetClass('quarter'), 'quarter');
  assert.equal(rules.requireExplicitSheetClass('half'), 'half');
  assert.throws(
    () => rules.requireExplicitSheetClass('auto'),
    error => error.code === 'FORM_SHEET_CLASS_REQUIRED' && error.statusCode === 400
  );
});

test('التصنيف الفني يأتي من نوع المنتج الصريح لا من المقاس', () => {
  assert.equal(rules.familyForProductType('شنط'), 'bag');
  assert.equal(rules.familyForProductType('أكياس سندوتش'), 'bag');
  assert.equal(rules.familyForProductType('علب برجر'), 'box');
  assert.equal(rules.familyForProductType('باكيتات بطاطس'), 'pouch');
});

test('السيرفر يرفض تعارض نوع المنتج مع التصنيف الفني', () => {
  assert.equal(rules.requireExplicitFormFamily('box', 'علب بطاطس'), 'box');
  assert.throws(
    () => rules.requireExplicitFormFamily('bag', 'علب بطاطس'),
    error => error.code === 'FORM_FAMILY_MISMATCH' && error.statusCode === 400
  );
});

test('نوع أخرى يسمح بتصنيف فني صريح يختاره المستخدم', () => {
  assert.equal(rules.requireExplicitFormFamily('bag', 'أخرى'), 'bag');
  assert.equal(rules.requireExplicitFormFamily('other', 'أخرى'), 'other');
});
