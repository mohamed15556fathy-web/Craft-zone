'use strict';

const SHEET_CLASSES = Object.freeze(['quarter', 'half', 'full']);
const FORM_FAMILIES = Object.freeze(['bag', 'box', 'pouch', 'other']);
const FORM_FAMILY_LABELS = Object.freeze({
  bag: 'شنطة / كيس',
  box: 'علبة',
  pouch: 'باكيت',
  other: 'أخرى'
});

const PRODUCT_TYPE_FAMILIES = Object.freeze({
  'شنط': 'bag',
  'أكياس سندوتش': 'bag',
  'علب بطاطس': 'box',
  'علب برجر': 'box',
  'كريب': 'box',
  'باكيتات بطاطس': 'pouch',
  'أخرى': 'other'
});

function normalizeSheetClass(value = '', allowAuto = true) {
  const clean = String(value || '').trim().toLowerCase();
  if (SHEET_CLASSES.includes(clean)) return clean;
  return allowAuto ? 'auto' : '';
}

function requireExplicitSheetClass(value = '') {
  const normalized = normalizeSheetClass(value, false);
  if (normalized) return normalized;
  const error = new Error('اختار تصنيف الفورمة صراحةً: ربع فرخ أو نصف فرخ أو فرخ كامل');
  error.code = 'FORM_SHEET_CLASS_REQUIRED';
  error.statusCode = 400;
  throw error;
}

function familyForProductType(productType = '') {
  return PRODUCT_TYPE_FAMILIES[String(productType || '').trim()] || 'other';
}

function normalizeFormFamily(value = '') {
  const clean = String(value || '').trim().toLowerCase();
  return FORM_FAMILIES.includes(clean) ? clean : '';
}

function requireExplicitFormFamily(value = '', productType = '') {
  const family = normalizeFormFamily(value);
  if (!family) {
    const error = new Error('اختار التصنيف الفني للفورمة صراحةً');
    error.code = 'FORM_FAMILY_REQUIRED';
    error.statusCode = 400;
    throw error;
  }
  const cleanType = String(productType || '').trim();
  const expected = familyForProductType(cleanType);
  if (cleanType && cleanType !== 'أخرى' && family !== expected) {
    const error = new Error(`التصنيف الفني لا يطابق نوع المنتج؛ التصنيف الصحيح هو ${FORM_FAMILY_LABELS[expected]}`);
    error.code = 'FORM_FAMILY_MISMATCH';
    error.statusCode = 400;
    throw error;
  }
  return family;
}

module.exports = {
  SHEET_CLASSES,
  FORM_FAMILIES,
  FORM_FAMILY_LABELS,
  PRODUCT_TYPE_FAMILIES,
  normalizeSheetClass,
  requireExplicitSheetClass,
  familyForProductType,
  normalizeFormFamily,
  requireExplicitFormFamily
};
