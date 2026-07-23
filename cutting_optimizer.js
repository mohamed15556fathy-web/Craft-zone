(function(root, factory){
  const api = factory();
  if(typeof module === 'object' && module.exports) module.exports = api;
  else root.CuttingOptimizer = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function(){
  'use strict';

  const EPS = 1e-7;
  const n = (value, fallback = 0) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  };
  const round = (value, digits = 4) => {
    const p = 10 ** digits;
    return Math.round((n(value) + Number.EPSILON) * p) / p;
  };
  const area = rect => n(rect?.w) * n(rect?.h);
  const overlaps = (a, b) => !(b.x >= a.x + a.w - EPS || b.x + b.w <= a.x + EPS || b.y >= a.y + a.h - EPS || b.y + b.h <= a.y + EPS);
  const contains = (outer, inner) => inner.x >= outer.x - EPS && inner.y >= outer.y - EPS && inner.x + inner.w <= outer.x + outer.w + EPS && inner.y + inner.h <= outer.y + outer.h + EPS;

  function normalizeMode(value){
    const mode = String(value || 'auto').trim();
    if(['single', 'singlePiece', 'single-piece'].includes(mode)) return 'single';
    if(['piece', 'pieceByPiece', 'piece-by-piece'].includes(mode)) return 'piece';
    return 'auto';
  }

  function normalizeItems(items){
    return (Array.isArray(items) ? items : []).map((item, index) => ({
      id: String(item.id ?? `item-${index + 1}`),
      label: String(item.label || `${n(item.length)}×${n(item.width)}×${n(item.gusset)}`),
      length: Math.max(0, n(item.length)),
      width: Math.max(0, n(item.width)),
      gusset: Math.max(0, n(item.gusset)),
      qty: Math.max(0, Math.ceil(n(item.qty))),
      mode: normalizeMode(item.mode)
    })).filter(item => item.length > 0 && item.width > 0 && item.qty > 0);
  }

  function cutDimensions(item, mode){
    const cleanMode = normalizeMode(mode) === 'auto' ? 'single' : normalizeMode(mode);
    const h = n(item.length) + (n(item.gusset) / 2) + 2;
    if(cleanMode === 'piece'){
      return { mode:'piece', modeLabel:'حتة ف حتة', w:n(item.width) + n(item.gusset) + 2, h, piecesPerBag:2 };
    }
    return { mode:'single', modeLabel:'حتة واحدة', w:(2 * n(item.width)) + (2 * n(item.gusset)) + 2, h, piecesPerBag:1 };
  }

  function allowedModes(item, globalMode = 'auto'){
    const own = normalizeMode(item.mode);
    if(own !== 'auto') return [own];
    const global = normalizeMode(globalMode);
    return global === 'auto' ? ['single', 'piece'] : [global];
  }

  function splitFreeRectangles(freeRects, used){
    const next = [];
    for(const free of freeRects){
      if(!overlaps(free, used)){
        next.push(free);
        continue;
      }
      if(used.x > free.x + EPS) next.push({ x:free.x, y:free.y, w:used.x - free.x, h:free.h });
      if(used.x + used.w < free.x + free.w - EPS) next.push({ x:used.x + used.w, y:free.y, w:(free.x + free.w) - (used.x + used.w), h:free.h });
      if(used.y > free.y + EPS) next.push({ x:free.x, y:free.y, w:free.w, h:used.y - free.y });
      if(used.y + used.h < free.y + free.h - EPS) next.push({ x:free.x, y:used.y + used.h, w:free.w, h:(free.y + free.h) - (used.y + used.h) });
    }
    const valid = next.filter(rect => rect.w > EPS && rect.h > EPS);
    return valid.filter((rect, index) => !valid.some((other, otherIndex) => index !== otherIndex && contains(other, rect)));
  }

  function placementOptions(state, width, height, allowRotation = true){
    const orientations = [{ w:width, h:height, rotated:false }];
    if(allowRotation && Math.abs(width - height) > EPS) orientations.push({ w:height, h:width, rotated:true });
    const options = [];
    state.free.forEach((free, freeIndex) => {
      orientations.forEach(size => {
        if(size.w <= free.w + EPS && size.h <= free.h + EPS){
          const shortSide = Math.min(free.w - size.w, free.h - size.h);
          const longSide = Math.max(free.w - size.w, free.h - size.h);
          options.push({
            x:free.x, y:free.y, w:size.w, h:size.h, rotated:size.rotated, freeIndex,
            fitScore:(free.w * free.h) - (size.w * size.h), shortSide, longSide
          });
        }
      });
    });
    return options.sort((a,b) => a.fitScore - b.fitScore || a.shortSide - b.shortSide || a.longSide - b.longSide || a.y - b.y || a.x - b.x);
  }

  function cloneState(state){
    return { free:state.free.map(rect => ({...rect})), placements:state.placements.map(rect => ({...rect})) };
  }

  function applyPlacement(state, choice, meta){
    const placed = { x:choice.x, y:choice.y, w:choice.w, h:choice.h, rotated:choice.rotated, ...meta };
    state.free = splitFreeRectangles(state.free, placed);
    state.placements.push(placed);
    return placed;
  }

  function tryBagGroup(state, item, mode, allowRotation){
    const cut = cutDimensions(item, mode);
    const firstOptions = placementOptions(state, cut.w, cut.h, allowRotation).slice(0, 10);
    let best = null;
    for(const first of firstOptions){
      const attempt = cloneState(state);
      applyPlacement(attempt, first, { itemId:item.id, itemLabel:item.label, mode:cut.mode, modeLabel:cut.modeLabel, bagIndex:0, pieceIndex:1 });
      // في حتة ف حتة يمكن قص كل وجه في فرخ مختلف؛ كل قطعة هنا تساوي نصف شنطة.
      const bagYield = cut.mode === 'piece' ? 0.5 : 1;
      const score = first.fitScore + attempt.free.length * 0.0001;
      if(!best || score < best.score) best = { state:attempt, score, cut, bagYield };
    }
    return best;
  }

  function chooseAction(actions, strategy, sheetArea){
    if(!actions.length) return null;
    const sorted = [...actions].sort((a,b) => {
      if(strategy === 'largest') return b.groupArea - a.groupArea || a.tryResult.score - b.tryResult.score || b.remaining - a.remaining;
      if(strategy === 'smallest') return a.groupArea - b.groupArea || a.tryResult.score - b.tryResult.score || b.remaining - a.remaining;
      if(strategy === 'demand') return b.remaining - a.remaining || b.groupArea - a.groupArea || a.tryResult.score - b.tryResult.score;
      if(strategy === 'single-first') return Number(b.mode === 'single') - Number(a.mode === 'single') || b.groupArea - a.groupArea || a.tryResult.score - b.tryResult.score;
      if(strategy === 'piece-first') return Number(b.mode === 'piece') - Number(a.mode === 'piece') || b.groupArea - a.groupArea || a.tryResult.score - b.tryResult.score;
      const fitA = a.tryResult.score / Math.max(sheetArea, 1);
      const fitB = b.tryResult.score / Math.max(sheetArea, 1);
      return fitA - fitB || b.groupArea - a.groupArea || b.remaining - a.remaining;
    });
    return sorted[0];
  }

  function buildPatternOnce(sheet, rawItems, remainingMap, options = {}, strategy = 'fit'){
    const items = normalizeItems(rawItems);
    const sheetW = Math.max(0, n(sheet.width));
    const sheetH = Math.max(0, n(sheet.length));
    if(sheetW <= 0 || sheetH <= 0) return null;
    let state = { free:[{ x:0, y:0, w:sheetW, h:sheetH }], placements:[] };
    const bags = Object.fromEntries(items.map(item => [item.id, 0]));
    const modes = {};
    const sheetArea = sheetW * sheetH;
    let guard = 0;
    while(guard++ < 10000){
      const actions = [];
      for(const item of items){
        const remaining = Math.max(0, n(remainingMap?.[item.id], item.qty) - bags[item.id]);
        if(remaining < 0.5 - EPS) continue;
        for(const mode of allowedModes(item, options.globalMode)){
          const tried = tryBagGroup(state, item, mode, options.allowRotation !== false);
          if(!tried) continue;
          if(remaining + EPS < tried.bagYield) continue;
          const cut = tried.cut;
          actions.push({ item, mode, remaining, tryResult:tried, groupArea:cut.w * cut.h });
        }
      }
      const action = chooseAction(actions, strategy, sheetArea);
      if(!action) break;
      const bagIndex = Math.floor(bags[action.item.id]) + 1;
      const beforeCount = state.placements.length;
      state = action.tryResult.state;
      for(let i = beforeCount; i < state.placements.length; i++) state.placements[i].bagIndex = bagIndex;
      bags[action.item.id] += action.tryResult.bagYield;
      modes[action.item.id] ||= {};
      modes[action.item.id][action.mode] = n(modes[action.item.id][action.mode]) + action.tryResult.bagYield;
    }
    const usedArea = state.placements.reduce((sum, rect) => sum + area(rect), 0);
    if(usedArea <= EPS) return null;
    return {
      sheet:{ width:sheetW, length:sheetH },
      bags, modes, placements:state.placements,
      usedArea:round(usedArea, 6), wasteArea:round(sheetArea - usedArea, 6), utilization:round((usedArea / sheetArea) * 100, 4)
    };
  }

  function buildBestPattern(sheet, items, remainingMap, options = {}){
    const strategies = options.strategies || ['fit','largest','demand','single-first','piece-first','smallest'];
    const patterns = strategies.map(strategy => buildPatternOnce(sheet, items, remainingMap, options, strategy)).filter(Boolean);
    patterns.sort((a,b) => b.usedArea - a.usedArea || sumValues(b.bags) - sumValues(a.bags) || a.placements.length - b.placements.length);
    return patterns[0] || null;
  }

  function sumValues(object){ return Object.values(object || {}).reduce((sum, value) => sum + n(value), 0); }

  function planSheet(rawSheet, rawItems, options = {}){
    const items = normalizeItems(rawItems);
    const sheet = {
      id:rawSheet?.id ?? null,
      label:String(rawSheet?.label || `${n(rawSheet?.length)}×${n(rawSheet?.width)}`),
      length:Math.max(0, n(rawSheet?.length)), width:Math.max(0, n(rawSheet?.width)),
      grammage:Math.max(0, n(rawSheet?.grammage, options.grammage)),
      priceKg:Math.max(0, n(rawSheet?.priceKg ?? rawSheet?.buy_price_kg, options.priceKg)),
      availableSheets:Math.max(0, Math.floor(n(rawSheet?.availableSheets ?? rawSheet?.total_sheets, Number.MAX_SAFE_INTEGER))),
      source:String(rawSheet?.source || options.source || 'manual')
    };
    const result = { sheet, items, patterns:[], completed:{}, remaining:{}, perItem:{}, totalSheets:0, totalKg:0, totalCost:0, usedArea:0, wasteArea:0, wastePercent:0, wasteKg:0, wasteCost:0, utilization:0, complete:false };
    if(!items.length || sheet.length <= 0 || sheet.width <= 0 || sheet.availableSheets <= 0) return result;
    const remaining = Object.fromEntries(items.map(item => [item.id, item.qty]));
    const completed = Object.fromEntries(items.map(item => [item.id, 0]));
    let sheetsLeft = sheet.availableSheets;
    let guard = 0;
    while(items.some(item => remaining[item.id] > 0) && sheetsLeft > 0 && guard++ < 1000){
      const pattern = buildBestPattern(sheet, items, remaining, options);
      if(!pattern) break;
      const usedItems = items.filter(item => n(pattern.bags[item.id]) > 0);
      if(!usedItems.length) break;
      let repetitions = Math.min(sheetsLeft, ...usedItems.map(item => Math.floor((remaining[item.id] + EPS) / n(pattern.bags[item.id]))));
      if(!Number.isFinite(repetitions) || repetitions < 1) repetitions = 1;
      pattern.repetitions = repetitions;
      pattern.patternNumber = result.patterns.length + 1;
      result.patterns.push(pattern);
      for(const item of usedItems){
        const made = Math.min(remaining[item.id], n(pattern.bags[item.id]) * repetitions);
        remaining[item.id] -= made;
        completed[item.id] += made;
      }
      sheetsLeft -= repetitions;
    }
    const sheetArea = sheet.length * sheet.width;
    const sheetWeightKg = sheetArea * sheet.grammage / 10000000;
    result.totalSheets = result.patterns.reduce((sum, pattern) => sum + pattern.repetitions, 0);
    result.usedArea = result.patterns.reduce((sum, pattern) => sum + pattern.usedArea * pattern.repetitions, 0);
    result.wasteArea = Math.max(0, result.totalSheets * sheetArea - result.usedArea);
    result.totalKg = result.totalSheets * sheetWeightKg;
    result.totalCost = result.totalKg * sheet.priceKg;
    result.wastePercent = result.totalSheets ? (result.wasteArea / (result.totalSheets * sheetArea)) * 100 : 0;
    result.utilization = 100 - result.wastePercent;
    result.wasteKg = result.wasteArea * sheet.grammage / 10000000;
    result.wasteCost = result.wasteKg * sheet.priceKg;
    result.completed = completed;
    result.remaining = remaining;
    result.complete = items.every(item => remaining[item.id] <= 0);

    const itemUsedArea = Object.fromEntries(items.map(item => [item.id, 0]));
    const modeCounts = Object.fromEntries(items.map(item => [item.id, { single:0, piece:0 }]));
    for(const pattern of result.patterns){
      for(const placement of pattern.placements) itemUsedArea[placement.itemId] = n(itemUsedArea[placement.itemId]) + area(placement) * pattern.repetitions;
      for(const item of items){
        modeCounts[item.id].single += n(pattern.modes?.[item.id]?.single) * pattern.repetitions;
        modeCounts[item.id].piece += n(pattern.modes?.[item.id]?.piece) * pattern.repetitions;
      }
    }
    for(const item of items){
      const used = itemUsedArea[item.id];
      const actualCost = result.usedArea > 0 ? result.totalCost * used / result.usedArea : 0;
      const theoreticalCost = used * sheet.grammage / 10000000 * sheet.priceKg;
      result.perItem[item.id] = {
        id:item.id, label:item.label, requested:item.qty, completed:completed[item.id], remaining:remaining[item.id],
        usedArea:round(used, 6), theoreticalCost:round(theoreticalCost, 4), actualCost:round(actualCost, 4),
        paperCostPerBag:completed[item.id] > 0 ? round(actualCost / completed[item.id], 4) : 0,
        theoreticalCostPerBag:completed[item.id] > 0 ? round(theoreticalCost / completed[item.id], 4) : 0,
        modeCounts:modeCounts[item.id]
      };
    }
    ['totalKg','totalCost','usedArea','wasteArea','wastePercent','wasteKg','wasteCost','utilization'].forEach(key => { result[key] = round(result[key], 4); });
    return result;
  }

  function ceilStep(value, step){ return Math.ceil(n(value) / Math.max(n(step, 1), EPS)) * Math.max(n(step, 1), EPS); }

  function generateCandidateSheets(rawItems, settings = {}){
    const items = normalizeItems(rawItems);
    const minSide = Math.max(20, n(settings.minSide, 50));
    const maxSide = Math.max(minSide, n(settings.maxSide, 160));
    const step = Math.max(1, n(settings.step, 5));
    const dimensions = [];
    for(const item of items){
      for(const mode of allowedModes(item, settings.globalMode)){
        const cut = cutDimensions(item, mode);
        dimensions.push([cut.w, cut.h]);
        if(settings.allowRotation !== false) dimensions.push([cut.h, cut.w]);
      }
    }
    const candidates = new Map();
    const add = (length, width, source = 'smart') => {
      length = ceilStep(length, step); width = ceilStep(width, step);
      if(length < minSide || width < minSide || length > maxSide || width > maxSide) return;
      const a = Math.max(length, width), b = Math.min(length, width);
      candidates.set(`${round(a,2)}|${round(b,2)}`, { length:a, width:b, source });
    };
    [[50,70],[64,90],[68,100],[68,115],[70,100],[80,120],[85,120],[86,100],[90,120],[100,100],[100,140],[120,160]].forEach(size => add(size[1], size[0], 'standard'));
    for(const [w,h] of dimensions){
      for(let x = 1; x <= 4; x++) for(let y = 1; y <= 4; y++) add(h * y, w * x);
    }
    for(let i = 0; i < dimensions.length; i++){
      for(let j = i; j < Math.min(dimensions.length, i + 12); j++){
        const [w1,h1] = dimensions[i], [w2,h2] = dimensions[j];
        add(Math.max(h1,h2), w1 + w2);
        add(h1 + h2, Math.max(w1,w2));
        add(Math.max(h1,w2), w1 + h2);
      }
    }
    return [...candidates.values()].slice(0, Math.max(20, n(settings.maxCandidates, 260)));
  }

  function planScore(plan, requestedTotal){
    const completedTotal = sumValues(plan.completed);
    const completionRatio = requestedTotal > 0 ? completedTotal / requestedTotal : 0;
    return completionRatio * 1000000 + n(plan.utilization) * 1000 - n(plan.totalSheets) * 0.01 - n(plan.totalCost) * 0.000001;
  }

  function suggestSheets(rawItems, settings = {}){
    const items = normalizeItems(rawItems);
    const requestedTotal = items.reduce((sum, item) => sum + item.qty, 0);
    const candidates = generateCandidateSheets(items, settings);
    const pre = candidates.map(sheet => {
      const pattern = buildBestPattern(sheet, items, Object.fromEntries(items.map(item => [item.id, item.qty])), settings);
      return { sheet, utilization:n(pattern?.utilization), bags:sumValues(pattern?.bags) };
    }).filter(row => row.bags > 0).sort((a,b) => b.utilization - a.utilization || b.bags - a.bags).slice(0, Math.max(8, n(settings.fullPlanCandidates, 28)));
    const plans = pre.map(row => planSheet({
      ...row.sheet,
      grammage:n(settings.grammage), priceKg:n(settings.priceKg),
      availableSheets:Number.MAX_SAFE_INTEGER, source:'smart'
    }, items, settings));
    plans.sort((a,b) => planScore(b, requestedTotal) - planScore(a, requestedTotal));
    return plans.slice(0, Math.max(1, n(settings.limit, 3)));
  }

  function stateFromPattern(pattern){
    const state = {
      free:[{ x:0, y:0, w:n(pattern?.sheet?.width), h:n(pattern?.sheet?.length) }],
      placements:[]
    };
    for(const raw of (pattern?.placements || [])){
      const placed = { ...raw, x:n(raw.x), y:n(raw.y), w:n(raw.w), h:n(raw.h) };
      state.free = splitFreeRectangles(state.free, placed);
      state.placements.push(placed);
    }
    return state;
  }

  function fillPatternOnce(pattern, rawFillers, options = {}, strategy = 'fit'){
    const fillers = normalizeItems((rawFillers || []).map(item => ({ ...item, qty:Math.max(1, n(item.qty, Number.MAX_SAFE_INTEGER)) })));
    let state = stateFromPattern(pattern);
    const fillerBags = Object.fromEntries(fillers.map(item => [item.id, 0]));
    const fillerModes = Object.fromEntries(fillers.map(item => [item.id, { single:0, piece:0 }]));
    const sheetArea = n(pattern?.sheet?.width) * n(pattern?.sheet?.length);
    let guard = 0;
    while(guard++ < 10000){
      const actions = [];
      for(const item of fillers){
        const maximum = Math.max(1, n(item.qty, Number.MAX_SAFE_INTEGER));
        if(fillerBags[item.id] >= maximum) continue;
        for(const mode of allowedModes(item, options.globalMode)){
          const tried = tryBagGroup(state, item, mode, options.allowRotation !== false);
          if(!tried) continue;
          const cut = tried.cut;
          actions.push({ item, mode, remaining:maximum - fillerBags[item.id], tryResult:tried, groupArea:cut.w * cut.h });
        }
      }
      const action = chooseAction(actions, strategy, sheetArea);
      if(!action) break;
      const beforeCount = state.placements.length;
      const bagIndex = Math.floor(fillerBags[action.item.id]) + 1;
      state = action.tryResult.state;
      for(let i = beforeCount; i < state.placements.length; i++){
        state.placements[i].bagIndex = bagIndex;
        state.placements[i].filler = true;
      }
      fillerBags[action.item.id] += action.tryResult.bagYield;
      fillerModes[action.item.id][action.mode] += action.tryResult.bagYield;
    }
    const addedArea = state.placements.filter(rect => rect.filler).reduce((sum, rect) => sum + area(rect), 0);
    const usedArea = n(pattern?.usedArea) + addedArea;
    const bags = { ...(pattern?.bags || {}) };
    const modes = JSON.parse(JSON.stringify(pattern?.modes || {}));
    for(const item of fillers){
      bags[item.id] = n(bags[item.id]) + n(fillerBags[item.id]);
      modes[item.id] ||= { single:0, piece:0 };
      modes[item.id].single = n(modes[item.id].single) + n(fillerModes[item.id].single);
      modes[item.id].piece = n(modes[item.id].piece) + n(fillerModes[item.id].piece);
    }
    return {
      ...pattern,
      placements:state.placements,
      bags, modes, fillerBags, fillerModes,
      addedArea:round(addedArea, 6), usedArea:round(usedArea, 6),
      wasteArea:round(Math.max(0, sheetArea - usedArea), 6),
      utilization:sheetArea > 0 ? round((usedArea / sheetArea) * 100, 4) : 0
    };
  }

  function fillPatternGaps(pattern, rawFillers, options = {}){
    const strategies = options.strategies || ['fit','largest','demand','single-first','piece-first','smallest'];
    const choices = strategies.map(strategy => fillPatternOnce(pattern, rawFillers, options, strategy));
    choices.sort((a,b) => b.addedArea - a.addedArea || sumValues(b.fillerBags) - sumValues(a.fillerBags));
    const best = choices[0] || { ...pattern, fillerBags:{}, fillerModes:{}, addedArea:0 };
    const repetitions = Math.max(1, n(pattern?.repetitions, 1));
    // لا نعرض أو نطبق نصف شنطة: لو تكرار النموذج ينتج عددًا فرديًا من الأوجه، نترك آخر وجه فراغًا.
    for(const itemId of Object.keys(best.fillerBags || {})){
      const produced = n(best.fillerBags[itemId]) * repetitions;
      if(Math.abs(produced - Math.round(produced)) <= EPS) continue;
      for(let index = best.placements.length - 1; index >= 0; index--){
        const placement = best.placements[index];
        if(!placement.filler || placement.itemId !== itemId || placement.mode !== 'piece') continue;
        best.placements.splice(index, 1);
        best.fillerBags[itemId] = Math.max(0, n(best.fillerBags[itemId]) - 0.5);
        if(best.fillerModes?.[itemId]) best.fillerModes[itemId].piece = Math.max(0, n(best.fillerModes[itemId].piece) - 0.5);
        if(best.bags?.[itemId] !== undefined) best.bags[itemId] = Math.max(0, n(best.bags[itemId]) - 0.5);
        if(best.modes?.[itemId]) best.modes[itemId].piece = Math.max(0, n(best.modes[itemId].piece) - 0.5);
        best.addedArea = Math.max(0, n(best.addedArea) - area(placement));
        best.usedArea = Math.max(0, n(best.usedArea) - area(placement));
        const sheetArea = n(best.sheet?.width) * n(best.sheet?.length);
        best.wasteArea = Math.max(0, sheetArea - best.usedArea);
        best.utilization = sheetArea > 0 ? round(best.usedArea / sheetArea * 100, 4) : 0;
        break;
      }
    }
    return best;
  }

  function analyzeFillers(plan, rawFillers, options = {}){
    const rawById = new Map((rawFillers || []).map(item => [String(item?.id ?? ''), item]));
    const fillers = normalizeItems((rawFillers || []).map(item => ({ ...item, qty:Number.MAX_SAFE_INTEGER })))
      .map(item => ({ ...(rawById.get(String(item.id)) || {}), ...item }));
    return fillers.map(item => {
      let totalBags = 0, addedArea = 0;
      const fits = [];
      for(const pattern of (plan?.patterns || [])){
        const filled = fillPatternGaps(pattern, [item], options);
        const perSheet = n(filled.fillerBags?.[item.id]);
        if(perSheet > 0){
          const repetitions = Math.max(1, n(pattern.repetitions, 1));
          totalBags += perSheet * repetitions;
          addedArea += n(filled.addedArea) * repetitions;
          fits.push({ patternNumber:n(pattern.patternNumber), perSheet, repetitions, total:perSheet * repetitions, modes:filled.fillerModes?.[item.id] || {} });
        }
      }
      return { ...item, totalBags, addedArea:round(addedArea, 6), fits };
    }).filter(row => row.totalBags > 0).sort((a,b) => b.addedArea - a.addedArea || b.totalBags - a.totalBags);
  }

  function applyFillers(plan, rawFillers, options = {}){
    const fillers = normalizeItems((rawFillers || []).map(item => ({ ...item, qty:Number.MAX_SAFE_INTEGER })));
    if(!fillers.length) return plan;
    const patterns = (plan?.patterns || []).map(pattern => fillPatternGaps(pattern, fillers, options));
    const result = {
      ...plan,
      sheet:{ ...(plan?.sheet || {}) },
      patterns,
      items:[...(plan?.items || []).map(item => ({...item}))],
      completed:{ ...(plan?.completed || {}) }, remaining:{ ...(plan?.remaining || {}) }, perItem:{},
      fillerIds:fillers.map(item => item.id), fillTotals:{}
    };
    for(const filler of fillers){
      const produced = patterns.reduce((sum, pattern) => sum + n(pattern.fillerBags?.[filler.id]) * Math.max(1, n(pattern.repetitions, 1)), 0);
      if(produced <= 0) continue;
      result.fillTotals[filler.id] = produced;
      result.items.push({ ...filler, qty:produced, isFiller:true });
      result.completed[filler.id] = produced;
      result.remaining[filler.id] = 0;
    }
    const sheetArea = n(result.sheet.length) * n(result.sheet.width);
    result.usedArea = patterns.reduce((sum, pattern) => sum + n(pattern.usedArea) * Math.max(1, n(pattern.repetitions, 1)), 0);
    result.wasteArea = Math.max(0, n(result.totalSheets) * sheetArea - result.usedArea);
    result.wastePercent = result.totalSheets && sheetArea ? result.wasteArea / (result.totalSheets * sheetArea) * 100 : 0;
    result.utilization = 100 - result.wastePercent;
    result.wasteKg = result.wasteArea * n(result.sheet.grammage) / 10000000;
    result.wasteCost = result.wasteKg * n(result.sheet.priceKg);
    const itemUsedArea = Object.fromEntries(result.items.map(item => [item.id, 0]));
    const modeCounts = Object.fromEntries(result.items.map(item => [item.id, { single:0, piece:0 }]));
    for(const pattern of patterns){
      const repetitions = Math.max(1, n(pattern.repetitions, 1));
      for(const placement of pattern.placements) itemUsedArea[placement.itemId] = n(itemUsedArea[placement.itemId]) + area(placement) * repetitions;
      for(const item of result.items){
        modeCounts[item.id].single += n(pattern.modes?.[item.id]?.single) * repetitions;
        modeCounts[item.id].piece += n(pattern.modes?.[item.id]?.piece) * repetitions;
      }
    }
    for(const item of result.items){
      const used = n(itemUsedArea[item.id]);
      const completed = n(result.completed[item.id]);
      const actualCost = result.usedArea > 0 ? n(result.totalCost) * used / result.usedArea : 0;
      const theoreticalCost = used * n(result.sheet.grammage) / 10000000 * n(result.sheet.priceKg);
      result.perItem[item.id] = {
        id:item.id, label:item.label, requested:item.isFiller ? completed : n(item.qty), completed,
        remaining:n(result.remaining[item.id]), usedArea:round(used, 6),
        theoreticalCost:round(theoreticalCost, 4), actualCost:round(actualCost, 4),
        paperCostPerBag:completed > 0 ? round(actualCost / completed, 4) : 0,
        theoreticalCostPerBag:completed > 0 ? round(theoreticalCost / completed, 4) : 0,
        modeCounts:modeCounts[item.id], isFiller:!!item.isFiller
      };
    }
    ['usedArea','wasteArea','wastePercent','wasteKg','wasteCost','utilization'].forEach(key => { result[key] = round(result[key], 4); });
    return result;
  }

  return {
    normalizeMode, normalizeItems, cutDimensions, allowedModes,
    buildPatternOnce, buildBestPattern, planSheet,
    generateCandidateSheets, suggestSheets, analyzeFillers, fillPatternGaps, applyFillers,
    sheetWeightKg:(length, width, grammage) => n(length) * n(width) * n(grammage) / 10000000
  };
});
