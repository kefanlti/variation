// ==UserScript==
// @name         Variation Dimensions 浮窗工具
// @namespace    https://github.com/variation-tool-float
// @version      7.1.0
// @description  在 Paragon case 页面以浮窗形式展示 P2C Variation Dimensions 查询结果
// @author       VTF
// @match        https://paragon-na.amazon.com/*
// @match        https://paragon-eu.amazon.com/*
// @match        https://paragon-fe.amazon.com/*
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_registerMenuCommand
// @grant        GM_xmlhttpRequest
// @connect      us-east-1-prod.catalog-data-nexus.iop.amazon.dev
// @run-at       document-idle
// ==/UserScript==

(function () {
  'use strict';

  /* ---- CBOR ---- */
  const CBOR = {
    encode(v) {
      const p = [];
      function e(v) {
        if (v === null || v === undefined) { p.push(0xf6); return; }
        if (typeof v === 'boolean') { p.push(v ? 0xf5 : 0xf4); return; }
        if (typeof v === 'number') { if (Number.isInteger(v) && v >= 0) u(0, v); else if (Number.isInteger(v) && v < 0) u(1, -1 - v); else { p.push(0xfb); const b = new ArrayBuffer(8); new DataView(b).setFloat64(0, v); for (const x of new Uint8Array(b)) p.push(x); } return; }
        if (typeof v === 'string') { const t = new TextEncoder().encode(v); u(3, t.length); for (const x of t) p.push(x); return; }
        if (Array.isArray(v)) { u(4, v.length); for (const x of v) e(x); return; }
        if (typeof v === 'object') { const k = Object.keys(v); u(5, k.length); for (const x of k) { e(x); e(v[x]); } }
      }
      function u(m, v) { const h = m << 5; if (v < 24) p.push(h | v); else if (v < 256) p.push(h | 24, v); else if (v < 65536) p.push(h | 25, v >> 8, v & 255); else p.push(h | 26, (v >> 24) & 255, (v >> 16) & 255, (v >> 8) & 255, v & 255); }
      e(v); return new Uint8Array(p);
    },
    decode(input) {
      const b = new Uint8Array(input instanceof ArrayBuffer ? input : new Uint8Array(input));
      const dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
      let o = 0;
      function ra(ai) { if (ai < 24) return ai; if (ai === 24) return dv.getUint8(o++); if (ai === 25) { const v = dv.getUint16(o); o += 2; return v; } if (ai === 26) { const v = dv.getUint32(o); o += 4; return v; } if (ai === 27) { const h = dv.getUint32(o); o += 4; const l = dv.getUint32(o); o += 4; return h * 4294967296 + l; } return -1; }
      function r() {
        if (o >= b.length) return undefined;
        const ib = dv.getUint8(o++); if (ib === 0xff) return undefined;
        const mj = ib >> 5, ai = ib & 31;
        switch (mj) {
          case 0: return ra(ai);
          case 1: return -1 - ra(ai);
          case 2: { const n = ra(ai); const s = b.slice(o, o + n); o += n; return s; }
          case 3: { const n = ra(ai); const s = new TextDecoder().decode(b.slice(o, o + n)); o += n; return s; }
          case 4: { const n = ra(ai); const a = []; if (n < 0) { while (o < b.length && b[o] !== 0xff) a.push(r()); if (o < b.length) o++; return a; } for (let i = 0; i < n; i++) a.push(r()); return a; }
          case 5: { const n = ra(ai); const m = {}; if (n < 0) { while (o < b.length && b[o] !== 0xff) { const k = r(); m[k] = r(); } if (o < b.length) o++; return m; } for (let i = 0; i < n; i++) { const k = r(); m[k] = r(); } return m; }
          case 6: { ra(ai); return r(); }
          case 7: { if (ai === 20) return false; if (ai === 21) return true; if (ai === 22) return null; if (ai === 23) return undefined; if (ai === 25) { o += 2; return 0; } if (ai === 26) { const f = dv.getFloat32(o); o += 4; return f; } if (ai === 27) { const f = dv.getFloat64(o); o += 8; return f; } return ai; }
        }
      }
      return r();
    }
  };

  /* ---- API ---- */
  const API = 'https://us-east-1-prod.catalog-data-nexus.iop.amazon.dev/service/CatalogDataNexusService/operation';
  function apiCall(op, body) {
    return new Promise((resolve, reject) => {
      const cbor = CBOR.encode(body);
      console.log('[VTF]', op, 'request:', body);
      GM_xmlhttpRequest({
        method: 'POST', url: API + '/' + op,
        headers: { 'Content-Type': 'application/cbor', 'smithy-protocol': 'rpc-v2-cbor', 'Accept': 'application/cbor' },
        data: new Blob([cbor], { type: 'application/cbor' }),
        responseType: 'arraybuffer', anonymous: false,
        onload(resp) {
          console.log('[VTF]', op, 'status:', resp.status, 'size:', resp.response ? resp.response.byteLength : 0);
          let d; try { d = CBOR.decode(new Uint8Array(resp.response)); } catch (e) { try { d = JSON.parse(new TextDecoder().decode(new Uint8Array(resp.response))); } catch (e2) { d = null; } }
          console.log('[VTF]', op, 'decoded:', d);
          resp.status >= 200 && resp.status < 300 ? resolve(d) : reject(new Error('API ' + resp.status + ': ' + (d ? JSON.stringify(d) : resp.statusText)));
        },
        onerror(e) { reject(new Error('Network error')); }
      });
    });
  }

  async function queryVariation(asin, mpId) {
    const sess = await apiCall('CreateSession', {});
    const sid = sess.sessionId;
    console.log('[VTF] sessionId:', sid);
    const src = await apiCall('CreateSourceFromDataView', { sessionId: sid, viewName: 'p2c_variation_dimensions_view', params: { item_id: asin, marketplace_id: mpId, stage: 'prod', caller: 'Harmony', v2_response_format: 'true', include_augmentation: 'true' }, title: 'Parent to Child Variation View' });
    console.log('[VTF] sourceId:', src.sourceId);
    const data = await apiCall('GetSource', { sessionId: sid, sourceId: src.sourceId, includeData: true });

    // Check if we got actual payload data
    var dataStr = typeof data.data === 'string' ? data.data : '';
    if (dataStr.indexOf('payload:[') >= 0) return data;

    // No payload - might be a child ASIN. Try to find parent.
    console.log('[VTF] No payload for', asin, '- looking for parent ASIN...');
    try {
      var relSrc = await apiCall('CreateSourceFromDataView', { sessionId: sid, viewName: 'c2p_item_relationships', params: { item_id: asin, marketplace_id: mpId, stage: 'prod', caller: 'Harmony', v2_response_format: 'true', include_augmentation: 'true' }, title: 'C2P Relationships' });
      var relData = await apiCall('GetSource', { sessionId: sid, sourceId: relSrc.sourceId, includeData: true });
      var relStr = typeof relData.data === 'string' ? relData.data : '';
      var pm = relStr.match(/parent_item_id:"([^"]+)"/) || relStr.match(/parent_item_id:([A-Z0-9]{10})/);
      if (pm && pm[1] && pm[1] !== asin) {
        console.log('[VTF] Found parent:', pm[1], '- re-querying...');
        var src2 = await apiCall('CreateSourceFromDataView', { sessionId: sid, viewName: 'p2c_variation_dimensions_view', params: { item_id: pm[1], marketplace_id: mpId, stage: 'prod', caller: 'Harmony', v2_response_format: 'true', include_augmentation: 'true' }, title: 'Parent to Child Variation View' });
        var data2 = await apiCall('GetSource', { sessionId: sid, sourceId: src2.sourceId, includeData: true });
        data2._vtf_parent = pm[1];
        return data2;
      }
    } catch (e) { console.log('[VTF] Parent lookup failed:', e.message); }
    return data;
  }

  /* ---- Parse response ---- */
  function parseData(resp) {
    if (!resp) return [];
    var text = typeof resp.data === 'string' ? resp.data : null;
    if (!text) {
      if (resp.data && typeof resp.data === 'object') {
        if (Array.isArray(resp.data)) return resp.data;
        if (resp.data.payload) return Array.isArray(resp.data.payload) ? resp.data.payload : [resp.data.payload];
      }
      console.log('[VTF] Unknown data format:', typeof resp.data);
      return [];
    }
    console.log('[VTF] Parsing ION text, length:', text.length);
    // Find payload:[ ... ] and extract each { } record
    var payloadStart = text.indexOf('payload:[');
    if (payloadStart < 0) {
      console.log('[VTF] No payload found, checking if empty result');
      // Check if this is a metadata-only response (no actual data rows)
      if (text.indexOf('field_labels') >= 0 && text.indexOf('raw_field_names') >= 0) {
        console.log('[VTF] Got metadata-only response - input may be a child ASIN, not parent');
        return [{_vtf_error: 'no_payload'}];
      }
      return [];
    }
    var inner = text.substring(payloadStart + 9);
    var rows = [], depth = 0, start = -1;
    for (var i = 0; i < inner.length; i++) {
      if (inner[i] === '{') { if (depth === 0) start = i; depth++; }
      else if (inner[i] === '}') { depth--; if (depth === 0 && start >= 0) { rows.push(inner.substring(start + 1, i)); start = -1; } }
      else if (inner[i] === ']' && depth === 0) break;
    }
    console.log('[VTF] Found', rows.length, 'records');
    var results = [];
    for (var ri = 0; ri < rows.length; ri++) {
      var record = rows[ri], obj = {}, pos = 0;
      while (pos < record.length) {
        while (pos < record.length && ',\n\r '.indexOf(record[pos]) >= 0) pos++;
        if (pos >= record.length) break;
        var ks = pos;
        while (pos < record.length && record[pos] !== ':') pos++;
        if (pos >= record.length) break;
        var key = record.substring(ks, pos).trim();
        pos++;
        while (pos < record.length && record[pos] === ' ') pos++;
        if (pos >= record.length) { if (key) obj[key] = ''; break; }
        var val = '';
        if (record[pos] === '"') {
          pos++; var vs = pos;
          while (pos < record.length) { if (record[pos] === '"' && record[pos - 1] !== '\\') break; pos++; }
          val = record.substring(vs, pos); pos++;
        } else if (record[pos] === '[') {
          var d = 1; pos++; var vs2 = pos;
          while (pos < record.length && d > 0) { if (record[pos] === '[') d++; if (record[pos] === ']') d--; pos++; }
          val = record.substring(vs2, pos - 1);
        } else if (record[pos] === '{') {
          var d2 = 1; pos++; var vs3 = pos;
          while (pos < record.length && d2 > 0) { if (record[pos] === '{') d2++; if (record[pos] === '}') d2--; pos++; }
          val = record.substring(vs3, pos - 1);
        } else {
          var vs4 = pos;
          while (pos < record.length && record[pos] !== ',' && record[pos] !== '\n') pos++;
          val = record.substring(vs4, pos).trim();
        }
        if (key) obj[key] = val;
      }
      if (Object.keys(obj).length > 0) results.push(obj);
    }
    console.log('[VTF] Parsed', results.length, 'records');
    // Extract variation dimensions from variation_dimension_order_value
    // ION format: [color_name::"Blue",size_name::"Large"] or color_name::"Blue"
    for (var i = 0; i < results.length; i++) {
      var vdov = results[i].variation_dimension_order_value || '';
      if (vdov) {
        // Parse patterns like: color_name::"Value" or size_name::"Value"
        var dimRe = /(\w+)::["']?([^"',\]]+)["']?/g;
        var dm;
        while ((dm = dimRe.exec(vdov)) !== null) {
          var dimKey = dm[1]; // e.g. color_name, size_name
          var dimVal = dm[2]; // e.g. Blue, Large
          if (!results[i][dimKey]) results[i][dimKey] = dimVal;
        }
        // Also set variation_theme from the dimension keys
        if (!results[i].variation_theme) {
          var themes = [];
          var thRe = /(\w+)::/g;
          var thm;
          while ((thm = thRe.exec(vdov)) !== null) {
            if (themes.indexOf(thm[1]) < 0) themes.push(thm[1]);
          }
          if (themes.length > 0) results[i].variation_theme = themes.join(', ');
        }
      }
    }
    if (results.length > 0) console.log('[VTF] First record keys:', Object.keys(results[0]).join(', '));
    if (results.length > 0) console.log('[VTF] First record:', JSON.stringify(results[0]).substring(0, 1000));
    return results;
  }

  /* ---- Config ---- */
  const COLS = [['parent_item_id','Parent Item'],['child_item_id','Child Item'],['color_name','Color'],['color_map','Color Map'],['size_name','Size'],['size_map','Size Map'],['variation_theme','Variation Theme'],['parentage_level','Parentage Level'],['relationship_type','Relationship Type'],['state','State'],['refusal_reason','Refused Reason'],['title','Child Item Name'],['brand','Brand'],['global_catalog_owner_id','gcor_id'],['item_classification','Item Classification'],['product_type','Product Type'],['sort_heuristic','Sort Heuristic'],['quantity','Quantity'],['item_weight','Item Weight'],['department_name','Department Name'],['material_type','Material Type'],['style_name','Style Name'],['country_of_origin','Country of Origin'],['description','Description'],['physicalID','physicalID']];
  const DEF_VIS = ['color_name','size_name','variation_theme','title','brand','product_type'];
  const FIXED_COLS = ['parent_item_id','child_item_id','state','refusal_reason'];
  const MPS = [['1','US'],['3','UK'],['4','DE'],['5','FR'],['35691','IT'],['44551','ES'],['6','JP'],['7','CA'],['111172','AU'],['44571','IN'],['771770','MX'],['526970','BR']];
  let visCols = GM_getValue('vtf_cols', DEF_VIS), lastData = null, querying = false;

  /* ---- Styles ---- */
  const CSS = '@keyframes vtfSlide{from{transform:translateY(10px);opacity:0}to{transform:translateY(0);opacity:1}}@keyframes vtfSpin{from{transform:rotate(0)}to{transform:rotate(360deg)}}#vtf-btn{position:fixed;bottom:80px;right:20px;z-index:2147483646;width:46px;height:46px;border-radius:50%;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;font-size:20px;display:flex;align-items:center;justify-content:center;cursor:pointer;box-shadow:0 4px 14px rgba(102,126,234,.45);transition:transform .2s;user-select:none;border:none}#vtf-btn:hover{transform:scale(1.12)}#vtf-btn.active{background:linear-gradient(135deg,#f093fb,#f5576c)}#vtf-win{position:fixed;z-index:2147483646;background:#fff;border-radius:10px;box-shadow:0 12px 48px rgba(0,0,0,.28);display:none;flex-direction:column;overflow:hidden;animation:vtfSlide .25s ease;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;font-size:13px;color:#333;min-width:600px;min-height:400px}.vtf-bar{display:flex;align-items:center;justify-content:space-between;padding:8px 14px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;cursor:move;user-select:none;flex-shrink:0}.vtf-bar .t{font-size:13px;font-weight:600}.vtf-bar .c{display:flex;gap:6px}.vtf-bar .c button{background:rgba(255,255,255,.2);border:none;color:#fff;width:26px;height:26px;border-radius:6px;cursor:pointer;font-size:12px;display:flex;align-items:center;justify-content:center}.vtf-bar .c button:hover{background:rgba(255,255,255,.35)}.vtf-input{padding:10px 14px;background:#f8f9fb;border-bottom:1px solid #e8e8e8;display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap;flex-shrink:0;position:relative}.vtf-input label{font-size:11px;color:#666;display:flex;flex-direction:column;gap:3px}.vtf-input input,.vtf-input select{padding:6px 8px;border:1px solid #d0d5dd;border-radius:5px;font-size:12px;outline:none}.vtf-input input:focus,.vtf-input select:focus{border-color:#667eea}.vtf-input input{width:120px}.vtf-qbtn{padding:6px 16px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border:none;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap}.vtf-qbtn:hover{opacity:.85}.vtf-qbtn:disabled{opacity:.5;cursor:not-allowed}.vtf-colbtn{padding:6px 10px;background:#fff;border:1px solid #d0d5dd;border-radius:5px;font-size:11px;cursor:pointer;white-space:nowrap;color:#555}.vtf-colbtn:hover{background:#f0f2ff;border-color:#667eea}.vtf-cp{position:absolute;top:100%;left:0;right:0;z-index:10;background:#fff;border:1px solid #e0e0e0;border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.15);padding:12px;max-height:300px;overflow-y:auto;display:none}.vtf-cp.show{display:block}.vtf-cp .vtf-cols-grid{display:flex;flex-wrap:wrap}.vtf-cp label{display:inline-flex;align-items:center;gap:6px;padding:3px 6px;border-radius:4px;cursor:pointer;font-size:12px;width:calc(16.66% - 4px);box-sizing:border-box;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.vtf-cp label:hover{background:#f0f2ff}.vtf-cp input[type=checkbox]{margin:0}.vtf-ca{display:flex;gap:8px;margin-bottom:8px;padding-bottom:8px;border-bottom:1px solid #eee}.vtf-ca button{padding:3px 8px;border:1px solid #d0d5dd;border-radius:4px;background:#fff;font-size:11px;cursor:pointer}.vtf-ca button:hover{background:#f0f2ff}.vtf-result{flex:1;overflow:auto;position:relative}.vtf-table{width:100%;border-collapse:collapse;font-size:12px}.vtf-table th{position:sticky;top:0;background:#f0f2ff;padding:7px 10px;text-align:left;font-weight:600;font-size:11px;color:#555;border-bottom:2px solid #667eea;white-space:nowrap;z-index:1}.vtf-table td{padding:5px 10px;border-bottom:1px solid #f0f0f0;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.vtf-table tr:hover td{background:#f8f9ff}.vtf-status{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px;color:#888;gap:10px}.vtf-spin{width:24px;height:24px;border:3px solid #e0e0e0;border-top-color:#667eea;border-radius:50%;animation:vtfSpin .8s linear infinite}.vtf-err{color:#e53e3e;text-align:center;padding:20px}.vtf-info{padding:6px 14px;background:#f8f9fb;border-top:1px solid #e8e8e8;font-size:11px;color:#888;display:flex;justify-content:space-between;flex-shrink:0}.vtf-rz{position:absolute;bottom:0;right:0;width:16px;height:16px;cursor:nwse-resize;background:linear-gradient(135deg,transparent 50%,rgba(102,126,234,.3) 50%)}';

  function injectStyles() { if (document.getElementById('vtf-s')) return; const s = document.createElement('style'); s.id = 'vtf-s'; s.textContent = CSS; document.head.appendChild(s); }
  function extractAsin() { const m = (document.body.innerText || '').match(/\b(B[0-9A-Z]{9})\b/); return m ? m[1] : ''; }
  function renderTable(data, cols) {
    if (!data || !data.length) return '<div class="vtf-status">无数据</div>';
    var allCols = FIXED_COLS.concat(cols.filter(function(c) { return FIXED_COLS.indexOf(c) < 0; }));
    const ths = allCols.map(id => { const c = COLS.find(x => x[0] === id); return '<th>' + (c ? c[1] : id) + '</th>'; }).join('');
    const trs = data.map(row => '<tr>' + allCols.map(id => { let v = row[id]; if (v == null) v = ''; if (typeof v === 'object') v = JSON.stringify(v); return '<td title="' + String(v).replace(/"/g, '&quot;') + '">' + String(v).replace(/</g, '&lt;') + '</td>'; }).join('') + '</tr>').join('');
    return '<table class="vtf-table"><thead><tr>' + ths + '</tr></thead><tbody>' + trs + '</tbody></table>';
  }

  /* ---- UI ---- */
  function createUI() {
    if (window.self !== window.top || document.getElementById('vtf-btn')) return;
    injectStyles();
    const W = GM_getValue('vtf_w', 1100), H = GM_getValue('vtf_h', 600), pos = GM_getValue('vtf_pos', null);
    const pageAsin = extractAsin();
    const mpOpts = MPS.map(function(m) { return '<option value="' + m[0] + '"' + (m[0] === '1' ? ' selected' : '') + '>' + m[1] + '</option>'; }).join('');
    const btn = document.createElement('div'); btn.id = 'vtf-btn'; btn.innerHTML = '🔀'; btn.title = 'Variation Dimensions (Ctrl+Shift+V)';
    const win = document.createElement('div'); win.id = 'vtf-win'; win.style.width = W + 'px'; win.style.height = H + 'px';
    if (pos) { win.style.left = pos.l + 'px'; win.style.top = pos.t + 'px'; } else { win.style.left = Math.max(0, (innerWidth - W) / 2) + 'px'; win.style.top = Math.max(0, (innerHeight - H) / 2) + 'px'; }
    win.innerHTML = '<div class="vtf-bar"><span class="t">🔀 Variation Dimensions</span><div class="c"><button id="vtf-x" title="关闭">✕</button></div></div><div class="vtf-input"><label>ASIN<input id="vtf-asin" value="' + pageAsin + '" spellcheck="false" placeholder="B0XXXXXXXXX"/></label><label>Marketplace<select id="vtf-mp">' + mpOpts + '</select></label><button class="vtf-qbtn" id="vtf-go">🔍 查询</button><button class="vtf-colbtn" id="vtf-ct">⚙ Columns</button><div class="vtf-cp" id="vtf-cp"></div></div><div class="vtf-result" id="vtf-r"><div class="vtf-status">输入 ASIN 和 Marketplace，点击查询</div></div><div class="vtf-info" id="vtf-i"><span></span><span></span></div><div class="vtf-rz" id="vtf-rz"></div>';
    document.body.appendChild(btn); document.body.appendChild(win);
    var q = function(s) { return win.querySelector(s); };
    var asinIn = q('#vtf-asin'), mpSel = q('#vtf-mp'), goBtn = q('#vtf-go'), result = q('#vtf-r'), info = q('#vtf-i'), cp = q('#vtf-cp'), ct = q('#vtf-ct');

    /* Columns */
    var cpOpen = false;
    function renderCP() {
      var html = '<div class="vtf-ca"><button id="vtf-ca">全选</button><button id="vtf-cn">全不选</button><button id="vtf-cd">默认</button></div><div style="font-size:11px;color:#888;margin-bottom:8px">固定列: Parent Item, Child Item, State, Refused Reason</div><div class="vtf-cols-grid">';
      COLS.forEach(function(c) { if (FIXED_COLS.indexOf(c[0]) >= 0) return; html += '<label title="' + c[1] + '"><input type="checkbox" value="' + c[0] + '"' + (visCols.indexOf(c[0]) >= 0 ? ' checked' : '') + '>' + c[1] + '</label>'; });
      html += '</div>';
      cp.innerHTML = html;
      cp.querySelectorAll('input[type=checkbox]').forEach(function(cb) { cb.addEventListener('change', function() { visCols = Array.from(cp.querySelectorAll('input:checked')).map(function(x) { return x.value; }); GM_setValue('vtf_cols', visCols); if (lastData) result.innerHTML = renderTable(lastData, visCols); }); });
      q('#vtf-ca').addEventListener('click', function() { visCols = COLS.map(function(c) { return c[0]; }).filter(function(c) { return FIXED_COLS.indexOf(c) < 0; }); GM_setValue('vtf_cols', visCols); renderCP(); if (lastData) result.innerHTML = renderTable(lastData, visCols); });
      q('#vtf-cn').addEventListener('click', function() { visCols = []; GM_setValue('vtf_cols', visCols); renderCP(); if (lastData) result.innerHTML = renderTable(lastData, visCols); });
      q('#vtf-cd').addEventListener('click', function() { visCols = DEF_VIS.slice(); GM_setValue('vtf_cols', visCols); renderCP(); if (lastData) result.innerHTML = renderTable(lastData, visCols); });
    }
    ct.addEventListener('click', function(e) { e.stopPropagation(); cpOpen = !cpOpen; if (cpOpen) { renderCP(); cp.classList.add('show'); } else cp.classList.remove('show'); });
    document.addEventListener('click', function(e) { if (!cp.contains(e.target) && e.target !== ct) { cp.classList.remove('show'); cpOpen = false; } });

    /* Query */
    goBtn.addEventListener('click', doQuery);
    asinIn.addEventListener('keydown', function(e) { if (e.key === 'Enter') doQuery(); });
    async function doQuery() {
      var asin = asinIn.value.trim().toUpperCase();
      if (!asin || querying) { asinIn.focus(); return; }
      querying = true; goBtn.disabled = true; goBtn.textContent = '⏳ 查询中...';
      result.innerHTML = '<div class="vtf-status"><div class="vtf-spin"></div><span>正在查询...</span></div>';
      info.innerHTML = '<span></span><span></span>';
      var t0 = Date.now();
      try {
        var resp = await queryVariation(asin, mpSel.value);
        var data = parseData(resp);
        var elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        if (Array.isArray(data) && data.length > 0) {
          if (data[0]._vtf_error === 'no_payload') {
            result.innerHTML = '<div class="vtf-status" style="gap:12px"><div style="font-size:24px">⚠️</div><div>该 ASIN 没有返回变体子项数据</div><div style="font-size:11px;color:#999">请确认输入的是 <b>Parent ASIN</b>（父ASIN）<br>如果输入的是 Child ASIN，请改用其 Parent ASIN 查询</div></div>';
          } else {
            lastData = data; result.innerHTML = renderTable(data, visCols);
            var infoText = asin;
            if (resp._vtf_parent) infoText = asin + ' → Parent: ' + resp._vtf_parent;
            info.innerHTML = '<span>' + data.length + ' 条记录 · ' + elapsed + 's</span><span>' + infoText + '</span>';
          }
        }
        else { result.innerHTML = '<div class="vtf-status">该 ASIN 没有 Variation 子项数据</div>'; console.log('[VTF] Raw:', resp); }
      } catch (err) { console.error('[VTF] Error:', err); result.innerHTML = '<div class="vtf-err">❌ ' + err.message + '</div>'; }
      finally { querying = false; goBtn.disabled = false; goBtn.textContent = '🔍 查询'; }
    }

    /* Toggle */
    var isOpen = false, btnMoved = false;
    function toggle() { isOpen = !isOpen; win.style.display = isOpen ? 'flex' : 'none'; btn.classList.toggle('active', isOpen); if (isOpen) setTimeout(function() { asinIn.focus(); }, 50); }
    function close() { isOpen = false; win.style.display = 'none'; btn.classList.remove('active'); }
    btn.addEventListener('click', function() { if (!btnMoved) toggle(); btnMoved = false; });
    q('#vtf-x').addEventListener('click', close);

    /* Drag */
    var bar = win.querySelector('.vtf-bar'), drag = false, dsx, dsy, dwx, dwy;
    bar.addEventListener('mousedown', function(e) { if (e.target.closest('.c')) return; drag = true; dsx = e.clientX; dsy = e.clientY; var r = win.getBoundingClientRect(); dwx = r.left; dwy = r.top; e.preventDefault(); });
    document.addEventListener('mousemove', function(e) { if (drag) { win.style.left = (dwx + e.clientX - dsx) + 'px'; win.style.top = (dwy + e.clientY - dsy) + 'px'; } });
    document.addEventListener('mouseup', function() { if (drag) { drag = false; var r = win.getBoundingClientRect(); GM_setValue('vtf_pos', { l: r.left, t: r.top }); } });

    /* Resize */
    var rz = q('#vtf-rz'), rsz = false, rx, ry, rw, rh;
    rz.addEventListener('mousedown', function(e) { rsz = true; rx = e.clientX; ry = e.clientY; rw = win.offsetWidth; rh = win.offsetHeight; e.preventDefault(); e.stopPropagation(); });
    document.addEventListener('mousemove', function(e) { if (rsz) { win.style.width = Math.max(600, rw + e.clientX - rx) + 'px'; win.style.height = Math.max(400, rh + e.clientY - ry) + 'px'; } });
    document.addEventListener('mouseup', function() { if (rsz) { rsz = false; GM_setValue('vtf_w', win.offsetWidth); GM_setValue('vtf_h', win.offsetHeight); } });

    /* Button drag */
    btn.addEventListener('mousedown', function(e) { btnMoved = false; var ox = e.clientX - btn.getBoundingClientRect().left, oy = e.clientY - btn.getBoundingClientRect().top; function onM(ev) { btnMoved = true; btn.style.right = 'auto'; btn.style.bottom = 'auto'; btn.style.left = (ev.clientX - ox) + 'px'; btn.style.top = (ev.clientY - oy) + 'px'; } function onU() { document.removeEventListener('mousemove', onM); document.removeEventListener('mouseup', onU); } document.addEventListener('mousemove', onM); document.addEventListener('mouseup', onU); });
    btn.addEventListener('click', function(e) { if (btnMoved) { btnMoved = false; e.stopImmediatePropagation(); } }, true);

    /* Shortcuts */
    document.addEventListener('keydown', function(e) { if (e.ctrlKey && e.shiftKey && (e.key === 'V' || e.key === 'v')) { e.preventDefault(); toggle(); } if (e.key === 'Escape' && isOpen) close(); });
  }

  GM_registerMenuCommand('🔀 打开 Variation 浮窗', function() { var b = document.getElementById('vtf-btn'); if (b) b.click(); });
  GM_registerMenuCommand('⚙️ 重置设置', function() { GM_setValue('vtf_pos', null); GM_setValue('vtf_w', 1100); GM_setValue('vtf_h', 600); GM_setValue('vtf_cols', DEF_VIS); alert('已重置'); });

  if (window.self === window.top) { createUI(); console.log('🔀 Variation Dimensions v7.1 loaded'); }
})();
