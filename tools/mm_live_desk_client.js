/* MM Live Desk UI — served as /mm_live_desk_client.js by mm_live_desk_core.py (edit here; not inlined in .py). */
try {
  if (typeof window.mmDeskDismissBoot === "function") window.mmDeskDismissBoot();
} catch (e) {}
function api(path, opt, timeoutMs) {
  const ms = timeoutMs || 60000;
  const url = path.charAt(0) === "/" ? (window.location.origin + path) : path;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  return fetch(url, Object.assign({ cache: "no-store" }, opt || {}, { signal: ctrl.signal }))
    .finally(() => clearTimeout(t))
    .then(async r => {
      const ct = r.headers.get("content-type") || "";
      let body = null;
      if (ct.includes("json")) {
        try {
          body = await r.json();
        } catch (e) {
          body = null;
        }
      }
      if (!r.ok) {
        const detail =
          body &&
          (body.message ||
            body.error ||
            body.detail ||
            (typeof body === "string" ? body : ""));
        const err = new Error(detail ? String(detail) : path + " HTTP " + r.status);
        // Attach status + parsed body so callers can act on structured errors (e.g. the
        // Item 3/4 needs_resume_confirm 409) instead of only seeing the message string.
        err.status = r.status;
        err.body = body;
        throw err;
      }
      return body;
    });
}
/**
 * Item 3/4 — resume-confirm wrapper for every desk->venue place/add.
 * POSTs `payload` to `path`. If the server refuses because the instrument is stopped
 * (HTTP 409 with `needs_resume_confirm`), ask the operator ONCE ("<ax> is stopped —
 * resume quoting?"). On confirm, re-POST the SAME request with `resume:true` so the
 * server flips the flag (flag-only, no re-placement) and then places the new order(s).
 * On decline, throw a tagged error and place NOTHING. Never silently places or resumes.
 */
async function mmDeskPostWithResumeConfirm(path, payload, timeoutMs) {
  var opt = { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) };
  try {
    return await api(path, opt, timeoutMs);
  } catch (e) {
    if (!(e && e.status === 409 && e.body && e.body.needs_resume_confirm)) throw e;
    var ax = (e.body && e.body.ax_symbol) || payload.ax_symbol || payload.symbol || "this instrument";
    var ok = window.confirm(
      ax + " is stopped — resume quoting?\n\n" +
      "This re-enables quoting for " + ax + " and then places your new order(s). " +
      "It does NOT re-place any previously cancelled orders."
    );
    if (!ok) {
      var cancelled = new Error(ax + " is stopped — resume cancelled. Nothing placed.");
      cancelled.__resumeCancelled = true;
      throw cancelled;
    }
    var p2 = Object.assign({}, payload, { resume: true });
    var opt2 = { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p2) };
    return await api(path, opt2, timeoutMs);
  }
}
/** Sidebar strategy fields are read-only divs; support both div textContent and legacy input.value. */
function deskRoContent(el) {
  if (!el) return "";
  var tag = el.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return String(el.value != null ? el.value : "").trim();
  return String(el.textContent != null ? el.textContent : "").trim();
}
function deskRoSet(el, val) {
  if (!el) return;
  var v = val != null ? String(val) : "";
  var tag = el.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") el.value = v;
  else el.textContent = v;
}
function deskRoSetById(id, val) {
  deskRoSet(document.getElementById(id), val);
}
function bootDecimals(tick) {
  const t = typeof tick === "number" ? tick : parseFloat(tick);
  if (!isFinite(t) || t <= 0) return 5;
  return Math.max(2, Math.min(8, -Math.floor(Math.log10(t))));
}
/**
 * market_maker.instruments[].tick_size (AX perp quote tick) — required when products differ
 * (e.g. JPYUSD 1e-6 vs a global FX tick). Using the global for every row collapses the whole
 * ladder to one display price.
 */
function tickForAxSymbol(s, sym) {
  const key = String(sym || "").trim();
  const rows = Array.isArray(s && s.mm_instruments) ? s.mm_instruments : [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (!r || typeof r !== "object") continue;
    const k = String(r.symbol || r.ax_symbol || "").trim();
    if (!k) continue;
    if (k === key || k.toUpperCase() === key.toUpperCase()) {
      for (const fld of ["tick_size", "tick", "price_tick"]) {
        const ts = r[fld];
        if (ts != null && isFinite(Number(ts)) && Number(ts) > 0) {
          return Number(ts);
        }
      }
      break;
    }
  }
  return tickFromState(s);
}
/** reference_fix_symbol / theo key (e.g. USD/JPY) → same leg tick as the AX row, for Ref column. */
function tickForRefSymbol(s, refSym) {
  const key = String(refSym || "").trim();
  if (!key) {
    return tickFromState(s);
  }
  const rows = Array.isArray(s && s.mm_instruments) ? s.mm_instruments : [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (!r || typeof r !== "object") continue;
    const ref = String(r.reference_fix_symbol || r.theo_symbol || "").trim();
    if (!ref) {
      continue;
    }
    if (ref === key || ref.toUpperCase() === key.toUpperCase()) {
      for (const fld of ["tick_size", "tick", "price_tick"]) {
        const ts = r[fld];
        if (ts != null && isFinite(Number(ts)) && Number(ts) > 0) {
          return Number(ts);
        }
      }
      break;
    }
  }
  return tickFromState(s);
}
function decimalsForAxSymbol(s, sym) {
  return bootDecimals(tickForAxSymbol(s, sym));
}
function decimalsForRefSymbol(s, refSym) {
  return bootDecimals(tickForRefSymbol(s, refSym));
}
/** Wrap ladder table so tall books scroll inside the card instead of stretching the page. */
function depthAppendOrderbookTable(wrap, tb) {
  var sc = document.createElement("div");
  sc.className = "depth-book-table-wrap";
  sc.appendChild(tb);
  wrap.appendChild(sc);
}
/** If *_all_books maps are empty but book_* has rows (older snapshot or race), synthesize maps so depth grids render. */
function normalizeStateForUi(s) {
  if (!s || typeof s !== "object") return s;
  if (!s.ref_all_books || typeof s.ref_all_books !== "object") s.ref_all_books = {};
  if (!s.ax_all_books || typeof s.ax_all_books !== "object") s.ax_all_books = {};
  var bns = String(s.ref_symbol || "").trim().toUpperCase();
  var axs = String(s.ax_symbol || "").trim();
  var bnHasLevels = Object.keys(s.ref_all_books).some(function(k) {
    var d = s.ref_all_books[k];
    return d && !d._err && ((d.bids && d.bids.length) || (d.asks && d.asks.length));
  });
  var bb = (s.book_reference && s.book_reference.bids) || [];
  var ba = (s.book_reference && s.book_reference.asks) || [];
  if (bns && !bnHasLevels && (bb.length || ba.length))
    s.ref_all_books[bns] = { bids: bb.slice(), asks: ba.slice() };
  var axHasLevels = Object.keys(s.ax_all_books).some(function(k) {
    var d = s.ax_all_books[k];
    return d && !d._err && ((d.bids && d.bids.length) || (d.asks && d.asks.length));
  });
  var ab = (s.book_ax && s.book_ax.bids) || [];
  var aa = (s.book_ax && s.book_ax.asks) || [];
  if (axs && !axHasLevels && (ab.length || aa.length))
    s.ax_all_books[axs] = { bids: ab.slice(), asks: aa.slice() };
  if (!Array.isArray(s.ref_book_symbols) || !s.ref_book_symbols.length) {
    /* Neon: multi-symbol books follow external_feed.fix.md_symbols — keep grid in sync if server omitted ref_book_symbols. */
    if (String(s.reference_provider || "") === "neon_fix" && Array.isArray(s.neon_fix_md_symbols) && s.neon_fix_md_symbols.length) {
      s.ref_book_symbols = s.neon_fix_md_symbols.map(String);
    } else if (bns) {
      s.ref_book_symbols = [bns];
    }
  }
  if (!Array.isArray(s.ax_book_symbols) || !s.ax_book_symbols.length) {
    if (axs) s.ax_book_symbols = [axs];
  }
  if (!Array.isArray(s.ax_grid_symbols) || !s.ax_grid_symbols.length) {
    s.ax_grid_symbols = (s.ax_book_symbols || []).slice();
  }
  if (typeof s.show_reference_depth_col === "undefined") {
    s.show_reference_depth_col = (s.web_desk_reference_feed === true) || (s.reference_provider === "neon_fix");
  }
  return s;
}
/**
 * Master AX product order for ladders and MM leg dropdowns:
 * **tick order in Architect depth multiselect** when any rows are selected (even before Apply),
 * else persisted ``ax_book_symbols``, else ``ax_grid_symbols``.
 */
function mmDeskMasterAxSymbolsOrderedFromUi(st) {
  st = st || {};
  var sel = document.getElementById("axOrderbookMulti");
  if (sel && sel.options && sel.options.length) {
    var picked = [];
    var seenP = {};
    for (var pi = 0; pi < sel.options.length; pi++) {
      if (!sel.options[pi].selected) continue;
      var pv = String(sel.options[pi].value || "").trim();
      if (!pv) continue;
      var pU = pv.toUpperCase();
      if (seenP[pU]) continue;
      seenP[pU] = true;
      picked.push(pv);
    }
    if (picked.length) return picked;
  }
  var abs = (Array.isArray(st.ax_book_symbols) && st.ax_book_symbols.length)
    ? st.ax_book_symbols.map(String)
    : [];
  abs = abs.map(function(x) { return String(x || "").trim(); }).filter(Boolean);
  if (abs.length) return abs;
  var ag = (Array.isArray(st.ax_grid_symbols) && st.ax_grid_symbols.length) ? st.ax_grid_symbols.map(String) : [];
  return ag.map(function(x) { return String(x || "").trim(); }).filter(Boolean);
}
function mmDeskLegRawRowsFromMaster(st, master) {
  var raw = [];
  var mm = (st && st.mm_instruments && st.mm_instruments.slice()) || [];
  var ref0 = String((st && st.ref_symbol != null ? st.ref_symbol : "") || "").trim();
  var seenM = {};
  for (var mi = 0; mi < master.length; mi++) {
    var axm = String(master[mi] || "").trim();
    if (!axm) continue;
    var um = axm.toUpperCase();
    if (seenM[um]) continue;
    seenM[um] = true;
    var rowM = null;
    for (var mj = 0; mj < mm.length; mj++) {
      var ir = mm[mj];
      if (!ir || typeof ir !== "object") continue;
      var sym = String(ir.symbol || ir.ax_symbol || "").trim();
      if (sym && sym.toUpperCase() === um) {
        rowM = ir;
        break;
      }
    }
    if (rowM) {
      raw.push({
        ax: String(rowM.symbol || rowM.ax_symbol || axm),
        ref: String(rowM.reference_fix_symbol || rowM.theo_symbol || ""),
        theo_source: String(rowM.theo_source || ""),
      });
    } else {
      raw.push({ ax: axm, ref: ref0, theo_source: "" });
    }
  }
  return raw;
}
/**
 * Single canonical list for header picker + modal prompts — same order as
 * :func:`mmDeskMasterAxSymbolsOrderedFromUi` × ``mm_instruments`` metadata; else server snapshot
 * ``theo_leg_choices``; else raw MM legs.
 */
function mmDeskUnifiedLegRows(st) {
  st = st || {};
  var out = [];
  var raw = [];
  var masterUi = mmDeskMasterAxSymbolsOrderedFromUi(st);
  if (masterUi.length) {
    raw = mmDeskLegRawRowsFromMaster(st, masterUi);
  } else if (st && Array.isArray(st.theo_leg_choices) && st.theo_leg_choices.length) {
    raw = st.theo_leg_choices.slice();
  } else if (st && Array.isArray(st.mm_instruments)) {
    for (var ii = 0; ii < st.mm_instruments.length; ii++) {
      var ir = st.mm_instruments[ii];
      if (!ir || typeof ir !== "object") continue;
      raw.push({
        ax: String(ir.symbol || ir.ax_symbol || ""),
        ref: String(ir.reference_fix_symbol || ir.theo_symbol || ""),
        theo_source: String(ir.theo_source || ""),
      });
    }
  }
  for (var j = 0; j < raw.length; j++) {
    var row = raw[j] || {};
    var ax = String(row.ax || "").trim();
    if (!ax) continue;
    out.push({
      ax: ax,
      ref: String(row.ref || "").trim(),
      theo_source: String(row.theo_source || "").trim(),
    });
  }
  if (!out.length) {
    var axF = String((st && st.ax_symbol) || "").trim();
    if (!axF) axF = String(deskRoContent(document.getElementById("axSym")) || "").trim();
    var refF = String((st && st.ref_symbol) || "").trim();
    if (!refF) refF = String(deskRoContent(document.getElementById("refSym")) || "").trim();
    if (axF) out.push({ ax: axF, ref: refF, theo_source: "" });
  }
  return out;
}
function mmDeskFormatLegOptionLabel(r) {
  var ax = String((r && r.ax) || "").trim();
  var ref = String((r && r.ref) || "").trim();
  var src = String((r && r.theo_source) || "").trim();
  if (ax && ref) return ax + " — " + ref + (src ? " (" + src + ")" : "");
  return ax || ref || "—";
}
/** AX depth card order: exactly the polled config list (ax_grid_symbols / ax_book_symbols) — no extra MM-leg prepend. */
function mmDeskAxBookGridOrder(s) {
  var books = (s && s.ax_all_books) || {};
  var gridSyms = (s && Array.isArray(s.ax_grid_symbols) && s.ax_grid_symbols.length)
    ? s.ax_grid_symbols.slice()
    : (s && Array.isArray(s.ax_book_symbols) ? s.ax_book_symbols.slice() : []);
  if (gridSyms.length) return gridSyms;
  return Object.keys(books).sort();
}
/** Uppercase set: multiselect ticks if any, else server ax_book_symbols (keeps ladder outline in sync). */
function mmDeskAxDepthSelectedUpperSetFromUi(s) {
  var out = {};
  var sel = document.getElementById("axOrderbookMulti");
  if (sel && sel.options && sel.options.length) {
    var any = false;
    for (var i = 0; i < sel.options.length; i++) {
      if (sel.options[i].selected) {
        any = true;
        var u = String(sel.options[i].value || "").trim().toUpperCase();
        if (u) out[u] = true;
      }
    }
    if (any) return out;
  }
  var abs = (s && Array.isArray(s.ax_book_symbols)) ? s.ax_book_symbols : [];
  for (var j = 0; j < abs.length; j++) {
    var u2 = String(abs[j] || "").trim().toUpperCase();
    if (u2) out[u2] = true;
  }
  return out;
}
var MM_DESK_AX_DEPTH_HIDDEN_LS = "mm_desk_architect_depth_hidden_syms_v1";
function mmDeskAxDepthHiddenGet() {
  try {
    var raw = localStorage.getItem(MM_DESK_AX_DEPTH_HIDDEN_LS);
    if (!raw) return [];
    var j = JSON.parse(raw);
    return Array.isArray(j) ? j.map(String) : [];
  } catch (e) {
    return [];
  }
}
function mmDeskAxDepthHiddenSet(arr) {
  try {
    localStorage.setItem(MM_DESK_AX_DEPTH_HIDDEN_LS, JSON.stringify(arr));
  } catch (e) {}
}
function mmDeskAxDepthHiddenAdd(sym) {
  var k = String(sym || "").trim();
  if (!k) return;
  var u = k.toUpperCase();
  var cur = mmDeskAxDepthHiddenGet();
  var have = {};
  for (var i = 0; i < cur.length; i++) have[String(cur[i] || "").trim().toUpperCase()] = true;
  if (have[u]) return;
  cur.push(k);
  mmDeskAxDepthHiddenSet(cur);
}
function mmDeskAxDepthHiddenClear() {
  try {
    localStorage.removeItem(MM_DESK_AX_DEPTH_HIDDEN_LS);
  } catch (e) {}
}
function mmDeskAxDepthHiddenUpperSet() {
  var arr = mmDeskAxDepthHiddenGet();
  var o = {};
  for (var i = 0; i < arr.length; i++) {
    var u = String(arr[i] || "").trim().toUpperCase();
    if (u) o[u] = true;
  }
  return o;
}
/** Reference depth column: Neon always (C++ file or Python feed); REST only when web_desk_reference_feed. */
function showReferenceDepthCol(s) {
  if (!s || typeof s !== "object") return false;
  if (s.show_reference_depth_col === true) return true;
  if (String(s.reference_provider || "") === "neon_fix") return true;
  if (s.web_desk_reference_feed === true) return true;
  return false;
}
window._axBookPrev = window._axBookPrev || {};
const AX_GRID_ROWS = 20;
const BN_GRID_ROWS = 20;
function axMineList(s, sym) {
  if (!s || !s.mine_by_symbol) return [];
  const k = String(sym || "").trim();
  const mbs = s.mine_by_symbol;
  let m = mbs[k];
  if (m == null && k) {
    const ku = k.toUpperCase();
    for (var mk in mbs) {
      if (mbs.hasOwnProperty(mk) && String(mk || "").trim().toUpperCase() === ku) {
        m = mbs[mk];
        break;
      }
    }
  }
  return Array.isArray(m) ? m.map(Number) : [];
}
function tickFromState(s) {
  const t = s.price_tick;
  return (typeof t === "number" && isFinite(t) && t > 0) ? t : 1e-5;
}
/**
 * Smallest **consecutive** price gap in the first rows (top-of-book can skip ticks — using only
 * [0]−[1] often yields a huge step and keeps a coarse declared tick).
 */
function inferBookPriceStepScan(side, maxPairs) {
  if (!side || side.length < 2) return null;
  const lim = Math.min(side.length - 1, maxPairs || 24);
  let best = null;
  for (let i = 0; i < lim; i++) {
    const d = Math.abs(Number(side[i][0]) - Number(side[i + 1][0]));
    if (!isFinite(d) || d <= 0) continue;
    if (best === null || d < best) best = d;
  }
  return best;
}
/**
 * JPYUSD-style perps need ~1e-6 quote tick; global price_tick is often 1e-4 — coarse rounding makes
 * almost every ladder level match one stored order (whole ladder yellow).
 */
function effectiveTickForMineMatch(declaredTick, bids, asks) {
  const b = inferBookPriceStepScan(bids, 24);
  const a = inferBookPriceStepScan(asks, 24);
  let fine = null;
  if (b && a) fine = Math.min(b, a);
  else fine = b || a;
  let refPx = NaN;
  if (bids && bids.length) refPx = Number(bids[0][0]);
  if (!isFinite(refPx) && asks && asks.length) refPx = Number(asks[0][0]);
  /* e.g. JPYUSD-PERP ~0.0063 — not EUR-sized ticks */
  const microFx = isFinite(refPx) && refPx > 0.002 && refPx < 0.02;
  let tick = declaredTick;
  if (fine && fine > 0 && fine < tick) tick = fine;
  if (microFx && tick > 2.5e-7) tick = Math.min(tick, 1e-6);
  if (!isFinite(tick) || tick <= 0) return declaredTick > 0 ? declaredTick : 1e-6;
  return tick;
}
/**
 * Match ladder px to open-order prices on a **single** quote step (per-leg or book-inferred).
 * Do not also try global `price_tick` — a coarse second pass re-buckets every JPY level onto the
 * same few prices and lights the whole ladder yellow.
 */
function priceMatchesMine(px, mines, tick) {
  if (!mines.length || !isFinite(px) || !isFinite(tick) || tick <= 0) return false;
  const r = Math.round(px / tick) * tick;
  const tol = tick * 0.501;
  for (let j = 0; j < mines.length; j++) {
    const mj = Number(mines[j]);
    if (!isFinite(mj)) continue;
    const rj = Math.round(mj / tick) * tick;
    if (Math.abs(rj - r) <= tol) return true;
  }
  return false;
}
function levelDir(prevSide, i, currPx) {
  if (!isFinite(currPx) || !prevSide || !Array.isArray(prevSide[i])) return "";
  const prevp = Number(prevSide[i][0]);
  if (!isFinite(prevp)) return "";
  if (currPx > prevp + 1e-12) return "px-up";
  if (currPx < prevp - 1e-12) return "px-down";
  return "";
}
function fillTable(id, rows, cols) {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = "<tr>" + cols.map(c => "<th>" + c + "</th>").join("") + "</tr>";
  for (const o of rows || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = cols.map(function(k) {
      var v = o[k];
      return "<td>" + (v != null ? String(v) : "") + "</td>";
    }).join("");
    el.appendChild(tr);
  }
}
function syncParamsForm(_srcPref) {
  /* Legacy: used to mirror a second “Params” tab (removed — edit mode popup only). */
}
function gatherAxOrderbookExtrasFromUi() {
  var sel = document.getElementById("axOrderbookMulti");
  if (!sel) return [];
  var out = [];
  for (var i = 0; i < sel.options.length; i++) {
    if (sel.options[i].selected) out.push(String(sel.options[i].value));
  }
  return out;
}
function gatherNeonMdFromUi() {
  var sel = document.getElementById("neonMdMulti");
  if (!sel) return [];
  var out = [];
  for (var i = 0; i < sel.options.length; i++) {
    if (sel.options[i].selected) out.push(String(sel.options[i].value));
  }
  return out;
}
function refreshNeonProductOptionsFromState(s) {
  var wrap = document.getElementById("neonMdWrap");
  var sel = document.getElementById("neonMdMulti");
  var hint = document.getElementById("neonSlHint");
  if (!wrap || !sel || !s) return;
  var isNeon = String(s.reference_provider || "") === "neon_fix";
  wrap.style.display = isNeon ? "" : "none";
  if (!isNeon) return;
  if (hint) {
    /* 0 means "never merged" — do not show epoch 1970-01-01 */
    var nsl = Number(s.neon_security_list_updated_ms);
    if (isFinite(nsl) && nsl > 1e11) {
      hint.style.display = "";
      try {
        var d = new Date(nsl);
        hint.textContent = "Security List merged: " + d.toISOString().replace("T", " ").slice(0, 19) + " UTC";
      } catch (e) {
        hint.textContent = "";
        hint.style.display = "none";
      }
    } else {
      hint.style.display = "none";
      hint.textContent = "";
    }
  }
  var opts = Array.isArray(s.neon_product_options) ? s.neon_product_options.slice() : [];
  /* Same list as the Reference column (#bnAllGrid): cfg ref_book_symbols ≡ fix md_symbols for neon_fix. */
  var wanted;
  if (Array.isArray(s.ref_book_symbols) && s.ref_book_symbols.length) {
    wanted = s.ref_book_symbols.map(String);
  } else {
    wanted = Array.isArray(s.neon_fix_md_symbols) ? s.neon_fix_md_symbols.map(String) : [];
  }
  var prevSel = gatherNeonMdFromUi();
  var keep = (window.__mmNeonMdUserTouched && prevSel.length) ? prevSel : wanted;
  var keepSet = {};
  for (var a = 0; a < keep.length; a++) keepSet[keep[a]] = true;
  sel.innerHTML = "";
  for (var k = 0; k < opts.length; k++) {
    var o = document.createElement("option");
    o.value = String(opts[k]);
    o.textContent = String(opts[k]);
    o.selected = !!keepSet[o.value];
    sel.appendChild(o);
  }
  var note = document.getElementById("neonMdCatalogNote");
  if (note) {
    var single = opts.length <= 1;
    note.style.display = single ? "block" : "none";
    if (single) {
      var nsl2 = Number(s.neon_security_list_updated_ms);
      if (isFinite(nsl2) && nsl2 > 1e11) {
        note.textContent = "Security List merged but catalog is thin — check desk logs for SecurityList 35=y or MM_DESK_NEON_FIX_DEBUG=1.";
      } else {
        note.textContent = "Waiting for Security List (35=y) or confirm Neon 35=x on this session.";
      }
    }
  }
}
function updateAxOrderbookExtrasSummary(s) {
  var note = document.getElementById("axOrderbookMmLegsNote");
  if (note && s) {
    var legs = mmDeskUnifiedLegRows(s);
    var axs = [];
    for (var li = 0; li < legs.length; li++) {
      var ax = String((legs[li] && legs[li].ax) || "").trim();
      if (ax) axs.push(ax);
    }
    var exMode = s.ax_architect_depth_multiselect_explicit === true;
    var nLeg = axs.length;
    note.textContent =
      (nLeg ? nLeg + " products" : "No products") +
      " · " +
      (exMode ? "explicit depth (polled = ticked)" : "legacy merge");
  }
  var el = document.getElementById("axOrderbookMultiSummary");
  if (!el) return;
  var saved = (s && Array.isArray(s.ax_orderbook_extras)) ? s.ax_orderbook_extras.filter(Boolean).map(String) : [];
  var polled = (s && Array.isArray(s.ax_book_symbols)) ? s.ax_book_symbols.filter(Boolean).map(String) : [];
  var cur = [];
  try {
    cur = gatherAxOrderbookExtrasFromUi();
  } catch (e) {
    cur = [];
  }
  var tCfg = saved.length ? saved.join(", ") : "—";
  var tPoll = polled.length ? polled.join(", ") : "—";
  var tSel = cur.length ? cur.join(", ") : "—";
  el.textContent = "Cfg " + saved.length + " · Poll " + polled.length + " · Ticked " + cur.length;
  el.title = "Cfg: " + tCfg + " | Poll: " + tPoll + " | Ticked: " + tSel;
}
function hydrateAxOrderbookMultiFromServerState(s) {
  var sel = document.getElementById("axOrderbookMulti");
  if (!sel || !s) return;
  if (window.__mmOrderbookUserTouched) {
    try {
      updateAxOrderbookExtrasSummary(s);
    } catch (e0) {}
    return;
  }
  var wantedSrc = Array.isArray(s.ax_book_symbols) ? s.ax_book_symbols.map(String) : [];
  var ws = {};
  for (var w = 0; w < wantedSrc.length; w++) {
    var uw = String(wantedSrc[w] || "").trim().toUpperCase();
    if (uw) ws[uw] = true;
  }
  for (var i = 0; i < sel.options.length; i++) {
    var op = sel.options[i];
    var ov = String(op.value || "").trim().toUpperCase();
    op.selected = !!ws[ov];
  }
  try {
    updateAxOrderbookExtrasSummary(s);
  } catch (e2) {}
}
/** Rebuild Architect AX depth multiselect from GET /instruments (includes MM legs). */
function populateAxOrderbookMultiOptions(sortedSyms) {
  var sel = document.getElementById("axOrderbookMulti");
  if (!sel || !Array.isArray(sortedSyms)) return;
  var st = window.__mmDeskLastState || {};
  var prevSel = gatherAxOrderbookExtrasFromUi();
  var fallback = [];
  if (!prevSel.length && st && !window.__mmOrderbookUserTouched) {
    var abs = st.ax_book_symbols;
    fallback = Array.isArray(abs) ? abs.map(String) : [];
  }
  var keep = prevSel.length ? prevSel : fallback;
  var keepU = {};
  for (var a = 0; a < keep.length; a++) {
    var ku = String(keep[a] || "").trim().toUpperCase();
    if (ku) keepU[ku] = true;
  }
  sel.innerHTML = "";
  for (var k = 0; k < sortedSyms.length; k++) {
    var raw = String(sortedSyms[k]);
    var o = document.createElement("option");
    o.value = raw;
    o.textContent = raw;
    o.selected = !!keepU[raw.toUpperCase()];
    sel.appendChild(o);
  }
  window.__mmOrderbookInstrumentSelectReady = true;
  try {
    if (window.__mmDeskLastState) hydrateAxOrderbookMultiFromServerState(window.__mmDeskLastState);
    else updateAxOrderbookExtrasSummary(st);
  } catch (e) { console.warn("hydrateAxOrderbookMultiFromServerState", e); }
  try {
    updateAxOrderbookExtrasSummary(window.__mmDeskLastState || st);
  } catch (e3) {}
}
async function refreshAxOrderbookInstrumentOptions() {
  var sel = document.getElementById("axOrderbookMulti");
  if (!sel) return;
  try {
    var j = await api("/api/instruments");
    if (!j || !j.ok || !Array.isArray(j.instruments)) return;
    var syms = j.instruments.slice().sort(function(a, b) { return String(a).localeCompare(String(b)); });
    window.__mmDeskInstrumentSyms = syms;
    populateAxOrderbookMultiOptions(syms);
  } catch (e) {
    console.warn("refreshAxOrderbookInstrumentOptions", e);
  }
}
function applyFromState(s) {
  if (!s) return;
  deskRoSetById("rest", s.rest_endpoint || "");
  var primAx = String(s.ax_symbol != null ? s.ax_symbol : "").trim();
  var legRows = mmDeskUnifiedLegRows(s);
  var legSet = {};
  for (var li = 0; li < legRows.length; li++) {
    var u = String(legRows[li].ax || "").trim().toUpperCase();
    if (u) legSet[u] = true;
  }
  var curAx0 = String(deskRoContent(document.getElementById("axSym")) || "").trim();
  if (!curAx0 || !legSet[curAx0.toUpperCase()]) {
    deskRoSetById("axSym", primAx);
  }
  var actAx = String(deskRoContent(document.getElementById("axSym")) || "").trim();
  var rowA = mmDeskFindInstrumentRow(s, actAx);
  if (rowA && (rowA.reference_fix_symbol || rowA.theo_symbol)) {
    deskRoSetById("refSym", String(rowA.reference_fix_symbol || rowA.theo_symbol || "").trim());
  } else {
    deskRoSetById("refSym", s.ref_symbol != null ? s.ref_symbol : "");
  }
  deskRoSetById("apiKey", s.prefill_api_key != null ? s.prefill_api_key : "");
  var sec = s.prefill_api_secret != null ? String(s.prefill_api_secret) : "";
  deskRoSetById("apiSec", sec.length ? "•••••••• (stored)" : "—");
  deskRoSetById("w", s.mm_width != null ? s.mm_width : 10);
  deskRoSetById("q", s.mm_order_size != null ? s.mm_order_size : 200);
  deskRoSetById("mx", s.mm_max_position != null ? s.mm_max_position : 1000000);
  deskRoSetById("ap", s.mm_adjust_position != null ? s.mm_adjust_position : 50);
  deskRoSetById("at", s.mm_adjust_ticks != null ? s.mm_adjust_ticks : 1);
  deskRoSetById("mmCurReload", s.mm_current_reload_count != null && s.mm_current_reload_count !== undefined ? String(s.mm_current_reload_count) : "0");
  deskRoSetById("mrc", s.mm_max_reload_cycles != null && s.mm_max_reload_cycles !== undefined ? String(s.mm_max_reload_cycles) : "");
  deskRoSetById("mrcFillOnly", s.mm_reload_cycles_count_fill_only !== false ? "Yes" : "No");
  if (s.mm_reload_limit_reset_nonce != null) deskRoSetById("mmReloadNonce", String(s.mm_reload_limit_reset_nonce));
  var rqOn = s.mm_requote_on_theo_move !== false;
  deskRoSetById("mmRequoteTheo", rqOn ? "Enabled" : "Disabled");
  deskRoSetById("mmMinTheoTicks", s.mm_min_theo_move_ticks_to_requote != null ? String(s.mm_min_theo_move_ticks_to_requote) : "");
  deskRoSetById("mmPriceTick", s.price_tick != null ? String(s.price_tick) : "");
  deskRoSetById("mmPricingTick", s.mm_pricing_tick != null ? String(s.mm_pricing_tick) : "");
  syncParamsForm("1");
  try {
    if (window.__mmOrderbookInstrumentSelectReady) hydrateAxOrderbookMultiFromServerState(s);
  } catch (e) { console.warn("axOrderbookMulti:", e); }
  try {
    updateAxOrderbookExtrasSummary(s);
  } catch (e) { console.warn("axOrderbookMultiSummary:", e); }
  try { refreshNeonProductOptionsFromState(s); } catch (e) { console.warn("neonMdMulti:", e); }
  try { hydrateTheoLegViewFromState(s); } catch (e) { console.warn("theoLegViewSelect:", e); }
  try { paintLive(s); } catch (e) { console.warn("paintLive:", e); }
}
function hydrateTheoLegViewFromState(s) {
  if (!s) return;
  var wrap = document.getElementById("theoLegMultiWrap");
  var sel = document.getElementById("theoLegViewSelect");
  if (!wrap || !sel) return;
  var rows = mmDeskUnifiedLegRows(s);
  wrap.classList.toggle("mm-desk-leg-visible", !!(s.multi_theo || rows.length >= 2));
  if (!s.multi_theo && rows.length < 2) {
    /* Was returning without clearing — legacy options stayed visible vs prompts built fresh each time. */
    sel.innerHTML = "";
    return;
  }
  var curAx = String(mmDeskActiveAxSymbol(s) || s.ax_symbol || "").trim();
  var found = -1;
  sel.innerHTML = "";
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i] || {};
    var ax = String(r.ax || "");
    var ref = String(r.ref || "");
    if (ax && curAx && ax.toUpperCase() === curAx.toUpperCase()) found = i;
    var o = document.createElement("option");
    o.value = String(i);
    o.setAttribute("data-ax", ax);
    o.setAttribute("data-ref", ref);
    o.textContent = mmDeskFormatLegOptionLabel(r);
    sel.appendChild(o);
  }
  if (found >= 0) sel.value = String(found);
  else if (rows.length) sel.value = "0";
  var idx0 = parseInt(String(sel.value || "0"), 10);
  if (isFinite(idx0) && idx0 >= 0 && idx0 < rows.length) {
    var rPick = rows[idx0] || {};
    try {
      mmDeskSetActiveInstrument(String(rPick.ax || "").trim(), String(rPick.ref || "").trim());
    } catch (eSync) { console.warn("theoLeg sync ax/ref", eSync); }
  }
}
function escapeHtmlChart(s) {
  return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function bookMetricsFromLevels(bids, asks) {
  if (!bids || !asks || bids.length === 0 || asks.length === 0) return null;
  var bidP = Number(bids[0][0]), askP = Number(asks[0][0]);
  if (!isFinite(bidP) || !isFinite(askP) || askP < bidP) return null;
  var mid = (bidP + askP) / 2;
  var spr = askP - bidP;
  var K = Math.min(20, bids.length, asks.length);
  var bVol = 0, aVol = 0;
  for (var i = 0; i < K; i++) {
    bVol += Math.abs(Number(bids[i][1]) || 0);
    aVol += Math.abs(Number(asks[i][1]) || 0);
  }
  var tot = bVol + aVol;
  var pressure = tot > 1e-12 ? (bVol - aVol) / tot : 0;
  return { mid: mid, spread: spr, spreadBps: mid ? (spr / mid) * 10000 : null, bVol: bVol, aVol: aVol, pressure: pressure, levels: K };
}
function renderDepthMetrics(s) {
  var el = document.getElementById("depthMetrics");
  if (!el) return;
  var bnBooks = s.ref_all_books || {};
  var axBooks = s.ax_all_books || {};
  var bnSyms = Array.isArray(s.ref_book_symbols) ? s.ref_book_symbols : [];
  var axSyms = (Array.isArray(s.ax_grid_symbols) && s.ax_grid_symbols.length) ? s.ax_grid_symbols.slice() : (Array.isArray(s.ax_book_symbols) ? s.ax_book_symbols.slice() : []);
  var showRefFeed = showReferenceDepthCol(s);
  var parts = [];
  function oneBlock(venue, sym, d, neonTob) {
    if (!d || d._err) {
      var wmsg = (d && d._err) ? String(d._err).slice(0, 120) : "Waiting…";
      if (!d && s.reference_provider === "neon_fix" && s.reference_error) {
        wmsg = String(s.reference_error).slice(0, 280);
      }
      var wcls = (!d && s.reference_provider === "neon_fix" && s.reference_error) ? "#e07070" : "#8b9cb3";
      parts.push('<div class="depth-metric-block"><div class="sym">' + escapeHtmlChart(venue + " · " + sym) + '</div><p class="muted" style="margin:0;font-size:11px;color:' + wcls + '">' + escapeHtmlChart(wmsg) + '</p></div>');
      return;
    }
    var m = bookMetricsFromLevels(d.bids || [], d.asks || []);
    if (!m) {
      parts.push('<div class="depth-metric-block"><div class="sym">' + escapeHtmlChart(venue + " · " + sym) + '</div><p class="muted" style="margin:0">Empty book</p></div>');
      return;
    }
    var dec = (venue === "AX")
      ? bootDecimals(tickForAxSymbol(s, sym))
      : bootDecimals(tickForRefSymbol(s, sym));
    var bps = m.spreadBps != null && isFinite(m.spreadBps) ? m.spreadBps.toFixed(2) : "—";
    if (neonTob) {
      parts.push('<div class="depth-metric-block"><div class="sym">' + escapeHtmlChart(venue + " · " + sym) + '</div>' +
        '<div class="metric-row"><span class="k">Mid</span><span class="v">' + m.mid.toFixed(dec) + '</span></div>' +
        '<div class="metric-row"><span class="k">Spread</span><span class="v">' + m.spread.toFixed(dec) + ' <span class="muted">(' + bps + ' bps)</span></span></div>' +
        '<div class="metric-row"><span class="k">Bid / Ask qty</span><span class="v">1 · 1 <span class="muted">(Neon TOB)</span></span></div></div>');
      return;
    }
    var prPct = (m.pressure * 100).toFixed(1);
    var prLabel = m.pressure > 0.05 ? "bid-heavy" : (m.pressure < -0.05 ? "ask-heavy" : "balanced");
    parts.push('<div class="depth-metric-block"><div class="sym">' + escapeHtmlChart(venue + " · " + sym) + '</div>' +
      '<div class="metric-row"><span class="k">Mid</span><span class="v">' + m.mid.toFixed(dec) + '</span></div>' +
      '<div class="metric-row"><span class="k">Spread</span><span class="v">' + m.spread.toFixed(dec) + ' <span class="muted">(' + bps + ' bps)</span></span></div>' +
      '<div class="metric-row"><span class="k">Pressure</span><span class="v">' + prPct + '% <span class="muted">' + prLabel + '</span></span></div>' +
      '<div class="metric-row"><span class="k">Σ size top ' + m.levels + '</span><span class="v">bid ' + m.bVol.toFixed(4) + ' · ask ' + m.aVol.toFixed(4) + '</span></div></div>');
  }
  var refM = (s.reference_metrics_tag != null && String(s.reference_metrics_tag).trim()) ? String(s.reference_metrics_tag).trim() : "BN";
  if (showRefFeed) {
    bnSyms.forEach(function(sym) {
      var d = bookLookup(bnBooks, sym);
      var nl = (d && d.bids) ? d.bids.length : 0;
      var neonTob = s.reference_provider === "neon_fix" && nl <= 1;
      oneBlock(refM, sym, d, neonTob);
    });
  }
  axSyms.forEach(function(sym) { oneBlock("AX", String(sym), bookLookup(axBooks, sym), false); });
  el.innerHTML = parts.length ? parts.join("") : '<p class="muted" style="margin:12px">No symbols in config.</p>';
}
/**
 * Paint the top-of-sidebar feed-health pills from /api/state.feed_health.
 *
 * The C++ trading_client writes logs/mm_feed_health.json every ~1.5s and the
 * Python desk passes it through as feed_health. We render three pills (Mettraders,
 * Hyperliquid, Neon) — green when up, red when down, dimmed when the file is
 * stale (>5s old) — and put the last error + age into each pill's tooltip so
 * the user can see *why* a leg is being blocked without leaving the page.
 */
function paintFeedHealthPills(fh) {
  function fmtAge(ms) {
    if (ms == null || ms < 0) return "n/a";
    if (ms < 1500) return "fresh";
    if (ms < 60000) return Math.round(ms / 1000) + "s ago";
    if (ms < 3600000) return Math.round(ms / 60000) + "m ago";
    return Math.round(ms / 3600000) + "h ago";
  }
  function paint(elId, key, label) {
    var el = document.getElementById(elId);
    if (!el) return;
    var f = (fh && typeof fh === "object" && fh[key]) ? fh[key] : null;
    var up = !!(f && f.up);
    var stale = !!(fh && fh.stale);
    el.classList.remove("up", "down", "stale");
    if (stale) el.classList.add("stale");
    el.classList.add(up ? "up" : "down");
    var age = f ? fmtAge(f.age_ms) : "n/a";
    var err = (f && f.last_error) ? String(f.last_error) : "";
    var tip = label + " · " + (up ? "UP" : "DOWN") + " · last ok " + age;
    if (!up && err) tip += " · " + err;
    if (stale) tip += " · (stale snapshot — trading_client may not be writing)";
    el.title = tip;
  }
  paint("feedPillMettraders", "mettraders", "Mettraders");
  paint("feedPillHl",   "hyperliquid", "Hyperliquid");
  paint("feedPillNeon", "neon",        "Neon FIX");
}

function paintLive(s) {
  if (!s) return;
  const ps = (typeof s.poll_sec === "number" && !isNaN(s.poll_sec)) ? s.poll_sec : 10;
  const uims = (typeof s.ui_poll_ms === "number" && !isNaN(s.ui_poll_ms)) ? s.ui_poll_ms : 1000;
  const archHost = (s.rest_endpoint || "").replace("https://", "").replace("http://", "").split("/")[0] || "—";
  var bnHost = (s.reference_tcp_endpoint && String(s.reference_tcp_endpoint).trim())
    ? String(s.reference_tcp_endpoint).trim()
    : ((s.ref_rest_url || "").replace("https://", "").replace("http://", "").split("/")[0] || "—");
  const bnBooks = s.ref_all_books || {};
  const bnOk = Object.keys(bnBooks).some(function(k) {
    var d = bnBooks[k];
    return d && !d._err && ((d.bids && d.bids.length) || (d.asks && d.asks.length));
  });
  const axBooks = s.ax_all_books || {};
  const axGridOk = Object.keys(axBooks).some(function(k) {
    var d = axBooks[k];
    return d && !d._err && ((d.bids && d.bids.length) || (d.asks && d.asks.length));
  });
  const axHas = axGridOk || ((s.book_ax && s.book_ax.bids && s.book_ax.bids.length) || (s.book_ax && s.book_ax.asks && s.book_ax.asks.length));
  var bc = s.book_row_counts || {};
  var axBk = (typeof s.ax_book_poll_sec === "number" && !isNaN(s.ax_book_poll_sec)) ? s.ax_book_poll_sec : 2;
  var axAcct = (typeof s.account_poll_sec === "number" && !isNaN(s.account_poll_sec)) ? s.account_poll_sec : 5;
  var cnt = (bc.ref_bids != null && bc.ref_asks != null) ? (" · rows primary ref " + bc.ref_bids + "/" + bc.ref_asks + " ax " + (bc.ax_bids || 0) + "/" + (bc.ax_asks || 0)) : "";
  var srvClock = "";
  if (s.server_time_ms != null && s.server_time_ms !== "") {
    var dSrv = new Date(s.server_time_ms);
    if (!isNaN(dSrv.getTime())) srvClock = " · srv " + dSrv.toISOString().slice(11, 19) + "Z";
  }
  var acctH = "";
  if (s.orders_api_ok === false) acctH += " orders?";
  if (s.fills_api_ok === false) acctH += " fills?";
  var showRefFeed = showReferenceDepthCol(s);
  var dcg = document.getElementById("depthColsGrid");
  if (dcg) {
    if (showRefFeed) dcg.classList.add("depth-cols--with-ref");
    else dcg.classList.remove("depth-cols--with-ref");
  }
  var rdc = document.getElementById("refDepthCol");
  if (rdc) rdc.style.display = showRefFeed ? "" : "none";
  var tm = document.getElementById("topMeta");
  if (tm) {
    var row = archHost;
    if (showRefFeed) row += " · " + bnHost;
    row += " · UI ~" + uims + "ms · AX bk " + axBk + "s · acct " + axAcct + "s" + acctH + srvClock;
    if (showRefFeed) row += cnt;
    else row += (" · rows ax " + (bc.ax_bids || 0) + "/" + (bc.ax_asks || 0));
    tm.textContent = row;
  }
  var bnDepthInst = document.getElementById("bnDepthInstrument");
  if (bnDepthInst) {
    var bns = (s.ref_symbol != null && String(s.ref_symbol).trim()) ? String(s.ref_symbol).trim() : "—";
    bnDepthInst.textContent = bns;
  }
  var axDepthInst = document.getElementById("axDepthInstrument");
  if (axDepthInst) {
    var mmLeg = String(mmDeskActiveAxSymbol(s) || s.ax_symbol || "").trim();
    var gs = (Array.isArray(s.ax_grid_symbols) && s.ax_grid_symbols.length)
      ? s.ax_grid_symbols.map(String)
      : (Array.isArray(s.ax_book_symbols) ? s.ax_book_symbols.map(String) : []);
    var badge = "—";
    if (gs.length === 1) {
      badge = gs[0].trim() || "—";
    } else if (gs.length === 2) {
      badge = gs.map(function(x) { return String(x || "").trim(); }).filter(Boolean).join(" · ") || "—";
    } else if (gs.length > 2) {
      var a = String(gs[0] || "").trim();
      var b = String(gs[1] || "").trim();
      badge = (a && b) ? (a + " · " + b + " · +" + (gs.length - 2)) : gs.join(" · ");
    } else if (mmLeg) {
      badge = mmLeg;
    }
    axDepthInst.textContent = badge;
    var tipParts = [];
    if (gs.length) tipParts.push("Depth poll: " + gs.join(", "));
    if (mmLeg) tipParts.push("Active MM leg (sidebar/header): " + mmLeg);
    axDepthInst.title = tipParts.join(" · ") || "";
  }
  var btag = document.getElementById("bnDepthTag");
  if (btag) {
    if (s.reference_provider === "neon_fix") {
      if (s.neon_reference_from_cpp_file === true) {
        btag.textContent = "Neon C++ depth ~1s";
      } else {
        btag.textContent = "Neon FIX live";
      }
      btag.className = "feed-tag rest";
    } else {
      var ws = s.ref_feed_mode === "websocket";
      btag.textContent = ws ? "WS depth20" : ("REST " + ps + "s");
      btag.className = "feed-tag" + (ws ? "" : " rest");
    }
  }
  var atag = document.getElementById("axDepthTag");
  if (atag) {
    var axUpd = "";
    if (s.ax_books_updated_ms) {
      var ago = Math.max(0, Math.floor((Date.now() - s.ax_books_updated_ms) / 1000));
      axUpd = " · grid " + ago + "s ago";
    }
    atag.textContent = "REST bk " + axBk + "s / acct " + axAcct + "s" + axUpd;
  }
  var pillBn = document.getElementById("pillBn");
  if (pillBn) {
    if (!showRefFeed) {
      pillBn.style.display = "none";
    } else {
      pillBn.style.display = "";
      if (s.reference_feed_pill) pillBn.textContent = s.reference_feed_pill;
      pillBn.className = "pill" + (bnOk ? " ok" : "");
    }
  }
  var bnCardTitle = document.getElementById("bnDepthCardTitle");
  if (bnCardTitle && s.reference_venue_label) bnCardTitle.textContent = s.reference_venue_label;
  var pillAx = document.getElementById("pillAx");
  if (pillAx) pillAx.className = "pill" + (axHas ? " ok" : (s.has_token ? " warn" : ""));
}
function bookLookup(books, sym) {
  if (!books || sym == null || sym === "") return undefined;
  var k = String(sym);
  if (Object.prototype.hasOwnProperty.call(books, k)) return books[k];
  var ku = k.toUpperCase();
  if (ku !== k && Object.prototype.hasOwnProperty.call(books, ku)) return books[ku];
  var kl = k.toLowerCase();
  if (kl !== k && Object.prototype.hasOwnProperty.call(books, kl)) return books[kl];
  return undefined;
}
function renderAllBnBooks(s) {
  const grid = document.getElementById("bnAllGrid");
  const hint = document.getElementById("hintBnAll");
  const meta = document.getElementById("bnMarketsMeta");
  if (!grid || !hint) {
    console.warn("mmDesk: missing #bnAllGrid or #hintBnAll — depth UI DOM mismatch (stale cached HTML?)");
    return;
  }
  if (!showReferenceDepthCol(s)) {
    grid.innerHTML = "";
    hint.style.display = "none";
    if (meta) meta.textContent = "";
    return;
  }
  const books = s.ref_all_books || {};
  const cfgSyms = Array.isArray(s.ref_book_symbols) ? s.ref_book_symbols : [];
  const nListed = cfgSyms.length;
  const nLoaded = Object.keys(books).length;
  const isNeon = s.reference_provider === "neon_fix";
  const fromCpp = s.neon_reference_from_cpp_file === true;
  if (meta) {
    if (isNeon) {
      var ageM = (typeof s.cpp_neon_depth_age_sec === "number" && isFinite(s.cpp_neon_depth_age_sec))
        ? (" · file " + s.cpp_neon_depth_age_sec.toFixed(1) + "s ago")
        : "";
      meta.textContent = nListed
        ? ("(" + (fromCpp ? "Neon C++ depth file" : "Neon FIX") + " · " + nListed + " symbol(s) · " + nLoaded + " loaded" + ageM + ")")
        : "";
    } else {
      meta.textContent = nListed ? ("(" + nListed + " in config · " + nLoaded + " in last poll)") : "";
    }
  }
  const ps = (typeof s.poll_sec === "number" && !isNaN(s.poll_sec)) ? s.poll_sec : 10;
  if (!nListed) {
    hint.style.display = "block";
    hint.textContent = (s.reference_provider === "neon_fix")
      ? "Set external_feed.fix.md_symbols (or external_feed.symbol) in default_config.json."
      : "Add symbols: external_feed.symbol (primary) or mm_desk.reference_orderbook_symbols in config.";
    grid.innerHTML = "";
    return;
  }
  if (!nLoaded) {
    hint.style.display = "block";
    var waitBn = (s.reference_provider === "neon_fix")
      ? (fromCpp
        ? "Waiting for logs/mm_external_depth.json from trading_client (same cwd, ~1s updates)…"
        : "Waiting for Neon FIX (stunnel or direct TLS)…")
      : ("Waiting for first reference REST poll (~" + ps + "s)…");
    var re = (s.reference_error != null && String(s.reference_error).trim()) ? String(s.reference_error).trim() : "";
    if (s.reference_provider === "neon_fix" && re) {
      hint.innerHTML = '<span style="color:#e07070;font-size:11px;display:block;margin-bottom:6px">' + escapeHtmlChart(re.slice(0, 420)) + '</span><span class="muted">' + escapeHtmlChart(waitBn) + '</span>';
    } else {
      hint.textContent = waitBn;
    }
    grid.innerHTML = "";
    return;
  }
  let hasGood = false;
  hint.style.display = "none";
  grid.innerHTML = "";
  for (const sym of cfgSyms) {
    const dec = decimalsForRefSymbol(s, sym);
    const d = bookLookup(books, sym);
    const wrap = document.createElement("div");
    wrap.className = "card micro ax-book-card";
    const bh = document.createElement("div");
    bh.className = "card-h";
    bh.textContent = sym;
    wrap.appendChild(bh);
    if (!d) {
      const p = document.createElement("p");
      p.style.cssText = "padding:8px;font-size:11px;color:#8b9cb3";
      p.textContent = "Waiting for next poll…";
      wrap.appendChild(p);
    } else if (d._err) {
      const p = document.createElement("p");
      p.style.cssText = "padding:8px;font-size:11px;color:#ff6b6b";
      p.textContent = d._err;
      wrap.appendChild(p);
    } else {
      const bids = d.bids || [];
      const asks = d.asks || [];
      if ((bids && bids.length) || (asks && asks.length)) hasGood = true;
      if (isNeon && bids.length <= 1 && asks.length <= 1) {
        const b0 = bids[0], a0 = asks[0];
        const bpf = b0 ? Number(b0[0]) : NaN;
        const apf = a0 ? Number(a0[0]) : NaN;
        const bpx = isFinite(bpf) ? bpf.toFixed(dec) : "—";
        const apx = isFinite(apf) ? apf.toFixed(dec) : "—";
        const bq = (b0 && b0[1] != null) ? String(b0[1]) : "1";
        const aq = (a0 && a0[1] != null) ? String(a0[1]) : "1";
        let midS = "—";
        if (isFinite(bpf) && isFinite(apf) && apf > bpf) {
          midS = ((bpf + apf) / 2).toFixed(dec);
        }
        const strip = document.createElement("div");
        strip.className = "neon-tob-strip";
        strip.innerHTML =
          '<div class="neon-tob-row"><span class="k">Bid</span><span class="v bid">' + bpx + '</span><span class="q">× ' + escapeHtmlChart(bq) + '</span></div>' +
          '<div class="neon-tob-row"><span class="k">Ask</span><span class="v ask">' + apx + '</span><span class="q">× ' + escapeHtmlChart(aq) + '</span></div>' +
          '<div class="neon-tob-row" style="margin-top:10px"><span class="k">Mid</span><span class="v">' + midS + '</span></div>';
        wrap.appendChild(strip);
        if (fromCpp) renderNeonMmPreviewSlot(s, wrap);
      } else {
        const nRows = Math.min(BN_GRID_ROWS, Math.max(bids.length, asks.length));
        const tb = document.createElement("table");
        tb.className = "data";
        tb.innerHTML = "<tr><th colspan=2>Bids</th><th colspan=2>Asks</th></tr>";
        for (let i = 0; i < nRows; i++) {
          const tr = document.createElement("tr");
          const b = bids[i], a = asks[i];
          const bpx = b ? Number(b[0]).toFixed(dec) : "";
          const bq = b ? b[1] : "";
          const apx = a ? Number(a[0]).toFixed(dec) : "";
          const aq = a ? a[1] : "";
          tr.innerHTML = "<td class=bid>" + bpx + "</td><td>" + bq + "</td><td class=ask>" + apx + "</td><td>" + aq + "</td>";
          tb.appendChild(tr);
        }
        depthAppendOrderbookTable(wrap, tb);
        if (fromCpp) renderNeonMmPreviewSlot(s, wrap);
      }
    }
    grid.appendChild(wrap);
  }
  if (!hasGood) {
    hint.style.display = "block";
    var re2 = (s.reference_error != null && String(s.reference_error).trim()) ? String(s.reference_error).trim() : "";
    if (s.reference_provider === "neon_fix" && re2) {
      hint.innerHTML = '<span style="color:#e07070;font-size:11px;display:block;margin-bottom:6px">' + escapeHtmlChart(re2.slice(0, 420)) + '</span><span class="muted">' + escapeHtmlChart("No Neon TOB in last poll — fix stunnel/TLS/FIX above, then reload.") + '</span>';
    } else {
      hint.textContent = (s.reference_provider === "neon_fix")
        ? "No Neon TOB yet (FIX session, stunnel, or credentials — see terminal)."
        : "No reference depth rows yet (invalid symbols, rate limit, or REST error).";
    }
  }
}
function renderAllAxBooks(s) {
  const grid = document.getElementById("axAllGrid");
  const hint = document.getElementById("hintAxAll");
  const meta = document.getElementById("axMarketsMeta");
  if (!grid || !hint) {
    console.warn("mmDesk: missing #axAllGrid or #hintAxAll — depth UI DOM mismatch (stale cached HTML?)");
    return;
  }
  const books = s.ax_all_books || {};
  const cfgAxSyms = Array.isArray(s.ax_book_symbols) ? s.ax_book_symbols : [];
  const gridSyms = (Array.isArray(s.ax_grid_symbols) && s.ax_grid_symbols.length) ? s.ax_grid_symbols : cfgAxSyms;
  const cfgN = cfgAxSyms.length;
  const nListed = gridSyms.length || cfgN || (s.ax_markets_count != null ? s.ax_markets_count : Object.keys(books).length);
  const nLoaded = Object.keys(books).length;
  const axBk = (typeof s.ax_book_poll_sec === "number" && !isNaN(s.ax_book_poll_sec)) ? s.ax_book_poll_sec : 2;
  if (meta) {
    var baseMeta = nListed ? ("(" + nListed + " in config · " + nLoaded + " books · up to " + AX_GRID_ROWS + " levels)") : "";
    meta.textContent = (s.ax_markets_error && nLoaded)
      ? (baseMeta + " · note: " + s.ax_markets_error)
      : baseMeta;
  }
  if (s.ax_markets_error && !nLoaded) {
    hint.style.display = "block";
    hint.textContent = "AX markets: " + s.ax_markets_error;
    grid.innerHTML = "";
    var saErr = document.getElementById("axDepthShowAllBtn");
    if (saErr) saErr.style.display = "none";
    return;
  }
  if (!nLoaded) {
    hint.style.display = "block";
    hint.textContent = "Waiting for first Architect book (~" + axBk + "s)…";
    grid.innerHTML = "";
    var sa0 = document.getElementById("axDepthShowAllBtn");
    if (sa0) sa0.style.display = "none";
    return;
  }
  var showAllBtn = document.getElementById("axDepthShowAllBtn");
  if (showAllBtn && !showAllBtn.__mmDeskWired) {
    showAllBtn.__mmDeskWired = true;
    showAllBtn.onclick = function() {
      mmDeskAxDepthHiddenClear();
      renderAllAxBooks(window.__mmDeskLastState || {});
    };
  }
  var hiddenU = mmDeskAxDepthHiddenUpperSet();
  var hiddenCount = mmDeskAxDepthHiddenGet().length;
  const fullOrder = mmDeskAxBookGridOrder(s);
  const cfgOrder = fullOrder.filter(function(sym) {
    return !hiddenU[String(sym || "").trim().toUpperCase()];
  });
  if (fullOrder.length && !cfgOrder.length) {
    hint.style.display = "block";
    hint.textContent = "Every Architect ladder is hidden in this browser. Use “Show all hidden ladders” above, or hide fewer books.";
    grid.innerHTML = "";
    if (showAllBtn) {
      showAllBtn.style.display = "inline-block";
      showAllBtn.textContent = "Show all hidden ladders (" + hiddenCount + ")";
    }
    return;
  }
  hint.style.display = "none";
  grid.innerHTML = "";
  if (showAllBtn) {
    showAllBtn.style.display = hiddenCount ? "inline-block" : "none";
    showAllBtn.textContent = hiddenCount ? ("Show all hidden ladders (" + hiddenCount + ")") : "Show all hidden ladders";
  }
  let axHasGood = false;
  var selectedU = mmDeskAxDepthSelectedUpperSetFromUi(s);
  for (const sym of cfgOrder) {
    const dec = decimalsForAxSymbol(s, sym);
    const d = bookLookup(books, sym);
    const wrap = document.createElement("div");
    wrap.className = "card micro ax-book-card" + (selectedU[String(sym || "").trim().toUpperCase()] ? " ax-book-card--selected" : "");
    wrap.title = "Double-click to tick or untick this symbol in Architect instruments (below), then Apply instruments · reload to save.";
    const bh = document.createElement("div");
    bh.className = "card-h ax-book-hdr";
    const ttl = document.createElement("span");
    ttl.textContent = sym;
    const hb = document.createElement("button");
    hb.type = "button";
    hb.className = "ax-book-hide-btn";
    hb.setAttribute("aria-label", "Hide ladder for " + sym);
    hb.title = "Hide this ladder in the desk (this browser only). Server still polls until you remove the symbol from Architect instruments and Apply · reload.";
    hb.textContent = "Hide";
    hb.onclick = function(ev) {
      ev.preventDefault();
      ev.stopPropagation();
      mmDeskAxDepthHiddenAdd(sym);
      renderAllAxBooks(window.__mmDeskLastState || {});
    };
    wrap.addEventListener("dblclick", function(ev) {
      if (ev.target && ev.target.closest && ev.target.closest(".ax-book-hide-btn")) return;
      var msel = document.getElementById("axOrderbookMulti");
      if (!msel || !msel.options || !msel.options.length) return;
      var want = String(sym || "").trim();
      var opt = null;
      for (var oi = 0; oi < msel.options.length; oi++) {
        if (String(msel.options[oi].value || "").trim().toUpperCase() === want.toUpperCase()) {
          opt = msel.options[oi];
          break;
        }
      }
      if (!opt) return;
      opt.selected = !opt.selected;
      window.__mmOrderbookUserTouched = true;
      var stD = window.__mmDeskLastState || {};
      try { updateAxOrderbookExtrasSummary(stD); } catch (e0) {}
      try { renderAllAxBooks(stD); } catch (e1) {}
      try {
        hydrateTheoLegViewFromState(stD);
        paintMmGateButtons(stD);
      } catch (e1c) {}
    });
    bh.appendChild(ttl);
    bh.appendChild(hb);
    wrap.appendChild(bh);
    if (!d) {
      const p = document.createElement("p");
      p.style.cssText = "padding:8px;font-size:11px;color:#8b9cb3";
      p.textContent = "Waiting for next book…";
      wrap.appendChild(p);
    } else if (d._err) {
      const p = document.createElement("p");
      p.style.cssText = "padding:8px;font-size:11px;color:#ff6b6b";
      p.textContent = d._err;
      wrap.appendChild(p);
    } else {
      const bids = d.bids || [];
      const asks = d.asks || [];
      if ((bids && bids.length) || (asks && asks.length)) axHasGood = true;
      const nRows = Math.min(AX_GRID_ROWS, Math.max(bids.length, asks.length));
      const prev = window._axBookPrev[sym] || null;
      const mines = axMineList(s, sym);
      const tick = effectiveTickForMineMatch(tickForAxSymbol(s, sym), bids, asks);
      const tb = document.createElement("table");
      tb.className = "data";
      tb.innerHTML = "<tr><th colspan=2>Bids</th><th colspan=2>Asks</th></tr>";
      for (let i = 0; i < nRows; i++) {
        const tr = document.createElement("tr");
        const b = bids[i], a = asks[i];
        let bpx = "", bq = "", apx = "", aq = "";
        let bpf = NaN, apf = NaN;
        if (b) { bpf = Number(b[0]); bpx = bpf.toFixed(dec); bq = String(b[1]); }
        if (a) { apf = Number(a[0]); apx = apf.toFixed(dec); aq = String(a[1]); }
        const bMine = isFinite(bpf) && priceMatchesMine(bpf, mines, tick);
        const aMine = isFinite(apf) && priceMatchesMine(apf, mines, tick);
        const bDir = bMine && prev && prev.bids ? levelDir(prev.bids, i, bpf) : "";
        const aDir = aMine && prev && prev.asks ? levelDir(prev.asks, i, apf) : "";
        const tdBpx = document.createElement("td");
        tdBpx.className = ("bid ob-px" + (bMine ? " mine-ax" : "") + (bDir ? " " + bDir : "")).trim();
        tdBpx.textContent = bpx;
        const tdBq = document.createElement("td");
        tdBq.textContent = bq;
        const tdApx = document.createElement("td");
        tdApx.className = ("ask ob-px" + (aMine ? " mine-ax" : "") + (aDir ? " " + aDir : "")).trim();
        tdApx.textContent = apx;
        const tdAq = document.createElement("td");
        tdAq.textContent = aq;
        tr.appendChild(tdBpx);
        tr.appendChild(tdBq);
        tr.appendChild(tdApx);
        tr.appendChild(tdAq);
        tb.appendChild(tr);
      }
      window._axBookPrev[sym] = {
        bids: bids.slice(0, nRows).map(function(r) { return [Number(r[0]), r[1]]; }),
        asks: asks.slice(0, nRows).map(function(r) { return [Number(r[0]), r[1]]; }),
      };
      depthAppendOrderbookTable(wrap, tb);
    }
    grid.appendChild(wrap);
  }
  if (nLoaded && !axHasGood && !s.ax_markets_error) {
    hint.style.display = "block";
    hint.textContent = "No Architect depth rows yet (check symbols or auth).";
  }
}
function mmPreviewMetaLine(p) {
  if (!p || !p.ok) return "";
  var parts = [];
  if (p.theo_mid != null && isFinite(Number(p.theo_mid))) {
    var tm = Number(p.theo_mid);
    var md = 6;
    if (tm >= 200) { md = 4; } else if (tm >= 1) { md = 5; } else if (tm < 0.1 && tm > 0) { md = 8; }
    parts.push("mid " + tm.toFixed(md));
  }
  parts.push(String(p.theo_source || "—"));
  if (p.net_position_unknown) parts.push("net ?");
  else parts.push("net " + String(p.net_position));
  if (p.ax_symbol) parts.push(String(p.ax_symbol));
  return parts.filter(function(x) { return x; }).join(" · ");
}
function fillMmPreviewTbody(tb, p) {
  if (!tb) return "";
  if (!p.ok) {
    tb.innerHTML = "<tr><td colspan=\"4\" class=\"muted\">" + escapeHtmlChart(p.error || "—") + "</td></tr>";
    return "Theo source: " + escapeHtmlChart(String(p.theo_source || "none"));
  }
  var qcfg = Number(p.qty);
  if (!isFinite(qcfg)) qcfg = 0;
  var qb = (p.qty_bid != null && isFinite(Number(p.qty_bid))) ? Number(p.qty_bid) : qcfg;
  var qa = (p.qty_ask != null && isFinite(Number(p.qty_ask))) ? Number(p.qty_ask) : qcfg;
  function sideNote(want, qleg) {
    if (!want) return "pulled (max pos)";
    if (want && qleg <= 0) return "zero headroom";
    if (p.crosses) return "would cross book";
    return "guide only";
  }
  tb.innerHTML =
    "<tr><td class=\"bid\">BUY</td><td class=\"bid\">" + escapeHtmlChart(String(p.bid_px_str != null ? p.bid_px_str : p.bid_px)) + "</td><td>" + escapeHtmlChart(String(qb)) + "</td><td class=\"muted\">" + escapeHtmlChart(sideNote(p.want_bid, qb)) + "</td></tr>" +
    "<tr><td class=\"ask\">SELL</td><td class=\"ask\">" + escapeHtmlChart(String(p.ask_px_str != null ? p.ask_px_str : p.ask_px)) + "</td><td>" + escapeHtmlChart(String(qa)) + "</td><td class=\"muted\">" + escapeHtmlChart(sideNote(p.want_ask, qa)) + "</td></tr>";
  return mmPreviewMetaLine(p);
}
function appendMmPreviewCard(wrap, p, headingPrefix) {
  if (!wrap || !p) return;
  var slot = document.createElement("div");
  slot.className = "neon-mm-preview-slot";
  var ttl = document.createElement("div");
  ttl.className = "neon-mm-preview-title";
  var prefix = headingPrefix ? String(headingPrefix) : "MM guide";
  var sym = p && p.ax_symbol ? String(p.ax_symbol) : "";
  ttl.textContent = sym ? (prefix + " · " + sym) : prefix;
  slot.appendChild(ttl);
  var table = document.createElement("table");
  table.className = "data neon-mm-preview-table";
  table.innerHTML = "<thead><tr><th>Side</th><th>Price</th><th>Qty</th><th>Note</th></tr></thead><tbody></tbody>";
  var tb = table.querySelector("tbody");
  var metaText = fillMmPreviewTbody(tb, p);
  slot.appendChild(table);
  var meta = document.createElement("p");
  meta.className = "muted neon-mm-preview-meta";
  meta.textContent = metaText;
  slot.appendChild(meta);
  wrap.appendChild(slot);
}
function renderNeonMmPreviewSlot(s, wrap) {
  if (!wrap || !s) return;
  // New: per-leg "MM guide" cards — one per configured product whose theo
  // is currently available (HL xyz / CME / Neon FIX). The C++ math is the
  // same; this is display-only.
  var perLeg = (s && Array.isArray(s.mm_order_previews_by_symbol))
    ? s.mm_order_previews_by_symbol
    : [];
  if (perLeg.length > 0) {
    perLeg.forEach(function(p) { appendMmPreviewCard(wrap, p, "MM guide"); });
    return;
  }
  // Legacy fallback (single-product, Neon-only desk).
  if (s.reference_provider !== "neon_fix" || !s.neon_reference_from_cpp_file) return;
  appendMmPreviewCard(wrap, (s && s.mm_order_preview) ? s.mm_order_preview : {}, "MM guide");
}
function renderMmPositionGateBanner(s) {
  var el = document.getElementById("mmPositionGateBanner");
  if (!el) return;
  var msg = s && s.mm_position_gate_alert ? String(s.mm_position_gate_alert).trim() : "";
  if (!msg) {
    el.style.display = "none";
    el.textContent = "";
    return;
  }
  el.style.display = "block";
  el.textContent = msg;
}
/** Match tools/mm_live_desk_core.mm_position_gate_fields_from_preview (Python). */
function mmPositionGateMessageForAbsNetAndMax(an, mx) {
  if (!isFinite(Number(an)) || !isFinite(Number(mx))) return "";
  var mx2 = Math.max(1, Math.floor(Number(mx)));
  var a = Math.abs(Math.floor(Number(an)));
  if (a > mx2) {
    return (
      "Position cap breach: |net|=" +
      a +
      " > max_position=" +
      mx2 +
      ". " +
      "Only the inventory-reducing side is quoted; reduce exposure manually if needed."
    );
  }
  if (a >= mx2) {
    return (
      "At max_position: |net|=" +
      a +
      " (cap " +
      mx2 +
      "). Add-side quotes are off until |net| < " +
      mx2 +
      "."
    );
  }
  return "";
}
function absNetForMmGateConfirm(st, serverAlertMsg) {
  if (st && st.mm_position_abs_net != null && isFinite(Number(st.mm_position_abs_net))) {
    return Math.abs(Math.floor(Number(st.mm_position_abs_net)));
  }
  var m = String(serverAlertMsg || "").match(/\|net\|=(\d+)/);
  if (m) return parseInt(m[1], 10);
  return NaN;
}
/** Max position about to be saved: sidebar / modal / config editor merge (disk + polled state lag behind). */
function pendingMarketMakerMaxPositionFromUi() {
  var el = document.getElementById("mx");
  var t = el ? deskRoContent(el) : "";
  var v = parseInt(String(t || "").trim(), 10);
  if (isFinite(v) && v > 0) return v;
  try {
    var merge = gatherDeskConfigMerge();
    var mm = merge && merge.market_maker;
    if (mm && mm.max_position != null) {
      v = parseInt(String(mm.max_position).trim(), 10);
      if (isFinite(v) && v > 0) return v;
    }
  } catch (e0) {}
  return NaN;
}
function deskMmGateConfirm(actionLabel) {
  var st = window.__mmDeskLastState;
  var serverMsg = st && st.mm_position_gate_alert ? String(st.mm_position_gate_alert).trim() : "";
  if (!serverMsg) return true;
  var pendingMx = pendingMarketMakerMaxPositionFromUi();
  var an = absNetForMmGateConfirm(st, serverMsg);
  var msg = serverMsg;
  // Polled /api/state used on-disk max_position; form may already show the next save. Recompute vs pending cap.
  if (isFinite(pendingMx) && pendingMx > 0 && isFinite(an)) {
    msg = mmPositionGateMessageForAbsNetAndMax(an, pendingMx);
  }
  if (!msg) return true;
  return confirm(msg + "\n\nProceed with " + actionLabel + "?");
}
/** True while desk edit mode is on (server flag or optimistic Enter click). Skips poll overwrites of strategy fields. */
function mmDeskEditModeActive() {
  return false;
}
/** --- Config editor (popup tabs): nested get/set for default_config.json merge --- */
function ceNestedGet(obj, path) {
  var cur = obj;
  for (var i = 0; i < path.length; i++) {
    if (cur == null || typeof cur !== "object") return undefined;
    cur = cur[path[i]];
  }
  return cur;
}
function ceNestedSet(out, path, val) {
  if (!path || !path.length) return;
  var cur = out;
  for (var i = 0; i < path.length - 1; i++) {
    var k = path[i];
    if (cur[k] == null || typeof cur[k] !== "object") cur[k] = {};
    cur = cur[k];
  }
  cur[path[path.length - 1]] = val;
}
/** Unchecked = Disabled (left); checked = Enabled (right). */
function ceSetDeskTradingToggle(id, checkedBool) {
  var el = document.getElementById(id);
  if (el && el.classList && el.classList.contains("ce-bool-cb")) el.checked = !!checkedBool;
}
function ceGetDeskTradingToggle(id) {
  var el = document.getElementById(id);
  if (!el) return undefined;
  return !!el.checked;
}
var CE_DYNAMIC_BUILT = false;
/** Field rows appended into #ceMountMm / External / Risk / System (maps to C++ config JSON). */
var CE_SPEC_MM = {
  ints: [
    ["bid_width", "bid_width"],
    ["ask_width", "ask_width"],
    ["spread_ticks", "spread_ticks"],
    ["max_theo_age_ms", "max_theo_age_ms"],
    ["update_interval_sec", "update_interval_sec"],
    ["feed_log_banner_min_interval_ms", "feed_log_banner_min_interval_ms"],
    ["post_fill_extra_ticks", "post_fill_extra_ticks"],
    ["resting_depth_extra_ticks", "resting_depth_extra_ticks"],
    ["order_size_step", "order_size_step"],
    ["quantity", "quantity"],
  ],
  floats: [
    ["basis", "basis"],
    ["exchange_position_reconcile_sec", "exchange_position_reconcile_sec"],
  ],
  strings: [
    ["theo_symbol", "theo_symbol"],
    ["order_symbol", "order_symbol"],
    ["desk_signal_path", "desk_signal_path"],
  ],
  bools: [
    ["enabled", "enabled"],
    ["mm_orders_enabled", "mm_orders_enabled"],
    ["desk_sync_enabled", "desk_sync_enabled"],
    ["cancel_on_disconnect", "cancel_on_disconnect"],
    ["cancel_on_external_feed_invalid", "cancel_on_external_feed_invalid"],
    ["paper_simulate_fills", "paper_simulate_fills"],
    ["reload_on_fill", "reload_on_fill"],
    ["reload_qty", "reload_qty"],
    ["requote_on_timer", "requote_on_timer"],
    ["suppress_periodic_theo_requote_when_at_inventory_cap", "suppress_periodic_theo_requote_when_at_inventory_cap"],
    ["feed_log_verbose", "feed_log_verbose"],
    ["validate_vs_market", "validate_vs_market"],
  ],
};
var CE_SPEC_EF = {
  ints: [
    ["consecutive_errors_before_invalidate", "consecutive_errors_before_invalidate"],
    ["poll_interval_ms", "poll_interval_ms"],
    ["desk_depth_levels", "desk_depth_levels"],
    ["desk_depth_write_interval_ms", "desk_depth_write_interval_ms"],
  ],
  strings: [
    ["name", "name"],
    ["description", "description"],
    ["provider", "provider"],
    ["symbol", "symbol"],
    ["display_symbol", "display_symbol"],
    ["rest_url", "rest_url"],
    ["desk_theo_cache_path", "desk_theo_cache_path"],
    ["desk_depth_cache_path", "desk_depth_cache_path"],
  ],
  bools: [
    ["enabled", "enabled"],
    ["rest_uses_spot_ticker_path", "rest_uses_spot_ticker_path"],
    ["write_desk_theo_cache", "write_desk_theo_cache"],
    ["write_desk_depth_cache", "write_desk_depth_cache"],
  ],
};
var CE_SPEC_RISK = {
  floats: [
    ["margin_call_threshold", "risk.margin_call_threshold"],
    ["max_drawdown_percent", "risk.max_drawdown_percent"],
  ],
  ints: [
    ["max_position_size", "risk.max_position_size"],
    ["max_total_exposure", "risk.max_total_exposure"],
  ],
};
var CE_SPEC_TR = {
  floats: [["default_slippage", "trading.default_slippage"]],
  ints: [
    ["max_orders_per_second", "trading.max_orders_per_second"],
    ["max_requests_per_second", "trading.max_requests_per_second"],
    ["price_precision", "trading.price_precision"],
    ["quantity_precision", "trading.quantity_precision"],
    ["maker_fee_bps", "trading.fees.maker_fee_bps"],
    ["taker_fee_bps", "trading.fees.taker_fee_bps"],
  ],
  bools: [
    ["feed_guardian_enabled", "trading.feed_guardian_enabled"],
    ["feed_guardian_require_external", "trading.feed_guardian_require_external"],
    ["feed_guardian_require_ws", "trading.feed_guardian_require_ws"],
  ],
};
var CE_SPEC_SYS = {
  startup_bools: [
    ["crash_on_auth_failure", "startup.crash_on_auth_failure"],
    ["crash_on_feed_failure", "startup.crash_on_feed_failure"],
    ["log_step_progress", "startup.log_step_progress"],
    ["require_api_connectivity", "startup.require_api_connectivity"],
    ["require_external_feed", "startup.require_external_feed"],
    ["require_websocket", "startup.require_websocket"],
  ],
  startup_ints: [
    ["feed_verify_timeout_ms", "startup.feed_verify_timeout_ms"],
    ["market_data_verify_timeout_ms", "startup.market_data_verify_timeout_ms"],
  ],
  perf_ints: [
    ["event_queue_size", "performance.event_queue_size"],
    ["http_buffer_size", "performance.http_buffer_size"],
    ["market_data_queue_size", "performance.market_data_queue_size"],
    ["order_queue_size", "performance.order_queue_size"],
    ["worker_threads", "performance.worker_threads"],
    ["ws_buffer_size", "performance.ws_buffer_size"],
  ],
  ws_ints: [
    ["heartbeat_interval_ms", "websocket.heartbeat_interval_ms"],
    ["max_reconnect_delay_ms", "websocket.max_reconnect_delay_ms"],
    ["reconnect_delay_ms", "websocket.reconnect_delay_ms"],
  ],
  ws_bools: [
    ["auto_reconnect", "websocket.auto_reconnect"],
    ["ping_pong_enabled", "websocket.ping_pong_enabled"],
  ],
  feed_str: [
    ["mode", "feed.mode"],
    ["default_level", "feed.default_level"],
    ["source", "feed.source"],
  ],
  feed_ints: [
    ["l2_depth", "feed.l2_depth"],
    ["snapshot_interval_ms", "feed.snapshot_interval_ms"],
    ["throttle_ms", "feed.throttle_ms"],
  ],
  feed_bools: [
    ["subscribe_ticker", "feed.subscribe_ticker"],
    ["subscribe_trades", "feed.subscribe_trades"],
    ["verify_on_startup", "feed.verify_on_startup"],
  ],
  feat_bools: [
    ["enable_auto_reconnect", "features.enable_auto_reconnect"],
    ["enable_order_validation", "features.enable_order_validation"],
    ["enable_pnl_calculation", "features.enable_pnl_calculation"],
    ["enable_position_tracking", "features.enable_position_tracking"],
    ["enable_rate_limiting", "features.enable_rate_limiting"],
  ],
  log_str: [
    ["level", "logging.level"],
    ["format", "logging.format"],
    ["directory", "logging.directory"],
  ],
  log_bools: [
    ["console_enabled", "logging.console_enabled"],
    ["csv_enabled", "logging.csv_enabled"],
    ["file_enabled", "logging.file_enabled"],
    ["log_connectivity", "logging.log_connectivity"],
    ["log_feed_updates", "logging.log_feed_updates"],
    ["log_fills", "logging.log_fills"],
    ["log_hedge_signals", "logging.log_hedge_signals"],
    ["log_order_lifecycle", "logging.log_order_lifecycle"],
    ["log_position_updates", "logging.log_position_updates"],
    ["log_qo_validation", "logging.log_qo_validation"],
    ["log_strategy_decisions", "logging.log_strategy_decisions"],
    ["log_theo_updates", "logging.log_theo_updates"],
  ],
  hedge: {
    floats: [["initial_price", "hedge.initial_price"], ["threshold_ratio", "hedge.threshold_ratio"]],
    ints: [["multiplier", "hedge.multiplier"]],
    strings: [["provider", "hedge.provider"], ["symbol", "hedge.symbol"], ["webhook_url", "hedge.webhook_url"]],
    bools: [["enabled", "hedge.enabled"]],
  },
  ob_ints: [
    ["default_depth", "orderbook.default_depth"],
    ["l1_depth", "orderbook.l1_depth"],
    ["l2_depth", "orderbook.l2_depth"],
    ["max_depth", "orderbook.max_depth"],
  ],
  mm_desk_str: [["reference_orderbook_symbols_csv", "mm_desk.reference_orderbook_symbols (comma-separated)"]],
  api: {
    strings: [
      ["fill_poll_cursor_file", "api.fill_poll_cursor_file"],
      ["session_token", "api.session_token"],
      ["ws_endpoint", "api.ws_endpoint"],
    ],
    ints: [
      ["timeout_connect_ms", "api.timeout_connect_ms"],
      ["timeout_read_ms", "api.timeout_read_ms"],
      ["timeout_write_ms", "api.timeout_write_ms"],
    ],
  },
};
/** Parse "a.b.c" into ['a','b','c'] */
function ceParsePath(dotted) {
  return dotted.split(".");
}
function ceAppendScalarRow(mount, path, label, kind) {
  var wrap = document.createElement("div");
  wrap.className = "fld";
  var id = "ce_" + path.join("_");
  var lab = document.createElement("label");
  lab.setAttribute("for", id);
  lab.textContent = label;
  var inp = document.createElement("input");
  inp.id = id;
  inp.setAttribute("data-ce-path", JSON.stringify(path));
  inp.setAttribute("data-ce-kind", kind);
  if (kind === "int") {
    inp.type = "number";
    inp.step = "1";
  } else if (kind === "float") {
    inp.type = "number";
    inp.step = "any";
  } else {
    inp.type = "text";
    inp.autocomplete = "off";
  }
  wrap.appendChild(lab);
  wrap.appendChild(inp);
  mount.appendChild(wrap);
}
function ceAppendBoolRowMount(mount, path, label) {
  var wrap = document.createElement("div");
  wrap.className = "fld";
  var sp = document.createElement("span");
  sp.style.cssText = "display:block;font-size:11px;margin-bottom:6px;color:#8b949e";
  sp.textContent = label;
  var row = document.createElement("div");
  row.className = "ce-toggle-wrap";
  row.innerHTML =
    "<span class=\"ce-tog-lab\">Disabled</span>" +
    "<label class=\"ce-toggle\" title=\"Left = Disabled, right = Enabled\">" +
    "<input type=\"checkbox\" class=\"ce-bool-cb\" />" +
    "<span class=\"ce-toggle-slider\" aria-hidden=\"true\"></span></label>" +
    "<span class=\"ce-tog-lab ce-tog-lab-right\">Enabled</span>";
  var inp = row.querySelector("input.ce-bool-cb");
  if (inp) inp.setAttribute("data-ce-bool-path", JSON.stringify(path));
  wrap.appendChild(sp);
  wrap.appendChild(row);
  mount.appendChild(wrap);
}
function ceBuildMmMount() {
  var m = document.getElementById("ceMountMm");
  if (!m || m.getAttribute("data-ce-built")) return;
  m.setAttribute("data-ce-built", "1");
  var mm = "market_maker";
  var i;
  for (i = 0; i < CE_SPEC_MM.floats.length; i++) {
    var mf = CE_SPEC_MM.floats[i];
    ceAppendScalarRow(m, [mm, mf[0]], mf[1], "float");
  }
  for (i = 0; i < CE_SPEC_MM.ints.length; i++) {
    var mi = CE_SPEC_MM.ints[i];
    ceAppendScalarRow(m, [mm, mi[0]], mi[1], "int");
  }
  for (i = 0; i < CE_SPEC_MM.strings.length; i++) {
    var ms = CE_SPEC_MM.strings[i];
    ceAppendScalarRow(m, [mm, ms[0]], ms[1], "str");
  }
  for (i = 0; i < CE_SPEC_MM.bools.length; i++) {
    var mb = CE_SPEC_MM.bools[i];
    ceAppendBoolRowMount(m, [mm, mb[0]], "market_maker." + mb[0]);
  }
}
function ceBuildExternalMount() {
  var m = document.getElementById("ceMountExternal");
  if (!m || m.getAttribute("data-ce-built")) return;
  m.setAttribute("data-ce-built", "1");
  var ef = "external_feed";
  var i;
  for (i = 0; i < CE_SPEC_EF.ints.length; i++) {
    var row = CE_SPEC_EF.ints[i];
    ceAppendScalarRow(m, [ef, row[0]], row[1], "int");
  }
  for (i = 0; i < CE_SPEC_EF.strings.length; i++) {
    var sr = CE_SPEC_EF.strings[i];
    ceAppendScalarRow(m, [ef, sr[0]], sr[1], "str");
  }
  for (i = 0; i < CE_SPEC_EF.bools.length; i++) {
    var br = CE_SPEC_EF.bools[i];
    ceAppendBoolRowMount(m, [ef, br[0]], "external_feed." + br[0]);
  }
}
function ceBuildRiskMount() {
  var m = document.getElementById("ceMountRisk");
  if (!m || m.getAttribute("data-ce-built")) return;
  m.setAttribute("data-ce-built", "1");
  var i;
  for (i = 0; i < CE_SPEC_RISK.floats.length; i++) {
    var rf = CE_SPEC_RISK.floats[i];
    ceAppendScalarRow(m, ceParsePath(rf[1]), rf[1], "float");
  }
  for (i = 0; i < CE_SPEC_RISK.ints.length; i++) {
    var ri = CE_SPEC_RISK.ints[i];
    ceAppendScalarRow(m, ceParsePath(ri[1]), ri[1], "int");
  }
  for (i = 0; i < CE_SPEC_TR.floats.length; i++) {
    var tf = CE_SPEC_TR.floats[i];
    ceAppendScalarRow(m, ceParsePath(tf[1]), tf[1], "float");
  }
  for (i = 0; i < CE_SPEC_TR.ints.length; i++) {
    var ti = CE_SPEC_TR.ints[i];
    ceAppendScalarRow(m, ceParsePath(ti[1]), ti[1], "int");
  }
  for (i = 0; i < CE_SPEC_TR.bools.length; i++) {
    var tb = CE_SPEC_TR.bools[i];
    ceAppendBoolRowMount(m, ceParsePath(tb[1]), tb[1]);
  }
}
function ceBuildSystemMount() {
  var m = document.getElementById("ceMountSystem");
  if (!m || m.getAttribute("data-ce-built")) return;
  m.setAttribute("data-ce-built", "1");
  var i;
  var sb = CE_SPEC_SYS.startup_bools;
  for (i = 0; i < sb.length; i++) {
    ceAppendBoolRowMount(m, ceParsePath(sb[i][1]), sb[i][1]);
  }
  var si = CE_SPEC_SYS.startup_ints;
  for (i = 0; i < si.length; i++) {
    ceAppendScalarRow(m, ceParsePath(si[i][1]), si[i][1], "int");
  }
  var pi = CE_SPEC_SYS.perf_ints;
  for (i = 0; i < pi.length; i++) {
    ceAppendScalarRow(m, ceParsePath(pi[i][1]), pi[i][1], "int");
  }
  var wi = CE_SPEC_SYS.ws_ints;
  for (i = 0; i < wi.length; i++) {
    ceAppendScalarRow(m, ceParsePath(wi[i][1]), wi[i][1], "int");
  }
  var wb = CE_SPEC_SYS.ws_bools;
  for (i = 0; i < wb.length; i++) {
    ceAppendBoolRowMount(m, ceParsePath(wb[i][1]), wb[i][1]);
  }
  var fs = CE_SPEC_SYS.feed_str;
  for (i = 0; i < fs.length; i++) {
    ceAppendScalarRow(m, ceParsePath(fs[i][1]), fs[i][1], "str");
  }
  var fi = CE_SPEC_SYS.feed_ints;
  for (i = 0; i < fi.length; i++) {
    ceAppendScalarRow(m, ceParsePath(fi[i][1]), fi[i][1], "int");
  }
  var fb = CE_SPEC_SYS.feed_bools;
  for (i = 0; i < fb.length; i++) {
    ceAppendBoolRowMount(m, ceParsePath(fb[i][1]), fb[i][1]);
  }
  var ft = CE_SPEC_SYS.feat_bools;
  for (i = 0; i < ft.length; i++) {
    ceAppendBoolRowMount(m, ceParsePath(ft[i][1]), ft[i][1]);
  }
  var ls = CE_SPEC_SYS.log_str;
  for (i = 0; i < ls.length; i++) {
    ceAppendScalarRow(m, ceParsePath(ls[i][1]), ls[i][1], "str");
  }
  var lb = CE_SPEC_SYS.log_bools;
  for (i = 0; i < lb.length; i++) {
    ceAppendBoolRowMount(m, ceParsePath(lb[i][1]), lb[i][1]);
  }
  var hg = CE_SPEC_SYS.hedge;
  for (i = 0; i < hg.floats.length; i++) {
    ceAppendScalarRow(m, ceParsePath(hg.floats[i][1]), hg.floats[i][1], "float");
  }
  for (i = 0; i < hg.ints.length; i++) {
    ceAppendScalarRow(m, ceParsePath(hg.ints[i][1]), hg.ints[i][1], "int");
  }
  for (i = 0; i < hg.strings.length; i++) {
    ceAppendScalarRow(m, ceParsePath(hg.strings[i][1]), hg.strings[i][1], "str");
  }
  for (i = 0; i < hg.bools.length; i++) {
    ceAppendBoolRowMount(m, ceParsePath(hg.bools[i][1]), hg.bools[i][1]);
  }
  var ob = CE_SPEC_SYS.ob_ints;
  for (i = 0; i < ob.length; i++) {
    ceAppendScalarRow(m, ceParsePath(ob[i][1]), ob[i][1], "int");
  }
  ceAppendScalarRow(m, ["mm_desk", "reference_orderbook_symbols_csv"], "mm_desk.reference_orderbook_symbols (comma-separated)", "str");
  var ap = CE_SPEC_SYS.api;
  for (i = 0; i < ap.strings.length; i++) {
    ceAppendScalarRow(m, ceParsePath(ap.strings[i][1]), ap.strings[i][1], "str");
  }
  for (i = 0; i < ap.ints.length; i++) {
    ceAppendScalarRow(m, ceParsePath(ap.ints[i][1]), ap.ints[i][1], "int");
  }
}
function deskEditOverlayBuildIfNeeded() {
  if (CE_DYNAMIC_BUILT) return;
  CE_DYNAMIC_BUILT = true;
  try {
    ceBuildMmMount();
    ceBuildExternalMount();
    ceBuildRiskMount();
    ceBuildSystemMount();
  } catch (e) {
    console.warn("deskEditOverlayBuildIfNeeded", e);
  }
}
var DESK_EDIT_TAB_MAP = {
  trading: "deskEditPanelTrading",
  mm: "deskEditPanelMm",
  external: "deskEditPanelExternal",
  risk: "deskEditPanelRisk",
  system: "deskEditPanelSystem",
  docs: "deskEditPanelDocs",
};
function showDeskEditTab(name) {
  var showId = DESK_EDIT_TAB_MAP[name] || DESK_EDIT_TAB_MAP.trading;
  Object.keys(DESK_EDIT_TAB_MAP).forEach(function(k) {
    var el = document.getElementById(DESK_EDIT_TAB_MAP[k]);
    if (el) el.classList.toggle("hidden", DESK_EDIT_TAB_MAP[k] !== showId);
  });
  var nav = document.getElementById("deskEditTabNav");
  if (nav) {
    nav.querySelectorAll("button[data-desk-edit-tab]").forEach(function(b) {
      b.classList.toggle("active", b.getAttribute("data-desk-edit-tab") === name);
    });
  }
}
(function deskEditTabWire() {
  var nav = document.getElementById("deskEditTabNav");
  if (!nav) return;
  nav.addEventListener("click", function(ev) {
    var t = ev.target;
    if (!t || !t.getAttribute) return;
    var tab = t.getAttribute("data-desk-edit-tab");
    if (!tab) return;
    ev.preventDefault();
    showDeskEditTab(tab);
  });
})();
function hydrateDeskConfigEditorTabs(cfg) {
  if (!cfg || typeof cfg !== "object") return;
  deskEditOverlayBuildIfNeeded();
  var overlay = document.getElementById("deskEditModeOverlay");
  if (!overlay) return;
  overlay.querySelectorAll("[data-ce-path]").forEach(function(inp) {
    var raw = inp.getAttribute("data-ce-path");
    if (!raw) return;
    var path;
    try {
      path = JSON.parse(raw);
    } catch (e) {
      return;
    }
    if (!Array.isArray(path)) return;
    var v = ceNestedGet(cfg, path);
    if (v === undefined || v === null) return;
    var kind = inp.getAttribute("data-ce-kind") || "str";
    if (kind === "int") inp.value = String(Math.trunc(Number(v)));
    else if (kind === "float") inp.value = String(Number(v));
    else inp.value = String(v);
  });
  overlay.querySelectorAll("input.ce-bool-cb[data-ce-bool-path]").forEach(function(cb) {
    var raw = cb.getAttribute("data-ce-bool-path");
    if (!raw) return;
    var path;
    try {
      path = JSON.parse(raw);
    } catch (e) {
      return;
    }
    var bv = ceNestedGet(cfg, path);
    if (typeof bv !== "boolean") return;
    cb.checked = bv;
  });
  var refCsv = document.getElementById("ce_mm_desk_reference_orderbook_symbols_csv");
  if (!refCsv) {
    refCsv = overlay.querySelector("[data-ce-path='[\"mm_desk\",\"reference_orderbook_symbols_csv\"]']");
  }
  var rs = ceNestedGet(cfg, ["mm_desk", "reference_orderbook_symbols"]);
  if (refCsv && Array.isArray(rs)) refCsv.value = rs.join(", ");
  function setJsonTa(id, pathArr) {
    var ta = document.getElementById(id);
    var ob = ceNestedGet(cfg, pathArr);
    if (ta && ob !== undefined && ob !== null && typeof ob === "object") {
      try {
        ta.value = JSON.stringify(ob, null, 2);
      } catch (e) {}
    }
  }
  setJsonTa("ce_json_external_feed_fix", ["external_feed", "fix"]);
  setJsonTa("ce_json_websocket_channels", ["websocket", "channels"]);
  setJsonTa("ce_json_feed_historical", ["feed", "historical"]);
  var wl = document.getElementById("ce_json_trading_watchlist");
  if (wl) {
    var wv = ceNestedGet(cfg, ["trading", "watchlist"]);
    if (Array.isArray(wv)) {
      try {
        wl.value = JSON.stringify(wv, null, 2);
      } catch (e) {}
    }
  }
}
function gatherDeskConfigMerge() {
  var out = {};
  var overlay = document.getElementById("deskEditModeOverlay");
  if (!overlay) return out;
  overlay.querySelectorAll("[data-ce-path]").forEach(function(inp) {
    var raw = inp.getAttribute("data-ce-path");
    if (!raw) return;
    var path;
    try {
      path = JSON.parse(raw);
    } catch (e) {
      return;
    }
    if (!Array.isArray(path)) return;
    var kind = inp.getAttribute("data-ce-kind") || "str";
    var t = String(inp.value || "").trim();
    if (t === "") return;
    if (kind === "int") {
      if (!/^-?\d+$/.test(t)) return;
      ceNestedSet(out, path, parseInt(t, 10));
    } else if (kind === "float") {
      var f = parseFloat(t);
      if (!isFinite(f)) return;
      ceNestedSet(out, path, f);
    } else {
      if (path[0] === "mm_desk" && path[1] === "reference_orderbook_symbols_csv") {
        var arr = t.split(/[,\s]+/).map(function(x) { return x.trim(); }).filter(Boolean);
        ceNestedSet(out, ["mm_desk", "reference_orderbook_symbols"], arr);
      } else {
        ceNestedSet(out, path, t);
      }
    }
  });
  overlay.querySelectorAll("input.ce-bool-cb[data-ce-bool-path]").forEach(function(cb) {
    var raw = cb.getAttribute("data-ce-bool-path");
    if (!raw) return;
    var path;
    try {
      path = JSON.parse(raw);
    } catch (e) {
      return;
    }
    ceNestedSet(out, path, !!cb.checked);
  });
  function parseTa(id, path) {
    var ta = document.getElementById(id);
    if (!ta) return;
    var tx = String(ta.value || "").trim();
    if (!tx) return;
    try {
      var parsed = JSON.parse(tx);
      ceNestedSet(out, path, parsed);
    } catch (e) {
      throw new Error(id + ": invalid JSON — " + (e && e.message ? e.message : e));
    }
  }
  try {
    parseTa("ce_json_external_feed_fix", ["external_feed", "fix"]);
    parseTa("ce_json_websocket_channels", ["websocket", "channels"]);
    parseTa("ce_json_feed_historical", ["feed", "historical"]);
    parseTa("ce_json_trading_watchlist", ["trading", "watchlist"]);
  } catch (e) {
    throw e;
  }
  return out;
}
/** Copy sidebar → modal (same keys as gatherPayload). */
function deskEditModalCopyFromSidebar() {
  var pairs = [
    ["axSym", "deskEditModalAx"],
    ["refSym", "deskEditModalRef"],
    ["rest", "deskEditModalRest"],
    ["apiKey", "deskEditModalApiKey"],
    ["apiSec", "deskEditModalApiSec"],
    ["w", "deskEditModalW"],
    ["q", "deskEditModalQ"],
    ["mx", "deskEditModalMx"],
    ["ap", "deskEditModalAp"],
    ["at", "deskEditModalAt"],
    ["mmMinTheoTicks", "deskEditModalMinTheo"],
    ["mmPriceTick", "deskEditModalPriceTick"],
    ["mmPricingTick", "deskEditModalPricingTick"],
    ["mmCurReload", "deskEditModalCurReload"],
    ["mrc", "deskEditModalMrc"],
    ["mmReloadNonce", "deskEditModalReloadNonce"],
  ];
  var st = window.__mmDeskLastState || {};
  for (var i = 0; i < pairs.length; i++) {
    var a = document.getElementById(pairs[i][0]);
    var b = document.getElementById(pairs[i][1]);
    if (a && b) b.value = deskRoContent(a);
  }
  var mk = document.getElementById("deskEditModalApiKey");
  if (mk) mk.value = st.prefill_api_key != null ? String(st.prefill_api_key) : "";
  var ms = document.getElementById("deskEditModalApiSec");
  if (ms) ms.value = st.prefill_api_secret != null ? String(st.prefill_api_secret) : "";
  ceSetDeskTradingToggle("deskEditCbRequoteTheo", st.mm_requote_on_theo_move !== false);
  ceSetDeskTradingToggle("deskEditCbMrcFillOnly", st.mm_reload_cycles_count_fill_only !== false);
}
/** Copy modal → Trading sidebar read-only snapshot. */
function deskEditModalCopyToSidebar() {
  var pairs = [
    ["deskEditModalAx", "axSym"],
    ["deskEditModalRef", "refSym"],
    ["deskEditModalRest", "rest"],
    ["deskEditModalApiKey", "apiKey"],
    ["deskEditModalW", "w"],
    ["deskEditModalQ", "q"],
    ["deskEditModalMx", "mx"],
    ["deskEditModalAp", "ap"],
    ["deskEditModalAt", "at"],
    ["deskEditModalMinTheo", "mmMinTheoTicks"],
    ["deskEditModalPriceTick", "mmPriceTick"],
    ["deskEditModalPricingTick", "mmPricingTick"],
    ["deskEditModalCurReload", "mmCurReload"],
    ["deskEditModalMrc", "mrc"],
    ["deskEditModalReloadNonce", "mmReloadNonce"],
  ];
  for (var i = 0; i < pairs.length; i++) {
    var a = document.getElementById(pairs[i][0]);
    var b = document.getElementById(pairs[i][1]);
    if (a && b) deskRoSet(b, a.value);
  }
  var apis = document.getElementById("deskEditModalApiSec");
  if (apis) {
    deskRoSetById("apiSec", String(apis.value || "").length ? "•••••••• (stored)" : "—");
  }
  var rqOn = ceGetDeskTradingToggle("deskEditCbRequoteTheo");
  if (rqOn !== undefined) {
    deskRoSetById("mmRequoteTheo", rqOn ? "Enabled" : "Disabled");
  }
  var foOn = ceGetDeskTradingToggle("deskEditCbMrcFillOnly");
  if (foOn !== undefined) {
    deskRoSetById("mrcFillOnly", foOn ? "Yes" : "No");
  }
  try { syncParamsForm("1"); } catch (e) {}
}
function showDeskEditModeOverlay(s) {
  mmDeskEndManualOrderUiMode();
  var el = document.getElementById("deskEditModeOverlay");
  if (!el) return;
  deskEditOverlayBuildIfNeeded();
  deskEditModalCopyFromSidebar();
  var cfg = (s && s.config_editor) || (window.__mmDeskLastState && window.__mmDeskLastState.config_editor);
  hydrateDeskConfigEditorTabs(cfg || {});
  showDeskEditTab("trading");
  try {
    document.querySelectorAll("#deskEditTabNav button").forEach(function(btn) {
      btn.style.display = "";
    });
  } catch (e0) { console.warn(e0); }
  el.classList.remove("hidden");
  el.setAttribute("aria-hidden", "false");
}
function hideDeskEditModeOverlay() {
  var el = document.getElementById("deskEditModeOverlay");
  if (!el) return;
  el.classList.add("hidden");
  el.setAttribute("aria-hidden", "true");
  mmDeskEndManualOrderUiMode();
}
function showDeskEditResumeToast(msg) {
  var t = document.getElementById("deskEditResumeToast");
  if (!t) return;
  t.textContent = msg || "Config saved — desk_edit_mode is false. C++ will resume MM (theo/timer/fills) on the next config read.";
  t.classList.remove("hidden");
  clearTimeout(window.__deskEditToastTimer);
  window.__deskEditToastTimer = setTimeout(function() {
    t.classList.add("hidden");
  }, 5200);
}
function paintDeskEditModeUi(s) {
  var on = mmDeskEditModeActive(s);
  var btn = document.getElementById("btnDeskEditEnter");
  if (btn) {
    btn.textContent = "Enter edit mode";
    btn.className = "btn btn-desk-edit-green";
    btn.style.width = "100%";
    btn.disabled = false;
    btn.removeAttribute("aria-disabled");
    btn.title = on
      ? "Reopens the popup. C++ stays paused until you Submit there (or use the main sidebar persist button below)."
      : "Sets desk_edit_mode and opens the translucent editor.";
  }
}
function renderMmPreview(s) {
  var hideMain = !!(s && s.reference_provider === "neon_fix" && s.neon_reference_from_cpp_file);
  var mainBlock = document.getElementById("mmPreviewMainBlock");
  var neonNote = document.getElementById("mmPreviewNeonNote");
  var blurb = document.getElementById("mmPreviewSectionBlurb");
  if (mainBlock) mainBlock.style.display = hideMain ? "none" : "";
  if (neonNote) neonNote.style.display = hideMain ? "block" : "none";
  if (blurb) blurb.style.display = hideMain ? "none" : "block";
  if (hideMain) {
    var tb0 = document.getElementById("mmPreviewBody");
    var meta0 = document.getElementById("mmPreviewMeta");
    if (tb0) tb0.innerHTML = "";
    if (meta0) meta0.textContent = "";
    return;
  }
  var tb = document.getElementById("mmPreviewBody");
  var meta = document.getElementById("mmPreviewMeta");
  if (!tb) return;
  var p = (s && s.mm_order_preview) ? s.mm_order_preview : {};
  var metaLine = fillMmPreviewTbody(tb, p);
  if (meta) meta.textContent = metaLine;
  renderMmPositionGateBanner(s);
}
function applyMmStrategyFieldsFromState(s) {
  if (!s) return;
  if (mmDeskEditModeActive(s)) return;
  function setVal(id, val) {
    if (val === undefined || val === null) return;
    deskRoSetById(id, val);
  }
  var rqOn = s.mm_requote_on_theo_move !== false;
  deskRoSetById("mmRequoteTheo", rqOn ? "Enabled" : "Disabled");
  setVal("mmMinTheoTicks", s.mm_min_theo_move_ticks_to_requote != null ? String(s.mm_min_theo_move_ticks_to_requote) : null);
  setVal("mmPriceTick", s.price_tick != null ? String(s.price_tick) : null);
  setVal("mmPricingTick", s.mm_pricing_tick != null ? String(s.mm_pricing_tick) : null);
  var numPairs = [
    ["w", s.mm_width], ["q", s.mm_order_size], ["mx", s.mm_max_position],
    ["ap", s.mm_adjust_position], ["at", s.mm_adjust_ticks], ["mmCurReload", s.mm_current_reload_count],
    ["mrc", s.mm_max_reload_cycles],
  ];
  for (var i = 0; i < numPairs.length; i++) {
    setVal(numPairs[i][0], numPairs[i][1]);
  }
  deskRoSetById("mrcFillOnly", s.mm_reload_cycles_count_fill_only !== false ? "Yes" : "No");
  setVal("mmReloadNonce", s.mm_reload_limit_reset_nonce != null ? String(s.mm_reload_limit_reset_nonce) : null);
  syncParamsForm("1");
}
function patchEmptyFormFieldsFromState(s) {
  if (!s) return;
  if (mmDeskEditModeActive(s)) return;
  function setIfEmpty(id, val) {
    if (val === undefined || val === null) return;
    var el = document.getElementById(id);
    if (!el) return;
    if (!String(deskRoContent(el) || "").trim()) deskRoSet(el, String(val));
  }
  setIfEmpty("axSym", s.ax_symbol);
  setIfEmpty("refSym", s.ref_symbol);
  setIfEmpty("rest", s.rest_endpoint);
  var numPairs = [
    ["w", s.mm_width], ["q", s.mm_order_size], ["mx", s.mm_max_position],
    ["ap", s.mm_adjust_position], ["at", s.mm_adjust_ticks], ["mmCurReload", s.mm_current_reload_count],
    ["mrc", s.mm_max_reload_cycles],
  ];
  for (var i = 0; i < numPairs.length; i++) {
    var id = numPairs[i][0], val = numPairs[i][1];
    if (val === undefined || val === null) continue;
    setIfEmpty(id, val);
  }
  var fo = document.getElementById("mrcFillOnly");
  if (fo && s.mm_reload_cycles_count_fill_only === true && deskRoContent(fo) === "No") deskRoSet(fo, "Yes");
  setIfEmpty("mmReloadNonce", s.mm_reload_limit_reset_nonce != null ? String(s.mm_reload_limit_reset_nonce) : "");
  syncParamsForm("1");
}
function hydrateDeskForms(s) {
  if (!s) return;
  try {
    if (!window.__mmDeskHydratedFromPoll) {
      applyFromState(s);
      window.__mmDeskHydratedFromPoll = true;
    } else {
      applyMmStrategyFieldsFromState(s);
      patchEmptyFormFieldsFromState(s);
    }
  } catch (e) {
    console.warn("mmDesk: hydrate failed — sidebar may stay empty; fix Console errors", e);
  }
}
function validateMmStrategyFromPrimaryForm() {
  function pintFromId(id) {
    var t = String(deskRoContent(document.getElementById(id)) || "").trim();
    if (!t) return NaN;
    return parseInt(t, 10);
  }
  // Quantity-denominated fields (order size / max position / adjust position) are
  // capped only by the engine's 32-bit int storage, not a product limit — the exchange
  // enforces margin. 2e9 is the int32-safe ceiling so large (e.g. JPY) sizes are allowed.
  var QTY_MAX = 2000000000;
  var spec = [
    ["w", "Width (bps)", 1, 1e9],
    ["q", "Order size", 1, QTY_MAX],
    ["mx", "Max position", 1, QTY_MAX],
    ["ap", "Adjust position", 1, QTY_MAX],
    ["at", "Adjust ticks", 1, 100],
  ];
  if (document.getElementById("mmCurReload")) spec.push(["mmCurReload", "Current reload count", 0, 1000000]);
  if (document.getElementById("mrc")) spec.push(["mrc", "Max reload cycles", 0, 1000000]);
  if (document.getElementById("mmMinTheoTicks")) spec.push(["mmMinTheoTicks", "Min drift ticks", 1, 1000000]);
  for (var i = 0; i < spec.length; i++) {
    var id = spec[i][0], label = spec[i][1], lo = spec[i][2], hi = spec[i][3];
    if (!document.getElementById(id)) continue;
    var v = pintFromId(id);
    if (!isFinite(v)) return label + " is required";
    if (v < lo || v > hi) return label + " must be between " + lo + " and " + hi + " inclusive";
  }
  function pfloatOk(id, label) {
    var t = String(deskRoContent(document.getElementById(id)) || "").trim();
    if (!t) return null;
    var x = parseFloat(t);
    if (!isFinite(x) || x <= 0) return label + " must be a positive number";
    return null;
  }
  var ePt = pfloatOk("mmPriceTick", "price_tick");
  if (ePt) return ePt;
  var ePg = pfloatOk("mmPricingTick", "pricing_tick");
  if (ePg) return ePg;
  return null;
}
function deskBoolFromRoId(id) {
  var t = String(deskRoContent(document.getElementById(id)) || "").toLowerCase();
  if (t.indexOf("enable") >= 0 || t === "on" || t === "yes" || t === "true") return true;
  if (t.indexOf("disable") >= 0 || t === "off" || t === "no" || t === "false") return false;
  return true;
}
function gatherPayload() {
  syncParamsForm("1");
  var verr = validateMmStrategyFromPrimaryForm();
  if (verr) throw new Error(verr);
  function pint(id) {
    return parseInt(String(deskRoContent(document.getElementById(id)) || "").trim(), 10);
  }
  var st = window.__mmDeskLastState || {};
  var secEl = document.getElementById("deskEditModalApiSec");
  var secFromModal = secEl && String(secEl.value || "").length > 0 ? String(secEl.value) : "";
  var secSidebar = deskRoContent(document.getElementById("apiSec"));
  var apiSecretOut = secFromModal
    ? secFromModal
    : (secSidebar.indexOf("•") >= 0 || secSidebar.indexOf("stored") >= 0)
      ? String(st.prefill_api_secret != null ? st.prefill_api_secret : "")
      : secSidebar;
  var out = {
    rest_endpoint: deskRoContent(document.getElementById("rest")),
    ax_symbol: mmDeskActiveAxSymbol(st),
    ref_symbol: (function() {
      var axNow = mmDeskActiveAxSymbol(st);
      var row = mmDeskFindInstrumentRow(st, axNow);
      if (row) {
        var rr = String(row.reference_fix_symbol || row.theo_symbol || "").trim();
        if (rr) return rr;
      }
      return deskRoContent(document.getElementById("refSym"));
    })(),
    api_key: deskRoContent(document.getElementById("apiKey")) || String(st.prefill_api_key != null ? st.prefill_api_key : ""),
    api_secret: apiSecretOut,
    width: pint("w"),
    order_size: pint("q"),
    max_position: pint("mx"),
    adjust_position: pint("ap"),
    adjust_ticks: pint("at"),
  };
  if (document.getElementById("mrc")) {
    if (document.getElementById("mmCurReload")) out.current_reload_count = pint("mmCurReload");
    out.max_reload_cycles = pint("mrc");
    out.reload_cycles_count_fill_only = deskBoolFromRoId("mrcFillOnly");
    out.reload_limit_reset_nonce = deskRoContent(document.getElementById("mmReloadNonce"));
  }
  if (document.getElementById("mmRequoteTheo")) {
    out.requote_on_theo_move = deskBoolFromRoId("mmRequoteTheo");
    out.min_theo_move_ticks_to_requote = pint("mmMinTheoTicks");
    var ptk = deskRoContent(document.getElementById("mmPriceTick"));
    var ptg = deskRoContent(document.getElementById("mmPricingTick"));
    if (ptk) out.price_tick = parseFloat(ptk);
    if (ptg) out.pricing_tick = parseFloat(ptg);
  }
  return out;
}
function mmDeskInstrumentChoicesFromState(st) {
  return mmDeskUnifiedLegRows(st).map(function(r) {
    return { ax: r.ax, ref: r.ref };
  });
}
function mmDeskEndManualOrderUiMode() {
  if (String(window.__mmDeskOpenMode || "") !== "manual") return;
  window.__mmDeskOpenMode = "";
}
async function postOrdersMmMove(body) {
  try {
    const j = await api("/api/desk/orders_mm_move", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!j || !j.ok) {
      alert((j && j.error) || (j && j.message) || "orders_mm_move failed");
      return false;
    }
    return true;
  } catch (e) {
    alert(String((e && e.message) || e));
    return false;
  }
}

/** Case-insensitive match of AX symbol to a ``products`` key in ``orders.json``. */
function mmDeskResolveOrdersJsonProductKey(products, ax) {
  var p = products || {};
  var a = String(ax || "").trim();
  if (!a) return null;
  if (Object.prototype.hasOwnProperty.call(p, a)) return a;
  var u = a.toUpperCase();
  for (var k in p) {
    if (!Object.prototype.hasOwnProperty.call(p, k)) continue;
    if (String(k).trim().toUpperCase() === u) return k;
  }
  return null;
}

/**
 * Symbols for the MM move panel: same order as depth / multiselect (``mmDeskMasterAxSymbolsOrderedFromUi``),
 * then any ``orders.json``-only product keys not already listed (so the dropdown matches visible orderbooks).
 */
function mmDeskMmMovePanelSymbolOrder(st0, products) {
  var ordered = [];
  var seenU = {};
  function pushAx(axRaw) {
    var a = String(axRaw || "").trim();
    if (!a) return;
    var u = a.toUpperCase();
    if (seenU[u]) return;
    seenU[u] = true;
    ordered.push(a);
  }
  var master = mmDeskMasterAxSymbolsOrderedFromUi(st0 || {});
  for (var i = 0; i < master.length; i++) pushAx(master[i]);
  var pk0 = Object.keys(products || {});
  for (var j = 0; j < pk0.length; j++) pushAx(pk0[j]);
  return ordered;
}

function mmDeskFormatMmMoveSideCell(live, sideObj) {
  var bSide = sideObj || {};
  if (bSide.order_id) {
    return bSide.price + " × " + bSide.qty + "  (" + bSide.order_id + ")";
  }
  if (bSide.exchange_oid) {
    return "oid " + String(bSide.exchange_oid).substring(0, 18) + (String(bSide.exchange_oid).length > 18 ? "…" : "") + " (json · pending AX)";
  }
  return live ? "—" : "(no AX yet)";
}

function mmDeskOpenOrdersMmMovePanel(st0) {
  st0 = st0 || {};
  var ordersCfg = st0.mm_orders_config || null;
  var products = (ordersCfg && ordersCfg.products && typeof ordersCfg.products === "object") ? ordersCfg.products : {};
  var productKeys = mmDeskMmMovePanelSymbolOrder(st0, products);
  if (!productKeys.length) {
    alert(
      "No AX symbols in the depth list and no products in orders.json. " +
        "Tick symbols in AX depth (or configure ax_book_symbols), and add stacks via Place manual order when needed."
    );
    return;
  }
  var ov = document.createElement("div");
  ov.style.cssText = "position:fixed;inset:0;background:rgba(7,12,20,.88);z-index:21000;display:flex;align-items:center;justify-content:center;overflow:auto";
  var card = document.createElement("div");
  card.style.cssText = "width:min(920px,96vw);max-height:88vh;overflow:auto;background:#0f1724;border:1px solid #2a3548;border-radius:12px;padding:12px;color:#e6edf3";
  var h2 = document.createElement("h3");
  h2.style.margin = "0 0 4px 0";
  h2.style.color = "#fca5a5";
  h2.textContent = "Pause / resume MM placement (orders.json)";
  card.appendChild(h2);
  var hint = document.createElement("p");
  hint.className = "muted";
  hint.style.fontSize = "12px";
  hint.style.margin = "0 0 10px 0";
  hint.textContent = "Global toggle sets products[AX].mm_move_all_enabled. Each row sets stacks[].mm_move_enabled. Effective move requires both true. C++ cancels venue legs when a stack goes paused; resume re-adopts from orders.json on the next cycles.";
  card.appendChild(hint);

  var rowSel = document.createElement("p");
  rowSel.className = "fld";
  var labSel = document.createElement("label");
  labSel.textContent = "Instrument (depth / book order; matches orders.json when a product exists)";
  labSel.style.display = "block";
  labSel.style.marginBottom = "4px";
  rowSel.appendChild(labSel);
  var prodSel = document.createElement("select");
  prodSel.style.cssText = "width:100%;padding:6px;background:#0b1220;color:#e6edf3;border:1px solid #30405a;border-radius:6px";
  var phOpt = document.createElement("option");
  phOpt.value = "";
  phOpt.textContent = "— select a product —";
  prodSel.appendChild(phOpt);
  for (var pi = 0; pi < productKeys.length; pi++) {
    var axUi = productKeys[pi];
    var pkCanon = mmDeskResolveOrdersJsonProductKey(products, axUi);
    var prod0 = pkCanon ? (products[pkCanon] || {}) : {};
    var nStacks = Array.isArray(prod0.stacks) ? prod0.stacks.length : 0;
    var opt = document.createElement("option");
    opt.value = axUi;
    opt.textContent = pkCanon
      ? pkCanon + "  ·  " + nStacks + " stack" + (nStacks === 1 ? "" : "s")
      : axUi + "  ·  (no orders.json product yet)";
    prodSel.appendChild(opt);
  }
  rowSel.appendChild(prodSel);
  card.appendChild(rowSel);

  var tableHost = document.createElement("div");
  tableHost.style.marginTop = "8px";
  card.appendChild(tableHost);

  function renderForAx(ax) {
    tableHost.innerHTML = "";
    if (!ax) return;
    var pkUse = mmDeskResolveOrdersJsonProductKey(products, ax) || (products[ax] ? ax : null);
    var hasBucket = !!(pkUse && products[pkUse] && typeof products[pkUse] === "object");
    var prod = hasBucket ? products[pkUse] : {};
    var stacks = Array.isArray(prod.stacks) ? prod.stacks.slice() : [];
    var prodAll = prod.mm_move_all_enabled !== false;
    if (!hasBucket) {
      var nb = document.createElement("p");
      nb.className = "muted";
      nb.style.fontSize = "12px";
      nb.style.lineHeight = "1.45";
      nb.textContent =
        "This symbol is in your AX depth / book list but there is no matching ``products[\"…\"]`` entry in orders.json yet. " +
        "Pause/resume flags apply per product row there — use Place manual order to add a stack for " +
        ax +
        " first.";
      tableHost.appendChild(nb);
      return;
    }
    var sub = document.createElement("p");
    sub.className = "muted";
    sub.style.fontSize = "11px";
    sub.textContent = "theo_source = " + (prod.theo_source || "—") + "  ·  mm_move_all_enabled = " + (prodAll ? "true" : "false");
    tableHost.appendChild(sub);

    var rowG = document.createElement("div");
    rowG.style.cssText = "display:flex;align-items:center;justify-content:space-between;gap:10px;margin:10px 0;padding:10px;background:#0b1220;border-radius:8px;border:1px solid #30405a";
    var lg = document.createElement("div");
    lg.style.fontWeight = "600";
    lg.textContent = "All stacks on " + pkUse;
    var bg = document.createElement("button");
    bg.type = "button";
    bg.className = "btn " + (prodAll ? "btn-mm-orders-pause" : "btn-mm-orders-resume");
    bg.textContent = prodAll ? "Pause all (global)" : "Resume all (global)";
    bg.onclick = async function() {
      if (!await postOrdersMmMove({ ax_symbol: pkUse, scope: "global", mm_move_all_enabled: !prodAll })) return;
      try { document.body.removeChild(ov); } catch (e) {}
      refresh();
    };
    rowG.appendChild(lg);
    rowG.appendChild(bg);
    tableHost.appendChild(rowG);

    if (!stacks.length) {
      var p1 = document.createElement("p");
      p1.className = "muted";
      p1.textContent = "No stacks for " + pkUse + " in orders.json.";
      tableHost.appendChild(p1);
      return;
    }
    var liveSnap = (st0 && st0.live_orders) ? st0.live_orders : null;
    var liveByStackId = {};
    if (liveSnap && Array.isArray(liveSnap.orders)) {
      for (var li = 0; li < liveSnap.orders.length; li++) {
        var lr = liveSnap.orders[li];
        if (!lr) continue;
        var sidL = String(lr.stack_id || lr.request_id || "");
        if (sidL) liveByStackId[sidL] = lr;
      }
    }
    var tb = document.createElement("table");
    tb.className = "data";
    tb.style.width = "100%";
    var hdr = document.createElement("tr");
    ["stack_id", "mm_move (stack)", "effective", "bid", "ask", ""].forEach(function(t) {
      var th = document.createElement("th"); th.textContent = t; hdr.appendChild(th);
    });
    tb.appendChild(hdr);
    for (var si = 0; si < stacks.length; si++) {
      var s = stacks[si] || {};
      var tr = document.createElement("tr");
      function td(text) { var c = document.createElement("td"); c.textContent = String(text == null ? "—" : text); tr.appendChild(c); return c; }
      var sid = String(s.id || "");
      td(sid.substring(0, 14) || "—");
      var stMove = s.mm_move_enabled !== false;
      td(stMove ? "on" : "off");
      var eff = prodAll && stMove;
      var tdE = td(eff ? "moving" : "paused");
      if (!eff) tdE.style.color = "#ff8a8a";
      var live = liveByStackId[sid] || null;
      var bSide = (live && live.sides && live.sides.bid) || {};
      var aSide = (live && live.sides && live.sides.ask) || {};
      td(mmDeskFormatMmMoveSideCell(!!live, bSide));
      td(mmDeskFormatMmMoveSideCell(!!live, aSide));
      var tdA = document.createElement("td");
      var aBtn = document.createElement("button");
      aBtn.type = "button";
      aBtn.className = "btn " + (eff ? "btn-mm-orders-pause" : "btn-mm-orders-resume");
      aBtn.textContent = eff ? "Pause stack" : "Resume stack";
      (function(stackId, axs, nextOn) {
        aBtn.onclick = async function() {
          if (!(await postOrdersMmMove({ ax_symbol: axs, scope: "stack", stack_id: stackId, mm_move_enabled: nextOn }))) return;
          try { document.body.removeChild(ov); } catch (e2) {}
          refresh();
        };
      })(sid, pkUse, !stMove);
      tdA.appendChild(aBtn);
      tr.appendChild(tdA);
      tb.appendChild(tr);
    }
    tableHost.appendChild(tb);
  }

  prodSel.onchange = function() { renderForAx(String(prodSel.value || "")); };
  var bc = document.createElement("button");
  bc.className = "btn";
  bc.textContent = "Close";
  bc.style.marginTop = "12px";
  bc.onclick = function() { try { document.body.removeChild(ov); } catch (e) {} };
  card.appendChild(bc);
  ov.appendChild(card);
  document.body.appendChild(ov);
  if (productKeys.length === 1) {
    prodSel.value = productKeys[0];
    renderForAx(productKeys[0]);
  }
}

function mmDeskPromptInstrumentSelection(st, opts) {
  opts = opts || {};
  var rows = mmDeskUnifiedLegRows(st);
  if (!rows.length) return Promise.resolve(null);
  if (rows.length === 1) return Promise.resolve({ ax: rows[0].ax, ref: rows[0].ref });
  return new Promise(function(resolve) {
    var ov = document.createElement("div");
    ov.style.cssText = "position:fixed;inset:0;background:rgba(7,12,20,.72);z-index:20000;display:flex;align-items:center;justify-content:center";
    var card = document.createElement("div");
    card.style.cssText = "width:min(560px,92vw);background:#0f1724;border:1px solid #2a3548;border-radius:12px;padding:14px;box-shadow:0 12px 30px rgba(0,0,0,.45)";
    var title = document.createElement("div");
    title.style.cssText = "font-weight:700;color:#e6edf3;margin-bottom:8px";
    title.textContent = opts.title != null ? String(opts.title) : "Select instrument for edit mode";
    var sub = document.createElement("div");
    sub.style.cssText = "color:#8b9cb3;font-size:12px;margin-bottom:10px";
    var subMain = opts.subtitle != null ? String(opts.subtitle) : "Pick which MM leg this applies to.";
    sub.textContent =
      subMain +
      " Same order as AX depth multiselect (requote ∩ MM instruments).";
    var sel = document.createElement("select");
    sel.style.cssText = "width:100%;padding:8px;background:#0b1220;color:#e6edf3;border:1px solid #30405a;border-radius:8px";
    var activeAx = String(mmDeskActiveAxSymbol(st) || "").trim().toUpperCase();
    var selIdx = 0;
    for (var i = 0; i < rows.length; i++) {
      var opt = document.createElement("option");
      opt.value = String(i);
      opt.textContent = mmDeskFormatLegOptionLabel(rows[i]);
      sel.appendChild(opt);
      if (activeAx && String(rows[i].ax || "").trim().toUpperCase() === activeAx) selIdx = i;
    }
    sel.value = String(selIdx);
    var row = document.createElement("div");
    row.style.cssText = "display:flex;justify-content:flex-end;gap:8px;margin-top:12px";
    var bCancel = document.createElement("button");
    bCancel.type = "button";
    bCancel.className = "btn";
    bCancel.textContent = "Cancel";
    var bOk = document.createElement("button");
    bOk.type = "button";
    bOk.className = "btn btn-primary";
    bOk.textContent = "Continue";
    row.appendChild(bCancel);
    row.appendChild(bOk);
    card.appendChild(title);
    card.appendChild(sub);
    card.appendChild(sel);
    card.appendChild(row);
    ov.appendChild(card);
    function done(v) {
      try { document.body.removeChild(ov); } catch (e) {}
      resolve(v);
    }
    bCancel.onclick = function() { done(null); };
    bOk.onclick = function() {
      var idx = parseInt(String(sel.value || "0"), 10);
      if (!isFinite(idx) || idx < 0 || idx >= rows.length) { done(null); return; }
      var picked = rows[idx] || {};
      done({ ax: picked.ax, ref: picked.ref });
    };
    ov.addEventListener("click", function(ev) { if (ev.target === ov) done(null); });
    document.body.appendChild(ov);
    sel.focus();
  });
}
function mmDeskSetActiveInstrument(ax, ref) {
  if (ax) deskRoSetById("axSym", ax);
  if (ref != null) deskRoSetById("refSym", ref);
}
function mmDeskActiveAxSymbol(st) {
  var ax = String(deskRoContent(document.getElementById("axSym")) || "").trim();
  if (!ax && st && st.ax_symbol) ax = String(st.ax_symbol || "").trim();
  return ax;
}
function mmDeskFindInstrumentRow(st, ax) {
  var want = String(ax || "").trim().toUpperCase();
  if (!want || !st || !Array.isArray(st.mm_instruments)) return null;
  for (var i = 0; i < st.mm_instruments.length; i++) {
    var r = st.mm_instruments[i] || {};
    var got = String(r.symbol || r.ax_symbol || "").trim().toUpperCase();
    if (got && got === want) return r;
  }
  return null;
}
function mmDeskEffectiveGateState(st) {
  st = st || {};
  var ax = mmDeskActiveAxSymbol(st);
  /* Single configured MM leg: never use a drifted sidebar AX (e.g. depth multiselect) for mm_gate —
     POST must target market_maker.instruments[].symbol or legacy mm.symbol. */
  var choices = mmDeskInstrumentChoicesFromState(st);
  if (choices.length === 1) {
    var sole = String((choices[0] && choices[0].ax) || "").trim();
    if (sole) ax = sole;
  }
  var onTheo = st == null ? true : st.mm_requote_on_theo_move !== false;
  var onOrd = st == null ? true : st.mm_orders_enabled !== false;
  var row = mmDeskFindInstrumentRow(st || {}, ax);
  if (row && typeof row.requote_on_theo_move === "boolean") onTheo = row.requote_on_theo_move;
  if (row && typeof row.mm_orders_enabled === "boolean") onOrd = row.mm_orders_enabled;
  return { ax: ax, requote_on_theo_move: !!onTheo, mm_orders_enabled: !!onOrd };
}
function paintMmGateButtons(s) {
  if (!s) return;
  var eff = mmDeskEffectiveGateState(s);
  var bOrd = document.getElementById("btnMmOrdersGate");
  if (bOrd) {
    bOrd.textContent = "Pause/Resume MM placement";
    bOrd.className = "btn btn-mm-orders-pause";
    bOrd.title =
      "Open orders.json mm_move controls (per stack + global). Config master mm_orders_enabled=" +
      (eff.mm_orders_enabled ? "true" : "false") +
      " for " +
      (eff.ax || "—") +
      " (POST /api/desk/mm_gate).";
  }
}
async function postMmGate(patch) {
  try {
    const j = await api("/api/desk/mm_gate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    });
    if (!j || !j.ok) {
      alert((j && j.message) || "mm_gate failed");
      return false;
    }
    return true;
  } catch (e) {
    alert(String((e && e.message) || e));
    return false;
  }
}
async function refresh() {
  var s;
  var errEl = document.getElementById("mmDeskApiErr");
  try {
    s = await api("/api/state");
    window.__mmDeskLastState = s;
  } catch (e) {
    console.warn("refresh: /api/state", e);
    if (errEl) {
      errEl.style.display = "block";
      errEl.textContent = "No /api/state — is this tab on the same host:port as mm_live_desk? " + String((e && e.message) || e);
    }
    return;
  }
  if (errEl) errEl.style.display = "none";
  try { normalizeStateForUi(s); } catch (e2) { console.warn("normalizeStateForUi", e2); }
  try {
    var axUpd = Number(s.ax_books_updated_ms || 0);
    var refUpd = Number(s.ref_books_updated_ms || 0);
    var booksChanged =
      (window.__mmDeskPrevAxBooksUpdatedMs == null || window.__mmDeskPrevAxBooksUpdatedMs !== axUpd) ||
      (window.__mmDeskPrevRefBooksUpdatedMs == null || window.__mmDeskPrevRefBooksUpdatedMs !== refUpd);
    if (booksChanged) {
      renderAllBnBooks(s);
      renderAllAxBooks(s);
      window.__mmDeskPrevAxBooksUpdatedMs = axUpd;
      window.__mmDeskPrevRefBooksUpdatedMs = refUpd;
    }
    renderMmPreview(s);
    renderMmPositionGateBanner(s);
  } catch (e) { console.warn("refresh: depth/preview (early)", e); }
  hydrateDeskForms(s);
  try { paintMmGateButtons(s); } catch (e) { console.warn("paintMmGateButtons", e); }
  try { paintLive(s); } catch (e) { console.warn("refresh: paintLive", e); }
  try {
    var sl = document.getElementById("statusLine");
    if (sl) sl.textContent = (s.status_line || "") + (s.config_last_write ? " · " + s.config_last_write : "");
    var ll = document.getElementById("latLine");
    if (ll) {
      ll.textContent = (showReferenceDepthCol(s)
        ? ((s.ref_latency_text || "") + " · " + (s.ax_latency_text || ""))
        : (s.ax_latency_text || ""));
    }
    var posEl = document.getElementById("pos");
    if (posEl) posEl.textContent = s.position_line || "";
    var stEl = document.getElementById("stats");
    if (stEl) stEl.textContent = s.stats_line || "";
    var fn = document.getElementById("feedNote");
    if (fn) {
      var nb = String(s.data_feed_note || "").replace(/\s+/g, " ").trim();
      if (nb.length > 120) nb = nb.slice(0, 117) + "…";
      if (s.account_api_detail) {
        var ad = String(s.account_api_detail);
        if (ad.length < 72) nb += (nb ? " · " : "") + ad;
      }
      fn.textContent = nb;
    }
    try { paintFeedHealthPills(s.feed_health); } catch (e) { console.warn("paintFeedHealthPills", e); }
  } catch (e) { console.warn("refresh: status lines", e); }
  try {
    const fills = (s.fills || []).map(f => ({
      time: String(f.timestamp || f.time || f.ts || f.created_at || f.t || "").slice(0, 23),
      side: String(f.side || f.d || "").slice(0, 6),
      price: f.price || f.p || "",
      qty: f.quantity || f.qty || f.q || "",
      symbol: String(f.symbol || f.s || "").slice(0, 12),
      order: String(f.order_id || f.oid || "").slice(0, 18),
      _ts: Date.parse(String(f.timestamp || f.time || f.ts || f.created_at || f.t || "")) || 0,
    }));
    fills.sort(function(a, b) { return b._ts - a._ts; });
    fills.forEach(function(r) { delete r._ts; });
    function formatOrderStatus(raw) {
      var s0 = String(raw || "").trim().replace(/_/g, " ");
      if (!s0) return "—";
      return s0.split(/\s+/).map(function(w) {
        return w ? w.charAt(0).toUpperCase() + w.slice(1).toLowerCase() : "";
      }).join(" ");
    }
    function orderRejectReason(o) {
      var r = o.txt || o.reject_reason || o.rejectReason || o.reason || o.r || o.error_message || o.error || o.message || "";
      if (r && typeof r === "object") {
        r = r.error || r.message || r.detail || JSON.stringify(r);
      }
      r = String(r || "").trim();
      return r.length > 96 ? r.slice(0, 93) + "…" : r;
    }
    function orderHistoryTime(o) {
      var t = o.time || o.created_at || o.updated_at;
      if (t) return String(t).slice(0, 23);
      var ts = o.ts;
      if (typeof ts === "number" && isFinite(ts)) {
        var sec = ts < 1e12 ? ts : ts / 1000;
        var d = new Date(sec * 1000);
        if (!isNaN(d.getTime())) return d.toISOString().slice(0, 16).replace("T", " ");
      }
      return "";
    }
    const ords = (s.orders || []).map(o => ({
      sym: String(o.symbol || o.s || "").slice(0, 14),
      time: orderHistoryTime(o),
      side: String(o.side || o.d || "").slice(0, 6),
      price: o.price || o.p || o.limit_price || "",
      qty: o.quantity || o.q || o.rq || o.remaining_quantity || "",
      status: formatOrderStatus(o.status || o.state || o.o || ""),
      reason: orderRejectReason(o) || "—",
      id: String(o.oid || o.id || o.order_id || "").slice(0, 22),
    }));
    fillTable("tf", fills, ["time", "side", "price", "qty", "symbol", "order"]);
    fillTable("to", ords, ["sym", "time", "side", "price", "qty", "status", "reason", "id"]);
    renderDepthMetrics(s);
    var da = document.getElementById("deskActionLine");
    if (da) {
      if (s.desk_last_action) {
        var ag = (typeof s.desk_last_action_ms === "number" && s.desk_last_action_ms) ? (Math.max(0, Math.round((Date.now() - s.desk_last_action_ms) / 1000)) + "s ago") : "";
        da.textContent = "Last desk action: " + s.desk_last_action + (ag ? " · " + ag : "");
      } else {
        da.textContent = "";
      }
    }
  } catch (e) { console.warn("refresh: tables/metrics", e); }
  window.__mmDeskLastState = s;
  var tu = s.mm_order_preview && s.mm_order_preview.theo_updated_ms;
  if (tu != null && window.__mmDeskPrevTheoUpdatedMs != null && tu !== window.__mmDeskPrevTheoUpdatedMs) {
    // Pull one faster follow-up snapshot without overlapping refresh calls.
    scheduleNextDeskPoll(120);
  }
  if (tu != null) window.__mmDeskPrevTheoUpdatedMs = tu;
}
window.mmDeskRefresh = refresh;
var _mmDeskPollTimer = null;
function scheduleNextDeskPoll(delayMs) {
  if (_mmDeskPollTimer) clearTimeout(_mmDeskPollTimer);
  var ms = typeof delayMs === "number" && isFinite(delayMs) && delayMs >= 200 ? delayMs : 1000;
  _mmDeskPollTimer = setTimeout(function deskPollTick() {
    Promise.resolve(refresh()).finally(function() {
      var next = 450;
      try {
        var st = window.__mmDeskLastState;
        var u = st && typeof st.ui_poll_ms === "number" && isFinite(st.ui_poll_ms) ? st.ui_poll_ms : 1000;
        var minPoll = st && typeof st.ui_poll_min_ms === "number" && isFinite(st.ui_poll_min_ms)
          ? st.ui_poll_min_ms
          : 150;
        next = Math.min(Math.max(u, minPoll), 1500);
        var pr = st && st.mm_order_preview;
        if (pr && pr.theo_source === "cpp_file" && pr.theo_updated_ms != null) next = Math.min(next, 400);
      } catch (e2) {}
      scheduleNextDeskPoll(next);
    });
  }, ms);
}
function showTab(name) {
  document.querySelectorAll("#deskTabPanels > .tab-panel").forEach(function(p) {
    var pid = p.id.replace("panel-", "");
    p.classList.toggle("hidden", pid !== name);
  });
  document.querySelectorAll(".tabnav-center button[data-tab]").forEach(function(b) {
    b.classList.toggle("active", b.getAttribute("data-tab") === name);
  });
}
(function tabNavInit() {
  var nav = document.getElementById("tabnav");
  if (!nav) return;
  nav.addEventListener("click", function(ev) {
    var b = ev.target;
    while (b && b !== nav) {
      if (b.getAttribute && b.getAttribute("data-tab")) break;
      b = b.parentElement;
    }
    if (!b || !b.getAttribute("data-tab")) return;
    showTab(b.getAttribute("data-tab"));
  });
  showTab("trading");
})();
async function bootDesk() {
  var banner = document.getElementById("bootBanner");
  var errMsg = null;
  try {
    const s = await api("/api/boot", {}, 45000);
    window.__mmDeskLastState = s;
    try { normalizeStateForUi(s); } catch (e2) { console.warn("normalizeStateForUi(boot)", e2); }
    hydrateDeskForms(s);
    renderAllBnBooks(s);
    renderAllAxBooks(s);
    try { renderMmPreview(s); } catch (e) { console.warn("renderMmPreview(boot)", e); }
    window.__mmOrderbookUserTouched = false;
    window.__mmNeonMdUserTouched = false;
    /* Must finish before refresh(): otherwise hydrateDeskForms skips applyFromState and the multiselect
       can still be empty when the user hits Apply — POST saves [] and explicit depth falls back to MM legs only. */
    try {
      await refreshAxOrderbookInstrumentOptions();
    } catch (e) {
      console.warn("refreshAxOrderbookInstrumentOptions(boot)", e);
    }
    try { refreshNeonProductOptionsFromState(s); } catch (e) { console.warn("neonMd(boot)", e); }
    if (!window.__mmOrderbookInstrumentsInterval) {
      window.__mmOrderbookInstrumentsInterval = setInterval(function() {
        refreshAxOrderbookInstrumentOptions();
      }, 30000);
    }
  } catch (e) {
    errMsg = (e && e.name === "AbortError") ? "Timed out (45s). Is mm_live_desk.py running?" : String((e && e.message) || e);
  } finally {
    if (typeof window.mmDeskDismissBoot === "function") window.mmDeskDismissBoot();
  }
  if (errMsg && banner) {
    banner.style.display = "block";
    banner.textContent = "Boot issue: " + errMsg + " — check terminal [mm_live_desk].";
  }
  try {
    await refresh();
  } catch (_) {}
  try {
    if (window.__mmOrderbookInstrumentSelectReady && window.__mmDeskLastState) {
      hydrateAxOrderbookMultiFromServerState(window.__mmDeskLastState);
      updateAxOrderbookExtrasSummary(window.__mmDeskLastState);
    }
  } catch (e) {
    console.warn("axOrderbookMulti post-refresh hydrate", e);
  }
}
(function wirePrimaryUi() {
  var el;
  function wireMmReloadBump(btnId) {
    var b0 = document.getElementById(btnId);
    if (!b0) return;
    b0.onclick = async function() {
      try {
        const j = await api("/api/desk/mm_reload_nonce_bump", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        });
        if (!j.ok) { alert(j.error || "bump failed"); return; }
        var v = j.reload_limit_reset_nonce ? String(j.reload_limit_reset_nonce) : "";
        deskRoSetById("mmReloadNonce", v);
        syncParamsForm("1");
        var _a3 = document.getElementById("applyOut");
        if (_a3) _a3.textContent = j.config_message || "nonce bumped";
        refresh();
      } catch (e) { alert(e); }
    };
  }
  wireMmReloadBump("btnMmReloadBump");
  function setDeskActionFromJson(j, fallback) {
    var da = document.getElementById("deskActionLine");
    if (!da) return;
    if (j && j.ok === false) {
      da.textContent = (fallback || "Error") + ": " + (j.error || j.detail || JSON.stringify(j));
      return;
    }
    da.textContent = (j && j.message) ? j.message : (fallback || "Done — see Last desk action on next tick");
  }
  // Manual-order popup, reusable for both "Place manual order" and template
  // editing. opts:
  //   { prefill: <stored template record | null>, template: {ax_symbol,name} | null }
  // When `prefill` is given the popup jumps straight to Step 2 with the
  // instrument locked and the fields filled from the template instead of the
  // latest live stack; the bottom action row swaps to the template buttons.
  function mmDeskCanonicalSymbol(value) {
    return String(value || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
  }

  function mmDeskCanonicalTheoSource(value) {
    var src = String(value || "").trim().toLowerCase();
    if (src === "hl" || src === "hyper_liquid") return "hyperliquid";
    if (src === "neon" || src === "neonfix") return "neon_fix";
    if (src === "cme") return "mettraders";
    return src;
  }

  function mmDeskFindBook(books, symbol) {
    books = books && typeof books === "object" ? books : {};
    var wanted = mmDeskCanonicalSymbol(symbol);
    if (!wanted) return null;
    var keys = Object.keys(books);
    for (var i = 0; i < keys.length; i++) {
      if (mmDeskCanonicalSymbol(keys[i]) === wanted) return { key: keys[i], book: books[keys[i]] };
    }
    return null;
  }

  function mmDeskBookMid(book) {
    if (!book || book._err) return null;
    var bids = Array.isArray(book.bids) ? book.bids : [];
    var asks = Array.isArray(book.asks) ? book.asks : [];
    var bestBid = null;
    var bestAsk = null;
    for (var i = 0; i < bids.length; i++) {
      var bid = Number(Array.isArray(bids[i]) ? bids[i][0] : bids[i].price);
      if (isFinite(bid) && bid > 0 && (bestBid == null || bid > bestBid)) bestBid = bid;
    }
    for (var j = 0; j < asks.length; j++) {
      var ask = Number(Array.isArray(asks[j]) ? asks[j][0] : asks[j].price);
      if (isFinite(ask) && ask > 0 && (bestAsk == null || ask < bestAsk)) bestAsk = ask;
    }
    if (bestBid == null || bestAsk == null || bestBid > bestAsk) return null;
    return (bestBid + bestAsk) / 2;
  }

  function mmDeskReferenceSourceForSymbol(state, symbol) {
    var sources = state && state.ref_book_sources && typeof state.ref_book_sources === "object"
      ? state.ref_book_sources : {};
    var wanted = mmDeskCanonicalSymbol(symbol);
    var keys = Object.keys(sources);
    for (var i = 0; i < keys.length; i++) {
      if (mmDeskCanonicalSymbol(keys[i]) === wanted) return mmDeskCanonicalTheoSource(sources[keys[i]]);
    }
    return "";
  }

  function mmDeskSnapshotNumber(value) {
    if (!isFinite(value)) return "";
    return String(Number(Number(value).toPrecision(12)));
  }

  async function mmOpenManualOrderPopup(opts) {
    opts = opts || {};
    // Step 1: pick Architect instrument. Step 2: instrument-level max_position +
    // order-level params (prefilled from latest stack for this product, if any).
    // POST /api/desk/place_order places the pair on the gateway, then
    // appends a desk_seeded row to orders.json; C++ adopts OIDs and moves
    // quotes (same MM math in strategy code).
    var moPrefill = (opts.prefill && typeof opts.prefill === "object") ? opts.prefill : null;
    var moTemplateCtx = (opts.template && typeof opts.template === "object") ? opts.template : null;
    var moTemplateMode = !!moPrefill;
    // Instrument for a template-opened popup. The stored template keeps all popup
    // fields under params{}; the instrument itself is the templates.json map key,
    // passed in via opts.template.ax_symbol.
    var moPrefillAx = (moTemplateCtx && moTemplateCtx.ax_symbol)
      ? String(moTemplateCtx.ax_symbol)
      : ((moPrefill && moPrefill.ax_symbol) ? String(moPrefill.ax_symbol) : "");
    var s = window.__mmDeskLastState || {};
    var axCatalog = (Array.isArray(s.ax_grid_symbols) && s.ax_grid_symbols.length)
      ? s.ax_grid_symbols.slice()
      : (Array.isArray(s.ax_book_symbols) ? s.ax_book_symbols.slice() : []);
    // Template mode: make sure the template's instrument is selectable even if the
    // live AX catalog hasn't loaded it (e.g. depth list trimmed).
    if (moPrefillAx) {
      var moHasAx = axCatalog.some(function(x) { return String(x).toUpperCase() === moPrefillAx.toUpperCase(); });
      if (!moHasAx) axCatalog.unshift(moPrefillAx);
    }
    if (!axCatalog.length) { alert("No AX instruments loaded — wait for the desk to fetch GET /instruments and try again."); return; }
    async function mmDeskOrdersCfgUnfreeze() {
      try {
        await api("/api/desk/orders_config_freeze", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ active: false }),
        });
      } catch (e) {
        console.warn("orders_config_freeze false", e);
      }
    }
    try {
      const fz = await api("/api/desk/orders_config_freeze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ active: true }),
      });
      if (!fz || !fz.ok) {
        alert((fz && (fz.message || fz.error)) || "orders_config_freeze (active:true) failed");
        return;
      }
    } catch (e) {
      alert(e);
      return;
    }
    var legsByAx = {};
    if (Array.isArray(s.mm_instruments)) {
      for (var li = 0; li < s.mm_instruments.length; li++) {
        var lr = s.mm_instruments[li];
        if (!lr) continue;
        var lax = String(lr.symbol || lr.ax_symbol || "").toUpperCase();
        if (lax) legsByAx[lax] = lr;
      }
    }
    var ov = document.createElement("div");
    ov.style.cssText = "position:fixed;inset:0;background:rgba(7,12,20,.85);z-index:21000;display:flex;align-items:center;justify-content:center;overflow-y:auto";
    var card = document.createElement("div");
    card.style.cssText = "width:min(460px,94vw);background:#0f1724;border:1px solid #2a3548;border-radius:12px;padding:14px;box-shadow:0 12px 30px rgba(0,0,0,.45);color:#e6edf3";
    var h = document.createElement("h3");
    h.style.margin = "0 0 4px 0";
    h.id = "moTitle";
    h.textContent = "Step 1: Architect (AX) product";
    card.appendChild(h);
    var hint = document.createElement("p");
    hint.id = "moHint";
    hint.className = "muted";
    hint.style.fontSize = "12px";
    hint.textContent = "Select the AX instrument first, then Next. Step 2 loads the latest stack's order-level settings (width, size, adjust ticks, …) when stacks already exist; max position is always at instrument level (all stacks on that product).";
    card.appendChild(hint);

    function row(labelTxt, child) {
      var p = document.createElement("p");
      p.className = "fld";
      var l = document.createElement("label");
      l.textContent = labelTxt;
      l.style.display = "block";
      l.style.marginBottom = "4px";
      p.appendChild(l);
      p.appendChild(child);
      return p;
    }
    function numInput(id, v0) {
      var i = document.createElement("input");
      i.id = id;
      i.type = "number";
      i.value = String(v0);
      i.style.width = "100%";
      i.style.boxSizing = "border-box";
      return i;
    }
    // Float input for the pricer-snapshot transform fields (quote_snapshot, pricer_snapshot, slope).
    // Native number inputs in browsers reject non-integer typing on some locales unless step is set;
    // step="any" lets the operator type values like 7280.7 or 0.5 without the up/down arrows
    // forcing them to integer ticks.
    function floatInput(id, v0, ph) {
      var i = document.createElement("input");
      i.id = id;
      i.type = "number";
      i.step = "any";
      i.value = (v0 == null || v0 === "") ? "" : String(v0);
      if (ph) i.placeholder = ph;
      i.style.width = "100%";
      i.style.boxSizing = "border-box";
      return i;
    }
    function txtInput(id, v0, ph) {
      var i = document.createElement("input");
      i.id = id;
      i.type = "text";
      i.value = String(v0 == null ? "" : v0);
      i.placeholder = ph || "";
      i.style.width = "100%";
      i.style.boxSizing = "border-box";
      return i;
    }
    var step1 = document.createElement("div");
    step1.id = "moStep1";
    var step2 = document.createElement("div");
    step2.id = "moStep2";
    step2.style.display = "none";

    var sel = document.createElement("select");
    sel.id = "moAxSym";
    sel.style.cssText = "width:100%;padding:6px;background:#0b1220;color:#e6edf3;border:1px solid #30405a;border-radius:6px";
    for (var i0 = 0; i0 < axCatalog.length; i0++) {
      var opt = document.createElement("option");
      var sym = String(axCatalog[i0]);
      opt.value = sym;
      var leg0 = legsByAx[sym.toUpperCase()];
      opt.textContent = sym + (leg0 ? "  ·  " + (leg0.theo_source || "") : "  ·  (unconfigured)");
      sel.appendChild(opt);
    }
    step1.appendChild(row("AX product", sel));

    var theoSrcSel = document.createElement("select");
    theoSrcSel.id = "moTheoSrc";
    theoSrcSel.style.cssText = "width:100%;padding:6px;background:#0b1220;color:#e6edf3;border:1px solid #30405a;border-radius:6px";
    ["mettraders", "hyperliquid", "neon_fix"].forEach(function(v) {
      var o = document.createElement("option"); o.value = v; o.textContent = v; theoSrcSel.appendChild(o);
    });
    step2.appendChild(row("Theo source (Mettraders / HL / Neon)", theoSrcSel));
    var theoVenue = txtInput("moTheoVenue", "", "e.g. USD/JPY, GC, silver-usdc");
    step2.appendChild(row("Theo venue symbol", theoVenue));

    function applyLegDefaults() {
      var ax = String(sel.value || "").toUpperCase();
      var leg = legsByAx[ax];
      if (leg) {
        theoSrcSel.value = String(leg.theo_source || "neon_fix");
        theoVenue.value = String(leg.theo_venue_symbol || leg.reference_fix_symbol || leg.theo_symbol || "");
      } else {
        theoSrcSel.value = "neon_fix";
        theoVenue.value = "";
      }
    }
    sel.onchange = applyLegDefaults;
    applyLegDefaults();

    var gw0 = s.mm_width != null ? s.mm_width : 10;
    var gq0 = s.mm_order_size != null ? s.mm_order_size : 200;
    var gmx0 = s.mm_max_position != null ? s.mm_max_position : 1000000;
    var gap0 = s.mm_adjust_position != null ? s.mm_adjust_position : 50;
    var gat0 = s.mm_adjust_ticks != null ? s.mm_adjust_ticks : 1;
    var gmd0 = s.mm_min_theo_move_ticks_to_requote != null ? s.mm_min_theo_move_ticks_to_requote : 1;
    var gmr0 = s.mm_max_reload_cycles != null ? s.mm_max_reload_cycles : 0;
    // Pricer-snapshot linear transform defaults — see C++ MarketMakerManualStack docstring.
    // Defaults are CHOSEN TO LEAVE BEHAVIOR UNCHANGED for any operator who doesn't fill the
    // fields: quote_snapshot=0 / pricer_snapshot=0 disable the transform server-side, and the
    // strategy quotes off raw theo as before. Slope=1 is the linear pass-through default.
    var gqs0 = s.mm_quote_snapshot != null ? s.mm_quote_snapshot : 0;
    var gps0 = s.mm_pricer_snapshot != null ? s.mm_pricer_snapshot : 0;
    var gsl0 = s.mm_slope != null ? s.mm_slope : 1;
    var instHint = document.createElement("p");
    instHint.id = "moInstHint";
    instHint.className = "muted";
    instHint.style.fontSize = "11px";
    instHint.style.margin = "0 0 6px 0";
    instHint.textContent = "Instrument-level cap (shared by every stack on this AX). Saved under products[symbol].max_position in orders.json.";
    step2.appendChild(instHint);
    step2.appendChild(row("Max position (instrument)", numInput("moInstMx", gmx0)));
    var ordHint = document.createElement("p");
    ordHint.className = "muted";
    ordHint.style.fontSize = "11px";
    ordHint.style.margin = "8px 0 6px 0";
    ordHint.textContent = "Order-level params for this new stack only (width, size, skew tuning, …). Prefilled from the latest stack for the selected instrument when available.";
    step2.appendChild(ordHint);
    step2.appendChild(row("Width (bps)", numInput("moW", gw0)));
    step2.appendChild(row("Order size", numInput("moQ", gq0)));
    step2.appendChild(row("Adjust position", numInput("moAp", gap0)));
    step2.appendChild(row("Adjust ticks", numInput("moAt", gat0)));
    step2.appendChild(row("Min drift (theo) ticks", numInput("moMinDr", gmd0)));
    step2.appendChild(row("Max reload cycles (0 = unlim.)", numInput("moMrc", gmr0)));

    // ─── Pricer-snapshot transform (per-stack) ───
    // Lets the desk quote an AX product whose price level differs from the live theo's price
    // level (e.g. SPY AX off S&P500 HL). Formula:
    //   newMidpoint = quote_snapshot
    //               + quote_snapshot * slope * ((theo - pricer_snapshot)/pricer_snapshot)
    // Then the existing skew/width formula is applied to newMidpoint instead of raw theo.
    // Leaving Quote/Pricer snapshot at 0 disables the transform → C++ uses raw theo (unchanged
    // legacy behavior). This is intentional so existing pairs are not affected.
    var xfHint = document.createElement("p");
    xfHint.className = "muted";
    xfHint.style.fontSize = "11px";
    xfHint.style.margin = "8px 0 6px 0";
    xfHint.innerHTML =
      "Pricer-snapshot transform (optional, per stack). Maps theo to AX-quoted units:" +
      "<br>&nbsp;&nbsp;<code>newMidpoint = quote_snapshot + quote_snapshot · slope · " +
      "((theo − pricer_snapshot) / pricer_snapshot)</code>" +
      "<br>Leave Quote/Pricer snapshot at 0 to quote off raw theo (transform disabled).";
    step2.appendChild(xfHint);
    var snapshotActions = document.createElement("div");
    snapshotActions.style.cssText = "display:flex;align-items:center;gap:8px;margin:4px 0 8px 0;flex-wrap:wrap";
    var bFillSnapshot = document.createElement("button");
    bFillSnapshot.type = "button";
    bFillSnapshot.className = "btn";
    bFillSnapshot.textContent = "Fill Snapshot";
    bFillSnapshot.title = "Fill Quote snapshot from the selected AX product mid and Pricer snapshot from the selected Theo Source / Theo Symbol mid.";
    var snapshotStatus = document.createElement("span");
    snapshotStatus.className = "muted";
    snapshotStatus.style.cssText = "font-size:11px;line-height:1.35";
    snapshotActions.appendChild(bFillSnapshot);
    snapshotActions.appendChild(snapshotStatus);
    step2.appendChild(snapshotActions);

    var quoteSnapshotInput = floatInput("moQs", gqs0, "e.g. 726.48");
    var pricerSnapshotInput = floatInput("moPs", gps0, "e.g. 7280.7");
    step2.appendChild(row("Quote snapshot (AX-side anchor; 0 = disabled)", quoteSnapshotInput));
    step2.appendChild(row("Pricer snapshot (theo-side anchor; 0 = disabled)", pricerSnapshotInput));
    step2.appendChild(row("Slope (1 = linear pass-through; 0 = fixed midpoint)", floatInput("moSl", gsl0, "1")));

    bFillSnapshot.onclick = async function() {
      bFillSnapshot.disabled = true;
      snapshotStatus.style.color = "";
      snapshotStatus.textContent = "Loading current prices...";
      try {
        // Fetch fresh books when clicked; the modal may have been open across several poll cycles.
        var liveState = await api("/api/state");
        normalizeStateForUi(liveState);
        window.__mmDeskLastState = liveState;
        var axSymbol = String(sel.value || "").trim();
        var theoSource = mmDeskCanonicalTheoSource(theoSrcSel.value);
        var theoSymbol = String(theoVenue.value || "").trim();
        if (!axSymbol) throw new Error("Select an AX product first.");
        if (!theoSymbol) throw new Error("Theo venue symbol is required.");

        var axMatch = mmDeskFindBook(liveState.ax_all_books, axSymbol);
        if (!axMatch) throw new Error("No Architect book is available for " + axSymbol + ".");
        var axMid = mmDeskBookMid(axMatch.book);
        if (axMid == null) {
          var axDetail = axMatch.book && axMatch.book._err ? ": " + axMatch.book._err : "";
          throw new Error("Architect bid/ask is unavailable for " + axSymbol + axDetail);
        }

        var refMatch = mmDeskFindBook(liveState.ref_all_books, theoSymbol);
        if (!refMatch) throw new Error("No " + theoSource + " price is available for Theo Symbol " + theoSymbol + ".");
        var actualSource = mmDeskReferenceSourceForSymbol(liveState, refMatch.key);
        if (actualSource && actualSource !== theoSource) {
          throw new Error("Theo Symbol " + theoSymbol + " is currently populated by " + actualSource + ", not " + theoSource + ".");
        }
        var pricerMid = mmDeskBookMid(refMatch.book);
        if (pricerMid == null) {
          var refDetail = refMatch.book && refMatch.book._err ? ": " + refMatch.book._err : "";
          throw new Error("Pricing-feed bid/ask is unavailable for " + theoSymbol + refDetail);
        }

        quoteSnapshotInput.value = mmDeskSnapshotNumber(axMid);
        pricerSnapshotInput.value = mmDeskSnapshotNumber(pricerMid);
        quoteSnapshotInput.dispatchEvent(new Event("input", { bubbles: true }));
        pricerSnapshotInput.dispatchEvent(new Event("input", { bubbles: true }));
        quoteSnapshotInput.dispatchEvent(new Event("change", { bubbles: true }));
        pricerSnapshotInput.dispatchEvent(new Event("change", { bubbles: true }));
        snapshotStatus.style.color = "#86efac";
        snapshotStatus.textContent = "Filled: AX " + axSymbol + " mid " + quoteSnapshotInput.value + " · " + theoSource + " " + theoSymbol + " mid " + pricerSnapshotInput.value + ". Values remain editable.";
      } catch (e) {
        snapshotStatus.style.color = "#fca5a5";
        snapshotStatus.textContent = String((e && e.message) || e);
      } finally {
        bFillSnapshot.disabled = false;
      }
    };

    function mmDeskInstrumentMaxPoFloor(ordersCfg, ax) {
      var up = String(ax || "").trim().toUpperCase();
      var floor = 0;
      if (!ordersCfg || !up) return 0;
      var products = ordersCfg.products && typeof ordersCfg.products === "object" ? ordersCfg.products : {};
      var prod = products[ax] || products[up] || {};
      var pm = parseInt(String(prod.max_position != null ? prod.max_position : 0), 10);
      if (isFinite(pm) && pm > floor) floor = pm;
      function bumpRow(row) {
        if (!row || typeof row !== "object") return;
        if (String(row.ax_symbol || "").trim().toUpperCase() !== up) return;
        var mp = parseInt(String(row.max_position != null ? row.max_position : 0), 10);
        if (isFinite(mp) && mp > floor) floor = mp;
      }
      var rootStacks = Array.isArray(ordersCfg.stacks) ? ordersCfg.stacks : [];
      for (var ri = 0; ri < rootStacks.length; ri++) bumpRow(rootStacks[ri]);
      var prodStacks = Array.isArray(prod.stacks) ? prod.stacks : [];
      for (var pi = 0; pi < prodStacks.length; pi++) bumpRow(prodStacks[pi]);
      return floor;
    }
    function mmDeskLatestStackForAx(products, ax) {
      var up = String(ax || "").toUpperCase();
      var prod = products && (products[ax] || products[up]);
      if (!prod || !Array.isArray(prod.stacks)) return null;
      var stacks = prod.stacks;
      var best = null;
      var bestMs = -1;
      for (var si = 0; si < stacks.length; si++) {
        var st = stacks[si];
        if (!st || typeof st !== "object") continue;
        var ms = st.created_ms != null ? parseInt(String(st.created_ms), 10) : 0;
        if (!isFinite(ms)) ms = 0;
        if (ms >= bestMs) {
          bestMs = ms;
          best = st;
        }
      }
      if (best) return best;
      return stacks.length ? stacks[stacks.length - 1] : null;
    }
    function mmDeskFillStep2ForAx(axSym) {
      var ax = String(axSym || "").trim();
      var up = ax.toUpperCase();
      var ordersCfg = (s.mm_orders_config && s.mm_orders_config.products) ? s.mm_orders_config.products : {};
      var prod = ordersCfg[ax] || ordersCfg[up] || {};
      var latest = mmDeskLatestStackForAx(ordersCfg, ax);
      var leg = legsByAx[up] || null;
      var floorMx = mmDeskInstrumentMaxPoFloor(ordersCfg, ax);
      var instMx = prod.max_position != null ? parseInt(String(prod.max_position), 10)
        : (leg && leg.max_position != null ? parseInt(String(leg.max_position), 10) : gmx0);
      if (!isFinite(instMx) || instMx < 1) instMx = gmx0;
      if (floorMx > 0 && instMx < floorMx) instMx = floorMx;
      var gw = gw0, gq = gq0, gap = gap0, gat = gat0, gmd = gmd0, gmr = gmr0;
      // Pricer-snapshot transform prefill (floats, 0 = disabled is a valid value so we accept 0).
      var gqs = gqs0, gps = gps0, gsl = gsl0;
      if (latest) {
        if (latest.width_bps != null) gw = parseInt(String(latest.width_bps), 10);
        else if (latest.width_ticks != null) gw = parseInt(String(latest.width_ticks), 10);
        else if (latest.width != null) gw = parseInt(String(latest.width), 10);
        if (latest.order_size != null) gq = parseInt(String(latest.order_size), 10);
        if (latest.adjust_position != null) gap = parseInt(String(latest.adjust_position), 10);
        if (latest.adjust_ticks != null) gat = parseInt(String(latest.adjust_ticks), 10);
        if (latest.min_theo_move_ticks_to_requote != null) gmd = parseInt(String(latest.min_theo_move_ticks_to_requote), 10);
        if (latest.max_reload_cycles != null) gmr = parseInt(String(latest.max_reload_cycles), 10);
        if (latest.quote_snapshot != null) gqs = parseFloat(String(latest.quote_snapshot));
        if (latest.pricer_snapshot != null) gps = parseFloat(String(latest.pricer_snapshot));
        if (latest.slope != null) gsl = parseFloat(String(latest.slope));
      }
      if (!isFinite(gw) || gw < 1) gw = gw0;
      if (!isFinite(gq) || gq < 1) gq = gq0;
      if (!isFinite(gap) || gap < 1) gap = gap0;
      if (!isFinite(gat) || gat < 1) gat = gat0;
      if (!isFinite(gmd) || gmd < 1) gmd = gmd0;
      if (!isFinite(gmr) || gmr < 0) gmr = gmr0;
      // Snapshots: NaN/non-finite → 0 (disabled). Negative is invalid (validated server-side).
      // Slope: NaN/non-finite → 1 (linear pass-through). Slope=0 is valid (clamps to fixed mid).
      if (!isFinite(gqs)) gqs = 0;
      if (!isFinite(gps)) gps = 0;
      if (!isFinite(gsl)) gsl = 1;
      // No auto-population (operator request 2026-06-12): the Step-2 stack params are left BLANK
      // for the operator to type explicitly, instead of being prefilled from the latest stack or
      // global config defaults (which were showing misleading values like order_size=100 /
      // max_position=5000). The instrument-cap floor (min + hint) is still enforced for safety.
      // gw/gq/gap/... above are intentionally left computed-but-unused; they remain only to drive
      // the floor calc and stay available if prefill is ever re-enabled.
      void gw; void gq; void gap; void gat; void gmd; void gmr; void gqs; void gps; void gsl; void instMx;
      var elI = document.getElementById("moInstMx");
      if (elI) {
        elI.value = "";
        elI.min = floorMx > 0 ? String(floorMx) : "1";
        elI.placeholder = floorMx > 0 ? ("required — " + floorMx + " or higher") : "required";
      }
      var instHintEl = document.getElementById("moInstHint");
      if (instHintEl) {
        instHintEl.textContent = floorMx > 0
          ? ("Instrument cap for " + ax + " is already " + floorMx +
             " (max across existing stacks). Enter " + floorMx + " or higher — lower values are rejected.")
          : ("Instrument-level cap (shared by every stack on this AX). Saved under products[symbol].max_position in orders.json.");
      }
      var elW = document.getElementById("moW");
      if (elW) { elW.value = ""; elW.placeholder = "required"; }
      var elQ = document.getElementById("moQ");
      if (elQ) { elQ.value = ""; elQ.placeholder = "required"; }
      var elAp = document.getElementById("moAp");
      if (elAp) { elAp.value = ""; elAp.placeholder = "required"; }
      var elAt = document.getElementById("moAt");
      if (elAt) { elAt.value = ""; elAt.placeholder = "required"; }
      var elMd = document.getElementById("moMinDr");
      if (elMd) { elMd.value = ""; elMd.placeholder = "required"; }
      var elMr = document.getElementById("moMrc");
      if (elMr) { elMr.value = ""; elMr.placeholder = "0 = unlimited"; }
      var elQs = document.getElementById("moQs");
      if (elQs) elQs.value = "";
      var elPs = document.getElementById("moPs");
      if (elPs) elPs.value = "";
      var elSl = document.getElementById("moSl");
      if (elSl) elSl.value = "";
    }

    var rowStep1 = document.createElement("div");
    rowStep1.style.cssText = "display:flex;justify-content:flex-end;gap:8px;margin-top:12px";
    var bClose0 = document.createElement("button");
    bClose0.type = "button"; bClose0.className = "btn"; bClose0.textContent = "Close";
    var bNext = document.createElement("button");
    bNext.type = "button"; bNext.className = "btn btn-primary"; bNext.textContent = "Next";
    rowStep1.appendChild(bClose0); rowStep1.appendChild(bNext);
    step1.appendChild(rowStep1);

    var rowB = document.createElement("div");
    rowB.style.cssText = "display:flex;justify-content:space-between;align-items:center;gap:8px;margin-top:12px;flex-wrap:wrap";
    var b0 = document.createElement("button");
    b0.type = "button"; b0.className = "btn"; b0.textContent = "Back";
    var b2 = document.createElement("button");
    b2.type = "button"; b2.className = "btn"; b2.textContent = "Close";
    var b1 = document.createElement("button");
    b1.type = "button"; b1.className = "btn btn-primary"; b1.textContent = "Submit (place + register)";
    // "Place manual order" mode: also offer to persist the form as a template.
    var bSaveTpl = document.createElement("button");
    bSaveTpl.type = "button"; bSaveTpl.className = "btn"; bSaveTpl.textContent = "Save Template & Place order";
    // Template-editing mode: update the open template or branch a new one, then submit.
    var bUpdTpl = document.createElement("button");
    bUpdTpl.type = "button"; bUpdTpl.className = "btn btn-primary"; bUpdTpl.textContent = "Update Template & Submit Order";
    var bNewTpl = document.createElement("button");
    bNewTpl.type = "button"; bNewTpl.className = "btn"; bNewTpl.textContent = "Create New Template & Submit Order";
    if (moTemplateMode) {
      // Step 1 is skipped — no Back button; show Close + template actions.
      rowB.appendChild(b2); rowB.appendChild(bUpdTpl); rowB.appendChild(bNewTpl);
    } else {
      rowB.appendChild(b0); rowB.appendChild(b2); rowB.appendChild(b1); rowB.appendChild(bSaveTpl);
    }
    step2.appendChild(rowB);
    var submitStatus = document.createElement("p");
    submitStatus.className = "muted";
    submitStatus.style.cssText = "margin:8px 0 0 0;font-size:12px;min-height:18px";
    submitStatus.textContent = "";
    step2.appendChild(submitStatus);

    card.appendChild(step1);
    card.appendChild(step2);
    ov.appendChild(card);

    bClose0.onclick = async function() {
      await mmDeskOrdersCfgUnfreeze();
      try { document.body.removeChild(ov); } catch (e) {}
    };
    bNext.onclick = function() {
      var ax = String(sel.value || "").trim();
      if (!ax) { alert("Pick a product on AX first."); return; }
      applyLegDefaults();
      mmDeskFillStep2ForAx(ax);
      document.getElementById("moTitle").textContent = "Step 2: instrument cap + new-stack params";
      document.getElementById("moHint").textContent = "Product " + ax + ": edit instrument max position (all stacks) and order-level params for this new stack, then Submit.";
      step1.style.display = "none";
      step2.style.display = "block";
    };
    b0.onclick = function() {
      document.getElementById("moTitle").textContent = "Step 1: Architect (AX) product";
      document.getElementById("moHint").textContent = "Select the product you want to add a stack for, then Next. On step 2 you edit per-stack theo/params; Submit places the pair on the gateway and writes orders.json for C++.";
      step2.style.display = "none";
      step1.style.display = "block";
    };
    b2.onclick = async function() {
      await mmDeskOrdersCfgUnfreeze();
      try { document.body.removeChild(ov); } catch (e) {}
    };

    function gi(id) { return parseInt(String(document.getElementById(id).value || "0"), 10); }
    // Float read for the snapshot/slope fields. Returns 0 (snapshots) / 1 (slope) when blank
    // so the server's "0 disables" semantics match what the user sees in the form.
    function gf(id, dflt) {
      var s = String((document.getElementById(id) || {}).value || "").trim();
      if (s === "") return dflt;
      var v = parseFloat(s);
      return isFinite(v) ? v : dflt;
    }
    function sleepMs(ms) { return new Promise(function(resolve) { setTimeout(resolve, ms); }); }
    // All Step-2 action buttons share one disabled state so a submit/save in
    // flight can't be double-fired from another button.
    function setActions(disabled) {
      [b1, bSaveTpl, bUpdTpl, bNewTpl].forEach(function(b) { if (b) b.disabled = !!disabled; });
    }
    async function runManualSubmitJob(payload) {
      try {
        // Resume-confirm gate (Item 3/4): if the instrument is stopped the server 409s and
        // this asks the operator; on confirm it re-posts with resume:true (flag-only resume
        // then place). On decline it throws __resumeCancelled and nothing is placed.
        const j0 = await mmDeskPostWithResumeConfirm(
          "/api/desk/place_order_async",
          payload,
          15000,
        );
        if (!j0 || !j0.ok || !j0.job_id) {
          setActions(false);
          await mmDeskOrdersCfgUnfreeze();
          submitStatus.style.color = "#fca5a5";
          submitStatus.textContent = (j0 && j0.error) || "Failed to start submit job";
          alert((j0 && j0.error) || "place_order failed");
          return;
        }
        var jobId = String(j0.job_id);
        var t0 = Date.now();
        var done = false;
        var result = null;
        while (!done) {
          await sleepMs(400);
          var elapsedSec = Math.max(0, Math.floor((Date.now() - t0) / 1000));
          submitStatus.textContent = "Submitting... (" + elapsedSec + "s)";
          var st = await api(
            "/api/desk/place_order_async_status",
            { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ job_id: jobId }) },
            15000,
          );
          if (!st || !st.ok) {
            setActions(false);
            await mmDeskOrdersCfgUnfreeze();
            submitStatus.style.color = "#fca5a5";
            submitStatus.textContent = (st && st.error) || "Submit status failed";
            alert((st && st.error) || "place_order status failed");
            return;
          }
          done = !!st.done;
          if (done) result = st.result || {};
        }
        setActions(false);
        if (!result || !result.ok) {
          await mmDeskOrdersCfgUnfreeze();
          submitStatus.style.color = "#fca5a5";
          submitStatus.textContent = (result && result.error) || "place_order failed";
          alert((result && result.error) || "place_order failed");
          return;
        }
        submitStatus.style.color = "#9ec5b8";
        submitStatus.textContent = "Done";
        alert((result.hint || "Stack written to orders.json") + "\nstack_id: " + (result.stack_id || result.request_id || "?"));
        try { document.body.removeChild(ov); } catch (e2) {}
        refresh();
      } catch (e) {
        setActions(false);
        await mmDeskOrdersCfgUnfreeze();
        submitStatus.style.color = "#fca5a5";
        var msg =
          e && e.name === "AbortError"
            ? "Request timed out — submit may still be processing in background; check status before retrying."
            : String((e && e.message) || e);
        submitStatus.textContent = msg;
        alert(msg);
      }
    }
    // Read + validate the Step-2 form. Returns {ax, theoSrc, payload} or null
    // (after alerting). Shared by plain submit and template save+submit so the
    // gateway always receives the identical payload shape.
    function mmReadAndValidateForm() {
      var ax = String(sel.value || "").trim();
      if (!ax) { alert("Pick a product"); return null; }
      var theoSrc = String(theoSrcSel.value || "").trim();
      var theoVen = String(theoVenue.value || "").trim();
      if (!theoSrc) { alert("Theo source is required"); return null; }
      var ordersCfgSubmit = s.mm_orders_config || {};
      var floorSubmit = mmDeskInstrumentMaxPoFloor(ordersCfgSubmit, ax);
      // Required-field validation. Fields are no longer auto-populated (operator request), so a
      // blank field must be rejected here rather than silently submitting 0 (which would place a
      // zero-size / zero-cap stack on the live venue). Blank → reject; an explicitly typed 0 is
      // accepted only where 0 is valid (max_reload_cycles = unlimited).
      function giReq(id, label, minV) {
        var raw = String((document.getElementById(id) || {}).value || "").trim();
        if (raw === "") { alert(label + " is required — enter a value."); return null; }
        var v = parseInt(raw, 10);
        if (!isFinite(v) || v < minV) { alert(label + " must be a whole number >= " + minV + "."); return null; }
        return v;
      }
      var instMxSubmit = giReq("moInstMx", "Max position (instrument)", 1);
      if (instMxSubmit === null) return null;
      if (floorSubmit > 0 && instMxSubmit < floorSubmit) {
        alert(
          "Instrument " + ax + ": max_position is already " + floorSubmit +
          " across existing order pairs. You entered " + instMxSubmit +
          ". Use " + floorSubmit + " or higher."
        );
        return null;
      }
      var wSubmit = giReq("moW", "Width (bps)", 1);
      if (wSubmit === null) return null;
      var qSubmit = giReq("moQ", "Order size", 1);
      if (qSubmit === null) return null;
      var apSubmit = giReq("moAp", "Adjust position", 1);
      if (apSubmit === null) return null;
      var atSubmit = giReq("moAt", "Adjust ticks", 1);
      if (atSubmit === null) return null;
      var mdSubmit = giReq("moMinDr", "Min drift (theo) ticks", 1);
      if (mdSubmit === null) return null;
      var mrSubmit = giReq("moMrc", "Max reload cycles (0 = unlimited)", 0);
      if (mrSubmit === null) return null;
      var payload = {
        ax_symbol: ax,
        theo_source: theoSrc,
        theo_venue_symbol: theoVen,
        instrument_max_position: instMxSubmit,
        params: {
          width_bps: wSubmit,
          order_size: qSubmit,
          adjust_position: apSubmit,
          adjust_ticks: atSubmit,
          min_theo_move_ticks_to_requote: mdSubmit,
          max_reload_cycles: mrSubmit,
          // Pricer-snapshot transform inputs. Defaults: snapshots=0 (transform disabled),
          // slope=1 (linear pass-through). Server persists them on the stack row in orders.json
          // and the C++ MakeMarketStrategy reads them via mm_manual_stack_->{quote_snapshot,
          // pricer_snapshot, slope}. See MarketMakerManualStack docstring in include/config/Config.h.
          quote_snapshot: gf("moQs", 0),
          pricer_snapshot: gf("moPs", 0),
          slope: gf("moSl", 1),
        },
      };
      return { ax: ax, theoSrc: theoSrc, payload: payload };
    }
    // Suggest the next default template name: {AX}_tmp{N}. Fetches the current
    // store so the suffix doesn't collide with names saved in other sessions.
    async function mmSuggestTplName(ax) {
      var axU = String(ax || "").toUpperCase();
      try {
        var jt = await api("/api/desk/templates", {}, 8000);
        var arr = (jt && jt.ok && jt.templates && jt.templates[axU]) ? jt.templates[axU] : [];
        var names = {};
        (arr || []).forEach(function(t) { if (t && t.name) names[String(t.name)] = 1; });
        var i = 1;
        while (names[axU + "_tmp" + i]) i++;
        return axU + "_tmp" + i;
      } catch (e) {
        return axU + "_tmp1";
      }
    }
    // mode: "create" (prompt for a fresh name) | "update" (reuse the open
    // template's name). Saves the template, then submits the order through the
    // same gateway path as a plain place-order.
    // Returns true iff it handed off to runManualSubmitJob (which then owns the action re-enable);
    // returns false on every early-return/failure path so the caller re-enables the buttons.
    async function mmSaveTemplateAndSubmit(mode) {
      var v = mmReadAndValidateForm();
      if (!v) return false;
      var name;
      if (mode === "update" && moTemplateCtx && moTemplateCtx.name) {
        name = moTemplateCtx.name;
      } else {
        var def = await mmSuggestTplName(v.ax);
        var entered = window.prompt("Template name", def);
        if (entered == null) return false;
        name = String(entered).trim();
        if (!name) { alert("Template name is required."); return false; }
      }
      if (!deskMmGateConfirm(
        (mode === "update" ? "Update template '" + name + "'" : "Save template '" + name + "'") +
        " and send manual order for " + v.ax + " (theo " + v.theoSrc + ")"
      )) return false;
      submitStatus.style.color = "";
      submitStatus.textContent = "Saving template...";
      try {
        // templates.json stores ALL popup fields flat under params{} (instrument
        // is the map key). The submit payload (v.payload) keeps its split shape.
        var flatParams = {
          theo_source: v.payload.theo_source,
          theo_venue_symbol: v.payload.theo_venue_symbol,
          instrument_max_position: v.payload.instrument_max_position,
          width_bps: v.payload.params.width_bps,
          order_size: v.payload.params.order_size,
          adjust_position: v.payload.params.adjust_position,
          adjust_ticks: v.payload.params.adjust_ticks,
          min_theo_move_ticks_to_requote: v.payload.params.min_theo_move_ticks_to_requote,
          max_reload_cycles: v.payload.params.max_reload_cycles,
          quote_snapshot: v.payload.params.quote_snapshot,
          pricer_snapshot: v.payload.params.pricer_snapshot,
          slope: v.payload.params.slope,
        };
        var body = {
          instrument: v.ax,
          params: flatParams,
          name: name,
          mode: mode,
        };
        if (mode === "update" && moTemplateCtx && moTemplateCtx.name) body.old_name = moTemplateCtx.name;
        var jt = await api(
          "/api/desk/templates_save",
          { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) },
          15000,
        );
        if (!jt || !jt.ok) {
          submitStatus.style.color = "#fca5a5";
          submitStatus.textContent = (jt && jt.error) || "Template save failed";
          alert((jt && jt.error) || "Template save failed");
          return false;
        }
        // Future "Update" clicks in this same popup target the just-saved name.
        moTemplateCtx = { ax_symbol: v.ax, name: (jt.template && jt.template.name) || name };
      } catch (e) {
        submitStatus.style.color = "#fca5a5";
        submitStatus.textContent = String((e && e.message) || e);
        alert(e);
        return false;
      }
      submitStatus.textContent = "Template saved — submitting order...";
      void runManualSubmitJob(v.payload);
      return true;
    }
    b1.onclick = function() {
      if (b1.disabled) return;
      // H1 (2026-06-11): disable ALL Step-2 actions synchronously as the first statement so a
      // rapid second click (or a different action button) cannot fire before we hand off to the
      // async job. Re-enable here only if we do NOT hand off (runManualSubmitJob owns re-enable).
      setActions(true);
      var proceed = false;
      try {
        var v = mmReadAndValidateForm();
        if (!v) return;
        if (!deskMmGateConfirm("Send manual order for " + v.ax + " (theo " + v.theoSrc + ")")) return;
        submitStatus.style.color = "";
        submitStatus.textContent = "Submitting...";
        proceed = true;
        void runManualSubmitJob(v.payload);
      } finally {
        if (!proceed) setActions(false);
      }
    };
    // H1: disable synchronously before mmSaveTemplateAndSubmit's first await; it returns true iff
    // it handed off to the in-flight submit job (which then owns re-enable), false otherwise.
    function mmWireTemplateAction(btn, mode) {
      if (btn.disabled) return;
      setActions(true);
      Promise.resolve(mmSaveTemplateAndSubmit(mode)).then(function(handedOff) {
        if (!handedOff) setActions(false);
      }, function() { setActions(false); });
    }
    bSaveTpl.onclick = function() { mmWireTemplateAction(bSaveTpl, "create"); };
    bUpdTpl.onclick = function() { mmWireTemplateAction(bUpdTpl, "update"); };
    bNewTpl.onclick = function() { mmWireTemplateAction(bNewTpl, "create"); };

    // Template mode: skip Step 1, lock the instrument, and fill Step 2 from the
    // saved template (params{} holds ALL popup fields), overriding the
    // latest-stack defaults.
    function mmApplyPrefillToStep2(pf) {
      var p = (pf && pf.params) ? pf.params : {};
      function setv(id, val) {
        var e = document.getElementById(id);
        if (e && val != null && val !== "") e.value = String(val);
      }
      setv("moInstMx", p.instrument_max_position);
      setv("moW", p.width_bps != null ? p.width_bps : (p.width_ticks != null ? p.width_ticks : p.width));
      setv("moQ", p.order_size);
      setv("moAp", p.adjust_position);
      setv("moAt", p.adjust_ticks);
      setv("moMinDr", p.min_theo_move_ticks_to_requote != null ? p.min_theo_move_ticks_to_requote : p.min_drift_ticks);
      setv("moMrc", p.max_reload_cycles);
      // snapshots/slope: 0 is a meaningful value (disables transform) so set even when 0.
      var qE = document.getElementById("moQs"); if (qE && p.quote_snapshot != null) qE.value = String(p.quote_snapshot);
      var pE = document.getElementById("moPs"); if (pE && p.pricer_snapshot != null) pE.value = String(p.pricer_snapshot);
      var slE = document.getElementById("moSl"); if (slE && p.slope != null) slE.value = String(p.slope);
    }
    // Attach the overlay to the document BEFORE the template-mode block. The block below
    // (and mmDeskFillStep2ForAx / mmApplyPrefillToStep2) locate fields via
    // document.getElementById(), which only resolves once `ov` is in the DOM. When this
    // append happened *after* the block (it used to be at the very end of the function),
    // every getElementById() returned null, so the blanking AND the template-value fill were
    // silent no-ops and the inputs kept their creation-time defaults (gmx0 = mm_max_position =
    // 5000, moW = mm_width = 10, moQ = mm_order_size = 100, …). That is the long-standing
    // "Open Template shows 5000 / global defaults instead of the saved values" bug.
    document.body.appendChild(ov);
    if (moTemplateMode && moPrefillAx) {
      var moPf = (moPrefill && moPrefill.params) ? moPrefill.params : {};
      sel.value = moPrefillAx;
      applyLegDefaults();
      if (moPf.theo_source) theoSrcSel.value = String(moPf.theo_source);
      if (moPf.theo_venue_symbol != null) theoVenue.value = String(moPf.theo_venue_symbol);
      mmDeskFillStep2ForAx(moPrefillAx);
      mmApplyPrefillToStep2(moPrefill);
      sel.disabled = true;
      var moTitleEl = document.getElementById("moTitle");
      if (moTitleEl) moTitleEl.textContent = "Template: " + (moTemplateCtx && moTemplateCtx.name ? moTemplateCtx.name : moPrefillAx);
      var moHintEl = document.getElementById("moHint");
      if (moHintEl) moHintEl.textContent = "Editing template for " + moPrefillAx +
        ". Change any values, then Update this template or Create a new one — both also place the order.";
      step1.style.display = "none";
      step2.style.display = "block";
    }
  }
  el = document.getElementById("btnAddManualOrder");
  if (el) el.onclick = function() { void mmOpenManualOrderPopup({}); };

  // Templates browser: pick an instrument → list its saved templates → Open one
  // into the manual-order popup (prefilled + editable). Stored per-instrument in
  // templates.json at the project root (git-ignored, survives code upgrades;
  // server: /api/desk/templates*). Templates are only ever created when the user
  // clicks a save button — never automatically. Opening a template reuses the
  // exact same place-order popup/gateway path, so submission behavior is unchanged.
  async function mmOpenTemplatesBrowser() {
    var doc = {};
    try {
      var jt = await api("/api/desk/templates", {}, 10000);
      if (!jt || !jt.ok) { alert((jt && jt.error) || "Failed to load templates"); return; }
      doc = (jt.templates && typeof jt.templates === "object") ? jt.templates : {};
    } catch (e) {
      alert("Failed to load templates: " + String((e && e.message) || e));
      return;
    }
    var ov = document.createElement("div");
    ov.style.cssText = "position:fixed;inset:0;background:rgba(7,12,20,.85);z-index:21000;display:flex;align-items:center;justify-content:center;overflow-y:auto";
    var card = document.createElement("div");
    card.style.cssText = "width:min(720px,95vw);max-height:88vh;overflow:auto;background:#0f1724;border:1px solid #2a3548;border-radius:12px;padding:14px;box-shadow:0 12px 30px rgba(0,0,0,.45);color:#e6edf3";
    var h = document.createElement("h3");
    h.style.margin = "0 0 4px 0";
    h.textContent = "Order templates";
    card.appendChild(h);
    var hint = document.createElement("p");
    hint.className = "muted";
    hint.style.cssText = "font-size:12px;margin:0 0 10px 0";
    hint.textContent = "Select an instrument to see its saved templates, then Open one to edit and place the order.";
    card.appendChild(hint);

    function row(labelTxt, child) {
      var p = document.createElement("p");
      p.className = "fld";
      var l = document.createElement("label");
      l.textContent = labelTxt;
      l.style.display = "block";
      l.style.marginBottom = "4px";
      p.appendChild(l);
      p.appendChild(child);
      return p;
    }
    var axKeys = Object.keys(doc).filter(function(k) {
      return Array.isArray(doc[k]) && doc[k].length;
    }).sort();
    var sel = document.createElement("select");
    sel.style.cssText = "width:100%;padding:6px;background:#0b1220;color:#e6edf3;border:1px solid #30405a;border-radius:6px";
    var ph = document.createElement("option");
    ph.value = ""; ph.textContent = axKeys.length ? "— select an instrument —" : "— no templates saved yet —";
    sel.appendChild(ph);
    axKeys.forEach(function(k) {
      var o = document.createElement("option");
      o.value = k;
      o.textContent = k + "  ·  " + doc[k].length + " template" + (doc[k].length === 1 ? "" : "s");
      sel.appendChild(o);
    });
    card.appendChild(row("Instrument", sel));

    var tableHost = document.createElement("div");
    tableHost.style.marginTop = "8px";
    card.appendChild(tableHost);

    function fmtParams(t) {
      var p = (t && t.params) ? t.params : {};
      var bits = [];
      if (p.width_bps != null) bits.push("w=" + p.width_bps + "bps");
      if (p.order_size != null) bits.push("sz=" + p.order_size);
      if (p.instrument_max_position != null) bits.push("maxpo=" + p.instrument_max_position);
      if (p.adjust_position != null) bits.push("adjpo=" + p.adjust_position);
      if (p.adjust_ticks != null) bits.push("adjt=" + p.adjust_ticks);
      if (p.max_reload_cycles != null) bits.push("reload=" + p.max_reload_cycles);
      return bits.join("  ·  ");
    }
    function renderRows(ax) {
      tableHost.innerHTML = "";
      if (!ax) return;
      var arr = Array.isArray(doc[ax]) ? doc[ax] : [];
      if (!arr.length) {
        var p0 = document.createElement("p");
        p0.className = "muted";
        p0.textContent = "No templates for " + ax + ".";
        tableHost.appendChild(p0);
        return;
      }
      var tb = document.createElement("table");
      tb.className = "data";
      tb.style.width = "100%";
      var hdr = document.createElement("tr");
      ["name", "theo", "params", "", ""].forEach(function(t) {
        var th = document.createElement("th"); th.textContent = t; hdr.appendChild(th);
      });
      tb.appendChild(hdr);
      arr.forEach(function(tpl) {
        var tr = document.createElement("tr");
        function td(text) { var c = document.createElement("td"); c.textContent = String(text == null ? "—" : text); tr.appendChild(c); return c; }
        var tplP = (tpl && tpl.params) ? tpl.params : {};
        td(tpl.name || "—");
        td((tplP.theo_source || "—") + (tplP.theo_venue_symbol ? " · " + tplP.theo_venue_symbol : ""));
        td(fmtParams(tpl));
        var tdOpen = document.createElement("td");
        var bOpen = document.createElement("button");
        bOpen.type = "button"; bOpen.className = "btn btn-primary"; bOpen.textContent = "Open Template";
        bOpen.onclick = function() {
          try { document.body.removeChild(ov); } catch (e) {}
          void mmOpenManualOrderPopup({
            prefill: tpl,
            template: { ax_symbol: ax, name: tpl.name },
          });
        };
        tdOpen.appendChild(bOpen);
        tr.appendChild(tdOpen);
        var tdDel = document.createElement("td");
        var bDel = document.createElement("button");
        bDel.type = "button"; bDel.className = "btn btn-danger"; bDel.textContent = "Delete";
        bDel.style.background = "#b91c1c"; bDel.style.borderColor = "#7f1d1d"; bDel.style.color = "#fff";
        bDel.onclick = async function() {
          if (!confirm("Delete template '" + tpl.name + "' for " + ax + "?")) return;
          try {
            var jd = await api(
              "/api/desk/templates_delete",
              { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ instrument: ax, name: tpl.name }) },
              10000,
            );
            if (!jd || !jd.ok) { alert((jd && jd.error) || "Delete failed"); return; }
            doc = (jd.templates && typeof jd.templates === "object") ? jd.templates : {};
            if (!Array.isArray(doc[ax]) || !doc[ax].length) {
              // Instrument now empty — rebuild the dropdown.
              try { document.body.removeChild(ov); } catch (e2) {}
              void mmOpenTemplatesBrowser();
              return;
            }
            renderRows(ax);
          } catch (e) {
            alert("Delete failed: " + String((e && e.message) || e));
          }
        };
        tdDel.appendChild(bDel);
        tr.appendChild(tdDel);
        tb.appendChild(tr);
      });
      tableHost.appendChild(tb);
    }
    sel.onchange = function() { renderRows(String(sel.value || "")); };

    var rowBtn = document.createElement("div");
    rowBtn.style.cssText = "display:flex;justify-content:flex-end;gap:8px;margin-top:14px";
    var bClose = document.createElement("button");
    bClose.type = "button"; bClose.className = "btn"; bClose.textContent = "Close";
    bClose.onclick = function() { try { document.body.removeChild(ov); } catch (e) {} };
    rowBtn.appendChild(bClose);
    card.appendChild(rowBtn);

    ov.appendChild(card);
    document.body.appendChild(ov);
  }
  el = document.getElementById("btnTemplates");
  if (el) el.onclick = function() { void mmOpenTemplatesBrowser(); };
  el = document.getElementById("btnCancelOrder");
  if (el) el.onclick = async function() {
    // Cancel-Order popup, two-step:
    //   1. Pick a product from a dropdown of products that currently have
    //      stacks in orders.json (Python-owned desired state).
    //   2. Show a table of that product's stacks (params from orders.json
    //      joined to live AX state from mm_orders.json by stack_id).
    //      The rightmost column is a Cancel button; clicking it removes
    //      that stack from orders.json. trading_client reconciles within
    //      ~1s and BaseStrategy::stop cancels the AX bid+ask before
    //      unregistering the strategy.
    var st0 = window.__mmDeskLastState || {};
    var ordersCfg = (st0 && st0.mm_orders_config) ? st0.mm_orders_config : null;
    var products = (ordersCfg && ordersCfg.products && typeof ordersCfg.products === "object") ? ordersCfg.products : {};
    var productKeys = mmDeskMmMovePanelSymbolOrder(st0, products);
    var liveSnap = (st0 && st0.live_orders) ? st0.live_orders : null;
    var liveByStackId = {};
    if (liveSnap && Array.isArray(liveSnap.orders)) {
      for (var li = 0; li < liveSnap.orders.length; li++) {
        var lr = liveSnap.orders[li];
        if (!lr) continue;
        var sid = String(lr.stack_id || lr.request_id || "");
        if (sid) liveByStackId[sid] = lr;
      }
    }
    var ov = document.createElement("div");
    ov.style.cssText = "position:fixed;inset:0;background:rgba(7,12,20,.88);z-index:21000;display:flex;align-items:center;justify-content:center;overflow:auto";
    var card = document.createElement("div");
    card.style.cssText = "width:min(960px,96vw);max-height:88vh;overflow:auto;background:#0f1724;border:1px solid #2a3548;border-radius:12px;padding:12px;color:#e6edf3";
    var h2 = document.createElement("h3");
    h2.style.margin = "0 0 4px 0";
    h2.style.color = "#fca5a5";
    h2.textContent = "Cancel Order";
    card.appendChild(h2);
    var hint = document.createElement("p");
    hint.className = "muted";
    hint.style.fontSize = "12px";
    hint.style.margin = "0 0 10px 0";
    hint.textContent = "Product list matches AX depth / book order. Pick a product, then Cancel one stack at a time: desk cancels venue OIDs first, then removes the row from orders.json.";
    card.appendChild(hint);

    if (!productKeys.length) {
      var p0 = document.createElement("p");
      p0.className = "muted";
      p0.textContent = "No AX symbols in depth list and no orders.json products. Configure depth / book symbols or add a stack.";
      card.appendChild(p0);
    } else {
      var rowSel = document.createElement("p");
      rowSel.className = "fld";
      var labSel = document.createElement("label");
      labSel.textContent = "Product (depth / book order)";
      labSel.style.display = "block";
      labSel.style.marginBottom = "4px";
      rowSel.appendChild(labSel);
      var prodSel = document.createElement("select");
      prodSel.style.cssText = "width:100%;padding:6px;background:#0b1220;color:#e6edf3;border:1px solid #30405a;border-radius:6px";
      var phOpt = document.createElement("option");
      phOpt.value = "";
      phOpt.textContent = "— select a product —";
      prodSel.appendChild(phOpt);
      for (var pi = 0; pi < productKeys.length; pi++) {
        var axUi = productKeys[pi];
        var pkCanon = mmDeskResolveOrdersJsonProductKey(products, axUi);
        var prod = pkCanon ? (products[pkCanon] || {}) : {};
        var nStacks = Array.isArray(prod.stacks) ? prod.stacks.length : 0;
        var opt = document.createElement("option");
        opt.value = axUi;
        opt.textContent = pkCanon
          ? pkCanon + "  ·  " + nStacks + " stack" + (nStacks === 1 ? "" : "s") + (prod.theo_source ? "  ·  " + prod.theo_source : "")
          : axUi + "  ·  (no orders.json product)";
        prodSel.appendChild(opt);
      }
      rowSel.appendChild(prodSel);
      card.appendChild(rowSel);

      var tableHost = document.createElement("div");
      tableHost.style.marginTop = "8px";
      card.appendChild(tableHost);

      function renderStackTable(ax) {
        tableHost.innerHTML = "";
        if (!ax) return;
        var pkUse = mmDeskResolveOrdersJsonProductKey(products, ax) || (products[ax] ? ax : null);
        var prod = pkUse && products[pkUse] ? products[pkUse] : {};
        var stacks = Array.isArray(prod.stacks) ? prod.stacks.slice() : [];
        // Merge in VENUE-ONLY (orphan) stacks: live in mm_orders.json for this AX but absent from
        // orders.json. These are exactly the rows that make the exchange show open orders while this
        // dropdown is otherwise empty. Cancelling one routes to the backend orphan path, which
        // signals C++ to force-cancel by exchange OID (Python has no exchange OID for them).
        var haveSid = {};
        for (var hi = 0; hi < stacks.length; hi++) {
          var hsid = String((stacks[hi] && stacks[hi].id) || "");
          if (hsid) haveSid[hsid] = true;
        }
        var axCanon = String(ax || "").trim().toUpperCase();
        if (liveSnap && Array.isArray(liveSnap.orders)) {
          for (var oi = 0; oi < liveSnap.orders.length; oi++) {
            var olr = liveSnap.orders[oi];
            if (!olr) continue;
            var oax = String(olr.ax_symbol || olr.order_symbol || "").trim().toUpperCase();
            if (axCanon && oax && oax !== axCanon) continue;
            var osid = String(olr.stack_id || olr.request_id || "");
            if (!osid || haveSid[osid]) continue;
            var op = (olr.params && typeof olr.params === "object") ? olr.params : {};
            stacks.push({
              id: osid,
              __orphan: true,
              width_ticks: op.width_ticks,
              order_size: op.order_size,
              max_position: op.max_position,
              adjust_position: op.adjust_position,
              adjust_ticks: op.adjust_ticks,
              min_theo_move_ticks_to_requote: op.min_theo_move_ticks_to_requote,
              max_reload_cycles: op.max_reload_cycles
            });
            haveSid[osid] = true;
          }
        }
        if (!stacks.length) {
          var p1 = document.createElement("p");
          p1.className = "muted";
          p1.textContent = !pkUse
            ? ("No orders.json product for " + ax + " and no venue-only orders — nothing to cancel here.")
            : ("No stacks for " + (pkUse || ax) + " in orders.json or on the venue.");
          tableHost.appendChild(p1);
          return;
        }
        var subhead = document.createElement("p");
        subhead.className = "muted";
        subhead.style.fontSize = "11px";
        subhead.textContent = "theo_source = " + (prod.theo_source || "—") + "  ·  theo_venue = " + (prod.theo_venue_symbol || "—") + "  ·  max_position (instrument) = " + (prod.max_position != null ? prod.max_position : "—");
        tableHost.appendChild(subhead);
        var tb = document.createElement("table");
        tb.className = "data";
        tb.style.width = "100%";
        var hdr = document.createElement("tr");
        ["stack_id", "width_bps", "size", "max_po", "adj_po", "adj_t", "min_drift", "max_reload", "bid (live)", "ask (live)", "fills", "blocked", ""].forEach(function(t) {
          var th = document.createElement("th"); th.textContent = t; hdr.appendChild(th);
        });
        tb.appendChild(hdr);
        for (var si = 0; si < stacks.length; si++) {
          var s = stacks[si] || {};
          var tr = document.createElement("tr");
          function td(text) { var c = document.createElement("td"); c.textContent = String(text == null ? "—" : text); tr.appendChild(c); return c; }
          var sid = String(s.id || "");
          var sidCell = td(sid.substring(0, 12) || "—");
          if (s.__orphan) {
            sidCell.textContent = (sid.substring(0, 12) || "—") + "  ⚠ orphan";
            sidCell.style.color = "#fbbf24";
            sidCell.title = "Live on the venue but missing from orders.json — cancel routes to C++ force-cancel by exchange OID.";
          }
          td(s.width_bps != null ? s.width_bps : (s.width_ticks != null ? s.width_ticks : (s.width != null ? s.width : "—")));
          td(s.order_size != null ? s.order_size : "—");
          td(s.max_position != null ? s.max_position : "—");
          td(s.adjust_position != null ? s.adjust_position : "—");
          td(s.adjust_ticks != null ? s.adjust_ticks : "—");
          td(s.min_theo_move_ticks_to_requote != null ? s.min_theo_move_ticks_to_requote : "—");
          td(s.max_reload_cycles != null ? s.max_reload_cycles : "—");
          var live = liveByStackId[sid] || null;
          var bSide = (live && live.sides && live.sides.bid) || {};
          var aSide = (live && live.sides && live.sides.ask) || {};
          td(mmDeskFormatMmMoveSideCell(!!live, bSide));
          td(mmDeskFormatMmMoveSideCell(!!live, aSide));
          td(live ? (String(live.fills_count || 0) + (live.last_fill_id ? "  · last=" + String(live.last_fill_id).substring(0, 10) : "")) : "—");
          var blk = !!(live && live.blocked_by_feed);
          var blkTd = td(blk ? "BLOCKED" : (live ? "ok" : "—"));
          if (blk) blkTd.style.color = "#ff8a8a";
          var tdA = document.createElement("td");
          var aBtn = document.createElement("button");
          aBtn.type = "button"; aBtn.className = "btn btn-danger"; aBtn.textContent = "Cancel";
          aBtn.style.background = "#b91c1c"; aBtn.style.borderColor = "#7f1d1d"; aBtn.style.color = "#fff";
          (function(stackId, axs, isOrphan) {
            aBtn.onclick = async function() {
              if (!stackId) return;
              var msg = isOrphan
                ? ("Cancel VENUE-ONLY stack " + stackId + " on " + axs + "?\n\nThis stack is live on the exchange but missing from orders.json. The desk signals C++ to cancel the tracked legs by exchange OID.")
                : ("Cancel stack " + stackId + " on " + axs + "?\n\nDesk cancels venue OIDs first, removes the row from orders.json, and signals C++ to force-cancel any moved legs.");
              if (!confirm(msg)) return;
              try {
                const cj = await api("/api/desk/cancel_one_order", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({ stack_id: stackId, ax_symbol: axs }),
                });
                if (!cj || !cj.ok) { alert((cj && cj.error) || JSON.stringify(cj) || "cancel failed"); return; }
                try { document.body.removeChild(ov); } catch (e2) {}
                refresh();
              } catch (e) { alert(e); }
            };
          })(sid, pkUse || ax, !!s.__orphan);
          tdA.appendChild(aBtn);
          tr.appendChild(tdA);
          tb.appendChild(tr);
        }
        tableHost.appendChild(tb);
      }
      prodSel.onchange = function() { renderStackTable(String(prodSel.value || "")); };
    }

    var bc = document.createElement("button");
    bc.className = "btn";
    bc.textContent = "Close";
    bc.style.marginTop = "12px";
    bc.onclick = function() { try { document.body.removeChild(ov); } catch (e) {} };
    card.appendChild(bc);
    ov.appendChild(card);
    document.body.appendChild(ov);
  };
  el = document.getElementById("btnOrdersReset");
  if (el) el.onclick = async function() {
    try {
      if (!confirm(
        "Clear ALL desk stacks?\n\nThis will POST cancel for every bid_exchange_oid / ask_exchange_oid "
          + "listed in orders.json, then delete the file contents. C++ will have nothing to move until you place again."
      )) return;
      const j = await api("/api/desk/orders_reset", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ confirm: true }),
      });
      if (!j || !j.ok) {
        alert((j && j.message) || (j && j.error) || "orders_reset failed");
        return;
      }
      setDeskActionFromJson(j, "orders.json cleared");
      refresh();
    } catch (e) {
      alert(String((e && e.message) || e));
    }
  };
  el = document.getElementById("btnMmOrdersGate");
  if (el) el.onclick = async function() {
    try {
      mmDeskOpenOrdersMmMovePanel(window.__mmDeskLastState || {});
    } catch (e) {
      alert(String((e && e.message) || e));
    }
  };
  wireInstrumentCancelHold();
  wireFastMarketPanel();
  el = document.getElementById("btnAxOrderbookApply");
  if (el) el.onclick = async function() {
    try {
      syncParamsForm("1");
      var st = window.__mmDeskLastState || {};
      var selOb = document.getElementById("axOrderbookMulti");
      if (!selOb || !selOb.options || selOb.options.length === 0) {
        alert(
          "Architect symbol list is not ready yet (GET /instruments). Wait until the multiselect has rows, " +
            "then select symbols and click Apply again."
        );
        return;
      }
      var extras = gatherAxOrderbookExtrasFromUi();
      var activeAx = String(mmDeskActiveAxSymbol(st) || "").trim();
      var activeRow = mmDeskFindInstrumentRow(st, activeAx);
      var payload = {
        ax_symbol: activeAx || String(st.ax_symbol || "").trim(),
        ref_symbol: String(
          (activeRow && (activeRow.reference_fix_symbol || activeRow.theo_symbol)) || st.ref_symbol || ""
        ).trim(),
        ax_orderbook_extras: extras,
      };
      const j = await api("/api/desk/instruments", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!j.ok) { alert(j.error || "save failed"); return; }
      location.reload();
    } catch (e) { alert(e); }
  };
  el = document.getElementById("theoLegViewSelect");
  if (el) {
    el.addEventListener("change", function() {
      var o = el.options[el.selectedIndex];
      if (!o) return;
      var ax = o.getAttribute("data-ax") || "";
      var ref = o.getAttribute("data-ref") || "";
      try { mmDeskSetActiveInstrument(ax, ref); } catch (e1) { console.warn(e1); }
      try { paintLive(window.__mmDeskLastState || {}); } catch (e1b) { console.warn(e1b); }
      try {
        if (Array.isArray(window.__mmDeskInstrumentSyms) && window.__mmDeskInstrumentSyms.length) {
          populateAxOrderbookMultiOptions(window.__mmDeskInstrumentSyms);
        }
      } catch (e2) { console.warn(e2); }
    });
  }
  el = document.getElementById("axOrderbookMulti");
  if (el) el.addEventListener("change", function() {
    window.__mmOrderbookUserTouched = true;
    var stCh = window.__mmDeskLastState || {};
    try {
      updateAxOrderbookExtrasSummary(stCh);
    } catch (e) { console.warn("axOrderbookMultiSummary", e); }
    try { renderAllAxBooks(stCh); } catch (e2) { console.warn("renderAllAxBooks after multiselect", e2); }
    try {
      hydrateTheoLegViewFromState(stCh);
      paintMmGateButtons(stCh);
    } catch (e3) { console.warn("theoLegView after multiselect", e3); }
  });
  el = document.getElementById("neonMdMulti");
  if (el) el.addEventListener("change", function() {
    window.__mmNeonMdUserTouched = true;
  });
  el = document.getElementById("btnNeonMdApply");
  if (el) el.onclick = async function() {
    try {
      var syms = gatherNeonMdFromUi();
      if (!syms.length) { alert("Select at least one Neon symbol"); return; }
      const j = await api("/api/desk/neon_md_symbols", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ md_symbols: syms }),
      });
      if (!j.ok) { alert(j.error || "save failed"); return; }
      window.__mmNeonMdUserTouched = false;
      location.reload();
    } catch (e) { alert(e); }
  };
})();
(function mmDeskRevealUi() {
  function go() {
    if (typeof window.mmDeskDismissBoot === "function") window.mmDeskDismissBoot();
  }
  go();
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", go);
})();

/* =========================================================================
   Per-instrument cancel-all + HOLD (Tim, Basecamp 2026-07-23).
   Dropdown = union of configured instruments and any symbol with live venue
   orders (D6). "Cancel all orders" opens a confirmation modal showing a FRESH
   venue open-order count, then runs the server-side D5 sequence (write hold ->
   wait for engine to observe -> cancel all -> verify -> retry). Never shows
   success on an unverified state: a NOT-DONE result lists the residual OIDs.
   Plain-language UI (Tim, 2026-07-26): no "HELD"/"HOLD" jargon; re-enable button
   removed (it double-placed old stacks). The dropdown refreshes on open so its
   open-order count/stopped marker can never go stale after a place elsewhere.
   ========================================================================= */
function mmDeskInstrHoldCountStacks(ax) {
  var st = window.__mmDeskLastState || {};
  var live = (st && st.live_orders) ? st.live_orders : null;
  var seen = {}, n = 0;
  var want = String(ax || "").trim().toUpperCase();
  if (live && Array.isArray(live.orders)) {
    for (var i = 0; i < live.orders.length; i++) {
      var r = live.orders[i]; if (!r) continue;
      if (String(r.ax_symbol || "").trim().toUpperCase() !== want) continue;
      var sid = String(r.stack_id || r.request_id || ("#" + i));
      if (!seen[sid]) { seen[sid] = 1; n++; }
    }
  }
  return n;
}
function mmDeskInstrHoldRenderOut(res) {
  var out = document.getElementById("instrHoldOut");
  if (!out) return;
  if (!res) { out.textContent = ""; return; }
  var ax = String(res.ax_symbol || "");
  if (res.clean) {
    out.style.color = "#8fe388";
    out.textContent = ax + " — all orders cancelled (0 open). Quoting stopped.";
    return;
  }
  out.style.color = "#ff8a8a";
  var oids = Array.isArray(res.residual_oids) ? res.residual_oids : [];
  out.textContent = "NOT DONE — " + ax + ": "
    + (res.residual_open != null ? res.residual_open : oids.length)
    + " order(s) still open after " + (res.attempts || 0) + " try/tries. OIDs: "
    + (oids.length ? oids.join(", ") : "(unknown)")
    + ". Try again.";
}
async function mmDeskInstrHoldRefreshOptions(preserveValue) {
  var sel = document.getElementById("instrHoldSelect");
  if (!sel) return null;
  var prev = preserveValue != null ? preserveValue : sel.value;
  var j = null;
  try {
    j = await api("/api/desk/instrument_hold_options", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
  } catch (e) { console.warn("instrument_hold_options", e); return null; }
  window.__mmDeskInstrHoldOptions = j;
  var items = (j && Array.isArray(j.instruments)) ? j.instruments : [];
  while (sel.options.length > 1) sel.remove(1);
  for (var i = 0; i < items.length; i++) {
    var it = items[i];
    var label = it.ax_symbol
      + " — " + it.live_open_count + " open"
      + (it.held ? "  (quoting stopped)" : "")
      + (it.unconfigured ? "  (not in config)" : "");
    var o = document.createElement("option");
    o.value = it.ax_symbol;
    o.textContent = label;
    if (it.held) o.style.color = "#f2b705";
    sel.appendChild(o);
  }
  if (prev) sel.value = prev;
  return j;
}
function mmDeskInstrHoldFindItem(ax) {
  var j = window.__mmDeskInstrHoldOptions;
  var items = (j && Array.isArray(j.instruments)) ? j.instruments : [];
  var want = String(ax || "").trim().toUpperCase();
  for (var i = 0; i < items.length; i++) {
    if (String(items[i].ax_symbol || "").toUpperCase() === want) return items[i];
  }
  return null;
}
function mmDeskInstrHoldCloseModal() {
  var m = document.getElementById("instrHoldModal");
  if (m) m.style.display = "none";
}
async function mmDeskInstrHoldOpenModal(ax) {
  // FRESH options so the modal's live open-order count comes from a fresh venue read.
  await mmDeskInstrHoldRefreshOptions(ax);
  var it = mmDeskInstrHoldFindItem(ax);
  var liveN = it ? it.live_open_count : "?";
  var unconf = it && it.unconfigured;
  var title = document.getElementById("instrHoldModalTitle");
  if (title) title.textContent = "Cancel all " + ax + "?";
  var body = document.getElementById("instrHoldModalBody");
  if (body) {
    body.innerHTML =
      "Open orders at venue right now: <strong>" + liveN + "</strong>."
      + (unconf ? " <span style='color:#f2b705'>(This instrument has live orders but is not in the config.)</span>" : "");
  }
  var m = document.getElementById("instrHoldModal");
  if (m) m.style.display = "flex";
}
function wireInstrumentCancelHold() {
  var sel = document.getElementById("instrHoldSelect");
  if (sel && !sel.__wired) {
    sel.__wired = true;
    mmDeskInstrHoldRefreshOptions();
    // Refresh on open so the open-order count / "quoting stopped" marker is never
    // stale after orders were placed or cancelled elsewhere (Tim, 2026-07-26).
    var refreshOpts = function() {
      try { mmDeskInstrHoldRefreshOptions(sel.value); } catch (e) {}
    };
    sel.addEventListener("mousedown", refreshOpts);
    sel.addEventListener("focus", refreshOpts);
  }
  var btnHold = document.getElementById("btnInstrCancelHold");
  if (btnHold && !btnHold.__wired) {
    btnHold.__wired = true;
    btnHold.onclick = async function() {
      var s = document.getElementById("instrHoldSelect");
      var ax = s ? String(s.value || "").trim() : "";
      if (!ax) { alert("Select an instrument first."); return; }
      try { await mmDeskInstrHoldOpenModal(ax); } catch (e) { alert(String((e && e.message) || e)); }
    };
  }
  var btnCancelModal = document.getElementById("btnInstrHoldCancelModal");
  if (btnCancelModal && !btnCancelModal.__wired) {
    btnCancelModal.__wired = true;
    btnCancelModal.onclick = function() { mmDeskInstrHoldCloseModal(); };
  }
  var btnConfirm = document.getElementById("btnInstrHoldConfirm");
  if (btnConfirm && !btnConfirm.__wired) {
    btnConfirm.__wired = true;
    btnConfirm.onclick = async function() {
      var s = document.getElementById("instrHoldSelect");
      var ax = s ? String(s.value || "").trim() : "";
      if (!ax) { mmDeskInstrHoldCloseModal(); return; }
      btnConfirm.disabled = true;
      var out = document.getElementById("instrHoldOut");
      if (out) { out.style.color = "#e8dcc4"; out.textContent = "Cancelling all " + ax + " orders…"; }
      try {
        var j = await api("/api/desk/instrument_cancel_all", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ax_symbol: ax, hold: true }),
        });
        mmDeskInstrHoldRenderOut(j);
      } catch (e) {
        if (out) { out.style.color = "#ff8a8a"; out.textContent = "Request failed: " + String((e && e.message) || e); }
      } finally {
        btnConfirm.disabled = false;
        mmDeskInstrHoldCloseModal();
        try { await mmDeskInstrHoldRefreshOptions(ax); } catch (e2) {}
        try { refresh(); } catch (e3) {}
      }
    };
  }
}

/* =========================================================================
   Fast-market breaker config panel (Item 6, Tim 2026-07-27).
   Renders market_maker.fast_market.* read-only first; Edit reveals the form
   (5 keys + explicit Enabled toggle). Save validates and writes via the
   hardened _default_config_mutate_write (server route). The engine reads the
   block live (~1 Hz) after its mtime-gated reload, so a Save is picked up
   within ~1-2s without a restart.
   ========================================================================= */
function mmFastMktSetText(id, val) {
  var el = document.getElementById(id);
  if (el) el.textContent = String(val);
}
function mmFastMktRenderView(cfg) {
  window.__mmFastMktCfg = cfg || {};
  var c = window.__mmFastMktCfg;
  var enEl = document.getElementById("fmEnabledView");
  if (enEl) {
    enEl.textContent = c.enabled ? "ON — watching" : "OFF";
    enEl.style.color = c.enabled ? "#8fe388" : "#f2b705";
  }
  mmFastMktSetText("fmSymbolView", c.hl_spx_symbol || "(none)");
  mmFastMktSetText("fmSizeView", c.size_of_move_ticks != null ? c.size_of_move_ticks : "—");
  mmFastMktSetText("fmWindowView", c.time_of_move_sec != null ? c.time_of_move_sec : "—");
  mmFastMktSetText("fmPullView", c.pull_sec != null ? c.pull_sec : "—");
  mmFastMktSetText("fmTickView", c.tick_size != null ? c.tick_size : "—");
}
async function mmFastMktLoad() {
  try {
    var c = await api("/api/desk/fast_market_get", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({}),
    });
    mmFastMktRenderView(c || {});
  } catch (e) {
    var out = document.getElementById("fastMktOut");
    if (out) { out.style.color = "#ff8a8a"; out.textContent = "Load failed: " + String((e && e.message) || e); }
  }
}
function mmFastMktShowEdit(show) {
  var v = document.getElementById("fastMktView");
  var ed = document.getElementById("fastMktEdit");
  if (v) v.style.display = show ? "none" : "block";
  if (ed) ed.style.display = show ? "block" : "none";
}
function mmFastMktFillEditFromView() {
  var c = window.__mmFastMktCfg || {};
  function setV(id, val) { var el = document.getElementById(id); if (el) el.value = (val == null ? "" : String(val)); }
  var en = document.getElementById("fmEnabled");
  if (en) en.checked = !!c.enabled;
  setV("fmSymbol", c.hl_spx_symbol || "");
  setV("fmSize", c.size_of_move_ticks || "");
  setV("fmWindow", c.time_of_move_sec || "");
  setV("fmPull", c.pull_sec || "");
  setV("fmTick", c.tick_size || "");
}
function wireFastMarketPanel() {
  var panel = document.getElementById("fastMktPanel");
  if (!panel || panel.__wired) { if (panel) mmFastMktLoad(); return; }
  panel.__wired = true;
  mmFastMktLoad();
  var btnEdit = document.getElementById("btnFastMktEdit");
  if (btnEdit) btnEdit.onclick = function() { mmFastMktFillEditFromView(); mmFastMktShowEdit(true); };
  var btnCancel = document.getElementById("btnFastMktCancel");
  if (btnCancel) btnCancel.onclick = function() {
    mmFastMktShowEdit(false);
    var out = document.getElementById("fastMktOut"); if (out) out.textContent = "";
  };
  var btnSave = document.getElementById("btnFastMktSave");
  if (btnSave) btnSave.onclick = async function() {
    var out = document.getElementById("fastMktOut");
    function gv(id) { var el = document.getElementById(id); return el ? String(el.value || "").trim() : ""; }
    var payload = {
      enabled: !!(document.getElementById("fmEnabled") || {}).checked,
      hl_spx_symbol: gv("fmSymbol"),
      size_of_move_ticks: parseInt(gv("fmSize"), 10),
      time_of_move_sec: parseInt(gv("fmWindow"), 10),
      pull_sec: parseInt(gv("fmPull"), 10),
      tick_size: parseFloat(gv("fmTick")),
    };
    // Light client-side validation; the server re-validates and is authoritative.
    if (!payload.hl_spx_symbol) { if (out) { out.style.color = "#ff8a8a"; out.textContent = "Symbol is required (use Enabled to turn off)."; } return; }
    if (!(payload.size_of_move_ticks > 0) || !(payload.time_of_move_sec > 0) || !(payload.pull_sec > 0)) {
      if (out) { out.style.color = "#ff8a8a"; out.textContent = "Move size, window, and pull must all be > 0."; } return;
    }
    if (!(payload.tick_size > 0)) { if (out) { out.style.color = "#ff8a8a"; out.textContent = "Tick size must be > 0."; } return; }
    btnSave.disabled = true;
    if (out) { out.style.color = "#e8dcc4"; out.textContent = "Saving…"; }
    try {
      var r = await api("/api/desk/fast_market_save", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
      });
      if (out) { out.style.color = "#8fe388"; out.textContent = "Saved. Engine picks it up within ~1–2s."; }
      await mmFastMktLoad();
      mmFastMktShowEdit(false);
    } catch (e) {
      if (out) { out.style.color = "#ff8a8a"; out.textContent = "Save rejected: " + String((e && e.message) || e); }
    } finally {
      btnSave.disabled = false;
    }
  };
}

bootDesk();
scheduleNextDeskPoll(150);
