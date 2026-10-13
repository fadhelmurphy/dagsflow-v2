/* dagsflow visual builder: drag-and-drop editor that saves ordinary YAML DAGs.
   Loaded on demand by index.html (route #/builder). Uses its globals: api, esc, enc,
   toast, can, fmt, openActDlg. */
(function () {
"use strict";

const W = 230, H = 64, GRID = 10;
const CAT_COLOR = { Sources: "#2F7BD9", Transform: "#8B5CF6", Destinations: "#2E9B57", Control: "#64748B", Sensors: "#D9971A" };
const ICONS = {
  globe: '<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c3 3.5 3 14.5 0 18M12 3c-3 3.5-3 14.5 0 18"/>',
  db: '<ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  bq: '<ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><circle cx="12" cy="13" r="2.5"/>',
  table: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M3 10h18M3 15h18M9 4v16"/>',
  dbin: '<ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><path d="M12 10v7m-3-3 3 3 3-3"/>',
  shuffle: '<path d="M16 3h5v5M4 20 21 3M21 16v5h-5M15 15l6 6M4 4l5 5"/>',
  bolt: '<path d="M13 2 4 14h7l-1 8 9-12h-7z"/>',
  terminal: '<path d="m4 17 6-6-6-6M12 19h8"/>',
  play: '<path d="m6 4 14 8-14 8z"/>',
  dot: '<circle cx="12" cy="12" r="4"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
};
const icon = (n, s = 18) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICONS[n] || ICONS.dot}</svg>`;
const SCHEDULES = [["", "Manual only"], ["@hourly", "Every hour"], ["@daily", "Every day (midnight)"], ["@weekly", "Every week"], ["@monthly", "Every month"], ["0 6 * * *", "Every day at 06:00"], ["*/15 * * * *", "Every 15 minutes"]];
const STEP_OPS = [["extract", "Extract path"], ["select", "Keep fields"], ["drop", "Remove fields"], ["rename", "Rename fields"], ["filter", "Filter rows"], ["set", "Set / add field"], ["sort", "Sort"], ["limit", "Limit"], ["dedupe", "Remove duplicates"]];
const CMPS = ["==", "!=", ">", ">=", "<", "<=", "contains", "exists", "not_exists"];

const STYLE = `
main.bldmain{padding:0;max-width:none}
.bld{display:flex;flex-direction:column;height:calc(100vh - 58px);min-height:420px;background:#F4F6F8}
.bld *{box-sizing:border-box}
.bld-top{display:flex;align-items:center;gap:8px;padding:8px 12px;background:#fff;border-bottom:1px solid var(--rule);flex-wrap:wrap}
.bld-top .sp{flex:1}
.bld-top input.bid{font-weight:600;font-size:15px;width:230px}
.bld-top .st{font-size:12.5px;color:var(--muted);white-space:nowrap}
.bld-top .st a,.bld-top .st button.lnk{color:var(--action);background:none;border:0;padding:0;cursor:pointer;font:inherit}
.bld-top .st.bad{color:var(--s-failed)}
.bld .btn.sm{padding:3px 9px;font-size:12.5px}
.bld-top .sep{width:1px;height:22px;background:var(--rule);margin:0 2px}
.bld-main{flex:1;display:flex;min-height:0}
.bld-pal{width:210px;background:#fff;border-right:1px solid var(--rule);overflow-y:auto;padding:10px;flex:none}
.bld-pal input{width:100%;margin-bottom:8px}
.bld-pal h4{margin:12px 0 6px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
.bp-item{display:flex;align-items:center;gap:9px;padding:7px 9px;margin-bottom:5px;border:1px solid var(--rule);border-radius:6px;background:#fff;cursor:grab;font-size:13px;user-select:none}
.bp-item:hover{border-color:#9FAAB5;box-shadow:0 1px 4px rgba(22,32,42,.1)}
.bp-ic{width:26px;height:26px;border-radius:6px;display:flex;align-items:center;justify-content:center;color:#fff;flex:none}
.bld-cv{position:relative;flex:1;overflow:hidden;background-color:#F4F6F8;background-image:radial-gradient(#C9D1D9 1px,transparent 1px);background-size:20px 20px;touch-action:none;cursor:grab}
.bld-cv.panning{cursor:grabbing}
.bld-cv.ro .bn{cursor:default}
.bld-world{position:absolute;left:0;top:0;transform-origin:0 0}
.bld-edges{position:absolute;left:0;top:0;width:1px;height:1px;overflow:visible;pointer-events:none}
.bld-edges .hit{stroke:transparent;stroke-width:16;fill:none;pointer-events:stroke;cursor:pointer}
.bld-edges .ln{stroke:#8A96A3;stroke-width:2;fill:none}
.bld-edges .sel .ln{stroke:var(--action);stroke-width:3}
.bld-edges .tmp{stroke:var(--action);stroke-width:2;stroke-dasharray:6 4;fill:none}
.bld-edges .edel{pointer-events:all;cursor:pointer}
.bn{position:absolute;width:${W}px;height:${H}px;background:#fff;border:1.5px solid #CBD3DB;border-radius:9px;display:flex;align-items:center;gap:10px;padding:0 12px 0 14px;cursor:grab;box-shadow:0 1px 3px rgba(22,32,42,.08);user-select:none}
.bn:hover{border-color:#9FAAB5}
.bn.sel{border-color:var(--action);box-shadow:0 0 0 3px rgba(31,95,209,.18)}
.bn.err{border-color:var(--s-failed)}
.bn.warn:not(.err){border-color:#E0A21B}
.bn-bar{position:absolute;left:0;top:0;bottom:0;width:5px;border-radius:8px 0 0 8px}
.bn-ic{width:30px;height:30px;border-radius:8px;display:flex;align-items:center;justify-content:center;color:#fff;flex:none}
.bn-tx{min-width:0;flex:1}
.bn-t{font-weight:600;font-size:13.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bn-s{font-size:11.5px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-family:var(--code)}
.bn-port{position:absolute;top:${H / 2 - 7}px;width:14px;height:14px;border-radius:50%;background:#fff;border:2px solid #8A96A3}
.bn-port.in{left:-8px}
.bn-port.out{right:-8px;cursor:crosshair}
.bn-port.out:hover,.bld-cv.connecting .bn-port.in{border-color:var(--action);background:#E6EEFB}
.bn-st{position:absolute;top:-6px;right:-6px;width:14px;height:14px;border-radius:50%;border:2px solid #fff;display:none}
.bn.hasst .bn-st{display:block}
.bn-bd{position:absolute;top:-9px;left:-9px;min-width:18px;height:18px;border-radius:9px;background:var(--s-failed);color:#fff;font-size:11px;font-weight:700;text-align:center;line-height:18px;display:none;padding:0 4px}
.bn.err .bn-bd{display:block}
.bn.warn:not(.err) .bn-bd{display:block;background:#E0A21B}
.bld-zoom{position:absolute;left:12px;bottom:12px;display:flex;gap:4px;background:#fff;border:1px solid var(--rule);border-radius:6px;padding:3px}
.bld-zoom button{border:0;background:none;width:28px;height:26px;border-radius:4px;font-size:15px}
.bld-zoom button:hover{background:#EEF1F4}
.bld-empty{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none;color:var(--muted);text-align:center;font-size:14px;line-height:1.7}
.bld-empty b{display:block;font-size:17px;color:var(--ink);margin-bottom:4px}
.bld-side{width:340px;background:#fff;border-left:1px solid var(--rule);overflow-y:auto;padding:14px;flex:none}
.bld-side h3{margin:0 0 4px;font-size:15px}
.bld-side .desc{font-size:12.5px;color:var(--muted);margin:0 0 12px}
.bf{margin-bottom:12px}
.bf>label{display:block;font-size:12px;font-weight:600;color:#3A4651;margin-bottom:4px}
.bf>label i{color:var(--s-failed);font-style:normal}
.bf input:not([type=checkbox]),.bf select,.bf textarea{width:100%}
.bf textarea.bcode{font:12.5px/1.45 var(--code);white-space:pre;overflow:auto;background:#FAFBFC}
.bf .help{font-size:11.5px;color:var(--muted);margin-top:3px}
.bf .bf-err{font-size:11.5px;color:var(--s-failed);margin-top:3px}
.bf label.chk{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:500;margin:0}
.kvr{display:flex;gap:4px;margin-bottom:4px}
.kvr input{flex:1;min-width:0}
.x{border:0;background:none;color:var(--muted);font-size:17px;line-height:1;padding:0 6px;border-radius:4px}
.x:hover{background:#EEF1F4;color:var(--s-failed)}
.bld-side details{border-top:1px solid var(--line);padding-top:8px;margin-top:6px}
.bld-side details>summary{cursor:pointer;font-weight:600;font-size:13px;margin-bottom:10px}
.step{border:1px solid var(--rule);border-radius:7px;padding:8px;margin-bottom:8px;background:#FAFBFC}
.step .sh{display:flex;gap:4px;align-items:center;margin-bottom:6px}
.step .sh select{flex:1}
.step .two{display:flex;gap:4px}
.step .two>*{flex:1;min-width:0}
.step input,.step select{margin-bottom:4px;width:100%}
.iss{display:block;width:100%;text-align:left;border:1px solid var(--line);border-radius:6px;background:#fff;padding:7px 9px;margin-bottom:6px;font-size:12.5px;cursor:pointer}
.iss:hover{background:#F7F8F9}
.iss b{font-family:var(--code);font-size:12px}
.iss.e{border-left:3px solid var(--s-failed)}
.iss.w{border-left:3px solid #E0A21B}
.bld-out{background:#16202A;color:#E6EDF3;border-radius:6px;padding:8px 10px;font:11.5px/1.45 var(--code);max-height:220px;overflow:auto;white-space:pre-wrap;word-break:break-word;margin:6px 0}
.bld-banner{padding:8px 14px;background:#FFF4E0;border-bottom:1px solid #F3D3A0;font-size:13px;display:flex;gap:10px;align-items:center}
.bld-pick{position:fixed;z-index:60;background:#fff;border:1px solid var(--rule);border-radius:8px;box-shadow:0 8px 24px rgba(22,32,42,.2);padding:6px;width:230px;max-height:340px;overflow:auto}
.bld-pick .bp-item{cursor:pointer;margin-bottom:2px;border-color:transparent}
.bld-pick h5{margin:6px 6px 3px;font-size:10.5px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.bld-pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11.5px;font-weight:600;color:#fff}
.bld-list td .badge{margin-right:6px}
@media(max-width:900px){.bld-pal{width:150px}.bld-side{width:280px}}
`;

// ---- small helpers ---------------------------------------------------------------
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const clone = o => JSON.parse(JSON.stringify(o));
const snapG = v => Math.round(v / GRID) * GRID;
const isObj = v => v && typeof v === "object" && !Array.isArray(v);
const empty = v => v === undefined || v === null || v === "" || v === false || (Array.isArray(v) && !v.length) || (isObj(v) && !Object.keys(v).length);

function getPath(o, p) { return p.split(".").reduce((a, k) => (a == null ? undefined : a[k]), o); }
function setPath(o, p, v) {
  const ks = p.split("."), stack = [];
  let cur = o;
  for (let i = 0; i < ks.length - 1; i++) {
    if (!isObj(cur[ks[i]])) { if (empty(v)) return; cur[ks[i]] = {}; }
    stack.push([cur, ks[i]]); cur = cur[ks[i]];
  }
  const last = ks[ks.length - 1];
  if (empty(v)) delete cur[last]; else cur[last] = v;
  for (let i = stack.length - 1; i >= 0; i--) { const [par, k] = stack[i]; if (isObj(par[k]) && !Object.keys(par[k]).length) delete par[k]; else break; }
}

let CAT = null;          // node catalog from the server
let B = null;            // editor state
let styleEl = null;
const typeOf = t => CAT.nodes.find(n => n.type === t) || { type: t, label: t, category: "Control", icon: "dot", fields: [], description: "" };
const colorOf = t => CAT_COLOR[typeOf(t).category] || "#64748B";

async function ensureCatalog() { if (!CAT) CAT = await api("/builder/nodes"); }
function ensureStyle() {
  if (styleEl) return;
  styleEl = document.createElement("style"); styleEl.textContent = STYLE; document.head.appendChild(styleEl);
}

// ---- entry points -------------------------------------------------------------------
async function view(arg, q) {
  ensureStyle();
  document.querySelector("main")?.classList.add("bldmain");
  await ensureCatalog();
  if (!arg && !q.get("new")) return listView();
  return editorView(arg || "", !arg);
}

function leave() {
  document.querySelector("main")?.classList.remove("bldmain");
  teardown();
  window.builderGuard = null;
}

function teardown() {
  if (!B) return;
  clearTimeout(B.vt); clearTimeout(B.pt);
  document.removeEventListener("keydown", onKey);
  window.removeEventListener("resize", onResize);
  window.removeEventListener("beforeunload", onUnload);
  B = null;
}

// ---- workflow list ---------------------------------------------------------------------
async function listView() {
  teardown();
  const r = await api("/builder/workflows");
  const rows = r.workflows.map(w => `<tr>
    <td><a class="dagname" href="#/builder/${enc(w.dag_id)}">${esc(w.dag_id)}</a>${w.error ? `<div class="sub" style="color:var(--s-failed)">${esc(w.error)}</div>` : ""}</td>
    <td>${w.tasks}</td><td class="id">${esc(w.schedule || "–")}</td>
    <td>${(w.tags || []).map(t => `<span class="badge">${esc(t)}</span>`).join("")}</td>
    <td>${w.editable ? '<span class="badge">builder</span>' : '<span class="badge grey">YAML file</span>'}<span class="sub">${esc(w.file.split("/").pop())}</span></td>
    <td style="text-align:right">${w.editable ? `<a class="btn sm" href="#/builder/${enc(w.dag_id)}">Edit</a> <button class="btn sm danger" data-bld-del="${esc(w.dag_id)}">Delete</button>` : `<a class="btn sm" href="#/builder/${enc(w.dag_id)}">Open</a>`}</td></tr>`).join("");
  $app.innerHTML = `<div style="padding:20px 24px;max-width:1200px;margin:0 auto" class="bld-list">
    <div class="secHead"><h1 class="pageTitle" style="margin:0">Workflow builder</h1><span class="grow"></span><a class="btn primary" href="#/builder?new=1">+ New workflow</a></div>
    <p class="sub">Drag nodes onto a canvas and connect them. Workflows are saved as ordinary YAML DAGs in <code>${esc(r.builder_dir)}</code>, so they schedule, run, retry and show up like any other DAG.</p>
    <div class="panel"><table class="list"><thead><tr><th>Workflow</th><th>Nodes</th><th>Schedule</th><th>Tags</th><th>Source</th><th></th></tr></thead><tbody>
    ${rows || `<tr><td colspan="6" class="muted">No YAML workflows yet. <a href="#/builder?new=1">Create the first one</a>.</td></tr>`}</tbody></table></div></div>`;
  $app.onclick = async e => {
    const b = e.target.closest("[data-bld-del]"); if (!b) return;
    const id = b.dataset.bldDel;
    if (!confirm(`Delete workflow "${id}"? Its run history is kept.`)) return;
    try { await api(`/builder/workflows/${enc(id)}`, { method: "DELETE" }); toast("Deleted " + id); listView(); } catch (x) { toast(x.message); }
  };
}

// ---- editor ---------------------------------------------------------------------------------
async function editorView(id, isNew) {
  teardown();
  B = { origId: isNew ? "" : id, isNew, ro: false, etag: "", file: "", paused: false, loaded: false,
        graph: { dag_id: id, settings: {}, nodes: [], edges: [] }, sel: null, vx: 40, vy: 40, k: 1,
        undo: [], redo: [], dirty: false, lastTouch: 0, issues: [], vseq: 0, run: null, conns: [], saving: false, needsFit: true };
  if (!isNew) {
    const r = await api(`/builder/workflows/${enc(id)}`);
    B.graph = r.graph; B.graph.settings = B.graph.settings || {}; B.graph.nodes = B.graph.nodes || []; B.graph.edges = B.graph.edges || [];
    B.graph.nodes.forEach(n => { n.config = n.config || {}; }); B.etag = r.etag; B.file = r.file; B.paused = r.paused; B.loaded = r.loaded;
    B.ro = !r.editable;
    if (r.graph.needs_layout) { B.graph.nodes.length && layoutNodes(); }
  } else {
    B.graph.settings = { schedule: "" };
  }
  if (can("connections.read")) { try { B.conns = (await api("/connections")).connections?.map(c => c.conn_id) || []; } catch { /* optional */ } }
  window.builderGuard = () => !B || !B.dirty || confirm("You have unsaved changes in the builder. Leave without saving?");
  document.addEventListener("keydown", onKey);
  window.addEventListener("resize", onResize);
  window.addEventListener("beforeunload", onUnload);
  drawShell();
  maybeRestoreDraft();
  renderAll();
  fitView();
  scheduleValidate(0);
  if (!isNew && B.loaded) loadLastRun();
}

function onUnload(e) { if (B?.dirty) { e.preventDefault(); e.returnValue = ""; } }
function onResize() { applyView(); }

function drawShell() {
  const cats = [...new Set(CAT.nodes.map(n => n.category))];
  $app.innerHTML = `<div class="bld">
    <div class="bld-top">
      <a class="btn sm" href="#/builder" data-act="back">← Workflows</a>
      <input class="bid" id="bid" value="${esc(B.graph.dag_id)}" placeholder="workflow_id" spellcheck="false" title="Workflow (DAG) id. Changing it saves a copy under the new id." aria-label="Workflow id">
      <button class="btn sm" data-act="settings">Settings</button>
      <span class="st" id="bst"></span>
      <span class="sp"></span>
      <span class="st" id="brun"></span>
      <button class="btn sm" data-act="undo" title="Undo (Ctrl+Z)">↶</button><button class="btn sm" data-act="redo" title="Redo (Ctrl+Shift+Z)">↷</button>
      <button class="btn sm" data-act="layout" title="Arrange nodes left to right">Auto-arrange</button>
      <button class="btn sm" data-act="fit">Fit</button>
      <span class="sep"></span>
      <button class="btn sm" data-act="yaml">YAML</button>
      <button class="btn sm" id="bactive" data-act="active" hidden></button>
      <button class="btn" data-act="save" id="bsave">Save</button>
      <button class="btn primary" data-act="run" id="brunbtn">Save &amp; Run</button>
    </div>
    <div id="bbanner"></div><div id="bdraft"></div>
    <div class="bld-main">
      <aside class="bld-pal" aria-label="Nodes"><input id="bpsearch" placeholder="Search nodes…" aria-label="Search nodes">
        <div id="bpal">${cats.map(c => `<h4>${esc(c)}</h4>` + CAT.nodes.filter(n => n.category === c).map(n => palItem(n)).join("")).join("")}</div>
        <p class="sub" style="margin-top:14px;font-size:11.5px">Drag a node onto the canvas, or click it to add it after the selected node.</p></aside>
      <div class="bld-cv" id="bc" tabindex="0" aria-label="Workflow canvas">
        <div class="bld-world" id="bw"><svg class="bld-edges" id="be"><defs><marker id="barrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 10 5 0 10z" fill="#8A96A3"/></marker><marker id="barrowsel" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 10 5 0 10z" fill="#1F5FD1"/></marker></defs><g id="bel"></g><path class="tmp" id="btmp" hidden></path></svg></div>
        <div class="bld-empty" id="bempty"><div><b>Start building</b>Drag a node from the left, or click one to add it.</div></div>
        <div class="bld-zoom"><button data-act="zout" title="Zoom out" aria-label="Zoom out">−</button><button data-act="zin" title="Zoom in" aria-label="Zoom in">+</button></div>
      </div>
      <aside class="bld-side" id="bside" aria-label="Details"></aside>
    </div>
    <datalist id="bconns">${B.conns.map(c => `<option value="${esc(c)}">`).join("")}</datalist>
  </div>`;
  bindShell();
}

const palItem = n => `<div class="bp-item" draggable="true" data-type="${n.type}" title="${esc(n.description)}"><span class="bp-ic" style="background:${CAT_COLOR[n.category] || "#64748B"}">${icon(n.icon, 15)}</span>${esc(n.label)}</div>`;

function bindShell() {
  const root = $(".bld");
  root.addEventListener("click", onClick);
  $("#bid").addEventListener("input", e => { B.graph.dag_id = e.target.value.trim(); touch(); updateStatus(); });
  $("#bpsearch").addEventListener("input", e => {
    const t = e.target.value.toLowerCase();
    $$("#bpal .bp-item").forEach(i => { i.hidden = !(i.textContent.toLowerCase().includes(t) || typeOf(i.dataset.type).description.toLowerCase().includes(t)); });
    $$("#bpal h4").forEach(h => { let n = h.nextElementSibling, any = false; while (n && n.tagName !== "H4") { if (!n.hidden) any = true; n = n.nextElementSibling; } h.hidden = !any; });
  });
  $("#bpal").addEventListener("dragstart", e => { const it = e.target.closest(".bp-item"); if (!it || B.ro) return e.preventDefault(); e.dataTransfer.setData("text/plain", "bld:" + it.dataset.type); e.dataTransfer.effectAllowed = "copy"; });
  const cv = $("#bc");
  cv.addEventListener("dragover", e => { if (!B.ro) { e.preventDefault(); e.dataTransfer.dropEffect = "copy"; } });
  cv.addEventListener("drop", e => {
    const t = e.dataTransfer.getData("text/plain"); if (!t.startsWith("bld:") || B.ro) return;
    e.preventDefault(); const p = toWorld(e.clientX, e.clientY);
    addNode(t.slice(4), p.x - W / 2, p.y - H / 2);
  });
  cv.addEventListener("focusin", e => { const n = e.target.closest?.(".bn"); if (n && !(B.sel?.k === "node" && B.sel.id === n.dataset.id)) select({ k: "node", id: n.dataset.id }); });
  cv.addEventListener("pointerdown", onDown);
  cv.addEventListener("pointermove", onMove);
  cv.addEventListener("pointerup", onUp);
  cv.addEventListener("pointercancel", onUp);
  cv.addEventListener("wheel", e => {
    e.preventDefault();
    const f = Math.exp(-e.deltaY * (e.ctrlKey ? 0.01 : 0.0015));
    zoomAt(e.clientX, e.clientY, f);
  }, { passive: false });
  const side = $("#bside");
  side.addEventListener("input", onSideInput);
  side.addEventListener("change", onSideChange);
  side.addEventListener("click", onSideClick);
  side.addEventListener("keydown", e => {
    if (e.key === "Tab" && e.target.classList.contains("bcode") && !e.shiftKey) {
      e.preventDefault(); const t = e.target, s = t.selectionStart;
      t.value = t.value.slice(0, s) + "  " + t.value.slice(t.selectionEnd); t.selectionStart = t.selectionEnd = s + 2;
      t.dispatchEvent(new Event("input", { bubbles: true }));
    }
  });
}

// ---- view transform --------------------------------------------------------------------------------
function applyView() {
  const cv = $("#bc"), w = $("#bw"); if (!cv || !w) return;
  w.style.transform = `translate(${B.vx}px,${B.vy}px) scale(${B.k})`;
  cv.style.backgroundPosition = `${B.vx}px ${B.vy}px`;
  cv.style.backgroundSize = `${20 * B.k}px ${20 * B.k}px`;
}
function toWorld(cx, cy) { const r = $("#bc").getBoundingClientRect(); return { x: (cx - r.left - B.vx) / B.k, y: (cy - r.top - B.vy) / B.k }; }
function zoomAt(cx, cy, f) {
  const r = $("#bc").getBoundingClientRect(), nk = Math.min(2, Math.max(0.3, B.k * f)), px = cx - r.left, py = cy - r.top;
  B.vx = px - (px - B.vx) * (nk / B.k); B.vy = py - (py - B.vy) * (nk / B.k); B.k = nk; applyView();
}
function fitView() {
  const cv = $("#bc"); if (!cv) return;
  const ns = B.graph.nodes, r = cv.getBoundingClientRect();
  if (!ns.length) { B.vx = 60; B.vy = 60; B.k = 1; return applyView(); }
  const minx = Math.min(...ns.map(n => n.x)), miny = Math.min(...ns.map(n => n.y));
  const maxx = Math.max(...ns.map(n => n.x + W)), maxy = Math.max(...ns.map(n => n.y + H));
  const k = Math.max(0.3, Math.min(1, (r.width - 100) / (maxx - minx), (r.height - 100) / (maxy - miny)));
  B.k = k; B.vx = (r.width - (maxx - minx) * k) / 2 - minx * k; B.vy = (r.height - (maxy - miny) * k) / 2 - miny * k; applyView();
}

// ---- graph model ops ------------------------------------------------------------------------------------
const node = id => B.graph.nodes.find(n => n.id === id);
function state() { const g = B.graph; return JSON.stringify({ dag_id: g.dag_id, settings: g.settings, nodes: g.nodes, edges: g.edges }); }
function snap() { B.undo.push(state()); if (B.undo.length > 100) B.undo.shift(); B.redo = []; }
function restore(s) {
  const o = JSON.parse(s); Object.assign(B.graph, o);
  if (B.sel?.k === "node" && !node(B.sel.id)) B.sel = null;
  $("#bid").value = B.graph.dag_id; markDirty(); renderAll(); scheduleValidate();
}
function undo() { if (B.ro || !B.undo.length) return; B.redo.push(state()); restore(B.undo.pop()); }
function redo() { if (B.ro || !B.redo.length) return; B.undo.push(state()); restore(B.redo.pop()); }
function touch() { // a text edit: group quick keystrokes into one undo step
  if (Date.now() - B.lastTouch > 1500) snap();
  B.lastTouch = Date.now(); markDirty(); scheduleValidate();
}
function markDirty() { B.dirty = true; updateStatus(); saveDraft(); }

function uniqueId(base) {
  const ids = new Set(B.graph.nodes.map(n => n.id)); let id = base, i = 2;
  while (ids.has(id)) id = `${base}_${i++}`;
  return id;
}
function freeSpot(x, y) {
  x = snapG(x); y = snapG(y);
  while (B.graph.nodes.some(n => Math.abs(n.x - x) < W - 20 && Math.abs(n.y - y) < H + 10)) y += H + 30;
  return { x, y };
}
function addNode(type, x, y, from) {
  if (B.ro) return;
  snap();
  const id = uniqueId(type), p = freeSpot(x, y), cfg = {};
  if (type === "http" || type === "http_sensor") cfg.method = "GET";
  B.graph.nodes.push({ id, type, x: p.x, y: p.y, config: cfg });
  if (from) B.graph.edges.push({ from, to: id });
  B.sel = { k: "node", id }; markDirty(); renderAll(); scheduleValidate();
  ensureVisible(p.x, p.y);
  return id;
}
function ensureVisible(x, y) { // pan so a new node is not hidden behind the side panel or off screen
  const r = $("#bc").getBoundingClientRect(), pad = 40;
  const l = x * B.k + B.vx, t = y * B.k + B.vy, rt = l + W * B.k, bt = t + H * B.k;
  if (rt > r.width - pad) B.vx -= rt - (r.width - pad); else if (l < pad) B.vx += pad - l;
  if (bt > r.height - pad) B.vy -= bt - (r.height - pad); else if (t < pad) B.vy += pad - t;
  applyView();
}
function addFromPalette(type) {
  const s = B.sel?.k === "node" ? node(B.sel.id) : null;
  if (s) return addNode(type, s.x + W + 70, s.y, s.id);
  const r = $("#bc").getBoundingClientRect(), c = toWorld(r.left + r.width / 2, r.top + r.height / 2);
  const last = B.graph.nodes.at(-1);
  addNode(type, last ? last.x + W + 70 : c.x - W / 2, last ? last.y : c.y - H / 2, last?.id);
}
function reaches(from, target) {
  const seen = new Set(), st = [from];
  while (st.length) { const c = st.pop(); if (c === target) return true; if (seen.has(c)) continue; seen.add(c); B.graph.edges.forEach(e => e.from === c && st.push(e.to)); }
  return false;
}
function connect(from, to) {
  if (B.ro || from === to) return;
  if (B.graph.edges.some(e => e.from === from && e.to === to)) return toast("Already connected");
  if (reaches(to, from)) return toast("That would create a loop");
  snap(); B.graph.edges.push({ from, to }); markDirty(); drawEdges(); renderSide(); scheduleValidate();
}
function removeEdge(i) { if (B.ro) return; snap(); B.graph.edges.splice(i, 1); B.sel = null; markDirty(); renderAll(); scheduleValidate(); }
function removeNode(id) {
  if (B.ro) return; snap();
  B.graph.nodes = B.graph.nodes.filter(n => n.id !== id);
  B.graph.edges = B.graph.edges.filter(e => e.from !== id && e.to !== id);
  B.graph.nodes.forEach(n => { // drop references to the removed node
    if (n.config.input === id) delete n.config.input;
    if (n.config.rows_from === id) delete n.config.rows_from;
    if (n.config.expand?.xcom?.task === id) delete n.config.expand;
  });
  B.sel = null; markDirty(); renderAll(); scheduleValidate();
}
function duplicateNode(id) {
  const n = node(id); if (!n || B.ro) return; snap();
  const nid = uniqueId(n.id.replace(/_\d+$/, "")), p = freeSpot(n.x + 30, n.y + H + 30);
  B.graph.nodes.push({ id: nid, type: n.type, x: p.x, y: p.y, config: clone(n.config) });
  B.sel = { k: "node", id: nid }; markDirty(); renderAll(); scheduleValidate();
}
function renameNode(oldId, nid) {
  nid = nid.trim();
  if (!nid || nid === oldId) return true;
  if (!/^[A-Za-z0-9_][A-Za-z0-9_.-]*$/.test(nid)) { toast("Use letters, digits, _ . - only"); return false; }
  if (node(nid)) { toast("Another node already uses that id"); return false; }
  snap(); const n = node(oldId); n.id = nid;
  B.graph.edges.forEach(e => { if (e.from === oldId) e.from = nid; if (e.to === oldId) e.to = nid; });
  B.graph.nodes.forEach(m => {
    if (m.config.input === oldId) m.config.input = nid;
    if (m.config.rows_from === oldId) m.config.rows_from = nid;
    if (m.config.expand?.xcom?.task === oldId) m.config.expand.xcom.task = nid;
  });
  B.sel = { k: "node", id: nid }; B.issues.forEach(i => { if (i.node === oldId) i.node = nid; });
  markDirty(); renderAll(); scheduleValidate();
  return true;
}
function upstreamOf(id) { return B.graph.edges.filter(e => e.to === id).map(e => e.from); }

function layoutNodes() {
  const ns = B.graph.nodes, ups = {}, memo = {}, busy = {};
  ns.forEach(n => (ups[n.id] = []));
  B.graph.edges.forEach(e => ups[e.to]?.push(e.from));
  const L = id => { if (memo[id] != null) return memo[id]; if (busy[id]) return 0; busy[id] = 1; const u = ups[id] || []; return (memo[id] = u.length ? 1 + Math.max(...u.map(L)) : 0); };
  const cols = [];
  ns.forEach(n => { const l = L(n.id); (cols[l] = cols[l] || []).push(n); });
  const pos = {};
  cols.forEach((col, ci) => {
    if (!col) return;
    if (ci > 0) col.sort((a, b) => avg(ups[a.id]) - avg(ups[b.id]));
    col.forEach((n, i) => { pos[n.id] = i; });
    function avg(list) { return list.length ? list.reduce((s, u) => s + (pos[u] ?? 0), 0) / list.length : 0; }
  });
  const tallest = Math.max(...cols.map(c => (c ? c.length : 0)), 1);
  cols.forEach((col, ci) => {
    if (!col) return;
    const off = ((tallest - col.length) * (H + 40)) / 2;
    col.forEach((n, i) => { n.x = 40 + ci * (W + 90); n.y = 40 + off + i * (H + 40); });
  });
}

// ---- rendering: canvas -------------------------------------------------------------------------------------------
function renderAll() { renderNodes(); drawEdges(); renderSide(); applyView(); updateStatus(); $("#bempty").hidden = B.graph.nodes.length > 0; }

function summary(n) {
  const c = n.config || {}, first = s => String(s || "").split("\n").find(l => l.trim())?.trim() || "";
  switch (n.type) {
    case "http": case "http_sensor": return `${c.method || "GET"} ${c.url || c.endpoint || ""}`.trim();
    case "postgres_query": return first(c.sql);
    case "bigquery_query": return first(c.sql);
    case "bigquery_insert": return "→ " + (c.dataset && c.table ? c.dataset + "." + c.table : "choose a table");
    case "gsheet_read": return c.range ? c.range : "choose a range";
    case "gsheet_write": return "→ " + (c.range || "choose a tab");
    case "postgres_insert": return "→ " + (c.table || "choose a table");
    case "transform": return `${(c.steps || []).length} step${(c.steps || []).length === 1 ? "" : "s"}`;
    case "bigquery": return first(c.sql);
    case "bash": return first(c.command);
    case "trigger_dagrun": return c.trigger_dag_id || "";
    case "file_sensor": return c.path || "";
    case "time_delta_sensor": return c.delta || "";
    case "external_task_sensor": return c.external_dag_id || "";
    default: return "";
  }
}

function nodeHtml(n) {
  const t = typeOf(n.type);
  return `<span class="bn-bar" style="background:${colorOf(n.type)}"></span><span class="bn-ic" style="background:${colorOf(n.type)}">${icon(t.icon)}</span>
    <div class="bn-tx"><div class="bn-t">${esc(n.id)}</div><div class="bn-s">${esc(summary(n) || t.label)}</div></div>
    <span class="bn-port in" data-port="in"></span><span class="bn-port out" data-port="out"></span><span class="bn-st"></span><span class="bn-bd">!</span>`;
}
function renderNodes() {
  const w = $("#bw"); $$(".bn", w).forEach(e => e.remove());
  B.graph.nodes.forEach(n => {
    const el = document.createElement("div");
    el.className = "bn"; el.dataset.id = n.id; el.style.left = n.x + "px"; el.style.top = n.y + "px";
    el.tabIndex = 0; el.setAttribute("role", "button"); el.setAttribute("aria-label", `${typeOf(n.type).label} ${n.id}`);
    el.innerHTML = nodeHtml(n); w.appendChild(el);
  });
  decorate();
}
function refreshNode(id) {
  const n = node(id), el = $(`.bn[data-id="${CSS.escape(id)}"]`); if (!n || !el) return;
  el.innerHTML = nodeHtml(n); decorate();
}
function decorate() { // selection, validation badges, run state
  const issuesBy = {};
  B.issues.forEach(i => { if (i.node) (issuesBy[i.node] = issuesBy[i.node] || []).push(i); });
  $$(".bn").forEach(el => {
    const id = el.dataset.id, is = issuesBy[id] || [], errs = is.filter(i => !i.warning);
    el.classList.toggle("sel", B.sel?.k === "node" && B.sel.id === id);
    el.classList.toggle("err", errs.length > 0); el.classList.toggle("warn", is.length > 0);
    el.title = is.map(i => i.message).join("\n");
    const bd = $(".bn-bd", el); if (bd) bd.textContent = is.length > 1 ? is.length : "!";
    const st = B.run?.tis?.[id]?.state, dot = $(".bn-st", el);
    el.classList.toggle("hasst", !!st && st !== "none");
    if (dot) dot.style.background = st ? `var(--s-${st})` : "";
  });
}

function portPos(id, out) { const n = node(id); return n ? { x: n.x + (out ? W : 0), y: n.y + H / 2 } : { x: 0, y: 0 }; }
function curve(a, b) { const dx = Math.max(40, Math.abs(b.x - a.x) / 2); return `M${a.x} ${a.y} C${a.x + dx} ${a.y}, ${b.x - dx} ${b.y}, ${b.x} ${b.y}`; }
function drawEdges() {
  const g = $("#bel"); if (!g) return;
  g.innerHTML = B.graph.edges.map((e, i) => {
    const a = portPos(e.from, true), b = portPos(e.to, false), sel = B.sel?.k === "edge" && B.sel.id === i, d = curve(a, b);
    const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
    return `<g class="${sel ? "sel" : ""}"><path class="hit" data-edge="${i}" d="${d}"/><path class="ln" d="${d}" marker-end="url(#${sel ? "barrowsel" : "barrow"})"/>
      ${sel && !B.ro ? `<g class="edel" data-edel="${i}"><circle cx="${mx}" cy="${my}" r="10" fill="#fff" stroke="#D23B3B" stroke-width="1.5"/><path d="M${mx - 4} ${my - 4}l8 8M${mx + 4} ${my - 4}l-8 8" stroke="#D23B3B" stroke-width="1.8" stroke-linecap="round"/></g>` : ""}</g>`;
  }).join("");
}

// ---- pointer interaction ---------------------------------------------------------------------------------------------------
let D = null; // active drag
function select(s) { B.sel = s; decorate(); drawEdges(); renderSide(); }

function onDown(e) {
  if (e.pointerType === "mouse" && e.button !== 0) return;
  if (e.target.closest(".bld-zoom")) return;
  const cv = $("#bc");
  const del = e.target.closest("[data-edel]");
  if (del) { removeEdge(+del.dataset.edel); return; }
  const nodeEl = e.target.closest(".bn"), port = e.target.closest(".bn-port.out"), hit = e.target.closest("[data-edge]");
  cv.setPointerCapture(e.pointerId);
  if (port && nodeEl && !B.ro) {
    D = { t: "conn", from: nodeEl.dataset.id }; cv.classList.add("connecting"); e.preventDefault(); return;
  }
  if (nodeEl) {
    const n = node(nodeEl.dataset.id);
    if (!(B.sel?.k === "node" && B.sel.id === n.id)) select({ k: "node", id: n.id });
    D = { t: "node", id: n.id, sx: e.clientX, sy: e.clientY, ox: n.x, oy: n.y, moved: false }; return;
  }
  if (hit) { select({ k: "edge", id: +hit.dataset.edge }); D = null; return; }
  if (B.sel) select(null);
  D = { t: "pan", sx: e.clientX, sy: e.clientY, ox: B.vx, oy: B.vy }; cv.classList.add("panning");
}
function onMove(e) {
  if (!D) return;
  if (D.t === "pan") { B.vx = D.ox + e.clientX - D.sx; B.vy = D.oy + e.clientY - D.sy; applyView(); return; }
  if (D.t === "node") {
    const dx = (e.clientX - D.sx) / B.k, dy = (e.clientY - D.sy) / B.k;
    if (!D.moved && Math.hypot(dx, dy) < 3) return;
    if (!D.moved) { D.moved = true; if (!B.ro) snap(); }
    if (B.ro) return;
    const n = node(D.id); n.x = snapG(D.ox + dx); n.y = snapG(D.oy + dy);
    const el = $(`.bn[data-id="${CSS.escape(D.id)}"]`); el.style.left = n.x + "px"; el.style.top = n.y + "px"; drawEdges(); return;
  }
  if (D.t === "conn") {
    const p = toWorld(e.clientX, e.clientY), a = portPos(D.from, true), t = $("#btmp");
    t.hidden = false; t.setAttribute("d", curve(a, p));
  }
}
function onUp(e) {
  const cv = $("#bc"); if (!D || !cv) return;
  cv.classList.remove("panning", "connecting");
  const d = D; D = null;
  if (d.t === "node" && d.moved && !B.ro) markDirty();
  if (d.t === "conn") {
    $("#btmp").hidden = true;
    const under = document.elementFromPoint(e.clientX, e.clientY)?.closest(".bn");
    if (under) connect(d.from, under.dataset.id);
    else if (e.type === "pointerup") showPick(e.clientX, e.clientY, type => {
      const p = toWorld(e.clientX, e.clientY); addNode(type, p.x + 10, p.y - H / 2, d.from);
    });
  }
}

function showPick(cx, cy, onPick) {
  closePick();
  const el = document.createElement("div"); el.className = "bld-pick"; el.id = "bpick";
  const cats = [...new Set(CAT.nodes.map(n => n.category))];
  el.innerHTML = cats.map(c => `<h5>${esc(c)}</h5>` + CAT.nodes.filter(n => n.category === c).map(palItem).join("")).join("");
  el.style.left = Math.min(cx, innerWidth - 250) + "px"; el.style.top = Math.min(cy, innerHeight - 350) + "px";
  $$(".bp-item", el).forEach(i => { i.draggable = false; });
  el.addEventListener("click", e => { const it = e.target.closest(".bp-item"); if (!it) return; closePick(); onPick(it.dataset.type); });
  document.body.appendChild(el);
  setTimeout(() => document.addEventListener("pointerdown", pickOutside, true), 0);
}
function pickOutside(e) { if (!e.target.closest("#bpick")) closePick(); }
function closePick() { $("#bpick")?.remove(); document.removeEventListener("pointerdown", pickOutside, true); }

function onKey(e) {
  if (!B || document.querySelector("dialog[open]")) return;
  const typing = e.target.closest?.("input,textarea,select,[contenteditable]");
  const mod = e.ctrlKey || e.metaKey;
  if (mod && e.key.toLowerCase() === "s") { e.preventDefault(); save(); return; }
  if (typing) return;
  if (mod && e.key.toLowerCase() === "z") { e.preventDefault(); e.shiftKey ? redo() : undo(); return; }
  if (mod && e.key.toLowerCase() === "y") { e.preventDefault(); redo(); return; }
  if (mod && e.key.toLowerCase() === "d" && B.sel?.k === "node") { e.preventDefault(); duplicateNode(B.sel.id); return; }
  if ((e.key === "Delete" || e.key === "Backspace") && B.sel) {
    e.preventDefault(); B.sel.k === "node" ? removeNode(B.sel.id) : removeEdge(B.sel.id);
  }
  if (e.key === "Escape") { closePick(); select(null); }
}

function onClick(e) {
  const pi = e.target.closest("#bpal .bp-item");
  if (pi) { if (!B.ro) addFromPalette(pi.dataset.type); return; }
  const a = e.target.closest("[data-act]"); if (!a) return;
  const act = a.dataset.act;
  if (act === "back") return;
  e.preventDefault?.();
  ({ settings: () => select(null), undo, redo, layout: () => { if (B.ro) return; snap(); layoutNodes(); markDirty(); renderAll(); fitView(); },
     fit: fitView, zin: () => { const r = $("#bc").getBoundingClientRect(); zoomAt(r.left + r.width / 2, r.top + r.height / 2, 1.2); },
     zout: () => { const r = $("#bc").getBoundingClientRect(); zoomAt(r.left + r.width / 2, r.top + r.height / 2, 1 / 1.2); },
     save: () => save(), run: () => saveAndRun(), yaml: showYaml, active: toggleActive, copy: makeCopy }[act] || (() => {}))();
}

function makeCopy() {
  B.ro = false; B.isNew = true; B.etag = ""; B.origId = ""; B.loaded = false;
  B.graph.dag_id = B.graph.dag_id + "_copy"; $("#bid").value = B.graph.dag_id; B.run = null;
  history.replaceState(null, "", "#/builder?new=1"); markDirty(); updateStatus(); renderAll(); scheduleValidate();
}

// ---- status bar / banner ------------------------------------------------------------------------------------------------------
function updateStatus() {
  const st = $("#bst"); if (!st) return;
  const errs = B.issues.filter(i => !i.warning).length, warns = B.issues.length - errs;
  let t = B.saving ? "Saving…" : B.dirty ? "Unsaved changes" : (B.isNew ? "Not saved yet" : "All changes saved");
  if (errs) t += ` · <button class="lnk" data-act="settings" style="color:var(--s-failed)">${errs} problem${errs > 1 ? "s" : ""}</button>`;
  else if (warns) t += ` · <button class="lnk" data-act="settings">${warns} warning${warns > 1 ? "s" : ""}</button>`;
  st.innerHTML = t; st.classList.toggle("bad", errs > 0);
  $("#bsave").disabled = B.ro || B.saving; $("#brunbtn").disabled = B.ro || B.saving || !B.graph.nodes.length;
  const ban = $("#bbanner");
  ban.innerHTML = B.ro ? `<div class="bld-banner"><span>This workflow lives in <code>${esc((B.file || "").split("/").pop())}</code>, outside the builder folder (or you may not edit it), so it is shown read-only.</span><button class="btn sm primary" data-act="copy">Make an editable copy</button></div>` : "";
  const ab = $("#bactive"), hasSch = !!(B.graph.settings.schedule || B.graph.settings.schedule_datasets?.length);
  ab.hidden = B.isNew || !B.loaded || !hasSch;
  ab.textContent = B.paused ? "Paused · turn on" : "Active · pause";
  ab.classList.toggle("primary", B.paused);
  const rb = $("#brun");
  rb.innerHTML = B.run ? `Run <a href="#/dags/${enc(B.graph.dag_id)}?tab=grid&run=${enc(B.run.id)}" title="Open in Grid">${esc(B.run.state || "queued")}</a>` : "";
}

// ---- side panel ----------------------------------------------------------------------------------------------------------------------
function renderSide() { const s = $("#bside"); if (!s) return; s.innerHTML = B.sel?.k === "node" ? nodePanel(node(B.sel.id)) : B.sel?.k === "edge" ? edgePanel() : dagPanel(); }

function edgePanel() {
  const e = B.graph.edges[B.sel.id]; if (!e) return "";
  return `<h3>Connection</h3><p class="desc"><b>${esc(e.from)}</b> runs first, then <b>${esc(e.to)}</b>. The second node can read the first one's output.</p>
    ${B.ro ? "" : `<button class="btn danger" data-sact="deledge">Remove connection</button>`}`;
}

function fieldHtml(f, obj, scope, id) {
  const v = getPath(obj, f.name), a = `data-scope="${scope}" data-f="${esc(f.name)}" data-kind="${f.kind}"`;
  const req = f.required ? " <i>*</i>" : "";
  let inp = "";
  switch (f.kind) {
    case "text": case "conn":
      inp = `<input ${a} value="${esc(v ?? "")}" placeholder="${esc(f.placeholder || "")}" ${f.kind === "conn" ? 'list="bconns"' : ""} autocomplete="off" spellcheck="false">`; break;
    case "number": inp = `<input ${a} type="number" value="${v ?? ""}" placeholder="${esc(f.placeholder || "")}">`; break;
    case "bool": return `<div class="bf"><label class="chk"><input ${a} type="checkbox" ${v ? "checked" : ""}> ${esc(f.label)}</label>${f.help ? `<div class="help">${esc(f.help)}</div>` : ""}</div>`;
    case "select": inp = `<select ${a}>${f.options.map(o => `<option value="${esc(o)}" ${(v ?? "") === o ? "selected" : ""}>${o === "" ? "(default)" : esc(o)}</option>`).join("")}</select>`; break;
    case "code": inp = `<textarea ${a} class="bcode" rows="${f.lang === "json" ? 5 : 9}" spellcheck="false">${esc(v ?? "")}</textarea>`; break;
    case "list": inp = `<textarea ${a} rows="2" spellcheck="false" placeholder="one per line">${esc((v || []).join("\n"))}</textarea>`; break;
    case "intlist": inp = `<input ${a} value="${esc((v || []).join(", "))}" placeholder="200, 201">`; break;
    case "json": inp = `<textarea ${a} class="bcode" rows="4" spellcheck="false" placeholder="{ }">${v == null ? "" : esc(JSON.stringify(v, null, 2))}</textarea>`; break;
    case "kv": inp = `<div class="kv" ${a}>${Object.entries(v || {}).map(([k, x]) => kvRow(k, x)).join("")}<button class="btn sm" data-sact="kvadd" type="button">+ Add</button></div>`; break;
    case "steps": inp = stepsHtml(v || []); break;
    case "upstream": {
      const ups = upstreamOf(id);
      inp = `<select ${a}><option value="">${ups.length === 1 ? `(automatic: ${esc(ups[0])})` : "(choose)"}</option>${ups.map(u => `<option value="${esc(u)}" ${v === u ? "selected" : ""}>${esc(u)}</option>`).join("")}</select>`; break;
    }
    case "expand": inp = expandHtml(obj, id); break;
  }
  return `<div class="bf"><label>${esc(f.label)}${req}</label>${inp}${f.help ? `<div class="help">${esc(f.help)}</div>` : ""}<div class="bf-err" data-err="${esc(f.name)}"></div></div>`;
}
const kvRow = (k, x) => `<div class="kvr"><input data-kvk value="${esc(k)}" placeholder="name" spellcheck="false"><input data-kvv value="${esc(typeof x === "string" ? x : JSON.stringify(x))}" placeholder="value" spellcheck="false"><button class="x" data-sact="kvdel" type="button" title="Remove" aria-label="Remove">×</button></div>`;

function stepsHtml(steps) {
  return `<div class="steps" data-scope="cfg" data-f="steps" data-kind="steps">${steps.map((s, i) => stepHtml(s, i, steps.length)).join("")}
    <div class="two" style="display:flex;gap:4px"><select data-newstep>${STEP_OPS.map(([v, l]) => `<option value="${v}">${l}</option>`).join("")}</select><button class="btn sm" data-sact="stepadd" type="button">+ Add step</button></div></div>`;
}
function stepHtml(s, i, n) {
  const f = (k, ph, v = s[k]) => `<input data-si="${i}" data-sf="${k}" value="${esc(v ?? "")}" placeholder="${esc(ph)}" spellcheck="false">`;
  let body = "";
  switch (s.op) {
    case "extract": body = f("path", "data.items  (dot path into the JSON)"); break;
    case "select": case "drop": body = f("fields", "id, name, total", (s.fields || []).join(", ")); break;
    case "dedupe": body = f("fields", "key fields (empty = whole row)", (s.fields || []).join(", ")); break;
    case "rename": body = Object.entries(s.map || {}).map(([k, v], j) => `<div class="two"><input data-si="${i}" data-sf="rk" data-j="${j}" value="${esc(k)}" placeholder="old name"><input data-si="${i}" data-sf="rv" data-j="${j}" value="${esc(v)}" placeholder="new name"></div>`).join("") + `<button class="btn sm" type="button" data-sact="renameadd" data-si="${i}">+ Rename</button>`; break;
    case "filter": body = f("field", "field") + `<div class="two"><select data-si="${i}" data-sf="cmp">${CMPS.map(c => `<option ${s.cmp === c ? "selected" : ""}>${c}</option>`).join("")}</select>${["exists", "not_exists"].includes(s.cmp) ? "" : f("value", "value (templates ok)", s.value)}</div>`; break;
    case "set": body = f("field", "field") + f("value", "value, e.g. {{ .ds }} or {{ .row.name }}", s.value); break;
    case "sort": body = f("field", "field") + `<label class="chk"><input type="checkbox" data-si="${i}" data-sf="desc" ${s.desc ? "checked" : ""}> Descending</label>`; break;
    case "limit": body = `<input type="number" data-si="${i}" data-sf="n" value="${s.n ?? ""}" placeholder="max rows">`; break;
  }
  return `<div class="step"><div class="sh"><select data-si="${i}" data-sf="op">${STEP_OPS.map(([v, l]) => `<option value="${v}" ${s.op === v ? "selected" : ""}>${l}</option>`).join("")}</select>
    <button class="x" type="button" data-sact="stepup" data-si="${i}" ${i === 0 ? "disabled" : ""} title="Move up" aria-label="Move up">↑</button><button class="x" type="button" data-sact="stepdown" data-si="${i}" ${i === n - 1 ? "disabled" : ""} title="Move down" aria-label="Move down">↓</button><button class="x" type="button" data-sact="stepdel" data-si="${i}" title="Remove" aria-label="Remove step">×</button></div>${body}</div>`;
}
function expandHtml(cfg, id) {
  const x = cfg.expand, mode = x?.xcom ? "xcom" : x?.items ? "items" : "";
  const all = B.graph.nodes.map(n => n.id).filter(n => n !== id);
  return `<div class="expand" data-scope="cfg" data-f="expand" data-kind="expand"><select data-xf="mode"><option value="">No (run once)</option><option value="xcom" ${mode === "xcom" ? "selected" : ""}>Once per item of another node's output</option><option value="items" ${mode === "items" ? "selected" : ""}>Once per item of a fixed list</option></select>
    ${mode === "xcom" ? `<div class="two" style="display:flex;gap:4px;margin-top:4px"><select data-xf="task"><option value="">(choose node)</option>${all.map(u => `<option ${x.xcom.task === u ? "selected" : ""}>${esc(u)}</option>`).join("")}</select><input data-xf="key" value="${esc(x.xcom.key || "")}" placeholder="return_value"></div>
    <div class="help">That node must output a JSON array. Each item is available as <code>{{ .item }}</code>.</div>` : ""}
    ${mode === "items" ? `<textarea data-xf="items" class="bcode" rows="3" style="margin-top:4px" placeholder='["a", "b"]'>${esc(JSON.stringify(x.items))}</textarea>` : ""}</div>`;
}

function dagPanel() {
  const s = B.graph.settings, sch = s.schedule || "", known = SCHEDULES.some(([v]) => v === sch);
  const errs = B.issues.filter(i => !i.warning), warns = B.issues.filter(i => i.warning);
  const issueHtml = (list, cls) => list.map(i => `<button class="iss ${cls}" data-sact="gotoissue" data-node="${esc(i.node || "")}">${i.node ? `<b>${esc(i.node)}</b> ` : ""}${esc(i.message)}</button>`).join("");
  const F = (name, label, kind, extra = {}) => fieldHtml({ name, label, kind, ...extra }, s, "set");
  return `<fieldset ${B.ro ? "disabled" : ""} style="border:0;padding:0;margin:0"><h3>Workflow settings</h3><p class="desc">Applies to the whole workflow.</p>
    ${errs.length || warns.length ? `<div class="bf"><label>Problems</label>${issueHtml(errs, "e")}${issueHtml(warns, "w")}</div>` : ""}
    <div class="bf"><label>Schedule</label><select data-sched>${SCHEDULES.map(([v, l]) => `<option value="${esc(v)}" ${v === sch ? "selected" : ""}>${l}${v ? ` (${v})` : ""}</option>`).join("")}<option value="__custom" ${known ? "" : "selected"}>Custom cron…</option></select>
      <input data-scope="set" data-f="schedule" data-kind="text" value="${esc(sch)}" placeholder="*/10 * * * *" style="margin-top:4px;${known ? "display:none" : ""}" id="bcron" spellcheck="false">
      <div class="help">Cron expression or @daily-style preset. New scheduled workflows start paused.</div></div>
    ${F("description", "Description", "text")}${F("tags", "Tags", "list", { help: "One per line" })}
    ${F("start_date", "Start date", "text", { placeholder: "2026-01-31", help: "YYYY-MM-DD. Empty = now." })}
    ${F("catchup", "Run missed intervals (catch up)", "bool")}${F("max_active_runs", "Max concurrent runs", "number")}
    ${F("schedule_datasets", "Run when these datasets update", "list", { help: "Dataset URIs. Instead of a time schedule." })}
    ${F("params", "Parameters", "kv", { help: "Available as {{ .params.name }}" })}
    <details><summary>Defaults for every node</summary>${F("default_args.retries", "Retries", "number")}${F("default_args.retry_delay", "Retry delay", "text", { placeholder: "5m" })}${F("default_args.timeout", "Timeout", "text", { placeholder: "30m" })}${F("default_args.owner", "Owner", "text")}${F("default_args.on_failure", "Alert on failure", "list", { help: "Connection ids of type google_chat" })}</details>
    ${B.run ? runSummaryHtml() : ""}</fieldset>`;
}

function runSummaryHtml() {
  const r = B.run, tis = Object.entries(r.tis || {});
  return `<details open><summary>Last run</summary><p class="desc">${esc(r.state || "queued")} · <a href="#/dags/${enc(B.graph.dag_id)}?tab=grid&run=${enc(r.id)}">open in Grid</a></p>
    ${tis.map(([id, ti]) => `<div style="font-size:12.5px;margin-bottom:3px"><span class="bld-pill" style="background:var(--s-${ti.state || "none"});${ti.state === "none" || !ti.state ? "color:#333;border:1px solid #ccc" : ""}">${esc(ti.state || "none")}</span> ${esc(id)}</div>`).join("")}</details>`;
}

function nodePanel(n) {
  if (!n) return "";
  const t = typeOf(n.type);
  const ro = B.ro ? "disabled" : "";
  const iss = B.issues.filter(i => i.node === n.id);
  return `<fieldset ${ro} style="border:0;padding:0;margin:0"><div style="display:flex;align-items:center;gap:10px;margin-bottom:6px"><span class="bp-ic" style="background:${colorOf(n.type)};width:32px;height:32px">${icon(t.icon)}</span><div><h3 style="margin:0">${esc(t.label)}</h3><div class="sub" style="font-size:12px">${esc(t.category)}</div></div></div>
    <p class="desc">${esc(t.description)}</p>
    ${iss.map(i => `<div class="iss ${i.warning ? "w" : "e"}" style="cursor:default">${esc(i.message)}</div>`).join("")}
    <div class="bf"><label>Node id</label><input data-sact="rename" id="bnid" value="${esc(n.id)}" spellcheck="false"><div class="help">Reference its output with <code>{{ xcom_pull "${esc(n.id)}" "return_value" }}</code></div></div>
    ${(t.fields || []).map(f => fieldHtml(f, n.config, "cfg", n.id)).join("")}
    <details><summary>Settings: retries, timeout, alerts…</summary>${CAT.common.map(f => fieldHtml(f, n.config, "cfg", n.id)).join("")}</details>
    <div style="display:flex;gap:6px;margin:14px 0"><button class="btn sm" data-sact="dup" type="button">Duplicate</button><button class="btn sm danger" data-sact="delnode" type="button">Delete</button></div></fieldset>
    ${nodeRunHtml(n)}`;
}

function nodeRunHtml(n) {
  const ti = B.run?.tis?.[n.id]; if (!ti) return "";
  const x = B.run.xcom?.[n.id] || {};
  const rv = x.return_value;
  let out = rv === undefined ? "" : typeof rv === "string" ? rv : JSON.stringify(rv, null, 2);
  const rows = Array.isArray(rv) ? rv.length : null;
  if (out.length > 6000) out = out.slice(0, 6000) + "\n… (truncated)";
  const mapped = B.run.mapped?.[n.id];
  return `<details open><summary>Last run: ${esc(ti.state || "none")}</summary>
    <div class="sub" style="font-size:12.5px">${ti.start_date ? fmt(ti.start_date) : ""}${ti.end_date ? ` · ${dur(ti.start_date, ti.end_date)}` : ""}${mapped ? ` · ${mapped.length} instances` : ""}</div>
    ${ti.error ? `<div class="iss e" style="cursor:default;margin-top:6px">${esc(ti.error)}</div>` : ""}
    ${out ? `<div class="sub" style="margin-top:8px;font-size:12px">Output${rows != null ? ` · ${rows} row${rows === 1 ? "" : "s"}` : ""}</div><pre class="bld-out">${esc(out)}</pre>` : ""}
    ${ti.try_number ? `<button class="btn sm" type="button" data-sact="showlog" data-try="${ti.try_number}">Show log</button><pre class="bld-out" id="blog" hidden></pre>` : ""}</details>`;
}

// ---- side panel events -----------------------------------------------------------------------------------------------------------------
function target(scope) { return scope === "set" ? B.graph.settings : node(B.sel.id)?.config; }

function onSideInput(e) {
  if (B.ro) return;
  const el = e.target;
  if (el.dataset.sact === "rename") return;
  if (el.matches("[data-sched]")) return;
  const kv = el.closest(".kv"); if (kv && (el.hasAttribute("data-kvk") || el.hasAttribute("data-kvv"))) return applyKv(kv);
  if (el.dataset.sf !== undefined && el.dataset.si !== undefined) return applyStep(el);
  if (el.dataset.xf) return;
  const f = el.dataset.f; if (!f || !el.dataset.scope) return;
  const kind = el.dataset.kind, obj = target(el.dataset.scope); if (!obj) return;
  let v;
  const err = $(`[data-err="${CSS.escape(f)}"]`, $("#bside")); if (err) err.textContent = "";
  switch (kind) {
    case "text": case "conn": case "code": case "select": v = el.value; break;
    case "number": v = el.value === "" ? undefined : Number(el.value); break;
    case "bool": v = el.checked || undefined; break;
    case "list": v = el.value.split("\n").map(s => s.trim()).filter(Boolean); break;
    case "intlist": v = el.value.split(/[,\s]+/).filter(Boolean).map(Number).filter(n => !isNaN(n)); break;
    case "json":
      if (!el.value.trim()) { v = undefined; break; }
      try { v = JSON.parse(el.value); } catch (x) { if (err) err.textContent = "Not valid JSON yet: " + x.message; return; }
      break;
    default: return;
  }
  touch(); setPath(obj, f, v);
  if (el.dataset.scope === "cfg") refreshNode(B.sel.id); else updateStatus();
}
function onSideChange(e) {
  if (B.ro) return;
  const el = e.target;
  if (el.dataset.sact === "rename") { if (!renameNode(B.sel.id, el.value)) el.value = B.sel.id; return; }
  if (el.matches("[data-sched]")) {
    touch(); const c = $("#bcron");
    if (el.value === "__custom") { c.style.display = ""; c.focus(); return; }
    c.style.display = "none"; setPath(B.graph.settings, "schedule", el.value || undefined); c.value = el.value; updateStatus(); return;
  }
  if (el.dataset.xf) return applyExpand(el);
  if (el.dataset.f === "schedule" || el.dataset.f?.startsWith("schedule")) updateStatus();
}

function applyKv(kv) {
  const o = {}, strOnly = ["env", "headers"].includes(kv.dataset.f);
  $$(".kvr", kv).forEach(r => {
    const k = $("[data-kvk]", r).value.trim(); if (!k) return;
    let v = $("[data-kvv]", r).value;
    if (!strOnly && /^(-?\d+(\.\d+)?|true|false)$/.test(v)) v = JSON.parse(v);
    o[k] = v;
  });
  touch(); setPath(target(kv.dataset.scope), kv.dataset.f, o);
  if (kv.dataset.scope === "cfg") refreshNode(B.sel.id);
}

function steps() { const n = node(B.sel.id); return (n.config.steps = n.config.steps || []); }
function applyStep(el, structural) {
  const i = +el.dataset.si, s = steps()[i], f = el.dataset.sf; if (!s) return;
  touch();
  switch (f) {
    case "op": { const keep = {}; steps()[i] = { op: el.value, ...keep }; if (el.value === "filter") steps()[i].cmp = "=="; if (el.value === "limit") steps()[i].n = 100; renderSide(); break; }
    case "fields": s.fields = el.value.split(",").map(x => x.trim()).filter(Boolean); if (!s.fields.length) delete s.fields; break;
    case "desc": s.desc = el.checked || undefined; if (!s.desc) delete s.desc; break;
    case "n": s.n = el.value === "" ? 0 : Number(el.value); break;
    case "cmp": s.cmp = el.value; renderSide(); break;
    case "rk": case "rv": {
      const rows = $$(`[data-sf="rk"][data-si="${i}"]`).map(a => [a.value.trim(), $(`[data-sf="rv"][data-si="${i}"][data-j="${a.dataset.j}"]`).value.trim()]);
      s.map = Object.fromEntries(rows.filter(([a]) => a)); break;
    }
    default: if (el.value === "") delete s[f]; else s[f] = el.value;
  }
  refreshNode(B.sel.id);
}
function applyExpand(el) {
  const n = node(B.sel.id), cfg = n.config, f = el.dataset.xf; touch();
  if (f === "mode") {
    if (!el.value) delete cfg.expand;
    else if (el.value === "xcom") { cfg.expand = { xcom: { task: upstreamOf(n.id)[0] || "" } }; }
    else cfg.expand = { items: [] };
    renderSide(); return;
  }
  if (f === "task") {
    cfg.expand.xcom.task = el.value;
    if (el.value && !B.graph.edges.some(e => e.from === el.value && e.to === n.id) && !reaches(n.id, el.value)) { B.graph.edges.push({ from: el.value, to: n.id }); drawEdges(); }
  }
  if (f === "key") { cfg.expand.xcom.key = el.value; if (!el.value) delete cfg.expand.xcom.key; }
  if (f === "items") { try { cfg.expand.items = JSON.parse(el.value); el.style.borderColor = ""; } catch { el.style.borderColor = "var(--s-failed)"; } }
}

async function onSideClick(e) {
  const b = e.target.closest("[data-sact]"); if (!b || B.ro && b.dataset.sact !== "gotoissue" && b.dataset.sact !== "showlog") return;
  const act = b.dataset.sact;
  switch (act) {
    case "kvadd": { const kv = b.closest(".kv"); b.insertAdjacentHTML("beforebegin", kvRow("", "")); $$("[data-kvk]", kv).at(-1).focus(); break; }
    case "kvdel": { const kv = b.closest(".kv"); b.closest(".kvr").remove(); applyKv(kv); break; }
    case "stepadd": { const op = $("[data-newstep]").value; snap(); const s = { op }; if (op === "filter") s.cmp = "=="; if (op === "limit") s.n = 100; if (op === "rename") s.map = { "": "" }; steps().push(s); markDirty(); renderSide(); refreshNode(B.sel.id); scheduleValidate(); break; }
    case "stepdel": snap(); steps().splice(+b.dataset.si, 1); if (!steps().length) delete node(B.sel.id).config.steps; markDirty(); renderSide(); refreshNode(B.sel.id); scheduleValidate(); break;
    case "stepup": case "stepdown": { const i = +b.dataset.si, j = act === "stepup" ? i - 1 : i + 1, st = steps(); if (j < 0 || j >= st.length) break; snap(); [st[i], st[j]] = [st[j], st[i]]; markDirty(); renderSide(); scheduleValidate(); break; }
    case "renameadd": { const s = steps()[+b.dataset.si]; s.map = { ...(s.map || {}), "": "" }; renderSide(); break; }
    case "dup": duplicateNode(B.sel.id); break;
    case "delnode": removeNode(B.sel.id); break;
    case "deledge": removeEdge(B.sel.id); break;
    case "gotoissue": if (b.dataset.node && node(b.dataset.node)) { select({ k: "node", id: b.dataset.node }); const n = node(b.dataset.node); const r = $("#bc").getBoundingClientRect(); B.vx = r.width / 2 - (n.x + W / 2) * B.k; B.vy = r.height / 2 - (n.y + H / 2) * B.k; applyView(); } break;
    case "showlog": {
      const pre = $("#blog"); if (!pre) break; pre.hidden = false; pre.textContent = "Loading…";
      try { pre.textContent = await api(`/dags/${enc(B.graph.dag_id)}/dagRuns/${enc(B.run.id)}/taskInstances/${enc(B.sel.id)}/logs/${b.dataset.try}`); } catch (x) { pre.textContent = x.message; }
      break;
    }
  }
}

// ---- validation ------------------------------------------------------------------------------------------------------------------------------------
function payload() { const g = B.graph; return { dag_id: g.dag_id, settings: g.settings, nodes: g.nodes, edges: g.edges }; }
function scheduleValidate(ms = 600) { clearTimeout(B.vt); B.vt = setTimeout(validate, ms); }
async function validate() {
  const seq = ++B.vseq;
  try {
    const r = await api("/builder/validate", { method: "POST", body: JSON.stringify(payload()) });
    if (!B || seq !== B.vseq) return;
    B.issues = r.issues || []; B.yaml = r.yaml;
    decorate(); updateStatus(); refreshSideSoft();
  } catch (x) { /* offline: ignore */ }
}
function refreshSideSoft() { // re-render the side panel unless the user is typing in it
  const side = $("#bside"); if (!side || (side.contains(document.activeElement) && document.activeElement.matches("input,textarea,select"))) return;
  renderSide();
}

async function showYaml() {
  let y = B.yaml;
  try { const r = await api("/builder/validate", { method: "POST", body: JSON.stringify(payload()) }); B.issues = r.issues || []; y = r.yaml; } catch { /* keep */ }
  const body = y ? `<pre class="bld-out" style="max-height:60vh">${esc(y)}</pre>` : `<div class="banner err">Fix the problems first; the YAML is generated once the workflow is valid.</div>${B.issues.filter(i => !i.warning).map(i => `<div class="iss e" style="cursor:default">${i.node ? `<b>${esc(i.node)}</b> ` : ""}${esc(i.message)}</div>`).join("")}`;
  openActDlg({ title: "Generated YAML", body, confirm: "Close", wide: true, onConfirm: async () => {} });
  decorate(); updateStatus();
}

// ---- local draft (survives an accidental reload) -------------------------------------------------------------------------------------------
const draftKey = () => "dagsflow.bld.draft." + (B.origId || "new");
function saveDraft() {
  clearTimeout(B.dt);
  B.dt = setTimeout(() => { try { if (B?.dirty) localStorage.setItem(draftKey(), JSON.stringify({ etag: B.etag, at: Date.now(), g: payload() })); } catch { /* storage unavailable */ } }, 800);
}
function clearDraft() { try { localStorage.removeItem(draftKey()); } catch { /* ignore */ } }
function maybeRestoreDraft() {
  if (B.ro) return;
  let d; try { d = JSON.parse(localStorage.getItem(draftKey()) || "null"); } catch { d = null; }
  if (!d || d.etag !== B.etag || !d.g?.nodes) return;
  if (state() === JSON.stringify(d.g)) return;
  const ban = $("#bdraft");
  ban.innerHTML = `<div class="bld-banner"><span>Unsaved changes from ${new Date(d.at).toLocaleString()} were found in this browser.</span><button class="btn sm primary" id="bdrest">Restore</button><button class="btn sm" id="bddisc">Discard</button></div>`;
  $("#bdrest").onclick = () => { Object.assign(B.graph, d.g); $("#bid").value = B.graph.dag_id; B.dirty = true; ban.innerHTML = ""; renderAll(); fitView(); scheduleValidate(); };
  $("#bddisc").onclick = () => { clearDraft(); ban.innerHTML = ""; };
}

// ---- save / run ---------------------------------------------------------------------------------------------------------------------------------------------
async function save() {
  if (B.ro || B.saving) return false;
  const id = B.graph.dag_id.trim();
  if (!id) { toast("Give the workflow an id first"); $("#bid").focus(); return false; }
  const sameId = !B.isNew && id === B.origId;
  B.saving = true; updateStatus();
  try {
    const r = await api(`/builder/workflows/${enc(id)}`, { method: "PUT", body: JSON.stringify({ ...payload(), etag: sameId ? B.etag : "" }) });
    clearDraft();
    B.etag = r.etag; B.origId = id; B.isNew = false; B.file = r.file; B.paused = r.paused; B.loaded = r.loaded; B.dirty = false;
    history.replaceState(null, "", "#/builder/" + enc(id));
    toast(r.created && r.paused ? "Created. The schedule is paused: turn it on when ready" : "Saved");
    return true;
  } catch (x) {
    toast(x.message); if (/Reload it|already defined/.test(x.message)) { /* conflict */ }
    validate(); return false;
  } finally { B.saving = false; if (B) updateStatus(); }
}

async function saveAndRun() {
  if (B.ro) return;
  if (B.dirty || B.isNew) { if (!(await save())) return; }
  const id = B.graph.dag_id, st = $("#bst");
  if (B.paused) {
    if (!confirm("This workflow is paused, so a manual run would only wait in the queue.\n\nTurn it on and run now?")) return;
    try { await api(`/dags/${enc(id)}`, { method: "PATCH", body: JSON.stringify({ is_paused: false }) }); B.paused = false; } catch (x) { return toast(x.message); }
  }
  st.textContent = "Deploying to the scheduler…";
  for (let i = 0; i < 20; i++) { try { await api(`/dags/${enc(id)}`); break; } catch { await new Promise(r => setTimeout(r, 500)); } }
  await new Promise(r => setTimeout(r, 2500)); // the scheduler reloads changed files every ~2s
  try {
    const run = await api(`/dags/${enc(id)}/dagRuns`, { method: "POST", body: JSON.stringify({}) });
    B.run = { id: run.dag_run_id, state: run.state, tis: {}, xcom: {} }; B.loaded = true;
    toast("Run started"); decorate(); updateStatus(); pollRun();
  } catch (x) { toast(x.message); updateStatus(); }
}

async function loadLastRun() {
  try {
    const r = await api(`/dags/${enc(B.graph.dag_id)}/dagRuns?limit=1`);
    const last = r.dag_runs?.[0]; if (!last) return;
    B.run = { id: last.dag_run_id, state: last.state, tis: {}, xcom: {} }; pollRun();
  } catch { /* no runs or no access */ }
}

async function pollRun() {
  clearTimeout(B.pt); if (!B?.run) return;
  const id = B.graph.dag_id, rid = B.run.id;
  try {
    const run = await api(`/dags/${enc(id)}/dagRuns/${enc(rid)}`);
    if (!B || B.run?.id !== rid) return;
    B.run = { id: rid, state: run.state, tis: run.task_instances || {}, xcom: run.xcom || {}, mapped: run.mapped_task_instances || {} };
    decorate(); updateStatus();
    refreshSideSoft();
    if (["running", "queued", ""].includes(run.state || "")) B.pt = setTimeout(pollRun, 1500);
  } catch (x) { if (B) B.pt = setTimeout(pollRun, 4000); }
}

async function toggleActive() {
  try {
    await api(`/dags/${enc(B.graph.dag_id)}`, { method: "PATCH", body: JSON.stringify({ is_paused: !B.paused }) });
    B.paused = !B.paused; updateStatus(); toast(B.paused ? "Paused" : "Schedule is on");
  } catch (x) { toast(x.message); }
}

window.Builder = { view, leave };
})();
