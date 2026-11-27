// mini_fkdb 控制台前端（原生 JS，零依赖）
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

let SCHEMA = null;

async function api(path, opts) {
  const res = await fetch(path, opts);
  const data = await res.json();
  if (!res.ok) throw data;
  return data;
}

const post = (path, body) => api(path, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

// ------------------------------------------------------------- Tab 切换
$$(".tab").forEach((btn) => btn.addEventListener("click", () => {
  $$(".tab").forEach((b) => b.classList.remove("active"));
  $$(".panel").forEach((p) => p.classList.remove("active"));
  btn.classList.add("active");
  $("#tab-" + btn.dataset.tab).classList.add("active");
}));

// ------------------------------------------------------------- 初始化
async function init() {
  SCHEMA = await api("/api/schema");
  const tables = SCHEMA.tables.map((t) => t.name);
  for (const id of ["#impact-table", "#browse-table", "#batch-table"]) {
    const sel = $(id);
    sel.innerHTML = tables.map((t) => `<option>${t}</option>`).join("");
  }
  $("#impact-table").value = "customers";
  $("#browse-table").value = "customers";
  $("#batch-table").value = "customers";
  await refreshIntegrity();
}

async function refreshIntegrity() {
  try {
    const r = await api("/api/integrity");
    const el = $("#integrity");
    if (r.ok) {
      el.className = "pill pill-ok";
      el.textContent = "✓ 外键完整性正常（0 悬空引用）";
    } else {
      el.className = "pill pill-bad";
      el.textContent = `✗ 发现 ${r.count} 条悬空外键`;
    }
  } catch (e) {
    $("#integrity").textContent = "完整性检查失败";
  }
}

// ------------------------------------------------------------- 动作徽章
function badge(action) {
  return `<span class="badge badge-${action}">${action}</span>`;
}

// ------------------------------------------------------------- 影响树渲染
function renderTree(node) {
  const kids = (node.children || []).map(renderTree).join("");
  const chain = node.chain && node.chain.length
    ? `<span class="chain">← ${node.chain.map((h) => h.fk).join(" → ")}</span>`
    : '<span class="chain">根行</span>';
  const cols = node.set_cols && Object.keys(node.set_cols).length
    ? `<span class="chain">列: ${Object.entries(node.set_cols)
        .map(([k, v]) => `${k}=${v === null ? "NULL" : v}`).join(", ")}</span>`
    : "";
  return `<li>
    <span class="node">
      <span class="t">${node.table}</span><span class="i">#${node.rowid}</span>
      ${badge(node.action)} ${chain} ${cols}
    </span>
    ${kids ? `<ul>${kids}</ul>` : ""}
  </li>`;
}

function renderCounts(counts) {
  const rows = Object.entries(counts).sort((a, b) => a[0].localeCompare(b[0]));
  let html = `<thead><tr><th>表</th><th>删除</th><th>置空</th>
    <th>置默认</th><th>阻止</th></tr></thead><tbody>`;
  for (const [t, c] of rows) {
    html += `<tr><td>${t}</td><td>${c.delete || 0}</td>
      <td>${c.set_null || 0}</td><td>${c.set_default || 0}</td>
      <td>${c.blocked || 0}</td></tr>`;
  }
  return html + "</tbody>";
}

function summaryCards(totals, extra) {
  const cards = [
    ["delete", "删除行", totals.delete || 0],
    ["null", "置空列", totals.set_null || 0],
    ["default", "置默认列", totals.set_default || 0],
    ["block", "阻止", totals.blocked || 0],
  ];
  if (extra) cards.push(["depth", "最大级联深度", extra]);
  return cards.map(([cls, lab, n]) =>
    `<div class="card ${cls}"><div class="num">${n}</div>
     <div class="lab">${lab}</div></div>`).join("");
}

async function analyzeImpact() {
  const t = $("#impact-table").value;
  const id = parseInt($("#impact-id").value, 10);
  $("#impact-blocked").classList.add("hidden");
  try {
    const r = await post("/api/estimate", { roots: [[t, id]] });
    $("#impact-summary").innerHTML = summaryCards(r.totals, r.max_depth);
    $("#impact-tree").innerHTML =
      `<ul>${r.impact_tree.tree.map(renderTree).join("")}</ul>`;
    $("#impact-counts").innerHTML = renderCounts(r.counts);
    window.__lastRoots = [[t, id]];
  } catch (e) {
    if (e.blocked) {
      $("#impact-summary").innerHTML =
        summaryCards({ delete: 0, set_null: 0, set_default: 0, blocked: e.blocks.length });
      $("#impact-tree").innerHTML = "";
      $("#impact-counts").innerHTML = "";
      const box = $("#impact-blocked");
      box.classList.remove("hidden");
      box.innerHTML = `<h4>删除被外键约束阻止（${e.blocks.length} 处），数据库不会有任何改动</h4>` +
        e.blocks.slice(0, 20).map((b) =>
          `<div class="mono">• ${b.action}: ${b.table}#${b.rowid} 经外键
           ${b.fk} 引用 ${b.parent_table}#${b.parent_rowid}
           （链: ${(b.chain || []).join(" → ")}）</div>`).join("");
    } else {
      alert(e.error || JSON.stringify(e));
    }
    window.__lastRoots = null;
  }
}

$("#btn-impact").addEventListener("click", analyzeImpact);

// ------------------------------------------------------------- 执行删除
async function executeDelete(mode) {
  const roots = window.__lastRoots;
  if (!roots) { alert("请先分析一行的级联影响"); return; }
  const label = mode === "batched" ? "拆批删除会分多个短事务执行" : "删除不可恢复";
  if (!confirm(`确认对 ${roots.length} 个根行执行${mode === "batched" ? "拆批" : "一次性"}级联删除？\n${label}`))
    return;
  try {
    const r = await post("/api/delete", {
      roots, mode, batch_size: parseInt($("#batch-size").value, 10) || 25,
    });
    const m = r.manifest;
    $("#delete-result").innerHTML =
      `✓ 完成，耗时 ${m.wall_ms}ms，删除 ${m.counts &&
       Object.values(m.counts).reduce((s, c) => s + (c.delete || 0), 0)} 行，` +
      `置空 ${m.set_null.length} 列，置默认 ${m.set_default.length} 列，` +
      (m.batch_count ? `共 ${m.batch_count} 批，` : "") +
      (r.integrity ? "完整性正常" : "⚠ 存在悬空外键");
    await refreshIntegrity();
    SCHEMA = await api("/api/schema");
    await analyzeImpact();
  } catch (e) {
    alert("删除被阻止或失败：\n" + (e.message || e.error || JSON.stringify(e)));
  }
}
$("#btn-delete-one").addEventListener("click", () => executeDelete("one_shot"));
$("#btn-delete-batch").addEventListener("click", () => executeDelete("batched"));

// ------------------------------------------------------------- 表浏览
async function browse() {
  const t = $("#browse-table").value;
  const r = await api(`/api/tables/${encodeURIComponent(t)}?limit=200`);
  $("#browse-meta").textContent = `共 ${r.total} 行，显示前 ${r.rows.length} 行（点击 id 分析该行级联影响）`;
  if (!r.rows.length) {
    $("#browse-data").innerHTML = "<tbody><tr><td>空表</td></tr></tbody>";
    return;
  }
  const cols = Object.keys(r.rows[0]);
  let html = "<thead><tr>" + cols.map((c) => `<th>${c}</th>`).join("") + "</tr></thead><tbody>";
  for (const row of r.rows) {
    html += "<tr>" + cols.map((c) => {
      if (c === SCHEMA.tables.find((x) => x.name === t).pk) {
        return `<td><span class="rowlink" data-table="${t}" data-id="${row[c]}">${row[c]}</span></td>`;
      }
      const v = row[c];
      return `<td>${v === null ? '<span class="hint">NULL</span>' :
        String(v).replace(/</g, "&lt;")}</td>`;
    }).join("") + "</tr>";
  }
  $("#browse-data").innerHTML = html + "</tbody>";
  $$("#browse-data .rowlink").forEach((el) =>
    el.addEventListener("click", () => {
      $$(".tab").forEach((b) => b.classList.remove("active"));
      $$(".panel").forEach((p) => p.classList.remove("active"));
      $('.tab[data-tab="impact"]').classList.add("active");
      $("#tab-impact").classList.add("active");
      $("#impact-table").value = el.dataset.table;
      $("#impact-id").value = el.dataset.id;
      analyzeImpact();
    }));
}
$("#btn-browse").addEventListener("click", browse);

// ------------------------------------------------------------- 批量预估
$("#btn-batch-estimate").addEventListener("click", async () => {
  const t = $("#batch-table").value;
  const ids = $("#batch-ids").value.split(",").map((s) => parseInt(s.trim(), 10))
    .filter((n) => Number.isInteger(n));
  $("#batch-blocked").classList.add("hidden");
  try {
    const r = await post("/api/estimate", { roots: ids.map((i) => [t, i]) });
    $("#batch-summary").innerHTML = summaryCards(r.totals, r.max_depth);
    $("#batch-counts").innerHTML = renderCounts(r.counts);
  } catch (e) {
    if (e.blocked) {
      $("#batch-summary").innerHTML = summaryCards(
        { delete: 0, set_null: 0, set_default: 0, blocked: e.blocks.length });
      $("#batch-counts").innerHTML = "";
      const box = $("#batch-blocked");
      box.classList.remove("hidden");
      box.innerHTML = `<h4>批量删除被阻止（${e.blocks.length} 处），首条：</h4>
        <div class="mono">${JSON.stringify(e.blocks[0], null, 2)}</div>`;
    } else alert(e.error || JSON.stringify(e));
  }
});

// ------------------------------------------------------------- 实验报告
function statBlock(name, s) {
  if (!s || s.total === undefined) return "";
  return `<div>
    <h4>${name}</h4>
    <div class="kv">
      <b>业务操作总数</b><span>${s.total}（失败 ${s.failed}）</span>
      <b>锁等待 p50 / p95</b><span>${s.wait_ms.p50} ms / ${s.wait_ms.p95} ms</span>
      <b>锁等待最大 / 总和</b><span>${s.wait_ms.max} ms / ${s.wait_ms.sum} ms</span>
      <b>操作延迟 p50 / p95</b><span>${s.latency_ms.p50} ms / ${s.latency_ms.p95} ms</span>
      <b>观测到悬空引用</b><span class="${s.dangling_observed ? "fail" : "pass"}">${s.dangling_observed}</span>
    </div>
    <h4>分操作类型</h4>
    <table class="data"><thead><tr><th>操作</th><th>数量</th>
      <th>等待 p95(ms)</th><th>延迟 p95(ms)</th></tr></thead><tbody>
    ${Object.entries(s.by_kind || {}).map(([k, v]) =>
      `<tr><td>${k}</td><td>${v.count}</td>
       <td>${v.wait_ms.p95}</td><td>${v.latency_ms.p95}</td></tr>`).join("")}
    </tbody></table>
  </div>`;
}

$("#btn-load-report").addEventListener("click", async () => {
  const r = await api("/api/report");
  if (!r.available) {
    $("#report-body").innerHTML = '<p class="hint">还没有实验报告，请先运行 python3 -I run.py exp</p>';
    return;
  }
  const s3 = r.stage3 || {};
  const one = s3.one_shot || {};
  const bat = s3.batched || {};
  let html = `<h3>库规模</h3><div class="kv">`;
  for (const [k, v] of Object.entries(r.table_counts || {}))
    html += `<b>${k}</b><span>${v} 行</span>`;
  html += "</div>";

  html += `<h3>阶段1：逐客户预估 vs 实际</h3>
    <table class="data"><thead><tr><th>客户</th><th>级联深度</th>
    <th>预估/实际一致</th><th>置空列</th><th>耗时(ms)</th></tr></thead><tbody>` +
    (r.stage1_per_customer || []).map((c) =>
      `<tr><td>${c.customer_id}</td><td>${c.depth}</td>
       <td class="${c.delete_match ? "pass" : "fail"}">${c.delete_match ? "✓" : "✗ " + JSON.stringify(c.mismatch)}</td>
       <td>${c.set_null_total}</td><td>${c.elapsed_ms}</td></tr>`).join("") +
    "</tbody></table>";

  html += `<h3>阶段2：RESTRICT 拦截（拦截后零改动：<span class="${r.stage2_zero_change ? "pass" : "fail"}">${r.stage2_zero_change}</span>）</h3>` +
    (r.stage2_blocks || []).map((b) =>
      `<div class="mono">• ${b.target.join("#")} 被 ${b.first_block.fk} 阻止</div>`).join("");

  html += `<h3>阶段3：一次性 vs 拆批（并发对比）</h3>
    <div class="compare">
      <div><h4>一次性大事务</h4>
        <p>删除耗时 <b>${one.delete_elapsed_ms} ms</b></p>
        ${statBlock("业务负载", one.workload)}
        <p>悬空外键：<span class="${one.final_dangling_count ? "fail" : "pass"}">${one.final_dangling_count}</span></p>
      </div>
      <div><h4>拆批短事务（${bat.batch_count} 批，每批提交点零悬空）</h4>
        <p>删除耗时 <b>${bat.delete_elapsed_ms} ms</b></p>
        ${statBlock("业务负载", bat.workload)}
        <p>悬空外键：<span class="${bat.final_dangling_count ? "fail" : "pass"}">${bat.final_dangling_count}</span></p>
        <p>最终行集合与一次性一致：<span class="${s3.snapshot_equal ? "pass" : "fail"}">${s3.snapshot_equal}</span></p>
      </div>
    </div>`;

  html += `<h3>阶段4：外键链抽样（可追溯传播路径）</h3>` +
    (r.stage4_chain_samples || []).map((s) =>
      `<div class="mono">• ${s.table}#${s.rowid} ${badge(s.action)} via ${s.via_fk}
       链: ${s.chain.map((h) => h.fk).join(" → ")}（备选路径 ${s.alt_paths}）</div>`).join("");

  html += `<h3>最终结论</h3><div class="kv">` +
    Object.entries(r.verdict || {}).map(([k, v]) =>
      `<b>${k}</b><span class="${v ? "pass" : "fail"}">${v ? "PASS" : "FAIL"}</span>`).join("") +
    "</div>";
  $("#report-body").innerHTML = html;
});

init().catch((e) => alert("初始化失败: " + JSON.stringify(e)));
browse();
