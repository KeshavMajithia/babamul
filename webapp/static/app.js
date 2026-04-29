/* ================================================================
   BOOM Filter UI — Application Logic
   ================================================================ */

(function () {
  "use strict";

  // ---- State ----
  const state = {
    schemaCache: {},
  };

  // ---- DOM refs ----
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // ---- Helpers ----

  function toast(message, type = "info") {
    const container = $("#toastContainer");
    const el = document.createElement("div");
    el.className = `toast ${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => {
      el.style.opacity = "0";
      el.style.transform = "translateX(20px)";
      el.style.transition = "all 0.3s ease";
      setTimeout(() => el.remove(), 300);
    }, 4000);
  }

  async function api(method, path, body = null) {
    const opts = {
      method,
      headers: { "Content-Type": "application/json" },
    };
    if (body) opts.body = JSON.stringify(body);

    const resp = await fetch(`/api${path}`, opts);
    if (!resp.ok) {
      let msg;
      try {
        const data = await resp.json();
        msg = data.detail || data.message || resp.statusText;
      } catch {
        msg = await resp.text();
      }
      throw new Error(msg);
    }
    return resp.json();
  }

  function setStatus(connected) {
    const dot = $("#statusDot");
    const text = $("#statusText");
    dot.className = `status-dot ${connected ? "connected" : "error"}`;
    text.textContent = connected ? "Connected" : "Disconnected";
  }

  // ---- Tabs ----

  function initTabs() {
    $$(".tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        $$(".tab").forEach((t) => {
          t.classList.remove("active");
          t.setAttribute("aria-selected", "false");
        });
        $$(".tab-panel").forEach((p) => p.classList.remove("active"));

        tab.classList.add("active");
        tab.setAttribute("aria-selected", "true");
        const panel = $(`#panel-${tab.dataset.tab}`);
        if (panel) panel.classList.add("active");

        // Lazy-load data
        if (tab.dataset.tab === "schema") loadSchema();
        if (tab.dataset.tab === "saved") loadFilters();
      });
    });
  }

  // ---- Pipeline helpers ----

  function getPipeline() {
    const raw = $("#pipelineEditor").value.trim();
    if (!raw) throw new Error("Pipeline is empty");
    try {
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) throw new Error("Pipeline must be a JSON array");
      return parsed;
    } catch (e) {
      throw new Error(`Invalid JSON: ${e.message}`);
    }
  }

  function getTestParams() {
    const survey = $("#surveySelect").value;
    const startJd = $("#startJd").value ? parseFloat($("#startJd").value) : null;
    const endJd = $("#endJd").value ? parseFloat($("#endJd").value) : null;
    const limit = parseInt($("#resultLimit").value) || 25;
    const objectIdsRaw = $("#objectIds").value.trim();
    const objectIds = objectIdsRaw
      ? objectIdsRaw.split(",").map((s) => s.trim()).filter(Boolean)
      : null;

    // ZTF public = programid 1
    const permissions = survey === "ZTF" ? { ZTF: [1] } : {};

    const body = {
      pipeline: getPipeline(),
      survey,
      permissions,
    };

    if (startJd !== null) body.start_jd = startJd;
    if (endJd !== null) body.end_jd = endJd;
    if (objectIds) body.object_ids = objectIds;
    if (limit) body.limit = limit;

    return body;
  }

  // ---- Filter Tester ----

  function showLoading() {
    $("#resultsContent").innerHTML = '<div class="spinner"></div>';
    $("#resultCount").textContent = "";
  }

  async function runCount() {
    try {
      showLoading();
      const params = getTestParams();
      delete params.limit;
      const result = await api("POST", "/filters/test/count", params);
      const count = result.data?.count ?? result.count ?? 0;

      $("#resultsContent").innerHTML = `
        <div class="count-result">
          <div class="count-number">${count.toLocaleString()}</div>
          <div class="count-label">alerts match this filter</div>
        </div>
      `;
      $("#resultCount").textContent = `${count.toLocaleString()} matches`;
    } catch (e) {
      toast(e.message, "error");
      $("#resultsContent").innerHTML = `<div class="empty-state"><p>${e.message}</p></div>`;
    }
  }

  async function runFilter() {
    try {
      showLoading();
      const params = getTestParams();
      params.sort_by = "candidate.jd";
      params.sort_order = "descending";

      const result = await api("POST", "/filters/test", params);
      const alerts = result.data?.results ?? result.results ?? [];

      if (alerts.length === 0) {
        $("#resultsContent").innerHTML = `
          <div class="empty-state">
            <p>No alerts matched this filter.</p>
            <p class="hint">Try adjusting your pipeline or broadening the JD range.</p>
          </div>
        `;
        $("#resultCount").textContent = "0 results";
        return;
      }

      // Extract display columns from first result
      const columns = extractColumns(alerts[0]);

      let html = '<table class="results-table"><thead><tr>';
      columns.forEach((col) => {
        html += `<th>${col.label}</th>`;
      });
      html += "</tr></thead><tbody>";

      alerts.forEach((alert) => {
        html += "<tr>";
        columns.forEach((col) => {
          const val = getNestedValue(alert, col.path);
          html += `<td>${formatValue(val)}</td>`;
        });
        html += "</tr>";
      });

      html += "</tbody></table>";
      $("#resultsContent").innerHTML = html;
      $("#resultCount").textContent = `${alerts.length} results`;
    } catch (e) {
      toast(e.message, "error");
      $("#resultsContent").innerHTML = `<div class="empty-state"><p>${e.message}</p></div>`;
    }
  }

  function extractColumns(alert) {
    const cols = [];

    // Always show key fields if present
    if ("objectId" in alert) cols.push({ label: "Object ID", path: "objectId" });
    if (alert.candidate) {
      if ("ra" in alert.candidate) cols.push({ label: "RA", path: "candidate.ra" });
      if ("dec" in alert.candidate) cols.push({ label: "Dec", path: "candidate.dec" });
      if ("magpsf" in alert.candidate) cols.push({ label: "Mag (PSF)", path: "candidate.magpsf" });
      if ("sigmapsf" in alert.candidate) cols.push({ label: "σ Mag", path: "candidate.sigmapsf" });
      if ("fid" in alert.candidate) cols.push({ label: "Filter", path: "candidate.fid" });
      if ("drb" in alert.candidate) cols.push({ label: "DRB", path: "candidate.drb" });
      if ("jd" in alert.candidate) cols.push({ label: "JD", path: "candidate.jd" });
    }

    // If pipeline projected custom fields, show those too
    if (cols.length === 0) {
      Object.keys(alert).forEach((key) => {
        if (key !== "_id") {
          cols.push({ label: key, path: key });
        }
      });
    }

    return cols;
  }

  function getNestedValue(obj, path) {
    return path.split(".").reduce((o, k) => (o && o[k] !== undefined ? o[k] : null), obj);
  }

  function formatValue(val) {
    if (val === null || val === undefined) return '<span style="color:var(--text-muted)">—</span>';
    if (typeof val === "number") {
      if (Number.isInteger(val)) return val.toLocaleString();
      return val.toFixed(6);
    }
    if (typeof val === "object") return JSON.stringify(val);
    return String(val);
  }

  // ---- Schema Explorer ----

  async function loadSchema() {
    const survey = $("#schemaSurvey").value;
    const tree = $("#schemaTree");

    if (state.schemaCache[survey]) {
      renderSchema(state.schemaCache[survey], tree);
      return;
    }

    tree.innerHTML = '<div class="spinner"></div>';

    try {
      const result = await api("GET", `/schemas/${survey}`);
      const schema = result.data || result;
      state.schemaCache[survey] = schema;
      renderSchema(schema, tree);
      setStatus(true);
    } catch (e) {
      tree.innerHTML = `<div class="empty-state"><p>Failed to load schema: ${e.message}</p></div>`;
      setStatus(false);
    }
  }

  function renderSchema(schema, container) {
    container.innerHTML = "";
    const node = document.createElement("div");
    node.className = "schema-node";

    if (schema.fields) {
      schema.fields.forEach((field) => {
        renderSchemaField(field, node, "");
      });
    } else {
      // Flat schema fallback
      node.innerHTML = `<pre style="padding:20px;font-size:12px;color:var(--text-secondary);white-space:pre-wrap">${JSON.stringify(schema, null, 2)}</pre>`;
    }

    container.appendChild(node);
  }

  function renderSchemaField(field, parent, prefix) {
    const fullPath = prefix ? `${prefix}.${field.name}` : field.name;
    const fieldType = resolveAvroType(field.type);

    // If it's a record (has sub-fields), render as group
    if (fieldType.fields) {
      const group = document.createElement("div");
      group.className = "schema-group";

      const header = document.createElement("div");
      header.className = "schema-group-header";
      header.innerHTML = `<span class="toggle">▼</span> <span class="field-name" style="font-family:var(--font-mono)">${field.name}</span> <span class="field-type">record</span>`;

      const children = document.createElement("div");
      children.className = "schema-group-children";

      header.addEventListener("click", () => {
        header.classList.toggle("collapsed");
        children.classList.toggle("collapsed");
      });

      fieldType.fields.forEach((subField) => {
        renderSchemaField(subField, children, fullPath);
      });

      group.appendChild(header);
      group.appendChild(children);
      parent.appendChild(group);
    } else if (fieldType.items) {
      // Array type
      const group = document.createElement("div");
      group.className = "schema-group";

      const itemType = resolveAvroType(fieldType.items);
      const header = document.createElement("div");
      header.className = "schema-group-header collapsed";
      header.innerHTML = `<span class="toggle">▼</span> <span class="field-name" style="font-family:var(--font-mono)">${field.name}</span> <span class="field-type">array</span>`;

      const children = document.createElement("div");
      children.className = "schema-group-children collapsed";

      header.addEventListener("click", () => {
        header.classList.toggle("collapsed");
        children.classList.toggle("collapsed");
      });

      if (itemType.fields) {
        itemType.fields.forEach((subField) => {
          renderSchemaField(subField, children, `${fullPath}[]`);
        });
      }

      group.appendChild(header);
      group.appendChild(children);
      parent.appendChild(group);
    } else {
      // Leaf field
      const el = document.createElement("div");
      el.className = "schema-field";
      el.innerHTML = `
        <span class="field-name">${field.name}</span>
        <span class="field-type">${typeof fieldType === "string" ? fieldType : JSON.stringify(fieldType)}</span>
        <span class="field-path">${fullPath}</span>
      `;
      el.addEventListener("click", () => {
        navigator.clipboard.writeText(fullPath).then(() => {
          toast(`Copied: ${fullPath}`, "info");
          el.classList.add("field-copied");
          setTimeout(() => el.classList.remove("field-copied"), 600);
        });
      });
      parent.appendChild(el);
    }
  }

  function resolveAvroType(type) {
    if (typeof type === "string") return type;
    if (Array.isArray(type)) {
      // Union type — find the non-null type
      const nonNull = type.find((t) => t !== "null");
      return resolveAvroType(nonNull || type[0]);
    }
    if (type && typeof type === "object") return type;
    return String(type);
  }

  // Schema search
  function initSchemaSearch() {
    const input = $("#schemaSearch");
    input.addEventListener("input", () => {
      const query = input.value.toLowerCase().trim();
      const fields = $$("#schemaTree .schema-field");
      const groups = $$("#schemaTree .schema-group");

      if (!query) {
        fields.forEach((f) => (f.style.display = ""));
        groups.forEach((g) => (g.style.display = ""));
        return;
      }

      fields.forEach((f) => {
        const name = f.querySelector(".field-name")?.textContent.toLowerCase() || "";
        const path = f.querySelector(".field-path")?.textContent.toLowerCase() || "";
        f.style.display = name.includes(query) || path.includes(query) ? "" : "none";
      });

      // Show groups that have visible children
      groups.forEach((g) => {
        const children = g.querySelector(".schema-group-children");
        if (!children) return;
        const visibleKids = children.querySelectorAll('.schema-field:not([style*="display: none"])');
        g.style.display = visibleKids.length > 0 ? "" : "none";
        // Expand matching groups
        if (visibleKids.length > 0) {
          g.querySelector(".schema-group-header")?.classList.remove("collapsed");
          children.classList.remove("collapsed");
        }
      });
    });
  }

  // ---- Saved Filters ----

  async function loadFilters() {
    const list = $("#filtersList");
    list.innerHTML = '<div class="spinner"></div>';

    try {
      const result = await api("GET", "/filters");
      const filters = result.data || [];

      if (filters.length === 0) {
        list.innerHTML = '<div class="empty-state"><p>No saved filters yet.</p></div>';
        return;
      }

      list.innerHTML = "";
      filters.forEach((f) => {
        const card = document.createElement("div");
        card.className = "filter-card";
        card.innerHTML = `
          <div class="filter-info">
            <div class="filter-name">${escapeHtml(f.name)}</div>
            <div class="filter-meta">
              <span>${f.survey}</span>
              <span>${f.fv?.length || 0} version(s)</span>
              <span class="filter-badge ${f.active ? "active" : "inactive"}">${f.active ? "Active" : "Inactive"}</span>
            </div>
          </div>
          <div class="filter-actions">
            <button class="filter-btn" data-action="load" data-id="${f.id}">Load</button>
            <button class="filter-btn" data-action="toggle" data-id="${f.id}" data-active="${f.active}">${f.active ? "Disable" : "Enable"}</button>
          </div>
        `;
        list.appendChild(card);
      });

      // Event delegation for filter actions
      list.addEventListener("click", async (e) => {
        const btn = e.target.closest("[data-action]");
        if (!btn) return;

        const action = btn.dataset.action;
        const id = btn.dataset.id;

        if (action === "load") {
          try {
            const result = await api("GET", `/filters/${id}`);
            const filter = result.data || result;
            // Load into editor
            const activeFv = filter.fv?.find((v) => v.fid === filter.active_fid);
            if (activeFv) {
              const pipeline = typeof activeFv.pipeline === "string"
                ? JSON.parse(activeFv.pipeline)
                : activeFv.pipeline;
              $("#pipelineEditor").value = JSON.stringify(pipeline, null, 2);
              $("#surveySelect").value = filter.survey;
              // Switch to tester tab
              $('[data-tab="tester"]').click();
              toast(`Loaded filter: ${filter.name}`, "success");
            }
          } catch (err) {
            toast(`Failed to load filter: ${err.message}`, "error");
          }
        }

        if (action === "toggle") {
          const isActive = btn.dataset.active === "true";
          try {
            await api("PATCH", `/filters/${id}`, { active: !isActive });
            toast(`Filter ${isActive ? "disabled" : "enabled"}`, "success");
            loadFilters();
          } catch (err) {
            toast(`Failed to update: ${err.message}`, "error");
          }
        }
      });

      setStatus(true);
    } catch (e) {
      list.innerHTML = `<div class="empty-state"><p>Failed to load filters: ${e.message}</p></div>`;
      setStatus(false);
    }
  }

  // ---- Save Modal ----

  function openSaveModal() {
    try {
      getPipeline(); // Validate pipeline first
      $("#saveModal").classList.add("open");
      $("#filterName").focus();
    } catch (e) {
      toast(e.message, "error");
    }
  }

  function closeSaveModal() {
    $("#saveModal").classList.remove("open");
    $("#filterName").value = "";
    $("#filterDesc").value = "";
  }

  async function saveFilter() {
    const name = $("#filterName").value.trim();
    if (!name) {
      toast("Filter name is required", "error");
      return;
    }

    const survey = $("#surveySelect").value;
    const permissions = survey === "ZTF" ? { ZTF: [1] } : {};

    try {
      const body = {
        name,
        pipeline: getPipeline(),
        survey,
        permissions,
      };
      const desc = $("#filterDesc").value.trim();
      if (desc) body.description = desc;

      await api("POST", "/filters", body);
      toast(`Filter "${name}" saved`, "success");
      closeSaveModal();
    } catch (e) {
      toast(`Save failed: ${e.message}`, "error");
    }
  }

  function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  // ---- Init ----

  function init() {
    initTabs();
    initSchemaSearch();

    // Buttons
    $("#btnCount").addEventListener("click", runCount);
    $("#btnRun").addEventListener("click", runFilter);
    $("#btnSave").addEventListener("click", openSaveModal);
    $("#btnRefreshFilters").addEventListener("click", loadFilters);

    // Modal
    $("#modalClose").addEventListener("click", closeSaveModal);
    $("#modalCancel").addEventListener("click", closeSaveModal);
    $("#modalSave").addEventListener("click", saveFilter);
    $("#saveModal").addEventListener("click", (e) => {
      if (e.target === $("#saveModal")) closeSaveModal();
    });

    // Schema survey change
    $("#schemaSurvey").addEventListener("change", () => {
      loadSchema();
    });

    // Tab key in editor inserts spaces
    $("#pipelineEditor").addEventListener("keydown", (e) => {
      if (e.key === "Tab") {
        e.preventDefault();
        const ta = e.target;
        const start = ta.selectionStart;
        ta.value = ta.value.substring(0, start) + "  " + ta.value.substring(ta.selectionEnd);
        ta.selectionStart = ta.selectionEnd = start + 2;
      }
    });

    // Connectivity check
    api("GET", "/schemas/ZTF")
      .then(() => setStatus(true))
      .catch(() => setStatus(false));
  }

  document.addEventListener("DOMContentLoaded", init);
})();
