const TIER_ORDER = ["automation", "workflow", "agent"];
const TIER_LABELS = {
  automation: "Rule-Based Automations",
  workflow: "AI Workflows",
  agent: "AI Agents",
};

const STEP_ICON = {
  pending: "⚪",
  running: "🟡",
  done: "🟢",
  failed: "🔴",
};

const THEME_STORAGE_KEY = "aiboss-theme";
const COLLAPSE_STORAGE_KEY = "aiboss-collapsed-sections";
const FAVORITES_STORAGE_KEY = "aiboss-favorites";

const sectionsEl = document.getElementById("sections");
const pipelineResultEl = document.getElementById("pipeline-result");
const runPipelineBtn = document.getElementById("run-pipeline-btn");
const toastContainer = document.getElementById("toast-container");

const themeToggleBtn = document.getElementById("theme-toggle");
const searchOmnibar = document.getElementById("search-omnibar");

const healthBeacon = document.getElementById("health-beacon");
const healthLabel = document.getElementById("health-label");
const healthPanel = document.getElementById("health-panel");

const notificationsBtn = document.getElementById("notifications-btn");
const notifBadge = document.getElementById("notif-badge");
const notificationsPanel = document.getElementById("notifications-panel");

const metricTotalEl = document.getElementById("metric-total");
const metricSuccessEl = document.getElementById("metric-success");
const metricDurationEl = document.getElementById("metric-duration");

const favoritesSection = document.getElementById("favorites-section");
const favoritesGrid = document.getElementById("favorites-grid");
const recentRunsTableEl = document.getElementById("recent-runs-table");

const builderToggleBtn = document.getElementById("builder-toggle");
const builderPanelEl = document.getElementById("builder-panel");
const builderNameEl = document.getElementById("builder-name");
const builderDescriptionEl = document.getElementById("builder-description");
const builderStepsEl = document.getElementById("builder-steps");
const builderAddStepBtn = document.getElementById("builder-add-step");
const builderLaunchBtn = document.getElementById("builder-launch");
const builderErrorEl = document.getElementById("builder-error");
const builderTrackerEl = document.getElementById("builder-tracker");
const builderResultEl = document.getElementById("builder-result");

const savedPipelinesSection = document.getElementById("saved-pipelines-section");
const savedPipelinesGrid = document.getElementById("saved-pipelines-grid");

let currentModulesByTier = {};
let currentPipelines = [];
let builderSteps = [];
let toastHistory = [];
let unreadNotifications = 0;

// ---- Field controls (shared by module cards and the pipeline builder) ----

function fieldId(tier, name, fieldName) {
  return `field__${tier}__${name}__${fieldName}`;
}

function renderControl(id, field, value) {
  if (field.type === "toggle") {
    return `
      <label class="switch">
        <input type="checkbox" id="${id}" ${value ? "checked" : ""} />
        <span class="switch-track"></span>
      </label>`;
  }

  if (field.type === "select") {
    const options = (field.options || [])
      .map((opt) => `<option value="${opt}" ${opt === value ? "selected" : ""}>${opt}</option>`)
      .join("");
    return `<select id="${id}">${options}</select>`;
  }

  if (field.type === "template") {
    const escaped = String(value ?? "").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    return `<textarea id="${id}" class="template-input" rows="2">${escaped}</textarea>`;
  }

  const inputType = field.type === "number" ? "number" : "text";
  const step = field.type === "number" ? ' step="any"' : "";
  return `<input type="${inputType}"${step} id="${id}" value="${value ?? ""}" />`;
}

// Small "insert {name}" chips shown under a template textarea — the closest
// thing to autocomplete without building a full contenteditable engine:
// click a chip, it's spliced into the textarea at the cursor position.
function renderVariableChips(controlId, variableNames) {
  if (!variableNames.length) return "";
  return `
    <div class="variable-chips" data-for="${controlId}">
      ${variableNames.map((name) => `<button type="button" class="variable-chip" data-insert="{${name}}">{${name}}</button>`).join("")}
    </div>`;
}

function wireVariableChips(container) {
  container.querySelectorAll(".variable-chips").forEach((chipRow) => {
    const textarea = container.querySelector(`#${CSS.escape(chipRow.dataset.for)}`);
    if (!textarea) return;
    chipRow.querySelectorAll(".variable-chip").forEach((chip) => {
      chip.addEventListener("click", () => {
        const start = textarea.selectionStart ?? textarea.value.length;
        const end = textarea.selectionEnd ?? textarea.value.length;
        const insert = chip.dataset.insert;
        textarea.value = textarea.value.slice(0, start) + insert + textarea.value.slice(end);
        textarea.focus();
        textarea.selectionStart = textarea.selectionEnd = start + insert.length;
        textarea.dispatchEvent(new Event("input", { bubbles: true }));
      });
    });
  });
}

function renderField(tier, name, field, siblingFields) {
  const id = fieldId(tier, name, field.name);
  const label = field.label || field.name;
  const control = renderControl(id, field, field.default);

  if (field.type === "toggle") {
    return `
      <div class="field toggle-row">
        <label for="${id}">${label}</label>
        ${control}
      </div>`;
  }

  if (field.type === "template") {
    const variableNames = (siblingFields || []).map((f) => f.name).filter((n) => n !== field.name);
    return `
      <div class="field">
        <label for="${id}">${label}</label>
        ${control}
        ${renderVariableChips(id, variableNames)}
      </div>`;
  }

  return `
    <div class="field">
      <label for="${id}">${label}</label>
      ${control}
    </div>`;
}

function statusPill(status) {
  return `<span class="status-pill ${status}"><i class="dot dot-${status}"></i>${status}</span>`;
}

// ---- Theme toggle ----

function applyTheme(theme) {
  document.body.classList.toggle("theme-light", theme === "light");
  themeToggleBtn.textContent = theme === "light" ? "🌙" : "☀️";
  localStorage.setItem(THEME_STORAGE_KEY, theme);
}

themeToggleBtn.addEventListener("click", () => {
  const next = document.body.classList.contains("theme-light") ? "dark" : "light";
  applyTheme(next);
});

applyTheme(localStorage.getItem(THEME_STORAGE_KEY) || "dark");

// ---- Toasts + notification history ----

function updateNotifBadge() {
  if (unreadNotifications > 0) {
    notifBadge.textContent = unreadNotifications > 9 ? "9+" : String(unreadNotifications);
    notifBadge.classList.remove("hidden");
  } else {
    notifBadge.classList.add("hidden");
  }
}

function renderNotificationsPanel() {
  if (!toastHistory.length) {
    notificationsPanel.innerHTML = `<h4>Notifications</h4><div class="dropdown-empty">Nothing yet — run something.</div>`;
    return;
  }
  notificationsPanel.innerHTML = `
    <h4>Notifications</h4>
    ${toastHistory
      .map(
        (n) => `
      <div class="notification-row">
        <i class="dot dot-${n.type === "success" ? "ready" : "error"}"></i>
        <div>
          <span class="notification-message">${n.message}</span>
          <span class="notification-time">${n.time.toLocaleTimeString()}</span>
        </div>
      </div>`
      )
      .join("")}`;
}

function showToast(message, type = "success") {
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.textContent = message;
  toastContainer.appendChild(el);
  setTimeout(() => {
    el.classList.add("leaving");
    setTimeout(() => el.remove(), 200);
  }, 4500);

  toastHistory.unshift({ message, type, time: new Date() });
  toastHistory = toastHistory.slice(0, 30);
  unreadNotifications += 1;
  updateNotifBadge();
  renderNotificationsPanel();
}

notificationsBtn.addEventListener("click", () => {
  const opening = notificationsPanel.classList.contains("hidden");
  notificationsPanel.classList.toggle("hidden");
  healthPanel.classList.add("hidden");
  if (opening) {
    unreadNotifications = 0;
    updateNotifBadge();
  }
});

// ---- System health beacon ----

async function loadHealth() {
  try {
    const res = await fetch("/api/health");
    const body = await res.json();
    const ready = body.status === "ready";

    healthBeacon.classList.toggle("degraded", !ready);
    healthLabel.textContent = ready ? "Ready" : "Degraded";
    healthBeacon.querySelector(".dot").className = `dot ${ready ? "dot-ready" : "dot-error"}`;

    healthPanel.innerHTML = `
      <h4>System Health</h4>
      ${body.checks
        .map(
          (c) => `
        <div class="health-check-row">
          <i class="dot ${c.ok ? "dot-ready" : "dot-error"}"></i>
          <div>
            <span class="health-check-name">${c.name.replace(/_/g, " ")}</span>
            <span class="health-check-detail">${c.detail}</span>
          </div>
        </div>`
        )
        .join("")}`;
  } catch (err) {
    healthLabel.textContent = "Unreachable";
    healthBeacon.classList.add("degraded");
  }
}

healthBeacon.addEventListener("click", () => {
  healthPanel.classList.toggle("hidden");
  notificationsPanel.classList.add("hidden");
});

document.addEventListener("click", (e) => {
  if (!e.target.closest(".dropdown-wrap")) {
    notificationsPanel.classList.add("hidden");
    healthPanel.classList.add("hidden");
  }
});

// ---- Metrics ticker ----

async function loadMetrics() {
  const res = await fetch("/api/metrics");
  const m = await res.json();
  metricTotalEl.textContent = m.total_runs;
  metricSuccessEl.textContent = m.success_rate != null ? `${Math.round(m.success_rate * 100)}%` : "–";
  metricDurationEl.textContent = m.avg_duration_seconds != null ? `${m.avg_duration_seconds}s` : "–";
}

// ---- Recent runs table ----

async function loadRecentRuns() {
  const res = await fetch("/api/runs?limit=15");
  const runs = await res.json();

  if (!runs.length) {
    recentRunsTableEl.innerHTML = `<div class="runs-empty">No runs yet — click any Run button below.</div>`;
    return;
  }

  const rows = runs
    .map((r) => {
      const duration =
        r.started_at && r.finished_at
          ? `${((new Date(r.finished_at) - new Date(r.started_at)) / 1000).toFixed(2)}s`
          : "–";
      return `
        <tr>
          <td>#${r.id}</td>
          <td class="status-${r.status}">${r.status}</td>
          <td>${new Date(r.started_at).toLocaleString()}</td>
          <td>${duration}</td>
        </tr>`;
    })
    .join("");

  recentRunsTableEl.innerHTML = `
    <table>
      <thead><tr><th>Run</th><th>Status</th><th>Started</th><th>Duration</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

async function refreshTelemetry() {
  await Promise.all([loadMetrics(), loadRecentRuns()]);
}

// ---- Search omnibar ----

function applySearchFilter() {
  const q = searchOmnibar.value.trim().toLowerCase();
  document.querySelectorAll(".card[data-search-text]").forEach((card) => {
    const matches = !q || card.dataset.searchText.includes(q);
    card.classList.toggle("search-hidden", !matches);
  });

  document.querySelectorAll(".tier-section").forEach((section) => {
    const grid = section.querySelector(".card-grid");
    if (!grid) return;
    const cards = [...grid.querySelectorAll(".card")];
    const anyVisible = cards.some((c) => !c.classList.contains("search-hidden"));
    section.classList.toggle("search-no-match", Boolean(q) && cards.length > 0 && !anyVisible);
  });
}

searchOmnibar.addEventListener("input", applySearchFilter);

// ---- Collapsible sections ----

function loadCollapsedState() {
  try {
    return JSON.parse(localStorage.getItem(COLLAPSE_STORAGE_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveCollapsedState(state) {
  localStorage.setItem(COLLAPSE_STORAGE_KEY, JSON.stringify(state));
}

function applyCollapsedState(container) {
  const state = loadCollapsedState();
  container.querySelectorAll(".collapse-toggle").forEach((btn) => {
    const section = btn.closest(".tier-section");
    if (section && state[btn.dataset.collapseKey]) section.classList.add("collapsed");
  });
}

function wireCollapseToggles(container) {
  container.querySelectorAll(".collapse-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const section = btn.closest(".tier-section");
      section.classList.toggle("collapsed");
      const state = loadCollapsedState();
      state[btn.dataset.collapseKey] = section.classList.contains("collapsed");
      saveCollapsedState(state);
    });
  });
}

applyCollapsedState(document);
wireCollapseToggles(document);

// ---- Favorites ----

function loadFavoriteKeys() {
  try {
    return new Set(JSON.parse(localStorage.getItem(FAVORITES_STORAGE_KEY) || "[]"));
  } catch {
    return new Set();
  }
}

let favoriteKeys = loadFavoriteKeys();

function saveFavoriteKeys() {
  localStorage.setItem(FAVORITES_STORAGE_KEY, JSON.stringify([...favoriteKeys]));
}

function isFavorite(key) {
  return favoriteKeys.has(key);
}

function toggleFavoriteKey(key) {
  if (favoriteKeys.has(key)) favoriteKeys.delete(key);
  else favoriteKeys.add(key);
  saveFavoriteKeys();
}

function favoriteButtonHtml(key) {
  const active = isFavorite(key);
  return `<button class="favorite-toggle ${active ? "active" : ""}" data-fav-key="${key}" type="button" title="${active ? "Remove from" : "Add to"} favorites">${active ? "★" : "☆"}</button>`;
}

function wireFavoriteToggles(container) {
  container.querySelectorAll(".favorite-toggle").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const key = btn.dataset.favKey;
      toggleFavoriteKey(key);
      btn.classList.toggle("active", isFavorite(key));
      btn.textContent = isFavorite(key) ? "★" : "☆";
      btn.title = `${isFavorite(key) ? "Remove from" : "Add to"} favorites`;
      renderFavoritesSection();
    });
  });
}

function renderFavoriteChip(entry) {
  return `
    <div class="card favorite-chip">
      <div class="card-head">
        <h3 class="card-title">${entry.label}</h3>
        ${favoriteButtonHtml(entry.key)}
      </div>
      <p class="card-desc">${entry.description || ""}</p>
      <button class="btn btn-run" data-fav-run="${entry.key}" type="button">Run</button>
    </div>`;
}

function renderFavoritesSection() {
  const entries = [];
  for (const key of favoriteKeys) {
    if (key.startsWith("module::")) {
      const [, tier, name] = key.split("::");
      const module = (currentModulesByTier[tier] || []).find((m) => m.name === name);
      if (module) entries.push({ key, label: module.name, description: module.description });
    } else if (key.startsWith("pipeline::")) {
      const slug = key.slice("pipeline::".length);
      const pipeline = currentPipelines.find((p) => p.slug === slug);
      if (pipeline) entries.push({ key, label: pipeline.name, description: pipeline.description });
    }
  }

  if (!entries.length) {
    favoritesSection.classList.add("hidden");
    return;
  }

  favoritesSection.classList.remove("hidden");
  favoritesGrid.innerHTML = entries.map(renderFavoriteChip).join("");
  wireFavoriteToggles(favoritesGrid);
  favoritesGrid.querySelectorAll("[data-fav-run]").forEach((btn) => {
    btn.addEventListener("click", () => runFavorite(btn.dataset.favRun));
  });
}

function runFavorite(key) {
  if (key.startsWith("module::")) {
    const [, tier, name] = key.split("::");
    runModule(tier, name);
  } else if (key.startsWith("pipeline::")) {
    const slug = key.slice("pipeline::".length);
    const pipeline = currentPipelines.find((p) => p.slug === slug);
    if (pipeline) runSavedPipeline(slug, pipeline.name, currentPipelines);
  }
}

// ---- Skeleton loaders ----

function skeletonCardHtml() {
  return `
    <div class="skeleton-card">
      <div class="skeleton-line short"></div>
      <div class="skeleton-line"></div>
      <div class="skeleton-line"></div>
      <div class="skeleton-line tall"></div>
    </div>`;
}

function renderSkeletonSections() {
  sectionsEl.innerHTML = TIER_ORDER.map(
    (tier) => `
      <section class="tier-section tier-${tier}">
        <div class="tier-heading">
          <span class="bar"></span>
          <h2>${TIER_LABELS[tier]}</h2>
        </div>
        <div class="card-grid">${skeletonCardHtml()}${skeletonCardHtml()}</div>
      </section>`
  ).join("");
}

// ---- Interactive log filtering tabs (System Info / Agent Thoughts / Raw Output) ----

function createRunLog() {
  return { system: [], thoughts: [] };
}

function logSystemEvent(log, event) {
  const ts = new Date().toLocaleTimeString();
  if (event.kind === "step_started") {
    log.system.push(`[${ts}] Step ${event.index + 1} (${event.tier}: ${event.name}) started`);
  } else if (event.kind === "step_completed") {
    log.system.push(`[${ts}] Step ${event.index + 1} (${event.tier}: ${event.name}) completed in ${event.duration_ms}ms`);
  } else if (event.kind === "step_failed") {
    log.system.push(`[${ts}] Step ${event.index + 1} (${event.tier}: ${event.name}) FAILED after ${event.duration_ms}ms: ${event.error}`);
  } else if (event.kind === "thought" || event.kind === "tool_call") {
    log.thoughts.push(`[${event.tier}: ${event.name}] ${event.kind === "tool_call" ? "🔧" : "💭"} ${event.message}`);
  }
}

function renderRunLogTabs(container, log, rawContext) {
  const active = container.dataset.activeTab || "system";
  const rawMode = container.dataset.rawMode || "pretty";
  const tabs = [
    { key: "system", label: "System Info" },
    { key: "thoughts", label: "Agent Thoughts" },
    { key: "raw", label: "Raw Output" },
  ];
  const rawText = rawContext
    ? rawMode === "pretty"
      ? JSON.stringify(rawContext, null, 2)
      : JSON.stringify(rawContext)
    : "Run still in progress…";
  const content = {
    system: log.system.length ? log.system.join("\n") : "No system events yet.",
    thoughts: log.thoughts.length ? log.thoughts.join("\n") : "No agent thoughts in this run.",
    raw: rawText,
  };

  container.dataset.activeTab = active;
  container.dataset.rawMode = rawMode;

  const rawToggle =
    active === "raw" && rawContext
      ? `<button class="log-tab" data-raw-toggle type="button">${rawMode === "pretty" ? "Compact" : "Pretty"}</button>`
      : "";

  container.innerHTML = `
    <div class="log-tab-header">
      <div class="log-tabs">
        ${tabs.map((t) => `<button class="log-tab ${t.key === active ? "active" : ""}" data-tab="${t.key}" type="button">${t.label}</button>`).join("")}
        ${rawToggle}
      </div>
      <button class="copy-btn" type="button">📋 Copy</button>
    </div>
    <pre class="log-tab-content">${content[active]}</pre>`;

  container.querySelectorAll(".log-tab[data-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      container.dataset.activeTab = btn.dataset.tab;
      renderRunLogTabs(container, log, rawContext);
    });
  });

  container.querySelector("[data-raw-toggle]")?.addEventListener("click", () => {
    container.dataset.rawMode = rawMode === "pretty" ? "compact" : "pretty";
    renderRunLogTabs(container, log, rawContext);
  });

  container.querySelector(".copy-btn").addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(content[active]);
      showToast("Copied to clipboard.", "success");
    } catch (err) {
      showToast(`Copy failed: ${err}`, "error");
    }
  });
}

// ---- Live progress tracker + agent thought stream ----
// Rows are keyed by *step index*, not module name — a pipeline can legitimately
// use the same module twice, and name-keying would make both rows update together.

function renderTracker(container, steps) {
  container.innerHTML = steps
    .map((s, index) => {
      const thoughtControls =
        s.tier === "agent"
          ? `<button class="thought-toggle hidden" type="button" data-index="${index}">Thoughts</button>`
          : "";
      const thoughtBox = s.tier === "agent" ? `<div class="thought-box hidden" data-index="${index}"></div>` : "";
      return `
        <div class="tracker-step" data-tier="${s.tier}" data-index="${index}">
          <span class="tracker-icon">${STEP_ICON.pending}</span>
          <span class="tracker-label">Step ${index + 1}: [${s.tier}] ${s.name}</span>
          ${thoughtControls}
        </div>
        ${thoughtBox}`;
    })
    .join("");

  container.querySelectorAll(".thought-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const box = container.querySelector(`.thought-box[data-index="${btn.dataset.index}"]`);
      box.classList.toggle("collapsed");
    });
  });
}

function setStepStatus(container, index, status, error) {
  const row = container.querySelector(`.tracker-step[data-index="${index}"]`);
  if (!row) return;
  const icon = row.querySelector(".tracker-icon");
  icon.textContent = STEP_ICON[status];
  icon.classList.toggle("spin", status === "running");
  row.classList.toggle("failed", status === "failed");
  if (error) row.title = error;

  if (row.dataset.tier === "agent" && status === "running") {
    const toggle = container.querySelector(`.thought-toggle[data-index="${index}"]`);
    toggle?.classList.remove("hidden");
  }
}

function appendThought(container, index, kind, message) {
  const box = container.querySelector(`.thought-box[data-index="${index}"]`);
  if (!box) return;
  box.classList.remove("hidden");
  const line = document.createElement("div");
  line.className = "thought-line";
  line.textContent = `${kind === "tool_call" ? "🔧" : "💭"} ${message}`;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

function handleTrackerEvent(container, event) {
  if (event.kind === "step_started") {
    setStepStatus(container, event.index, "running");
  } else if (event.kind === "step_completed") {
    setStepStatus(container, event.index, "done");
  } else if (event.kind === "step_failed") {
    setStepStatus(container, event.index, "failed", event.error);
  } else if (event.kind === "thought" || event.kind === "tool_call") {
    appendThought(container, event.index, event.kind, event.message);
  }
}

// ---- SSE subscription ----

function subscribeToStream(streamId, { onEvent, onDone }) {
  const source = new EventSource(`/api/stream/${streamId}`);
  let finished = false;

  const finish = (event) => {
    if (finished) return;
    finished = true;
    source.close();
    onDone(event);
  };

  source.onmessage = (e) => {
    const event = JSON.parse(e.data);
    onEvent(event);
    if (event.kind === "run_completed" || event.kind === "run_failed") {
      finish(event);
    }
  };

  source.onerror = () => {
    finish({ kind: "run_failed", error: "Connection to the server was lost." });
  };
}

// ---- Cards ----

function renderCard(module) {
  const cardId = `card__${module.tier}__${module.name}`;
  const favKey = `module::${module.tier}::${module.name}`;
  const searchText = `${module.name} ${module.description}`.toLowerCase();
  const fieldsHtml = module.inputs.length
    ? `<div class="form-fields">${module.inputs.map((f) => renderField(module.tier, module.name, f, module.inputs)).join("")}</div>`
    : "";

  return `
    <div class="card" id="${cardId}" data-search-text="${searchText}">
      <div class="card-head">
        <h3 class="card-title">${module.name}</h3>
        <div class="card-head-actions">
          ${favoriteButtonHtml(favKey)}
          <button class="code-toggle" data-tier="${module.tier}" data-name="${module.name}" type="button" title="View source">&lt;/&gt;</button>
          <div class="status-slot">${statusPill(module.status)}</div>
        </div>
      </div>
      <p class="card-desc">${module.description}</p>
      <div class="code-panel hidden"></div>
      ${fieldsHtml}
      <button class="btn btn-run" data-tier="${module.tier}" data-name="${module.name}">Run</button>
      <div class="tracker hidden"></div>
      <div class="result-panel hidden"></div>
    </div>`;
}

function escapeHtml(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

async function toggleModuleSource(tier, name) {
  const card = document.getElementById(`card__${tier}__${name}`);
  const panel = card.querySelector(".code-panel");

  if (!panel.classList.contains("hidden")) {
    panel.classList.add("hidden");
    return;
  }

  panel.classList.remove("hidden");
  panel.innerHTML = `<div class="runs-empty">Loading source…</div>`;

  try {
    const res = await fetch(`/api/modules/${tier}/${name}/source`);
    const body = await res.json();
    if (!res.ok) {
      panel.innerHTML = `<div class="runs-empty">Error: ${body.detail || "Could not load source."}</div>`;
      return;
    }

    const issueCount = body.issues.length;
    const badgeStatus = issueCount ? "error" : "ready";
    const badgeText = issueCount ? `${issueCount} issue${issueCount === 1 ? "" : "s"}` : "no issues";
    const badge = `<span class="status-pill ${badgeStatus}"><i class="dot dot-${badgeStatus}"></i>${badgeText}</span>`;

    const issuesHtml = issueCount
      ? `<div class="lint-issues">${body.issues
          .map(
            (i) =>
              `<div class="lint-issue">Line ${i.line}:${i.column} <span class="lint-code">${i.code}</span> — ${escapeHtml(i.message)}</div>`
          )
          .join("")}</div>`
      : "";

    panel.innerHTML = `
      <div class="code-panel-header">
        <span class="code-path">${body.path}</span>
        ${badge}
      </div>
      ${issuesHtml}
      <pre class="code-view">${escapeHtml(body.source)}</pre>`;
  } catch (err) {
    panel.innerHTML = `<div class="runs-empty">Request failed: ${err}</div>`;
  }
}

function renderSections(modulesByTier) {
  sectionsEl.innerHTML = TIER_ORDER.map((tier) => {
    const modules = modulesByTier[tier] || [];
    if (!modules.length) return "";
    return `
      <section class="tier-section tier-${tier}">
        <div class="tier-heading">
          <span class="bar"></span>
          <h2>${TIER_LABELS[tier]}</h2>
          <div class="tier-heading-actions">
            <button class="collapse-toggle" type="button" data-collapse-key="tier-${tier}">▾</button>
          </div>
        </div>
        <div class="card-grid">
          ${modules.map(renderCard).join("")}
        </div>
      </section>`;
  }).join("");

  sectionsEl.querySelectorAll(".btn-run").forEach((btn) => {
    btn.addEventListener("click", () => runModule(btn.dataset.tier, btn.dataset.name));
  });
  sectionsEl.querySelectorAll(".code-toggle").forEach((btn) => {
    btn.addEventListener("click", () => toggleModuleSource(btn.dataset.tier, btn.dataset.name));
  });
  wireFavoriteToggles(sectionsEl);
  wireVariableChips(sectionsEl);
  applyCollapsedState(sectionsEl);
  wireCollapseToggles(sectionsEl);
  applySearchFilter();
}

async function loadModules() {
  const res = await fetch("/api/modules");
  const modulesByTier = await res.json();
  currentModulesByTier = modulesByTier;
  renderSections(modulesByTier);
  renderFavoritesSection();
  return modulesByTier;
}

function setCardStatus(cardId, status) {
  const card = document.getElementById(cardId);
  if (!card) return;
  card.querySelector(".status-slot").innerHTML = statusPill(status);
}

async function runModule(tier, name) {
  const cardId = `card__${tier}__${name}`;
  const card = document.getElementById(cardId);
  card.scrollIntoView({ behavior: "smooth", block: "center" });

  const button = card.querySelector(".btn-run");
  const resultPanel = card.querySelector(".result-panel");
  const tracker = card.querySelector(".tracker");

  const inputEls = card.querySelectorAll("[id^='field__']");
  const inputs = {};
  inputEls.forEach((el) => {
    const fieldName = el.id.split("__").slice(3).join("__");
    inputs[fieldName] = el.type === "checkbox" ? el.checked : el.value;
  });

  button.disabled = true;
  setCardStatus(cardId, "running");
  resultPanel.classList.add("hidden");
  renderTracker(tracker, [{ tier, name }]);
  tracker.classList.remove("hidden");

  let lastOutput = null;
  let lastError = null;

  try {
    const res = await fetch(`/api/modules/${tier}/${name}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs }),
    });
    const { stream_id } = await res.json();

    subscribeToStream(stream_id, {
      onEvent: (event) => {
        handleTrackerEvent(tracker, event);
        if (event.kind === "step_completed") lastOutput = event.output;
        if (event.kind === "step_failed") lastError = event.error;
      },
      onDone: async (event) => {
        button.disabled = false;
        const success = event.kind === "run_completed";
        setCardStatus(cardId, success ? "ready" : "error");

        resultPanel.classList.remove("hidden", "error");
        if (success) {
          resultPanel.textContent = JSON.stringify(lastOutput, null, 2);
        } else {
          resultPanel.classList.add("error");
          resultPanel.textContent = `Error: ${lastError ?? event.error}`;
        }

        showToast(
          success ? `${name} completed successfully.` : `${name} failed: ${lastError ?? event.error}`,
          success ? "success" : "error"
        );
        await refreshTelemetry();
      },
    });
  } catch (err) {
    button.disabled = false;
    setCardStatus(cardId, "error");
    resultPanel.classList.remove("hidden");
    resultPanel.classList.add("error");
    resultPanel.textContent = `Request failed: ${err}`;
    showToast(`${name} failed: ${err}`, "error");
  }
}

async function runFullPipeline() {
  runPipelineBtn.disabled = true;
  runPipelineBtn.textContent = "Running...";

  const modulesByTier = await (await fetch("/api/modules")).json();
  const steps = TIER_ORDER.flatMap((tier) => (modulesByTier[tier] || []).map((m) => ({ tier, name: m.name })));

  pipelineResultEl.classList.remove("hidden");
  pipelineResultEl.innerHTML = `
    <h3>Full Pipeline</h3>
    <div class="tracker" id="pipeline-tracker"></div>
    <div class="log-tabs-wrap" id="pipeline-log"></div>`;
  const tracker = document.getElementById("pipeline-tracker");
  const logContainer = document.getElementById("pipeline-log");
  renderTracker(tracker, steps);
  const log = createRunLog();
  renderRunLogTabs(logContainer, log, null);

  try {
    const res = await fetch("/api/pipeline/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs: {} }),
    });
    const { stream_id } = await res.json();

    subscribeToStream(stream_id, {
      onEvent: (event) => {
        handleTrackerEvent(tracker, event);
        logSystemEvent(log, event);
        renderRunLogTabs(logContainer, log, null);
      },
      onDone: async (event) => {
        runPipelineBtn.disabled = false;
        runPipelineBtn.textContent = "Run Full Pipeline";

        const success = event.kind === "run_completed";
        renderRunLogTabs(logContainer, log, event.context ?? {});

        showToast(
          success ? "Full pipeline completed successfully." : `Full pipeline failed: ${event.error}`,
          success ? "success" : "error"
        );
        await loadModules();
        await refreshTelemetry();
      },
    });
  } catch (err) {
    runPipelineBtn.disabled = false;
    runPipelineBtn.textContent = "Run Full Pipeline";
    showToast(`Full pipeline failed: ${err}`, "error");
  }
}

runPipelineBtn.addEventListener("click", runFullPipeline);

// ---- No-code visual pipeline builder ----

function blankBuilderStep() {
  return { tier: "", name: "", module: null, fieldSources: {} };
}

function defaultFieldSources(module) {
  return Object.fromEntries(module.inputs.map((f) => [f.name, { type: "static", value: f.default }]));
}

function renderBuilderField(stepIndex, field, source, priorOutputs) {
  const controlId = `bfield__${stepIndex}__${field.name}`;
  const srcId = `bsrc__${stepIndex}__${field.name}`;
  const isMapped = source.type === "mapping";

  const sourceOptions = ['<option value="static">Static value</option>']
    .concat(
      priorOutputs.map((o) => {
        const optionValue = `map:${o.stepIndex}:${o.name}`;
        const selected = isMapped && source.step === o.stepIndex && source.output === o.name;
        return `<option value="${optionValue}"${selected ? " selected" : ""}>From Step ${o.stepIndex + 1}: ${o.label}</option>`;
      })
    )
    .join("");

  const control = isMapped
    ? `<div class="mapping-tag">↳ Step ${source.step + 1}: ${source.output}</div>`
    : renderControl(controlId, field, source.value);

  const chips =
    !isMapped && field.type === "template" ? renderVariableChips(controlId, priorOutputs.map((o) => o.name)) : "";

  return `
    <div class="field builder-field">
      <label>${field.label || field.name}</label>
      <div class="mapping-row">
        <select class="source-select" id="${srcId}" data-step="${stepIndex}" data-field="${field.name}">${sourceOptions}</select>
        <div class="mapping-control">${control}</div>
      </div>
      ${chips}
    </div>`;
}

function renderBuilderStep(index, step) {
  const priorOutputs = builderSteps.slice(0, index).flatMap((s, i) =>
    (s.module?.outputs || []).map((o) => ({ stepIndex: i, name: o.name, label: o.label || o.name }))
  );

  const moduleOptions = ['<option value="">Select a module…</option>']
    .concat(
      TIER_ORDER.flatMap((tier) =>
        (currentModulesByTier[tier] || []).map(
          (m) =>
            `<option value="${tier}::${m.name}"${step.tier === tier && step.name === m.name ? " selected" : ""}>[${tier}] ${m.name}</option>`
        )
      )
    )
    .join("");

  const fieldsHtml = step.module
    ? step.module.inputs.map((field) => renderBuilderField(index, field, step.fieldSources[field.name], priorOutputs)).join("")
    : '<p class="card-desc">Pick a module above to configure its inputs.</p>';

  const canRemove = builderSteps.length > 1 && index === builderSteps.length - 1;

  return `
    <div class="builder-step" data-index="${index}">
      <div class="builder-step-head">
        <span class="step-badge">${index + 1}</span>
        <select class="module-select" data-index="${index}">${moduleOptions}</select>
        ${canRemove ? `<button class="remove-step-btn" data-index="${index}" type="button" title="Remove step">×</button>` : ""}
      </div>
      <div class="builder-step-fields">${fieldsHtml}</div>
    </div>`;
}

function renderBuilder() {
  builderStepsEl.innerHTML = builderSteps.map((step, index) => renderBuilderStep(index, step)).join("");
  attachBuilderStepListeners();
  wireVariableChips(builderStepsEl);
}

function attachBuilderStepListeners() {
  builderStepsEl.querySelectorAll(".module-select").forEach((sel) => {
    sel.addEventListener("change", (e) => {
      const index = Number(e.target.dataset.index);
      const [tier, name] = e.target.value.split("::");

      if (!tier || !name) {
        builderSteps[index] = blankBuilderStep();
      } else {
        const module = (currentModulesByTier[tier] || []).find((m) => m.name === name);
        builderSteps[index] = { tier, name, module, fieldSources: defaultFieldSources(module) };
      }

      // A later step may have been mapping from this step's old module — its
      // outputs may no longer exist, so reset every later step's mappings.
      for (let i = index + 1; i < builderSteps.length; i++) {
        if (builderSteps[i].module) {
          builderSteps[i].fieldSources = defaultFieldSources(builderSteps[i].module);
        }
      }
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".remove-step-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      builderSteps.splice(Number(e.currentTarget.dataset.index), 1);
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".source-select").forEach((sel) => {
    sel.addEventListener("change", (e) => {
      const stepIndex = Number(e.target.dataset.step);
      const fieldName = e.target.dataset.field;
      const value = e.target.value;

      if (value === "static") {
        const field = builderSteps[stepIndex].module.inputs.find((f) => f.name === fieldName);
        builderSteps[stepIndex].fieldSources[fieldName] = { type: "static", value: field.default };
      } else {
        const [, stepStr, output] = value.split(":");
        builderSteps[stepIndex].fieldSources[fieldName] = { type: "mapping", step: Number(stepStr), output };
      }
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".mapping-control input, .mapping-control select, .mapping-control textarea").forEach((control) => {
    const handler = (e) => {
      const wrapper = e.target.closest(".field.builder-field");
      const srcSelect = wrapper.querySelector(".source-select");
      const stepIndex = Number(srcSelect.dataset.step);
      const fieldName = srcSelect.dataset.field;
      builderSteps[stepIndex].fieldSources[fieldName].value =
        e.target.type === "checkbox" ? e.target.checked : e.target.value;
    };
    control.addEventListener("input", handler);
    control.addEventListener("change", handler);
  });
}

function openBuilder() {
  builderPanelEl.classList.remove("hidden");
  builderToggleBtn.textContent = "Close Builder";
  if (builderSteps.length === 0) {
    builderSteps.push(blankBuilderStep());
    renderBuilder();
  }
}

function closeBuilder() {
  builderPanelEl.classList.add("hidden");
  builderToggleBtn.textContent = "+ New Pipeline";
}

builderToggleBtn.addEventListener("click", () => {
  if (builderPanelEl.classList.contains("hidden")) openBuilder();
  else closeBuilder();
});

builderAddStepBtn.addEventListener("click", () => {
  builderSteps.push(blankBuilderStep());
  renderBuilder();
});

function showBuilderError(message) {
  builderErrorEl.textContent = message;
  builderErrorEl.classList.remove("hidden");
}

builderLaunchBtn.addEventListener("click", async () => {
  builderErrorEl.classList.add("hidden");

  const name = builderNameEl.value.trim();
  if (!name) return showBuilderError("Pipeline name is required.");
  if (builderSteps.length === 0 || builderSteps.some((s) => !s.module)) {
    return showBuilderError("Every step needs a module selected.");
  }

  const steps = builderSteps.map((step) => {
    const inputs = {};
    const mappings = {};
    for (const [fieldName, source] of Object.entries(step.fieldSources)) {
      if (source.type === "mapping") {
        mappings[fieldName] = { step: source.step, output: source.output };
      } else {
        inputs[fieldName] = source.value;
      }
    }
    return { tier: step.tier, name: step.name, inputs, mappings };
  });

  builderLaunchBtn.disabled = true;
  builderLaunchBtn.textContent = "Launching...";

  try {
    const res = await fetch("/api/pipelines", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, description: builderDescriptionEl.value.trim(), steps }),
    });

    if (!res.ok) {
      const err = await res.json();
      showBuilderError(err.detail || "Failed to save this pipeline.");
      builderLaunchBtn.disabled = false;
      builderLaunchBtn.textContent = "Save & Launch";
      return;
    }

    const { stream_id } = await res.json();
    const trackerSteps = builderSteps.map((s) => ({ tier: s.tier, name: s.name }));
    builderTrackerEl.classList.remove("hidden");
    builderResultEl.classList.remove("hidden");
    renderTracker(builderTrackerEl, trackerSteps);
    const log = createRunLog();
    renderRunLogTabs(builderResultEl, log, null);

    subscribeToStream(stream_id, {
      onEvent: (event) => {
        handleTrackerEvent(builderTrackerEl, event);
        logSystemEvent(log, event);
        renderRunLogTabs(builderResultEl, log, null);
      },
      onDone: async (event) => {
        builderLaunchBtn.disabled = false;
        builderLaunchBtn.textContent = "Save & Launch";

        const success = event.kind === "run_completed";
        renderRunLogTabs(builderResultEl, log, event.context ?? {});

        showToast(
          success ? `Pipeline "${name}" completed successfully.` : `Pipeline "${name}" failed: ${event.error}`,
          success ? "success" : "error"
        );
        await loadSavedPipelines();
        await refreshTelemetry();
      },
    });
  } catch (err) {
    builderLaunchBtn.disabled = false;
    builderLaunchBtn.textContent = "Save & Launch";
    showBuilderError(`Request failed: ${err}`);
  }
});

// ---- Saved pipelines ----

function renderSavedPipelines(pipelinesList) {
  currentPipelines = pipelinesList;

  if (!pipelinesList.length) {
    savedPipelinesSection.classList.add("hidden");
  } else {
    savedPipelinesSection.classList.remove("hidden");

    savedPipelinesGrid.innerHTML = pipelinesList
      .map((p) => {
        const chain = p.steps.map((s) => `[${s.tier}] ${s.name}`).join(" → ");
        const favKey = `pipeline::${p.slug}`;
        const searchText = `${p.name} ${p.description || ""} ${chain}`.toLowerCase();
        return `
          <div class="card" id="pipeline-card__${p.slug}" data-search-text="${searchText}">
            <div class="card-head">
              <h3 class="card-title">${p.name}</h3>
              ${favoriteButtonHtml(favKey)}
            </div>
            <p class="card-desc">${p.description || "No description."}</p>
            <p class="card-desc pipeline-chain">${chain}</p>
            <div class="pipeline-card-actions">
              <button class="btn btn-run" data-slug="${p.slug}" data-name="${p.name}">Run</button>
              <button class="btn btn-secondary btn-small" data-clone-slug="${p.slug}" type="button">Clone</button>
            </div>
            <div class="tracker hidden"></div>
            <div class="log-tabs-wrap hidden"></div>
          </div>`;
      })
      .join("");

    savedPipelinesGrid.querySelectorAll(".btn-run").forEach((btn) => {
      btn.addEventListener("click", () => runSavedPipeline(btn.dataset.slug, btn.dataset.name, pipelinesList));
    });
    savedPipelinesGrid.querySelectorAll("[data-clone-slug]").forEach((btn) => {
      btn.addEventListener("click", () => clonePipeline(btn.dataset.cloneSlug));
    });
    wireFavoriteToggles(savedPipelinesGrid);
  }

  applySearchFilter();
  renderFavoritesSection();
}

async function loadSavedPipelines() {
  const res = await fetch("/api/pipelines");
  const pipelinesList = await res.json();
  renderSavedPipelines(pipelinesList);
  return pipelinesList;
}

async function clonePipeline(slug) {
  try {
    const res = await fetch(`/api/pipelines/${slug}/duplicate`, { method: "POST" });
    if (!res.ok) {
      const err = await res.json();
      showToast(err.detail || "Failed to clone this pipeline.", "error");
      return;
    }
    const { pipeline } = await res.json();
    showToast(`Cloned as "${pipeline.name}".`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Clone failed: ${err}`, "error");
  }
}

async function runSavedPipeline(slug, name, pipelinesList) {
  const card = document.getElementById(`pipeline-card__${slug}`);
  card.scrollIntoView({ behavior: "smooth", block: "center" });

  const button = card.querySelector(".btn-run");
  const tracker = card.querySelector(".tracker");
  const logContainer = card.querySelector(".log-tabs-wrap");

  const definition = pipelinesList.find((p) => p.slug === slug);
  const trackerSteps = (definition?.steps || []).map((s) => ({ tier: s.tier, name: s.name }));

  button.disabled = true;
  renderTracker(tracker, trackerSteps);
  tracker.classList.remove("hidden");
  logContainer.classList.remove("hidden");
  const log = createRunLog();
  renderRunLogTabs(logContainer, log, null);

  try {
    const res = await fetch(`/api/pipelines/${slug}/run`, { method: "POST" });
    const { stream_id } = await res.json();

    subscribeToStream(stream_id, {
      onEvent: (event) => {
        handleTrackerEvent(tracker, event);
        logSystemEvent(log, event);
        renderRunLogTabs(logContainer, log, null);
      },
      onDone: async (event) => {
        button.disabled = false;
        const success = event.kind === "run_completed";
        renderRunLogTabs(logContainer, log, event.context ?? {});
        showToast(
          success ? `${name} completed successfully.` : `${name} failed: ${event.error}`,
          success ? "success" : "error"
        );
        await refreshTelemetry();
      },
    });
  } catch (err) {
    button.disabled = false;
    showToast(`${name} failed: ${err}`, "error");
  }
}

// ---- Data ingestion (CSV/PDF upload, dedupe, artifacts, purge) ----

const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("file-input");
const ingestionResultEl = document.getElementById("ingestion-result");
const artifactsListEl = document.getElementById("artifacts-list");
const purgeHoursInput = document.getElementById("purge-hours");
const purgeBtn = document.getElementById("purge-btn");

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

async function loadArtifacts() {
  const res = await fetch("/api/artifacts");
  const files = await res.json();

  if (!files.length) {
    artifactsListEl.innerHTML = `<div class="runs-empty">No artifacts yet — upload a CSV or PDF above.</div>`;
    return;
  }

  const rows = files
    .map(
      (f) => `
      <tr>
        <td>${f.name}</td>
        <td>${formatBytes(f.size_bytes)}</td>
        <td>${new Date(f.modified_at * 1000).toLocaleString()}</td>
      </tr>`
    )
    .join("");
  artifactsListEl.innerHTML = `
    <table>
      <thead><tr><th>File</th><th>Size</th><th>Modified</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

async function uploadFile(file) {
  const lowerName = file.name.toLowerCase();
  const isPdf = lowerName.endsWith(".pdf");
  const isCsv = lowerName.endsWith(".csv");
  if (!isPdf && !isCsv) {
    showToast("Only .csv and .pdf files are supported.", "error");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  ingestionResultEl.classList.remove("hidden");
  ingestionResultEl.innerHTML = `<pre class="log-tab-content">Uploading ${file.name}…</pre>`;

  try {
    const res = await fetch(isPdf ? "/api/ingest/pdf" : "/api/ingest/csv", { method: "POST", body: formData });
    const body = await res.json();

    if (!res.ok) {
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">Error: ${body.detail || "Upload failed."}</pre>`;
      showToast(`Upload failed: ${body.detail || "unknown error"}`, "error");
      return;
    }

    if (body.duplicate) {
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">This exact file was already ingested as "${body.original_filename}" at ${new Date(body.ingested_at).toLocaleString()}. Skipped.</pre>`;
      showToast(`Duplicate of "${body.original_filename}" — skipped.`, "error");
    } else if (isCsv) {
      const preview = JSON.stringify(body.preview, null, 2);
      const note = body.truncated ? `\n… (truncated — ${body.row_count} rows total)` : "";
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">${preview}${note}</pre>`;
      showToast(`Ingested ${body.filename}: ${body.row_count} row(s).`, "success");
    } else {
      const note = body.truncated ? "\n… (truncated)" : "";
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">${body.preview}${note}</pre>`;
      showToast(`Ingested ${body.filename}: ${body.char_count} character(s) extracted.`, "success");
    }

    await loadArtifacts();
  } catch (err) {
    ingestionResultEl.innerHTML = `<pre class="log-tab-content">Request failed: ${err}</pre>`;
    showToast(`Upload failed: ${err}`, "error");
  }
}

fileInput.addEventListener("change", (e) => {
  if (e.target.files[0]) uploadFile(e.target.files[0]);
  fileInput.value = "";
});

["dragenter", "dragover"].forEach((evt) =>
  dropzone.addEventListener(evt, (e) => {
    e.preventDefault();
    dropzone.classList.add("dragover");
  })
);
["dragleave", "drop"].forEach((evt) =>
  dropzone.addEventListener(evt, (e) => {
    e.preventDefault();
    dropzone.classList.remove("dragover");
  })
);
dropzone.addEventListener("drop", (e) => {
  const file = e.dataTransfer.files[0];
  if (file) uploadFile(file);
});

purgeBtn.addEventListener("click", async () => {
  const hours = Number(purgeHoursInput.value) || 0;
  const res = await fetch(`/api/artifacts/purge?older_than_hours=${hours}`, { method: "POST" });
  const body = await res.json();
  showToast(`Purged ${body.removed_count} artifact file(s).`, "success");
  await loadArtifacts();
});

// ---- Folder watcher (polls for auto-ingested files) ----

const watcherFeedEl = document.getElementById("watcher-feed");
const watchDirPathEl = document.getElementById("watch-dir-path");
let lastWatcherCount = 0;

function renderWatcherFeed(processed) {
  if (!processed.length) {
    watcherFeedEl.innerHTML = `<div class="runs-empty">No files auto-processed yet.</div>`;
    return;
  }
  watcherFeedEl.innerHTML = processed
    .slice()
    .reverse()
    .map(
      (p) => `
        <div class="notification-row">
          <i class="dot ${p.error ? "dot-error" : "dot-ready"}"></i>
          <div>
            <span class="notification-message">${p.filename}${p.duplicate ? " (duplicate, skipped)" : ""}${p.error ? ` — ${p.error}` : ""}</span>
            <span class="notification-time">${new Date(p.at).toLocaleTimeString()}</span>
          </div>
        </div>`
    )
    .join("");
}

async function pollWatcherStatus() {
  const res = await fetch("/api/watcher/status");
  const body = await res.json();
  watchDirPathEl.textContent = body.watch_dir;

  if (body.processed.length !== lastWatcherCount) {
    const isFirstLoad = lastWatcherCount === 0 && !watcherFeedEl.dataset.loaded;
    lastWatcherCount = body.processed.length;
    renderWatcherFeed(body.processed);
    watcherFeedEl.dataset.loaded = "true";

    if (!isFirstLoad && body.processed.length) {
      const latest = body.processed[body.processed.length - 1];
      showToast(
        latest.error
          ? `Auto-ingest failed for "${latest.filename}": ${latest.error}`
          : latest.duplicate
            ? `"${latest.filename}" from watched_input/ was a duplicate — skipped.`
            : `Auto-ingested "${latest.filename}" from watched_input/.`,
        latest.error ? "error" : "success"
      );
      await loadArtifacts();
    }
  }
}

setInterval(pollWatcherStatus, 4000);

// ---- Boot ----

renderSkeletonSections();
loadModules();
loadSavedPipelines();
loadHealth();
refreshTelemetry();
loadArtifacts();
pollWatcherStatus();
renderNotificationsPanel();
