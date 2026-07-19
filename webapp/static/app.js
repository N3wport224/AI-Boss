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
  skipped: "⏭️",
  retrying: "🔁",
};

const CONDITION_OPERATORS = ["equals", "not_equals", "contains", "gt", "lt", "truthy", "falsy"];
const CONDITION_OPERATOR_LABELS = {
  equals: "equals",
  not_equals: "does not equal",
  contains: "contains",
  gt: "is greater than",
  lt: "is less than",
  truthy: "has any value",
  falsy: "is empty/false",
};

const THEME_STORAGE_KEY = "aiboss-theme";
const COLLAPSE_STORAGE_KEY = "aiboss-collapsed-sections";
const FAVORITES_STORAGE_KEY = "aiboss-favorites";
const RECENTLY_VIEWED_STORAGE_KEY = "aiboss-recently-viewed";
const RECENTLY_VIEWED_MAX = 8;

const sectionsEl = document.getElementById("sections");
const selectedModuleRefs = new Set();
const modulesSelectAllEl = document.getElementById("modules-select-all");
const modulesBulkEnableBtn = document.getElementById("modules-bulk-enable-btn");
const modulesBulkDisableBtn = document.getElementById("modules-bulk-disable-btn");
const modulesBulkResetBreakerBtn = document.getElementById("modules-bulk-reset-breaker-btn");
const modulesBulkClearThresholdBtn = document.getElementById("modules-bulk-clear-threshold-btn");
const modulesProblemsFilterEl = document.getElementById("modules-problems-filter");
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

const selfTestBtn = document.getElementById("self-test-btn");
const selfTestPanel = document.getElementById("self-test-panel");

const metricTotalEl = document.getElementById("metric-total");
const metricSuccessEl = document.getElementById("metric-success");
const metricDurationEl = document.getElementById("metric-duration");

const perfCpuEl = document.getElementById("perf-cpu");
const perfMemEl = document.getElementById("perf-mem");
const perfThreadsEl = document.getElementById("perf-threads");
const perfUptimeEl = document.getElementById("perf-uptime");
const resourceWarningBannerEl = document.getElementById("resource-warning-banner");

const favoritesSection = document.getElementById("favorites-section");
const favoritesGrid = document.getElementById("favorites-grid");
const recentlyViewedSection = document.getElementById("recently-viewed-section");
const recentlyViewedGrid = document.getElementById("recently-viewed-grid");
const recentRunsTableEl = document.getElementById("recent-runs-table");

const builderToggleBtn = document.getElementById("builder-toggle");
const builderPanelEl = document.getElementById("builder-panel");
const builderNameEl = document.getElementById("builder-name");
const builderDescriptionEl = document.getElementById("builder-description");
const builderStepsEl = document.getElementById("builder-steps");
const builderAddStepBtn = document.getElementById("builder-add-step");
const builderLaunchBtn = document.getElementById("builder-launch");
const builderSaveOnlyBtn = document.getElementById("builder-save-only");
const builderValidateBtn = document.getElementById("builder-validate");
const builderErrorEl = document.getElementById("builder-error");
const builderTrackerEl = document.getElementById("builder-tracker");
const builderResultEl = document.getElementById("builder-result");
const builderEditingBannerEl = document.getElementById("builder-editing-banner");
const builderEditingNameEl = document.getElementById("builder-editing-name");
const builderCancelEditBtn = document.getElementById("builder-cancel-edit");

const savedPipelinesSection = document.getElementById("saved-pipelines-section");
const savedPipelinesGrid = document.getElementById("saved-pipelines-grid");
const pipelineTagFilterInput = document.getElementById("pipeline-tag-filter");
const pipelineDeepSearchInput = document.getElementById("pipeline-deep-search");
const pipelineDeepSearchResultsEl = document.getElementById("pipeline-deep-search-results");
const pipelinesBulkDeleteBtn = document.getElementById("pipelines-bulk-delete-btn");
const pipelinesSelectAllEl = document.getElementById("pipelines-select-all");
const pipelinesBulkUntagInput = document.getElementById("pipelines-bulk-untag-input");
const pipelinesBulkUntagBtn = document.getElementById("pipelines-bulk-untag-btn");
const pipelinesBulkTagInput = document.getElementById("pipelines-bulk-tag-input");
const pipelinesBulkTagBtn = document.getElementById("pipelines-bulk-tag-btn");
const pipelinesBulkDuplicateBtn = document.getElementById("pipelines-bulk-duplicate-btn");

const selectedPipelineSlugs = new Set();

function updatePipelinesBulkDeleteBtn() {
  pipelinesBulkDeleteBtn.disabled = selectedPipelineSlugs.size === 0;
  pipelinesBulkDeleteBtn.textContent = selectedPipelineSlugs.size
    ? `Delete selected (${selectedPipelineSlugs.size})`
    : "Delete selected";
  pipelinesBulkUntagBtn.disabled = selectedPipelineSlugs.size === 0;
  pipelinesBulkUntagBtn.textContent = selectedPipelineSlugs.size
    ? `Remove tag from selected (${selectedPipelineSlugs.size})`
    : "Remove tag from selected";
  pipelinesBulkTagBtn.disabled = selectedPipelineSlugs.size === 0;
  pipelinesBulkTagBtn.textContent = selectedPipelineSlugs.size
    ? `Apply tag to selected (${selectedPipelineSlugs.size})`
    : "Apply tag to selected";
  pipelinesBulkDuplicateBtn.disabled = selectedPipelineSlugs.size === 0;
  pipelinesBulkDuplicateBtn.textContent = selectedPipelineSlugs.size
    ? `Duplicate selected (${selectedPipelineSlugs.size})`
    : "Duplicate selected";
}

pipelinesBulkDuplicateBtn.addEventListener("click", async () => {
  if (!selectedPipelineSlugs.size) return;
  try {
    const res = await fetch("/api/pipelines/bulk-duplicate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ slugs: [...selectedPipelineSlugs] }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Failed to duplicate selected pipelines.", "error");
      return;
    }
    showToast(`Duplicated ${body.duplicated.length} pipeline(s).`, "success");
    selectedPipelineSlugs.clear();
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Bulk duplicate failed: ${err}`, "error");
  }
});

pipelinesBulkTagBtn.addEventListener("click", async () => {
  const tag = pipelinesBulkTagInput.value.trim();
  if (!tag) {
    showToast("Enter a tag first.", "error");
    return;
  }
  if (!selectedPipelineSlugs.size) return;

  try {
    const res = await fetch("/api/pipelines/bulk-tags", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ slugs: [...selectedPipelineSlugs], tag }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Failed to apply tag.", "error");
      return;
    }
    showToast(`Applied "${tag}" to ${body.tagged.length} pipeline(s).`, "success");
    pipelinesBulkTagInput.value = "";
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Bulk tag failed: ${err}`, "error");
  }
});

pipelinesBulkUntagBtn.addEventListener("click", async () => {
  const tag = pipelinesBulkUntagInput.value.trim();
  if (!tag) {
    showToast("Enter a tag first.", "error");
    return;
  }
  if (!selectedPipelineSlugs.size) return;

  try {
    const res = await fetch("/api/pipelines/bulk-untag", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ slugs: [...selectedPipelineSlugs], tag }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Failed to remove tag.", "error");
      return;
    }
    showToast(`Removed "${tag}" from ${body.untagged.length} pipeline(s).`, "success");
    pipelinesBulkUntagInput.value = "";
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Bulk untag failed: ${err}`, "error");
  }
});

const pipelineTemplatesGrid = document.getElementById("pipeline-templates-grid");

const scheduleAddToggleBtn = document.getElementById("schedule-add-toggle");
const scheduleFormEl = document.getElementById("schedule-form");
const scheduleKindEl = document.getElementById("schedule-kind");
const scheduleTargetEl = document.getElementById("schedule-target");
const scheduleFrequencyEl = document.getElementById("schedule-frequency");
const scheduleIntervalFieldEl = document.getElementById("schedule-interval-field");
const scheduleDailyFieldEl = document.getElementById("schedule-daily-field");
const scheduleWeeklyFieldEl = document.getElementById("schedule-weekly-field");
const scheduleIntervalEl = document.getElementById("schedule-interval");
const scheduleDailyTimeEl = document.getElementById("schedule-daily-time");
const scheduleDayOfWeekEl = document.getElementById("schedule-day-of-week");
const scheduleWeeklyTimeEl = document.getElementById("schedule-weekly-time");
const scheduleOnceFieldEl = document.getElementById("schedule-once-field");
const scheduleRunAtEl = document.getElementById("schedule-run-at");
const scheduleErrorEl = document.getElementById("schedule-error");
const scheduleCreateBtn = document.getElementById("schedule-create-btn");
const schedulesListEl = document.getElementById("schedules-list");
const schedulerPauseToggleBtn = document.getElementById("scheduler-pause-toggle");
const schedulerPausedBannerEl = document.getElementById("scheduler-paused-banner");
const schedulesBulkPauseBtn = document.getElementById("schedules-bulk-pause-btn");
const schedulesBulkResumeBtn = document.getElementById("schedules-bulk-resume-btn");
const schedulesBulkFavoriteBtn = document.getElementById("schedules-bulk-favorite-btn");
const schedulesBulkClearLabelBtn = document.getElementById("schedules-bulk-clear-label-btn");
const schedulesBulkExportBtn = document.getElementById("schedules-bulk-export-btn");
const schedulesBulkDeleteBtn = document.getElementById("schedules-bulk-delete-btn");
const schedulesSelectAllEl = document.getElementById("schedules-select-all");
const scheduleFilterInput = document.getElementById("schedule-filter");
const scheduleSortSelect = document.getElementById("schedule-sort");

const selectedScheduleIds = new Set();

scheduleFilterInput.addEventListener("input", () => renderSchedulesList());
scheduleSortSelect.addEventListener("change", () => renderSchedulesList());

function updateSchedulesBulkButtons() {
  const disabled = selectedScheduleIds.size === 0;
  schedulesBulkPauseBtn.disabled = disabled;
  schedulesBulkResumeBtn.disabled = disabled;
  schedulesBulkFavoriteBtn.disabled = disabled;
  schedulesBulkClearLabelBtn.disabled = disabled;
  schedulesBulkExportBtn.disabled = disabled;
  schedulesBulkDeleteBtn.disabled = disabled;
  const suffix = selectedScheduleIds.size ? ` (${selectedScheduleIds.size})` : "";
  schedulesBulkPauseBtn.textContent = `Pause selected${suffix}`;
  schedulesBulkResumeBtn.textContent = `Resume selected${suffix}`;
  schedulesBulkFavoriteBtn.textContent = `★ Favorite selected${suffix}`;
  schedulesBulkClearLabelBtn.textContent = `Clear labels${suffix}`;
  schedulesBulkExportBtn.textContent = `⬇ Export selected JSON${suffix}`;
  schedulesBulkDeleteBtn.textContent = `Delete selected${suffix}`;
}

let currentModulesByTier = {};
let currentPipelines = [];
let currentArtifacts = [];
let builderSteps = [];
let editingPipelineSlug = null; // non-null while the builder holds a loaded saved pipeline

// Undo covers structural changes only — add/remove a step or branch, reorder,
// swap a step's module — not every field-level keystroke or toggle, which
// would flood the stack with edits nobody thinks of as an undo-able "action."
const BUILDER_UNDO_LIMIT = 20;
let builderUndoStack = [];
let toastHistory = [];
let unreadNotifications = 0;

// Every run-triggering fetch (module run, full pipeline, saved pipeline,
// builder launch) shares this: `fetch()` only throws on a network failure,
// not on a 4xx/5xx response (e.g. a 429 from the run-trigger rate limiter),
// so without this check a rejected request would silently try to stream
// from stream_id `undefined` instead of surfacing the server's error message.
async function fetchRunTrigger(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed with status ${res.status}`);
  }
  return res.json();
}

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

// ---- Compact/dense card view toggle ----

const DENSITY_STORAGE_KEY = "aiboss-density";
const densityToggleBtn = document.getElementById("density-toggle");

function applyDensity(density) {
  document.body.classList.toggle("density-compact", density === "compact");
  densityToggleBtn.classList.toggle("active", density === "compact");
  localStorage.setItem(DENSITY_STORAGE_KEY, density);
}

densityToggleBtn.addEventListener("click", () => {
  const next = document.body.classList.contains("density-compact") ? "comfortable" : "compact";
  applyDensity(next);
});

applyDensity(localStorage.getItem(DENSITY_STORAGE_KEY) || "comfortable");

// ---- Toasts + notification history ----

let persistentUnreadCount = 0;
let persistentNotifications = [];
const selectedNotificationIds = new Set();

function renderNotificationRows(notifications, emptyMessage) {
  if (!notifications.length) return `<div class="dropdown-empty">${emptyMessage}</div>`;
  return notifications
    .map(
      (n) => `
    <div class="notification-row ${n.read ? "" : "notification-unread"}">
      <input type="checkbox" class="notification-select-checkbox" data-notification-id="${n.id}" ${selectedNotificationIds.has(n.id) ? "checked" : ""} />
      <i class="dot dot-${
        n.kind === "breaker_tripped" || n.kind === "schedule_failed"
          ? "error"
          : n.kind === "schedule_once_fired"
          ? "ready"
          : "running"
      }"></i>
      <div>
        <span class="notification-message">${escapeHtml(n.message)}</span>
        <span class="notification-time">${new Date(n.created_at).toLocaleString()}</span>
      </div>
    </div>`
    )
    .join("");
}

function updateNotificationBulkBtns() {
  const markReadBtn = document.getElementById("notifications-bulk-mark-read-btn");
  const deleteBtn = document.getElementById("notifications-bulk-delete-selected-btn");
  if (markReadBtn) markReadBtn.disabled = selectedNotificationIds.size === 0;
  if (deleteBtn) deleteBtn.disabled = selectedNotificationIds.size === 0;
}

function wireNotificationCheckboxes(container) {
  container.querySelectorAll(".notification-select-checkbox").forEach((checkbox) => {
    checkbox.addEventListener("click", (e) => e.stopPropagation());
    checkbox.addEventListener("change", () => {
      const id = Number(checkbox.dataset.notificationId);
      if (checkbox.checked) selectedNotificationIds.add(id);
      else selectedNotificationIds.delete(id);
      updateNotificationBulkBtns();
    });
  });
  updateNotificationBulkBtns();
}

function wireNotificationBulkControls(container) {
  wireNotificationCheckboxes(container);
  const markReadBtn = container.querySelector("#notifications-bulk-mark-read-btn");
  const deleteBtn = container.querySelector("#notifications-bulk-delete-selected-btn");
  const selectAllEl = container.querySelector("#notifications-select-all");

  selectAllEl?.addEventListener("click", (e) => e.stopPropagation());
  selectAllEl?.addEventListener("change", () => {
    const checkboxes = container.querySelectorAll(".notification-select-checkbox");
    checkboxes.forEach((checkbox) => {
      checkbox.checked = selectAllEl.checked;
      const id = Number(checkbox.dataset.notificationId);
      if (selectAllEl.checked) selectedNotificationIds.add(id);
      else selectedNotificationIds.delete(id);
    });
    updateNotificationBulkBtns();
  });

  markReadBtn?.addEventListener("click", async (e) => {
    e.stopPropagation();
    if (!selectedNotificationIds.size) return;
    await fetch("/api/notifications/bulk-mark-read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notification_ids: [...selectedNotificationIds] }),
    });
    selectedNotificationIds.clear();
    const res = await fetch("/api/notifications");
    persistentNotifications = await res.json();
    renderNotificationsPanel();
  });

  deleteBtn?.addEventListener("click", async (e) => {
    e.stopPropagation();
    if (!selectedNotificationIds.size) return;
    await fetch("/api/notifications/bulk-delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notification_ids: [...selectedNotificationIds] }),
    });
    selectedNotificationIds.clear();
    const res = await fetch("/api/notifications");
    persistentNotifications = await res.json();
    renderNotificationsPanel();
  });
}

function updateNotifBadge() {
  const total = unreadNotifications + persistentUnreadCount;
  if (total > 0) {
    notifBadge.textContent = total > 9 ? "9+" : String(total);
    notifBadge.classList.remove("hidden");
  } else {
    notifBadge.classList.add("hidden");
  }
}

async function refreshUnreadNotificationCount() {
  try {
    const res = await fetch("/api/notifications/unread-count");
    const body = await res.json();
    persistentUnreadCount = body.count;
    updateNotifBadge();
  } catch (err) {
    // best-effort — a failed poll just leaves the badge stale until the next one
  }
}

let notificationPreferences = {};
const NOTIFICATION_KIND_LABELS = {
  breaker_tripped: "Circuit breaker trips",
  schedule_failed: "Scheduled run failures",
  resource_alert: "Resource usage alerts",
  schedule_once_fired: "One-time schedule fired",
};

function renderNotificationsPanel() {
  const preferencesHtml = Object.keys(NOTIFICATION_KIND_LABELS)
    .map(
      (kind) => `
      <label class="notification-pref-row">
        <input type="checkbox" class="notification-mute-toggle" data-kind="${kind}" ${notificationPreferences[kind] ? "" : "checked"} />
        ${NOTIFICATION_KIND_LABELS[kind]}
      </label>`
    )
    .join("");

  const alertsHtml = renderNotificationRows(persistentNotifications, "No alerts yet.");

  const activityHtml = toastHistory.length
    ? toastHistory
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
        .join("")
    : `<div class="dropdown-empty">Nothing yet — run something.</div>`;

  const hasReadAlerts = persistentNotifications.some((n) => n.read);

  notificationsPanel.innerHTML = `
    <div class="notification-section-head">
      <h4>Alerts</h4>
      <div class="notification-section-actions">
        <a class="btn btn-secondary btn-small" href="/api/notifications.csv" download>Export CSV</a>
        ${hasReadAlerts ? `<button class="btn btn-secondary btn-small" id="notifications-clear-read-btn" type="button">Clear read</button>` : ""}
      </div>
    </div>
    <input type="search" id="notifications-search-input" class="artifact-search-input" placeholder="Search alerts…" />
    <div class="notification-section-actions" style="padding: 4px 0;">
      <label class="schedule-select-all-label">
        <input type="checkbox" id="notifications-select-all" ${persistentNotifications.length && persistentNotifications.every((n) => selectedNotificationIds.has(n.id)) ? "checked" : ""} />
        Select all
      </label>
      <button class="btn btn-secondary btn-small" id="notifications-bulk-mark-read-btn" type="button" disabled>Mark selected read</button>
      <button class="btn btn-secondary btn-small" id="notifications-bulk-delete-selected-btn" type="button" disabled>Delete selected</button>
    </div>
    <div id="notifications-alerts-list">${alertsHtml}</div>
    <h4>Recent activity</h4>
    ${activityHtml}
    <div class="notification-section-head">
      <h4>Preferences</h4>
      <div class="notification-section-actions">
        <button class="btn btn-secondary btn-small" id="notifications-mute-all-btn" type="button">Mute all</button>
        <button class="btn btn-secondary btn-small" id="notifications-unmute-all-btn" type="button">Unmute all</button>
      </div>
    </div>
    ${preferencesHtml}`;

  notificationsPanel.querySelector("#notifications-mute-all-btn")?.addEventListener("click", async (e) => {
    e.stopPropagation();
    await setAllNotificationPreferences(true);
  });
  notificationsPanel.querySelector("#notifications-unmute-all-btn")?.addEventListener("click", async (e) => {
    e.stopPropagation();
    await setAllNotificationPreferences(false);
  });

  let notificationSearchDebounce = null;
  notificationsPanel.querySelector("#notifications-search-input")?.addEventListener("input", (e) => {
    e.stopPropagation();
    const query = e.target.value.trim();
    const listEl = notificationsPanel.querySelector("#notifications-alerts-list");
    clearTimeout(notificationSearchDebounce);
    notificationSearchDebounce = setTimeout(async () => {
      if (!query) {
        listEl.innerHTML = renderNotificationRows(persistentNotifications, "No alerts yet.");
        wireNotificationCheckboxes(listEl);
        return;
      }
      try {
        const res = await fetch(`/api/notifications/search?q=${encodeURIComponent(query)}`);
        const body = await res.json();
        listEl.innerHTML = renderNotificationRows(body.results, "No alerts match that search.");
        wireNotificationCheckboxes(listEl);
      } catch (err) {
        listEl.innerHTML = `<div class="dropdown-empty">Search failed: ${err}</div>`;
      }
    }, 250);
  });

  notificationsPanel.querySelector("#notifications-clear-read-btn")?.addEventListener("click", async (e) => {
    e.stopPropagation();
    try {
      await fetch("/api/notifications/clear-read", { method: "POST" });
      const res = await fetch("/api/notifications");
      persistentNotifications = await res.json();
      renderNotificationsPanel();
    } catch (err) {
      showToast(`Could not clear read notifications: ${err}`, "error");
    }
  });

  notificationsPanel.querySelectorAll(".notification-mute-toggle").forEach((toggle) => {
    toggle.addEventListener("click", async (e) => {
      e.stopPropagation();
      const kind = toggle.dataset.kind;
      const muted = !toggle.checked;
      notificationPreferences[kind] = muted;
      try {
        await fetch(`/api/notifications/preferences/${kind}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ muted }),
        });
      } catch (err) {
        showToast(`Could not save notification preference: ${err}`, "error");
      }
    });
  });

  wireNotificationBulkControls(notificationsPanel);
}

async function setAllNotificationPreferences(muted) {
  try {
    notificationPreferences = await (
      await fetch("/api/notifications/preferences", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ muted }),
      })
    ).json();
    renderNotificationsPanel();
    showToast(muted ? "Muted all notification kinds." : "Unmuted all notification kinds.", "success");
  } catch (err) {
    showToast(`Could not update notification preferences: ${err}`, "error");
  }
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

notificationsBtn.addEventListener("click", async () => {
  const opening = notificationsPanel.classList.contains("hidden");
  notificationsPanel.classList.toggle("hidden");
  healthPanel.classList.add("hidden");
  selfTestPanel.classList.add("hidden");
  if (opening) {
    try {
      const [notifRes, prefRes] = await Promise.all([
        fetch("/api/notifications"),
        fetch("/api/notifications/preferences"),
      ]);
      persistentNotifications = await notifRes.json();
      notificationPreferences = await prefRes.json();
      renderNotificationsPanel();
    } catch (err) {
      // best-effort — the panel just keeps whatever it last rendered
    }
    unreadNotifications = 0;
    if (persistentUnreadCount > 0) {
      fetch("/api/notifications/mark-all-read", { method: "POST" }).catch(() => {});
      persistentUnreadCount = 0;
    }
    updateNotifBadge();
  }
});

refreshUnreadNotificationCount();
setInterval(refreshUnreadNotificationCount, 5000);

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
        .join("")}
      <div class="health-panel-footer">
        <a class="btn btn-secondary btn-small" href="/api/backup/export" download>Export JSON backup</a>
        <a class="btn btn-secondary btn-small" href="/api/backup/db" download>Download .db file</a>
      </div>
      <div class="health-panel-footer">
        <label class="btn btn-secondary btn-small restore-backup-label" for="restore-backup-input">Restore backup…</label>
        <input type="file" id="restore-backup-input" accept=".json" hidden />
      </div>`;

    document.getElementById("restore-backup-input").addEventListener("change", async (e) => {
      const file = e.target.files[0];
      if (!file) return;
      const formData = new FormData();
      formData.append("file", file);
      try {
        const res = await fetch("/api/backup/restore", { method: "POST", body: formData });
        const body = await res.json();
        if (!res.ok) {
          showToast(`Restore failed: ${body.detail || "unknown error"}`, "error");
          return;
        }
        const parts = Object.entries(body)
          .filter(([, count]) => count > 0)
          .map(([kind, count]) => `${count} ${kind}`);
        showToast(parts.length ? `Restored: ${parts.join(", ")}.` : "Nothing new to restore — already up to date.", "success");
        await refreshTelemetry();
        await loadMemory();
        await loadSchedules();
        await loadSavedPipelines();
        await loadAuditLog();
      } catch (err) {
        showToast(`Restore failed: ${err}`, "error");
      }
      e.target.value = "";
    });
  } catch (err) {
    healthLabel.textContent = "Unreachable";
    healthBeacon.classList.add("degraded");
  }
}

healthBeacon.addEventListener("click", () => {
  healthPanel.classList.toggle("hidden");
  notificationsPanel.classList.add("hidden");
  selfTestPanel.classList.add("hidden");
});

document.addEventListener("click", (e) => {
  if (!e.target.closest(".dropdown-wrap")) {
    notificationsPanel.classList.add("hidden");
    healthPanel.classList.add("hidden");
    selfTestPanel.classList.add("hidden");
  }
});

// ---- One-click module self-test ----

function renderSelfTestResults(body) {
  const STATUS_DOT = { pass: "dot-ready", fail: "dot-error", skipped: "dot-running" };
  selfTestPanel.innerHTML = `
    <h4>Module Self-Test</h4>
    <p class="dropdown-empty" style="padding: 0 0 8px;">
      ${body.passed} passed · ${body.failed} failed · ${body.skipped} skipped
    </p>
    ${body.results
      .map(
        (r) => `
      <div class="health-check-row">
        <i class="dot ${STATUS_DOT[r.status] || "dot-error"}"></i>
        <div>
          <span class="health-check-name">[${escapeHtml(r.tier)}] ${escapeHtml(r.name)}</span>
          <span class="health-check-detail">${
            r.status === "pass"
              ? `passed in ${r.duration_seconds}s`
              : r.status === "skipped"
              ? "skipped — module disabled"
              : `failed in ${r.duration_seconds}s: ${escapeHtml(r.detail || "")}`
          }</span>
        </div>
      </div>`
      )
      .join("")}`;
}

selfTestBtn.addEventListener("click", async () => {
  const opening = selfTestPanel.classList.contains("hidden");
  notificationsPanel.classList.add("hidden");
  healthPanel.classList.add("hidden");
  selfTestPanel.classList.toggle("hidden");
  if (!opening) return;

  selfTestPanel.innerHTML = `<h4>Module Self-Test</h4><div class="dropdown-empty">Running every enabled module once…</div>`;
  selfTestBtn.disabled = true;
  try {
    const res = await fetch("/api/self-test", { method: "POST" });
    const body = await res.json();
    renderSelfTestResults(body);
    showToast(`Self-test: ${body.passed} passed, ${body.failed} failed, ${body.skipped} skipped.`, body.failed ? "error" : "success");
  } catch (err) {
    selfTestPanel.innerHTML = `<h4>Module Self-Test</h4><p class="dropdown-empty">Self-test failed: ${err}</p>`;
  } finally {
    selfTestBtn.disabled = false;
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

const selectedRunIds = new Set();

function updateBulkDeleteButton() {
  runsBulkDeleteBtn.disabled = selectedRunIds.size === 0;
  runsBulkDeleteBtn.textContent = selectedRunIds.size ? `Delete selected (${selectedRunIds.size})` : "Delete selected";
}

async function loadRecentRuns() {
  const res = await fetch("/api/runs?limit=15");
  const runs = await res.json();

  const liveIds = new Set(runs.map((r) => r.id));
  [...selectedRunIds].forEach((id) => {
    if (!liveIds.has(id)) selectedRunIds.delete(id);
  });

  if (!runs.length) {
    recentRunsTableEl.innerHTML = `<div class="runs-empty">No runs yet — click any Run button below.</div>`;
    populateCompareSelects([]);
    updateBulkDeleteButton();
    return;
  }

  const rows = runs
    .map((r) => {
      const duration =
        r.started_at && r.finished_at
          ? `${((new Date(r.finished_at) - new Date(r.started_at)) / 1000).toFixed(2)}s`
          : "–";
      const checked = selectedRunIds.has(r.id) ? "checked" : "";
      return `
        <tr class="history-row" data-run-id="${r.id}" data-status="${r.status}">
          <td class="run-select-cell"><input type="checkbox" class="run-select-checkbox" data-run-id="${r.id}" ${checked} /></td>
          <td><span class="run-expand-chevron">▸</span> #${r.id} <span class="run-note-indicator" title="This run has a note">${r.note ? "📝" : ""}</span></td>
          <td class="status-${r.status}">${r.status}</td>
          <td>${new Date(r.started_at).toLocaleString()}</td>
          <td>${duration}</td>
        </tr>
        <tr class="run-detail-row hidden" data-run-id="${r.id}">
          <td colspan="5"></td>
        </tr>`;
    })
    .join("");

  const allSelected = runs.every((r) => selectedRunIds.has(r.id));
  recentRunsTableEl.innerHTML = `
    <table>
      <thead><tr>
        <th class="run-select-cell"><input type="checkbox" id="run-select-all" ${allSelected ? "checked" : ""} /></th>
        <th>Run</th><th>Status</th><th>Started</th><th>Duration</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;

  recentRunsTableEl.querySelectorAll(".history-row").forEach((row) => {
    row.addEventListener("click", (e) => {
      if (e.target.closest(".run-select-cell")) return;
      toggleRunDetail(row.dataset.runId);
    });
  });

  recentRunsTableEl.querySelectorAll(".run-select-checkbox").forEach((checkbox) => {
    checkbox.addEventListener("click", (e) => e.stopPropagation());
    checkbox.addEventListener("change", () => {
      const runId = Number(checkbox.dataset.runId);
      if (checkbox.checked) selectedRunIds.add(runId);
      else selectedRunIds.delete(runId);
      updateBulkDeleteButton();
      const selectAllCheckbox = document.getElementById("run-select-all");
      if (selectAllCheckbox) selectAllCheckbox.checked = runs.every((r) => selectedRunIds.has(r.id));
    });
  });

  const selectAllCheckbox = document.getElementById("run-select-all");
  selectAllCheckbox.addEventListener("click", (e) => e.stopPropagation());
  selectAllCheckbox.addEventListener("change", () => {
    if (selectAllCheckbox.checked) runs.forEach((r) => selectedRunIds.add(r.id));
    else runs.forEach((r) => selectedRunIds.delete(r.id));
    loadRecentRuns();
  });

  updateBulkDeleteButton();

  populateCompareSelects(runs);
  renderRunsTrend(runs);
  applyRunStatusFilter();
}

// ---- Quick status-filter chips for Recent Runs ----
let currentRunStatusFilter = "all";

function applyRunStatusFilter() {
  recentRunsTableEl.querySelectorAll(".history-row").forEach((row) => {
    const matches = currentRunStatusFilter === "all" || row.dataset.status === currentRunStatusFilter;
    row.classList.toggle("status-filter-hidden", !matches);
  });
}

document.querySelectorAll("[data-run-status-filter]").forEach((chip) => {
  chip.addEventListener("click", () => {
    currentRunStatusFilter = chip.dataset.runStatusFilter;
    document.querySelectorAll("[data-run-status-filter]").forEach((c) => c.classList.toggle("active", c === chip));
    applyRunStatusFilter();
  });
});

// ---- Run history trend sparkline ----
// One bar per run (oldest -> newest, left to right): height encodes duration,
// color encodes status (the app's existing reserved status colors — never
// invented per-chart). A hover tooltip and the legend below carry the values
// a glance can't, so nothing here is color-only.
const STATUS_TREND_COLOR = { completed: "var(--ready)", failed: "var(--error)", running: "var(--running)" };

function runDurationSeconds(run) {
  if (!run.started_at || !run.finished_at) return null;
  return (new Date(run.finished_at) - new Date(run.started_at)) / 1000;
}

function renderRunsTrend(runs) {
  const trendEl = document.getElementById("runs-trend");
  if (!trendEl) return;
  if (!runs.length) {
    trendEl.innerHTML = "";
    return;
  }

  // API returns newest-first; a trend reads left (older) -> right (newer).
  const ordered = [...runs].reverse();
  const durations = ordered.map(runDurationSeconds);
  const maxDuration = Math.max(0.05, ...durations.filter((d) => d != null));

  const plotHeight = 32;
  const svgHeight = 40;
  const gap = 2;
  const containerWidth = trendEl.clientWidth || ordered.length * 14;
  const barWidth = Math.max(3, Math.min(24, Math.floor((containerWidth - (ordered.length - 1) * gap) / ordered.length)));
  const svgWidth = ordered.length * barWidth + (ordered.length - 1) * gap;

  const bars = ordered
    .map((run, i) => {
      const duration = durations[i];
      const barHeight = duration == null ? 4 : Math.max(3, Math.round((duration / maxDuration) * plotHeight));
      const x = i * (barWidth + gap);
      const yTop = plotHeight - barHeight;
      const r = Math.min(4, barHeight / 2, barWidth / 2);
      const color = STATUS_TREND_COLOR[run.status] || "var(--text-dim)";
      const path =
        `M ${x} ${yTop + r} ` +
        `Q ${x} ${yTop} ${x + r} ${yTop} ` +
        `L ${x + barWidth - r} ${yTop} ` +
        `Q ${x + barWidth} ${yTop} ${x + barWidth} ${yTop + r} ` +
        `L ${x + barWidth} ${plotHeight} ` +
        `L ${x} ${plotHeight} Z`;
      const durationLabel = duration == null ? "in progress" : `${duration.toFixed(2)}s`;
      return `<path class="trend-bar" d="${path}" fill="${color}"
        data-run-id="${run.id}" data-status="${escapeHtml(run.status)}"
        data-duration="${escapeHtml(durationLabel)}" data-started="${escapeHtml(run.started_at || "")}"></path>`;
    })
    .join("");

  trendEl.innerHTML = `
    <div class="runs-trend-chart-wrap">
      <svg class="runs-trend-svg" viewBox="0 0 ${svgWidth} ${svgHeight}" width="${svgWidth}" height="${svgHeight}" preserveAspectRatio="xMinYMax meet">
        ${bars}
      </svg>
    </div>
    <div class="runs-trend-legend">
      <span class="dot dot-ready"></span><span>Success</span>
      <span class="dot dot-running"></span><span>Running</span>
      <span class="dot dot-error"></span><span>Failed</span>
    </div>
    <div class="runs-trend-tooltip hidden" id="runs-trend-tooltip"></div>`;

  const tooltip = document.getElementById("runs-trend-tooltip");
  trendEl.querySelectorAll(".trend-bar").forEach((bar) => {
    bar.addEventListener("pointerenter", (e) => showTrendTooltip(e, bar, tooltip));
    bar.addEventListener("pointermove", (e) => showTrendTooltip(e, bar, tooltip));
    bar.addEventListener("pointerleave", () => tooltip.classList.add("hidden"));
  });
}

function showTrendTooltip(event, bar, tooltip) {
  const { runId, status, duration, started } = bar.dataset;
  const startedLabel = started ? new Date(started).toLocaleString() : "–";
  tooltip.innerHTML = `
    <strong>${escapeHtml(duration)}</strong>
    <span>Run #${escapeHtml(runId)} · ${escapeHtml(status)}</span>
    <span>${escapeHtml(startedLabel)}</span>`;
  tooltip.classList.remove("hidden");
  const wrapRect = tooltip.parentElement.getBoundingClientRect();
  tooltip.style.left = `${event.clientX - wrapRect.left + 12}px`;
  tooltip.style.top = `${event.clientY - wrapRect.top - 10}px`;
}

// ---- Run detail drill-down ----
// Steps recorded against a run are immutable once the run finishes, so
// caching by run id never goes stale — only a purge removes the row
// entirely, at which point there's nothing left to look up anyway.
const runDetailCache = {};

function renderBlackboardSection(blackboard) {
  if (!blackboard || !blackboard.length) return "";
  const entriesHtml = blackboard
    .map(
      (entry) => `
      <div class="blackboard-entry">
        <div class="blackboard-entry-head">
          <strong>${escapeHtml(entry.author)}</strong>
          <span class="run-detail-step-timing">${new Date(entry.at).toLocaleTimeString()}</span>
        </div>
        <p class="card-desc">${escapeHtml(entry.note)}</p>
      </div>`
    )
    .join("");

  return `
    <div class="blackboard-section">
      <h4 class="blackboard-title">🗒 Shared Agent Blackboard</h4>
      <p class="card-desc">Notes any agent in this run posted for any other agent to read — not addressed to a specific recipient.</p>
      ${entriesHtml}
    </div>`;
}

function renderRunDetailSteps(runId, steps, blackboard, note) {
  if (!steps.length) return `<div class="runs-empty">No recorded steps for this run.</div>`;
  const stepsHtml = steps
    .map(
      (s, i) => `
      <div class="run-detail-step ${s.success ? "" : "run-detail-step-failed"}">
        <div class="run-detail-step-head">
          <strong>Step ${i + 1}: [${s.tier}] ${s.name}</strong>
          <span class="run-detail-step-status">${s.success ? "✅ success" : "❌ failed"}</span>
          <span class="run-detail-step-timing">${new Date(s.started_at).toLocaleTimeString()} → ${new Date(s.finished_at).toLocaleTimeString()}</span>
        </div>
        ${s.error ? `<div class="run-detail-step-error">${escapeHtml(s.error)}</div>` : ""}
        <pre class="run-detail-step-output">${escapeHtml(JSON.stringify(s.output, null, 2))}</pre>
      </div>`
    )
    .join("");

  return `
    <div class="run-detail-toolbar">
      <button class="btn btn-secondary btn-small rerun-btn" data-run-id="${runId}" type="button">↻ Re-run with these inputs</button>
      <a class="btn btn-secondary btn-small" href="/api/runs/${runId}.json" download title="Download this run's full detail as JSON">⬇ Download JSON</a>
    </div>
    <div class="run-note-row">
      <label for="run-note__${runId}">Note</label>
      <textarea id="run-note__${runId}" class="run-note-input" data-run-id="${runId}" placeholder="Add a note for your own future reference…" rows="2">${escapeHtml(note || "")}</textarea>
      <button class="btn btn-secondary btn-small run-note-save-btn" data-run-id="${runId}" type="button">Save note</button>
    </div>
    ${stepsHtml}
    ${renderBlackboardSection(blackboard)}
    <div class="tracker hidden rerun-tracker" data-run-id="${runId}"></div>
    <div class="log-tabs-wrap hidden rerun-log" data-run-id="${runId}"></div>`;
}

async function rerunHistoricalRun(runId, steps) {
  const btn = recentRunsTableEl.querySelector(`.rerun-btn[data-run-id="${runId}"]`);
  const tracker = recentRunsTableEl.querySelector(`.rerun-tracker[data-run-id="${runId}"]`);
  const logWrap = recentRunsTableEl.querySelector(`.rerun-log[data-run-id="${runId}"]`);
  if (!btn || !tracker || !logWrap) return;

  btn.disabled = true;
  btn.textContent = "Launching...";

  try {
    const res = await fetch(`/api/runs/${runId}/rerun`, { method: "POST" });
    const body = await res.json();
    if (!res.ok) {
      showToast(`Re-run failed: ${body.detail || "unknown error"}`, "error");
      btn.disabled = false;
      btn.textContent = "↻ Re-run with these inputs";
      return;
    }

    tracker.classList.remove("hidden");
    logWrap.classList.remove("hidden");
    const trackerSteps = steps.map((s) => ({ tier: s.tier, name: s.name }));
    renderTracker(tracker, trackerSteps);
    const log = createRunLog();
    renderRunLogTabs(logWrap, log, null);

    subscribeToStream(body.stream_id, {
      onEvent: (event) => {
        handleTrackerEvent(tracker, event);
        logSystemEvent(log, event);
        renderRunLogTabs(logWrap, log, null);
      },
      onDone: async (event) => {
        const success = event.kind === "run_completed";
        renderRunLogTabs(logWrap, log, event.context ?? {});
        showToast(success ? "Re-run completed." : `Re-run failed: ${event.error}`, success ? "success" : "error");

        // Rebuilding the table collapses whatever row was expanded (this one
        // included) — re-open the fresh replay's own row afterward so the
        // result the user was just watching doesn't just vanish.
        await loadRecentRuns();
        await refreshTelemetry();
        const runs = await (await fetch("/api/runs?limit=1")).json();
        if (runs.length) await toggleRunDetail(String(runs[0].id));
      },
    });
  } catch (err) {
    btn.disabled = false;
    btn.textContent = "↻ Re-run with these inputs";
    showToast(`Re-run failed: ${err}`, "error");
  }
}

async function toggleRunDetail(runId) {
  const detailRow = recentRunsTableEl.querySelector(`.run-detail-row[data-run-id="${runId}"]`);
  const mainRow = recentRunsTableEl.querySelector(`.history-row[data-run-id="${runId}"]`);
  if (!detailRow || !mainRow) return;

  const alreadyOpen = !detailRow.classList.contains("hidden");

  // Only one run's detail open at a time — keeps the table from growing
  // unboundedly tall as someone clicks through several runs.
  recentRunsTableEl.querySelectorAll(".run-detail-row").forEach((row) => row.classList.add("hidden"));
  recentRunsTableEl.querySelectorAll(".history-row").forEach((row) => row.classList.remove("expanded"));

  if (alreadyOpen) return;

  detailRow.classList.remove("hidden");
  mainRow.classList.add("expanded");
  const cell = detailRow.querySelector("td");

  if (!runDetailCache[runId]) {
    cell.innerHTML = `<div class="runs-empty">Loading step detail…</div>`;
    try {
      const res = await fetch(`/api/runs/${runId}`);
      const body = await res.json();
      if (!res.ok) {
        cell.innerHTML = `<div class="runs-empty">${escapeHtml(body.detail || "Could not load run detail.")}</div>`;
        return;
      }
      runDetailCache[runId] = { steps: body.steps, blackboard: body.blackboard || [], note: body.note || "" };
    } catch (err) {
      cell.innerHTML = `<div class="runs-empty">Failed to load run detail: ${err}</div>`;
      return;
    }
  }
  cell.innerHTML = renderRunDetailSteps(
    runId,
    runDetailCache[runId].steps,
    runDetailCache[runId].blackboard,
    runDetailCache[runId].note
  );
  cell.querySelector(".rerun-btn").addEventListener("click", () => rerunHistoricalRun(runId, runDetailCache[runId].steps));
  cell.querySelector(".run-note-save-btn").addEventListener("click", async () => {
    const textarea = cell.querySelector(`.run-note-input[data-run-id="${runId}"]`);
    const note = textarea.value.trim();
    try {
      const res = await fetch(`/api/runs/${runId}/note`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ note }),
      });
      const body = await res.json();
      if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
      runDetailCache[runId].note = body.note;
      showToast(body.note ? "Note saved." : "Note cleared.", "success");
      const mainRow = recentRunsTableEl.querySelector(`.history-row[data-run-id="${runId}"]`);
      const indicator = mainRow?.querySelector(".run-note-indicator");
      if (indicator) indicator.textContent = body.note ? "📝" : "";
    } catch (err) {
      showToast(`Could not save note: ${err}`, "error");
    }
  });
}

// ---- Run comparison ----

const compareRunAEl = document.getElementById("compare-run-a");
const compareRunBEl = document.getElementById("compare-run-b");
const compareRunsBtn = document.getElementById("compare-runs-btn");
const runCompareResultEl = document.getElementById("run-compare-result");

function populateCompareSelects(runs) {
  const options = runs
    .map((r) => `<option value="${r.id}">#${r.id} — ${r.status} — ${new Date(r.started_at).toLocaleString()}</option>`)
    .join("");
  const previousA = compareRunAEl.value;
  const previousB = compareRunBEl.value;
  compareRunAEl.innerHTML = options;
  compareRunBEl.innerHTML = options;
  if (runs.some((r) => String(r.id) === previousA)) compareRunAEl.value = previousA;
  if (runs.some((r) => String(r.id) === previousB)) compareRunBEl.value = previousB;
  else if (runs.length > 1) compareRunBEl.value = String(runs[1].id);
}

compareRunsBtn.addEventListener("click", async () => {
  const a = compareRunAEl.value;
  const b = compareRunBEl.value;
  if (!a || !b) return;

  runCompareResultEl.classList.remove("hidden");
  if (a === b) {
    runCompareResultEl.innerHTML = `<div class="runs-empty">Pick two different runs to compare.</div>`;
    return;
  }

  const res = await fetch(`/api/runs/compare?a=${a}&b=${b}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    runCompareResultEl.innerHTML = `<div class="runs-empty">${escapeHtml(body.detail || "Could not compare these runs.")}</div>`;
    return;
  }

  const body = await res.json();
  runCompareResultEl.innerHTML = body.steps
    .map((step) => {
      const nameA = step.a ? `${step.a.name} (${step.a.success ? "ok" : "failed"})` : "—";
      const nameB = step.b ? `${step.b.name} (${step.b.success ? "ok" : "failed"})` : "—";
      const diffKeys = Object.keys(step.output_diff);
      const diffRows = diffKeys.length
        ? diffKeys
            .map(
              (key) => `
              <div class="schedule-row">
                <div class="schedule-row-main">
                  <strong>${escapeHtml(key)}</strong>
                  <span class="schedule-row-meta">A: ${escapeHtml(JSON.stringify(step.output_diff[key].a))} · B: ${escapeHtml(JSON.stringify(step.output_diff[key].b))}</span>
                </div>
              </div>`
            )
            .join("")
        : `<div class="schedule-row"><div class="schedule-row-main"><span class="schedule-row-meta">No output differences.</span></div></div>`;

      return `
        <div class="run-compare-step">
          <h4>Step ${step.index + 1}: ${escapeHtml(nameA)} vs ${escapeHtml(nameB)}</h4>
          ${diffRows}
        </div>`;
    })
    .join("");
});

function formatUptime(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

// Reasonable defaults for a single lightweight local process — not user
// configurable (yet); revisit if this app's own baseline usage ever changes.
const CPU_WARN_PERCENT = 80;
const MEMORY_WARN_MB = 500;
let resourceWarningActive = false;

async function loadPerformance() {
  const res = await fetch("/api/performance");
  const p = await res.json();
  perfCpuEl.textContent = `${p.cpu_percent.toFixed(1)}%`;
  perfMemEl.textContent = `${p.memory_rss_mb} MB`;
  perfThreadsEl.textContent = p.active_run_threads > 0 ? `${p.thread_count} (${p.active_run_threads} running)` : p.thread_count;
  perfUptimeEl.textContent = formatUptime(p.uptime_seconds);

  const cpuOver = p.cpu_percent >= CPU_WARN_PERCENT;
  const memOver = p.memory_rss_mb >= MEMORY_WARN_MB;
  perfCpuEl.classList.toggle("metric-warn", cpuOver);
  perfMemEl.classList.toggle("metric-warn", memOver);

  const isOver = cpuOver || memOver;
  if (isOver) {
    const reasons = [];
    if (cpuOver) reasons.push(`CPU at ${p.cpu_percent.toFixed(1)}%`);
    if (memOver) reasons.push(`memory at ${p.memory_rss_mb} MB`);
    resourceWarningBannerEl.textContent = `⚠ High resource usage — ${reasons.join(", ")}.`;
    resourceWarningBannerEl.classList.remove("hidden");
  } else {
    resourceWarningBannerEl.classList.add("hidden");
  }

  if (isOver && !resourceWarningActive) {
    showToast("Resource usage is unusually high — see the banner below the header.", "error");
    fetch("/api/notifications", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ kind: "resource_alert", message: `High resource usage — ${reasons.join(", ")}.` }),
    })
      .then(() => refreshUnreadNotificationCount())
      .catch(() => {});
  }
  resourceWarningActive = isOver;
}

async function refreshTelemetry() {
  await Promise.all([loadMetrics(), loadRecentRuns(), loadPerformance()]);
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
  const actionLabel = entry.action === "view" ? "View" : "Run";
  return `
    <div class="card favorite-chip">
      <div class="card-head">
        <h3 class="card-title">${entry.label}</h3>
        ${favoriteButtonHtml(entry.key)}
      </div>
      <p class="card-desc">${entry.description || ""}</p>
      <button class="btn btn-run" data-fav-run="${entry.key}" type="button">${actionLabel}</button>
    </div>`;
}

function renderFavoritesSection() {
  const entries = [];
  for (const key of favoriteKeys) {
    if (key.startsWith("module::")) {
      const [, tier, name] = key.split("::");
      const module = (currentModulesByTier[tier] || []).find((m) => m.name === name);
      if (module) entries.push({ key, label: module.name, description: module.description, action: "run" });
    } else if (key.startsWith("pipeline::")) {
      const slug = key.slice("pipeline::".length);
      const pipeline = currentPipelines.find((p) => p.slug === slug);
      if (pipeline) entries.push({ key, label: pipeline.name, description: pipeline.description, action: "run" });
    } else if (key.startsWith("schedule::")) {
      const id = Number(key.slice("schedule::".length));
      const schedule = cachedSchedules.find((s) => s.id === id);
      if (schedule) {
        const label = schedule.kind === "module" ? `[${schedule.tier}] ${schedule.name}` : `pipeline: ${schedule.name}`;
        const cadenceDesc =
          schedule.schedule_type === "daily"
            ? `daily at ${schedule.daily_time}`
            : schedule.schedule_type === "weekly"
              ? `weekly at ${schedule.daily_time}`
              : schedule.schedule_type === "once"
                ? "one-time schedule"
                : `every ${formatInterval(schedule.interval_seconds)}`;
        entries.push({ key, label, description: cadenceDesc, action: "view" });
      }
    } else if (key.startsWith("artifact::")) {
      const filename = key.slice("artifact::".length);
      const artifact = currentArtifacts.find((f) => f.name === filename);
      if (artifact) entries.push({ key, label: artifact.name, description: formatBytes(artifact.size_bytes), action: "view" });
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

const favoritesClearAllBtn = document.getElementById("favorites-clear-all-btn");
favoritesClearAllBtn.addEventListener("click", () => {
  if (!favoriteKeys.size) return;
  favoriteKeys.clear();
  saveFavoriteKeys();
  renderFavoritesSection();
  renderRecentlyViewedSection();
  showToast("Cleared all favorites.", "success");
});

function runFavorite(key) {
  if (key.startsWith("module::")) {
    const [, tier, name] = key.split("::");
    runModule(tier, name);
  } else if (key.startsWith("pipeline::")) {
    const slug = key.slice("pipeline::".length);
    const pipeline = currentPipelines.find((p) => p.slug === slug);
    if (pipeline) runSavedPipeline(slug, pipeline.name, currentPipelines);
  } else if (key.startsWith("schedule::")) {
    const id = key.slice("schedule::".length);
    const row = document.getElementById(`schedule-row__${id}`);
    if (row) {
      row.scrollIntoView({ behavior: "smooth", block: "center" });
      row.classList.add("jump-highlight");
      setTimeout(() => row.classList.remove("jump-highlight"), 1500);
    }
  } else if (key.startsWith("artifact::")) {
    const filename = key.slice("artifact::".length);
    artifactsListEl.scrollIntoView({ behavior: "smooth", block: "center" });
    toggleArtifactContent(filename);
  }
}

// ---- Recently viewed (implicit, recency-based, capped) ----

function loadRecentlyViewedKeys() {
  try {
    const parsed = JSON.parse(localStorage.getItem(RECENTLY_VIEWED_STORAGE_KEY) || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

let recentlyViewedKeys = loadRecentlyViewedKeys();

function saveRecentlyViewedKeys() {
  localStorage.setItem(RECENTLY_VIEWED_STORAGE_KEY, JSON.stringify(recentlyViewedKeys));
}

function recordRecentlyViewed(key) {
  recentlyViewedKeys = recentlyViewedKeys.filter((k) => k !== key);
  recentlyViewedKeys.unshift(key);
  recentlyViewedKeys = recentlyViewedKeys.slice(0, RECENTLY_VIEWED_MAX);
  saveRecentlyViewedKeys();
  renderRecentlyViewedSection();
}

function renderRecentlyViewedSection() {
  const entries = [];
  for (const key of recentlyViewedKeys) {
    if (key.startsWith("module::")) {
      const [, tier, name] = key.split("::");
      const module = (currentModulesByTier[tier] || []).find((m) => m.name === name);
      if (module) entries.push({ key, label: module.name, description: module.description, action: "run" });
    } else if (key.startsWith("pipeline::")) {
      const slug = key.slice("pipeline::".length);
      const pipeline = currentPipelines.find((p) => p.slug === slug);
      if (pipeline) entries.push({ key, label: pipeline.name, description: pipeline.description, action: "run" });
    } else if (key.startsWith("artifact::")) {
      const filename = key.slice("artifact::".length);
      const artifact = currentArtifacts.find((f) => f.name === filename);
      if (artifact) entries.push({ key, label: artifact.name, description: formatBytes(artifact.size_bytes), action: "view" });
    }
  }

  if (!entries.length) {
    recentlyViewedSection.classList.add("hidden");
    return;
  }

  recentlyViewedSection.classList.remove("hidden");
  recentlyViewedGrid.innerHTML = entries.map(renderFavoriteChip).join("");
  wireFavoriteToggles(recentlyViewedGrid);
  recentlyViewedGrid.querySelectorAll("[data-fav-run]").forEach((btn) => {
    btn.addEventListener("click", () => runFavorite(btn.dataset.favRun));
  });
}

const recentlyViewedClearBtn = document.getElementById("recently-viewed-clear-btn");
recentlyViewedClearBtn.addEventListener("click", () => {
  if (!recentlyViewedKeys.length) return;
  recentlyViewedKeys = [];
  saveRecentlyViewedKeys();
  renderRecentlyViewedSection();
  showToast("Cleared Recently Viewed.", "success");
});

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
  const stepLabel = event.parallel
    ? `Step ${event.index + 1} branch ${event.branch_index + 1}`
    : `Step ${event.index + 1}`;
  if (event.kind === "step_started") {
    log.system.push(`[${ts}] ${stepLabel} (${event.tier}: ${event.name}) started`);
  } else if (event.kind === "step_completed") {
    log.system.push(`[${ts}] ${stepLabel} (${event.tier}: ${event.name}) completed in ${event.duration_ms}ms`);
  } else if (event.kind === "step_failed") {
    log.system.push(`[${ts}] ${stepLabel} (${event.tier}: ${event.name}) FAILED after ${event.duration_ms}ms: ${event.error}`);
  } else if (event.kind === "step_skipped") {
    const why = event.condition ? ` (condition not met: ${event.condition})` : "";
    log.system.push(`[${ts}] ${stepLabel} (${event.tier}: ${event.name}) skipped${why}`);
  } else if (event.kind === "step_retrying") {
    log.system.push(
      `[${ts}] ${stepLabel} (${event.tier}: ${event.name}) failed (${event.error}) — retrying ${event.attempt}/${event.max_retries} in ${event.delay_seconds.toFixed(1)}s`
    );
  } else if (event.kind === "group_started") {
    log.system.push(`[${ts}] Step ${event.index + 1}: parallel group "${event.name}" started (${event.branch_count} branches)`);
  } else if (event.kind === "group_completed") {
    log.system.push(`[${ts}] Step ${event.index + 1}: parallel group "${event.name}" finished${event.had_failure ? " with a failing branch" : ""}`);
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
// A parallel group renders one indented row per branch, keyed "index:branchIndex"
// (the same composite the SSE events carry as index + branch_index).

function trackerRowHtml(key, tier, name, label) {
  const thoughtControls =
    tier === "agent" ? `<button class="thought-toggle hidden" type="button" data-index="${key}">Thoughts</button>` : "";
  const thoughtBox = tier === "agent" ? `<div class="thought-box hidden" data-index="${key}"></div>` : "";
  return `
    <div class="tracker-step ${key.includes(":") ? "tracker-branch" : ""}" data-tier="${tier}" data-index="${key}">
      <span class="tracker-icon">${STEP_ICON.pending}</span>
      <span class="tracker-label">${label}</span>
      ${thoughtControls}
    </div>
    ${thoughtBox}`;
}

function renderTracker(container, steps) {
  container.innerHTML = steps
    .map((s, index) => {
      if (s.parallel) {
        const header = `<div class="tracker-group-head">Step ${index + 1}: ⫲ ${s.name || "parallel group"} (${s.branches.length} branches)</div>`;
        const rows = s.branches
          .map((b, bi) => trackerRowHtml(`${index}:${bi}`, b.tier, b.name, `Branch ${bi + 1}: [${b.tier}] ${b.name}`))
          .join("");
        return header + rows;
      }
      return trackerRowHtml(String(index), s.tier, s.name, `Step ${index + 1}: [${s.tier}] ${s.name}`);
    })
    .join("");

  container.querySelectorAll(".thought-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const box = container.querySelector(`.thought-box[data-index="${btn.dataset.index}"]`);
      box.classList.toggle("collapsed");
    });
  });
}

function trackerEventKey(event) {
  return event.parallel ? `${event.index}:${event.branch_index}` : String(event.index);
}

function setStepStatus(container, index, status, error) {
  const row = container.querySelector(`.tracker-step[data-index="${index}"]`);
  if (!row) return;
  const icon = row.querySelector(".tracker-icon");
  icon.textContent = STEP_ICON[status];
  icon.classList.toggle("spin", status === "running" || status === "retrying");
  row.classList.toggle("failed", status === "failed");
  row.classList.toggle("skipped", status === "skipped");
  row.classList.toggle("retrying", status === "retrying");
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
  const key = trackerEventKey(event);
  if (event.kind === "step_started") {
    setStepStatus(container, key, "running");
  } else if (event.kind === "step_completed") {
    setStepStatus(container, key, "done");
    recordStepDuration(container, key, event.duration_ms);
  } else if (event.kind === "step_failed") {
    setStepStatus(container, key, "failed", event.error);
    recordStepDuration(container, key, event.duration_ms);
  } else if (event.kind === "step_skipped") {
    setStepStatus(container, key, "skipped", event.condition ? `Skipped — condition not met: ${event.condition}` : "Skipped");
  } else if (event.kind === "step_retrying") {
    setStepStatus(
      container,
      key,
      "retrying",
      `Failed (${event.error}) — retrying ${event.attempt}/${event.max_retries} in ${event.delay_seconds.toFixed(1)}s`
    );
  } else if (event.kind === "thought" || event.kind === "tool_call") {
    appendThought(container, event.index, event.kind, event.message);
  }
}

function recordStepDuration(container, index, durationMs) {
  const row = container.querySelector(`.tracker-step[data-index="${index}"]`);
  if (row && durationMs != null) row.dataset.durationMs = durationMs;
}

// Marks whichever step took the longest wall-clock time in a multi-step run —
// a quick visual cue for "which step is the bottleneck" without having to
// read every duration by eye.
function highlightSlowestStep(container) {
  const rows = Array.from(container.querySelectorAll(".tracker-step[data-duration-ms]"));
  if (rows.length < 2) return;

  const slowest = rows.reduce((a, b) => (Number(b.dataset.durationMs) > Number(a.dataset.durationMs) ? b : a));
  slowest.classList.add("slowest-step");
  const label = slowest.querySelector(".tracker-label");
  if (label) {
    const badge = document.createElement("span");
    badge.className = "slowest-badge";
    badge.textContent = `🐢 slowest (${Math.round(Number(slowest.dataset.durationMs))}ms)`;
    label.appendChild(badge);
  }
}

// ---- SSE subscription ----

function subscribeToStream(streamId, { onEvent, onDone }) {
  const source = new EventSource(`/api/stream/${streamId}`);
  let finished = false;
  let consecutiveErrors = 0;

  const finish = (event) => {
    if (finished) return;
    finished = true;
    source.close();
    onDone(event);
  };

  source.onmessage = (e) => {
    consecutiveErrors = 0;
    const event = JSON.parse(e.data);
    onEvent(event);
    if (event.kind === "run_completed" || event.kind === "run_failed") {
      finish(event);
    }
  };

  source.onerror = () => {
    if (finished) return;
    // EventSource retries a dropped connection on its own (and the server
    // replays anything missed via Last-Event-ID) — only give up after
    // several failed attempts in a row, rather than on the first blip.
    consecutiveErrors += 1;
    if (consecutiveErrors >= 5) {
      finish({ kind: "run_failed", error: "Connection to the server was lost." });
    }
  };
}

// ---- Cards ----

function formatDurationSeconds(seconds) {
  if (seconds == null) return "–";
  return seconds < 1 ? `${Math.round(seconds * 1000)}ms` : `${seconds.toFixed(2)}s`;
}

function renderCard(module) {
  const cardId = `card__${module.tier}__${module.name}`;
  const favKey = `module::${module.tier}::${module.name}`;
  const searchText = `${module.name} ${module.description}`.toLowerCase();
  const fieldsHtml = module.inputs.length
    ? `<div class="form-fields">${module.inputs.map((f) => renderField(module.tier, module.name, f, module.inputs)).join("")}</div>`
    : "";

  const tripped = module.breaker?.tripped;
  const runtimeEnabled = module.runtime_enabled !== false;
  const blocked = tripped || !runtimeEnabled;
  const breakerHtml = tripped
    ? `<div class="breaker-banner">
         ⛔ Circuit breaker open — ${module.breaker.consecutive_failures} consecutive failure${module.breaker.consecutive_failures === 1 ? "" : "s"} (trips at ${module.breaker.threshold}). Runs are blocked.
         <button class="btn breaker-reset-btn" data-tier="${module.tier}" data-name="${module.name}" type="button">Reset breaker</button>
       </div>`
    : "";
  const disabledHtml = !runtimeEnabled
    ? `<div class="module-disabled-banner">
         🚫 Module disabled — runs are blocked until re-enabled.
         <button class="btn module-enable-btn" data-tier="${module.tier}" data-name="${module.name}" type="button">Enable module</button>
       </div>`
    : "";

  return `
    <div class="card ${tripped ? "breaker-tripped" : ""} ${runtimeEnabled ? "" : "module-disabled"}" id="${cardId}" data-search-text="${searchText}" data-tier="${module.tier}" data-name="${module.name}">
      <div class="card-head">
        <input type="checkbox" class="module-select-checkbox" data-tier="${module.tier}" data-name="${module.name}" ${selectedModuleRefs.has(`${module.tier}::${module.name}`) ? "checked" : ""} title="Select for bulk enable/disable" />
        <h3 class="card-title">${module.name}</h3>
        <div class="card-head-actions">
          ${favoriteButtonHtml(favKey)}
          <button class="module-toggle-btn ${runtimeEnabled ? "" : "off"}" data-tier="${module.tier}" data-name="${module.name}" data-enabled="${runtimeEnabled}" type="button" title="${runtimeEnabled ? "Disable this module" : "Enable this module"}">${runtimeEnabled ? "⏻ On" : "⏻ Off"}</button>
          <button class="code-toggle" data-tier="${module.tier}" data-name="${module.name}" type="button" title="View source">&lt;/&gt;</button>
          <button class="module-duplicate-btn" data-tier="${module.tier}" data-name="${module.name}" type="button" title="Duplicate this module as a starting point for a new one">⧉</button>
          <div class="status-slot">${tripped ? statusPill("tripped") : statusPill(module.status)}</div>
        </div>
      </div>
      <p class="card-desc">${module.description}</p>
      <p class="card-stats">${
        module.stats.total_runs
          ? `${module.stats.total_runs} run${module.stats.total_runs === 1 ? "" : "s"} · ${Math.round(module.stats.success_rate * 100)}% success · avg ${formatDurationSeconds(module.stats.avg_duration_seconds)}`
          : "No runs recorded yet."
      }</p>
      ${
        module.used_by && module.used_by.length
          ? `<p class="card-desc module-used-by" title="Saved pipelines with a step using this module">Used by: ${module.used_by
              .map((slug) => `<span class="used-by-chip" data-jump-slug="${escapeHtml(slug)}">${escapeHtml(slug)}</span>`)
              .join(", ")}</p>`
          : ""
      }
      <div class="breaker-threshold-row" title="Consecutive failures before this module's circuit breaker trips">
        <label>Breaker trips after</label>
        <input type="number" class="breaker-threshold-input" min="1" value="${module.breaker.threshold}" data-tier="${module.tier}" data-name="${module.name}" />
        <span>failure(s)</span>
        <button class="btn btn-secondary btn-small breaker-threshold-save-btn" data-tier="${module.tier}" data-name="${module.name}" type="button">Set</button>
        ${
          module.breaker.threshold_overridden
            ? `<button class="btn btn-secondary btn-small breaker-threshold-clear-btn" data-tier="${module.tier}" data-name="${module.name}" type="button">Use default</button>`
            : ""
        }
      </div>
      ${breakerHtml}
      ${disabledHtml}
      <div class="code-panel hidden"></div>
      ${
        module.inputs.length
          ? `<div class="input-presets-row" data-tier="${module.tier}" data-name="${module.name}">
               <select class="input-preset-select" title="Saved input presets"><option value="">Presets…</option></select>
               <button class="btn btn-secondary btn-small input-preset-load-btn" type="button">Load</button>
               <button class="btn btn-secondary btn-small input-preset-save-btn" type="button">Save as…</button>
               <button class="btn btn-secondary btn-small input-preset-delete-btn" type="button">Delete</button>
               <button class="btn btn-secondary btn-small input-last-run-btn" type="button" title="Fill in whatever values this module was run with most recently">↺ Use last run's inputs</button>
             </div>`
          : ""
      }
      ${fieldsHtml}
      <div class="run-row">
        <button class="btn btn-run" data-tier="${module.tier}" data-name="${module.name}" ${blocked ? "disabled" : ""}>Run</button>
        <label class="force-refresh-toggle" title="Skip the cached result (if any) and run fresh">
          <input type="checkbox" class="force-refresh-check" id="refresh__${cardId}" />
          Force refresh
        </label>
      </div>
      <div class="tracker hidden"></div>
      <div class="result-panel hidden"></div>
    </div>`;
}

function escapeHtml(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

async function setModuleEnabled(tier, name, enabled) {
  try {
    const res = await fetch(`/api/modules/${tier}/${name}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled }),
    });
    if (!res.ok) throw new Error((await res.json()).detail || `HTTP ${res.status}`);
    showToast(`${name} ${enabled ? "enabled" : "disabled"}.`, "success");
    await loadModules();
  } catch (err) {
    showToast(`Could not update ${name}: ${err.message}`, "error");
  }
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
  sectionsEl.querySelectorAll(".module-duplicate-btn").forEach((btn) => {
    btn.addEventListener("click", () => duplicateModule(btn.dataset.tier, btn.dataset.name));
  });
  sectionsEl.querySelectorAll(".used-by-chip").forEach((chip) => {
    chip.addEventListener("click", () => {
      const card = document.getElementById(`pipeline-card__${chip.dataset.jumpSlug}`);
      if (card) {
        card.scrollIntoView({ behavior: "smooth", block: "center" });
        flashHighlight(card);
      }
    });
  });
  sectionsEl.querySelectorAll(".breaker-reset-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        const res = await fetch(`/api/breakers/${btn.dataset.tier}/${btn.dataset.name}/reset`, { method: "POST" });
        if (!res.ok) throw new Error((await res.json()).detail || `HTTP ${res.status}`);
        showToast(`Circuit breaker reset for ${btn.dataset.name}.`, "success");
        await loadModules(); // re-render: banner gone, Run re-enabled
        await loadAuditLog();
      } catch (err) {
        showToast(`Reset failed: ${err.message}`, "error");
      }
    });
  });
  sectionsEl.querySelectorAll(".module-toggle-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const currentlyEnabled = btn.dataset.enabled === "true";
      setModuleEnabled(btn.dataset.tier, btn.dataset.name, !currentlyEnabled);
    });
  });
  sectionsEl.querySelectorAll(".module-enable-btn").forEach((btn) => {
    btn.addEventListener("click", () => setModuleEnabled(btn.dataset.tier, btn.dataset.name, true));
  });
  sectionsEl.querySelectorAll(".breaker-threshold-save-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const card = btn.closest(".card");
      const input = card.querySelector(".breaker-threshold-input");
      const threshold = Number(input.value);
      if (!Number.isInteger(threshold) || threshold < 1) {
        showToast("Breaker threshold must be a whole number of at least 1.", "error");
        return;
      }
      try {
        const res = await fetch(`/api/breakers/${btn.dataset.tier}/${btn.dataset.name}/threshold`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ threshold }),
        });
        if (!res.ok) throw new Error((await res.json()).detail || `HTTP ${res.status}`);
        showToast(`${btn.dataset.name}'s breaker now trips after ${threshold} failure(s).`, "success");
        await loadModules();
      } catch (err) {
        showToast(`Could not set breaker threshold: ${err.message}`, "error");
      }
    });
  });
  sectionsEl.querySelectorAll(".breaker-threshold-clear-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        const res = await fetch(`/api/breakers/${btn.dataset.tier}/${btn.dataset.name}/threshold`, { method: "DELETE" });
        if (!res.ok) throw new Error((await res.json()).detail || `HTTP ${res.status}`);
        showToast(`${btn.dataset.name}'s breaker threshold reset to its default.`, "success");
        await loadModules();
      } catch (err) {
        showToast(`Could not reset breaker threshold: ${err.message}`, "error");
      }
    });
  });
  sectionsEl.querySelectorAll(".input-presets-row").forEach((row) => {
    wireInputPresetsRow(row.dataset.tier, row.dataset.name);
    loadInputPresetsIntoRow(row.dataset.tier, row.dataset.name);
  });
  wireFavoriteToggles(sectionsEl);
  wireVariableChips(sectionsEl);
  applyCollapsedState(sectionsEl);
  wireCollapseToggles(sectionsEl);
  wireModuleSelectCheckboxes();
  applyModulesProblemsFilter();
  applySearchFilter();
}

// ---- Bulk enable/disable modules ----

function moduleCheckboxes() {
  return [...sectionsEl.querySelectorAll(".module-select-checkbox")].filter(
    (cb) => !cb.closest(".card").classList.contains("search-hidden") && !cb.closest(".card").classList.contains("problems-filter-hidden")
  );
}

function updateModulesBulkButtons() {
  const disabled = selectedModuleRefs.size === 0;
  modulesBulkEnableBtn.disabled = disabled;
  modulesBulkDisableBtn.disabled = disabled;
  modulesBulkResetBreakerBtn.disabled = disabled;
  modulesBulkClearThresholdBtn.disabled = disabled;
  const suffix = selectedModuleRefs.size ? ` (${selectedModuleRefs.size})` : "";
  modulesBulkEnableBtn.textContent = `Enable selected${suffix}`;
  modulesBulkDisableBtn.textContent = `Disable selected${suffix}`;
  modulesBulkResetBreakerBtn.textContent = `Reset breakers for selected${suffix}`;
  modulesBulkClearThresholdBtn.textContent = `Clear threshold overrides${suffix}`;
}

function wireModuleSelectCheckboxes() {
  const liveRefs = new Set(
    [...sectionsEl.querySelectorAll(".module-select-checkbox")].map((cb) => `${cb.dataset.tier}::${cb.dataset.name}`)
  );
  [...selectedModuleRefs].forEach((ref) => {
    if (!liveRefs.has(ref)) selectedModuleRefs.delete(ref);
  });

  sectionsEl.querySelectorAll(".module-select-checkbox").forEach((checkbox) => {
    checkbox.addEventListener("click", (e) => e.stopPropagation());
    checkbox.addEventListener("change", () => {
      const ref = `${checkbox.dataset.tier}::${checkbox.dataset.name}`;
      if (checkbox.checked) selectedModuleRefs.add(ref);
      else selectedModuleRefs.delete(ref);
      updateModulesBulkButtons();
      modulesSelectAllEl.checked =
        moduleCheckboxes().length > 0 && moduleCheckboxes().every((cb) => cb.checked);
    });
  });
  updateModulesBulkButtons();
}

modulesSelectAllEl.addEventListener("change", () => {
  moduleCheckboxes().forEach((checkbox) => {
    checkbox.checked = modulesSelectAllEl.checked;
    const ref = `${checkbox.dataset.tier}::${checkbox.dataset.name}`;
    if (modulesSelectAllEl.checked) selectedModuleRefs.add(ref);
    else selectedModuleRefs.delete(ref);
  });
  updateModulesBulkButtons();
});

async function bulkSetSelectedModulesEnabled(enabled) {
  if (!selectedModuleRefs.size) return;
  const modules = [...selectedModuleRefs].map((ref) => {
    const [tier, name] = ref.split("::");
    return { tier, name };
  });
  await fetch("/api/modules/bulk-set-enabled", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ modules, enabled }),
  });
  showToast(`${enabled ? "Enabled" : "Disabled"} ${modules.length} module(s).`, "success");
  selectedModuleRefs.clear();
  await loadModules();
}

modulesBulkEnableBtn.addEventListener("click", () => bulkSetSelectedModulesEnabled(true));
modulesBulkDisableBtn.addEventListener("click", () => bulkSetSelectedModulesEnabled(false));

modulesBulkResetBreakerBtn.addEventListener("click", async () => {
  if (!selectedModuleRefs.size) return;
  const modules = [...selectedModuleRefs].map((ref) => {
    const [tier, name] = ref.split("::");
    return { tier, name };
  });
  await fetch("/api/breakers/bulk-reset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ modules }),
  });
  showToast(`Reset ${modules.length} circuit breaker(s).`, "success");
  selectedModuleRefs.clear();
  await loadModules();
});

modulesBulkClearThresholdBtn.addEventListener("click", async () => {
  if (!selectedModuleRefs.size) return;
  const modules = [...selectedModuleRefs].map((ref) => {
    const [tier, name] = ref.split("::");
    return { tier, name };
  });
  await fetch("/api/breakers/bulk-clear-threshold", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ modules }),
  });
  showToast(`Cleared threshold override for ${modules.length} module(s).`, "success");
  selectedModuleRefs.clear();
  await loadModules();
});

function applyModulesProblemsFilter() {
  const onlyProblems = modulesProblemsFilterEl.checked;
  sectionsEl.querySelectorAll(".card[data-tier][data-name]").forEach((card) => {
    const isProblem = card.classList.contains("module-disabled") || card.classList.contains("breaker-tripped");
    card.classList.toggle("problems-filter-hidden", onlyProblems && !isProblem);
  });
  sectionsEl.querySelectorAll(".tier-section").forEach((section) => {
    const grid = section.querySelector(".card-grid");
    if (!grid) return;
    const cards = [...grid.querySelectorAll(".card")];
    const anyVisible = cards.some((c) => !c.classList.contains("problems-filter-hidden"));
    section.classList.toggle("problems-filter-no-match", onlyProblems && cards.length > 0 && !anyVisible);
  });
}

modulesProblemsFilterEl.addEventListener("change", applyModulesProblemsFilter);

// ---- Saved input presets per module ----

function presetsRowFor(tier, name) {
  return sectionsEl.querySelector(`.input-presets-row[data-tier="${CSS.escape(tier)}"][data-name="${CSS.escape(name)}"]`);
}

async function loadInputPresetsIntoRow(tier, name) {
  const row = presetsRowFor(tier, name);
  if (!row) return;
  const select = row.querySelector(".input-preset-select");
  try {
    const res = await fetch(`/api/modules/${tier}/${name}/presets`);
    const presets = await res.json();
    const previousValue = select.value;
    select.innerHTML =
      `<option value="">Presets…</option>` +
      presets.map((p) => `<option value="${escapeHtml(p.preset_name)}">${escapeHtml(p.preset_name)}</option>`).join("");
    if (presets.some((p) => p.preset_name === previousValue)) select.value = previousValue;
  } catch {
    // Presets are a convenience, not core functionality — a failed fetch just leaves the dropdown empty.
  }
}

function wireInputPresetsRow(tier, name) {
  const row = presetsRowFor(tier, name);
  if (!row) return;
  const select = row.querySelector(".input-preset-select");
  const card = row.closest(".card");

  row.querySelector(".input-preset-load-btn").addEventListener("click", async () => {
    const presetName = select.value;
    if (!presetName) return;
    try {
      const res = await fetch(`/api/modules/${tier}/${name}/presets`);
      const presets = await res.json();
      const preset = presets.find((p) => p.preset_name === presetName);
      if (!preset) return;
      Object.entries(preset.inputs).forEach(([fieldName, value]) => {
        const el = card.querySelector(`#${CSS.escape(fieldId(tier, name, fieldName))}`);
        if (!el) return;
        if (el.type === "checkbox") el.checked = Boolean(value);
        else el.value = value ?? "";
      });
      showToast(`Loaded preset "${presetName}".`, "success");
    } catch (err) {
      showToast(`Could not load preset: ${err}`, "error");
    }
  });

  row.querySelector(".input-last-run-btn").addEventListener("click", async () => {
    try {
      const res = await fetch(`/api/modules/${tier}/${name}/last-run-inputs`);
      const body = await res.json();
      if (!res.ok) {
        showToast(body.detail || "This module has never been run.", "error");
        return;
      }
      Object.entries(body.inputs).forEach(([fieldName, value]) => {
        const el = card.querySelector(`#${CSS.escape(fieldId(tier, name, fieldName))}`);
        if (!el) return;
        if (el.type === "checkbox") el.checked = Boolean(value);
        else el.value = value ?? "";
      });
      showToast("Filled in with last run's inputs.", "success");
    } catch (err) {
      showToast(`Could not load last run's inputs: ${err}`, "error");
    }
  });

  row.querySelector(".input-preset-save-btn").addEventListener("click", async () => {
    const presetName = prompt("Save the current values as a preset named:");
    if (!presetName || !presetName.trim()) return;

    const inputEls = card.querySelectorAll("[id^='field__']");
    const inputs = {};
    inputEls.forEach((el) => {
      const fieldName = el.id.split("__").slice(3).join("__");
      inputs[fieldName] = el.type === "checkbox" ? el.checked : el.value;
    });

    try {
      const res = await fetch(`/api/modules/${tier}/${name}/presets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ preset_name: presetName.trim(), inputs }),
      });
      if (!res.ok) throw new Error((await res.json()).detail || `HTTP ${res.status}`);
      showToast(`Saved preset "${presetName.trim()}".`, "success");
      await loadInputPresetsIntoRow(tier, name);
      select.value = presetName.trim();
    } catch (err) {
      showToast(`Could not save preset: ${err}`, "error");
    }
  });

  row.querySelector(".input-preset-delete-btn").addEventListener("click", async () => {
    const presetName = select.value;
    if (!presetName) return;
    try {
      const res = await fetch(`/api/modules/${tier}/${name}/presets/${encodeURIComponent(presetName)}`, { method: "DELETE" });
      if (!res.ok) throw new Error((await res.json()).detail || `HTTP ${res.status}`);
      showToast(`Deleted preset "${presetName}".`, "success");
      await loadInputPresetsIntoRow(tier, name);
    } catch (err) {
      showToast(`Could not delete preset: ${err}`, "error");
    }
  });
}

async function loadModules() {
  const res = await fetch("/api/modules");
  const modulesByTier = await res.json();
  currentModulesByTier = modulesByTier;
  renderSections(modulesByTier);
  renderFavoritesSection();
  renderRecentlyViewedSection();
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
  recordRecentlyViewed(`module::${tier}::${name}`);

  const button = card.querySelector(".btn-run");
  const resultPanel = card.querySelector(".result-panel");
  const tracker = card.querySelector(".tracker");
  const forceRefreshEl = card.querySelector(".force-refresh-check");
  const forceRefresh = forceRefreshEl ? forceRefreshEl.checked : false;

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
  let wasCached = false;

  try {
    const { stream_id } = await fetchRunTrigger(`/api/modules/${tier}/${name}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs, force_refresh: forceRefresh }),
    });

    subscribeToStream(stream_id, {
      onEvent: (event) => {
        handleTrackerEvent(tracker, event);
        if (event.kind === "step_completed") {
          lastOutput = event.output;
          if (event.cached) wasCached = true;
        }
        if (event.kind === "step_failed") lastError = event.error;
      },
      onDone: async (event) => {
        button.disabled = false;
        const success = event.kind === "run_completed";
        setCardStatus(cardId, success ? "ready" : "error");

        resultPanel.classList.remove("hidden", "error");
        if (success) {
          const cachedBadge = wasCached ? '<div class="cached-badge">⚡ cached result — check "Force refresh" to re-run</div>' : "";
          resultPanel.innerHTML = `${cachedBadge}<div>${escapeHtml(JSON.stringify(lastOutput, null, 2))}</div>`;
        } else {
          resultPanel.classList.add("error");
          resultPanel.textContent = `Error: ${lastError ?? event.error}`;
        }

        showToast(
          success
            ? `${name} completed successfully.${wasCached ? " (cached)" : ""}`
            : `${name} failed: ${lastError ?? event.error}`,
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
    const { stream_id } = await fetchRunTrigger("/api/pipeline/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs: {} }),
    });

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
        highlightSlowestStep(tracker);

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
  return { tier: "", name: "", module: null, fieldSources: {}, condition: null, retry: null, note: "" };
}

function blankBuilderBranch() {
  return { tier: "", name: "", module: null, fieldSources: {}, retry: null };
}

function blankBuilderGroup() {
  // Two empty branches: the backend refuses a group with fewer than two, and
  // an empty group would be pointless anyway.
  return { parallel: true, name: "", note: "", branches: [blankBuilderBranch(), blankBuilderBranch()] };
}

// Resolve a builder step key — "2" for a top-level step, "2:1" for branch 1
// of the group in slot 2 — to the object whose module/fieldSources it owns.
function getBuilderStepRef(key) {
  const [i, b] = String(key).split(":");
  const step = builderSteps[Number(i)];
  return b === undefined ? step : step.branches[Number(b)];
}

// Every fieldSources map in the whole builder — top-level module steps plus
// every group branch — for mapping-dependency checks and index rewiring.
// Mappings always reference *top-level* slot indexes, wherever they live.
function allBuilderFieldSources() {
  const result = [];
  for (const step of builderSteps) {
    if (step.parallel) for (const branch of step.branches) result.push(branch.fieldSources || {});
    else result.push(step.fieldSources || {});
  }
  return result;
}

function stepFieldSourceMaps(step) {
  return step.parallel ? step.branches.map((b) => b.fieldSources || {}) : [step.fieldSources || {}];
}

function defaultFieldSources(module) {
  return Object.fromEntries(module.inputs.map((f) => [f.name, { type: "static", value: f.default }]));
}

// Swapping two positions only ever risks breaking a mapping in whichever step
// ends up in the *earlier* slot — the step already in the earlier slot can't
// reference the later one (a mapping can only point at a strictly earlier
// step), so only the step moving down into the earlier slot needs a guard
// before every mapping's step index gets rewritten.
function moveBuilderStep(from, to) {
  if (to < 0 || to >= builderSteps.length || from === to) return;
  const earlierIndex = Math.min(from, to);
  const laterIndex = Math.max(from, to);

  const dependsOnStepAbove = stepFieldSourceMaps(builderSteps[laterIndex]).some((sources) =>
    Object.values(sources).some((source) => source.type === "mapping" && source.step === earlierIndex)
  );
  if (dependsOnStepAbove) {
    showBuilderError("Can't move this step above a step it maps a field from.");
    return;
  }

  snapshotBuilderUndo();
  for (const sources of allBuilderFieldSources()) {
    for (const source of Object.values(sources)) {
      if (source.type !== "mapping") continue;
      if (source.step === earlierIndex) source.step = laterIndex;
      else if (source.step === laterIndex) source.step = earlierIndex;
    }
  }

  [builderSteps[from], builderSteps[to]] = [builderSteps[to], builderSteps[from]];
  renderBuilder();
}

// Removing a step shifts every later step up one position, so any mapping
// referencing an index after the removed one needs to shift down by one to
// keep pointing at the same step. Blocked outright if another step's
// explicit mapping depends on the one being removed — there's no "step N no
// longer exists" value to fall back to.
function removeBuilderStep(index) {
  if (builderSteps.length <= 1) return;

  const hasDependents = builderSteps.some(
    (step, i) =>
      i !== index &&
      stepFieldSourceMaps(step).some((sources) =>
        Object.values(sources).some((source) => source.type === "mapping" && source.step === index)
      )
  );
  if (hasDependents) {
    showBuilderError("Can't remove this step — another step maps a field from it.");
    return;
  }

  snapshotBuilderUndo();
  for (const sources of allBuilderFieldSources()) {
    for (const source of Object.values(sources)) {
      if (source.type === "mapping" && source.step > index) {
        source.step -= 1;
      }
    }
  }

  builderSteps.splice(index, 1);
  renderBuilder();
}

// Inserting a copy right after `index` shifts every later step down one
// position, so any mapping referencing one of those needs to shift up by
// one to keep pointing at the same step -- the mirror image of
// removeBuilderStep's own index bookkeeping. A mapping referencing `index`
// itself needs no change: it still means the original step, not the copy.
function duplicateBuilderStep(index) {
  const original = builderSteps[index];

  snapshotBuilderUndo();
  for (const sources of allBuilderFieldSources()) {
    for (const source of Object.values(sources)) {
      if (source.type === "mapping" && source.step > index) {
        source.step += 1;
      }
    }
  }

  const cloneFieldSources = (fs) => JSON.parse(JSON.stringify(fs || {}));
  const duplicate = original.parallel
    ? {
        parallel: true,
        name: original.name,
        note: original.note || "",
        branches: original.branches.map((b) => ({
          tier: b.tier,
          name: b.name,
          module: b.module,
          fieldSources: cloneFieldSources(b.fieldSources),
          retry: b.retry ? { ...b.retry } : null,
        })),
      }
    : {
        tier: original.tier,
        name: original.name,
        module: original.module,
        fieldSources: cloneFieldSources(original.fieldSources),
        condition: original.condition ? { ...original.condition } : null,
        retry: original.retry ? { ...original.retry } : null,
        note: original.note || "",
      };

  builderSteps.splice(index + 1, 0, duplicate);
  renderBuilder();
}

function renderBuilderField(stepKey, field, source, priorOutputs) {
  const idKey = String(stepKey).replace(":", "-"); // ids stay selector-safe; data-step keeps the raw key
  const controlId = `bfield__${idKey}__${field.name}`;
  const srcId = `bsrc__${idKey}__${field.name}`;
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
    ? `<div class="mapping-tag">↳ Step ${source.step + 1}: ${source.output}${source.nestedPath ? `.${source.nestedPath}` : ""}</div>
       <input type="text" class="nested-path-input" data-step="${stepKey}" data-field="${field.name}"
              placeholder="nested key (optional, e.g. risk_level)" value="${source.nestedPath || ""}" />`
    : renderControl(controlId, field, source.value);

  const chips =
    !isMapped && field.type === "template" ? renderVariableChips(controlId, priorOutputs.map((o) => o.name)) : "";

  return `
    <div class="field builder-field">
      <label>${field.label || field.name}</label>
      <div class="mapping-row">
        <select class="source-select" id="${srcId}" data-step="${stepKey}" data-field="${field.name}">${sourceOptions}</select>
        <div class="mapping-control">${control}</div>
      </div>
      ${chips}
    </div>`;
}

function moduleOptionsHtml(selectedTier, selectedName) {
  return ['<option value="">Select a module…</option>']
    .concat(
      TIER_ORDER.flatMap((tier) =>
        (currentModulesByTier[tier] || []).map(
          (m) =>
            `<option value="${tier}::${m.name}"${selectedTier === tier && selectedName === m.name ? " selected" : ""}>[${tier}] ${m.name}</option>`
        )
      )
    )
    .join("");
}

// Outputs available to a step in slot `index`: every earlier top-level step's
// declared outputs — for a parallel group, the union of its branches'.
function priorOutputsForSlot(index) {
  const outputs = [];
  builderSteps.slice(0, index).forEach((s, i) => {
    const modules = s.parallel ? s.branches.map((b) => b.module).filter(Boolean) : s.module ? [s.module] : [];
    for (const m of modules) {
      for (const o of m.outputs || []) outputs.push({ stepIndex: i, name: o.name, label: o.label || o.name });
    }
  });
  return outputs;
}

function stepControlsHtml(index) {
  const canRemove = builderSteps.length > 1;
  const canMoveUp = index > 0;
  const canMoveDown = index < builderSteps.length - 1;
  return `
    <button class="move-step-btn" data-index="${index}" data-dir="up" type="button" title="Move step up" ${canMoveUp ? "" : "disabled"}>▲</button>
    <button class="move-step-btn" data-index="${index}" data-dir="down" type="button" title="Move step down" ${canMoveDown ? "" : "disabled"}>▼</button>
    <button class="duplicate-step-btn" data-index="${index}" type="button" title="Duplicate this step">⧉</button>
    ${canRemove ? `<button class="remove-step-btn" data-index="${index}" type="button" title="Remove step">×</button>` : ""}`;
}

function renderBuilderGroup(index, step) {
  const priorOutputs = priorOutputsForSlot(index);

  const branchesHtml = step.branches
    .map((branch, bi) => {
      const key = `${index}:${bi}`;
      const fieldsHtml = branch.module
        ? branch.module.inputs.map((f) => renderBuilderField(key, f, branch.fieldSources[f.name], priorOutputs)).join("")
        : '<p class="card-desc">Pick a module for this branch.</p>';
      const canRemoveBranch = step.branches.length > 2;
      return `
        <div class="builder-branch">
          <div class="builder-step-head">
            <span class="branch-badge">${String.fromCharCode(97 + bi)}</span>
            <select class="module-select" data-index="${key}">${moduleOptionsHtml(branch.tier, branch.name)}</select>
            ${canRemoveBranch ? `<button class="remove-branch-btn" data-index="${key}" type="button" title="Remove branch">×</button>` : ""}
          </div>
          ${branch.module ? renderBuilderRetry(key, branch.retry) : ""}
          <div class="builder-step-fields">${fieldsHtml}</div>
        </div>`;
    })
    .join("");

  return `
    <div class="builder-step builder-group" data-index="${index}">
      <div class="builder-step-head">
        <span class="step-badge group-badge">${index + 1}</span>
        <span class="group-label">⫲ Parallel group</span>
        <input type="text" class="group-name-input" data-index="${index}" placeholder="group name (optional)" value="${step.name || ""}" />
        ${stepControlsHtml(index)}
      </div>
      ${renderBuilderNote(index, step.note)}
      <p class="card-desc group-hint">Branches run at the same time and must not depend on each other — each may map fields from steps <em>above</em> this group only.</p>
      <div class="builder-branches">${branchesHtml}</div>
      <button class="btn btn-secondary btn-small add-branch-btn" data-index="${index}" type="button">+ Add branch</button>
    </div>`;
}

function renderBuilderStep(index, step) {
  if (step.parallel) return renderBuilderGroup(index, step);

  const priorOutputs = priorOutputsForSlot(index);
  const fieldsHtml = step.module
    ? step.module.inputs.map((field) => renderBuilderField(index, field, step.fieldSources[field.name], priorOutputs)).join("")
    : '<p class="card-desc">Pick a module above to configure its inputs.</p>';

  return `
    <div class="builder-step" data-index="${index}">
      <div class="builder-step-head">
        <span class="step-badge">${index + 1}</span>
        <select class="module-select" data-index="${index}">${moduleOptionsHtml(step.tier, step.name)}</select>
        ${stepControlsHtml(index)}
      </div>
      ${renderBuilderNote(index, step.note)}
      ${step.module ? renderBuilderCondition(index, step) : ""}
      ${step.module ? renderBuilderRetry(index, step.retry) : ""}
      <div class="builder-step-fields">${fieldsHtml}</div>
    </div>`;
}

// The optional skip-unless-condition row for one builder step. Conditions
// reference context *keys* (strings resolved at run time), never step indexes,
// so reordering or removing other steps needs no condition rewiring — unlike
// mappings, which point at a specific earlier step.
function renderBuilderCondition(index, step) {
  const cond = step.condition;
  const enabled = cond !== null && cond !== undefined;
  const operator = cond?.operator || "equals";
  const needsValue = !["truthy", "falsy"].includes(operator);

  const operatorOptions = CONDITION_OPERATORS.map(
    (op) => `<option value="${op}"${op === operator ? " selected" : ""}>${CONDITION_OPERATOR_LABELS[op]}</option>`
  ).join("");

  const controls = enabled
    ? `<input type="text" class="condition-source" data-index="${index}" placeholder="context key, e.g. insight.risk_level" value="${cond.source || ""}" />
       <select class="condition-operator" data-index="${index}">${operatorOptions}</select>
       ${needsValue ? `<input type="text" class="condition-value" data-index="${index}" placeholder="value" value="${cond.value ?? ""}" />` : ""}`
    : "";

  return `
    <div class="builder-condition ${enabled ? "active" : ""}">
      <label class="condition-enable-label">
        <input type="checkbox" class="condition-enable" data-index="${index}" ${enabled ? "checked" : ""} />
        Run only if…
      </label>
      ${controls}
    </div>`;
}

// The optional retry-on-failure row for one step or branch. Backoff doubles
// each attempt (attempt N waits backoff_seconds * 2**N), same as the engine.
function renderBuilderNote(index, note) {
  return `
    <textarea class="builder-step-note" data-index="${index}" placeholder="Add a note for anyone reading this pipeline later (optional)…" rows="1">${escapeHtml(note || "")}</textarea>`;
}

function renderBuilderRetry(key, retry) {
  const enabled = retry !== null && retry !== undefined;
  const maxRetries = retry?.max_retries ?? 2;
  const backoffSeconds = retry?.backoff_seconds ?? 1;

  const controls = enabled
    ? `<input type="number" class="retry-max" data-index="${key}" min="1" step="1" value="${maxRetries}" title="Max retries" />
       <span class="retry-label-inline">retries, backoff</span>
       <input type="number" class="retry-backoff" data-index="${key}" min="0" step="0.5" value="${backoffSeconds}" title="Backoff seconds" />
       <span class="retry-label-inline">s (doubles each attempt)</span>`
    : "";

  return `
    <div class="builder-retry ${enabled ? "active" : ""}">
      <label class="retry-enable-label">
        <input type="checkbox" class="retry-enable" data-index="${key}" ${enabled ? "checked" : ""} />
        Retry on failure
      </label>
      ${controls}
    </div>`;
}

function renderBuilder() {
  builderStepsEl.innerHTML = builderSteps.map((step, index) => renderBuilderStep(index, step)).join("");
  attachBuilderStepListeners();
  wireVariableChips(builderStepsEl);
}

// A later step may have been mapping from slot `index`'s old module — its
// outputs may no longer exist, so reset every later step's (and branch's)
// mappings back to that module's static defaults.
function resetMappingsAfterSlot(index) {
  for (let i = index + 1; i < builderSteps.length; i++) {
    const step = builderSteps[i];
    if (step.parallel) {
      for (const branch of step.branches) {
        if (branch.module) branch.fieldSources = defaultFieldSources(branch.module);
      }
    } else if (step.module) {
      step.fieldSources = defaultFieldSources(step.module);
    }
  }
}

const builderUndoBtn = document.getElementById("builder-undo");

function snapshotBuilderUndo() {
  builderUndoStack.push(JSON.parse(JSON.stringify(builderSteps)));
  if (builderUndoStack.length > BUILDER_UNDO_LIMIT) builderUndoStack.shift();
  updateBuilderUndoButton();
}

function clearBuilderUndo() {
  builderUndoStack = [];
  updateBuilderUndoButton();
}

function updateBuilderUndoButton() {
  builderUndoBtn.disabled = builderUndoStack.length === 0;
}

function undoBuilderStep() {
  if (!builderUndoStack.length) return;
  builderSteps = builderUndoStack.pop();
  updateBuilderUndoButton();
  renderBuilder();
}

builderUndoBtn.addEventListener("click", undoBuilderStep);

function attachBuilderStepListeners() {
  builderStepsEl.querySelectorAll(".module-select").forEach((sel) => {
    sel.addEventListener("change", (e) => {
      const key = e.target.dataset.index;
      const [slotStr, branchStr] = key.split(":");
      const slot = Number(slotStr);
      const [tier, name] = e.target.value.split("::");
      const module = tier && name ? (currentModulesByTier[tier] || []).find((m) => m.name === name) : null;

      snapshotBuilderUndo();
      if (branchStr !== undefined) {
        const oldBranch = builderSteps[slot].branches[Number(branchStr)];
        builderSteps[slot].branches[Number(branchStr)] = module
          ? { tier, name, module, fieldSources: defaultFieldSources(module), retry: oldBranch?.retry ?? null }
          : blankBuilderBranch();
      } else if (!module) {
        builderSteps[slot] = blankBuilderStep();
      } else {
        // A condition/retry policy (or an author's note) references context
        // keys, failure behavior, or just documentation -- none of it is
        // about the module itself, so all three survive swapping which
        // module the step runs.
        const condition = builderSteps[slot]?.condition ?? null;
        const retry = builderSteps[slot]?.retry ?? null;
        const note = builderSteps[slot]?.note ?? "";
        builderSteps[slot] = { tier, name, module, fieldSources: defaultFieldSources(module), condition, retry, note };
      }

      resetMappingsAfterSlot(slot);
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".remove-step-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      removeBuilderStep(Number(e.currentTarget.dataset.index));
    });
  });

  builderStepsEl.querySelectorAll(".duplicate-step-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      duplicateBuilderStep(Number(e.currentTarget.dataset.index));
    });
  });

  builderStepsEl.querySelectorAll(".add-branch-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      snapshotBuilderUndo();
      builderSteps[Number(e.currentTarget.dataset.index)].branches.push(blankBuilderBranch());
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".remove-branch-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const [slotStr, branchStr] = e.currentTarget.dataset.index.split(":");
      const step = builderSteps[Number(slotStr)];
      if (step.branches.length <= 2) return; // groups need at least two branches
      snapshotBuilderUndo();
      step.branches.splice(Number(branchStr), 1);
      resetMappingsAfterSlot(Number(slotStr));
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".group-name-input").forEach((input) => {
    input.addEventListener("input", (e) => {
      builderSteps[Number(e.target.dataset.index)].name = e.target.value;
    });
  });

  builderStepsEl.querySelectorAll(".builder-step-note").forEach((textarea) => {
    textarea.addEventListener("input", (e) => {
      builderSteps[Number(e.target.dataset.index)].note = e.target.value;
    });
  });

  builderStepsEl.querySelectorAll(".move-step-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const index = Number(e.currentTarget.dataset.index);
      const target = e.currentTarget.dataset.dir === "up" ? index - 1 : index + 1;
      moveBuilderStep(index, target);
    });
  });

  builderStepsEl.querySelectorAll(".source-select").forEach((sel) => {
    sel.addEventListener("change", (e) => {
      const ref = getBuilderStepRef(e.target.dataset.step);
      const fieldName = e.target.dataset.field;
      const value = e.target.value;

      if (value === "static") {
        const field = ref.module.inputs.find((f) => f.name === fieldName);
        ref.fieldSources[fieldName] = { type: "static", value: field.default };
      } else {
        const [, stepStr, output] = value.split(":");
        ref.fieldSources[fieldName] = { type: "mapping", step: Number(stepStr), output };
      }
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".mapping-control input, .mapping-control select, .mapping-control textarea").forEach((control) => {
    if (control.classList.contains("nested-path-input")) return; // handled separately below
    const handler = (e) => {
      const wrapper = e.target.closest(".field.builder-field");
      const srcSelect = wrapper.querySelector(".source-select");
      const ref = getBuilderStepRef(srcSelect.dataset.step);
      const fieldName = srcSelect.dataset.field;
      ref.fieldSources[fieldName].value = e.target.type === "checkbox" ? e.target.checked : e.target.value;
    };
    control.addEventListener("input", handler);
    control.addEventListener("change", handler);
  });

  builderStepsEl.querySelectorAll(".nested-path-input").forEach((input) => {
    input.addEventListener("input", (e) => {
      const ref = getBuilderStepRef(e.target.dataset.step);
      const fieldName = e.target.dataset.field;
      const source = ref.fieldSources[fieldName];
      source.nestedPath = e.target.value.trim();

      // Update the mapping-tag label in place instead of a full re-render,
      // which would steal focus from the input mid-keystroke.
      const tag = e.target.closest(".mapping-control")?.querySelector(".mapping-tag");
      if (tag) tag.textContent = `↳ Step ${source.step + 1}: ${source.output}${source.nestedPath ? `.${source.nestedPath}` : ""}`;
    });
  });

  builderStepsEl.querySelectorAll(".condition-enable").forEach((box) => {
    box.addEventListener("change", (e) => {
      const index = Number(e.target.dataset.index);
      builderSteps[index].condition = e.target.checked ? { source: "", operator: "equals", value: "" } : null;
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".condition-source").forEach((input) => {
    input.addEventListener("input", (e) => {
      builderSteps[Number(e.target.dataset.index)].condition.source = e.target.value.trim();
    });
  });

  builderStepsEl.querySelectorAll(".condition-operator").forEach((sel) => {
    sel.addEventListener("change", (e) => {
      const index = Number(e.target.dataset.index);
      builderSteps[index].condition.operator = e.target.value;
      renderBuilder(); // truthy/falsy hide the value input; others show it
    });
  });

  builderStepsEl.querySelectorAll(".condition-value").forEach((input) => {
    input.addEventListener("input", (e) => {
      builderSteps[Number(e.target.dataset.index)].condition.value = e.target.value;
    });
  });

  builderStepsEl.querySelectorAll(".retry-enable").forEach((box) => {
    box.addEventListener("change", (e) => {
      const ref = getBuilderStepRef(e.target.dataset.index);
      ref.retry = e.target.checked ? { max_retries: 2, backoff_seconds: 1 } : null;
      renderBuilder();
    });
  });

  builderStepsEl.querySelectorAll(".retry-max").forEach((input) => {
    input.addEventListener("input", (e) => {
      getBuilderStepRef(e.target.dataset.index).retry.max_retries = Math.max(0, parseInt(e.target.value, 10) || 0);
    });
  });

  builderStepsEl.querySelectorAll(".retry-backoff").forEach((input) => {
    input.addEventListener("input", (e) => {
      getBuilderStepRef(e.target.dataset.index).retry.backoff_seconds = Math.max(0, parseFloat(e.target.value) || 0);
    });
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

function updateBuilderEditingUI() {
  builderNameEl.disabled = editingPipelineSlug !== null;
  builderEditingBannerEl.classList.toggle("hidden", editingPipelineSlug === null);
  if (editingPipelineSlug !== null) {
    builderEditingNameEl.textContent = builderNameEl.value;
  }
}

// Rebuild one saved-pipeline module step (or parallel branch) into the
// builder's internal shape: every field the module declares gets a
// fieldSources entry — mapped fields resolved back into {step, output,
// nestedPath} from the saved dotted-path string, everything else falling
// back to its saved static value or the manifest default.
function moduleStepToBuilderStep(step) {
  const module = (currentModulesByTier[step.tier] || []).find((m) => m.name === step.name);
  const fieldSources = {};
  if (module) {
    for (const field of module.inputs) {
      const mapping = (step.mappings || {})[field.name];
      if (mapping) {
        const [base, ...rest] = mapping.output.split(".");
        fieldSources[field.name] = { type: "mapping", step: mapping.step, output: base, nestedPath: rest.join(".") };
      } else {
        fieldSources[field.name] = { type: "static", value: (step.inputs || {})[field.name] ?? field.default };
      }
    }
  }
  return {
    tier: step.tier,
    name: step.name,
    module,
    fieldSources,
    condition: step.condition || null,
    retry: step.retry || null,
    note: step.note || "",
  };
}

function definitionToBuilderSteps(definition) {
  return (definition.steps || []).map((step) =>
    step.type === "parallel"
      ? {
          parallel: true,
          name: step.name || "",
          note: step.note || "",
          branches: (step.branches || []).map(moduleStepToBuilderStep),
        }
      : moduleStepToBuilderStep(step)
  );
}

// Loads a saved pipeline's full definition back into the builder for
// modification. Renaming is disabled while editing (see updateBuilderEditingUI)
// so "Save & Launch" always overwrites the same slug/file rather than risking
// an orphaned duplicate under a new name — clone the pipeline first if a
// genuinely new, differently-named copy is what's wanted.
function openPipelineForEditing(slug) {
  const definition = currentPipelines.find((p) => p.slug === slug);
  if (!definition) return;

  builderSteps = definitionToBuilderSteps(definition);
  builderNameEl.value = definition.name;
  builderDescriptionEl.value = definition.description || "";
  editingPipelineSlug = slug;
  clearBuilderUndo(); // a freshly loaded pipeline starts its own undo history, not the last one's

  openBuilder();
  renderBuilder();
  updateBuilderEditingUI();
  builderPanelEl.scrollIntoView({ behavior: "smooth", block: "start" });
}

builderCancelEditBtn.addEventListener("click", () => {
  editingPipelineSlug = null;
  builderSteps = [blankBuilderStep()];
  builderNameEl.value = "";
  builderDescriptionEl.value = "";
  clearBuilderUndo();
  renderBuilder();
  updateBuilderEditingUI();
});

document.getElementById("pipeline-import-input").addEventListener("change", async (e) => {
  const file = e.target.files[0];
  if (!file) return;

  const formData = new FormData();
  formData.append("file", file);
  try {
    const res = await fetch("/api/pipelines/import", { method: "POST", body: formData });
    const body = await res.json();
    if (!res.ok) {
      showToast(`Import failed: ${body.detail || "unknown error"}`, "error");
      return;
    }
    showToast(`Imported pipeline "${body.pipeline.name}".`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Import failed: ${err}`, "error");
  }
  e.target.value = "";
});

document.getElementById("pipeline-import-zip-input").addEventListener("change", async (e) => {
  const file = e.target.files[0];
  if (!file) return;

  const formData = new FormData();
  formData.append("file", file);
  try {
    const res = await fetch("/api/pipelines/import-zip", { method: "POST", body: formData });
    const body = await res.json();
    if (!res.ok) {
      showToast(`Import failed: ${body.detail || "unknown error"}`, "error");
      return;
    }
    const failedNote = body.failed.length ? `, ${body.failed.length} failed` : "";
    showToast(`Imported ${body.imported.length} pipeline(s) from zip${failedNote}.`, body.failed.length ? "error" : "success");
    if (body.failed.length) {
      console.warn("Pipeline zip import failures:", body.failed);
    }
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Import failed: ${err}`, "error");
  }
  e.target.value = "";
});

const pipelineImportUrlInput = document.getElementById("pipeline-import-url-input");
const pipelineImportUrlBtn = document.getElementById("pipeline-import-url-btn");

pipelineImportUrlBtn.addEventListener("click", async () => {
  const url = pipelineImportUrlInput.value.trim();
  if (!url) {
    showToast("Enter a URL first.", "error");
    return;
  }

  pipelineImportUrlBtn.disabled = true;
  try {
    const res = await fetch("/api/pipelines/import-url", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(`Import failed: ${body.detail || "unknown error"}`, "error");
      return;
    }
    showToast(`Imported pipeline "${body.pipeline.name}".`, "success");
    pipelineImportUrlInput.value = "";
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Import failed: ${err}`, "error");
  } finally {
    pipelineImportUrlBtn.disabled = false;
  }
});

builderToggleBtn.addEventListener("click", () => {
  if (builderPanelEl.classList.contains("hidden")) openBuilder();
  else closeBuilder();
});

builderAddStepBtn.addEventListener("click", () => {
  snapshotBuilderUndo();
  builderSteps.push(blankBuilderStep());
  renderBuilder();
});

document.getElementById("builder-add-group").addEventListener("click", () => {
  if (builderPanelEl.classList.contains("hidden")) openBuilder();
  snapshotBuilderUndo();
  builderSteps.push(blankBuilderGroup());
  renderBuilder();
});

function showBuilderError(message) {
  builderErrorEl.textContent = message;
  builderErrorEl.classList.remove("hidden");
}

// Validates the builder's current state and returns {name, description, steps}
// ready to POST, or null (after showing the specific validation error) if
// it isn't launchable/saveable yet. Shared by both "Save" and "Save & Launch".
function collectBuilderPipelinePayload() {
  builderErrorEl.classList.add("hidden");

  const name = builderNameEl.value.trim();
  if (!name) {
    showBuilderError("Pipeline name is required.");
    return null;
  }
  if (builderSteps.length === 0) {
    showBuilderError("Every step needs a module selected.");
    return null;
  }
  for (const s of builderSteps) {
    if (s.parallel) {
      if (s.branches.some((b) => !b.module)) {
        showBuilderError("Every parallel branch needs a module selected.");
        return null;
      }
    } else if (!s.module) {
      showBuilderError("Every step needs a module selected.");
      return null;
    }
  }
  if (builderSteps.some((s) => !s.parallel && s.condition && !s.condition.source.trim())) {
    showBuilderError("A 'Run only if' condition needs a context key to check (or untick it).");
    return null;
  }

  const moduleStepPayload = (step) => {
    const inputs = {};
    const mappings = {};
    for (const [fieldName, source] of Object.entries(step.fieldSources)) {
      if (source.type === "mapping") {
        const output = source.nestedPath ? `${source.output}.${source.nestedPath}` : source.output;
        mappings[fieldName] = { step: source.step, output };
      } else {
        inputs[fieldName] = source.value;
      }
    }
    const spec = { tier: step.tier, name: step.name, inputs, mappings };
    if (step.condition) spec.condition = step.condition;
    if (step.retry) spec.retry = step.retry;
    if (step.note) spec.note = step.note;
    return spec;
  };

  const steps = builderSteps.map((step) =>
    step.parallel
      ? { type: "parallel", name: step.name.trim(), note: step.note || "", branches: step.branches.map(moduleStepPayload) }
      : moduleStepPayload(step)
  );

  return { name, description: builderDescriptionEl.value.trim(), steps };
}

builderValidateBtn.addEventListener("click", async () => {
  const payload = collectBuilderPipelinePayload();
  if (!payload) return;

  builderValidateBtn.disabled = true;
  builderValidateBtn.textContent = "Validating...";
  try {
    const res = await fetch("/api/pipelines/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const body = await res.json();
    if (!res.ok) {
      showBuilderError(body.detail || "This pipeline is invalid.");
      return;
    }
    showToast(`"${payload.name}" looks valid — nothing saved or run.`, "success");
  } catch (err) {
    showBuilderError(`Request failed: ${err}`);
  } finally {
    builderValidateBtn.disabled = false;
    builderValidateBtn.textContent = "Validate";
  }
});

builderSaveOnlyBtn.addEventListener("click", async () => {
  const payload = collectBuilderPipelinePayload();
  if (!payload) return;

  builderSaveOnlyBtn.disabled = true;
  builderSaveOnlyBtn.textContent = "Saving...";
  try {
    const res = await fetch("/api/pipelines", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...payload, launch: false }),
    });
    const body = await res.json();
    if (!res.ok) {
      showBuilderError(body.detail || "Failed to save this pipeline.");
      return;
    }
    showToast(`Saved "${body.pipeline.name}" as a draft — not run yet.`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showBuilderError(`Request failed: ${err}`);
  } finally {
    builderSaveOnlyBtn.disabled = false;
    builderSaveOnlyBtn.textContent = "Save";
  }
});

builderLaunchBtn.addEventListener("click", async () => {
  const payload = collectBuilderPipelinePayload();
  if (!payload) return;
  const { name, description, steps } = payload;

  builderLaunchBtn.disabled = true;
  builderLaunchBtn.textContent = "Launching...";

  try {
    const res = await fetch("/api/pipelines", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, description, steps }),
    });

    if (!res.ok) {
      const err = await res.json();
      showBuilderError(err.detail || "Failed to save this pipeline.");
      builderLaunchBtn.disabled = false;
      builderLaunchBtn.textContent = "Save & Launch";
      return;
    }

    const { stream_id } = await res.json();
    const trackerSteps = builderSteps.map((s) =>
      s.parallel
        ? { parallel: true, name: s.name, branches: s.branches.map((b) => ({ tier: b.tier, name: b.name })) }
        : { tier: s.tier, name: s.name }
    );
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
        highlightSlowestStep(builderTrackerEl);

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
    savedPipelinesGrid.innerHTML = "";
  } else {
    savedPipelinesSection.classList.remove("hidden");

    savedPipelinesGrid.innerHTML = pipelinesList
      .map((p) => {
        const chain = p.steps.map((s) => `[${s.tier}] ${s.name}`).join(" → ");
        const favKey = `pipeline::${p.slug}`;
        const tags = p.tags || [];
        const searchText = `${p.name} ${p.description || ""} ${chain} ${tags.join(" ")}`.toLowerCase();
        const tagChips = tags
          .map(
            (t) => `
            <span class="tag-chip">
              <span class="tag-chip-label">${escapeHtml(t)}</span>
              <button type="button" class="pipeline-tag-remove" data-slug="${p.slug}" data-tag="${escapeHtml(t)}" title="Remove tag">×</button>
            </span>`
          )
          .join("");
        const bulkChecked = selectedPipelineSlugs.has(p.slug) ? "checked" : "";
        return `
          <div class="card" id="pipeline-card__${p.slug}" data-search-text="${searchText}">
            <div class="card-head">
              <input type="checkbox" class="pipeline-select-checkbox" data-slug="${p.slug}" ${bulkChecked} title="Select for bulk delete" />
              <h3 class="card-title">${p.name}</h3>
              ${favoriteButtonHtml(favKey)}
            </div>
            <p class="card-desc">${p.description || "No description."}</p>
            <p class="card-desc pipeline-chain">${chain}</p>
            <div class="pipeline-tags-row">
              <span class="tag-chip-list">${tagChips}</span>
              <input type="text" class="pipeline-tag-add-input" data-slug="${p.slug}" placeholder="+ tag" />
            </div>
            <div class="webhook-row">
              <code class="webhook-url" title="POST a JSON body here to launch this pipeline — it overrides step 1's own inputs">POST /api/pipelines/${p.slug}/webhook</code>
              <button class="btn btn-secondary btn-small webhook-copy-btn" data-slug="${p.slug}" type="button">📋 Copy URL</button>
              <button class="btn btn-secondary btn-small webhook-test-btn" data-slug="${p.slug}" data-name="${escapeHtml(p.name)}" type="button" title="Send an empty test POST to this pipeline's own webhook endpoint">▶ Send test</button>
            </div>
            <div class="pipeline-card-actions">
              <button class="btn btn-run" data-slug="${p.slug}" data-name="${p.name}">Run</button>
              <button class="btn btn-secondary btn-small" data-edit-slug="${p.slug}" type="button">Edit</button>
              <button class="btn btn-secondary btn-small" data-rename-slug="${p.slug}" data-rename-name="${escapeHtml(p.name)}" type="button">Rename</button>
              <button class="btn btn-secondary btn-small" data-clone-slug="${p.slug}" type="button">Clone</button>
              <button class="btn btn-secondary btn-small" data-graph-slug="${p.slug}" type="button">Graph</button>
              <button class="btn btn-secondary btn-small" data-history-slug="${p.slug}" type="button">History</button>
              <a class="btn btn-secondary btn-small" href="/api/pipelines/${p.slug}/export" download="${p.slug}.yaml">Export YAML</a>
              <a class="btn btn-secondary btn-small" href="/api/pipelines/${p.slug}/export.json" download="${p.slug}.json">Export JSON</a>
              <button class="btn btn-danger btn-small" data-delete-slug="${p.slug}" data-delete-name="${p.name}" type="button">Delete</button>
            </div>
            <div class="tracker hidden"></div>
            <div class="log-tabs-wrap hidden"></div>
            <div class="dag-panel hidden"></div>
            <div class="history-panel hidden"></div>
          </div>`;
      })
      .join("");

    savedPipelinesGrid.querySelectorAll(".btn-run").forEach((btn) => {
      btn.addEventListener("click", () => runSavedPipeline(btn.dataset.slug, btn.dataset.name, pipelinesList));
    });
    savedPipelinesGrid.querySelectorAll("[data-clone-slug]").forEach((btn) => {
      btn.addEventListener("click", () => clonePipeline(btn.dataset.cloneSlug));
    });
    savedPipelinesGrid.querySelectorAll("[data-rename-slug]").forEach((btn) => {
      btn.addEventListener("click", () => renamePipeline(btn.dataset.renameSlug, btn.dataset.renameName));
    });
    savedPipelinesGrid.querySelectorAll("[data-edit-slug]").forEach((btn) => {
      btn.addEventListener("click", () => openPipelineForEditing(btn.dataset.editSlug));
    });
    savedPipelinesGrid.querySelectorAll("[data-graph-slug]").forEach((btn) => {
      btn.addEventListener("click", () => togglePipelineGraph(btn.dataset.graphSlug));
    });
    savedPipelinesGrid.querySelectorAll("[data-history-slug]").forEach((btn) => {
      btn.addEventListener("click", () => togglePipelineHistory(btn.dataset.historySlug));
    });
    savedPipelinesGrid.querySelectorAll("[data-delete-slug]").forEach((btn) => {
      btn.addEventListener("click", () => deleteSavedPipeline(btn.dataset.deleteSlug, btn.dataset.deleteName));
    });
    savedPipelinesGrid.querySelectorAll(".pipeline-tag-remove").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const slug = btn.dataset.slug;
        const removeTag = btn.dataset.tag;
        const p = pipelinesList.find((pl) => pl.slug === slug);
        const nextTags = (p.tags || []).filter((t) => t !== removeTag);
        await savePipelineTags(slug, nextTags);
        await loadSavedPipelines();
      });
    });
    savedPipelinesGrid.querySelectorAll(".pipeline-tag-add-input").forEach((input) => {
      input.addEventListener("keydown", async (e) => {
        if (e.key !== "Enter") return;
        const slug = input.dataset.slug;
        const newTag = input.value.trim();
        if (!newTag) return;
        const p = pipelinesList.find((pl) => pl.slug === slug);
        const nextTags = Array.from(new Set([...(p.tags || []), newTag]));
        await savePipelineTags(slug, nextTags);
        await loadSavedPipelines();
      });
    });
    savedPipelinesGrid.querySelectorAll(".webhook-copy-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const url = `${window.location.origin}/api/pipelines/${btn.dataset.slug}/webhook`;
        try {
          await navigator.clipboard.writeText(url);
          showToast("Webhook URL copied to clipboard.", "success");
        } catch (err) {
          showToast(`Copy failed: ${err}`, "error");
        }
      });
    });
    savedPipelinesGrid.querySelectorAll(".webhook-test-btn").forEach((btn) => {
      btn.addEventListener("click", () => sendTestWebhook(btn.dataset.slug, btn.dataset.name, pipelinesList));
    });
    wireFavoriteToggles(savedPipelinesGrid);

    const liveSlugs = new Set(pipelinesList.map((p) => p.slug));
    [...selectedPipelineSlugs].forEach((slug) => {
      if (!liveSlugs.has(slug)) selectedPipelineSlugs.delete(slug);
    });
    savedPipelinesGrid.querySelectorAll(".pipeline-select-checkbox").forEach((checkbox) => {
      checkbox.addEventListener("change", () => {
        if (checkbox.checked) selectedPipelineSlugs.add(checkbox.dataset.slug);
        else selectedPipelineSlugs.delete(checkbox.dataset.slug);
        updatePipelinesBulkDeleteBtn();
        pipelinesSelectAllEl.checked = pipelinesList.every((p) => selectedPipelineSlugs.has(p.slug));
      });
    });
    pipelinesSelectAllEl.checked = pipelinesList.length > 0 && pipelinesList.every((p) => selectedPipelineSlugs.has(p.slug));
  }

  updatePipelinesBulkDeleteBtn();
  populatePipelineCompareSelects(pipelinesList);
  applySearchFilter();
  renderFavoritesSection();
  renderRecentlyViewedSection();
}

pipelinesSelectAllEl.addEventListener("change", () => {
  const checkboxes = savedPipelinesGrid.querySelectorAll(".pipeline-select-checkbox");
  checkboxes.forEach((checkbox) => {
    checkbox.checked = pipelinesSelectAllEl.checked;
    if (pipelinesSelectAllEl.checked) selectedPipelineSlugs.add(checkbox.dataset.slug);
    else selectedPipelineSlugs.delete(checkbox.dataset.slug);
  });
  updatePipelinesBulkDeleteBtn();
});

pipelinesBulkDeleteBtn.addEventListener("click", async () => {
  if (!selectedPipelineSlugs.size) return;
  if (!confirm(`Delete ${selectedPipelineSlugs.size} selected pipeline(s)? This cannot be undone.`)) return;

  try {
    const res = await fetch("/api/pipelines/bulk-delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ slugs: [...selectedPipelineSlugs] }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Bulk delete failed.", "error");
      return;
    }
    selectedPipelineSlugs.clear();
    showToast(`Deleted ${body.deleted.length} pipeline(s).`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Bulk delete failed: ${err}`, "error");
  }
});

// ---- Pipeline comparison ----

const pipelineCompareBarEl = document.getElementById("pipeline-compare-bar");
const comparePipelineAEl = document.getElementById("compare-pipeline-a");
const comparePipelineBEl = document.getElementById("compare-pipeline-b");
const comparePipelinesBtn = document.getElementById("compare-pipelines-btn");
const pipelineCompareResultEl = document.getElementById("pipeline-compare-result");

function populatePipelineCompareSelects(pipelinesList) {
  if (pipelinesList.length < 2) {
    pipelineCompareBarEl.classList.add("hidden");
    pipelineCompareResultEl.classList.add("hidden");
    return;
  }
  pipelineCompareBarEl.classList.remove("hidden");

  const options = pipelinesList.map((p) => `<option value="${p.slug}">${escapeHtml(p.name)}</option>`).join("");
  const previousA = comparePipelineAEl.value;
  const previousB = comparePipelineBEl.value;
  comparePipelineAEl.innerHTML = options;
  comparePipelineBEl.innerHTML = options;
  if (pipelinesList.some((p) => p.slug === previousA)) comparePipelineAEl.value = previousA;
  if (pipelinesList.some((p) => p.slug === previousB)) comparePipelineBEl.value = previousB;
  else if (pipelinesList.length > 1) comparePipelineBEl.value = pipelinesList[1].slug;
}

comparePipelinesBtn.addEventListener("click", async () => {
  const a = comparePipelineAEl.value;
  const b = comparePipelineBEl.value;
  if (!a || !b) return;

  pipelineCompareResultEl.classList.remove("hidden");
  if (a === b) {
    pipelineCompareResultEl.innerHTML = `<div class="runs-empty">Pick two different pipelines to compare.</div>`;
    return;
  }

  try {
    const res = await fetch(`/api/pipelines/compare?a=${a}&b=${b}`);
    const body = await res.json();
    if (!res.ok) {
      pipelineCompareResultEl.innerHTML = `<div class="runs-empty">${escapeHtml(body.detail || "Could not compare these pipelines.")}</div>`;
      return;
    }

    const diffKeys = Object.keys(body.diff);
    pipelineCompareResultEl.innerHTML = diffKeys.length
      ? diffKeys
          .map(
            (key) => `
            <div class="schedule-row">
              <div class="schedule-row-main">
                <strong>${escapeHtml(key)}</strong>
                <span class="schedule-row-meta">${escapeHtml(body.a.name)}: ${escapeHtml(JSON.stringify(body.diff[key].a))}</span>
                <span class="schedule-row-meta">${escapeHtml(body.b.name)}: ${escapeHtml(JSON.stringify(body.diff[key].b))}</span>
              </div>
            </div>`
          )
          .join("")
      : `<div class="runs-empty">These pipelines are identical.</div>`;
  } catch (err) {
    pipelineCompareResultEl.innerHTML = `<div class="runs-empty">Compare failed: ${err}</div>`;
  }
});

const TIER_NODE_COLOR = { automation: "#38bdf8", workflow: "#a78bfa", agent: "#34d399" };

function renderDagSvg(graph) {
  const nodeWidth = 150;
  const nodeHeight = 42;
  const gapX = 70;
  const rowY = 90;
  const maxSkip = Math.max(1, ...graph.edges.filter((e) => e.kind === "mapping").map((e) => e.to - e.from));
  const topMargin = 24 + maxSkip * 26;
  const width = graph.nodes.length * (nodeWidth + gapX) + gapX;
  const height = topMargin + rowY + nodeHeight + 20;

  const centerX = (i) => gapX + i * (nodeWidth + gapX) + nodeWidth / 2;
  const nodeY = topMargin + rowY;

  const nodeBoxes = graph.nodes
    .map((n) => {
      const x = gapX + n.index * (nodeWidth + gapX);
      const color = TIER_NODE_COLOR[n.tier] || "#94a3b8";
      const tooltip = n.note ? `${n.name}\n\n${n.note}` : n.name;
      return `
        <g>
          <title>${escapeHtml(tooltip)}</title>
          <rect x="${x}" y="${nodeY}" width="${nodeWidth}" height="${nodeHeight}" rx="8"
                fill="rgba(255,255,255,0.03)" stroke="${color}" stroke-width="1.5"></rect>
          <text x="${x + nodeWidth / 2}" y="${nodeY + 17}" text-anchor="middle" font-size="10"
                fill="${color}" font-family="monospace">${escapeHtml(n.tier)}</text>
          <text x="${x + nodeWidth / 2}" y="${nodeY + 31}" text-anchor="middle" font-size="12"
                fill="var(--text, #e2e8f0)" font-family="monospace">${escapeHtml(n.name)}</text>
          ${n.note ? `<circle cx="${x + nodeWidth - 10}" cy="${nodeY + 10}" r="4" fill="${color}" opacity="0.7"></circle>` : ""}
        </g>`;
    })
    .join("");

  const sequenceEdges = graph.edges
    .filter((e) => e.kind === "sequence")
    .map((e) => {
      const x1 = centerX(e.from) + nodeWidth / 2;
      const x2 = centerX(e.to) - nodeWidth / 2;
      const y = nodeY + nodeHeight / 2;
      return `<line x1="${x1}" y1="${y}" x2="${x2}" y2="${y}" stroke="var(--text-dim, #64748b)" stroke-width="1.5" marker-end="url(#arrow-seq)"></line>`;
    })
    .join("");

  const mappingEdges = graph.edges
    .filter((e) => e.kind === "mapping")
    .map((e) => {
      const skip = e.to - e.from;
      const x1 = centerX(e.from);
      const x2 = centerX(e.to);
      const peakY = nodeY - 16 - skip * 26;
      const path = `M ${x1} ${nodeY} Q ${(x1 + x2) / 2} ${peakY} ${x2} ${nodeY}`;
      const labelY = peakY + 4;
      return `
        <path d="${path}" fill="none" stroke="#facc15" stroke-width="1.5" marker-end="url(#arrow-map)"></path>
        <text x="${(x1 + x2) / 2}" y="${labelY}" text-anchor="middle" font-size="10" fill="#facc15" font-family="monospace">${escapeHtml(e.field)} ← ${escapeHtml(e.output)}</text>`;
    })
    .join("");

  return `
    <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="arrow-seq" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
          <path d="M0,0 L8,4 L0,8 Z" fill="var(--text-dim, #64748b)"></path>
        </marker>
        <marker id="arrow-map" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
          <path d="M0,0 L8,4 L0,8 Z" fill="#facc15"></path>
        </marker>
      </defs>
      ${sequenceEdges}
      ${mappingEdges}
      ${nodeBoxes}
    </svg>`;
}

async function togglePipelineGraph(slug) {
  const card = document.getElementById(`pipeline-card__${slug}`);
  const panel = card.querySelector(".dag-panel");

  if (!panel.classList.contains("hidden")) {
    panel.classList.add("hidden");
    return;
  }

  panel.classList.remove("hidden");
  panel.innerHTML = `<p class="card-desc">Loading graph…</p>`;

  try {
    const res = await fetch(`/api/pipelines/${slug}/graph`);
    const graph = await res.json();
    panel.innerHTML = `
      <div class="dag-toolbar">
        <button class="btn btn-secondary btn-small" data-dag-download-btn>Download SVG</button>
      </div>
      <div class="dag-scroll">${renderDagSvg(graph)}</div>`;
    panel.querySelector("[data-dag-download-btn]").addEventListener("click", () => downloadPipelineDagSvg(slug, panel));
  } catch (err) {
    panel.innerHTML = `<p class="card-desc">Could not load graph: ${err}</p>`;
  }
}

function downloadPipelineDagSvg(slug, panel) {
  const svgEl = panel.querySelector("svg");
  if (!svgEl) return;
  const serialized = new XMLSerializer().serializeToString(svgEl);
  const svgContent = `<?xml version="1.0" encoding="UTF-8"?>\n${serialized}`;
  const blob = new Blob([svgContent], { type: "image/svg+xml" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${slug}_dag.svg`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function togglePipelineHistory(slug) {
  const card = document.getElementById(`pipeline-card__${slug}`);
  const panel = card.querySelector(".history-panel");

  if (!panel.classList.contains("hidden")) {
    panel.classList.add("hidden");
    return;
  }

  panel.classList.remove("hidden");
  panel.innerHTML = `<p class="card-desc">Loading version history…</p>`;

  try {
    const res = await fetch(`/api/pipelines/${slug}/versions`);
    const versions = await res.json();
    renderPipelineHistory(slug, panel, versions);
  } catch (err) {
    panel.innerHTML = `<p class="card-desc">Could not load version history: ${err}</p>`;
  }
}

function renderPipelineHistory(slug, panel, versions) {
  if (!versions.length) {
    panel.innerHTML = `<p class="card-desc">No earlier versions yet — saved once, no overwrites recorded.</p>`;
    return;
  }

  const rows = versions
    .map(
      (v) => `
      <div class="schedule-row" data-version-row="${v.version_id}">
        <input type="checkbox" class="version-compare-checkbox" data-version-id="${v.version_id}" title="Select to compare against another version" />
        <div class="schedule-row-main">
          <strong>${new Date(v.saved_at).toLocaleString()}</strong>
          <span class="schedule-row-meta">version ${escapeHtml(v.version_id)}</span>
        </div>
        <div class="schedule-row-actions">
          <button class="btn btn-secondary btn-small" data-view-version="${v.version_id}" type="button">View diff</button>
          <button class="btn btn-secondary btn-small" data-restore-version="${v.version_id}" type="button">Restore</button>
          <button class="btn btn-secondary btn-small" data-branch-version="${v.version_id}" type="button" title="Save this version as a brand-new pipeline instead of overwriting the current one">Branch as new…</button>
        </div>
      </div>
      <div class="version-diff-detail hidden" data-diff-for="${v.version_id}"></div>`
    )
    .join("");

  panel.innerHTML = `
    <div class="version-compare-bar">
      <span class="schedule-row-meta">Select two versions to compare them against each other:</span>
      <button class="btn btn-secondary btn-small" id="version-compare-btn-${slug}" type="button" disabled>Compare selected</button>
    </div>
    <div class="version-compare-result" id="version-compare-result-${slug}"></div>
    <div class="version-prune-bar">
      <label class="schedule-row-meta" for="version-prune-keep-${slug}">Keep latest</label>
      <input type="number" id="version-prune-keep-${slug}" min="0" step="1" value="5" style="width: 4em;" />
      <button class="btn btn-secondary btn-small" id="version-prune-btn-${slug}" type="button" title="Delete older archived versions of this pipeline, keeping only the newest N">Prune older versions</button>
    </div>
    <div class="version-history-list">${rows}</div>`;

  const compareBtn = panel.querySelector(`#version-compare-btn-${slug}`);
  const compareResultEl = panel.querySelector(`#version-compare-result-${slug}`);
  const pruneKeepInput = panel.querySelector(`#version-prune-keep-${slug}`);
  const pruneBtn = panel.querySelector(`#version-prune-btn-${slug}`);

  pruneBtn.addEventListener("click", async () => {
    const keep = Number(pruneKeepInput.value);
    if (!Number.isInteger(keep) || keep < 0) {
      showToast("Enter a whole number of versions to keep.", "error");
      return;
    }
    if (!confirm(`Delete every archived version of this pipeline older than the newest ${keep}? This cannot be undone.`)) return;
    try {
      const res = await fetch(`/api/pipelines/${slug}/versions/prune?keep=${keep}`, { method: "POST" });
      const body = await res.json();
      if (!res.ok) {
        showToast(body.detail || "Failed to prune version history.", "error");
        return;
      }
      showToast(`Pruned ${body.deleted} old version(s).`, "success");
      const refreshed = await fetch(`/api/pipelines/${slug}/versions`);
      renderPipelineHistory(slug, panel, await refreshed.json());
    } catch (err) {
      showToast(`Prune failed: ${err}`, "error");
    }
  });

  panel.querySelectorAll(".version-compare-checkbox").forEach((checkbox) => {
    checkbox.addEventListener("change", () => {
      const checkedCount = panel.querySelectorAll(".version-compare-checkbox:checked").length;
      compareBtn.disabled = checkedCount !== 2;
    });
  });

  compareBtn.addEventListener("click", async () => {
    const checked = [...panel.querySelectorAll(".version-compare-checkbox:checked")];
    if (checked.length !== 2) return;
    const [versionA, versionB] = checked.map((cb) => cb.dataset.versionId);

    compareResultEl.innerHTML = `<p class="card-desc">Comparing…</p>`;
    try {
      const res = await fetch(`/api/pipelines/${slug}/versions/${versionA}/compare/${versionB}`);
      const body = await res.json();
      if (!res.ok) {
        compareResultEl.innerHTML = `<p class="card-desc">${escapeHtml(body.detail || "Could not compare these versions.")}</p>`;
        return;
      }
      const diffKeys = Object.keys(body.diff);
      compareResultEl.innerHTML = diffKeys.length
        ? diffKeys
            .map(
              (key) => `
              <div class="schedule-row">
                <div class="schedule-row-main">
                  <strong>${escapeHtml(key)}</strong>
                  <span class="schedule-row-meta">version ${escapeHtml(versionA)}: ${escapeHtml(JSON.stringify(body.diff[key].a))}</span>
                  <span class="schedule-row-meta">version ${escapeHtml(versionB)}: ${escapeHtml(JSON.stringify(body.diff[key].b))}</span>
                </div>
              </div>`
            )
            .join("")
        : `<p class="card-desc">These two versions are identical.</p>`;
    } catch (err) {
      compareResultEl.innerHTML = `<p class="card-desc">Compare failed: ${err}</p>`;
    }
  });

  panel.querySelectorAll("[data-view-version]").forEach((btn) => {
    btn.addEventListener("click", () => togglePipelineVersionDiff(slug, panel, btn.dataset.viewVersion));
  });

  panel.querySelectorAll("[data-restore-version]").forEach((btn) => {
    btn.addEventListener("click", () => restorePipelineVersion(slug, btn.dataset.restoreVersion));
  });

  panel.querySelectorAll("[data-branch-version]").forEach((btn) => {
    btn.addEventListener("click", () => branchPipelineVersion(slug, btn.dataset.branchVersion));
  });
}

async function branchPipelineVersion(slug, versionId) {
  const newName = prompt("Save this version as a new pipeline named:");
  if (!newName || !newName.trim()) return;

  try {
    const res = await fetch(`/api/pipelines/${slug}/versions/${versionId}/branch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ new_name: newName.trim() }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Branch failed.", "error");
      return;
    }
    showToast(`Created "${body.pipeline.name}" from this version.`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Branch failed: ${err}`, "error");
  }
}

async function togglePipelineVersionDiff(slug, panel, versionId) {
  const detail = panel.querySelector(`[data-diff-for="${versionId}"]`);
  if (!detail.classList.contains("hidden")) {
    detail.classList.add("hidden");
    return;
  }

  detail.classList.remove("hidden");
  detail.innerHTML = `<p class="card-desc">Loading diff…</p>`;

  try {
    const res = await fetch(`/api/pipelines/${slug}/versions/${versionId}`);
    const body = await res.json();
    const diffKeys = Object.keys(body.diff);
    if (!diffKeys.length) {
      detail.innerHTML = `<p class="card-desc">No differences from the current definition.</p>`;
      return;
    }
    detail.innerHTML = diffKeys
      .map(
        (key) => `
        <div class="schedule-row">
          <div class="schedule-row-main">
            <strong>${escapeHtml(key)}</strong>
            <span class="schedule-row-meta">this version: ${escapeHtml(JSON.stringify(body.diff[key].a))}</span>
            <span class="schedule-row-meta">current: ${escapeHtml(JSON.stringify(body.diff[key].b))}</span>
          </div>
        </div>`
      )
      .join("");
  } catch (err) {
    detail.innerHTML = `<p class="card-desc">Could not load diff: ${err}</p>`;
  }
}

async function restorePipelineVersion(slug, versionId) {
  try {
    const res = await fetch(`/api/pipelines/${slug}/versions/${versionId}/restore`, { method: "POST" });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Restore failed.", "error");
      return;
    }
    showToast(`Restored "${body.pipeline.name}" to this version.`, "success");
    await loadSavedPipelines();
    await loadAuditLog();
  } catch (err) {
    showToast(`Restore failed: ${err}`, "error");
  }
}

async function loadSavedPipelines() {
  const tagFilter = pipelineTagFilterInput.value.trim();
  const url = tagFilter ? `/api/pipelines?tag=${encodeURIComponent(tagFilter)}` : "/api/pipelines";
  const res = await fetch(url);
  const pipelinesList = await res.json();
  renderSavedPipelines(pipelinesList);
  return pipelinesList;
}

async function savePipelineTags(slug, tags) {
  const res = await fetch(`/api/pipelines/${encodeURIComponent(slug)}/tags`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tags }),
  });
  return res.json();
}

let pipelineTagFilterDebounce = null;
pipelineTagFilterInput.addEventListener("input", () => {
  clearTimeout(pipelineTagFilterDebounce);
  pipelineTagFilterDebounce = setTimeout(() => loadSavedPipelines(), 250);
});

let pipelineDeepSearchDebounce = null;

async function runPipelineDeepSearch(query) {
  if (!query.trim()) {
    pipelineDeepSearchResultsEl.classList.add("hidden");
    savedPipelinesGrid.classList.remove("hidden");
    return;
  }

  const res = await fetch(`/api/pipelines/search?q=${encodeURIComponent(query)}`);
  const body = await res.json();

  savedPipelinesGrid.classList.add("hidden");
  pipelineDeepSearchResultsEl.classList.remove("hidden");

  if (!body.results.length) {
    pipelineDeepSearchResultsEl.innerHTML = `<div class="runs-empty">No saved pipeline matches "${escapeHtml(query)}".</div>`;
    return;
  }

  pipelineDeepSearchResultsEl.innerHTML = body.results
    .map(
      (r) => `
      <div class="schedule-row">
        <div class="schedule-row-main">
          <strong>${escapeHtml(r.slug)}</strong>
          <span class="schedule-row-meta">…${escapeHtml(r.snippet)}…</span>
        </div>
      </div>`
    )
    .join("");
}

pipelineDeepSearchInput.addEventListener("input", () => {
  clearTimeout(pipelineDeepSearchDebounce);
  const query = pipelineDeepSearchInput.value;
  pipelineDeepSearchDebounce = setTimeout(() => runPipelineDeepSearch(query), 250);
});

// ---- Pipeline starter templates ----

let cachedPipelineTemplates = [];
const pipelineTemplateFilterInput = document.getElementById("pipeline-template-filter");

function renderPipelineTemplates(templatesList) {
  cachedPipelineTemplates = templatesList;
  applyPipelineTemplateFilter();
}

function applyPipelineTemplateFilter() {
  const query = pipelineTemplateFilterInput.value.trim().toLowerCase();
  const filtered = query
    ? cachedPipelineTemplates.filter((t) => {
        const chainText = t.steps.map((s) => `${s.tier} ${s.name}`).join(" ");
        const haystack = `${t.name} ${t.description} ${chainText}`.toLowerCase();
        return haystack.includes(query);
      })
    : cachedPipelineTemplates;

  if (!filtered.length) {
    pipelineTemplatesGrid.innerHTML = `<p class="card-desc">No templates match your filter.</p>`;
    return;
  }

  pipelineTemplatesGrid.innerHTML = filtered
    .map((t) => {
      const chain = t.steps.map((s) => `[${s.tier}] ${s.name}`).join(" → ");
      return `
        <div class="card">
          <div class="card-head">
            <h3 class="card-title">${t.name}</h3>
          </div>
          <p class="card-desc">${t.description}</p>
          <p class="card-desc pipeline-chain">${chain}</p>
          <div class="pipeline-card-actions">
            <button class="btn btn-run" data-template-id="${t.id}" type="button">Use this template</button>
          </div>
        </div>`;
    })
    .join("");

  pipelineTemplatesGrid.querySelectorAll("[data-template-id]").forEach((btn) => {
    btn.addEventListener("click", () => cloneTemplate(btn.dataset.templateId, btn));
  });
}

async function loadPipelineTemplates() {
  const res = await fetch("/api/pipeline-templates");
  const templatesList = await res.json();
  renderPipelineTemplates(templatesList);
  return templatesList;
}

pipelineTemplateFilterInput.addEventListener("input", () => applyPipelineTemplateFilter());

async function cloneTemplate(templateId, btn) {
  btn.disabled = true;
  try {
    const res = await fetch(`/api/pipeline-templates/${templateId}/clone`, { method: "POST" });
    if (!res.ok) {
      const err = await res.json();
      showToast(err.detail || "Failed to clone this template.", "error");
      return;
    }
    const { pipeline } = await res.json();
    showToast(`Added "${pipeline.name}" to your saved pipelines.`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Clone failed: ${err}`, "error");
  } finally {
    btn.disabled = false;
  }
}

async function deleteSavedPipeline(slug, name) {
  if (!confirm(`Delete "${name}"? This can't be undone from here.`)) {
    return;
  }
  try {
    const res = await fetch(`/api/pipelines/${slug}`, { method: "DELETE" });
    if (!res.ok) {
      const err = await res.json();
      showToast(err.detail || "Failed to delete this pipeline.", "error");
      return;
    }
    showToast(`Deleted "${name}".`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Delete failed: ${err}`, "error");
  }
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

async function renamePipeline(slug, currentName) {
  const newName = prompt(`Rename "${currentName}" to:`, currentName);
  if (!newName || !newName.trim() || newName.trim() === currentName) return;
  try {
    const res = await fetch(`/api/pipelines/${slug}/rename`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: newName.trim() }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Failed to rename this pipeline.", "error");
      return;
    }
    showToast(`Renamed to "${body.pipeline.name}".`, "success");
    await loadSavedPipelines();
  } catch (err) {
    showToast(`Rename failed: ${err}`, "error");
  }
}

async function runSavedPipeline(slug, name, pipelinesList) {
  const card = document.getElementById(`pipeline-card__${slug}`);
  card.scrollIntoView({ behavior: "smooth", block: "center" });
  recordRecentlyViewed(`pipeline::${slug}`);

  const button = card.querySelector(".btn-run");
  const tracker = card.querySelector(".tracker");
  const logContainer = card.querySelector(".log-tabs-wrap");

  const definition = pipelinesList.find((p) => p.slug === slug);
  const trackerSteps = (definition?.steps || []).map((s) =>
    s.type === "parallel"
      ? { parallel: true, name: s.name || "", branches: (s.branches || []).map((b) => ({ tier: b.tier, name: b.name })) }
      : { tier: s.tier, name: s.name }
  );

  button.disabled = true;
  renderTracker(tracker, trackerSteps);
  tracker.classList.remove("hidden");
  logContainer.classList.remove("hidden");
  const log = createRunLog();
  renderRunLogTabs(logContainer, log, null);

  try {
    const { stream_id } = await fetchRunTrigger(`/api/pipelines/${slug}/run`, { method: "POST" });

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
        highlightSlowestStep(tracker);
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

async function sendTestWebhook(slug, name, pipelinesList) {
  const card = document.getElementById(`pipeline-card__${slug}`);
  card.scrollIntoView({ behavior: "smooth", block: "center" });

  const button = card.querySelector(".webhook-test-btn");
  const tracker = card.querySelector(".tracker");
  const logContainer = card.querySelector(".log-tabs-wrap");

  const definition = pipelinesList.find((p) => p.slug === slug);
  const trackerSteps = (definition?.steps || []).map((s) =>
    s.type === "parallel"
      ? { parallel: true, name: s.name || "", branches: (s.branches || []).map((b) => ({ tier: b.tier, name: b.name })) }
      : { tier: s.tier, name: s.name }
  );

  button.disabled = true;
  renderTracker(tracker, trackerSteps);
  tracker.classList.remove("hidden");
  logContainer.classList.remove("hidden");
  const log = createRunLog();
  renderRunLogTabs(logContainer, log, null);

  try {
    const { stream_id } = await fetchRunTrigger(`/api/pipelines/${slug}/webhook`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });

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
        highlightSlowestStep(tracker);
        showToast(
          success
            ? `Test webhook for "${name}" completed successfully.`
            : `Test webhook for "${name}" failed: ${event.error}`,
          success ? "success" : "error"
        );
        await refreshTelemetry();
      },
    });
  } catch (err) {
    button.disabled = false;
    showToast(`Test webhook for "${name}" failed: ${err}`, "error");
  }
}

// ---- Data ingestion (CSV/PDF upload, dedupe, artifacts, purge) ----

const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("file-input");
const ingestionResultEl = document.getElementById("ingestion-result");
const artifactsListEl = document.getElementById("artifacts-list");
const artifactsStatsSummaryEl = document.getElementById("artifacts-stats-summary");
const purgeHoursInput = document.getElementById("purge-hours");
const purgeBtn = document.getElementById("purge-btn");
const artifactSearchInput = document.getElementById("artifact-search");
const artifactSearchResultsEl = document.getElementById("artifact-search-results");
const artifactTagFilterInput = document.getElementById("artifact-tag-filter");
const artifactTagDirectoryEl = document.getElementById("artifact-tag-directory");
const artifactBulkTagInput = document.getElementById("artifact-bulk-tag-input");
const artifactBulkTagBtn = document.getElementById("artifact-bulk-tag-btn");
const artifactBulkUntagBtn = document.getElementById("artifact-bulk-untag-btn");
const artifactCompareBtn = document.getElementById("artifact-compare-btn");
const artifactCompareResultEl = document.getElementById("artifact-compare-result");
const artifactBulkFavoriteBtn = document.getElementById("artifact-bulk-favorite-btn");
const artifactBulkDownloadBtn = document.getElementById("artifact-bulk-download-btn");
const artifactBulkDeleteBtn = document.getElementById("artifact-bulk-delete-btn");

const selectedArtifactNames = new Set();

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

async function saveArtifactTags(filename, tags) {
  const res = await fetch(`/api/artifacts/${encodeURIComponent(filename)}/tags`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tags }),
  });
  return res.json();
}

function updateArtifactBulkTagBtn() {
  artifactBulkTagBtn.disabled = selectedArtifactNames.size === 0;
  artifactBulkTagBtn.textContent = selectedArtifactNames.size
    ? `Apply tag to selected (${selectedArtifactNames.size})`
    : "Apply tag to selected";
  artifactBulkUntagBtn.disabled = selectedArtifactNames.size === 0;
  artifactBulkUntagBtn.textContent = selectedArtifactNames.size
    ? `Remove tag from selected (${selectedArtifactNames.size})`
    : "Remove tag from selected";
  artifactCompareBtn.disabled = selectedArtifactNames.size !== 2;
  artifactBulkFavoriteBtn.disabled = selectedArtifactNames.size === 0;
  artifactBulkFavoriteBtn.textContent = selectedArtifactNames.size
    ? `★ Favorite selected (${selectedArtifactNames.size})`
    : "★ Favorite selected";
  artifactBulkDownloadBtn.disabled = selectedArtifactNames.size === 0;
  artifactBulkDownloadBtn.textContent = selectedArtifactNames.size
    ? `⬇ Download selected (${selectedArtifactNames.size})`
    : "⬇ Download selected";
  artifactBulkDeleteBtn.disabled = selectedArtifactNames.size === 0;
  artifactBulkDeleteBtn.textContent = selectedArtifactNames.size
    ? `Delete selected (${selectedArtifactNames.size})`
    : "Delete selected";
}

async function loadArtifactTagDirectory() {
  const res = await fetch("/api/artifacts/tags-summary");
  const summary = await res.json();

  if (!summary.length) {
    artifactTagDirectoryEl.classList.add("hidden");
    artifactTagDirectoryEl.innerHTML = "";
    return;
  }

  artifactTagDirectoryEl.classList.remove("hidden");
  artifactTagDirectoryEl.innerHTML = summary
    .map(
      (entry) => `
      <span class="tag-directory-entry">
        <button type="button" class="tag-directory-chip" data-tag="${escapeHtml(entry.tag)}">${escapeHtml(entry.tag)} <span class="tag-directory-count">${entry.count}</span></button>
        <button type="button" class="tag-directory-rename-btn" data-tag="${escapeHtml(entry.tag)}" title="Rename this tag everywhere it's used">✎</button>
      </span>`
    )
    .join("");

  artifactTagDirectoryEl.querySelectorAll(".tag-directory-chip").forEach((chip) => {
    chip.addEventListener("click", () => {
      artifactTagFilterInput.value = chip.dataset.tag;
      loadArtifacts();
    });
  });

  artifactTagDirectoryEl.querySelectorAll(".tag-directory-rename-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const oldTag = btn.dataset.tag;
      const newTag = prompt(`Rename tag "${oldTag}" to:`, oldTag);
      if (!newTag || !newTag.trim() || newTag.trim() === oldTag) return;
      try {
        const res = await fetch("/api/artifacts/rename-tag", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ old_tag: oldTag, new_tag: newTag.trim() }),
        });
        const body = await res.json();
        if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
        showToast(`Renamed "${oldTag}" to "${newTag.trim()}" on ${body.renamed.length} file(s).`, "success");
        await loadArtifacts();
      } catch (err) {
        showToast(`Could not rename tag: ${err}`, "error");
      }
    });
  });
}

async function loadArtifacts() {
  const tagFilter = artifactTagFilterInput.value.trim();
  const url = tagFilter ? `/api/artifacts?tag=${encodeURIComponent(tagFilter)}` : "/api/artifacts";
  const res = await fetch(url);
  const files = await res.json();
  currentArtifacts = files;
  loadArtifactTagDirectory();

  const liveNames = new Set(files.map((f) => f.name));
  [...selectedArtifactNames].forEach((name) => {
    if (!liveNames.has(name)) selectedArtifactNames.delete(name);
  });

  const totalBytes = files.reduce((sum, f) => sum + (f.size_bytes || 0), 0);
  artifactsStatsSummaryEl.textContent = files.length
    ? `(${files.length} file${files.length === 1 ? "" : "s"}, ${formatBytes(totalBytes)})`
    : "";

  if (!files.length) {
    artifactsListEl.innerHTML = `<div class="runs-empty">${
      tagFilter ? `No artifacts tagged "${escapeHtml(tagFilter)}".` : "No artifacts yet — upload a CSV or PDF above."
    }</div>`;
    updateArtifactBulkTagBtn();
    renderFavoritesSection();
    renderRecentlyViewedSection();
    return;
  }

  const allSelected = files.every((f) => selectedArtifactNames.has(f.name));
  const rows = files
    .map((f) => {
      const tagChips = (f.tags || [])
        .map(
          (t) => `
          <span class="tag-chip">
            <span class="tag-chip-label">${escapeHtml(t)}</span>
            <button type="button" class="tag-chip-remove" data-artifact="${escapeHtml(f.name)}" data-tag="${escapeHtml(t)}" title="Remove tag">×</button>
          </span>`
        )
        .join("");
      const checked = selectedArtifactNames.has(f.name) ? "checked" : "";
      const favKey = `artifact::${f.name}`;
      return `
      <tr>
        <td><input type="checkbox" class="artifact-select-checkbox" data-artifact="${escapeHtml(f.name)}" ${checked} /></td>
        <td>${favoriteButtonHtml(favKey)} ${escapeHtml(f.name)}</td>
        <td>${formatBytes(f.size_bytes)}</td>
        <td>${new Date(f.modified_at * 1000).toLocaleString()}</td>
        <td class="artifact-tags-cell">
          <span class="tag-chip-list">${tagChips}</span>
          <input type="text" class="tag-add-input" data-artifact="${escapeHtml(f.name)}" placeholder="+ tag" />
        </td>
        <td>
          <button class="btn btn-secondary btn-small artifact-view-btn" data-artifact="${escapeHtml(f.name)}" type="button">View</button>
          <a class="btn btn-secondary btn-small" href="/api/artifacts/${encodeURIComponent(f.name)}/download" download title="Download the original file">⬇</a>
        </td>
      </tr>
      <tr class="artifact-content-row hidden" data-content-for="${escapeHtml(f.name)}">
        <td colspan="6"></td>
      </tr>`;
    })
    .join("");
  artifactsListEl.innerHTML = `
    <table>
      <thead><tr>
        <th><input type="checkbox" id="artifact-select-all" ${allSelected ? "checked" : ""} /></th>
        <th>File</th><th>Size</th><th>Modified</th><th>Tags</th><th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;

  artifactsListEl.querySelectorAll(".artifact-select-checkbox").forEach((checkbox) => {
    checkbox.addEventListener("change", () => {
      const filename = checkbox.dataset.artifact;
      if (checkbox.checked) selectedArtifactNames.add(filename);
      else selectedArtifactNames.delete(filename);
      updateArtifactBulkTagBtn();
      const selectAllCheckbox = document.getElementById("artifact-select-all");
      if (selectAllCheckbox) selectAllCheckbox.checked = files.every((f) => selectedArtifactNames.has(f.name));
    });
  });

  const selectAllCheckbox = document.getElementById("artifact-select-all");
  selectAllCheckbox.addEventListener("change", () => {
    if (selectAllCheckbox.checked) files.forEach((f) => selectedArtifactNames.add(f.name));
    else files.forEach((f) => selectedArtifactNames.delete(f.name));
    loadArtifacts();
  });

  updateArtifactBulkTagBtn();
  wireFavoriteToggles(artifactsListEl);
  renderFavoritesSection();
  renderRecentlyViewedSection();

  artifactsListEl.querySelectorAll(".artifact-view-btn").forEach((btn) => {
    btn.addEventListener("click", () => toggleArtifactContent(btn.dataset.artifact));
  });

  artifactsListEl.querySelectorAll(".tag-chip-remove").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const filename = btn.dataset.artifact;
      const removeTag = btn.dataset.tag;
      const file = files.find((f) => f.name === filename);
      const nextTags = (file.tags || []).filter((t) => t !== removeTag);
      await saveArtifactTags(filename, nextTags);
      await loadArtifacts();
    });
  });

  artifactsListEl.querySelectorAll(".tag-add-input").forEach((input) => {
    input.addEventListener("keydown", async (e) => {
      if (e.key !== "Enter") return;
      const filename = input.dataset.artifact;
      const newTag = input.value.trim();
      if (!newTag) return;
      const file = files.find((f) => f.name === filename);
      const nextTags = Array.from(new Set([...(file.tags || []), newTag]));
      await saveArtifactTags(filename, nextTags);
      await loadArtifacts();
    });
  });
}

function renderArtifactContent(body) {
  if (body.kind === "unsupported") {
    return `<p class="card-desc">${escapeHtml(body.message)}</p>`;
  }
  if (body.kind === "table") {
    if (!body.content.length) return `<p class="card-desc">No rows.</p>`;
    const columns = Object.keys(body.content[0]);
    const headerRow = columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
    const bodyRows = body.content
      .map((row) => `<tr>${columns.map((c) => `<td>${escapeHtml(row[c] ?? "")}</td>`).join("")}</tr>`)
      .join("");
    const schemaHtml = body.schema
      ? `<div class="artifact-schema-summary">
           <strong>Schema:</strong> ${body.schema.row_count} row${body.schema.row_count === 1 ? "" : "s"} ·
           ${body.schema.columns
             .map((c) => `<span class="schema-column-chip">${escapeHtml(c.name)}: ${escapeHtml(c.type)}</span>`)
             .join(" ")}
         </div>`
      : "";
    return `
      ${schemaHtml}
      <div class="artifact-content-table-wrap">
        <table><thead><tr>${headerRow}</tr></thead><tbody>${bodyRows}</tbody></table>
      </div>
      ${body.truncated ? `<p class="card-desc">Showing the first ${body.content.length} row(s) — truncated.</p>` : ""}`;
  }
  if (body.kind === "json") {
    return `<pre class="log-tab-content">${escapeHtml(JSON.stringify(body.content, null, 2))}</pre>`;
  }
  // "text"
  return `
    <pre class="log-tab-content">${escapeHtml(body.content)}</pre>
    ${body.truncated ? `<p class="card-desc">Truncated.</p>` : ""}`;
}

async function toggleArtifactContent(filename) {
  const row = artifactsListEl.querySelector(`.artifact-content-row[data-content-for="${CSS.escape(filename)}"]`);
  if (!row) return;
  const cell = row.querySelector("td");

  if (!row.classList.contains("hidden")) {
    row.classList.add("hidden");
    return;
  }

  row.classList.remove("hidden");
  recordRecentlyViewed(`artifact::${filename}`);
  cell.innerHTML = `<p class="card-desc">Loading…</p>`;
  try {
    const res = await fetch(`/api/artifacts/${encodeURIComponent(filename)}/content`);
    const body = await res.json();
    if (!res.ok) {
      cell.innerHTML = `<p class="card-desc">${escapeHtml(body.detail || "Could not load this artifact.")}</p>`;
      return;
    }
    cell.innerHTML = renderArtifactContent(body);
  } catch (err) {
    cell.innerHTML = `<p class="card-desc">Failed to load content: ${err}</p>`;
  }
}

let artifactTagFilterDebounce = null;
artifactTagFilterInput.addEventListener("input", () => {
  clearTimeout(artifactTagFilterDebounce);
  artifactTagFilterDebounce = setTimeout(() => loadArtifacts(), 250);
});

let artifactSearchDebounce = null;

async function runArtifactSearch(query) {
  if (!query.trim()) {
    artifactSearchResultsEl.classList.add("hidden");
    artifactsListEl.classList.remove("hidden");
    return;
  }

  const res = await fetch(`/api/artifacts/search?q=${encodeURIComponent(query)}`);
  const body = await res.json();

  artifactsListEl.classList.add("hidden");
  artifactSearchResultsEl.classList.remove("hidden");

  if (!body.results.length) {
    artifactSearchResultsEl.innerHTML = `<div class="runs-empty">No ingested content matches "${escapeHtml(query)}".</div>`;
    return;
  }

  artifactSearchResultsEl.innerHTML = body.results
    .map(
      (r) => `
      <div class="schedule-row">
        <div class="schedule-row-main">
          <strong>${escapeHtml(r.artifact)}</strong>
          <span class="schedule-row-meta">…${escapeHtml(r.snippet)}…</span>
        </div>
      </div>`
    )
    .join("");
}

artifactSearchInput.addEventListener("input", () => {
  clearTimeout(artifactSearchDebounce);
  const query = artifactSearchInput.value;
  artifactSearchDebounce = setTimeout(() => runArtifactSearch(query), 250);
});

async function uploadFile(file) {
  const lowerName = file.name.toLowerCase();
  const isPdf = lowerName.endsWith(".pdf");
  const isCsv = lowerName.endsWith(".csv");
  const isJson = lowerName.endsWith(".json");
  const isXlsx = lowerName.endsWith(".xlsx");
  if (!isPdf && !isCsv && !isJson && !isXlsx) {
    showToast("Only .csv, .pdf, .json, and .xlsx files are supported.", "error");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  ingestionResultEl.classList.remove("hidden");
  ingestionResultEl.innerHTML = `<pre class="log-tab-content">Uploading ${file.name}…</pre>`;

  const endpoint = isPdf ? "/api/ingest/pdf" : isJson ? "/api/ingest/json" : isXlsx ? "/api/ingest/xlsx" : "/api/ingest/csv";

  try {
    const res = await fetch(endpoint, { method: "POST", body: formData });
    const body = await res.json();

    if (!res.ok) {
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">Error: ${body.detail || "Upload failed."}</pre>`;
      showToast(`Upload failed: ${body.detail || "unknown error"}`, "error");
      return;
    }

    if (body.duplicate) {
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">This exact file was already ingested as "${body.original_filename}" at ${new Date(body.ingested_at).toLocaleString()}. Skipped.</pre>`;
      showToast(`Duplicate of "${body.original_filename}" — skipped.`, "error");
    } else if (isCsv || isJson || isXlsx) {
      const preview = JSON.stringify(body.preview, null, 2);
      const note = body.truncated ? `\n… (truncated — ${body.row_count} rows total)` : "";
      const cleaning = body.cleaning || {};
      const cleaningNotes = [];
      if (cleaning.blank_rows_removed) cleaningNotes.push(`${cleaning.blank_rows_removed} blank row(s) removed`);
      if (cleaning.duplicate_rows_removed) cleaningNotes.push(`${cleaning.duplicate_rows_removed} duplicate row(s) removed`);
      if (cleaning.cells_trimmed) cleaningNotes.push(`${cleaning.cells_trimmed} cell(s) trimmed`);
      const cleaningBanner = cleaningNotes.length
        ? `<div class="cached-badge">🧹 auto-cleansed: ${cleaningNotes.join(", ")}</div>`
        : "";
      ingestionResultEl.innerHTML = `${cleaningBanner}<pre class="log-tab-content">${preview}${note}</pre>`;
      showToast(
        cleaningNotes.length
          ? `Ingested ${body.filename}: ${body.row_count} row(s) (${cleaningNotes.join(", ")}).`
          : `Ingested ${body.filename}: ${body.row_count} row(s).`,
        "success"
      );
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

const ingestUrlInput = document.getElementById("ingest-url-input");
const ingestUrlBtn = document.getElementById("ingest-url-btn");

async function ingestFromUrl() {
  const url = ingestUrlInput.value.trim();
  if (!url) {
    showToast("Enter a URL first.", "error");
    return;
  }

  ingestionResultEl.classList.remove("hidden");
  ingestionResultEl.innerHTML = `<pre class="log-tab-content">Fetching ${escapeHtml(url)}…</pre>`;
  ingestUrlBtn.disabled = true;

  try {
    const res = await fetch("/api/ingest/url", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const body = await res.json();

    if (!res.ok) {
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">Error: ${escapeHtml(body.detail || "Fetch failed.")}</pre>`;
      showToast(`Ingest failed: ${body.detail || "unknown error"}`, "error");
      return;
    }

    if (body.duplicate) {
      ingestionResultEl.innerHTML = `<pre class="log-tab-content">This exact file was already ingested as "${escapeHtml(body.original_filename)}" at ${new Date(body.ingested_at).toLocaleString()}. Skipped.</pre>`;
      showToast(`Duplicate of "${body.original_filename}" — skipped.`, "error");
    } else {
      const preview = JSON.stringify(body.preview, null, 2);
      const note = body.truncated ? `\n… (truncated — ${body.row_count} rows total)` : "";
      const cleaning = body.cleaning || {};
      const cleaningNotes = [];
      if (cleaning.blank_rows_removed) cleaningNotes.push(`${cleaning.blank_rows_removed} blank row(s) removed`);
      if (cleaning.duplicate_rows_removed) cleaningNotes.push(`${cleaning.duplicate_rows_removed} duplicate row(s) removed`);
      if (cleaning.cells_trimmed) cleaningNotes.push(`${cleaning.cells_trimmed} cell(s) trimmed`);
      const cleaningBanner = cleaningNotes.length
        ? `<div class="cached-badge">🧹 auto-cleansed: ${cleaningNotes.join(", ")}</div>`
        : "";
      ingestionResultEl.innerHTML = `${cleaningBanner}<pre class="log-tab-content">${preview}${note}</pre>`;
      showToast(`Ingested ${body.filename}: ${body.row_count} row(s).`, "success");
    }

    ingestUrlInput.value = "";
    await loadArtifacts();
  } catch (err) {
    ingestionResultEl.innerHTML = `<pre class="log-tab-content">Request failed: ${err}</pre>`;
    showToast(`Ingest failed: ${err}`, "error");
  } finally {
    ingestUrlBtn.disabled = false;
  }
}

ingestUrlBtn.addEventListener("click", ingestFromUrl);
ingestUrlInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") ingestFromUrl();
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
  await loadAuditLog();
});

artifactBulkTagBtn.addEventListener("click", async () => {
  const tag = artifactBulkTagInput.value.trim();
  if (!tag) {
    showToast("Enter a tag first.", "error");
    return;
  }
  if (!selectedArtifactNames.size) return;

  try {
    const res = await fetch("/api/artifacts/bulk-tags", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filenames: [...selectedArtifactNames], tag }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Failed to apply tag.", "error");
      return;
    }
    showToast(`Tagged ${body.tagged.length} artifact(s) with "${tag}".`, "success");
    artifactBulkTagInput.value = "";
    await loadArtifacts();
  } catch (err) {
    showToast(`Bulk tag failed: ${err}`, "error");
  }
});

artifactBulkUntagBtn.addEventListener("click", async () => {
  const tag = artifactBulkTagInput.value.trim();
  if (!tag) {
    showToast("Enter a tag first.", "error");
    return;
  }
  if (!selectedArtifactNames.size) return;

  try {
    const res = await fetch("/api/artifacts/bulk-untag", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filenames: [...selectedArtifactNames], tag }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Failed to remove tag.", "error");
      return;
    }
    showToast(`Removed "${tag}" from ${body.untagged.length} artifact(s).`, "success");
    artifactBulkTagInput.value = "";
    await loadArtifacts();
  } catch (err) {
    showToast(`Bulk untag failed: ${err}`, "error");
  }
});

artifactCompareBtn.addEventListener("click", async () => {
  if (selectedArtifactNames.size !== 2) return;
  const [a, b] = [...selectedArtifactNames];

  artifactCompareResultEl.classList.remove("hidden");
  artifactCompareResultEl.innerHTML = `<div class="runs-empty">Comparing…</div>`;

  try {
    const res = await fetch(`/api/artifacts/compare-schema?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`);
    const body = await res.json();
    if (!res.ok) {
      artifactCompareResultEl.innerHTML = `<div class="runs-empty">${escapeHtml(body.detail || "Could not compare these artifacts.")}</div>`;
      return;
    }

    const rows = [];
    body.only_in_a.forEach((col) => rows.push({ label: col, meta: `Only in ${body.a.filename}` }));
    body.only_in_b.forEach((col) => rows.push({ label: col, meta: `Only in ${body.b.filename}` }));
    body.type_mismatches.forEach((m) =>
      rows.push({ label: m.column, meta: `${body.a.filename}: ${m.a_type} vs. ${body.b.filename}: ${m.b_type}` })
    );

    artifactCompareResultEl.innerHTML = rows.length
      ? rows
          .map(
            (row) => `
            <div class="schedule-row">
              <div class="schedule-row-main">
                <strong>${escapeHtml(row.label)}</strong>
                <span class="schedule-row-meta">${escapeHtml(row.meta)}</span>
              </div>
            </div>`
          )
          .join("") +
        `<div class="schedule-row-meta" style="padding: 6px 0;">${body.matching.length} matching column(s): ${escapeHtml(body.matching.join(", ") || "none")}</div>`
      : `<div class="runs-empty">These schemas match exactly (${body.matching.length} column(s)).</div>`;
  } catch (err) {
    artifactCompareResultEl.innerHTML = `<div class="runs-empty">Compare failed: ${err}</div>`;
  }
});

artifactBulkFavoriteBtn.addEventListener("click", () => {
  if (!selectedArtifactNames.size) return;
  selectedArtifactNames.forEach((filename) => favoriteKeys.add(`artifact::${filename}`));
  saveFavoriteKeys();
  renderFavoritesSection();
  showToast(`Favorited ${selectedArtifactNames.size} artifact(s).`, "success");
  loadArtifacts();
});

artifactBulkDownloadBtn.addEventListener("click", async () => {
  if (!selectedArtifactNames.size) return;
  try {
    const res = await fetch("/api/artifacts/bulk-download", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filenames: [...selectedArtifactNames] }),
    });
    if (!res.ok) {
      const body = await res.json();
      showToast(body.detail || "Failed to download selected artifacts.", "error");
      return;
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "artifacts_export.zip";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    showToast(`Downloaded ${selectedArtifactNames.size} artifact(s) as a zip.`, "success");
  } catch (err) {
    showToast(`Bulk download failed: ${err}`, "error");
  }
});

artifactBulkDeleteBtn.addEventListener("click", async () => {
  if (!selectedArtifactNames.size) return;
  if (!confirm(`Delete ${selectedArtifactNames.size} selected artifact(s)? This cannot be undone.`)) return;

  try {
    const res = await fetch("/api/artifacts/bulk-delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filenames: [...selectedArtifactNames] }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Bulk delete failed.", "error");
      return;
    }
    selectedArtifactNames.clear();
    showToast(`Deleted ${body.deleted.length} artifact(s).`, "success");
    await loadArtifacts();
  } catch (err) {
    showToast(`Bulk delete failed: ${err}`, "error");
  }
});

const runsPurgeHoursInput = document.getElementById("runs-purge-hours");
const runsPurgeBtn = document.getElementById("runs-purge-btn");

runsPurgeBtn.addEventListener("click", async () => {
  const hours = Number(runsPurgeHoursInput.value) || 0;
  const res = await fetch(`/api/runs/purge?older_than_hours=${hours}`, { method: "POST" });
  const body = await res.json();
  showToast(`Purged ${body.removed_count} old run(s) from history.`, "success");
  await refreshTelemetry();
  await loadAuditLog();
});

const runsBulkDeleteBtn = document.getElementById("runs-bulk-delete-btn");

runsBulkDeleteBtn.addEventListener("click", async () => {
  if (!selectedRunIds.size) return;
  const runIds = [...selectedRunIds];
  const res = await fetch("/api/runs/bulk-delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ run_ids: runIds }),
  });
  const body = await res.json();
  selectedRunIds.clear();
  showToast(`Deleted ${body.removed_count} selected run(s).`, "success");
  await refreshTelemetry();
  await loadAuditLog();
});

// ---- Full-text search across run history (step outputs + errors) ----

const runsSearchInput = document.getElementById("runs-search-input");
const runsSearchBtn = document.getElementById("runs-search-btn");
const runsSearchResultsEl = document.getElementById("runs-search-results");

async function searchRunHistory() {
  const query = runsSearchInput.value.trim();
  if (!query) {
    runsSearchResultsEl.classList.add("hidden");
    runsSearchResultsEl.innerHTML = "";
    return;
  }

  const res = await fetch(`/api/runs/search?q=${encodeURIComponent(query)}`);
  const body = await res.json();
  runsSearchResultsEl.classList.remove("hidden");

  if (!body.results.length) {
    runsSearchResultsEl.innerHTML = `<div class="runs-empty">No step outputs or errors match "${escapeHtml(query)}".</div>`;
    return;
  }

  runsSearchResultsEl.innerHTML = body.results
    .map(
      (r) => `
      <div class="runs-search-hit ${r.success ? "" : "hit-failed"}">
        <span class="hit-meta">Run #${r.run_id} · [${r.tier}] ${r.step} · in ${r.matched_in}${r.success ? "" : " · ❌ failed step"}</span>
        <span class="hit-snippet">…${escapeHtml(r.snippet)}…</span>
      </div>`
    )
    .join("");
}

runsSearchBtn.addEventListener("click", searchRunHistory);
runsSearchInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") searchRunHistory();
});
runsSearchInput.addEventListener("input", () => {
  if (!runsSearchInput.value.trim()) {
    runsSearchResultsEl.classList.add("hidden");
    runsSearchResultsEl.innerHTML = "";
  }
});

// ---- Search across run notes (distinct from step-output/error search above) ----

const runNotesSearchInput = document.getElementById("run-notes-search-input");
const runNotesSearchBtn = document.getElementById("run-notes-search-btn");
const runNotesSearchResultsEl = document.getElementById("run-notes-search-results");

async function searchRunNotes() {
  const query = runNotesSearchInput.value.trim();
  if (!query) {
    runNotesSearchResultsEl.classList.add("hidden");
    runNotesSearchResultsEl.innerHTML = "";
    return;
  }

  const res = await fetch(`/api/runs/search-notes?q=${encodeURIComponent(query)}`);
  const body = await res.json();
  runNotesSearchResultsEl.classList.remove("hidden");

  if (!body.results.length) {
    runNotesSearchResultsEl.innerHTML = `<div class="runs-empty">No run notes match "${escapeHtml(query)}".</div>`;
    return;
  }

  runNotesSearchResultsEl.innerHTML = body.results
    .map(
      (r) => `
      <div class="runs-search-hit">
        <span class="hit-meta">Run #${r.run_id}</span>
        <span class="hit-snippet">${escapeHtml(r.note)}</span>
      </div>`
    )
    .join("");

  runNotesSearchResultsEl.querySelectorAll(".hit-meta").forEach((el, i) => {
    el.style.cursor = "pointer";
    el.addEventListener("click", () => {
      const runId = body.results[i].run_id;
      recentRunsTableEl.scrollIntoView({ behavior: "smooth", block: "center" });
      toggleRunDetail(String(runId));
    });
  });
}

runNotesSearchBtn.addEventListener("click", searchRunNotes);
runNotesSearchInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") searchRunNotes();
});
runNotesSearchInput.addEventListener("input", () => {
  if (!runNotesSearchInput.value.trim()) {
    runNotesSearchResultsEl.classList.add("hidden");
    runNotesSearchResultsEl.innerHTML = "";
  }
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

// ---- Scheduler ----

function populateScheduleTargets() {
  const kind = scheduleKindEl.value;
  scheduleTargetEl.innerHTML = "";

  if (kind === "module") {
    TIER_ORDER.forEach((tier) => {
      (currentModulesByTier[tier] || []).forEach((m) => {
        const opt = document.createElement("option");
        opt.value = `${tier}::${m.name}`;
        opt.textContent = `[${tier}] ${m.name}`;
        scheduleTargetEl.appendChild(opt);
      });
    });
  } else {
    currentPipelines.forEach((p) => {
      const opt = document.createElement("option");
      opt.value = p.slug;
      opt.textContent = p.name;
      scheduleTargetEl.appendChild(opt);
    });
  }
}

function formatInterval(seconds) {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${Math.round(seconds / 3600)}h`;
}

function timeUntil(isoString) {
  const diffMs = new Date(isoString).getTime() - Date.now();
  if (diffMs <= 0) return "due now";
  return `in ${formatInterval(Math.round(diffMs / 1000))}`;
}

let cachedSchedules = [];

async function loadSchedules() {
  const res = await fetch("/api/schedules");
  cachedSchedules = await res.json();
  renderSchedulesList();
  renderFavoritesSection();
}

function renderSchedulesList() {
  const schedules = cachedSchedules;

  if (!schedules.length) {
    schedulesListEl.className = "runs-empty";
    schedulesListEl.textContent = "No schedules yet — recurring runs will appear here.";
    selectedScheduleIds.clear();
    updateSchedulesBulkButtons();
    return;
  }

  const liveIds = new Set(schedules.map((s) => s.id));
  [...selectedScheduleIds].forEach((id) => {
    if (!liveIds.has(id)) selectedScheduleIds.delete(id);
  });

  const filterQuery = scheduleFilterInput.value.trim().toLowerCase();
  const filtered = [
    ...(filterQuery
      ? schedules.filter((s) => {
          const haystack = `${s.kind} ${s.tier || ""} ${s.name}`.toLowerCase();
          return haystack.includes(filterQuery);
        })
      : schedules),
  ];

  if (scheduleSortSelect.value === "name") {
    filtered.sort((a, b) => a.name.localeCompare(b.name));
  } else {
    filtered.sort((a, b) => new Date(a.next_run_at) - new Date(b.next_run_at));
  }

  if (!filtered.length) {
    schedulesListEl.className = "runs-empty";
    schedulesListEl.textContent = "No schedules match your filter.";
    return;
  }

  schedulesListEl.className = "runs-table";
  schedulesListEl.innerHTML = filtered
    .map((s) => {
      const targetLabel = s.kind === "module" ? `[${s.tier}] ${s.name}` : `pipeline: ${s.name}`;
      const status = s.last_status
        ? `last: ${escapeHtml(s.last_status)}${s.last_run_at ? ` @ ${new Date(s.last_run_at).toLocaleTimeString()}` : ""}`
        : "never run yet";
      const WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
      const cadence =
        s.schedule_type === "daily"
          ? `daily at ${escapeHtml(s.daily_time)}`
          : s.schedule_type === "weekly"
            ? `weekly on ${WEEKDAY_NAMES[s.day_of_week]} at ${escapeHtml(s.daily_time)}`
            : s.schedule_type === "once"
              ? `once at ${new Date(s.next_run_at).toLocaleString()}`
              : `every ${formatInterval(s.interval_seconds)}`;
      const checked = selectedScheduleIds.has(s.id) ? "checked" : "";
      const labelBadge = s.label
        ? `<span class="schedule-row-label" data-schedule-label-text="${s.id}">${escapeHtml(s.label)}</span>`
        : "";
      return `
        <div class="schedule-row" id="schedule-row__${s.id}">
          <input type="checkbox" class="schedule-select-checkbox" data-schedule-id="${s.id}" ${checked} />
          <div class="schedule-row-main">
            <strong>${escapeHtml(targetLabel)}</strong>
            ${labelBadge}
            <span class="schedule-row-meta">${cadence} · next ${s.enabled ? timeUntil(s.next_run_at) : "paused"} · ${status}</span>
          </div>
          <div class="schedule-row-actions">
            ${favoriteButtonHtml(`schedule::${s.id}`)}
            <button class="btn btn-secondary btn-small" data-schedule-edit-label="${s.id}">${s.label ? "Edit label" : "Add label"}</button>
            <button class="btn btn-secondary btn-small" data-schedule-toggle="${s.id}" data-enabled="${s.enabled}">
              ${s.enabled ? "Pause" : "Resume"}
            </button>
            <button class="btn btn-secondary btn-small" data-schedule-duplicate="${s.id}">Duplicate</button>
            <button class="btn btn-secondary btn-small" data-schedule-delete="${s.id}">Delete</button>
          </div>
        </div>`;
    })
    .join("");

  schedulesListEl.querySelectorAll(".schedule-select-checkbox").forEach((checkbox) => {
    checkbox.addEventListener("change", () => {
      const id = Number(checkbox.dataset.scheduleId);
      if (checkbox.checked) selectedScheduleIds.add(id);
      else selectedScheduleIds.delete(id);
      updateSchedulesBulkButtons();
      schedulesSelectAllEl.checked = filtered.every((s) => selectedScheduleIds.has(s.id));
    });
  });
  wireFavoriteToggles(schedulesListEl);

  schedulesSelectAllEl.checked = filtered.every((s) => selectedScheduleIds.has(s.id));
  updateSchedulesBulkButtons();

  schedulesListEl.querySelectorAll("[data-schedule-duplicate]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const schedule = cachedSchedules.find((s) => String(s.id) === btn.dataset.scheduleDuplicate);
      if (schedule) fillScheduleFormFrom(schedule);
    });
  });

  schedulesListEl.querySelectorAll("[data-schedule-toggle]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.dataset.scheduleToggle;
      const enabled = btn.dataset.enabled === "true";
      await fetch(`/api/schedules/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: !enabled }),
      });
      await loadSchedules();
    });
  });

  schedulesListEl.querySelectorAll("[data-schedule-delete]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await fetch(`/api/schedules/${btn.dataset.scheduleDelete}`, { method: "DELETE" });
      await loadSchedules();
      showToast("Schedule deleted.", "success");
    });
  });

  schedulesListEl.querySelectorAll("[data-schedule-edit-label]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.dataset.scheduleEditLabel;
      const schedule = cachedSchedules.find((s) => String(s.id) === id);
      const next = prompt("Label for this schedule (blank to clear):", schedule ? schedule.label || "" : "");
      if (next === null) return;
      await fetch(`/api/schedules/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ label: next }),
      });
      await loadSchedules();
    });
  });
}

async function bulkSetSelectedSchedulesEnabled(enabled) {
  if (!selectedScheduleIds.size) return;
  await fetch("/api/schedules/bulk-set-enabled", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ schedule_ids: [...selectedScheduleIds], enabled }),
  });
  showToast(`${enabled ? "Resumed" : "Paused"} ${selectedScheduleIds.size} schedule(s).`, "success");
  await loadSchedules();
}

schedulesBulkPauseBtn.addEventListener("click", () => bulkSetSelectedSchedulesEnabled(false));
schedulesBulkResumeBtn.addEventListener("click", () => bulkSetSelectedSchedulesEnabled(true));

schedulesBulkFavoriteBtn.addEventListener("click", () => {
  if (!selectedScheduleIds.size) return;
  selectedScheduleIds.forEach((id) => favoriteKeys.add(`schedule::${id}`));
  saveFavoriteKeys();
  renderFavoritesSection();
  showToast(`Favorited ${selectedScheduleIds.size} schedule(s).`, "success");
});

schedulesBulkClearLabelBtn.addEventListener("click", async () => {
  if (!selectedScheduleIds.size) return;
  await fetch("/api/schedules/bulk-clear-label", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ schedule_ids: [...selectedScheduleIds] }),
  });
  showToast(`Cleared label on ${selectedScheduleIds.size} schedule(s).`, "success");
  await loadSchedules();
});

schedulesBulkExportBtn.addEventListener("click", async () => {
  if (!selectedScheduleIds.size) return;
  try {
    const res = await fetch("/api/schedules/bulk-export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ schedule_ids: [...selectedScheduleIds] }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "schedules_selected.json";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    showToast(`Exported ${selectedScheduleIds.size} schedule(s) as JSON.`, "success");
  } catch (err) {
    showToast(`Export failed: ${err}`, "error");
  }
});

schedulesSelectAllEl.addEventListener("change", () => {
  const checkboxes = schedulesListEl.querySelectorAll(".schedule-select-checkbox");
  checkboxes.forEach((checkbox) => {
    checkbox.checked = schedulesSelectAllEl.checked;
    const id = Number(checkbox.dataset.scheduleId);
    if (schedulesSelectAllEl.checked) selectedScheduleIds.add(id);
    else selectedScheduleIds.delete(id);
  });
  updateSchedulesBulkButtons();
});

schedulesBulkDeleteBtn.addEventListener("click", async () => {
  if (!selectedScheduleIds.size) return;
  if (!confirm(`Delete ${selectedScheduleIds.size} selected schedule(s)? This cannot be undone.`)) return;

  await fetch("/api/schedules/bulk-delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ schedule_ids: [...selectedScheduleIds] }),
  });
  showToast(`Deleted ${selectedScheduleIds.size} schedule(s).`, "success");
  selectedScheduleIds.clear();
  await loadSchedules();
});

function fillScheduleFormFrom(schedule) {
  scheduleFormEl.classList.remove("hidden");
  scheduleKindEl.value = schedule.kind;
  populateScheduleTargets();
  scheduleTargetEl.value = schedule.kind === "module" ? `${schedule.tier}::${schedule.name}` : schedule.name;

  scheduleFrequencyEl.value = schedule.schedule_type || "interval";
  scheduleFrequencyEl.dispatchEvent(new Event("change"));

  if (schedule.schedule_type === "daily") {
    scheduleDailyTimeEl.value = schedule.daily_time;
  } else if (schedule.schedule_type === "weekly") {
    scheduleWeeklyTimeEl.value = schedule.daily_time;
    scheduleDayOfWeekEl.value = String(schedule.day_of_week);
  } else if (schedule.schedule_type === "once") {
    // The original run_at has already passed by the time a fired schedule
    // could be duplicated -- leave it blank so the user must pick a new
    // future time rather than duplicating a create-time validation error.
    scheduleRunAtEl.value = "";
  } else {
    scheduleIntervalEl.value = schedule.interval_seconds;
  }

  scheduleFormEl.scrollIntoView({ behavior: "smooth", block: "center" });
  showToast("Schedule settings copied below — adjust and create.", "success");
}

scheduleAddToggleBtn.addEventListener("click", () => {
  scheduleFormEl.classList.toggle("hidden");
  if (!scheduleFormEl.classList.contains("hidden")) populateScheduleTargets();
});

scheduleKindEl.addEventListener("change", populateScheduleTargets);

scheduleFrequencyEl.addEventListener("change", () => {
  const frequency = scheduleFrequencyEl.value;
  scheduleIntervalFieldEl.classList.toggle("hidden", frequency !== "interval");
  scheduleDailyFieldEl.classList.toggle("hidden", frequency !== "daily");
  scheduleWeeklyFieldEl.classList.toggle("hidden", frequency !== "weekly");
  scheduleOnceFieldEl.classList.toggle("hidden", frequency !== "once");
});

scheduleCreateBtn.addEventListener("click", async () => {
  scheduleErrorEl.classList.add("hidden");
  const kind = scheduleKindEl.value;
  const target = scheduleTargetEl.value;
  const scheduleType = scheduleFrequencyEl.value;

  if (!target) {
    scheduleErrorEl.textContent = "No target available to schedule yet.";
    scheduleErrorEl.classList.remove("hidden");
    return;
  }

  const payload = { kind, schedule_type: scheduleType, inputs: {} };
  if (scheduleType === "daily") {
    payload.daily_time = scheduleDailyTimeEl.value;
  } else if (scheduleType === "weekly") {
    payload.daily_time = scheduleWeeklyTimeEl.value;
    payload.day_of_week = Number(scheduleDayOfWeekEl.value);
  } else if (scheduleType === "once") {
    payload.run_at = scheduleRunAtEl.value;
  } else {
    payload.interval_seconds = Number(scheduleIntervalEl.value);
  }
  if (kind === "module") {
    const [tier, name] = target.split("::");
    payload.tier = tier;
    payload.name = name;
  } else {
    payload.name = target;
  }

  const res = await fetch("/api/schedules", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    scheduleErrorEl.textContent = body.detail || "Could not create schedule.";
    scheduleErrorEl.classList.remove("hidden");
    return;
  }

  scheduleFormEl.classList.add("hidden");
  await loadSchedules();
  showToast("Schedule created.", "success");
});

async function loadSchedulerStatus() {
  const res = await fetch("/api/scheduler/status");
  const body = await res.json();
  schedulerPausedBannerEl.classList.toggle("hidden", !body.paused);
  schedulerPauseToggleBtn.textContent = body.paused ? "▶ Resume all" : "⏸ Pause all";
  schedulerPauseToggleBtn.classList.toggle("active", body.paused);
}

schedulerPauseToggleBtn.addEventListener("click", async () => {
  const isPaused = schedulerPauseToggleBtn.textContent.startsWith("▶");
  const endpoint = isPaused ? "/api/scheduler/resume" : "/api/scheduler/pause";
  await fetch(endpoint, { method: "POST" });
  await loadSchedulerStatus();
  showToast(isPaused ? "Scheduler resumed." : "Scheduler paused — no schedule will fire until resumed.", "success");
  await loadAuditLog();
});

loadSchedulerStatus();
setInterval(loadSchedulerStatus, 5000);
setInterval(loadSchedules, 5000);
setInterval(loadPerformance, 3000);

// ---- Agent memory ----

const memoryListEl = document.getElementById("memory-list");
const memoryClearBtn = document.getElementById("memory-clear-btn");
const memorySearchInput = document.getElementById("memory-search-input");

function renderMemoryRows(entries) {
  return entries
    .map(
      (entry) => `
      <div class="schedule-row">
        <div class="schedule-row-main">
          <strong>${escapeHtml(entry.key)}</strong>
          <span class="schedule-row-meta">${escapeHtml(JSON.stringify(entry.value))} · updated ${new Date(entry.updated_at).toLocaleString()}</span>
        </div>
        <div class="schedule-row-actions">
          <button class="btn btn-secondary btn-small" data-memory-delete="${encodeURIComponent(entry.key)}">Delete</button>
        </div>
      </div>`
    )
    .join("");
}

function wireMemoryDeleteButtons() {
  memoryListEl.querySelectorAll("[data-memory-delete]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await fetch(`/api/memory/${btn.dataset.memoryDelete}`, { method: "DELETE" });
      await loadMemory();
    });
  });
}

async function loadMemory() {
  const res = await fetch("/api/memory");
  const entries = await res.json();

  if (!entries.length) {
    memoryListEl.className = "runs-empty";
    memoryListEl.textContent = "Nothing remembered yet.";
    return;
  }

  memoryListEl.className = "runs-table";
  memoryListEl.innerHTML = renderMemoryRows(entries);
  wireMemoryDeleteButtons();
}

let memorySearchDebounce = null;
memorySearchInput.addEventListener("input", () => {
  const query = memorySearchInput.value.trim();
  clearTimeout(memorySearchDebounce);
  memorySearchDebounce = setTimeout(async () => {
    if (!query) {
      await loadMemory();
      return;
    }
    try {
      const res = await fetch(`/api/memory/search?q=${encodeURIComponent(query)}`);
      const body = await res.json();
      if (!body.results.length) {
        memoryListEl.className = "runs-empty";
        memoryListEl.textContent = "No memory entries match that search.";
        return;
      }
      memoryListEl.className = "runs-table";
      memoryListEl.innerHTML = renderMemoryRows(body.results);
      wireMemoryDeleteButtons();
    } catch (err) {
      memoryListEl.className = "runs-empty";
      memoryListEl.textContent = `Search failed: ${err}`;
    }
  }, 250);
});

memoryClearBtn.addEventListener("click", async () => {
  await fetch("/api/memory", { method: "DELETE" });
  await loadMemory();
  showToast("Agent memory cleared.", "success");
});

document.getElementById("memory-import-input").addEventListener("change", async (e) => {
  const file = e.target.files[0];
  if (!file) return;

  const formData = new FormData();
  formData.append("file", file);
  try {
    const res = await fetch("/api/memory/import", { method: "POST", body: formData });
    const body = await res.json();
    if (!res.ok) {
      showToast(`Import failed: ${body.detail || "unknown error"}`, "error");
      return;
    }
    showToast(`Imported ${body.imported.length} memory key(s).`, "success");
    await loadMemory();
  } catch (err) {
    showToast(`Import failed: ${err}`, "error");
  }
  e.target.value = "";
});

setInterval(loadMemory, 5000);

// ---- Per-module performance stats ----

const moduleStatsListEl = document.getElementById("module-stats-list");

async function loadModuleStats() {
  const res = await fetch("/api/modules/stats");
  const stats = await res.json();
  const withRuns = stats.filter((s) => s.total_runs > 0);

  if (!withRuns.length) {
    moduleStatsListEl.className = "runs-empty";
    moduleStatsListEl.textContent = "No runs recorded yet.";
    return;
  }

  moduleStatsListEl.className = "runs-table";
  moduleStatsListEl.innerHTML = withRuns
    .map(
      (s) => `
      <div class="schedule-row">
        <div class="schedule-row-main">
          <strong>[${escapeHtml(s.tier)}] ${escapeHtml(s.name)}</strong>
          <span class="schedule-row-meta">
            ${s.total_runs} run(s) · ${(s.success_rate * 100).toFixed(0)}% success
            ${s.avg_duration_seconds !== null ? ` · avg ${s.avg_duration_seconds.toFixed(2)}s` : ""}
          </span>
        </div>
      </div>`
    )
    .join("");
}

setInterval(loadModuleStats, 5000);

// ---- Environment & config viewer ----

const envVarsListEl = document.getElementById("env-vars-list");
const envSettingsListEl = document.getElementById("env-settings-list");
const envRefreshBtn = document.getElementById("env-refresh-btn");

async function loadEnvironment() {
  const res = await fetch("/api/environment");
  const body = await res.json();

  envVarsListEl.className = "runs-table";
  envVarsListEl.innerHTML = body.environment
    .map(
      (v) => `
      <div class="schedule-row">
        <div class="schedule-row-main">
          <strong>${escapeHtml(v.name)}${v.secret ? " 🔒" : ""}</strong>
          <span class="schedule-row-meta">${
            v.set ? escapeHtml(v.preview) : "not set" + (v.secret ? "" : " — declared in .env.example")
          }</span>
        </div>
        <span class="status-pill ${v.set ? "ready" : "error"}"><i class="dot dot-${v.set ? "ready" : "error"}"></i>${v.set ? "set" : "missing"}</span>
      </div>`
    )
    .join("");

  envSettingsListEl.className = "runs-table";
  envSettingsListEl.innerHTML = body.settings
    .map(
      (s) => `
      <div class="schedule-row">
        <div class="schedule-row-main">
          <strong>${escapeHtml(s.name)}</strong>
          <span class="schedule-row-meta">${escapeHtml(s.detail)}</span>
        </div>
        <span class="env-setting-value">${escapeHtml(s.value)}</span>
      </div>`
    )
    .join("");
}

envRefreshBtn.addEventListener("click", async () => {
  await loadEnvironment();
  showToast("Environment view refreshed.", "success");
});

const ratelimitMaxRequestsInput = document.getElementById("ratelimit-max-requests");
const ratelimitWindowSecondsInput = document.getElementById("ratelimit-window-seconds");
const ratelimitSaveBtn = document.getElementById("ratelimit-save-btn");
const ratelimitResetBtn = document.getElementById("ratelimit-reset-btn");

async function loadRateLimitConfig() {
  const res = await fetch("/api/ratelimit");
  const body = await res.json();
  ratelimitMaxRequestsInput.value = body.max_requests;
  ratelimitWindowSecondsInput.value = body.window_seconds;
}

ratelimitSaveBtn.addEventListener("click", async () => {
  const max_requests = Number(ratelimitMaxRequestsInput.value);
  const window_seconds = Number(ratelimitWindowSecondsInput.value);
  const res = await fetch("/api/ratelimit", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ max_requests, window_seconds }),
  });
  const body = await res.json();
  if (!res.ok) {
    showToast(body.detail || "Failed to update the rate limit.", "error");
    return;
  }
  showToast(`Rate limit set to ${body.max_requests} requests / ${body.window_seconds}s.`, "success");
  await loadEnvironment();
});

ratelimitResetBtn.addEventListener("click", async () => {
  const res = await fetch("/api/ratelimit", { method: "DELETE" });
  const body = await res.json();
  showToast(`Reverted to the default rate limit: ${body.max_requests} requests / ${body.window_seconds}s.`, "success");
  await loadRateLimitConfig();
  await loadEnvironment();
});

// ---- Recent actions audit trail ----

const auditLogListEl = document.getElementById("audit-log-list");
const auditLogRefreshBtn = document.getElementById("audit-log-refresh-btn");
const auditLogClearBtn = document.getElementById("audit-log-clear-btn");
const auditLogPurgeHoursInput = document.getElementById("audit-log-purge-hours");
const auditLogPurgeBtn = document.getElementById("audit-log-purge-btn");

const AUDIT_ACTION_LABELS = {
  run_purge: "Run purge",
  run_bulk_delete: "Bulk delete runs",
  artifact_purge: "Artifact purge",
  backup_restore: "Backup restore",
  circuit_breaker_reset: "Circuit breaker reset",
  scheduler_pause: "Scheduler paused",
  scheduler_resume: "Scheduler resumed",
  pipeline_version_restore: "Pipeline version restore",
};

function renderAuditLogRows(events, emptyMessage) {
  if (!events.length) return { className: "runs-empty", html: emptyMessage };
  const html = events
    .map(
      (e) => `
      <div class="schedule-row">
        <div class="schedule-row-main">
          <strong>${escapeHtml(AUDIT_ACTION_LABELS[e.action] || e.action)}</strong>
          <span class="schedule-row-meta">${escapeHtml(e.detail)}</span>
        </div>
        <span class="schedule-row-meta">${new Date(e.created_at).toLocaleString()}</span>
      </div>`
    )
    .join("");
  return { className: "runs-table", html };
}

async function loadAuditLog() {
  const res = await fetch("/api/audit-log");
  const events = await res.json();

  const { className, html } = renderAuditLogRows(events, "No administrative actions recorded yet.");
  auditLogListEl.className = className;
  auditLogListEl.innerHTML = html;
}

const auditLogSearchInput = document.getElementById("audit-log-search-input");
let auditLogSearchDebounce = null;
auditLogSearchInput.addEventListener("input", () => {
  const query = auditLogSearchInput.value.trim();
  clearTimeout(auditLogSearchDebounce);
  auditLogSearchDebounce = setTimeout(async () => {
    if (!query) {
      await loadAuditLog();
      return;
    }
    try {
      const res = await fetch(`/api/audit-log/search?q=${encodeURIComponent(query)}`);
      const body = await res.json();
      const { className, html } = renderAuditLogRows(body.results, "No actions match that search.");
      auditLogListEl.className = className;
      auditLogListEl.innerHTML = html;
    } catch (err) {
      auditLogListEl.className = "runs-empty";
      auditLogListEl.textContent = `Search failed: ${err}`;
    }
  }, 250);
});

auditLogRefreshBtn.addEventListener("click", async () => {
  await loadAuditLog();
  showToast("Recent actions refreshed.", "success");
});

auditLogClearBtn.addEventListener("click", async () => {
  if (!confirm("Clear the entire Recent Actions audit log? This cannot be undone.")) return;
  const res = await fetch("/api/audit-log/clear", { method: "POST" });
  const body = await res.json();
  showToast(`Cleared ${body.deleted} audit log entr${body.deleted === 1 ? "y" : "ies"}.`, "success");
  await loadAuditLog();
});

auditLogPurgeBtn.addEventListener("click", async () => {
  const hours = Number(auditLogPurgeHoursInput.value) || 0;
  const res = await fetch(`/api/audit-log/purge?older_than_hours=${hours}`, { method: "POST" });
  const body = await res.json();
  showToast(`Purged ${body.deleted} audit log entr${body.deleted === 1 ? "y" : "ies"} older than ${hours}h.`, "success");
  await loadAuditLog();
});

// ---- Regex tester ----

const regexPatternEl = document.getElementById("regex-pattern");
const regexFlagsEl = document.getElementById("regex-flags");
const regexTestStringEl = document.getElementById("regex-test-string");
const regexResultEl = document.getElementById("regex-result");

function runRegexTest() {
  const pattern = regexPatternEl.value;
  const flags = regexFlagsEl.value;
  const text = regexTestStringEl.value;

  if (!pattern) {
    regexResultEl.innerHTML = `<p class="card-desc">Enter a pattern to test.</p>`;
    return;
  }

  let re;
  try {
    re = new RegExp(pattern, flags.includes("g") ? flags : `${flags}g`);
  } catch (err) {
    regexResultEl.innerHTML = `<p class="card-desc regex-error">Invalid pattern: ${escapeHtml(err.message)}</p>`;
    return;
  }

  const matches = [...text.matchAll(re)];
  let highlighted = "";
  let lastIndex = 0;
  for (const m of matches) {
    highlighted += escapeHtml(text.slice(lastIndex, m.index));
    highlighted += `<mark>${escapeHtml(m[0])}</mark>`;
    lastIndex = m.index + m[0].length;
  }
  highlighted += escapeHtml(text.slice(lastIndex));

  const groupsInfo = matches
    .map((m, i) => (m.length > 1 ? `Match ${i + 1} groups: ${JSON.stringify(m.slice(1))}` : null))
    .filter(Boolean)
    .join("\n");

  regexResultEl.innerHTML = `
    <p class="card-desc">${matches.length} match(es)</p>
    <pre class="log-tab-content">${highlighted || "<em>(empty test string)</em>"}</pre>
    ${groupsInfo ? `<pre class="log-tab-content">${escapeHtml(groupsInfo)}</pre>` : ""}`;
}

[regexPatternEl, regexFlagsEl, regexTestStringEl].forEach((el) => el.addEventListener("input", runRegexTest));
runRegexTest();

// ---- New-module scaffolding wizard ----

const scaffoldTierEl = document.getElementById("scaffold-tier");
const scaffoldNameEl = document.getElementById("scaffold-name");
const scaffoldDescriptionEl = document.getElementById("scaffold-description");
const scaffoldCreateBtn = document.getElementById("scaffold-create-btn");
const scaffoldResultEl = document.getElementById("scaffold-result");

scaffoldCreateBtn.addEventListener("click", async () => {
  const name = scaffoldNameEl.value.trim();
  if (!name) {
    scaffoldResultEl.classList.remove("hidden");
    scaffoldResultEl.textContent = "Module name is required.";
    return;
  }

  scaffoldCreateBtn.disabled = true;
  scaffoldCreateBtn.textContent = "Scaffolding...";
  try {
    const res = await fetch("/api/modules/scaffold", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        tier: scaffoldTierEl.value,
        name,
        description: scaffoldDescriptionEl.value.trim(),
      }),
    });
    const body = await res.json();
    scaffoldResultEl.classList.remove("hidden");
    if (!res.ok) {
      scaffoldResultEl.textContent = body.detail || "Failed to scaffold this module.";
      return;
    }
    scaffoldResultEl.innerHTML = `Created <code>${escapeHtml(body.py_path)}</code> and <code>${escapeHtml(body.yaml_path)}</code> — edit <code>run()</code> to bring it to life.`;
    showToast(`Scaffolded "${body.name}" in the ${body.tier} tier.`, "success");
    scaffoldNameEl.value = "";
    scaffoldDescriptionEl.value = "";
    await loadModules();
  } catch (err) {
    scaffoldResultEl.classList.remove("hidden");
    scaffoldResultEl.textContent = `Request failed: ${err}`;
  } finally {
    scaffoldCreateBtn.disabled = false;
    scaffoldCreateBtn.textContent = "Scaffold module";
  }
});

async function duplicateModule(tier, name) {
  const newName = prompt(`Duplicate "${name}" as a new module — enter a name for the copy:`);
  if (!newName || !newName.trim()) return;

  try {
    const res = await fetch(`/api/modules/${tier}/${name}/duplicate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ new_name: newName.trim() }),
    });
    const body = await res.json();
    if (!res.ok) {
      showToast(body.detail || "Failed to duplicate this module.", "error");
      return;
    }
    showToast(`Duplicated "${name}" as "${body.name}" (${body.tier}).`, "success");
    await loadModules();
  } catch (err) {
    showToast(`Duplicate failed: ${err}`, "error");
  }
}

// ---- Command palette (Ctrl/Cmd+K) ----

const commandPaletteOverlay = document.getElementById("command-palette-overlay");
const commandPaletteInput = document.getElementById("command-palette-input");
const commandPaletteResultsEl = document.getElementById("command-palette-results");

let commandPaletteItems = [];
let commandPaletteActiveIndex = 0;

function flashHighlight(el) {
  el.classList.remove("jump-highlight");
  void el.offsetWidth; // force reflow so the animation restarts if it just ran
  el.classList.add("jump-highlight");
  setTimeout(() => el.classList.remove("jump-highlight"), 1500);
}

function buildCommandPaletteCommands() {
  const commands = [];

  TIER_ORDER.forEach((tier) => {
    (currentModulesByTier[tier] || []).forEach((m) => {
      commands.push({
        kind: "Run",
        title: m.name,
        desc: `[${tier}] ${m.description || ""}`,
        action: () => {
          closeCommandPalette();
          runModule(tier, m.name);
        },
      });
    });
  });

  currentPipelines.forEach((p) => {
    commands.push({
      kind: "Pipeline",
      title: p.name,
      desc: p.description || "Jump to this saved pipeline",
      action: () => {
        closeCommandPalette();
        const card = document.getElementById(`pipeline-card__${p.slug}`);
        if (card) {
          card.scrollIntoView({ behavior: "smooth", block: "center" });
          flashHighlight(card);
        }
      },
    });
  });

  commands.push({
    kind: "Action",
    title: "Open pipeline builder",
    desc: "Start building a new pipeline",
    action: () => {
      closeCommandPalette();
      if (builderPanelEl.classList.contains("hidden")) openBuilder();
      builderPanelEl.scrollIntoView({ behavior: "smooth", block: "start" });
    },
  });

  commands.push({
    kind: "Action",
    title: "Toggle theme",
    desc: "Switch between light and dark mode",
    action: () => {
      closeCommandPalette();
      themeToggleBtn.click();
    },
  });

  commands.push({
    kind: "Action",
    title: "Toggle density",
    desc: "Switch between comfortable and compact card view",
    action: () => {
      closeCommandPalette();
      densityToggleBtn.click();
    },
  });

  return commands;
}

function setCommandPaletteActive(index) {
  commandPaletteActiveIndex = index;
  commandPaletteResultsEl.querySelectorAll(".command-palette-item").forEach((el) => {
    el.classList.toggle("active", Number(el.dataset.index) === index);
  });
  const activeEl = commandPaletteResultsEl.querySelector(".command-palette-item.active");
  if (activeEl) activeEl.scrollIntoView({ block: "nearest" });
}

function renderCommandPaletteResults(query) {
  const all = buildCommandPaletteCommands();
  const q = query.trim().toLowerCase();
  const filtered = q ? all.filter((c) => `${c.title} ${c.desc}`.toLowerCase().includes(q)) : all;

  commandPaletteItems = filtered.slice(0, 30);
  commandPaletteActiveIndex = 0;

  if (!commandPaletteItems.length) {
    commandPaletteResultsEl.innerHTML = `<div class="command-palette-empty">No matching commands.</div>`;
    return;
  }

  commandPaletteResultsEl.innerHTML = commandPaletteItems
    .map(
      (c, i) => `
      <div class="command-palette-item ${i === 0 ? "active" : ""}" data-index="${i}">
        <div class="command-palette-item-label">
          <span class="command-palette-item-title">${escapeHtml(c.title)}</span>
          <span class="command-palette-item-desc">${escapeHtml(c.desc)}</span>
        </div>
        <span class="command-palette-kind">${escapeHtml(c.kind)}</span>
      </div>`
    )
    .join("");

  commandPaletteResultsEl.querySelectorAll(".command-palette-item").forEach((el) => {
    el.addEventListener("click", () => commandPaletteItems[Number(el.dataset.index)].action());
    el.addEventListener("mouseenter", () => setCommandPaletteActive(Number(el.dataset.index)));
  });
}

function openCommandPalette() {
  commandPaletteOverlay.classList.remove("hidden");
  commandPaletteInput.value = "";
  renderCommandPaletteResults("");
  commandPaletteInput.focus();
}

function closeCommandPalette() {
  commandPaletteOverlay.classList.add("hidden");
}

commandPaletteInput.addEventListener("input", () => renderCommandPaletteResults(commandPaletteInput.value));

commandPaletteInput.addEventListener("keydown", (e) => {
  if (e.key === "ArrowDown") {
    e.preventDefault();
    if (commandPaletteItems.length) setCommandPaletteActive((commandPaletteActiveIndex + 1) % commandPaletteItems.length);
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    if (commandPaletteItems.length) setCommandPaletteActive((commandPaletteActiveIndex - 1 + commandPaletteItems.length) % commandPaletteItems.length);
  } else if (e.key === "Enter") {
    e.preventDefault();
    const item = commandPaletteItems[commandPaletteActiveIndex];
    if (item) item.action();
  } else if (e.key === "Escape") {
    e.preventDefault();
    closeCommandPalette();
  }
});

commandPaletteOverlay.addEventListener("click", (e) => {
  if (e.target === commandPaletteOverlay) closeCommandPalette();
});

// ---- Keyboard shortcuts ----

document.addEventListener("keydown", (event) => {
  const isTyping = ["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement.tagName);

  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    if (commandPaletteOverlay.classList.contains("hidden")) openCommandPalette();
    else closeCommandPalette();
  } else if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "z" && !isTyping) {
    if (!builderPanelEl.classList.contains("hidden") && builderUndoStack.length) {
      event.preventDefault();
      undoBuilderStep();
    }
  } else if (event.key === "/" && !isTyping) {
    event.preventDefault();
    searchOmnibar.focus();
  } else if (event.key === "Escape") {
    document.activeElement.blur();
    notificationsPanel.classList.add("hidden");
    healthPanel.classList.add("hidden");
  } else if (event.key === "?" && !isTyping) {
    showToast("Shortcuts: “Ctrl/Cmd+K” command palette · “Ctrl/Cmd+Z” undo in builder · “/” search · Esc close panels · “?” this help", "success");
  }
});

// ---- Boot ----

renderSkeletonSections();
loadModules();
loadSavedPipelines();
loadPipelineTemplates();
loadHealth();
refreshTelemetry();
loadArtifacts();
pollWatcherStatus();
renderNotificationsPanel();
loadSchedules();
loadMemory();
loadModuleStats();
loadEnvironment();
loadRateLimitConfig();
loadAuditLog();
