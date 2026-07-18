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

const sectionsEl = document.getElementById("sections");
const pipelineResultEl = document.getElementById("pipeline-result");
const runPipelineBtn = document.getElementById("run-pipeline-btn");
const toastContainer = document.getElementById("toast-container");

function fieldId(tier, name, fieldName) {
  return `field__${tier}__${name}__${fieldName}`;
}

function renderField(tier, name, field) {
  const id = fieldId(tier, name, field.name);
  const label = field.label || field.name;

  if (field.type === "toggle") {
    return `
      <div class="field toggle-row">
        <label for="${id}">${label}</label>
        <label class="switch">
          <input type="checkbox" id="${id}" ${field.default ? "checked" : ""} />
          <span class="switch-track"></span>
        </label>
      </div>`;
  }

  if (field.type === "select") {
    const options = (field.options || [])
      .map((opt) => `<option value="${opt}" ${opt === field.default ? "selected" : ""}>${opt}</option>`)
      .join("");
    return `
      <div class="field">
        <label for="${id}">${label}</label>
        <select id="${id}">${options}</select>
      </div>`;
  }

  const inputType = field.type === "number" ? "number" : "text";
  const step = field.type === "number" ? ' step="any"' : "";
  return `
    <div class="field">
      <label for="${id}">${label}</label>
      <input type="${inputType}"${step} id="${id}" value="${field.default ?? ""}" />
    </div>`;
}

function statusPill(status) {
  return `<span class="status-pill ${status}"><i class="dot dot-${status}"></i>${status}</span>`;
}

// ---- Toasts ----

function showToast(message, type = "success") {
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.textContent = message;
  toastContainer.appendChild(el);
  setTimeout(() => {
    el.classList.add("leaving");
    setTimeout(() => el.remove(), 200);
  }, 4500);
}

// ---- Live progress tracker + agent thought stream ----

function renderTracker(container, steps) {
  container.innerHTML = steps
    .map((s) => {
      const thoughtControls =
        s.tier === "agent"
          ? `<button class="thought-toggle hidden" type="button" data-name="${s.name}">Thoughts</button>`
          : "";
      const thoughtBox = s.tier === "agent" ? `<div class="thought-box hidden" data-name="${s.name}"></div>` : "";
      return `
        <div class="tracker-step" data-tier="${s.tier}" data-name="${s.name}">
          <span class="tracker-icon">${STEP_ICON.pending}</span>
          <span class="tracker-label">[${s.tier}] ${s.name}</span>
          ${thoughtControls}
        </div>
        ${thoughtBox}`;
    })
    .join("");

  container.querySelectorAll(".thought-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const box = container.querySelector(`.thought-box[data-name="${btn.dataset.name}"]`);
      box.classList.toggle("collapsed");
    });
  });
}

function setStepStatus(container, tier, name, status, error) {
  const row = container.querySelector(`.tracker-step[data-name="${name}"]`);
  if (!row) return;
  const icon = row.querySelector(".tracker-icon");
  icon.textContent = STEP_ICON[status];
  icon.classList.toggle("spin", status === "running");
  row.classList.toggle("failed", status === "failed");
  if (error) row.title = error;

  if (tier === "agent" && status === "running") {
    const toggle = container.querySelector(`.thought-toggle[data-name="${name}"]`);
    toggle?.classList.remove("hidden");
  }
}

function appendThought(container, name, kind, message) {
  const box = container.querySelector(`.thought-box[data-name="${name}"]`);
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
    setStepStatus(container, event.tier, event.name, "running");
  } else if (event.kind === "step_completed") {
    setStepStatus(container, event.tier, event.name, "done");
  } else if (event.kind === "step_failed") {
    setStepStatus(container, event.tier, event.name, "failed", event.error);
  } else if (event.kind === "thought" || event.kind === "tool_call") {
    appendThought(container, event.name, event.kind, event.message);
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
  const fieldsHtml = module.inputs.length
    ? `<div class="form-fields">${module.inputs.map((f) => renderField(module.tier, module.name, f)).join("")}</div>`
    : "";

  return `
    <div class="card" id="${cardId}">
      <div class="card-head">
        <h3 class="card-title">${module.name}</h3>
        <div class="status-slot">${statusPill(module.status)}</div>
      </div>
      <p class="card-desc">${module.description}</p>
      ${fieldsHtml}
      <button class="btn btn-run" data-tier="${module.tier}" data-name="${module.name}">Run</button>
      <div class="tracker hidden"></div>
      <div class="result-panel hidden"></div>
    </div>`;
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
        </div>
        <div class="card-grid">
          ${modules.map(renderCard).join("")}
        </div>
      </section>`;
  }).join("");

  sectionsEl.querySelectorAll(".btn-run").forEach((btn) => {
    btn.addEventListener("click", () => runModule(btn.dataset.tier, btn.dataset.name));
  });
}

async function loadModules() {
  const res = await fetch("/api/modules");
  const modulesByTier = await res.json();
  renderSections(modulesByTier);
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
      onDone: (event) => {
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
    <div class="result-panel hidden" id="pipeline-context"></div>`;
  const tracker = document.getElementById("pipeline-tracker");
  renderTracker(tracker, steps);

  try {
    const res = await fetch("/api/pipeline/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs: {} }),
    });
    const { stream_id } = await res.json();

    subscribeToStream(stream_id, {
      onEvent: (event) => handleTrackerEvent(tracker, event),
      onDone: async (event) => {
        runPipelineBtn.disabled = false;
        runPipelineBtn.textContent = "Run Full Pipeline";

        const success = event.kind === "run_completed";
        const contextPanel = document.getElementById("pipeline-context");
        contextPanel.classList.remove("hidden");
        contextPanel.textContent = JSON.stringify(event.context ?? {}, null, 2);

        showToast(
          success ? "Full pipeline completed successfully." : `Full pipeline failed: ${event.error}`,
          success ? "success" : "error"
        );
        await loadModules();
      },
    });
  } catch (err) {
    runPipelineBtn.disabled = false;
    runPipelineBtn.textContent = "Run Full Pipeline";
    showToast(`Full pipeline failed: ${err}`, "error");
  }
}

runPipelineBtn.addEventListener("click", runFullPipeline);
loadModules();
