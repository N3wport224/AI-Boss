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
let builderSteps = [];

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

  const inputType = field.type === "number" ? "number" : "text";
  const step = field.type === "number" ? ' step="any"' : "";
  return `<input type="${inputType}"${step} id="${id}" value="${value ?? ""}" />`;
}

function renderField(tier, name, field) {
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

  return `
    <div class="field">
      <label for="${id}">${label}</label>
      ${control}
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
  currentModulesByTier = modulesByTier;
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

  return `
    <div class="field builder-field">
      <label>${field.label || field.name}</label>
      <div class="mapping-row">
        <select class="source-select" id="${srcId}" data-step="${stepIndex}" data-field="${field.name}">${sourceOptions}</select>
        <div class="mapping-control">${control}</div>
      </div>
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

  builderStepsEl.querySelectorAll(".mapping-control input, .mapping-control select").forEach((control) => {
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
    builderResultEl.classList.add("hidden");
    renderTracker(builderTrackerEl, trackerSteps);

    subscribeToStream(stream_id, {
      onEvent: (event) => handleTrackerEvent(builderTrackerEl, event),
      onDone: async (event) => {
        builderLaunchBtn.disabled = false;
        builderLaunchBtn.textContent = "Save & Launch";

        const success = event.kind === "run_completed";
        builderResultEl.classList.remove("hidden", "error");
        if (!success) builderResultEl.classList.add("error");
        builderResultEl.textContent = JSON.stringify(event.context ?? {}, null, 2);

        showToast(
          success ? `Pipeline "${name}" completed successfully.` : `Pipeline "${name}" failed: ${event.error}`,
          success ? "success" : "error"
        );
        await loadSavedPipelines();
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
  if (!pipelinesList.length) {
    savedPipelinesSection.classList.add("hidden");
    return;
  }
  savedPipelinesSection.classList.remove("hidden");

  savedPipelinesGrid.innerHTML = pipelinesList
    .map((p) => {
      const chain = p.steps.map((s) => `[${s.tier}] ${s.name}`).join(" → ");
      return `
        <div class="card" id="pipeline-card__${p.slug}">
          <div class="card-head">
            <h3 class="card-title">${p.name}</h3>
          </div>
          <p class="card-desc">${p.description || "No description."}</p>
          <p class="card-desc pipeline-chain">${chain}</p>
          <button class="btn btn-run" data-slug="${p.slug}" data-name="${p.name}">Run</button>
          <div class="tracker hidden"></div>
          <div class="result-panel hidden"></div>
        </div>`;
    })
    .join("");

  savedPipelinesGrid.querySelectorAll(".btn-run").forEach((btn) => {
    btn.addEventListener("click", () => runSavedPipeline(btn.dataset.slug, btn.dataset.name, pipelinesList));
  });
}

async function loadSavedPipelines() {
  const res = await fetch("/api/pipelines");
  const pipelinesList = await res.json();
  renderSavedPipelines(pipelinesList);
  return pipelinesList;
}

async function runSavedPipeline(slug, name, pipelinesList) {
  const card = document.getElementById(`pipeline-card__${slug}`);
  const button = card.querySelector(".btn-run");
  const tracker = card.querySelector(".tracker");
  const resultPanel = card.querySelector(".result-panel");

  const definition = pipelinesList.find((p) => p.slug === slug);
  const trackerSteps = (definition?.steps || []).map((s) => ({ tier: s.tier, name: s.name }));

  button.disabled = true;
  resultPanel.classList.add("hidden");
  renderTracker(tracker, trackerSteps);
  tracker.classList.remove("hidden");

  try {
    const res = await fetch(`/api/pipelines/${slug}/run`, { method: "POST" });
    const { stream_id } = await res.json();

    subscribeToStream(stream_id, {
      onEvent: (event) => handleTrackerEvent(tracker, event),
      onDone: (event) => {
        button.disabled = false;
        const success = event.kind === "run_completed";
        resultPanel.classList.remove("hidden", "error");
        if (!success) resultPanel.classList.add("error");
        resultPanel.textContent = JSON.stringify(event.context ?? {}, null, 2);
        showToast(
          success ? `${name} completed successfully.` : `${name} failed: ${event.error}`,
          success ? "success" : "error"
        );
      },
    });
  } catch (err) {
    button.disabled = false;
    showToast(`${name} failed: ${err}`, "error");
  }
}

loadModules();
loadSavedPipelines();
