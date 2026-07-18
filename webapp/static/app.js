const TIER_ORDER = ["automation", "workflow", "agent"];
const TIER_LABELS = {
  automation: "Rule-Based Automations",
  workflow: "AI Workflows",
  agent: "AI Agents",
};

const sectionsEl = document.getElementById("sections");
const pipelineResultEl = document.getElementById("pipeline-result");
const runPipelineBtn = document.getElementById("run-pipeline-btn");

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

function collectInputs(tier, module) {
  const values = {};
  for (const field of module.inputs) {
    const el = document.getElementById(fieldId(tier, module.name, field.name));
    if (!el) continue;
    values[field.name] = field.type === "toggle" ? el.checked : el.value;
  }
  return values;
}

function statusPill(status) {
  return `<span class="status-pill ${status}"><i class="dot dot-${status}"></i>${status}</span>`;
}

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

  const inputEls = card.querySelectorAll("[id^='field__']");
  const inputs = {};
  inputEls.forEach((el) => {
    const fieldName = el.id.split("__").slice(3).join("__");
    inputs[fieldName] = el.type === "checkbox" ? el.checked : el.value;
  });

  button.disabled = true;
  setCardStatus(cardId, "running");
  resultPanel.classList.add("hidden");

  try {
    const res = await fetch(`/api/modules/${tier}/${name}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs }),
    });
    const data = await res.json();

    setCardStatus(cardId, data.success ? "ready" : "error");
    resultPanel.classList.remove("hidden", "error");
    if (data.success) {
      resultPanel.textContent = JSON.stringify(data.output, null, 2);
    } else {
      resultPanel.classList.add("error");
      resultPanel.textContent = `Error: ${data.error}`;
    }
  } catch (err) {
    setCardStatus(cardId, "error");
    resultPanel.classList.remove("hidden");
    resultPanel.classList.add("error");
    resultPanel.textContent = `Request failed: ${err}`;
  } finally {
    button.disabled = false;
  }
}

async function runFullPipeline() {
  runPipelineBtn.disabled = true;
  runPipelineBtn.textContent = "Running...";

  try {
    const res = await fetch("/api/pipeline/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs: {} }),
    });
    const data = await res.json();

    const stepsHtml = data.steps
      .map((s) => `<span class="pipeline-step ${s.success ? "ok" : "fail"}">[${s.tier}] ${s.name}</span>`)
      .join("");

    pipelineResultEl.classList.remove("hidden");
    pipelineResultEl.innerHTML = `
      <h3>Full Pipeline Result</h3>
      <div class="pipeline-steps">${stepsHtml}</div>
      <div class="result-panel">${JSON.stringify(data.context, null, 2)}</div>`;

    await loadModules();
  } finally {
    runPipelineBtn.disabled = false;
    runPipelineBtn.textContent = "Run Full Pipeline";
  }
}

runPipelineBtn.addEventListener("click", runFullPipeline);
loadModules();
