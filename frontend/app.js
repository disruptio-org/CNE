const STAGES = {
  ingest: {
    title: "Ingestion",
    description: "Inspect stored metadata and verify the upload classification.",
  },
  ocr: {
    title: "Optical Character Recognition",
    description: "Run OCR for scanned PDFs and refresh extracted text artefacts.",
  },
  operator_a: {
    title: "Operator A",
    description: "Execute text-first parsing heuristics to populate candidate rows.",
  },
  operator_b: {
    title: "Operator B",
    description: "Execute table-first parsing heuristics to populate candidate rows.",
  },
  match: {
    title: "Match",
    description: "Regenerate comparison rows to surface agreements and disputes.",
  },
  review: {
    title: "Review",
    description: "Fetch latest disputes and reviewer decisions for the selected document.",
  },
  approve: {
    title: "Approve",
    description: "Mark the document as approved once disputes are resolved.",
  },
  export: {
    title: "Export",
    description: "Produce the CSV export bundle for approved documents.",
  },
};

const dashboardBody = document.querySelector("#dashboard-body");
const stageNav = document.querySelector(".stage-nav");
const stageContainer = document.querySelector("#stage-container");
const refreshDashboardButton = document.querySelector("#refresh-dashboard");
const template = document.querySelector("#stage-template");

let dashboardState = [];
let activeStage = "ingest";

async function fetchJSON(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();
  if (!response.ok) {
    throw new Error(text || response.statusText);
  }
  try {
    return JSON.parse(text || "{}");
  } catch (error) {
    return {};
  }
}

async function loadDashboard() {
  dashboardBody.innerHTML = `<tr><td colspan="5" class="placeholder">Loading…</td></tr>`;
  try {
    const data = await fetchJSON("/api/documents/progress");
    dashboardState = Array.isArray(data) ? data : [];
    renderDashboard();
    renderStage(activeStage);
  } catch (error) {
    dashboardBody.innerHTML = `<tr><td colspan="5" class="placeholder">${error.message}</td></tr>`;
  }
}

function renderDashboard() {
  if (!dashboardState.length) {
    dashboardBody.innerHTML = `<tr><td colspan="5" class="placeholder">No documents available.</td></tr>`;
    return;
  }

  const rows = dashboardState
    .map((entry) => {
      const stageLinks = Object.keys(STAGES)
        .map(
          (stage) =>
            `<a href="#" data-jump="${stage}" data-doc="${entry.id}">${STAGES[stage].title}</a>`
        )
        .join("");

      const progress = Math.round(entry.completion * 100);
      const statusBadge = `<span class="badge" data-state="${entry.status_state}">${entry.status}</span>`;
      const typeBadge = `<span class="badge" data-state="completed">${entry.detected_type}</span>`;
      return `
        <tr>
          <td><strong>${entry.file_name}</strong></td>
          <td>${statusBadge}</td>
          <td>${typeBadge}</td>
          <td>
            <div class="progress-bar" role="progressbar" aria-valuenow="${progress}" aria-valuemin="0" aria-valuemax="100">
              <span style="width: ${progress}%"></span>
            </div>
            <small class="hint">${progress}% complete</small>
          </td>
          <td class="stage-links">${stageLinks}</td>
        </tr>
      `;
    })
    .join("");

  dashboardBody.innerHTML = rows;
}

function renderStage(stage) {
  activeStage = stage;
  stageNav.querySelectorAll("button").forEach((button) => {
    button.classList.toggle("active", button.dataset.stage === stage);
  });

  const config = STAGES[stage];
  const node = template.content.firstElementChild.cloneNode(true);
  node.querySelector("[data-title]").textContent = config.title;
  node.querySelector("[data-description]").textContent = config.description;

  const select = node.querySelector("[data-document]");
  select.innerHTML = dashboardState
    .map((entry) => `<option value="${entry.id}">${entry.file_name}</option>`)
    .join("");

  if (!dashboardState.length) {
    select.innerHTML = `<option disabled selected>No documents</option>`;
  }

  const extra = node.querySelector("[data-extra]");
  if (stage === "approve") {
    extra.innerHTML = `
      <label>
        <span>Approver ID</span>
        <input name="approver_id" type="text" placeholder="user@example" required />
      </label>
      <label>
        <span>Approval summary (optional)</span>
        <textarea name="summary" rows="2" placeholder="Summary for the audit log"></textarea>
      </label>
    `;
  } else if (stage === "review") {
    extra.innerHTML = `
      <label>
        <span>Dispute filter</span>
        <select name="status">
          <option value="">All</option>
          <option value="dispute">Disputes</option>
          <option value="agreement">Agreements</option>
        </select>
      </label>
    `;
  } else if (stage === "export") {
    extra.innerHTML = `
      <label>
        <span>Output directory (optional)</span>
        <input name="output_dir" type="text" placeholder="data/exports" />
      </label>
    `;
  } else {
    extra.innerHTML = "";
  }

  const form = node.querySelector("[data-form]");
  const output = node.querySelector("[data-output]");
  const statusContainer = node.querySelector("[data-status]");

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const documentId = formData.get("document") || select.value;
    if (!documentId) {
      output.textContent = "Select a document first.";
      return;
    }
    output.textContent = "Running…";
    const payload = Object.fromEntries(formData.entries());
    delete payload.document;
    try {
      const response = await fetchJSON(`/api/documents/${documentId}/stages/${stage}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      output.textContent = response.message || "Stage completed.";
      await loadDashboard();
      renderStage(stage);
      showStageDetails(statusContainer, response.details);
    } catch (error) {
      output.textContent = error.message;
    }
  });

  const refreshButton = node.querySelector("[data-refresh]");
  refreshButton.addEventListener("click", () => {
    const documentId = select.value;
    showStageDetails(statusContainer, lookupDocumentStage(documentId, stage));
  });

  select.addEventListener("change", () => {
    showStageDetails(statusContainer, lookupDocumentStage(select.value, stage));
  });

  const initialDoc = select.value;
  showStageDetails(statusContainer, lookupDocumentStage(initialDoc, stage));

  stageContainer.replaceChildren(node);
}

function lookupDocumentStage(documentId, stage) {
  if (!documentId) return null;
  const entry = dashboardState.find((item) => String(item.id) === String(documentId));
  if (!entry) return null;
  return entry.stages ? entry.stages[stage] : null;
}

function showStageDetails(container, details) {
  if (!details) {
    container.innerHTML = `<p class="hint">No data available for this stage.</p>`;
    return;
  }
  const cards = [];
  if (Array.isArray(details.metrics)) {
    details.metrics.forEach((metric) => {
      cards.push(`
        <article class="stage-card">
          <header>
            <strong>${metric.label}</strong>
            <span class="badge" data-state="${metric.state || "pending"}">${metric.value}</span>
          </header>
          ${metric.description ? `<p class="hint">${metric.description}</p>` : ""}
        </article>
      `);
    });
  }
  if (details.updated_at) {
    cards.push(`
      <article class="stage-card">
        <header>
          <strong>Last updated</strong>
          <span>${new Date(details.updated_at).toLocaleString()}</span>
        </header>
      </article>
    `);
  }
  container.innerHTML = cards.join("") || `<p class="hint">Stage metadata pending.</p>`;
}

stageNav.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-stage]");
  if (!button) return;
  renderStage(button.dataset.stage);
});

dashboardBody.addEventListener("click", (event) => {
  const link = event.target.closest("a[data-jump]");
  if (!link) return;
  event.preventDefault();
  const stage = link.dataset.jump;
  renderStage(stage);
  const select = stageContainer.querySelector("select[data-document]");
  if (select && link.dataset.doc) {
    select.value = link.dataset.doc;
    select.dispatchEvent(new Event("change", { bubbles: true }));
  }
  stageContainer.scrollIntoView({ behavior: "smooth" });
});

refreshDashboardButton.addEventListener("click", () => loadDashboard());

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/app/sw.js").catch(() => {
      // ignore registration failures in offline/unsupported environments
    });
  });
}

loadDashboard();
