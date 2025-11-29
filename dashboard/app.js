const PROCESSING_STATS_URL = "http://localhost:8100/stats";
const ANALYZER_STATS_URL   = "http://localhost:9100/stats";
const RANDOM_ADMISSION_URL = "http://localhost:9100/hospital/admission/random";
const RANDOM_CAPACITY_URL  = "http://localhost:9100/hospital/capacity/random";

const REFRESH_INTERVAL_MS = 3000;

function setEndpointLabels() {
  document.getElementById("processing-endpoint-label").textContent =
    `Endpoint: ${PROCESSING_STATS_URL}`;

  document.getElementById("analyzer-endpoint-label").textContent =
    `Endpoint: ${ANALYZER_STATS_URL}`;

  document.getElementById("admission-endpoint-label").textContent =
    `Endpoint: ${RANDOM_ADMISSION_URL}`;

  document.getElementById("capacity-endpoint-label").textContent =
    `Endpoint: ${RANDOM_CAPACITY_URL}`;

  document.getElementById("refresh-interval").textContent =
    (REFRESH_INTERVAL_MS / 1000).toFixed(1);
}

function updateLastUpdated() {
  const el = document.getElementById("last-updated");
  const now = new Date();
  el.textContent = now.toLocaleString();
}

async function fetchJson(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`HTTP ${resp.status}`);
  }
  return resp.json();
}

function renderStatsTable(tbodySelector, dataObj) {
  const tbody = document.querySelector(tbodySelector);
  tbody.innerHTML = "";


  const entries = Object.entries(dataObj);

  if (entries.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 2;
    td.textContent = "No data.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  for (const [key, value] of entries) {
    const tr = document.createElement("tr");
    const tdKey = document.createElement("td");
    const tdValue = document.createElement("td");

    tdKey.textContent = key;
    tdValue.textContent =
      typeof value === "object" ? JSON.stringify(value) : value;

    tr.appendChild(tdKey);
    tr.appendChild(tdValue);
    tbody.appendChild(tr);
  }
}


async function updateProcessingStats() {
  const errorEl = document.getElementById("processing-error");
  errorEl.textContent = "";

  try {
    const data = await fetchJson(PROCESSING_STATS_URL);
    renderStatsTable("#processing-stats tbody", data);
  } catch (err) {
    console.error("Error fetching processing stats:", err);
    errorEl.textContent = `Failed to load processing stats: ${err.message}`;
  }
}

async function updateAnalyzerStats() {
  const errorEl = document.getElementById("analyzer-error");
  errorEl.textContent = "";

  try {
    const data = await fetchJson(ANALYZER_STATS_URL);
    renderStatsTable("#analyzer-stats tbody", data);
  } catch (err) {
    console.error("Error fetching analyzer stats:", err);
    errorEl.textContent = `Failed to load analyzer stats: ${err.message}`;
  }
}

async function updateRandomAdmission() {
  const errorEl = document.getElementById("admission-error");
  const preEl = document.getElementById("random-admission");
  errorEl.textContent = "";

  try {
    const data = await fetchJson(RANDOM_ADMISSION_URL);
    preEl.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    console.error("Error fetching random admission:", err);
    errorEl.textContent = `Failed to load admission event: ${err.message}`;
    preEl.textContent = "—";
  }
}

async function updateRandomCapacity() {
  const errorEl = document.getElementById("capacity-error");
  const preEl = document.getElementById("random-capacity");
  errorEl.textContent = "";

  try {
    const data = await fetchJson(RANDOM_CAPACITY_URL);
    preEl.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    console.error("Error fetching random capacity:", err);
    errorEl.textContent = `Failed to load capacity event: ${err.message}`;
    preEl.textContent = "—";
  }
}

async function refreshAll() {
  await Promise.all([
    updateProcessingStats(),
    updateAnalyzerStats(),
    updateRandomAdmission(),
    updateRandomCapacity()
  ]);

  updateLastUpdated();
}

document.addEventListener("DOMContentLoaded", () => {
  setEndpointLabels();
  refreshAll();
  setInterval(refreshAll, REFRESH_INTERVAL_MS);
});