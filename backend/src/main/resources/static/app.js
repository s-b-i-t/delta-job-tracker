(() => {
  const companyIdInput = document.getElementById("companyId");
  const queryInput = document.getElementById("query");
  const csModeToggle = document.getElementById("csMode");
  const usePresetBtn = document.getElementById("usePreset");
  const activeFilter = document.getElementById("activeFilter");
  const sinceInput = document.getElementById("since");
  const limitInput = document.getElementById("limit");
  const loadActiveBtn = document.getElementById("loadActive");
  const loadNewBtn = document.getElementById("loadNew");
  const loadClosedBtn = document.getElementById("loadClosed");
  const loadAllBtn = document.getElementById("loadAll");
  const resultsList = document.getElementById("resultsList");
  const resultsMeta = document.getElementById("resultsMeta");
  const detailPanel = document.getElementById("detailPanel");
  const detailTitle = document.getElementById("detailTitle");
  const detailMeta = document.getElementById("detailMeta");
  const detailDescription = document.getElementById("detailDescription");
  const closeDetail = document.getElementById("closeDetail");

  const CS_PRESET_QUERY = "(\"software engineer\" OR \"software developer\" OR \"data engineer\" OR \"data scientist\" OR \"machine learning\" OR \"ml engineer\" OR backend OR \"front end\" OR frontend OR \"full stack\" OR devops OR sre OR \"site reliability\" OR security OR cloud OR platform OR infrastructure OR \"distributed systems\" OR ios OR android OR embedded OR firmware) -recruiter -recruiting -sales -marketing -warehouse -cashier -nurse -driver";

  const STORAGE_KEYS = {
    companyId: "dj_companyId",
    query: "dj_query",
    csMode: "dj_csMode",
    activeFilter: "dj_activeFilter",
    since: "dj_since",
    limit: "dj_limit"
  };

  const state = {
    jobs: []
  };

  const defaultSince = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  loadSettings();

  closeDetail.addEventListener("click", () => {
    detailPanel.classList.add("hidden");
  });

  usePresetBtn.addEventListener("click", () => {
    queryInput.value = CS_PRESET_QUERY;
    csModeToggle.checked = true;
    persistSettings();
  });

  csModeToggle.addEventListener("change", () => {
    if (csModeToggle.checked && !queryInput.value.trim()) {
      queryInput.value = CS_PRESET_QUERY;
    }
    persistSettings();
  });

  [companyIdInput, queryInput, activeFilter, sinceInput, limitInput].forEach((input) => {
    input.addEventListener("change", persistSettings);
  });

  loadActiveBtn.addEventListener("click", async () => {
    const params = baseParams();
    params.active = "true";
    await loadJobs("/api/jobs", params, "Active Jobs");
  });

  loadNewBtn.addEventListener("click", async () => {
    const params = baseParams();
    await loadJobs("/api/jobs/new", params, "New Jobs");
  });

  loadClosedBtn.addEventListener("click", async () => {
    const params = baseParams();
    await loadJobs("/api/jobs/closed", params, "Closed Jobs");
  });

  loadAllBtn.addEventListener("click", async () => {
    const params = baseParams();
    delete params.active;
    await loadJobs("/api/jobs", params, "All Jobs");
  });

  function baseParams() {
    const params = {};
    const companyId = companyIdInput.value.trim();
    const since = sinceInput.value.trim();
    const limit = limitInput.value.trim();
    const query = resolveQuery();

    if (companyId) {
      params.companyId = companyId;
    }
    if (query) {
      params.q = query;
    }
    if (since) {
      params.since = since;
    }
    if (activeFilter.value === "true" || activeFilter.value === "false") {
      params.active = activeFilter.value;
    }
    if (limit) {
      params.limit = limit;
    }
    return params;
  }

  async function loadJobs(path, params, label) {
    persistSettings();
    resultsMeta.textContent = "Loading...";
    resultsList.innerHTML = "";
    try {
      const url = new URL(path, window.location.origin);
      Object.entries(params).forEach(([key, value]) => {
        url.searchParams.set(key, value);
      });
      const response = await fetch(url.toString());
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || "Request failed");
      }
      const jobs = await response.json();
      state.jobs = jobs;
      resultsMeta.textContent = `${label} (${jobs.length})`;
      renderResults(jobs);
    } catch (err) {
      resultsMeta.textContent = `Error: ${err.message}`;
    }
  }

  function renderResults(jobs) {
    if (!jobs || jobs.length === 0) {
      resultsList.innerHTML = "<p class=\"empty\">No jobs found.</p>";
      return;
    }
    resultsList.innerHTML = "";
    jobs.forEach((job, index) => {
      const item = document.createElement("div");
      item.className = "job-card";
      item.dataset.index = index;

      const title = document.createElement("div");
      title.className = "job-title";
      title.textContent = job.title || "(Untitled)";

      const meta = document.createElement("div");
      meta.className = "job-meta";
      meta.textContent = [
        job.companyName || job.ticker || "Unknown Company",
        job.locationText || "Location N/A",
        job.datePosted ? `Posted ${job.datePosted}` : null,
        job.isActive ? "Active" : "Closed"
      ].filter(Boolean).join(" • ");

      const timestamps = document.createElement("div");
      timestamps.className = "job-timestamps";
      timestamps.textContent = `First seen: ${formatInstant(job.firstSeenAt)} | Last seen: ${formatInstant(job.lastSeenAt)}`;

      const actions = document.createElement("div");
      actions.className = "job-actions";
      const openBtn = document.createElement("button");
      openBtn.className = "btn small";
      openBtn.textContent = "View Details";
      openBtn.addEventListener("click", () => showDetail(job));
      actions.appendChild(openBtn);

      item.appendChild(title);
      item.appendChild(meta);
      item.appendChild(timestamps);
      item.appendChild(actions);
      resultsList.appendChild(item);
    });
  }

  function showDetail(job) {
    detailTitle.textContent = job.title || "Job Detail";
    detailMeta.textContent = [
      job.companyName || job.ticker || "Unknown Company",
      job.locationText || "Location N/A",
      job.isActive ? "Active" : "Closed"
    ].join(" • ");

    if (job.sourceUrl) {
      const link = document.createElement("a");
      link.href = job.sourceUrl;
      link.target = "_blank";
      link.rel = "noopener";
      link.textContent = "Open posting";
      link.className = "detail-link";
      detailMeta.appendChild(document.createTextNode(" • "));
      detailMeta.appendChild(link);
    }

    const rawHtml = job.descriptionText || "<em>No description available.</em>";
    const sanitized = window.DOMPurify ? window.DOMPurify.sanitize(rawHtml) : rawHtml;
    detailDescription.innerHTML = sanitized;

    detailPanel.classList.remove("hidden");
  }

  function formatInstant(value) {
    if (!value) {
      return "N/A";
    }
    try {
      return new Date(value).toISOString();
    } catch (err) {
      return value;
    }
  }

  function resolveQuery() {
    const raw = queryInput.value.trim();
    if (!raw && csModeToggle.checked) {
      queryInput.value = CS_PRESET_QUERY;
      return CS_PRESET_QUERY;
    }
    return raw;
  }

  function loadSettings() {
    const storedCompanyId = localStorage.getItem(STORAGE_KEYS.companyId);
    const storedQuery = localStorage.getItem(STORAGE_KEYS.query);
    const storedCsMode = localStorage.getItem(STORAGE_KEYS.csMode);
    const storedActiveFilter = localStorage.getItem(STORAGE_KEYS.activeFilter);
    const storedSince = localStorage.getItem(STORAGE_KEYS.since);
    const storedLimit = localStorage.getItem(STORAGE_KEYS.limit);

    if (storedCompanyId !== null) {
      companyIdInput.value = storedCompanyId;
    }
    if (storedQuery !== null) {
      queryInput.value = storedQuery;
    }
    if (storedCsMode !== null) {
      csModeToggle.checked = storedCsMode === "true";
    } else {
      csModeToggle.checked = true;
    }
    if (storedActiveFilter !== null) {
      activeFilter.value = storedActiveFilter;
    }
    if (storedSince !== null && storedSince.trim()) {
      sinceInput.value = storedSince;
    } else {
      sinceInput.value = defaultSince;
    }
    if (storedLimit !== null) {
      limitInput.value = storedLimit;
    }
  }

  function persistSettings() {
    localStorage.setItem(STORAGE_KEYS.companyId, companyIdInput.value.trim());
    localStorage.setItem(STORAGE_KEYS.query, queryInput.value.trim());
    localStorage.setItem(STORAGE_KEYS.csMode, csModeToggle.checked ? "true" : "false");
    localStorage.setItem(STORAGE_KEYS.activeFilter, activeFilter.value);
    localStorage.setItem(STORAGE_KEYS.since, sinceInput.value.trim());
    localStorage.setItem(STORAGE_KEYS.limit, limitInput.value.trim());
  }
})();
