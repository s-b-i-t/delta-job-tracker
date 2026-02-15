(() => {
  const companyIdInput = document.getElementById("companyId");
  const apiBaseInput = document.getElementById("apiBase");
  const companySearchInput = document.getElementById("companySearch");
  const companySuggestions = document.getElementById("companySuggestions");
  const queryInput = document.getElementById("query");
  const activeFilter = document.getElementById("activeFilter");
  const sinceInput = document.getElementById("since");
  const limitInput = document.getElementById("limit");
  const loadActiveBtn = document.getElementById("loadActive");
  const loadNewBtn = document.getElementById("loadNew");
  const loadNewSinceCheckBtn = document.getElementById("loadNewSinceCheck");
  const loadClosedBtn = document.getElementById("loadClosed");
  const loadAllBtn = document.getElementById("loadAll");
  const setLastCheckBtn = document.getElementById("setLastCheck");
  const lastCheckHint = document.getElementById("lastCheckHint");
  const autoUpdateLastCheckToggle = document.getElementById("autoUpdateLastCheck");
  const resultsList = document.getElementById("resultsList");
  const resultsMeta = document.getElementById("resultsMeta");
  const detailPanel = document.getElementById("detailPanel");
  const detailTitle = document.getElementById("detailTitle");
  const detailMeta = document.getElementById("detailMeta");
  const detailDescription = document.getElementById("detailDescription");
  const closeDetail = document.getElementById("closeDetail");

  const STORAGE_KEYS = {
    companyId: "dj_companyId",
    apiBase: "dj_apiBase",
    query: "dj_query",
    activeFilter: "dj_activeFilter",
    since: "dj_since",
    limit: "dj_limit",
    lastCheckAt: "dj_lastCheckAt",
    autoUpdateLastCheck: "dj_autoUpdateLastCheck"
  };

  const state = {
    jobs: []
  };

  const defaultSince = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  loadSettings();

  closeDetail.addEventListener("click", () => {
    detailPanel.classList.add("hidden");
  });

  setLastCheckBtn.addEventListener("click", () => {
    setLastCheck(new Date().toISOString(), true);
  });

  [companyIdInput, apiBaseInput, queryInput, activeFilter, sinceInput, limitInput].forEach((input) => {
    input.addEventListener("change", persistSettings);
  });

  autoUpdateLastCheckToggle.addEventListener("change", persistSettings);

  loadActiveBtn.addEventListener("click", async () => {
    const params = baseParams();
    params.active = "true";
    await loadJobs("/api/jobs", params, "Active Jobs");
  });

  loadNewBtn.addEventListener("click", async () => {
    const params = baseParams();
    await loadJobs("/api/jobs/new", params, "New Jobs");
  });

  loadNewSinceCheckBtn.addEventListener("click", async () => {
    const params = baseParams();
    const lastCheck = localStorage.getItem(STORAGE_KEYS.lastCheckAt);
    const since = lastCheck || sinceInput.value.trim();
    if (since) {
      params.since = since;
    }
    const result = await loadJobs("/api/jobs/new", params, "New Jobs Since Last Check");
    if (result.success && autoUpdateLastCheckToggle.checked) {
      setLastCheck(new Date().toISOString(), true);
    }
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

  let searchTimeout = null;
  companySearchInput.addEventListener("input", () => {
    clearTimeout(searchTimeout);
    const term = companySearchInput.value.trim();
    if (!term) {
      companySuggestions.innerHTML = "";
      return;
    }
    searchTimeout = setTimeout(() => searchCompanies(term), 250);
  });

  function baseParams() {
    const params = {};
    const companyId = companyIdInput.value.trim();
    const since = sinceInput.value.trim();
    const limit = limitInput.value.trim();
    const query = queryInput.value.trim();

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
    let requestUrl;
    try {
      requestUrl = buildApiUrl(path);
      const url = new URL(requestUrl);
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
      return { success: true, jobs };
    } catch (err) {
      const hint = "Check API Base URL / backend is running.";
      const urlText = requestUrl ? ` URL: ${requestUrl}` : "";
      resultsMeta.textContent = `Error: ${err.message}.${urlText} ${hint}`;
      return { success: false, jobs: [] };
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
      const postingUrl = resolvePostingUrl(job);
      if (postingUrl) {
        const link = document.createElement("a");
        link.href = postingUrl;
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = job.title || "(Untitled)";
        title.appendChild(link);
      } else {
        title.textContent = job.title || "(Untitled)";
      }

      const meta = document.createElement("div");
      meta.className = "job-meta";
      meta.textContent = [
        job.companyName || job.ticker || "Unknown Company",
        job.locationText || "Location N/A",
        job.datePosted ? `Posted ${job.datePosted}` : null,
        job.isActive ? "Active" : "Closed"
      ].filter(Boolean).join(" | ");

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

  async function showDetail(job) {
    detailTitle.textContent = job.title || "Job Detail";
    detailMeta.textContent = [
      job.companyName || job.ticker || "Unknown Company",
      job.locationText || "Location N/A",
      job.isActive ? "Active" : "Closed"
    ].join(" | ");

    detailDescription.innerHTML = "<em>Loading description...</em>";
    detailPanel.classList.remove("hidden");

    let detail = job;
    try {
      const response = await fetch(buildApiUrl(`/api/jobs/${job.id}`));
      if (response.ok) {
        detail = await response.json();
      }
    } catch (err) {
      detail = job;
    }

    const postingUrl = resolvePostingUrl(detail);
    if (postingUrl) {
      const link = document.createElement("a");
      link.href = postingUrl;
      link.target = "_blank";
      link.rel = "noopener";
      link.textContent = "Open posting";
      link.className = "detail-link";
      detailMeta.appendChild(document.createTextNode(" | "));
      detailMeta.appendChild(link);
    }

    const rawHtml = detail.descriptionText || "<em>No description available.</em>";
    const sanitized = window.DOMPurify ? window.DOMPurify.sanitize(rawHtml) : rawHtml;
    detailDescription.innerHTML = sanitized;
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

  function loadSettings() {
    const storedCompanyId = localStorage.getItem(STORAGE_KEYS.companyId);
    const storedApiBase = localStorage.getItem(STORAGE_KEYS.apiBase);
    const storedQuery = localStorage.getItem(STORAGE_KEYS.query);
    const storedActiveFilter = localStorage.getItem(STORAGE_KEYS.activeFilter);
    const storedSince = localStorage.getItem(STORAGE_KEYS.since);
    const storedLimit = localStorage.getItem(STORAGE_KEYS.limit);
    const storedLastCheck = localStorage.getItem(STORAGE_KEYS.lastCheckAt);
    const storedAutoUpdate = localStorage.getItem(STORAGE_KEYS.autoUpdateLastCheck);

    if (storedCompanyId !== null) {
      companyIdInput.value = storedCompanyId;
    }
    if (storedApiBase !== null) {
      apiBaseInput.value = storedApiBase;
    } else {
      apiBaseInput.value = defaultApiBase();
    }
    if (storedQuery !== null) {
      queryInput.value = storedQuery;
    }
    if (storedActiveFilter !== null) {
      activeFilter.value = storedActiveFilter;
    }
    if (storedSince !== null && storedSince.trim()) {
      sinceInput.value = storedSince;
    } else if (storedLastCheck) {
      sinceInput.value = storedLastCheck;
    } else {
      sinceInput.value = defaultSince;
    }
    if (storedLimit !== null) {
      limitInput.value = storedLimit;
    }
    if (storedAutoUpdate !== null) {
      autoUpdateLastCheckToggle.checked = storedAutoUpdate === "true";
    } else {
      autoUpdateLastCheckToggle.checked = true;
    }
    updateLastCheckHint();
  }

  function persistSettings() {
    localStorage.setItem(STORAGE_KEYS.companyId, companyIdInput.value.trim());
    localStorage.setItem(STORAGE_KEYS.apiBase, apiBaseInput.value.trim());
    localStorage.setItem(STORAGE_KEYS.query, queryInput.value.trim());
    localStorage.setItem(STORAGE_KEYS.activeFilter, activeFilter.value);
    localStorage.setItem(STORAGE_KEYS.since, sinceInput.value.trim());
    localStorage.setItem(STORAGE_KEYS.limit, limitInput.value.trim());
    localStorage.setItem(
      STORAGE_KEYS.autoUpdateLastCheck,
      autoUpdateLastCheckToggle.checked ? "true" : "false"
    );
  }

  function setLastCheck(timestamp, updateSinceInput) {
    localStorage.setItem(STORAGE_KEYS.lastCheckAt, timestamp);
    if (updateSinceInput) {
      sinceInput.value = timestamp;
    }
    updateLastCheckHint();
    persistSettings();
  }

  function updateLastCheckHint() {
    if (!lastCheckHint) {
      return;
    }
    const storedLastCheck = localStorage.getItem(STORAGE_KEYS.lastCheckAt);
    lastCheckHint.textContent = storedLastCheck
      ? `Last check: ${storedLastCheck}`
      : "Last check: Not set";
  }

  async function searchCompanies(term) {
    companySuggestions.innerHTML = "<div class=\"meta\">Searching...</div>";
    try {
      const url = new URL(buildApiUrl("/api/companies"));
      url.searchParams.set("search", term);
      url.searchParams.set("limit", "10");
      const response = await fetch(url.toString());
      if (!response.ok) {
        companySuggestions.innerHTML = "<div class=\"meta\">Search failed. Check API Base URL.</div>";
        return;
      }
      const results = await response.json();
      renderCompanySuggestions(results);
    } catch (err) {
      companySuggestions.innerHTML = "<div class=\"meta\">Search failed. Check API Base URL.</div>";
    }
  }

  function renderCompanySuggestions(results) {
    if (!results || results.length === 0) {
      companySuggestions.innerHTML = "<div class=\"meta\">No matches.</div>";
      return;
    }
    companySuggestions.innerHTML = "";
    results.forEach((item) => {
      const entry = document.createElement("div");
      entry.className = "suggestion";
      entry.textContent = `${item.ticker || ""} - ${item.name || ""} (ID ${item.id})`;
      entry.addEventListener("click", () => {
        companyIdInput.value = item.id;
        persistSettings();
        companySuggestions.innerHTML = "";
      });
      companySuggestions.appendChild(entry);
    });
  }

  function defaultApiBase() {
    return window.location.origin.includes(":8080") ? window.location.origin : "http://localhost:8080";
  }

  function normalizeApiBase(raw) {
    if (!raw) {
      return defaultApiBase();
    }
    const trimmed = raw.trim();
    if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
      return trimmed;
    }
    return `http://${trimmed}`;
  }

  function buildApiUrl(path) {
    const base = normalizeApiBase(apiBaseInput.value);
    return new URL(path, base).toString();
  }

  function resolvePostingUrl(job) {
    const canonical = job.canonicalUrl || "";
    if (canonical.trim()) {
      return canonical;
    }
    const source = job.sourceUrl || "";
    return source.trim() ? source : null;
  }
})();
