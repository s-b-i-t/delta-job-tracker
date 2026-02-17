(() => {
  const companySearchInput = document.getElementById("companySearch");
  const companySuggestions = document.getElementById("companySuggestions");
  const selectedCompany = document.getElementById("selectedCompany");
  const queryInput = document.getElementById("query");
  const searchBtn = document.getElementById("searchBtn");
  const clearBtn = document.getElementById("clearBtn");
  const resultsList = document.getElementById("resultsList");
  const resultsMeta = document.getElementById("resultsMeta");
  const prevPageBtn = document.getElementById("prevPage");
  const nextPageBtn = document.getElementById("nextPage");
  const pageIndicator = document.getElementById("pageIndicator");

  const STORAGE_KEYS = {
    companyId: "dj_companyId",
    companyLabel: "dj_companyLabel",
    query: "dj_query"
  };

  const state = {
    companyId: null,
    companyLabel: "",
    page: 0,
    pageSize: 50,
    total: 0,
    items: []
  };

  loadSettings();
  updateSelectedCompany();

  queryInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      state.page = 0;
      loadPage();
    }
  });

  searchBtn.addEventListener("click", () => {
    state.page = 0;
    loadPage();
  });

  clearBtn.addEventListener("click", () => {
    state.companyId = null;
    state.companyLabel = "";
    queryInput.value = "";
    state.page = 0;
    persistSettings();
    updateSelectedCompany();
    loadPage();
  });

  prevPageBtn.addEventListener("click", () => {
    if (state.page > 0) {
      state.page -= 1;
      loadPage();
    }
  });

  nextPageBtn.addEventListener("click", () => {
    if ((state.page + 1) * state.pageSize < state.total) {
      state.page += 1;
      loadPage();
    }
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

  async function loadPage() {
    persistSettings();
    resultsMeta.textContent = "Loading results...";
    resultsList.innerHTML = "";
    let requestUrl;
    try {
      requestUrl = buildApiUrl("/api/jobs/page");
      const url = new URL(requestUrl);
      url.searchParams.set("page", state.page.toString());
      url.searchParams.set("pageSize", state.pageSize.toString());
      if (state.companyId) {
        url.searchParams.set("companyId", state.companyId);
      }
      const query = queryInput.value.trim();
      if (query) {
        url.searchParams.set("q", query);
      }
      const response = await fetch(url.toString());
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || "Request failed");
      }
      const payload = await response.json();
      state.total = payload.total || 0;
      state.items = payload.items || [];
      updatePagination();
      renderResults(state.items);
      return { success: true, items: state.items };
    } catch (err) {
      const urlText = requestUrl ? ` URL: ${requestUrl}` : "";
      resultsMeta.textContent = `Error: ${err.message}.${urlText}`;
      updatePagination();
      return { success: false, items: [] };
    }
  }

  function renderResults(items) {
    if (!items || items.length === 0) {
      resultsList.innerHTML = "<p class=\"empty\">No jobs found.</p>";
      return;
    }
    resultsList.innerHTML = "";
    items.forEach((job) => {
      const item = document.createElement("div");
      item.className = "job-card";

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

      item.appendChild(title);
      item.appendChild(meta);
      item.appendChild(timestamps);
      resultsList.appendChild(item);
    });
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
    const storedQuery = localStorage.getItem(STORAGE_KEYS.query);
    const storedCompanyLabel = localStorage.getItem(STORAGE_KEYS.companyLabel);

    if (storedCompanyId !== null) {
      state.companyId = storedCompanyId;
    }
    if (storedCompanyLabel !== null) {
      state.companyLabel = storedCompanyLabel;
    }
    if (storedQuery !== null) {
      queryInput.value = storedQuery;
    }
  }

  function persistSettings() {
    localStorage.setItem(STORAGE_KEYS.companyId, state.companyId || "");
    localStorage.setItem(STORAGE_KEYS.companyLabel, state.companyLabel || "");
    localStorage.setItem(STORAGE_KEYS.query, queryInput.value.trim());
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
        state.companyId = String(item.id);
        state.companyLabel = `${item.ticker || ""} - ${item.name || ""} (ID ${item.id})`;
        state.page = 0;
        persistSettings();
        updateSelectedCompany();
        companySuggestions.innerHTML = "";
        loadPage();
      });
      companySuggestions.appendChild(entry);
    });
  }

  function updateSelectedCompany() {
    if (!state.companyId || !state.companyLabel) {
      selectedCompany.classList.add("hidden");
      selectedCompany.innerHTML = "";
      return;
    }
    selectedCompany.classList.remove("hidden");
    selectedCompany.innerHTML = "";
    const label = document.createElement("span");
    label.textContent = `Selected: ${state.companyLabel}`;
    const clear = document.createElement("button");
    clear.className = "chip-btn";
    clear.type = "button";
    clear.textContent = "Clear";
    clear.addEventListener("click", () => {
      state.companyId = null;
      state.companyLabel = "";
      persistSettings();
      updateSelectedCompany();
      loadPage();
    });
    selectedCompany.appendChild(label);
    selectedCompany.appendChild(clear);
  }

  function buildApiUrl(path) {
    const base = window.location.origin && window.location.origin !== "null"
      ? window.location.origin
      : "http://localhost:8080";
    return new URL(path, base).toString();
  }

  function resolvePostingUrl(job) {
    const canonical = safeUrl(job.canonicalUrl);
    if (canonical) {
      return canonical;
    }
    const source = safeUrl(job.sourceUrl);
    return source || null;
  }

  function safeUrl(value) {
    if (!value) {
      return null;
    }
    const trimmed = String(value).trim();
    if (!trimmed || trimmed.includes("invalid-url")) {
      return null;
    }
    try {
      const parsed = new URL(trimmed);
      if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        return null;
      }
      return trimmed;
    } catch (err) {
      return null;
    }
  }

  function updatePagination() {
    const totalPages = state.total === 0 ? 0 : Math.ceil(state.total / state.pageSize);
    const pageLabel = totalPages === 0 ? "Page 0 of 0" : `Page ${state.page + 1} of ${totalPages}`;
    pageIndicator.textContent = pageLabel;
    prevPageBtn.disabled = state.page <= 0;
    nextPageBtn.disabled = state.total === 0 || (state.page + 1) * state.pageSize >= state.total;
    const start = state.total === 0 ? 0 : state.page * state.pageSize + 1;
    const end = Math.min(state.total, (state.page + 1) * state.pageSize);
    resultsMeta.textContent = state.total === 0
      ? "No results."
      : `Showing ${start}-${end} of ${state.total} jobs.`;
  }

  loadPage();
})();
