(() => {
  const companyIdInput = document.getElementById("companyId");
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

  const state = {
    jobs: []
  };

  const defaultSince = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  sinceInput.value = defaultSince;

  closeDetail.addEventListener("click", () => {
    detailPanel.classList.add("hidden");
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

    if (companyId) {
      params.companyId = companyId;
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
})();
