package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.JobPostingUrlRef;
import com.delta.jobtracker.crawl.model.WorkdayInvalidUrlCleanupResponse;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.util.JobUrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class WorkdayInvalidUrlCleanupService {
    private static final Logger log = LoggerFactory.getLogger(WorkdayInvalidUrlCleanupService.class);
    private static final String HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    private static final int DEFAULT_BATCH_SIZE = 200;
    private static final int DELETE_BATCH_SIZE = 50;

    private final PoliteHttpClient httpClient;
    private final CrawlJdbcRepository repository;

    public WorkdayInvalidUrlCleanupService(PoliteHttpClient httpClient, CrawlJdbcRepository repository) {
        this.httpClient = httpClient;
        this.repository = repository;
    }

    public WorkdayInvalidUrlCleanupResponse cleanupInvalidWorkdayUrls(Integer limit, Integer batchSize, boolean dryRun) {
        int safeLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : Math.max(1, limit);
        int safeBatchSize = batchSize == null || batchSize <= 0 ? DEFAULT_BATCH_SIZE : Math.max(1, batchSize);
        int scanned = 0;
        int invalid = 0;
        int deleted = 0;
        int errors = 0;
        long lastId = 0L;

        while (scanned < safeLimit) {
            int fetchSize = Math.min(safeBatchSize, safeLimit - scanned);
            List<JobPostingUrlRef> batch = repository.findWorkdayJobPostingUrls(lastId, fetchSize);
            if (batch.isEmpty()) {
                break;
            }

            List<Long> toDelete = new ArrayList<>();
            for (JobPostingUrlRef ref : batch) {
                lastId = ref.jobId();
                scanned++;

                String url = ref.canonicalUrl();
                if (url == null || url.isBlank()) {
                    continue;
                }
                if (JobUrlUtils.isInvalidWorkdayUrl(url) || JobUrlUtils.isInvalidWorkdayRedirectUrl(url)) {
                    invalid++;
                    if (!dryRun) {
                        toDelete.add(ref.jobId());
                    }
                    continue;
                }

                HttpFetchResult fetch = httpClient.get(url, HTML_ACCEPT);
                if (fetch == null) {
                    errors++;
                    continue;
                }
                if (JobUrlUtils.isInvalidWorkdayRedirectUrl(fetch.finalUrlOrRequested())) {
                    invalid++;
                    if (!dryRun) {
                        toDelete.add(ref.jobId());
                    }
                    continue;
                }
                if (!fetch.isSuccessful()) {
                    errors++;
                }
            }

            if (!dryRun && !toDelete.isEmpty()) {
                deleted += deleteInBatches(toDelete);
            }

            log.info(
                "Workday invalid URL cleanup progress: scanned={}, invalid={}, deleted={}, errors={}, lastId={}",
                scanned,
                invalid,
                deleted,
                errors,
                lastId
            );
        }

        boolean limitReached = limit != null && limit > 0 && scanned >= limit;
        return new WorkdayInvalidUrlCleanupResponse(scanned, invalid, deleted, errors, dryRun, lastId, limitReached);
    }

    private int deleteInBatches(List<Long> ids) {
        int removed = 0;
        for (int i = 0; i < ids.size(); i += DELETE_BATCH_SIZE) {
            int end = Math.min(ids.size(), i + DELETE_BATCH_SIZE);
            removed += repository.deleteJobPostingsByIds(ids.subList(i, end));
        }
        return removed;
    }
}
