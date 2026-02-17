package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.crawl.model.CrawlDaemonBootstrapResponse;
import com.delta.jobtracker.crawl.model.CrawlDaemonStatusResponse;
import com.delta.jobtracker.crawl.service.CrawlDaemonService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/daemon")
public class CrawlDaemonController {
    private final CrawlDaemonService daemonService;

    public CrawlDaemonController(CrawlDaemonService daemonService) {
        this.daemonService = daemonService;
    }

    @PostMapping("/start")
    public CrawlDaemonStatusResponse start() {
        daemonService.start();
        return daemonService.getStatus();
    }

    @PostMapping("/stop")
    public CrawlDaemonStatusResponse stop() {
        daemonService.stop();
        return daemonService.getStatus();
    }

    @GetMapping("/status")
    public CrawlDaemonStatusResponse status() {
        return daemonService.getStatus();
    }

    @PostMapping("/bootstrap")
    public CrawlDaemonBootstrapResponse bootstrap(
        @RequestParam(name = "source", required = false, defaultValue = "wiki") String source
    ) {
        return daemonService.bootstrap(source);
    }
}
