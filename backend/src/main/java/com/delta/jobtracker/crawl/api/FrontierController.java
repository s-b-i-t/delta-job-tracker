package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.crawl.model.FrontierSeedResponse;
import com.delta.jobtracker.crawl.service.FrontierSeedService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/frontier")
public class FrontierController {
  private final FrontierSeedService frontierSeedService;

  public FrontierController(FrontierSeedService frontierSeedService) {
    this.frontierSeedService = frontierSeedService;
  }

  @PostMapping("/seed")
  public FrontierSeedResponse seedFromCompanyDomains(
      @RequestParam(name = "domainLimit", required = false) Integer domainLimit,
      @RequestParam(name = "maxSitemapFetches", required = false) Integer maxSitemapFetches) {
    return frontierSeedService.seedFromCompanyDomains(domainLimit, maxSitemapFetches);
  }
}
