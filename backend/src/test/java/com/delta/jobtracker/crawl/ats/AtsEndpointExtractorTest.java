package com.delta.jobtracker.crawl.ats;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import java.util.List;
import org.junit.jupiter.api.Test;

class AtsEndpointExtractorTest {
  private final AtsEndpointExtractor extractor = new AtsEndpointExtractor();

  @Test
  void extractsGreenhouseEmbedBoard() {
    String html = "<script src=\"https://boards.greenhouse.io/embed/job_board?for=acme\"></script>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(AtsType.GREENHOUSE, "https://boards.greenhouse.io/acme"));
  }

  @Test
  void normalizesJobBoardsGreenhouseHost() {
    String html = "<a href=\"https://job-boards.greenhouse.io/acme\">Jobs</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(AtsType.GREENHOUSE, "https://boards.greenhouse.io/acme"));
  }

  @Test
  void extractsLeverAccount() {
    String html = "<a href=\"https://jobs.lever.co/rocket\">Jobs</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(new AtsDetectionRecord(AtsType.LEVER, "https://jobs.lever.co/rocket"));
  }

  @Test
  void extractsLeverApplyAccount() {
    String html = "<a href=\"https://apply.lever.co/rocket/abc\">Apply</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(new AtsDetectionRecord(AtsType.LEVER, "https://jobs.lever.co/rocket"));
  }

  @Test
  void extractsWorkdayEndpoint() {
    String html = "<a href=\"https://acme.wd5.myworkdayjobs.com/en-US/External\">Careers</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.WORKDAY, "https://acme.wd5.myworkdayjobs.com/en-US/External"));
  }

  @Test
  void extractsWorkdayCxsEndpoint() {
    String html =
        "<a href=\"https://acme.wd5.myworkdayjobs.com/wday/cxs/acme/External/jobs\">Jobs</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(AtsType.WORKDAY, "https://acme.wd5.myworkdayjobs.com/External"));
  }

  @Test
  void stripsTrailingJunkFromWorkdayEndpoint() {
    String html = "<a href=\"https://acme.wd5.myworkdayjobs.com/en-US/External&\">Careers</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.WORKDAY, "https://acme.wd5.myworkdayjobs.com/en-US/External"));
  }

  @Test
  void extractsSmartRecruitersEndpoint() {
    String html = "<a href=\"https://careers.smartrecruiters.com/AcmeCorp\">Jobs</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.SMARTRECRUITERS, "https://careers.smartrecruiters.com/AcmeCorp"));
  }
}
