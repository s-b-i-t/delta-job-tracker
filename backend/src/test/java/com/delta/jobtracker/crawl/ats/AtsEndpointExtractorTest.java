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

  @Test
  void extractsIcimsEndpoint() {
    String html = "<a href=\"https://jobs.icims.com/jobs/search?ss=1\">Open roles</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(AtsType.ICIMS, "https://jobs.icims.com/jobs/search"));
  }

  @Test
  void extractsTaleoEndpoint() {
    String html =
        "<a href=\"https://myco.taleo.net/careersection/prof/jobsearch.ftl?lang=en\">Jobs</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.TALEO, "https://myco.taleo.net/careersection/prof/jobsearch.ftl"));
  }

  @Test
  void extractsSuccessFactorsEndpoint() {
    String html =
        "<a href=\"https://career2.successfactors.eu/career?company=acme\">Open roles</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract(null, html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.SUCCESSFACTORS, "https://career2.successfactors.eu/career"));
  }

  @Test
  void extractsPaylocityEndpointFromLandingLink() {
    String html =
        "<a href=\"https://recruiting.paylocity.com/recruiting/jobs/All/f2fda2c7-bfc6-4840-90d0-83d242cada87/Arbutus-Biopharma-Inc\">Jobs</a>";
    List<AtsDetectionRecord> endpoints = extractor.extract("https://example.com/careers", html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.PAYLOCITY,
                "https://recruiting.paylocity.com/recruiting/jobs/All/f2fda2c7-bfc6-4840-90d0-83d242cada87/Arbutus-Biopharma-Inc"));
  }

  @Test
  void extractsBrassRingEndpointFromFormAction() {
    String html =
        "<form action=\"https://sjobs.brassring.com/TGnewUI/Search/Home/HomeWithPreLoad?partnerid=25008&siteid=5246\"></form>";
    List<AtsDetectionRecord> endpoints = extractor.extract("https://example.com/careers", html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.BRASSRING,
                "https://sjobs.brassring.com/TGnewUI/Search/Home/HomeWithPreLoad?partnerid=25008&siteid=5246"));
  }

  @Test
  void extractsSuccessFactorsEndpointFromAssetHost() {
    String html =
        "<script src=\"https://career4.successfactors.com/career?company=acme\"></script>";
    List<AtsDetectionRecord> endpoints = extractor.extract("https://example.com/careers", html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.SUCCESSFACTORS, "https://career4.successfactors.com/career"));
  }

  @Test
  void extractsDayforceEndpoint() {
    String html =
        "<a href=\"https://careers.dayforcehcm.com/en-US/acmejobs\"></a>";
    List<AtsDetectionRecord> endpoints = extractor.extract("https://example.com/careers", html);
    assertThat(endpoints)
        .containsExactly(
            new AtsDetectionRecord(
                AtsType.DAYFORCE, "https://careers.dayforcehcm.com/en-US/acmejobs"));
  }
}
