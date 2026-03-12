package com.delta.jobtracker.crawl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.model.AtsType;
import org.junit.jupiter.api.Test;

class AtsDetectorTest {
  private final AtsDetector detector = new AtsDetector();

  @Test
  void detectsWorkday() {
    assertEquals(
        AtsType.WORKDAY, detector.detect("https://company.wd5.myworkdayjobs.com/en-US/External"));
  }

  @Test
  void detectsGreenhouse() {
    assertEquals(
        AtsType.GREENHOUSE, detector.detect("https://boards.greenhouse.io/example/jobs/123"));
  }

  @Test
  void detectsLever() {
    assertEquals(AtsType.LEVER, detector.detect("https://jobs.lever.co/example/abc"));
  }

  @Test
  void detectsLeverApply() {
    assertEquals(AtsType.LEVER, detector.detect("https://apply.lever.co/example/abc"));
  }

  @Test
  void detectsSmartRecruiters() {
    assertEquals(
        AtsType.SMARTRECRUITERS, detector.detect("https://careers.smartrecruiters.com/example"));
  }

  @Test
  void detectsIcims() {
    assertEquals(AtsType.ICIMS, detector.detect("https://jobs.icims.com/jobs/search?ss=1"));
  }

  @Test
  void detectsTaleo() {
    assertEquals(
        AtsType.TALEO,
        detector.detect("https://myco.taleo.net/careersection/prof/jobsearch.ftl?lang=en"));
  }

  @Test
  void detectsSuccessFactors() {
    assertEquals(
        AtsType.SUCCESSFACTORS,
        detector.detect("https://career2.successfactors.eu/career?company=acme"));
  }

  @Test
  void detectsPaylocity() {
    assertEquals(
        AtsType.PAYLOCITY,
        detector.detect(
            "https://recruiting.paylocity.com/recruiting/jobs/All/f2fda2c7-bfc6-4840-90d0-83d242cada87/Arbutus-Biopharma-Inc"));
  }

  @Test
  void detectsBrassRing() {
    assertEquals(
        AtsType.BRASSRING,
        detector.detect(
            "https://sjobs.brassring.com/TGnewUI/Search/Home/HomeWithPreLoad?partnerid=25008&siteid=5246"));
  }

  @Test
  void detectsDayforce() {
    assertEquals(
        AtsType.DAYFORCE,
        detector.detect("https://careers.dayforcehcm.com/en-US/acmejobs"));
  }

  @Test
  void unknownForRegularCompanySite() {
    assertEquals(AtsType.UNKNOWN, detector.detect("https://example.com/careers"));
  }

  @Test
  void detectsFromHtmlMarkers() {
    String html =
        "<html><script>var x='https://api.lever.co/v0/postings/example?mode=json';</script></html>";
    assertEquals(AtsType.LEVER, detector.detect("https://example.com/careers", html));
  }

  @Test
  void detectsSuccessFactorsFromHtmlMarkers() {
    String html = "<a href='https://career2.successfactors.eu/career?company=acme'>Jobs</a>";
    assertEquals(AtsType.SUCCESSFACTORS, detector.detect("https://example.com/careers", html));
  }

  @Test
  void detectsBrassRingFromHtmlMarkers() {
    String html =
        "<form action='https://sjobs.brassring.com/TGnewUI/Search/Home/HomeWithPreLoad?partnerid=25008&siteid=5246'></form>";
    assertEquals(AtsType.BRASSRING, detector.detect("https://example.com/careers", html));
  }

  @Test
  void detectsDayforceFromHtmlMarkers() {
    String html =
        "<iframe src='https://careers.dayforcehcm.com/en-US/acmejobs'></iframe>";
    assertEquals(AtsType.DAYFORCE, detector.detect("https://example.com/careers", html));
  }
}
