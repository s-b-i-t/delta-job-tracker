ALTER TABLE companies
    ADD COLUMN wikipedia_title TEXT;

ALTER TABLE company_domains
    ADD COLUMN wikidata_qid VARCHAR(32);

ALTER TABLE company_domains
    ADD COLUMN resolution_method VARCHAR(64);

ALTER TABLE ats_endpoints
    ADD COLUMN detection_method VARCHAR(32) NOT NULL DEFAULT 'legacy';

ALTER TABLE ats_endpoints
    ADD COLUMN verified BOOLEAN NOT NULL DEFAULT true;
