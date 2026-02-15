ALTER TABLE job_postings
    ADD COLUMN IF NOT EXISTS description_plain TEXT;

UPDATE job_postings
SET description_plain = regexp_replace(description_text, '<[^>]*>', ' ', 'g')
WHERE description_plain IS NULL
  AND description_text IS NOT NULL;

ALTER TABLE job_postings
    ADD COLUMN IF NOT EXISTS search_tsv ${search_tsv_type} ${search_tsv_generated};

${search_tsv_index}
