-- migration: 2

ALTER TABLE run_lifecycle ADD COLUMN resume_token TEXT;
