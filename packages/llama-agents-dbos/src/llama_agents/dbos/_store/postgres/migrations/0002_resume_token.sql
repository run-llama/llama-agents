-- migration: 2

ALTER TABLE run_lifecycle ADD COLUMN IF NOT EXISTS resume_token VARCHAR(64);
