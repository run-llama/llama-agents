-- migration: 3

-- Earlier installations checked constraint names across the entire database.
-- Each schema needs its own constraints. Existing duplicate cursors have no
-- unambiguous replay order; stop without changing any rows rather than guessing.
DO $$
DECLARE
    bad_run TEXT;
    bad_sequence INTEGER;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_wf_events_run_id_sequence'
          AND conrelid = 'wf_events'::regclass AND contype = 'u'
    ) THEN
        SELECT run_id, sequence INTO bad_run, bad_sequence
        FROM wf_events GROUP BY run_id, sequence HAVING COUNT(*) > 1 LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'Cannot enforce event sequence identity: duplicate run_id %, sequence %. Preserve and reconcile event rows before retrying migration.', bad_run, bad_sequence;
        END IF;
        ALTER TABLE wf_events ADD CONSTRAINT uq_wf_events_run_id_sequence UNIQUE (run_id, sequence);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_wf_ticks_run_id_sequence'
          AND conrelid = 'wf_ticks'::regclass AND contype = 'u'
    ) THEN
        SELECT run_id, sequence INTO bad_run, bad_sequence
        FROM wf_ticks GROUP BY run_id, sequence HAVING COUNT(*) > 1 LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'Cannot enforce tick sequence identity: duplicate run_id %, sequence %. Preserve and reconcile tick rows before retrying migration.', bad_run, bad_sequence;
        END IF;
        ALTER TABLE wf_ticks ADD CONSTRAINT uq_wf_ticks_run_id_sequence UNIQUE (run_id, sequence);
    END IF;
END $$;
