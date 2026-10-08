# Golden serialization fixtures

These pin the current snapshot and journal serialization formats. They must load
and replay unchanged at the behavioral level:

- `snapshot.json` — a `Context.to_dict()` (v2 `SerializedContext`) taken mid-run
  from a HITL workflow suspended on a `ctx.wait_for_event` waiter. Loading it
  must preserve current snapshot compatibility.
- `snapshot_meta.json` — `{"expected_result_after_resume": ...}`: resuming the
  snapshot and delivering `HumanResponse(response="42")` must yield this.
- `journal.json` — `{"result": 12, "ticks": [...]}`: a full tick journal for a
  fan-out + `collect_events` run. Replaying the ticks from a canonical
  `BrokerState.from_workflow` must reach `StopEvent(result=12)`.
- `broker_state_main_py314.b64` — DBOS default `py_pickle` workflow inputs with
  a `BrokerState` as the first argument. The state has a collection binding,
  an open stream, queued and active work, and a suspended waiter. It was
  generated through DBOS `serialize_args` from `origin/main` commit
  `b81a735e4e9e384b08dd26a975b43332180e996c` with Python 3.14.5 and
  pickle protocol 5. Its event classes live in
  `tests.test_golden_serialization_fixtures`; keep those names importable when
  updating the test. The workflows test uses the same base64 and pickle decoder
  as DBOS so the workflows package does not require DBOS. Loading it must
  preserve collection routing and pending work. The test accepts additional
  fixture names for later pickle epochs.
- `current_journal.json` — the same completed workflow journal in the current
  format, including a `session_start` marker and non-null stamps on stamped
  ticks. It pins additions to the current journal shape without changing the
  legacy compatibility fixture.

Regenerate only when intentionally updating the pinned main serialization
formats; see `tests/test_golden_serialization_fixtures.py` for the workflow
definitions the fixtures were produced from.
