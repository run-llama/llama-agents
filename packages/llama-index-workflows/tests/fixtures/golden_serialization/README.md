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
- `current_journal.json` — the same completed workflow journal in the current
  format, including a `session_start` marker and non-null stamps on stamped
  ticks. It pins additions to the current journal shape without changing the
  legacy compatibility fixture.

Regenerate only when intentionally updating the pinned main serialization
formats; see `tests/test_golden_serialization_fixtures.py` for the workflow
definitions the fixtures were produced from.
