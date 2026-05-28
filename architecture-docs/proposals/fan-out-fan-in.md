# Fan-out / fan-in: a syntax proposal

**Status:** draft, for team review
**Scope:** API surface only — implementation deferred

## Problem

Map-reduce in workflows today requires manual buffering, static cardinality, and dishonest return annotations. A representative example:

```python
class FanOutFanIn(Workflow):
    @step
    async def fan_out(self, ctx: Context, ev: StartEvent) -> TaskRequested | None:
        times = random.randint(1, 10)
        await ctx.store.set("times", times)
        for _ in range(times):
            ctx.send_event(TaskRequested())

    @step
    async def process_task(self, ev: TaskRequested) -> TaskReady:
        return TaskReady()

    @step
    async def fan_in(self, ctx: Context, ev: TaskReady) -> StopEvent | None:
        n = await ctx.store.get("times")
        events = ctx.collect_events(ev, [TaskReady] * n)
        if events is not None:
            return StopEvent(results=events)
```

The issues:

- **Type annotations lie.** `fan_out` declares `-> TaskRequested | None` but actually emits via `ctx.send_event` and returns `None`. The validator can't see what's really sent.
- **Static cardinality.** `n` has to be threaded through `ctx.store` and re-read in the fan-in.
- **Re-entrant fan-in.** `collect_events` works by re-invoking the step per arrival, gating on a counter.
- **Doesn't compose.** Multi-level fan-out requires nested counters, nested buffer ids, and careful bookkeeping.

The type graph is the substrate every richer feature would build on, and `ctx.send_event` makes it untrustworthy. Fixing that unlocks everything else.

## Proposal

Two changes, working together.

### 1. Steps return what they emit

A step's return signature exhaustively describes what events it produces. Single events, lists, and async iterators are all legal:

```python
@step
async def step_a(ev: StartEvent) -> Done: ...                  # one event

@step
async def step_b(ev: StartEvent) -> list[Task]: ...            # static-count batch

@step
async def step_c(ev: StartEvent) -> AsyncIterator[Task]:       # streamed batch
    for i in range(random.randint(1, 10)):
        yield Task(i)
```

`list[E]` and `AsyncIterator[E]` are batch emissions: every event in the batch shares a `batch_id`, and the batch closes when the list is consumed or the iterator exits. `ctx.send_event` continues to exist but is restricted to types declared in the step's return annotation.

### 2. Fan-in is parameter-typed

A parameter typed `list[E]` means *"give me the closed batch of E events."* Cardinality, scope, and provenance default sensibly. Non-defaults opt in via `Annotated[..., Collect(...)]`.

```python
@step
async def join(events: list[Done]) -> StopEvent:
    return StopEvent(results=events)
```

That's the whole common case. No decorator argument, no buffer id, no counter in `ctx.store`.

## How it reads in practice

```python
# 1. Atomic homogeneous batch
async def join(events: list[Done]) -> StopEvent: ...

# 2. Two upstream fan-outs converge
async def merge(
    queries: list[QueryDone],
    fetches: list[FetchDone],
) -> StopEvent: ...

# 3. Heterogeneous siblings of one batch (replaces today's collect_events)
async def assemble(h: Header, b: Body, f: Footer) -> Page: ...

# 4. Heterogeneous flat list
async def report(events: list[Done | Skipped]) -> StopEvent: ...

# 5. Multi-level fan-out, joining at each level
async def per_inner(events: list[Done]) -> InnerSummary: ...
async def per_outer(
    events: Annotated[list[Done], Collect(at=outer)],
) -> StopEvent: ...

# 6. First wins
async def fastest(
    events: Annotated[list[Result], Collect(Take(1))],
) -> StopEvent: ...

# 7. Quorum
async def commit(
    acks: Annotated[list[Ack], Collect(AtLeast(3))],
) -> StopEvent: ...

# 8. Provenance disambiguation (validator forces this when ambiguous)
async def compare(
    a: Annotated[list[Doc], Collect(from_=index_a)],
    b: Annotated[list[Doc], Collect(from_=index_b)],
) -> StopEvent: ...

# 9. Streaming windowed aggregation
async def summarize_chunk(
    tokens: Annotated[list[Token], Collect(Buffer(size=20))],
) -> ChunkSummary: ...
```

## Inference rules

A step is **collect-mode** if any parameter is `list[Event]`, has a `Collect` marker, or there is more than one event-shaped param. Otherwise it is the existing event-triggered model.

| Parameter shape | Means |
|---|---|
| `ev: E` (only event-shaped param) | Event-triggered, fires per arrival (today's model) |
| `events: list[E]` | `Collect(All)` from nearest enclosing batch producing `E` |
| `events: list[A \| B]` | Same, union widens the type universe |
| `e: E` alongside other event-shaped params | `Collect(Take(1))` — exactly one of `E` from the batch |
| `Annotated[list[E], Collect(...)]` | Explicit, for non-default scope / provenance / cardinality |

**Mixed-mode rule:** once any parameter is collect-shaped, every event-shaped parameter is collect. Single-event triggers exist only when the step has exactly one event-shaped parameter and nothing else.

**Backward compatibility:** today's signature validator rejects multiple event params and `list[Event]` params outright. We are claiming previously-invalid syntactic real estate. No existing code changes meaning.

## Selection algebra (`Collect`)

Three orthogonal axes wrapped in one marker:

```python
@dataclass(frozen=True)
class Collect:
    cardinality: Cardinality = All()      # inferred from param if omitted
    at: StepRef | None = None             # promote scope to this step's batch
    from_: StepRef | None = None          # provenance restriction
    where: Callable[[E], bool] | None = None
```

`Cardinality` is a closed hierarchy:

- `All()` — release on batch close (default)
- `Take(n)` — release on Nth arrival, drop the rest
- `AtLeast(n)` — release on Nth arrival, continue accepting
- `Buffer(size)` — re-fire every `size` events (tumbling)
- `Window(size, slide)` — re-fire every `slide`, payload is last `size` (sliding)

**Scope resolution:** `Collect` defaults to "nearest enclosing batch that produces this type." `at=step` promotes to that step's batch. The validator computes scope statically and errors when ambiguous, demanding explicit disambiguation.

**Multi-slot release:** when a step has multiple `Collect` parameters, all slots must be satisfied at their own scope before the step fires (once). Per-key release becomes an explicit `@step(release="any")` opt-in.

## AsyncIterator semantics

Iterator and `list[E]` returns emit events as they are yielded. Replay (worker death, retry, journal restart) re-runs the iterator from the start and **events emitted on prior runs may be emitted again**. This matches the existing semantics of `ctx.send_event` — the framework does not enforce a determinism contract or attempt to dedupe.

**Replay journal.** Steps that want to skip previously-emitted events on replay can read them from the context:

```python
@step
async def fan_out(ev: StartEvent, ctx: Context) -> AsyncIterator[Task]:
    already_sent = ctx.replayed_events()    # list[Event]; empty on first run
    seen = {e.id for e in already_sent}
    for item in source():
        if item.id in seen:
            continue
        yield Task(item)
```

The framework provides the data; the dedupe policy is user-defined. "Same event" is a domain concept (id equality, content hash, position in some external stream) — only the user knows which one applies. Steps that don't read the journal will produce duplicate emissions on replay, exactly as `send_event` does today.

**Batch closure.** A batch closes on iterator exit (`BatchClosed`) or on retry exhaustion (`BatchAborted`). Joins fire on closure; `Take` and `Window` modifiers may fire earlier. Without user-side dedupe, replays can extend a batch with duplicate events; cardinality modifiers count what they receive.

**Retry exhaustion mid-stream.** If the iterator step exhausts its retry budget before exiting, emit `BatchAborted(batch_id, partial=k, error=...)`. Joins decide what to do via `on_partial="fire" | "fail"` (default `"fail"` — silent partial joins are footguns).

## What's explicitly out of scope (for now)

- **`GroupBy(E, key=...)`** and **`Zip(A, B, on=...)`** — push to user code or composable steps. Reconsider if real demand emerges.
- **Predicate filters as cardinality affecters.** `where` is narrowing only.
- **Time-based windows.** Need a reducer-level clock; orthogonal feature.
- **Cross-run joins.** Different problem; needs persistent watermarks.
- **Cancellation of `Take`'d siblings.** Cancellation is its own feature; today siblings keep running and outputs land in the journal as unconsumed.
- **Mixing event trigger and `Collect` in one step.** Forced by the inference rule above.

## Migration

`ctx.collect_events` and the `ctx.send_event`-in-a-loop pattern continue to work — they're additive. Documentation will steer new code toward the typed forms; we can deprecate the old paths on a separate timeline once the new ones are stable.

## Open questions for the team

1. **Empty batches.** `return []` from a fan-out — does the join fire once with `[]`, or skip entirely? Proposal: fire with empty list. "No work to do" is a valid result and one place to handle it is better than two.

2. **Branch death.** If a downstream step in a fan-out branch returns `None`, does the join see fewer events than were emitted? Proposal: yes. Track closure by branches-terminated, not events-received. Matches "filter inside the map" intuition.

3. **`Take(1)` and cancellation.** Sibling branches keep running by default. Is that surprising? Worth a follow-up cancellation primitive, but explicitly not in v1.

4. **Per-key release on multi-slot collects.** Default is all-keys-gate (single fire). Is `release="any"` valuable enough to ship in v1, or defer?

5. **`Annotated[list[Done], Collect()]` as documentation form.** Allow as a synonym for bare `list[Done]`? Some teams will want the explicit marker for grep-ability. Proposal: allow.

6. **OpenTelemetry alignment.** Internal lineage fields (`batch_id`, parent linkage) are isomorphic to OTel's `trace_id` / `span_id` / `parent_span_id`. Worth naming them the same so instrumentation lines up, even if we don't expose them as user API.

7. **Replay-journal accessor.** What's the right name and shape for `ctx.replayed_events()`? Candidates: a method returning `list[Event]`, an attribute, a typed view (`ctx.replay.events`), or scoped per-batch (`ctx.replay.for_batch(batch_id)`). It only matters for steps that want to dedupe explicitly — most won't.

## Phasing

**v1** — atom shape, multi-param structuring, `Collect(at=, from_=)`, `All` / `Take` / `AtLeast`, all-keys-gate release, `list[E]` and `AsyncIterator[E]` returns, `BatchClosed` / `BatchAborted` ticks, `ctx.replayed_events()` accessor.

**v2** — `Buffer`, `Window`, `release="any"`, `where` predicates.

**v3 / never** — `GroupBy`, `Zip(on=)`, time-based windows, cross-run joins.

The v1 cut covers the patterns we see in real workflows today. v2 adds streaming aggregation. v3 is where graph-query-DSL ambitions get traded against scope.

## Why this shape

- **Type graph stays honest.** Every event in the system has a static `produced_by: set[StepName]`. The validator can compute lineage selection at decoration time.
- **Single source of truth.** Type info lives on the parameter, not duplicated in a decorator dict. Editing the signature can't drift.
- **Locality.** Selection annotation sits two characters from the parameter that receives the result.
- **Consistency with resources.** Resources already use `Annotated[T, ResourceDescriptor]`. `Collect` joins the same lane instead of inventing a parallel one.
- **Composes.** Multi-level fan-outs Just Work because batch ids stack and scope defaults to the nearest enclosing producer. `at=` is the only knob needed to reach across levels.
- **The terse case is genuinely terse.** `async def join(events: list[Done]) -> StopEvent` is the entire fan-in step. Zero imports beyond what the user already has.

## Inspirations

- **Cypher (Neo4j)** — bindings as result keys; we get this for free from parameter names.
- **Dagster `AssetSelection`** — atoms + traversal + set operators over a known DAG. The `at=` / `from_=` shape borrows directly.
- **ReactiveX** — operator vocabulary (`take`, `buffer`, `window`); names retained where semantics align.
- **Apache Beam** — main-input / side-input distinction informs the multi-level scope mental model.
- **OpenTelemetry** — lineage field naming, for free instrumentation alignment downstream.
