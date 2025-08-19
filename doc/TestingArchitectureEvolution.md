# Testing Architecture Evolution Roadmap
Alignment with Tier‑B / Tier‑C Streaming, Pipeline Runtime Orchestration, and Post‑Beta Convergence

Date: 2025-08-19  
Owner: Architecture & Test Engineering  
Status: Draft (Foundational Analysis)  

---

## Table of Contents
1. Executive Summary
2. Current Testing Posture (Snapshot)
3. Emerging Architecture Drivers (Runtime & Execution Backends)
4. Gap Analysis: Present Tests vs Future Needs
5. Layered Test Strategy (End-State Vision)
6. Evolution Plan by Layer
7. Synchronous Engine & Deterministic Execution Support
8. Exploratory / Systematic Concurrency Engine (Schedule Control)
9. Backpressure & Flow-Control Validation
10. Performance & Allocation Testing Strategy
11. Token / Streaming (Tier‑C) Specific Test Extensions
12. Orchestration Runtime (Segments, Bridges, Backends) Test Strategy
13. DSL / EIP Overlay & Connector Registry Testing
14. State Backend & Window/Trigger Testing
15. Failure Semantics & Error Channel Tests
16. Observability & Diagnostics Validation
17. Risk Register & Mitigations
18. Tooling / Instrumentation Enhancements
19. Recommended Execution Phasing
20. Key Metrics & Success Criteria
21. Summary Checklist
22. Appendix: Sample Test Matrix (Phase Mapping)

---

## 1. Executive Summary

We are expanding from a *monadic, envelope-by-envelope* synchronous-paradigm test harness toward a **multi-backend, partially asynchronous, token-enabled, stateful streaming runtime** supporting:
- Multiple execution backends (Sync, Async, Hopac, TPL)
- Cross-boundary bridges with backpressure
- Streaming token pipelines (Tier‑C)
- Windowing, accumulation triggers, dynamic parallelism, and (later) checkpoints
- Systematic concurrency exploration

Our existing property and model-based tests are strong at:
- Functional correctness of parsing/accumulation/projection
- Structural invariants (depth, completion)
- Core monad laws
- Some robustness/error simulation

They are **not yet sufficient** for:
- Validating deterministic vs non-deterministic execution under different backends
- Schedule control and interleaving enumeration
- Cross-backend backpressure propagation
- Memory churn & throughput regressions under pipeline fusion / parallel splits
- Runtime adaptation (dynamic parallelism, window triggers)
- Multi-segment lifecycle management (on-demand vs persistent vs continuous)
- Fault containment across bridges (error routing, dead-letter semantics)

This roadmap outlines how the testing stack must evolve in lockstep with Tier‑B/Tier‑C and the architecture runway.

---

## 2. Current Testing Posture (Snapshot)

| Domain | Coverage Strength | Notes |
|--------|-------------------|-------|
| JSON assembly (length, frame, streaming) | High | Structural & parity properties; partial negative fuzz. |
| Token streaming prototype | Low | Token reader unit-level; no integration/machine invariants yet. |
| Core Flow & StreamProcessor semantics | Medium-High | Monad/functor laws, stateful accumulators, probabilistic failure, timeout. |
| Integration (direct/chunk pipelines) | Medium | Model-based machine (Direct/Chunk only), accumulation & projection invariants. |
| Performance | Low | Only conceptual benchmarks; not yet part of CI gating. |
| Concurrency/scheduling | Very Low | No schedule control, no interleaving exploration. |
| Backpressure | Very Low | Implicit via `Consume`; no channel-level stress metrics. |
| Memory / allocation regression | None | No allocation baselines recorded. |
| Window/trigger/state backends | None | Not implemented yet. |
| Connector DSL / EIP patterns | None | Future feature set. |
| Error channel / dead-letter semantics | None | Planned for Camel-like adoption. |

---

## 3. Emerging Architecture Drivers

From the **Pipeline Runtime Architecture** and convergence plan:

1. **Execution Segments** (uniform backend slices) and **bridges** at backend boundaries.
2. **StageLifecycle** (OnDemand / Persistent / Continuous) with resource pooling & disposal semantics.
3. **Backend Abstraction** with channels & tasks (Async, Hopac, TPL, Sync).
4. **Parallel Executors** (partitioning, merge/combine).
5. **Backpressure Coordination** (queue depth, adaptive throttling).
6. **Window / Trigger** semantics + future dynamic parallelism inference.
7. **Systematic Concurrency Exploration** (Coyote-inspired schedule control).
8. **State Backends** (pluggable, possible level-tiered persistence).
9. **Connector Registry & EIP DSL** (split, aggregate, resequence, enrich).
10. **Reactive Interop** (Publisher/Subscriber bridging).
11. **Checkpointing & Recovery** (later phases; at-least-once → exactly-once).

Each introduces novel correctness and performance risks requiring new test layers.

---

## 4. Gap Analysis: Present vs Future Needs

| Need | Existing Capability | Gap |
|------|---------------------|-----|
| Deterministic sync execution (no async) | Partial (Flow may wrap tasks) | Must audit and offer pure sync engine harness. |
| Schedule capture/replay | None | Add schedule controller + trace serializer. |
| Cross-backend bridge correctness | None | Add segment contract tests + metrics invariants. |
| Backpressure propagation | Indirect (Consume) | Simulate bounded channels; measure stall/dropping behavior. |
| Parallel partition & combine correctness | None | Partition determinism and associative combine invariants. |
| Lifecycle (reuse vs on-demand) | None | Pool leak detection, reinitialization invariants. |
| Token streaming invariants (multi-root, nested) | Minimal | Depth invariants integrated into machine; event ordering checks. |
| Window/trigger semantics | None | Trigger firing criteria, watermark ordering, late element policy. |
| State backend migration & serialization | None | Snapshot round-trip, version skew, corruption injection. |
| Fusion optimization safety | None | Semantic parity pre/post fusion with trace introspection. |
| Reactive interop backpressure | None | Demand-driven tests with virtual downstream subscriber. |
| Performance regression detection | None | Establish baselines + threshold assertions. |
| Memory pressure / GC behavior | None | Allocation sampling & object lifetime assertions (bench/perf tests). |
| Error channel / dead-letter routing | None | Pattern-level graph augmentation invariants. |

---

## 5. Layered Test Strategy (End-State Vision)

| Layer | Purpose | Representative Tools |
|-------|---------|----------------------|
| Unit | Fast feedback on pure transforms, token readers, triggers, state serialization | Expecto + FsCheck (small sizes) |
| Property (Functional) | Structural/event invariants under random generation | FsCheck properties |
| Model-Based | Pipeline semantics & invariants across command sequences | FsCheck Experimental Machine |
| Execution Simulation | Deterministic scheduling + systematic interleavings | Custom schedule controller (Coyote-inspired) |
| Concurrency Stress | High parallelism, race detection, ordering & liveness | Schedule controller + randomized load |
| Backpressure & Flow Control | Validate queue depth boundaries, credit algorithms | Instrumented channels + property thresholds |
| Performance Benchmarks | Throughput & allocation baselines | BenchmarkDotNet (separate project) |
| Fault Injection / Resilience | Error propagation, bridge isolation, restart behavior | Scenario scripts + chaos hooks |
| State / Persistence | Snapshot/corruption/migration validity | Golden snapshot comparisons |
| Interop | Reactive Streams compliance subset | Adapter TCK harness |
| DSL / Pattern | EIP translation parity & invariants | DSL graph vs expanded graph comparison |
| Observability | Metrics/logging/tracing correctness, cardinality | Golden metric key sets, trace count assertions |

---

## 6. Evolution Plan by Layer

Phased enhancements mapped to architecture timeline:

| Phase | Core Focus | Test Additions |
|-------|------------|----------------|
| P1 (Now → Tier‑C) | Streaming tokens, finalize semantics, framing default | Token Machine extension; finalize error tests; assembler parity across modes |
| P2 | Sync vs Async engine split | Dual-engine property harness; ensure determinism under Sync |
| P3 | Schedule control (basic random + replay) | Schedule trace capture/replay tests; divergence detection |
| P4 | Parallel executor & partitioning | Associativity & commutativity invariants for Combine; partition isolation |
| P5 | Backpressure channels + bridges | Queue depth monotonic bounds; drop/blocked counters properties |
| P6 | Window/Trigger API | Trigger firing matrix tests (element/time), watermark late arrival |
| P7 | State backend abstraction | Serialization round-trip; snapshot consistency; failure injection |
| P8 | Reactive adapters | Demand accounting test harness; partial TCK style properties |
| P9 | DSL / EIP overlay | Graph expansion equivalence; provider registry integrity |
| P10 | Fusion optimization | Pre/post fusion semantic equivalence; instrumentation consistency |
| P11 | Dynamic parallelism inference | Stability (hysteresis) properties; scaling decision reproducibility |
| P12 | Checkpoint & recovery | At-least-once offset monotonic property; replay idempotency |
| P13 | Systematic exploration & fairness | Controlled interleaving exhaustion stats; fairness liveness assertions |

---

## 7. Synchronous Engine & Deterministic Execution Support

### Required Actions
1. Introduce explicit `ExecutionMode = Sync | Async` in Flow runner.
2. Add a pure synchronous scheduler: no internal `task {}` or async suspension.
3. Provide guard analyzers: reject `Flow.liftTask` inside Sync mode (fail-fast property).

### Tests
- Law re-run under Sync ensuring identical outcomes to Async for pure computations.
- Property: any Flow not using async constructs yields same SeqId ordering under both modes.
- Negative property: injecting `Flow.liftTask` in Sync engine surfaces a runtime diagnostic (predictable failure label).

---

## 8. Exploratory / Systematic Concurrency Engine

### Components
- `IScheduleController` (choose next ready op).
- Instrumented points: channel write/read, timer events, partition dispatch.
- Decision trace capture (`int list`) + replay.

### Tests
- Property: Replay(trace(run)) == run (idempotent outcome).
- Interleaving divergence detection: schedule randomness only in controller, no hidden nondeterminism in pure stages.
- Liveness: no deadlock if outstanding ready operations non-empty (bounded fairness step count).

### Metrics
- Interleavings explored per test execution window.
- Unique deadlock patterns discovered (should be zero in green runs).

---

## 9. Backpressure & Flow-Control Validation

### Mechanisms
- Bounded channels & credit tokens.
- Bridge throughput adaptation (throttle when downstream queue > threshold).

### Properties
- Queue depth upper bound never violated (except test harness intentionally disables limit).
- Producer blocking ratio within expected range (classification bucket).
- Drop policy semantics: For Dropping(N, policy), number of dropped items equals (input - emitted - queued).

### Instrumentation
Add channel metrics struct:
```
{ Writes; Reads; BlockedWrites; Drops; MaxDepthObserved }
```

Properties assert invariants against observed counters.

---

## 10. Performance & Allocation Testing

### Strategy
- Maintain BenchmarkDotNet baselines per commit window (nightly).
- Add “budget assertions” for microbench (e.g., token parse of 100KB JSON alloc < X bytes).

### Test Artifacts
- JSON size scaling benchmarks (1KB → 10MB).
- Variation sets: parse mode, chunk count, backend.

### Regression Detection
- Accept PR if delta < defined threshold (e.g., +10% throughput reduction flagged).

---

## 11. Token / Streaming (Tier‑C) Extensions

### Needed Invariants
1. Single rootTerminal token.
2. Depth progression consistent with reconstructed JToken.
3. PropertyName followed by exactly one value or structure start at depth+1 (root-level).
4. Accum streaming domain completeness: union(tokens seen) == domain at root closure (if using accumulation).

### Tests
- Model-based pipeline with `PStream` variant (like PChunk).
- Parity: Materialized JTokenWriter build vs parser-based reconstruction.

---

## 12. Orchestration Runtime Testing

### Segments & Bridges
- Segment fusion test: segmentation plan stable for identical stage descriptors.
- Bridge correctness: no payload duplication or loss across backend boundaries.

### Lifecycle
- OnDemand: instance count == envelopes processed.
- Persistent: instance count constant; state persistence across invocations.
- Continuous: start exactly once; termination triggers graceful shutdown.

### Fault Isolation
- Inject error in segment N: Upstream backpressure triggered; downstream receives no partial duplicates.

---

## 13. DSL / EIP Overlay & Connector Registry

### Registry Tests
- URI parsing property: round-trip (serialize(parse(uri))) == normalizedUri.
- Required param enforcement & default application.
- Negative variant: unknown scheme yields diagnostic.

### Pattern Translation
- `split |> aggregate` DSL expansion equivalence to manual graph construction.
- Resequencer: emits ordered sequence; property with randomly permuted inputs.

---

## 14. State Backend & Window/Trigger Testing

### State Round-Trip
- Serialize / deserialize state snapshot under random workload; invariants hold post-restore.

### Trigger Firing
Matrix tests over (elementTime progression, watermark sequence):
- Fire exactly when condition satisfied (size/time/session boundary).
- Purge semantics remove state (state size shrinks to zero after purge decision).

### Late Element Policy
- Count of late elements classified; if drop policy, none contribute to aggregates.

---

## 15. Failure Semantics & Error Channel

### Properties
- Any StageError tagged with route metadata ends in designated error channel sink.
- Backoff / retry policy: attemptCount <= configured max; if succeeded before max, no dead-letter emission.

### Chaos Injection
- Random cancellation, transient exceptions, forced timeouts— measure survival (no resource leakage).

---

## 16. Observability & Diagnostics

### Metrics
- Required metric keys emitted for channel & schedule instrumentation.
- Tag consistency after stage fusion (child ops reported inside fused stage parent).

### Tracing
- Span nesting depth consistent with stage graph depth.
- Replay run reproduces identical span sequence (IDs may differ, names same).

---

## 17. Risk Register & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Hidden async in sync engine | Non-determinism | Static analysis + runtime guard property |
| Schedule controller overhead | Test slowdown | Configurable sampling (only N runs per property) |
| Fusion obscuring debugging | Reduced trace fidelity | Emit sub-span events inside fused segment |
| Backpressure deadlocks (credit miscalc) | Stalls | Deadlock watchdog property (progress within M steps) |
| State backend snapshot drift | Data loss risk | Hash/integrity checksum property |
| Dynamic parallelism oscillation | Throughput jitter | Hysteresis property (min dwell time enforcement) |
| Memory regressions unnoticed | Latency spikes | Allocation baseline gating |

---

## 18. Tooling / Instrumentation Enhancements

1. `TestHarnessMetrics` aggregator (channel depths, schedule steps, token emission).
2. Decision trace format: JSON lines `{ step, opType, id }`.
3. Allocation sampler (optional) — use EventCounters or CLR MD fallback (benchmark only).
4. Snapshot Serializer for state backends (versioned header + CRC).

---

## 19. Recommended Execution Phasing (Test-Focused)

| Phase | Dev Feature | Test Deliverables |
|-------|-------------|-------------------|
| A | Finalize PStream | Token invariants suite; parity properties |
| B | Sync engine | Dual-mode Flow property pack |
| C | Schedule controller v1 | Trace capture/replay properties |
| D | Parallel executor | Partition isolation & combine associativity |
| E | Bounded channels | Backpressure & drop counters properties |
| F | Window/Trigger MVP | Trigger firing & late element properties |
| G | State backend API | Snapshot round-trip tests |
| H | Reactive adapters | Demand accounting properties |
| I | DSL/EIP overlay | Graph equivalence & pattern invariants |
| J | Fusion | Pre/post equivalence and metric parity |
| K | Dynamic parallelism | Scaling hysteresis property |
| L | Checkpoint (at-least-once) | Replay equivalence under crash simulation |
| M | Systematic exploration expansion | Fairness & liveness coverage metrics |

---

## 20. Key Metrics & Success Criteria

| Metric | Target |
|--------|--------|
| Property suite runtime increase per phase | ≤ +20% (optimize classification) |
| Concurrency bug discovery rate (new features) | ≥ 1 per major feature during dev |
| Performance regression acceptance threshold | ≤ 10% throughput / 15% alloc rise |
| Deterministic replay fidelity | 100% matching output & trace sequence |
| Backpressure queue overflow incidents (tests) | Zero in green runs |
| Trigger misfire rate in window tests | Zero |

---

## 21. Summary Checklist

- [ ] Add finalize semantics & strict corruption error test
- [ ] Implement PStream machine extension
- [ ] Build sync engine guard & dual-mode property pack
- [ ] Introduce schedule controller (trace + replay)
- [ ] Channel instrumentation & backpressure properties
- [ ] Token accumulation & projection streaming invariants
- [ ] State backend snapshot harness placeholder
- [ ] Performance regression baseline established
- [ ] Observability golden metric set defined
- [ ] Plan for fusion equivalence harness

---

## 22. Appendix: Sample Test Matrix

| Feature | Unit | Property | Model-Based | Concurrency | Perf | Fault Injection |
|---------|------|----------|-------------|-------------|------|-----------------|
| Length/Frame/Stream Parsers | ✓ | ✓ | (via pipelines) | — | (bench) | Corruption |
| Token Reader | ✓ | ✓ (ordering) | Planned | Planned | — | Malformed tokens |
| Accumulation (JToken) | ✓ | ✓ | ✓ | — | — | Batch threshold misuse |
| Streaming Accum (Tier‑C) | ✓ | ✓ | Planned | Planned | — | Late key arrival |
| Parallel Executor | ✓ | ✓ (assoc) | Planned | ✓ | ✓ | Worker failure |
| Bridges | — | ✓ (no loss/dup) | Planned | ✓ | ✓ | Bridge drop |
| Backpressure | — | ✓ | Planned | ✓ | ✓ | Saturation |
| Schedule Controller | — | — | ✓ | ✓ | — | Forced starvation |
| State Backend | ✓ | ✓ (idempotence) | ✓ | ✓ | ✓ | Snapshot corruption |
| Window/Trigger | ✓ | ✓ | Planned | ✓ | — | Late elements |
| Reactive Adapter | ✓ | ✓ (demand) | — | ✓ | ✓ | Downstream cancel |
| Fusion | — | ✓ (equivalence) | ✓ | ✓ | ✓ | Fault inside fused |
| Checkpointing | — | ✓ (offset monotonicity) | ✓ | ✓ | ✓ | Restart mid-flight |

---

## Closing

This evolution plan upgrades the current strong functional & structural test foundation into a comprehensive, multi-layered verification ecosystem capable of supporting the advanced orchestration, concurrency, and performance ambitions outlined in our architecture roadmap. Early investment in schedule control, channel/backpressure instrumentation, and dual engine (sync/async) discipline will yield the highest leverage and reduce compounded refactor cost later.

Please review and annotate with:
- Priority adjustments
- Additional invariants desired
- Target thresholds for performance gates

Once confirmed, we will generate the initial implementation backlog (issues) mapped to the phases above.

---