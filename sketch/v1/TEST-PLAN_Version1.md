# Flux.IO Test Plan (Iteration P0–P2)

Date: 2025-08-17  
Scope: Core implementation delivered in initial iteration (Core.fs, Memory, Backpressure, Accumulation, Pipeline, Execution Engines, Planning).

---

## 1. Principles

| Goal | Application in Tests |
|------|----------------------|
| Determinism | Sync engine baselines expected output order & counts |
| Property Orientation | FsCheck for streaming fusion correctness & accumulator liveness |
| Isolation | Each module tested with fakes/mocks for dependencies (Logger, Metrics, Tracer) |
| Zero-Copy Integrity | Ensure no unintended buffer copies (length identity & reference equality where applicable) |
| Regression Seeds | Concurrency seeds captured (future scheduling harness) |

---

## 2. Unit Tests

### 2.1 Envelope
- Mapping preserves headers & ids.
- `withHeader` immutability (original unchanged).
- Cost propagation.

### 2.2 Flow Monad
- `bind` associativity (property).
- Early completion path vs awaited path equivalence.
- Retry wrapper (future extension) placeholder test verifying structure compile.

### 2.3 Memory / BufferPool
- Rent/Release invariants: Outstanding returns to zero.
- Peak tracking monotonic.
- Fragmentation placeholder zero.

### 2.4 BackpressureChannels
- Bounded write capacity respected.
- Drop policies:
  - DropNewest: Writes beyond capacity fail; dropped counter increments.
  - DropOldest: Validate earlier element replaced (simulate with counting).
  - Block: Provide producer/consumer orchestrated test verifying blocking (time measurement threshold).
- Stats consistency: Enqueued - Dequeued - Dropped approximates current length.

### 2.5 Accumulation (VariableSetAccumulator)
- Property: When all required variable indices delivered, completeness score = 1.0.
- TTL partial flush:
  - Simulate time progression (inject fake clock or manipulate FirstTs/LastTs).
- Score monotonic non-decreasing.
- Partial flush emits `Expired(Some x, _)` only when `PartialFlush=true` and TTL exceeded.
- Large variable sets handle bitmask boundary (indices up to 63).

### 2.6 Pipeline Fusion
- Two adjacent pure `map` stages → fused output equals baseline composition.
- Mixed non-pure (marking: use a fake stage with internal counter mutation) not fused.
- Id concatenation rule correctness.

### 2.7 Execution Engines
- Sync vs Async identical output payload sequence (single-thread linear pipeline).
- Cancellation: Provide token cancelled mid-stream; assert early termination (partial count).
- Resource Closure: `Close` invoked on all stages after completion (track with flags).

### 2.8 Error Policy Placeholder
- Stage throwing exception: Ensure propagation and metric increment (mock metrics).

---

## 3. Integration Tests

### 3.1 Linear Pipeline E2E
- Source generating N envelopes → map → sinkIgnore.
- Validate throughput metrics recorded (mock metrics increments > 0).

### 3.2 Accumulator in Pipeline
- Source emits variable fragments out-of-order.
- Pipeline: source → accumulation stage wrapper (adaptor, to be implemented) → map (projection) → sink.
- Validate number of completions vs partial flush.

### 3.3 Backpressure + Accumulator
- Slow consumer simulation: insert artificial delay in sink.
- Measure effective queue growth; ensure bounded channel does not exceed capacity.

### 3.4 Zero-Copy Pass-through
- Source uses pooled buffer referencing same memory into transform stage.
- After pipeline finishes, ensure release executed and no copies created (instrument clones counter).

---

## 4. Property-Based Tests (FsCheck)

| Property | Description |
|----------|-------------|
| Fusion Idempotence | `fuse (fuse p) == fuse p` |
| Map Composition | Sequence of map stages combined equals single map with composed function |
| Accumulator Liveness | Given eventual completeness set fragments always yields completion (no infinite Incomplete) |
| Backpressure Capacity Safety | For any write sequence length `k`, Length ≤ capacity |

---

## 5. Performance Micro-Benchmarks (BenchmarkDotNet)

| Scenario | Metric |
|----------|-------|
| Baseline map pipeline (N=1M) | Throughput ops/sec, allocation bytes/op |
| Accumulator with 10 variable fields | Average microseconds per fragment |
| Bounded channel under contention | Mean enqueue latency |
| Fusion vs non-fusion (10 maps) | Latency reduction % |

---

## 6. Stress / Soak Tests

- Long-running (30 min) pipelined ingestion with random payload sizes, validating:
  - No memory leak (`Outstanding` near 0 after drain).
  - Stable latency (no increasing trend > threshold).

---

## 7. Deterministic Scheduling (Future Hook)

Placeholder tests verifying presence of scheduling hook type and no-op behavior now; ensures non-breaking addition of interleaving explorer.

---

## 8. Observability Validation

- Mock tracer captures span start/end counts = stages executed.
- Observer events: StageStart count = stage count; StageComplete count = stage count.
- Accumulator event recorded on completion & partial flush.

---

## 9. Failure Mode Tests

| Failure | Expected Outcome |
|---------|------------------|
| Stage throws | Pipeline abort; error recorded once; resources closed. |
| Accumulator serialization not implemented but spill flagged | Fallback path (skip spill) + warning log. |
| Bounded channel blocking with cancellation | Task cancelled promptly (< timeout). |

---

## 10. Test Data Generation

- Arbitrary JSON fragments builder (variable index distribution uniform).
- Random map chain length (1–15) for fusion property tests.
- Backpressure load generator (producer vs consumer speed ratio varied).

---

## 11. Tooling & Frameworks

| Need | Tool |
|------|------|
| Property tests | FsCheck |
| Benchmarks | BenchmarkDotNet |
| Async enumeration helper | custom taskSeq or FSharp.Control.AsyncSeq (evaluate cost) |
| Metrics mocks | Lightweight in-memory collector |

---

## 12. Exit Criteria for Iteration

- 95% statement coverage on Core, Accumulation, Backpressure modules (excluding stubs).
- All property tests pass with 500–1000 successful cases each.
- No allocation regression >10% between fused and baseline optimized runs.
- Memory pool outstanding buffers zero after each test suite.

---

*End of Test Plan*