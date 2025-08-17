# Flux.IO Subsystem Design (Initial Increment)
Date: 2025-08-17  
Status: Inception / Implementation Blueprint (Iteration P0–P2 Scope)  
Author: Principal Engineering  
Target Runtimes: .NET 8 (baseline), .NET 9 (optimization path)  
Languages: F# (primary), future C# interop layer  
Out-of-Scope (This Iteration): URI connector registry, advanced state backends, exactly-once semantics, dynamic engine migration, full EIP overlay, external checkpointing, RocksDB integration.

---

## 1. Purpose

Provide a type-safe, zero-copy-first I/O and streaming subsystem that:
- Implements the in-scope portions of the unified architecture.
- Establishes *foundational abstractions* (Envelope, Stage, Accumulator, Backpressure).
- Enables deterministic and asynchronous execution modes.
- Allows staged adoption of future enrichment (Flink/Camel/Coyote-inspired) without rework.

This design **eliminates unsafe `obj` usage** in public/core APIs by leveraging *type-indexed pipeline composition*.

---

## 2. Guiding Principles

| Principle | Application |
|-----------|-------------|
| Type Safety End-to-End | No unbounded `obj`; pipelines are statically typed through a recursive DU shape. |
| Zero-Copy by Default | Use `Memory<byte>`/`ReadOnlyMemory<byte>` and buffer pooling; explicit opt-in copying (branching, multi-consumer). |
| Layered Abstractions | Low-level non-blocking primitives → typed stages → accumulation → high-level DSL. |
| Async Transparency | Nodes written as pure/algebraic *Flow* computations; runtime manages `ValueTask` & scheduling. |
| Extensibility (Micro-Kernel) | Small kernel of core traits (`Stage`, `Accumulator`, backpressure policies); plugins register additional codecs/connectors later. |
| Deterministic Testing | Synchronous engine & schedule hooks to enable controlled interleavings in future (Coyote-inspired). |
| Planning / Pre-Compilation | Pipeline builder performs static fusion & validation (linear first; branching later). |
| Separation of Concerns | Execution engine independent of stage semantics and memory pools. |

---

## 3. Core Type Model

### 3.1 Envelope & Cost Model
- `Envelope<'T>` holds immutable payload + metadata + cost (`bytes`, `cpuHint`).
- Maintains trace context + sequencing; supports observability without mutability.

### 3.2 Streams
- Logical stream: `StreamIn<'T> = IAsyncEnumerable<Envelope<'T>>`
- All *processing* emits another typed stream (`StreamOut<'U>`).
- Composition uses *pipeline recursion* rather than heterogenous lists with erased types.

### 3.3 Stages
```fsharp
type Stage<'In,'Out> =
  abstract member Id     : StageId
  abstract member Kind   : StageKind
  abstract member Init   : StageContext -> ValueTask
  abstract member Process: ct:CancellationToken -> StreamIn<'In> -> StreamOut<'Out>
  abstract member Close  : unit -> ValueTask
```

### 3.4 Typed Pipeline Shape (No `obj`)
Recursive discriminated union:

```
Pipeline<'In,'Out> =
  | terminal of Stage<'In,'Out>
  | chain of Stage<'In,'Mid> * Pipeline<'Mid,'Out>
```

Advantages:
- Type correctness enforced at build time.
- Eliminates unsafe casts.
- Supports static fusion of adjacent pure transforms.

### 3.5 Flow Monad (Effect Context)
- Abstracts async + environment access while hiding `ValueTask`.
- `Flow<'a>` implemented as:
  - Representation: `ExecutionEnv -> CancellationToken -> ValueTask<'a>`
  - Provides combinators: `map`, `bind`, `retry`, `timeout`.

### 3.6 Accumulation
Accumulators:
- Declarative policies (count/size/TTL/watermark/predicate).
- Stateful keyed collection with *completeness score* (0.0–1.0).
- Emission via completeness or forced partial flush.

### 3.7 Backpressure
Strategies embedded at pipeline edges:
- Bounded
- Credit-based
- Adaptive batching
- Rate shaping
- Reactive pull (source gating)
Effective queue size formula includes accumulator memory pressure.

---

## 4. Zero-Copy Strategy

| Layer | Mechanism | Fallback |
|-------|-----------|----------|
| Ingestion | Rent slabs → slice as `ReadOnlyMemory<byte>` | Copy only at branching or multi-retain |
| Transform | Pass reference if single downstream | Copy-on-write for fan-out |
| Accumulation | Store references; copy if state persistence required (spill) | Compressed spill buffer |
| Sink | Write direct spans into `PipeWriter` or memory-mapped region | Buffered accumulation with flush thresholds |

Spill tiers (current iteration):
1. In-memory compressed (LZ4 placeholder interface).
2. Memory-mapped file region (future).
3. External/durable (future extension).

---

## 5. Execution Model

### 5.1 Engines (Current Iteration)
| Engine | Included | Purpose |
|--------|----------|---------|
| SyncEngine | Yes | Deterministic test & debugging |
| AsyncEngine | Yes | Baseline non-blocking production |
| Hopac/Dataflow | Stub interfaces (future) | Prepared extension points |

### 5.2 Composition
`compilePipeline: Pipeline<'A,'B> -> CompiledPipeline<'A,'B>`
- Performs:
  - Purity inference (annotation or detection hook).
  - Fusion of contiguous pure transforms into a *FusedStage*.
  - Pre-build of scheduling graph (linear variant).
  - Attachment of backpressure channels (bounded channel mapping).

### 5.3 Scheduling Hooks
- Internal hooks capture “decision points” (send, receive, timer).
- Exposed only in *test mode* for later systematic exploration.

---

## 6. Accumulation Model (In-Scope Subset)

| Policy Aspect | Supported |
|---------------|-----------|
| maxItems/maxBytes | Yes |
| maxLatency | Yes |
| completenessTTL + partial flush | Yes |
| variable-set bitmask | Yes |
| watermark/event time | Placeholder struct + comparator |
| spillAllowed | Strategy injection (no durable backend yet) |

Completeness flow:
1. Receive fragment
2. Merge state
3. Evaluate thresholds & variable mask
4. Decide: `Incomplete | Complete | Expired`
5. Emit Envelope annotated with `acc.completeness` header.

---

## 7. Backpressure Integration Points

| Layer | Signal |
|-------|--------|
| Source gating | Credits or bounded queue occupancy |
| Accumulator | Pending bytes + keys contribute to upstream effective queue |
| Adaptive batching | Latency + queue depth + completion rarity |
| Circuit placeholder | Integrated but simplified (open/close events metric only) |

---

## 8. Planning / Pre-Compilation

Pipeline planner responsibilities:
1. Validate stage ID uniqueness.
2. Validate type continuity.
3. Extract fusible segments.
4. Produce *ExecutionPlan* with:
   - Stage execution order
   - Backpressure channel descriptors
   - Accumulator stage indexes
5. Emit *PlanDiagnostics* (fusion count, estimated memory, theoretical max latency window).

---

## 9. API DSL (Initial)

Fluent + computation expression hybrid (F#):

```fsharp
pipeline {
  source fileSource
  |> map parseJsonTokens
  |> accumulate customerTemplateAcc
  |> map projectDomainModel
  |> sink neo4jSink
}
```

Each operator is a *lifted stage constructor* returning a new `Pipeline<'In,'Out>` via recursive `chain`.

---

## 10. Error Semantics (Minimal Iteration)

| Category | Handling |
|----------|----------|
| Transient | Policy-based retry decorator |
| Permanent | Wrap into `ErrorEnvelope<'T>` (preserves correlation) |
| Catastrophic | Abort execution, snapshot metrics |
| Accumulator deadlock | TTL triggers partial flush + metric increment |

---

## 11. Observability

| Primitive | Detail |
|-----------|--------|
| Metrics | Stage throughput, queue depths, latency histograms, accumulator counts |
| Tracing | Span per stage, events for spill/fusion/flush |
| Logging | Structured log builder with correlation injection |
| Hooks | `Observer` interface (stage start, batch processed, accumulator event, error) |

---

## 12. Extensibility Points

| Interface | Purpose |
|-----------|---------|
| `BufferPool` | Specialized memory management |
| `Accumulator` | Custom completeness semantics |
| `BackpressurePolicy` | Alternative throttling algorithms |
| `StageDecorator` | Cross-cutting instrumentation |
| `PlannerPass` | Additional plan transformations |

---

## 13. Deterministic Mode

- All async enumerations drained using synchronous loops.
- No tasks spawned; timeouts replaced with immediate evaluation.
- Simulated backpressure counters for parity.
- Future: schedule controller drives alternative interleavings.

---

## 14. Out-of-Scope Preparation

| Future Feature | Current Hook |
|----------------|--------------|
| URI connector registry | `ConnectorDescriptor` placeholder type |
| Durable checkpointing | `CheckpointToken` + `IRecoverableSource` prototype interfaces |
| Exactly-once sinks | `ISinkTransactional` stub |
| Dynamic parallelism inference | Planner metric hook capturing per-stage cost signature |
| Engine migration | ExecutionPlan stage metadata includes `engineHint` |

---

## 15. Security & Safety (Subset)

| Concern | Mitigation |
|---------|------------|
| Memory leak in pooling | Statistics + invariant assertion at engine stop |
| Stage sandboxing | (Future) Capability flags on stage context |
| Spill confidentiality | Hook for encryption transform (no-op default) |
| Unbounded key growth | Accumulator policy soft limit + eviction strategy placeholder |

---

## 16. Initial Deliverables (This PR / Commit Set)

| Artifact | File |
|----------|------|
| Core Types | `Core.fs` |
| Flow Monad & Builders | `Flow.fs` |
| Pipeline & DSL | `Pipeline.fs` |
| Accumulation | `Accumulation.fs` |
| Backpressure & Channels | `Backpressure.fs` |
| Execution Engine (Sync/Async) | `ExecutionEngine.fs` |
| Memory Pool | `Memory.fs` |
| Observability | `Observability.fs` |
| Planning | `Planning.fs` |
| Test Plan | `TEST-PLAN.md` |

---

## 17. Risks & Mitigation (Iteration Focus)

| Risk | Impact | Action |
|------|--------|--------|
| Over-engineered fusion prematurely | Delays delivery | Implement simple adjacency fusion only |
| Accumulator complexity creeping | Miss schedule | Limit policies to defined subset |
| Hidden allocations in Flow monad | Latency regressions | Benchmark & inline crucial bind/map |
| Backpressure mismatch across engines | Inconsistent perf | Normalized channel adapter abstraction |

---

## 18. Summary

This initial implementation establishes a *purely typed* foundation for streaming and accumulation without runtime type erasure. It prioritizes correctness, zero-copy flow, and extensibility while deferring high-complexity features. The architecture is intentionally *open for enrichment* (Flink triggers, Camel EIPs, Coyote scheduling) with pre-placed extension seams.

---

*End of Design Document*