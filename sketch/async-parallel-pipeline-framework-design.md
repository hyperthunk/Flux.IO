# Async & Parallel Streaming Framework (F#)
### Design Sketch for a Reusable Non‑Blocking I/O & Parallelism Abstraction  
Date: 2025‑08‑15  
Status: Draft Architecture / Design Spec  
Authors: Architecture Team  
Target Runtime: .NET 8+, F# first; optional interop C# wrappers  

---

## 0. Executive Summary

We propose a modular, dependency‑inverted framework enabling construction of high‑throughput, low‑latency, *composable* streaming & batch pipelines with:

- Unified abstractions for non‑blocking I/O sources & sinks (pull, push, hybrid).
- Pluggable *parallelism engines*:  
  - Synchronous (deterministic / test mode)  
  - Task/Async (standard .NET `ValueTask` / IOCP)  
  - Hopac (fine‑grained parallelism + work stealing)  
  - (Optional) Dataflow block instantiation for coarse stage partitioning.
- Micro‑batch & record streaming modes with *adaptive batching*.
- Flow control: backpressure strategies (credit‑based, bounded buffer, rate shaping).
- Resilience: circuit breakers, retries, fallback, deadline & budget enforcement.
- Separation of *Pipeline DSL* (semantic topology) from runtime *Execution Engine* (strategy pattern).
- Convergent semantics for error propagation, cancellation, metrics, and resource accounting.
- Extensibility: custom schedulers, instrumentation hooks, alternate memory allocators.
- Reusability across: Neuro‑Semantic integration (Intake, Feature Fabric, Template Mining), open-source RML pipeline library, generic ETL workloads.

We explicitly **borrow conceptual patterns from `System.IO.Pipelines`** (producer/consumer decoupling, buffer segments, zero‑copy where possible) without forcing end‑users to adopt its concrete types unless opted in.

---

## 1. Design Goals & Non‑Goals

| Goal | Rationale |
|------|-----------|
| Decoupled layers (I/O, Processing, Scheduling) | Enables swapping concurrency strategy or integrating in constrained environments |
| Deterministic test mode | Simplifies property & differential testing; reproducibility |
| Pluggable backpressure | Different workloads need distinct throttling semantics |
| Bounded memory / pooling | High throughput + GC pressure mitigation |
| Unified error & cancellation semantics | Reduces accidental deadlocks / orphan tasks |
| Observability first-class | Telemetry correlation across engines |
| Minimal cross-layer compile-time coupling | Fosters independent versioning & partial adoption |
| Support Hopac AND vanilla async | Optimize hot code with Hopac, keep onboarding shallow |

Non‑Goals:
- Full reactive operator algebra (we interoperate with Rx if needed, but keep core minimal).
- Reinventing System.IO.Pipelines (we wrap/compose as needed).
- Opinionated serialization formats (left to adapters).

---

## 2. Layered Architecture

```
+------------------------------------------------------------+
| Application / Domain Pipelines (Parsing → Sampling → ...)  |
+---------------------+--------------------------------------+
|  Pipeline DSL Layer |  (Topology & Stage semantics)        |
+---------------------+--------------------------------------+
|  Execution Engine Abstraction (IExecutionEngine)           |
|   - Hopac Engine                                           |
|   - Async/Task Engine                                      |
|   - Dataflow Engine Adapter                                |
|   - Synchronous Engine                                     |
+---------------------+--------------------------------------+
|  Concurrency & Flow Control (Schedulers, Backpressure,     |
|  Circuit Breakers, Retry Policies, Budgeters)              |
+---------------------+--------------------------------------+
|  I/O Layer: Sources / Sinks / Transports                   |
|   - PullSource, PushSource, DuplexChannel                  |
|   - Pipe-backed adapters (System.IO.Pipelines)             |
|   - File / Network / Message Broker connectors             |
+---------------------+--------------------------------------+
| Memory & Buffer Mgmt (Segment Pool, MicroBatch Pool)       |
| Metrics & Tracing Interceptors                             |
| Config & Policy Providers                                  |
+------------------------------------------------------------+
```

Dependency Inversion:
- DSL depends only on **ports / interfaces** (no concrete engine).
- Engines implement interfaces, optionally using internal specialized libs (Hopac, Dataflow).
- Stages express *pure* transformations over `Envelope<'T>` (data + metadata + context).
- Telemetry and fault policies injected via decorators (interceptor pattern).

---

## 3. Core Abstractions

### 3.1 Data Units

```text
Envelope<'T>:
  payload : 'T
  headers : Map<string,string>
  seqId   : int64
  spanCtx : TraceContext
  ts      : Timestamp
  attrs   : ImmutableDictionary<string,obj> (lightweight)
```

`Batch<'T>`:
- Immutable logical grouping (micro‑batch).
- Provides structural & time window metadata.
- Carry aggregated size (bytes, item count) for heuristics.

### 3.2 Stage Contracts

```fsharp
type StageKind = Source | Transform | Branch | Merge | Sink

type IStage<'In,'Out> =
  abstract member Id : StageId
  abstract member Kind : StageKind
  abstract member Init : StageContext -> Task<unit>
  abstract member Process : CancellationToken -> StreamIn<'In> -> StreamOut<'Out>
  abstract member Close : unit -> Task<unit>
```

Notes:
- `StreamIn<'In>` & `StreamOut<'Out>` are *async iterables* (producer/consumer abstraction).  
  Implementation choices:
  - F#: `IAsyncEnumerable<'T>` + custom extensions
  - Hopac: `Ch<'T>` bridging adapter
  - Synchronous engine: simple `seq<'T>` enumeration

### 3.3 Pipeline Topology

```fsharp
type PipelineGraph =
  { Nodes : StageNode list
    Edges : Edge list
    Metadata : Map<string,string> }

type StageNode =
  { Stage : obj // IStage<_,_>
    Parallelism : ParallelismHint
    BufferPolicy : BufferPolicy option
    Backpressure : BackpressureStrategy option
    ErrorPolicy : ErrorPolicy }

type Edge =
  { From : StageId; To : StageId; Channel : ChannelDescriptor }
```

Dependency Inversion: `PipelineBuilder` constructs `PipelineGraph` independent of execution engine.

### 3.4 Execution Engine Interface

```fsharp
type ExecutionOptions =
  { Cancellation : CancellationToken
    MetricsSink  : IMetrics
    Tracing      : ITracer
    Logger       : ILogger
    MemoryPool   : IMemoryPool
    Policies     : PolicyRegistry }

type IExecutionEngine =
  abstract member Run : PipelineGraph * ExecutionOptions -> Task<PipelineResult>
  abstract member Inspect : StageId -> StageRuntimeState
  abstract member Stop : unit -> Task
```

Engines map stage semantics to their concurrency primitives (Hopac jobs, async tasks, Dataflow blocks, or direct invocation).

---

## 4. Pluggable Parallelism Engines

| Engine | Use Case | Characteristics | Implementation Notes |
|--------|----------|-----------------|----------------------|
| Synchronous | Deterministic, test, low-volume dev | Single-thread sequential | Direct recursion / enumeration |
| Async/Task | General non-Hot path server workloads | Uses `ValueTask` / IOCP | Minimal overhead, integrates with ASP.NET hosting |
| Hopac | High fan-out micro tasks (e.g., tokenization, scoring) | Work stealing, low scheduling overhead | Wrap Hopac `Job` as engine; bridging wrappers for StreamIn/Out |
| Dataflow | Coarse-grained stage-level parallelization | Bounded capacity blocks, built-in backpressure | Auto-sizing blocks; optional in .NET 8 kept minimal |
| Hybrid (Composite) | Mixed micro tasks + heavy transforms | Stage-level engine selection | Each stage annotated with `EngineHint` |

### Engine Selection Heuristic
- Stage `EstimateCPUIntensity` + `Statefulness` + `ExpectedFanOut`.
- Choose Hopac for *stateless, CPU‑bound micro ops*; Async for I/O heavy; Dataflow for coarse merges/branches.

---

## 5. Backpressure & Flow Control

### Strategies

| Strategy | Mechanism | Parameters | Use |
|----------|----------|------------|-----|
| Bounded Buffer | Fixed queue size; producer awaits | `capacity`, `dropPolicy` | General default |
| Credit-Based | Downstream issues credits upstream | `initialCredits`, `replenishThreshold` | Precise flow control for strict memory caps |
| Rate Shaping | Token bucket; delay emission | `rate`, `burst` | External SLA adherence |
| Adaptive Batch Size | Increase/decrease batch based on processing latency & queue depth | `minBatch`, `maxBatch`, sensitivity | Optimize throughput |
| Reactive Pull | Upstream only produces on explicit pull request | window size | Source bridging remote streams |

### Circuit Breaking (Per Stage)
- Policy interfaces:
  - `IThroughputMonitor`: tracks success/failure rates & latency histograms.
  - `ICircuitBreaker`: open/half-open/closed with exponential backoff.

Activation conditions:
```
if (errorRate > E_threshold AND p95Latency > L_threshold) -> OPEN
```

Recovery:
- Half-open trial batch size (e.g., batch of 10) to test recovery.
- Fallback: route to alternative stage (if defined) or drop with metric (configurable).

### Implementation Hooks
- Injected via decorators wrapping `Process` invocation.
- Unified outcome reporting to metrics sink.

---

## 6. Memory & Buffer Management

### Principles
- Minimize per-item allocation; prefer pooled segment arrays.
- Use `ArrayPool<byte>` / custom slab allocator for large transient buffers (e.g., parsing).
- `MicroBatchPool`: fixed-size object pool for `Batch<'T>` wrappers (reinitialize).
- Stage-level *pressure signals* drive dynamic batch sizing:
  - If downstream queue occupancy > 80% => shrink batch.
  - If < 30% & cpu < threshold => expand batch.

### Zero-Copy Policy
- Pipeline Option: `ZeroCopyEnabled`.
- For fused contiguous stages (no branching), allow pass-through references to underlying memory spans.
- When branching or late materialization needed (multiple consumers), copy/clone strategically (copy-on-write semantics).

---

## 7. Error Handling & Semantics

| Error Type | Propagation | Policy Options |
|------------|-------------|----------------|
| Recoverable (transient I/O) | Retry with backoff (bounded) | maxRetries, jitter, DLQ fallback |
| Non-recoverable (schema violation) | Dead-letter emission; metrics | Log + sample retention for forensic |
| Catastrophic (OutOfMemory, Invariant Violated) | Immediate pipeline stop | Generate crash report / telemetry dump |

**Error Envelope:** Replace failed item with `ErrorRecord` maintaining correlation IDs.

**Termination Semantics:**
- *Graceful*: All in-flight batches drained.
- *Abort*: Immediate cancellation (circuit open or manual stop).
- *Quarantine*: Stage isolated; downstream receives synthetic “failure sentinel”.

---

## 8. Metrics, Tracing, Telemetry

### Metrics (Per Stage)
- throughput (items/s), inputQueueDepth, outputQueueDepth
- latency: p50/p90/p99 per batch & per item
- errorRate, retryCount, circuitState
- batchSize dynamics, memory usage (pool rent/return statistics)
- backpressure signals frequency

### Tracing
- Each `Envelope` carries `spanCtx`; root span created at ingestion or test harness.
- Baggage: `pipelineId`, `domainId`, `engineType`, `parallelismHint`.
- Exporters: OpenTelemetry (OTLP), console for dev.

### Observability Hooks
- Provided via `IPipelineObserver`:
```fsharp
type IPipelineObserver =
  abstract member OnStageStart : StageId * StageRuntimeInfo -> unit
  abstract member OnBatchProcessed : StageId * BatchMetrics -> unit
  abstract member OnError : StageId * exn * ErrorContext -> unit
  abstract member OnCircuitChange : StageId * CircuitState -> unit
```

---

## 9. Configuration & Policy Injection

Configuration objects are *immutable snapshots* (hot reload by version bump) delivered via:

- `IPolicyProvider`: resolves `ErrorPolicy`, `BackpressureStrategy`, `CircuitBreakerPolicy`.
- `IResourceBudgetProvider`: per pipeline budgets (max concurrency tokens, memory ceiling).
- Supports dynamic runtime override token (feature flag) for experiment toggling.

### Example Policy Descriptor (Sketch)

```fsharp
type BackpressureDescriptor =
  | Bounded of capacity:int * drop:DropPolicy
  | CreditBased of initial:int * replenish:int
  | Adaptive of minBatch:int * maxBatch:int
```

---

## 10. Pipeline Construction DSL (F# Fluent + Computation Expression)

A minimal DSL enabling declarative composition:

```fsharp
pipeline {
  source  (Files.readAll "*.json") |> parallelism 4
  |> transform parseJson |> bp (Adaptive (50, 500))
  |> transform sampleFields
  |> transform clusterTokens |> parallelism (DynamicCpuBound)
  |> transform computeMetrics |> engine Hopac
  |> sink metricsStore
}
```

DSL evaluates to `PipelineGraph`; no runtime side effects until `engine.Run`.

Determinism: ordering determined by DSL evaluation order; recorded into hash.

---

## 11. Interoperability with System.IO.Pipelines

### Integration Patterns
- Wrap `PipeReader` as `IAsyncEnumerable<ReadOnlyMemory<byte>>`.
- Provide `PipeWriter` sink adapter that enforces flush thresholds & batch boundaries.
- Optional *fusion stage* that manipulates `SequencePosition` for incremental parse (e.g., streaming JSON / CSV scanner).

### Rationale
- Leverage existing high-performance buffer segmentation.
- Avoid re-implementation of ring buffer behavior.
- Allow advanced users to plug in custom protocol parsers.

---

## 12. Micro-Batching & Streaming Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| Record (Single) | Item per `Envelope` | Low-latency, small object events |
| Fixed Micro-Batch | N items per batch | Throughput optimum known workload |
| Adaptive | Dynamic N based on latency & resource usage | Mixed variability workloads |
| Time-Windowed | Batch by time slice (e.g., 200ms) | External ordering fairness |
| Hybrid (Count or Time) | Flush on size or interval whichever first | Balanced throughput/latency |

Implementation:
- `IBatchStrategy` with callbacks: `OnItem`, `ShouldFlush`, `Flush`.
- Uses monotonic clock + counters; integrates with backpressure signals.

---

## 13. Backpressure & Circuit Breaking Implementation Hooks

Example internal flow:

```
Producer Stage
  -> attempts enqueue to OutBoundBuffer
     if buffer full:
         BackpressureStrategy.apply()
             - await space (bounded)
             - drop oldest (drop-tail)
             - signal upstream slowdown (credit-based)
             - block & accumulate metrics
  -> On repeated timeout or error pattern => notify CircuitBreaker
Consumer Stage
  -> On processing failure increments error counters
CircuitBreaker
  -> Observes sliding window metrics
  -> Emits state transition events
```

Sliding window metrics stored in ring buffer (constant memory). 

---

## 14. Testing & Verification Strategy

| Layer | Technique | Examples |
|-------|-----------|----------|
| DSL Graph Generation | Property-based (FsCheck) | Graph continuity, no orphan nodes |
| Execution Engines | Stress tests (mass concurrency), deterministic seed runs | Hopac vs Async result equivalence |
| Backpressure Strategies | Simulation harness | Assert throughput under capacity constraints |
| Circuit Breaker | Scenario modeling | Error bursts trigger open, recover after cooldown |
| Memory Pool | Leak detection (allocation profiling), invariants | Rented = Returned |
| End-to-End | Golden trace replays; race condition detection with `CHESS` / custom harness | Ingestion to sink trace equivalence |
| Formal Invariants | TLA+ spec (circuit states, credit-based flow) | Liveness: no deadlock when credits > 0 |

### Deterministic Mode
- Synchronous engine validates stage purity & reproducibility (hash comparison).
- Used for CI: *stateless stage outcome must equal baseline snapshots*.

---

## 15. Security & Robustness

| Concern | Mitigation |
|---------|-----------|
| Malicious Plugin Stage | Capability gating + time & memory quotas + sandbox loader |
| Data Exfiltration (Telemetry) | Redaction policies; configuration for PII stripping |
| Resource Exhaustion | Concurrency budgets per pipeline; adaptive throttling |
| Deadlock / Livelock | Formal model for credit-based; watchdog thread injection |
| Partial Failure Isolation | Stage isolation mode (quarantine) enabling soft restart |

---

## 16. Example High-Level Algorithms (Pseudo / Semantics)

### Adaptive Batch Size Controller
```
Input: targetLatency, maxBatch, minBatch
Loop:
  observe currentLatency, queueDepth
  if currentLatency < targetLatency * 0.7 AND queueDepth > 50%:
       batchSize = min(batchSize * 1.5, maxBatch)
  else if currentLatency > targetLatency OR queueDepth < 20%:
       batchSize = max(batchSize / 2, minBatch)
  smooth via EMA for stability
```

### Credit-Based Flow (Pull Model)
```
Downstream initializes credits = C
Upstream before sending batch:
  if credits >= batchCost:
       send batch
       credits -= batchCost
  else await credit refill event
Downstream after processing batch: issue credit += batchCost
```

### Circuit Breaker Sliding Window
```
Collect metrics in fixed time buckets (N buckets = window)
failRate = failures / (successes + failures)
if failRate > threshold OR p95Latency > threshold:
    open breaker
on half-open trial: allow T test batches
if success ratio >= successThreshold => close
```

---

## 17. Integration Examples

### Dataflow Block Instantiation (Coarse)
- Each `StageNode` -> TransformBlock / BufferBlock with target options:
  - `MaxDegreeOfParallelism = ParallelismHint.Resolve()`
  - `BoundedCapacity = BufferPolicy.capacity`
- Link blocks with `linkOptions` preserving ordering if required.

### Hopac Fine-Grained Stage
- Stage `Process` returns a channel (`Ch<Envelope<'T>>`) producing items via `Job.foreverServer`.
- Merge stage: `Alt.choose` over multiple input channels.

### Synchronous Test Mode
- Evaluate pipeline graph depth-first.
- Replace asynchronous enumerations with `seq` folds.
- Disable backpressure logic (no concurrency), but simulate counters for metrics parity.

---

## 18. Extensibility Points

| Extension | Interface | Purpose |
|-----------|-----------|---------|
| Custom Scheduler | `IScheduler` | Inject different scheduling (e.g., priority tiers) |
| Memory Pool | `IMemoryPool` | Domain-specific allocation pooling |
| Stage Decorators | `IStageDecorator` | Add cross-cutting (logging, tracing, metrics, auth) |
| Retry Policy | `IRetryPolicy` | Alternative algorithms (e.g., hedged requests) |
| Serialization Adapter | `ISerializer<'T>` | Custom wire formats without core coupling |
| Validation Filter | `IEnvelopeValidator<'T>` | Pre-stage input validation |

---

## 19. Deployment & Packaging

| Mode | Packaging | Notes |
|------|-----------|-------|
| Library (NuGet) | `Company.Pipelines.Core` (DSL + interfaces), `Company.Pipelines.Engines.Hopac`, `Company.Pipelines.Backpressure` | Consumers opt-in granularly |
| CLI (Optional) | Pipeline plan visualization, latency simulation | Developer tool |
| Self-Host | Same packages; optional trimmed runtime build (AOT) | Provide guidelines for GC tuning |
| SaaS Integration | Service hosts instantiate pipelines dynamically from stored graph specs | Hot reload via versioned spec |

---

## 20. Observability Data Model (Excerpt)

```
metric: pipeline_stage_throughput{pipelineId,stageId} -> items/sec
metric: pipeline_stage_latency_bucket{le} -> histogram
metric: pipeline_backpressure_events{strategy,stageId} -> count
metric: pipeline_circuit_state{state} -> gauge
trace span: stage.process (attributes: engine, batchSize, queueDepthIn, queueDepthOut)
log structured: {"event":"error","stage":"parseJson","seqId":1234,"errorType":"SchemaMismatch"}
```

---

## 21. Migration & Adoption Plan

| Phase | Deliverables | Success Criteria |
|-------|--------------|------------------|
| P0 (MVP) | Core DSL, Async engine, bounded buffer backpressure, metrics | Replace ad-hoc parsing pipeline; equal throughput |
| P1 | Hopac engine, adaptive batching, circuit breaker, credit-based flow | Latency p95 ↓ 25%, CPU utilization ↑ |
| P2 | Dataflow adapter, hybrid engine selection, memory pool | Memory footprint stabilized < baseline | 
| P3 | Formal invariants, test harness expansion, CLI simulator | Zero deadlock issues in stress tests |
| P4 | Marketplace / plugin integration & open-core extraction | External adoption & contribution |

---

## 22. Risk & Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Over-complexity discourages adoption | Fragmented usage | Provide sane defaults & cookbook examples |
| Hopac dependency friction | Learning curve | Clear docs; keep optional; fallback engine parity tests |
| Memory leaks via pooling misuse | Latency spikes | Pool diagnostic counters + leak detection harness |
| Misconfigured backpressure causing throughput collapse | SLA breach | Default safe Bounded + adaptive tuning; static analysis of spec |
| Circuit breaker thrash | Oscillation | Hysteresis & minimum open time; backoff jitter |
| Non-deterministic test flakiness | CI noise | Deterministic synchronous engine baseline golden tests |

---

## 23. Example Minimal F# Sketch (Illustrative Only)

```fsharp
// Stage signature sample
type JsonParseStage() =
  interface IStage<ReadOnlyMemory<byte>, JsonDocument> with
    member _.Id = StageId "parse-json"
    member _.Kind = Transform
    member _.Init _ = Task.CompletedTask
    member _.Process ct (input: StreamIn<_>) : StreamOut<_> =
      StreamOut.asyncSeq (async {
        for! chunk in input.ReadAll() do
          let doc = JsonDocument.Parse(chunk.Span) // assume pooled buffer
          yield Envelope.withPayload doc chunk
      })
    member _.Close () = Task.CompletedTask
```
(Real impl would use pooled parsers, error envelopes, metrics decorators.)

---

## 24. Open-Source Extraction (Future Consideration)

Open-Core Modules:
- DSL + Async engine + bounded buffer + metrics.
Proprietary:
- Advanced adaptive batching algorithms, hybrid engine auto-selection, specialized calibration integration, domain-intelligent backpressure heuristics.

---

## 25. Summary

This framework provides a *unified, decoupled* foundation for high-performance, testable, and resilient streaming/parallel processing in F#. By abstracting pipeline topology from execution strategy and encapsulating backpressure, memory, and fault policies, it empowers both internal (Neuro-Semantic platform) and external (open-source RML) workloads to achieve consistent semantics with tailored performance profiles. Its design leans on established patterns (IOCP, pipelines), embraces formal & property-based verification for critical invariants, and offers progressive adoption paths.

---

*End of Design Document*