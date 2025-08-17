# Async & Parallel Streaming Framework (F# / .NET 9)
### Revised Design (with Accumulation / Threshold Semantics & C# Wrapper API)
Date: 2025‑08‑15  
Status: Updated Architecture / Design Spec (v2)  
Target Runtime: .NET 9 (LTS pipeline assumed)  
Primary Language: F# (idiomatic)  
Interop Layer: C# wrapper with `I*` interfaces mirroring F# counterparts  

---

## 0. Change Log (from v1)
| Area | Change |
|------|--------|
| Runtime | Updated to .NET 9 (JIT / Native AOT improvements, enhanced `System.IO.Pipelines`, new rate-limiting & activity sampling refinements). |
| Naming | F# interfaces now *drop* leading `I` (e.g. `Stage`, `ExecutionEngine`); C# wrapper exposes traditional `IStage`, `IExecutionEngine`. |
| Accumulation Paradigm | Added formal **Accumulator / Completeness** model for keyed, predicate, watermark, and semantic (dependency) thresholds (e.g., RML template expansion). |
| Engines | Clarified hybrid engine selection heuristics using .NET 9 performance counters & adaptive thread pool. |
| Memory | Expanded accumulation-aware memory pool and spill strategy (size/time/predicate flush). |
| Backpressure | Added accumulator-aware backpressure (upstream gating based on *latent heap cost*). |
| Testing & Verification | Added accumulator-specific property tests & liveness invariants. |
| Formal Spec Targets | Accumulator completeness lattice; credit+accumulator interaction invariants. |

---

## 1. Design Goals (Recap + Extensions)
| Goal | Explanation |
|------|-------------|
| Unified async + parallel abstraction | Compose coarse (stage) & fine (micro task) concurrency; reuse across semantic platform & RML pipelines. |
| Deterministic optional mode | Sequential engine for testing & formal verification. |
| Pluggable parallel engines | Hopac, Async/Task, Dataflow, synchronous — per stage hint. |
| Declarative accumulation | Support workflows needing *conditional materialization* (e.g. collect JSON fragments until template variable set complete). |
| Resilient flow control | Backpressure, circuit breakers, adaptive batching & accumulation flush interplay. |
| Memory predictability | Pooled buffers + accumulation spill (disk / compressed) when thresholds risk GC pressure. |
| Observability & debug-ability | Accumulator state metrics (fill ratio, completeness %, wait time). |
| Decoupled layers & D.I.P. | DSL independent of engines and I/O implementations. |

---

## 2. Layered Architecture (Revised)

```
Application Pipelines (Parsing → Sampling → Accumulation → Clustering → Metrics)
        │
Pipeline DSL (graph + accumulators + policies)
        │
Execution Engines (ExecutionEngine, HopacEngine, AsyncEngine, DataflowEngine, SyncEngine)
        │
Flow Control (Backpressure, Batching, Accumulation Flushers, Circuit Breakers)
        │
I/O Layer (Sources/Sinks, Pipe adapters, Connectors)
        │
Memory & Pools (SegmentPool, BatchPool, AccumulatorStatePool, Spill Manager)
        │
Instrumentation (Metrics, Tracing, Event hooks)
        │
Config & Policy Providers (immutable snapshots)
```

Dependency flow: *Top depends only on abstractions*. Engines & flow-control plug in via interfaces.  

---

## 3. Core Abstractions (F# Naming) & C# Mirrors

### 3.1 Data Model
```text
Envelope<'T>:
  payload : 'T
  headers : Map<string,string>
  seqId   : int64
  spanCtx : TraceContext
  ts      : Timestamp
  attrs   : IReadOnlyDictionary<string,obj>
  cost    : struct { bytes:int; cpuHint:float }
```

`Batch<'T>` = logically grouped `Envelope<'T>` (micro-batch).  
`Accumulated<'Key,'State,'Out>` = accumulator result wrapper (state snapshot + optional final output).

### 3.2 Interfaces (F#)

```fsharp
type StageKind = Source | Transform | Accumulator | Branch | Merge | Sink

type StageContext =
  { pipelineId : string
    stageId    : string
    config     : obj
    logger     : ILogger
    tracer     : ITracer }

type StreamIn<'T>   = IAsyncEnumerable<Envelope<'T>>
type StreamOut<'T>  = IAsyncEnumerable<Envelope<'T>>

type Stage<'In,'Out> =
  abstract member id    : string
  abstract member kind  : StageKind
  abstract member init  : StageContext -> ValueTask
  abstract member process :
      ct:CancellationToken ->
      input:StreamIn<'In> ->
      StreamOut<'Out>
  abstract member close : unit -> ValueTask

type ExecutionOptions =
  { cancellation : CancellationToken
    metrics      : IMetrics
    tracer       : ITracer
    logger       : ILogger
    memoryPool   : IMemoryPool
    policies     : PolicyRegistry }

type ExecutionEngine =
  abstract member run     : PipelineGraph * ExecutionOptions -> Task<PipelineResult>
  abstract member inspect : stageId:string -> StageRuntimeState
  abstract member stop    : unit -> Task
```

### 3.3 C# Wrapper (Example Signatures)
```csharp
public interface IStage<in TIn, TOut> {
  string Id { get; }
  StageKind Kind { get; }
  ValueTask Init(StageContext ctx);
  IAsyncEnumerable<Envelope<TOut>> Process(
      CancellationToken ct,
      IAsyncEnumerable<Envelope<TIn>> input);
  ValueTask Close();
}

public interface IExecutionEngine {
  Task<PipelineResult> Run(PipelineGraph graph, ExecutionOptions opts);
  StageRuntimeState Inspect(string stageId);
  Task Stop();
}
```

Wrapper adapts F# records to POCOs (immutable in F#, with C# getters).

---

## 4. Accumulation / Threshold Model

### 4.1 Motivation
Some stages must *delay emission* until a completeness condition is satisfied (e.g., RML logical source fragments needed for template expansion). Requirements:

| Requirement | Example |
|-------------|---------|
| Keyed accumulation | Group fragments by logical subject key |
| Predicate threshold | Emit when required variable set satisfied |
| Count / size threshold | Emit after N items or M bytes |
| Time / watermark threshold | Emit when event time passes watermark |
| Semantic dependency graph | All dependent variable groups resolved (e.g., join across partial maps) |
| Partial flush fallback | Emit partial with “missing” metadata after TTL to avoid deadlock |

### 4.2 Accumulator Abstractions

```fsharp
type CompletenessResult<'State,'Out> =
  | Incomplete of 'State
  | Complete   of 'Out * 'State
  | Expired    of 'Out option * 'State // forced flush

type CompletenessChecker<'State,'In,'Out> =
  'State -> Envelope<'In> -> CompletenessResult<'State,'Out>

type AccumulatorPolicy =
  { maxItems        : int option
    maxBytes        : int option
    maxLatency      : TimeSpan option
    completenessTTL : TimeSpan option
    partialFlush    : bool
    spillAllowed    : bool }

type AccumulatorState<'Key,'State> =
  { key        : 'Key
    state      : 'State
    sizeItems  : int
    sizeBytes  : int
    firstTs    : Timestamp
    lastTs     : Timestamp
    completenessScore : float } // 0.0–1.0

type Accumulator<'In,'Out,'Key,'State> =
  abstract member id          : string
  abstract member init        : StageContext -> ValueTask
  abstract member extractKey  : Envelope<'In> -> 'Key
  abstract member updateState :
      AccumulatorState<'Key,'State> option ->
      Envelope<'In> ->
      AccumulatorState<'Key,'State>
  abstract member checkComplete :
      AccumulatorState<'Key,'State> -> CompletenessResult<'State,'Out>
  abstract member flushForced  :
      AccumulatorState<'Key,'State> -> 'Out option
  abstract member serializeSpill   : AccumulatorState<'Key,'State> -> ReadOnlyMemory<byte>
  abstract member deserializeSpill : ReadOnlyMemory<byte> -> AccumulatorState<'Key,'State>
  abstract member close       : unit -> ValueTask
```

C# wrappers expose `IAccumulator`.

### 4.3 Completeness Semantics Examples

| Scenario | Completeness Logic |
|----------|-------------------|
| Template Expansion | Required variable set R; current variable set V; complete if R ⊆ V |
| JSON Join (multi segments) | Partition map expecting distinct segment labels; complete when all segment labels captured |
| Count Threshold | sizeItems >= N |
| Size Threshold | sizeBytes >= M |
| Time Watermark | eventTime ≥ watermarkTime (holds input) |
| Hybrid | (R ⊆ V) OR (latency > TTL AND partialFlush) |

Represent required variable set as immutable bitset for O(1) subset test.

### 4.4 Spill Strategy
When memory pressure or thresholds exceed:
1. Serialize accumulator state → pooled buffer or off-heap (memory-mapped file).
2. Keep minimal index (key, completenessScore, lastTs).
3. On new fragment for spilled key: reload, merge, re-check.

Heuristics:
- Spill oldest incomplete with lowest progress score.
- Avoid spill if state size below *spillMinBytes*.

---

## 5. Accumulator Stage Processing Algorithm (Pseudo)

```
for each incoming envelope:
    key = extractKey(envelope)
    state = stateTable[key] (or create new)
    updatedState = updateState(state, envelope)
    if thresholds (maxItems/maxBytes) reached => attempt complete
    result = checkComplete(updatedState)
    match result:
        Incomplete -> maybe spill if memory pressure
        Complete(out, st) -> emit Envelope(out) with attrs completeness=1.0
                             reset or remove (depending on reuse policy)
        Expired(outOpt, st) -> if partialFlush emit with completeness<1.0
Periodic (timer):
    For each state:
        if TTL exceeded:
            forced = flushForced(state)
            emit partial if allowed
```

Backpressure Integration:
- Pending accumulator memory cost contributes to *effective queue size*.
- If (sumPendingBytes > memoryBudget) → upstream request slowdown / credit depletion.

Observability:
- Metrics: `accumulator_active_keys`, `accumulator_spilled_keys`, `accumulator_avg_latency`, `accumulator_completeness_distribution`.

---

## 6. Pipeline Graph Extensions

Augment `StageNode` with accumulation metadata:

```fsharp
type AccumulationDescriptor =
  { policy          : AccumulatorPolicy
    engineHint      : ParallelismHint
    progressWeights : Map<string,float> } // weigh variable groups

type StageNode =
  { stage          : obj
    kind           : StageKind
    parallelism    : ParallelismHint
    accumulation   : AccumulationDescriptor option
    bufferPolicy   : BufferPolicy option
    backpressure   : BackpressureStrategy option
    errorPolicy    : ErrorPolicy }
```

Graph validation ensures only one accumulator declaration per node and no illegal cycles (accumulator stages cannot feed back into themselves without explicit feedback channel).

---

## 7. Execution Engines (Adjustments for .NET 9)

| Engine | .NET 9 Leverage |
|--------|------------------|
| AsyncEngine | Uses improved ThreadPool heuristics; integrates `ValueTask` path; `Socket`/`FileStream` optimizations; Activity sampling for traces. |
| HopacEngine | Interop with `Task` boundary via minimal wrappers; optional pinned object heaps for high-frequency small objects. |
| DataflowEngine | Bounded capacity tuning with .NET 9 scheduler; automatic `ExecutionDataflowBlockOptions.EnsureOrdered` toggled by stage attribute. |
| SyncEngine | Baseline deterministic mode; *no* parallel scheduling. |

Hybrid selection uses:
- Stage declared cost hints (CPU vs IO weight).
- Historical metrics feed (adaptive re-plan interval).
- Could integrate **engine migration**: draining stage and re-attaching with different engine (future extension).

---

## 8. Backpressure (Accumulator-Aware Enhancements)

Effective Upstream Pressure Metric:

```
effectiveQueue = rawQueueLength
               + (pendingAccumulatorBytes / bytesPerUnitWindow)
               + (activeSpillCount * spillPenaltyFactor)
```

Backpressure Strategy consumes `effectiveQueue` rather than raw queue size. Prevents hidden memory ballooning due to accumulation.

Adaptive Batching Coordination:
- If accumulator fill ratio high and completion rare → reduce batch size (improve latency to completions).
- If many small completions → increase batch size to amortize overhead.

---

## 9. Memory & Buffer Enhancements

| Pool | Purpose | Key API |
|------|---------|---------|
| SegmentPool | Raw byte segments (parsing) | `rent(size) -> Span<byte>` |
| BatchPool | Reusable Batch wrappers | `allocate(count)` |
| AccumulatorStatePool | Preallocated state containers | `newState() / reclaim(state)` |
| SpillBufferPool | Buffers for serialization staging | `reserve(bytes)` |

Spill Manager chooses medium:
1. In-memory compressed (LZ4) if under threshold.
2. Memory-mapped file (region reuse) for large states.
3. Optional ephemeral disk (config gating) for self-host.

GC Pressure Monitor:
- Periodically sample LOH allocations; if rising faster than threshold → escalate spill aggressiveness.

---

## 10. Error & Resilience Expansion

Accumulator-specific conditions:
| Error | Policy Option |
|-------|---------------|
| CompletenessDeadlock (no progress > TTL) | Force partial flush or abort pipeline |
| KeyExplosion (key cardinality > limit) | Evict LRU incomplete; emit warning metrics |
| SpillFailure | Retry spill, fallback to synchronous completion (partial) |
| SerializationError | Mark accumulator corrupted, attempt salvage emission |

Circuit Breaker integrates accumulator latency percentile; if  p99(specifiedAccumStage) > SLA for sustained window → open & route to degrade path (e.g., partials only).

---

## 11. DSL Enhancements (F# Example)

```fsharp
pipeline {
  source (Files.readAll "*.json") |> parallelism 2
  |> transform parseJson
  |> accumulate templateExpansion {
        key (fun env -> env.attrs["subjectId"] :?> string)
        requireVars ["id"; "name"; "riskClass"; "limit"]
        maxItems 200
        maxLatency (TimeSpan.FromSeconds 5)
        partialFlushAfter (TimeSpan.FromSeconds 10)
        spillOnMemoryPressure true
     }
  |> transform clusterTokens |> engine Hopac
  |> transform computeMetrics
  |> sink metricsStore
}
```

Builder expands to `PipelineGraph` + `AccumulationDescriptor`.

C# Example (fluent):
```csharp
var p = Pipeline
  .Source(Files.ReadAll("*.json")).WithParallelism(2)
  .Transform(ParseJson.Instance)
  .Accumulate(acc => acc
     .Key(e => (string)e.Attrs["subjectId"])
     .RequireVars("id","name","riskClass","limit")
     .MaxItems(200)
     .MaxLatencySeconds(5)
     .PartialFlushAfterSeconds(10)
     .SpillOnMemoryPressure(true))
  .Transform(ClusterTokens.Instance).UseEngine(EngineType.Hopac)
  .Sink(MetricsStore.Instance);
```

---

## 12. System.IO.Pipelines Integration (Refined)

| Role | Usage |
|------|-------|
| Source | Wrap `PipeReader` to produce `Envelope<ReadOnlyMemory<byte>>`. |
| Accumulator Input | `PipeReader.AdvanceTo` only after local copy *or* zero-copy attach (if ephemeral & single consumer). |
| Sink | `PipeWriter` for serialized batches; flush triggered by batch size or watermark. |

Parser Example:
- Multi-segment read → accumulate into pooled buffer if JSON token not terminated.
- On full logical record, yield envelope; return unused tail to pipe (low overhead).

---

## 13. Testing & Verification Extensions

| Target | Additional Tests |
|--------|------------------|
| Accumulator Completeness | Property: feeding required variable set permutations always yields 1 final completion; partial flush path correct count. |
| Key Cardinality | Fuzz waveform of keys; ensure eviction policy stable & no memory blow-up. |
| Spill & Restore | Roundtrip serialization idempotence; performance under repeated spill/reload cycles. |
| Liveness | TLA+/Alloy model: no permanent block if partialFlush enabled & inputs eventually deliver required variables OR TTL triggers. |
| Deterministic Mode | Accumulator order of completions stable given same input ordering. |

---

## 14. Metrics (Accumulator-Focused)

| Metric | Description |
|--------|-------------|
| `accumulator_active_keys` | Current keys in memory |
| `accumulator_spilled_keys` | Keys spilled |
| `accumulator_completeness_ratio` | Histogram (progress) |
| `accumulator_flush_latency` | Time from first fragment → emission |
| `accumulator_partial_flushes_total` | Count of expired partials |
| `accumulator_memory_bytes` | Total bytes held |
| `accumulator_spill_io` | Bytes read/written (spill) |
| `accumulator_deadlock_incidents` | TTL expirations without emissions |

---

## 15. Performance & Optimization Strategies

| Issue | Mitigation |
|-------|------------|
| High GC due to fragment copies | Zero-copy spans + pooled buffer reuse; copy-on-write only at branching |
| Accumulator hot lock contention | Shard state tables by hash (striped dictionaries) |
| Large key explosion | Adaptive eviction: oldest incomplete with lowest completenessScore |
| Spill overhead | Batch serialize multiple states; asynchronous flush (Hopac job batch) |
| Completion latency variance | Adaptive micro-batch shrinks when completeness progress slow |

---

## 16. Security Considerations

| Vector | Control |
|--------|---------|
| Malicious key amplification | Key cardinality limit + rate of new keys threshold |
| Sensitive data in spill | Spill encryption (AES-GCM) with key rotation (config) |
| Poison partial flush spam | Partial flush minimum progress threshold, or throttle per key |
| Plugin misuse inside accumulator | Capability gating (no file/network unless allowed) |

---

## 17. Migration Strategy (from v1)

| Phase | Action |
|-------|--------|
| M0 | Introduce new F# interface naming; add C# wrappers. |
| M1 | Integrate accumulator stage base & minimal completeness (count, size). |
| M2 | Add required variable & hybrid TTL completeness. |
| M3 | Implement spill manager + instrumentation. |
| M4 | Formal liveness model + property suite; performance tuning. |
| M5 | Release hybrid engine auto-selection (observed metrics). |

---

## 18. Risk & Mitigation (New Items)

| Risk | Mitigation |
|------|------------|
| Complex completeness logic causing starvation | TTL + partial flush plumbing + liveness checks |
| Spill thrashing under oscillating pressure | Hysteresis threshold; spill cool-down interval |
| Cross-language API divergence (F#/C#) | Shared contract generation (Source Generators) |
| Accumulator memory leak via lost state references | State registry + finalizer sentinel audit job |
| Partial flush semantics misused as “normal” path | Metrics & alert if partialFlushRatio > expected threshold |

---

## 19. Example Accumulator Scenario (RML Template Expansion)

| Requirement | Implementation |
|-------------|----------------|
| Need all variables `{subjectId, name, riskClass, limit}` | Maintain bitset per key; each variable maps to bit index |
| JSON fragments arrive out-of-order | Update bitset on each fragment; union existing bits |
| Timeout if not complete in 10s | TTL → partial flush with missingVars list in headers |
| Memory pressure triggers spill | Serialize state (bitset + fragment store offsets) |
| Emission | On completeness or forced flush → compose merged dictionary & yield envelope |

Headers added:
```
"acc.completeness" = "1.0" | "<fraction>"
"acc.missingVars"  = "riskClass;limit" (if partial)
"acc.spilled"      = "true|false"
```

---

## 20. Summary

The revised framework:
- Aligns with .NET 9 performance primitives.
- Separates *semantic concerns* (pipeline, accumulation) from *mechanics* (execution engines).
- Adds robust, configurable accumulation/threshold semantics vital for RML and semantic integration workloads.
- Ensures high throughput via adaptive batching, accumulation-aware backpressure, and pooled memory.
- Maintains testability and rigorous correctness through deterministic mode, property testing, and formal liveness modeling.
- Provides a dual-interface strategy (idiomatic F#, familiar C#) without compromising internal purity or external adoption.

This design sustains reuse across the Universal Semantic Integration Platform, open-source RML pipelines, and future streaming analytics offerings.

---

*End of Document*