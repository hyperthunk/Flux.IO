# Performance & Reliability Impact Analysis  
Architecture Roadmap Alignment – Space, Throughput, Saturation Robustness, Fault Tolerance  
Date: 2025-08-19  

---

## 1. Scope & Method

This document evaluates the architectural roadmap features against four critical non‑functional dimensions:

1. Space Efficiency / Utilisation
2. Raw Throughput (steady‑state)
3. Robustness Under Load / Saturation
4. Fault Tolerance (error containment & recovery)

It also defines a scaling blueprint (cloud‑agnostic, F#/.NET native) and updates the test & benchmarking strategy to systematically measure and regress these qualities.

---

## 2. Feature Impact Matrix

### Legend for Qualitative Impact
- (++): Strong positive impact if implemented well
- (+): Moderate positive
- (0): Neutral / minimal impact
- (-): Potential negative unless mitigation applied
- (~): Mixed (trade‑off; needs tuning)

| Roadmap Feature | Space | Throughput | Saturation Robustness | Fault Tolerance | Primary Risks | Key Mitigations |
|-----------------|-------|-----------|-----------------------|-----------------|--------------|-----------------|
| LengthGate Parser | (0) | (+) (simple logic) | (0) | (-) (needs trusted length) | Mismatch length → parse fail late | Prefer Frame default |
| FramingAssembler | (+) (no full buffer copies) | (+) | (+) (incremental boundary detection) | (0) | Pathological nesting | Depth guard & max token size |
| StreamingMaterialize (scan + final parse) | (+) | (+) (once per doc) | (+) (early boundary detect) | (0) | Pending state for truncated input | Introduce finalize signal |
| Token Streaming (PStream) | (++), if zero‑copy spans | (++) (downstream early start) | (++): progressive pressure relief | (+) (partial result recovery) | Token queue backpressure | Bounded token queue & adaptive chunking |
| Stage Lifecycle (OnDemand/Persistent/Continuous) | (~) persistent caches can grow | (++) (avoid re-init) | (+) (continuous sources decouple bursts) | (+) (isolation on restart) | Over-retention in persistent instances | Pool size caps & LRU |
| Multi-Backend Execution (Sync/Async/Hopac/TPL) | (0) | (++), choose best backend | (++), localize saturation | (+) (backend-specific isolation) | Context switching overhead | Coalesce pure stages (fusion) |
| Bridges Between Backends | (-), added buffering | (~) overhead unless fused | (+) (absorbs bursts) | (+) (contain failure domain) | Copy overhead & latency | Zero-copy envelope pass + bounded channel sizing |
| Parallel Executor (partition/merge) | (~) extra queues | (++) CPU usage scaling | (+) (drains queues faster) | (+) (fail one partition) | Skew causing hot partition | Dynamic repartition / key hashing metrics |
| Dynamic Parallelism Inference | (+) by right-sizing state | (++) (auto scaling) | (++), avoids overload | (+) if shrink logic safe | Oscillation / thrash | Hysteresis & min dwell times |
| Window/Trigger API | (~) state retention | (0/+), improved batch efficiency | (+) smoothing of emission patterns | (+) checkpoint trigger alignment | Unbounded late element buffering | Late element drop / watermark policy |
| Pluggable State Backend (In-mem/MMF/RocksDB) | (++) off-heap / disk tiers | (+) (hot set in-memory) | (++), avoids memory blowup | (+) durable snapshotting | Serialization overhead | Adaptive tier thresholds |
| Checkpointing (at-least-once) | (-) snapshot space | (-) pause cost if blocking | (+) bounded replay scope | (++), recover progress | Large snapshot pause | Incremental / async snapshot |
| Exactly-once (optional) | (- -) meta state & logs | (-) two-phase overhead | (+) prevents duplicates under load | (+++) highest processing guarantee | High complexity & latency | Offer opt-in; default at-least-once |
| Stage Fusion (pure transform coalescing) | (+) fewer objects | (++) less queue handoff cost | (+) reduces intermediate pressure | (0/-) larger blast radius on failure | Debuggability loss | Emit sub-metrics inside fused stage |
| Reactive Streams Adapter | (0) | (~) adapter overhead | (+) demand-driven gating | (+) spec backpressure semantics | Mismatch credit algorithm | Spec subset compliance tests |
| Schedule Controller (systematic concurrency) | (0) | (0/-) overhead in test mode | (++), reveals deadlocks before prod | (++), surfaces race conditions | Over-instrumentation cost | Build-time flag & sampling |
| Backpressure Channels (bounded, credit-based) | (+) prevents unbounded growth | (++) stable latency under load | (+++) principal saturation defense | (+) avoids OOM crash | Wrong sizing → throttling | Auto-scale capacity heuristics |
| Error Channel / Dead-Letter Routing | (0) | (0) | (+) isolates poison data | (++), ensures forward progress | Error storms flood DLQ | Rate-limit + summarization |
| Accumulator Slice Merging | (+) partial state compaction | (+) (smaller working set) | (+) reduces GC churn | (+) smaller recovery boundaries | Merge contention | Lock striping / async merge |
| Token Materializer (JTokenWriter path) | (~) duplicated representation unless discarded | (-) overhead if always enabled | (0) | (+) reproduction for auditing | Double parse overhead | Lazy / on-demand materialization |
| Connector URI Registry / DSL | (0) | (+) developer efficiency | (0) | (0) | Misconfiguration | Early validation & schema |
| Resequencer Pattern | (-) buffer window memory | (-/+) reorder cost | (+) absorbs out-of-order bursts | (+) ensures logical order | Window overflow | Dynamic window cap & drop policy |
| Adaptive Engine Migration | (+) memory aware staging | (+) re-place compute bound tasks | (++), redistribute hot spots | (+) isolate failing engine | Migration churn | Warm standby & cutover threshold |
| State Spill Tiers (RAM→MMF→Disk) | (+++) prevents OOM | (+/-) MMF slower than RAM | (++), graceful under high cardinality | (+) persists across restarts | Spill fragmentation | Compaction & page pooling |
| Late Element Handling (watermarks) | (~) hold buffer | (0) | (+) controlled staleness | (+) predictable completeness | Misconfigured watermark → data loss | Monitoring watermarks vs ingest lag |
| Partition Key Skew Analytics | (+) enable targeted scaling | (++) rebalances throughput | (++), prevents systemic head-of-line | (+) isolates hotspots | Inaccurate sampling | Periodic full histogram |

---

## 3. Design Decision Trade-offs & Their NFR Consequences

| Decision | Benefit | Cost | Mitigation |
|----------|---------|------|------------|
| Final parse vs incremental building (StreamingMaterialize) | Simplicity & parity | Extra full-buffer copy + final parse | Token pipeline bypass for low-latency consumers |
| Single-token emission per Feed (TokenStreamReader) | Predictable backpressure math | Higher per-token call overhead | Introduce `EmitMany` batch mode for high-volume arrays |
| Channels for all backend boundaries | Clear backpressure nodes | Memory footprint for queued envelopes | Fusion of trivial adjacent stages |
| Pluggable backends (Hopac/TPL) | Optimized per workload | Complexity & bridging overhead | Limit bridging frequency; cluster “like” stages |
| State snapshot synchronous start | Consistent root state | Pause time | Prefetch snapshot regions + incremental diffing |
| Fuse only pure transformations | Lower overhead | Need purity inference | Developer metadata + static analyzer |
| Envelope metadata retention (full) | Observability & replay | Memory per envelope | Tiered metadata (essential vs verbose toggle) |

---

## 4. Scaling Blueprint (Cloud-Agnostic, F# / .NET)

### 4.1 Horizontal & Vertical Scaling Axes

| Axis | Mechanism | Core Abstractions |
|------|-----------|-------------------|
| Vertical (single node) | ParallelExecutor + DynamicParallelism | Work partition by key / consistent hashing |
| Horizontal (multi-node) | Sharded pipeline clusters | Partition router → remote channel bridge (gRPC / QUIC / raw TCP) |
| Storage scaling | Tiered state backend | In-mem hot, MMF warm, disk / remote store cold |
| Control plane | Declarative pipeline registry | Config maps or embedded DSL manifest |

### 4.2 Topology Variants

1. **Monolithic Node (Dev / Small)**  
   All backends in one process; minimal bridging.

2. **Multi-Process Single Host**  
   - Supervisor (control plane) + worker processes  
   - Shared memory (MMF) for warm state, named pipes / `System.IO.Pipelines` for envelope transfer.

3. **Clustered Deployment (Any Cloud / On-Prem)**  
   - Hash ring for partitioned routes (e.g., sessionId hashing)  
   - Inter-node envelope channel: pluggable transport interface (QUIC preferred for low CPU, fallback TCP).  
   - Gossip-based membership (Serf-style) or control plane.

4. **Geo-Distributed Edge + Core**  
   - Edge nodes perform parsing + token filtering; core does heavy joins / enrichment.  
   - Envelope compression (Zstd) optional on egress.

### 4.3 Key Architectural Components

| Component | Role | Cloud-Agnostic Implementation |
|-----------|------|--------------------------------|
| Execution Backends | Scheduling & concurrency | Async: .NET `Channels`; Hopac; TPL Dataflow fallback |
| Transport Bridge | Inter-process / inter-node streams | Abstraction over `Stream` + length-prefix framing |
| State Store Interface | Keyed & window state | Pluggable provider pattern & DI container |
| Checkpoint Coordinator | Snapshot barrier orchestration | gRPC or local message bus (inproc) |
| Metric & Telemetry Export | Observability | OpenTelemetry exporters (console, OTLP, Prometheus) |
| Config & Pipeline Registry | Declarative graph | JSON/YAML + DSL compile step |
| Security Layer | Envelope authz/validation | Optional per-envelope ACL check stage |

### 4.4 Space Efficiency Strategies

- Use `struct` discriminated unions for hot path StreamCommand (minimize allocations).
- Reuse `ArrayBufferWriter<byte>` via pools (per dynamic partition).
- Implement slab allocator for small envelopes (reduces small object heap churn).
- Adopt `ReadOnlyMemory<byte>` / `Span<byte>` across parse & tokenization boundaries to eliminate copies.
- Introduce adaptive batch size negotiation in `EmitMany` to align with L1/L2 cache sizing (e.g., 32–128 KB buckets).
- State tier thresholds: configure via percentile memory watermark (ex: spill when heap usage > 65%).

### 4.5 Throughput Enhancements

- Stage Fusion Planner: pre-execution compile graph → fused delegates (IL emit optional).
- Lock Striping for partition counters (avoid global contention).
- Affinity pinning: assign partitions to dedicated threads (via Hopac jobs or dedicated tasks) for cache locality.
- Payload classification: small vs large messages → separate channel classes (avoid head-of-line blocking).
- Optional NativeAOT for long-running services to improve startup & reduce JIT jitter (profile stable pipeline shapes).

### 4.6 Saturation & Backpressure

- Bounded channel default; unbounded only under explicit override.
- Credit-based upstream request: a segment requests N credits proportional to downstream queue headroom.
- “Shed early” policy: if global pressure > threshold, drop oldest non-critical class envelopes (policy plug-in).
- Slow partition isolator: detect skew, replicate stage horizontally for hot key subset (temporary split).

### 4.7 Fault Tolerance & Recovery

| Fault Type | Strategy | Mechanism |
|------------|----------|----------|
| Stage crash (transient) | Auto-restart with jitter | Lifecycle manager & exponential backoff |
| Poison message | Reroute & quarantine | Error channel + envelope fingerprint |
| Node loss | Rebalance partitions | Hash ring update + checkpoint resume |
| State corruption (detected by checksum) | Rollback to last snapshot | Snapshot catalog with version chain |
| Backpressure deadlock | Watchdog detection | Progress heartbeat (tokens / output per interval) |
| Memory pressure spike | Spill + GC compaction trigger | Adaptive spill policy & LOH defragment scheduling |

---

## 5. Reliability Design Patterns

| Pattern | Purpose | Implementation Notes |
|---------|---------|----------------------|
| Circuit Breaker Stage | Block flapping downstream sink | Track error rate & fail-fast state |
| Bulkhead Partition | Prevent single load spike affecting all | Independent bounded channels per partition group |
| Supervising Router | Restart failing branches only | Keep stable statistics for health classification |
| Rolling Snapshot | Avoid large pause for checkpoint | Differential chunking; MERKLE-like segment hashing |
| Late Event Guard | Bound out-of-order window memory | Timestamp watermark & thresholding |
| Replay Journal (for at-least-once) | Rehydrate sources post-crash | Sequence offset + minimal metadata |

---

## 6. Benchmark & Profiling Architecture

### 6.1 Benchmark Layers

| Layer | Tooling | KPIs |
|-------|--------|------|
| Micro (Stage / Parser / Tokenizer) | BenchmarkDotNet | ns/op, alloc B/op, branches, cache misses (ETW) |
| Meso (Segment Chains) | Custom harness + BDN | Throughput docs/sec, p99 stage latency |
| Macro (Full Pipeline) | Scenario runner | End-to-end latency distribution, CPU %, RSS, GC stats |
| Stress / Soak | Long-running driver | Error rate, sustained throughput decay, memory growth slope |
| Chaos | Fault injector | Recovery time, message loss %, duplicate ratio |

### 6.2 Metrics Taxonomy

| Category | Metric | Description |
|----------|--------|-------------|
| Throughput | `pipeline_docs_total`, `pipeline_docs_rate` | Ingest & processed counts / rate |
| Latency | `stage_latency_ms_{p50,p95,p99}` | Per stage percentile histograms |
| Backpressure | `channel_depth_current`, `channel_depth_max` | Depth watermarks |
| Allocation | `alloc_bytes_total`, `alloc_rate` | Track trends for regression |
| GC | `gc_pause_total_ms`, `gen2_collections` | Latency spikes correlation |
| Errors | `stage_error_total`, `poison_routed_total` | Stability signals |
| State | `state_size_bytes`, `snapshot_duration_ms` | Snapshot health |
| Tokens | `tokens_emitted_total`, `tokens_per_doc_avg` | Token streaming load shape |
| Fusion | `fused_stage_count`, `fused_ops_per_stage_avg` | Optimization effectiveness |
| Parallelism | `partition_skew_ratio`, `active_workers` | Balance metrics |
| Watermarks | `watermark_lag_ms` | Window timeliness |

### 6.3 Baseline + Regression Workflow

1. **Establish Golden Baseline** per feature set (commit tag).  
2. **Run Benchmark Suite** (nightly + PR gating subset).  
3. **Compare KPIs**:
   - Apply threshold: e.g., >10% latency regression or >15% allocation increase → FAIL gate.
4. **Publish Report** (Markdown + JSON) into CI artifact & trend store.  
5. **Profiling Deep-Dive** on flagged deltas (PerfView / Dotnet trace).  
6. **Store Snapshots** to analyze long-term drift (S-curves, capacity planning).

### 6.4 Memory Profiling Strategy

- Enable EventPipe counters for allocation rate & LOH usage.
- Periodic heap diff (Test fixture controlling load for deterministic sampling).
- Fragmentation monitor: track average object size vs segment utilization.

---

## 7. Updated Testing Strategy (Aligned with Findings)

| Area | New / Enhanced Test Type | Objective |
|------|---------------------------|-----------|
| Finalize Semantics | Corruption strict failure property | Ensure truncated JSON errors deterministically |
| Token Pipelines | PStream model extension | Depth/order invariants + parity |
| Sync vs Async Engines | Dual-mode equivalence properties | Deterministic semantics under both modes |
| Backpressure | Saturation fuzz with bounded channels | Validate no unbounded growth / deadlocks |
| Parallelism & Skew | Partition distribution property | Ensure <= threshold skew ratio |
| Dynamic Scaling | Hysteresis property (no oscillation) | Ensure stable resizing decisions |
| State Backend | Snapshot round-trip + corruption simulation | Data integrity & recovery |
| Checkpoint & Replay | Crash injection scenario property | At-least-once: no lost processed outputs |
| Fusion | Equivalence (pre/post compile plan) | Semantic parity & metric mapping |
| Reactive Adapter | Demand accounting property (Publisher mock) | Credits ↔ emission correctness |
| Window/Trigger | Trigger firing matrix tests | Fire/Purge decisions correct on timelines |
| Schedule Controller | Trace replay & exploration coverage | Deterministic reproduction of interleavings |
| Chaos / Fault | Injected stage failures & restart | Confirm isolation & absence of cascades |
| Soak / Memory | Long-run allocation & leak detection | Stabilized memory slope < threshold |
| Performance Regression | Automated macro bench guard | Hard gating on SLA KPIs |

### 7.1 Benchmark Suites

| Suite | Composition | Frequency |
|-------|-------------|-----------|
| MicroParse | Length vs Frame vs StreamingMaterialize vs Tokens | PR + Nightly |
| FusionChain | 3–10 pure transforms w/ & w/o fusion | Nightly |
| PartitionSkew | Synthetic heavy-skew key distribution | Weekly |
| StateGrowth | Increasing key cardinality (1k→1M) | Weekly |
| WindowLatency | Sliding & tumbling windows under jitter | Nightly |
| CheckpointSoak | 6h run w/ periodic snapshot & kill | Weekly |
| ChaosLatency | Inject stage failure every N seconds | Weekly |
| ReactiveInterop | RS demand scenario matrix | Nightly (short) |

### 7.2 Test Result Classification

- **Functional Fail**: Invariant violation (must fix).
- **Performance Alert**: Regression beyond soft threshold (triage).
- **Reliability Warning**: Elevated error or drop rates but within fallback tolerance.
- **Observability Gap**: Missing mandatory metric discovered during test run.

---

## 8. Actionable Recommendations

| Priority | Recommendation | Rationale |
|----------|----------------|-----------|
| P0 | Implement assembler finalize & tighten corruption property | Deterministic error semantics pre Tier‑C |
| P0 | Introduce PStream DU & token machine invariants | Unlock Tier‑C streaming correctness |
| P1 | Build synchronous execution engine wrapper (no tasks) | Deterministic scheduling & simpler debug |
| P1 | Add bounded channel instrumentation & backpressure properties | Early saturation observability |
| P1 | Create baseline microbench suite (parsers, token reader) | Foundation for perf regression control |
| P2 | Add schedule controller (trace capture + replay) | Precondition for concurrency systematic tests |
| P2 | Add fusion planner & parity tests (feature flagged) | Immediate throughput gains + safe rollout |
| P2 | Start state backend abstraction with in-memory + MMF tier | Prepare for high cardinality workloads |
| P3 | Integrate window/trigger test harness | Enables advanced accumulation scenarios |
| P3 | Reactive adapter + demand property harness | Ecosystem interoperability |
| P3 | Partition skew analytics & dynamic inference w/ hysteresis tests | Prevent hot partitions early |

---

## 9. Open Risks & Mitigation Paths

| Risk | Status | Mitigation Path |
|------|--------|-----------------|
| Pending/truncated JSON indefinite `NeedMore` | OPEN | Finalize signal & timeout guard |
| Token queue memory creep under burst | OPEN | Bounded queue + drop/wait policy, metric alerts |
| Fusion debug difficulty | OPEN | Sub-span emission, fuse metadata mapping |
| Dynamic scaling oscillation | OPEN | Damped EMA throughput metrics + min dwell |
| Snapshot pause spikes | FUTURE | Incremental chunk hashing & write-ahead diffs |
| Skew-induced SLA breach | FUTURE | Real-time histograms + targeted oversharding |
| Late window memory blowout | FUTURE | Watermark lag-based eviction strategy |

---

## 10. Conclusion

The path to Tier‑C and beyond materially shifts complexity from *single-threaded parse correctness* to *distributed concurrency, adaptive resource management, and sustained reliability under load*. Early investment in:

1. Deterministic sync engine
2. Backpressure instrumentation
3. Token streaming invariants
4. Baseline performance harness
5. Schedule control (systematic exploration)

will drastically reduce future refactor costs and shorten feedback loops on high-risk concurrency and scaling features.

A disciplined, layered test approach ensures each architectural addition (fusion, windowing, state tiers, backends) lands with measurable performance & reliability characteristics—and regressions are surfaced before broad adoption.

---

## 11. Immediate Next Steps (Execution Checklist)

- [ ] Add finalize API to IJsonAssembler (or wrapper) + strict error test
- [ ] Implement PStream pipeline & token invariants
- [ ] Dual execution modes (Sync/Async) + equivalence property
- [ ] Channel metrics & backpressure saturation property
- [ ] Microbench baseline (publish artifact)
- [ ] Schedule controller v0 (random + replay)
- [ ] Fusion prototype (pure transform tagging) + parity tests

(Track via issues; link test artifacts to CI dashboards.)

---

*Prepared for: Architecture & Platform Engineering*