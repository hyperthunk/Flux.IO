# Flux.IO Test & Verification Architecture

This document gives a comprehensive deep‑dive into the current testing and verification infrastructure of the Flux.IO test suite.  
It explains the layers, abstractions, invariants, generators, model‑based machinery, property suites, and how they interoperate.  
It also outlines remaining Tier‑B (framing) action items and the roadmap foundation for Tier‑C (true external token streaming).

---

## Table of Contents

1. Conceptual Stack Overview
2. Core Runtime Primitives
   - Flow
   - Envelope
   - StreamProcessor / StreamCommand
   - Execution Environment (TestEnv)
3. Data & Random Generation
   - General Generators
   - JSON Generators
   - Test Seeds, Sizing & Shrinking
4. Integration Stages (Domain-Specific Streaming Primitives)
   - JsonSource
   - ToBytes
   - Chunker
   - Parser (direct & length-chunk reassembly)
   - Accumulation
   - Projection
   - Terminal
5. JSON Assembly Layer (Tier-A / Tier-B / Pre‑Tier-C)
   - IJsonAssembler & AssemblerStatus
   - LengthAssembler
   - FramingAssembler
   - StreamingMaterializeAssembler
   - Instrumented Streaming Assembler
   - Adapter (AssemblerProcessor)
6. Pipelines
   - DirectPipeline
   - ChunkPipeline
   - ParseMode
   - Execution Harness (PipelineExec)
   - Future PStream (StreamingPipeline) Scaffolding
7. Core Integration Machine (Model-Based Testing)
   - Document Model
   - Runtime & SUT
   - Commands & Operations
   - Model Updates
   - Machine.Next Scheduling
   - Invariants
8. Higher-Level Integration Properties
   - Extended Invariants
   - Classification Labels
   - Script Generation & Execution
9. Streaming Tests (Phase 4 Artifacts)
   - Materialization Property Tests
   - Error Simulation
   - Structural / Depth Invariants (Instrumentation)
10. Framing Tests
11. Token Streaming Prototype
12. StreamProcessor Model-Based Specs (Micro-Level)
13. Flow Monad & Functional Law Tests
14. Additional Unit & Domain Tests
15. Error / Failure Scenario Testing
16. Architectural Testing Patterns & Conventions
17. Extending / Customizing Tests
18. Remaining Tier‑B Actions
19. Tier‑C Roadmap Preparation
20. Quick Reference Summary

---

## 1. Conceptual Stack Overview

Layers (bottom → top):

1. **Core Runtime Abstractions** (`Flow`, `Envelope`, `StreamProcessor`, `StreamCommand`)  
   Provide functional + effectful streaming semantics.

2. **Execution Environment**  
   Test harness environment (metrics, logging, tracing, memory) used in flows.

3. **Generators**  
   FsCheck-based composable random data fabrication (JSON, envelopes, cost, trace context).

4. **Integration Stages**  
   Primitive streaming processors (source, chunker, parser, accumulation, projection, sinks).

5. **Assemblers (IJsonAssembler)**  
   Unified polymorphic interface enabling plug-in parse strategies (length, framing, incremental scanning).

6. **Pipeline Composition**  
   Assemble stages into `DirectPipeline` and `ChunkPipeline` with parse modes.

7. **Machine-Based Integration Test Harness**  
   - `CoreIntegrationMachine`: Model → Commands → Operations → Runtime stepping → Invariants.

8. **Property Suites**  
   Three broad categories:
   - Pipeline integration invariants (model-based system-level).
   - Micro-level stream processor specs (model-based / stateful).
   - Structural parsers / assemblers correctness & robustness (framing, streaming).

9. **Instrumentation & Error Simulations**  
   Depth tracing, token counting, corruption injection for negative test coverage.

---

## 2. Core Runtime Primitives

### Flow
A lightweight monadic abstraction encapsulating asynchronous computations executed in a shared `ExecutionEnv`.

Key operations:
- `Flow.ret`, `Flow.bind`, `Flow.map`
- `Flow.run` executes against an environment + cancellation token.
- Additional utilities: `withTimeout`, `catch`, `tryFinally`, `local`, `ask`, `liftTask`.

### Envelope
Metadata wrapper around an input/output payload:
- `SeqId`, `SpanCtx` (trace), `Attrs`, `Headers`, `Cost`, timestamp, etc.
- `mapEnvelope f` transforms payload while preserving metadata.

### StreamProcessor<'a,'b>
A stage abstraction: `Envelope<'a> -> Flow<StreamCommand<'b>>`.

### StreamCommand<'b>
Finite state transitions:
- `Emit env`
- `EmitMany envs`
- `Consume` (a.k.a. RequireMore)
- `Complete`
- `Error ex`

---

## 3. Data & Random Generation

### General Generators (`Generators.fs`)
- Random trace context, costs, arbitrary envelopes (ints, strings).
- Controlled sized generation for flows with optional async/delay.
- Function generators (linear forms for int transformations).
- Probability distributions for failure simulation.

### JSON Generators (`Generators.JsonGenerators`)
- Property name generator with suffix variety.
- Scalars: null, bool, int, float, typical strings, random ISO dates.
- Recursive object/array generation with size and depth scaling (`Gen.sized`).
- Additional structured & typical domain JSON patterns (users, config, arrays of items).
- `genJson` final output: formatted JSON string; used widely across integration tests.

Design notes:
- Depth limited (max ~5) to bound complexity.
- Balanced frequencies: scalars favored for variability + reduce heavy nested shrinkers.
- Shrinking: FsCheck automatically shrinks JSON by reducing depth/size components.

---

## 4. Integration Stages

All under `IntegrationStages`:

| Stage | Signature | Description |
|-------|-----------|-------------|
| JsonSource.create | unit → string | Emits exactly one randomly generated JSON string. |
| ToBytes.create | string → ReadOnlyMemory<byte> | Converts UTF‑8 to bytes (single emission). |
| Chunker.fromByteArray | unit → ReadOnlyMemory<byte> | Emits deterministic pre-sliced byte chunks derived from an array. |
| Parser.direct | string → JToken | Parses whole JSON string into a `JToken`. |
| Parser.chunkReassembly | ReadOnlyMemory<byte> → JToken | Accumulates bytes until expected length then parses once. |
| Accumulation.create | JToken → AccumEvent | Root scalar property batching + completion domain event. |
| Projection.scalarsAndObjects | JToken → ProjectionEvent | Emits Scalars then Objects (root-level). |
| Terminal.consume/completeOnFirst | 'a → unit | Sink/termination primitives. |

#### Accumulation Semantics
- Maintains:
  - `working` map until threshold reached → emits `Batch`
  - `seen` set to avoid duplicate keys
  - `Done set` when all root scalar keys discovered
- Residual keys (those not yet emitted in a final batch) stored until domain complete.

#### Projection Semantics
- Scalars: all root non-object properties (one event).
- Objects: root properties whose value is an object (one event).
- Order enforced: Scalars before Objects.

---

## 5. JSON Assembly Layer

Abstracts parsing strategy behind a stable interface.

### IJsonAssembler
```
type AssemblerStatus<'a> = StatusNeedMore | StatusComplete of 'a | StatusError of exn
type IJsonAssembler<'a> =
  abstract Feed : ReadOnlyMemory<byte> -> AssemblerStatus<'a>
  abstract Completed : bool
```

### Implementations
- **LengthAssembler**: Requires known full length, accumulates all, then parses.
- **FramingAssembler**: Structural scanning (braces/brackets + string handling) to detect document boundary without length.
- **StreamingMaterializeAssembler**:
  - Incrementally scans tokens with `Utf8JsonReader` only to detect root completion.
  - Parses full buffer *once* at completion (ensures canonical equality with single parse).
  - Avoids incremental JTokenWriter complexity (eliminated earlier edge-case writer exception).
- **InstrumentedStreamingMaterializeAssembler**: Adds depth/structural counters for invariants, while still materializing (used only in tests).

### Adapter
`AssemblerProcessor.create` wraps any `IJsonAssembler<JToken>` into a `StreamProcessor<ReadOnlyMemory<byte>, JToken>` cooperating with pipeline stepping.

---

## 6. Pipelines

### ParseMode
```
LengthGate | Frame | StreamMaterialize | StreamTokens (reserved / future)
```

### DirectPipeline
Stages: Source → Parser → (Accum?) → (Projection?) → (Sink?)

### ChunkPipeline
Stages: Source → ToBytes → Chunker → Parser (assembler processor) → (Accum?) → (Projection?) → (Sink?)

### Execution Harness (`PipelineExec`)
State objects:
- `DirectRuntimeState` (flags for SourceDone, Parsed, AccumDone, Projection phases).
- `ChunkRuntimeState` (SourceDone, ToBytesRun, ChunkingDone, Parsed).

`stepDirect` & `stepChunk` implement “drive one step” semantics, returning coarse progress labels.

### Planned `PStream` (Phase 5 / Tier-C)
Not yet integrated: would add `TokenReader` stage emitting `TokenEvent` + streaming accumulation/projection.

---

## 7. Core Integration Machine

Central file: `CoreIntegrationMachine.fs`

### Model Entities
- `Scenario = Direct | Chunking`
- `DocumentRuntime = DirectDoc | ChunkDoc`
- `DocumentModel` captures:
  - Seed, JSON text, parsed root token
  - Classification (root scalar keys)
  - Optional Accum & Projection model state (pure state for invariants)
  - Chunk metadata (total bytes, consumed, parsed flag)
  - Completed flag

### SUT (System Under Test)
Holds a `ConcurrentDictionary<int, RuntimePipeline>` mapping document id to active pipeline runtime states.

### Commands
```
StartDirect | StartChunk | StepSource | StepChunkFeed
| StepAccum | StepProjection | MarkComplete | NoOp
```
(Accum/Projection “stepping” currently stubbed; actual feed occurs indirectly.)

### Operation Semantics
- `.Run` mutates the pure model (assumes heuristics for parse completion).
- `.Check` performs real pipeline stepping effects via harness and validates invariants after each command.

### Invariants (Local)
- Chunk doc cannot be parsed before all bytes consumed (guard).
- Accum coverage: if Done then union of batches + residual keys equals root scalar domain.
- No overlapping keys across batches (disjointness enforced in higher-level tests).

---

## 8. Higher-Level Integration Properties

File: `CoreIntegrationTests.fs`

Adds a second layer of properties that re-run invariants independently of the machine’s embedded checks, plus aggregates richer classification labels for coverage insight:

Key Additional Invariants:
- Source uniqueness & structural parse equality with stored JSON.
- Chunk consumption counters plausible.
- Accum union subset/equality rules + thresholds.
- Projection ordering.
- Completion implies parse & source emission.
- Monotonic properties (e.g., accumulation union never shrinks).

### Classification Labels
Generated to observe distribution:
- Scenario mix (direct vs chunking)
- Document counts
- Completion coverage
- Accumulation state (none|progress|mixed|allDone)
- Projection state (none|declared-none|scalarsOnly|full|mixed)
- Chunk size style (micro|small|medium|large)
- Scalar key cardinality buckets
- Coverage ratio buckets (0%, partial ranges, 100%)

These classification labels are attached with `Prop.classify` to help detect generation skew early.

### Command Script Generation
Custom script builder `ScriptGen.buildScript`:
- Iteratively picks next command based on evolving model (guided random).
- Combined with second-pass execution: model-first run, then real runtime side-effects applied.

---

## 9. Streaming Tests (Phase 4)

### CoreStreamingMaterializeTests
Properties:
- Completes once (first completion only counted).
- Parity with direct parse (DeepEquals).
- Robust partition fuzz: ensures a single completion across multiple random partitions; flags no-emission anomalies.

### CoreStreamingErrorTests
Corruption transformations (truncation, injection, structural glyph removal):
- Property allows either early error OR “pending” (no completion & no error) because assembler does not yet have a final-block signal.
- Future improvement: add explicit finalization to force error for truncated endings.

### CoreStreamingInvariantsTests
Instrumented assembler invariants:
- Depth never negative.
- Balanced object/array counts.
- Root completion implies final depth zero.
- Token count > 0 at completion.

---

## 10. Framing Tests

File: `FramingTests.fs` covers structural framing:
- Single completion.
- Parity with direct parse.
- No early completion at a prefix shorter than full document (unless a scalar that legitimately finishes).
- Fuzz partitions across multiple seeds.

Framer ensures length independence and correct detection of root closure.

---

## 11. Token Streaming Prototype

File: `JsonTokenStreaming.fs`:
- `TokenEvent` representation (JsonTokenType, optional value, depth, root termination flag).
- `TokenStreamReader.create()`:
  - Maintains an `ArrayBufferWriter<byte>` and unread slice progression.
  - Enqueues all tokens discovered since last invocation.
  - Emits **one** token per feed (simplifies model reasoning, ready for an `EmitMany` upgrade later).
  - Completion after root terminal token dequeued.

Planned next:
- Streaming accumulation & projection stages fed by `TokenEvent`.
- Materializer using `JTokenWriter` rehydration (future if parity needed in streaming mode).

---

## 12. StreamProcessor Model-Based Specs (Micro-Level)

File: `CoreMachine.fs` (module `CoreMachine` / `StreamProcessorModel`):
- Machines for:
  - Lift processors
  - Filter processors
  - Stateful batch accumulator variants
  - Environment-aware processors (`withEnv`)
  - Complex composite pipeline (filter → transform → accumulate)
- Commands:
  - ProcessEnvelope
  - ProcessWithTimeout
  - ProcessWithCancellation
- State tracked: inputs processed, outputs emitted, consume counts, completion, errors.
- Properties validate distribution of outcomes, law-like constraints on use-time semantics, probabilistic failure tolerances.

### ErrorScenarios Module
- Failing processor (probabilistic & deterministic seeded).
- Delayed processor (async).
- Complex accumulator with emitted average & resets.

---

## 13. Flow Monad & Functional Law Tests

File: `CoreTests.fs`:
- Functor identity & composition
- Applicative identity
- Monad laws (left identity, right identity, associativity)
- Timeout behavior (withTimeout success/failure)
- Exception capture (Flow.catch)
- Resource finalization (tryFinally)
- Environment scoping (ask/local)
- Additional fairness tests for envelope mapping and metadata preservation.

---

## 14. Additional Unit & Domain Tests

- Envelope creation & structural attribute preservation.
- Stateful sum “mini model” verifying stateful processors align with comment-model invariants.
- JSON generator smoke test (prints examples) in `CoreStreamProcessingTests`.

---

## 15. Error / Failure Scenario Testing

- Corruption injection for streaming assembler.
- Random failure rates & deterministic seeds for failing processors (distribution validation).
- Timeout / cancellation pathways.

---

## 16. Architectural Testing Patterns & Conventions

| Pattern | Where Used | Purpose |
|---------|------------|---------|
| Pure Model + Side-effect `Check` | Integration machine | Deterministic model reasoning + real runtime validation |
| Instrumentation Layer | Instrumented assembler | Depth & structural invariant extraction without production pollution |
| Classification Labels | Integration properties | Distribution introspection (anti-skew) |
| Scripted Command Sequences | ScriptGen | Scenario exploration with adaptive branching |
| FsCheck Machines | StreamProcessorModel | Systematic multi-operation state exploration |
| Negative Fuzz (Corruption) | Error tests | Robustness & failure detection |
| Law Testing | Flow & Envelope | Algebraic correctness of core combinators |

---

## 17. Extending / Customizing Tests

### Adding a New Parse Mode
1. Implement an `IJsonAssembler<JToken>` variant.
2. Extend `ParseMode` DU.
3. Modify `PipelineBuilder.chunked` parser selection.
4. Add focused property tests:
   - Completion semantics
   - Parity (if outputs JToken)
   - Early completion safety
5. (Optional) Add instrumentation if structural invariants needed.

### Adding Streaming Tier-C
1. Introduce `Pipeline.PStream` & `StreamingPipeline` record.
2. Provide builder (remove `failwith` placeholder).
3. Add Machine support:
   - New `Scenario` variant OR annotate existing docs with parse mode.
   - New commands: `StepToken`, `StepStreamAccum`, `StepStreamProjection`.
4. Add invariants:
   - Token depth progression monotonic non-negative.
   - RootTerminal single occurrence.
   - Accumulation monotonic coverage (partial domain).
   - Materialized JToken (if enabled) equals concatenated root parse.
5. Extend properties and classification labels for streaming metrics (token count buckets, token-to-byte ratio).

### Adding Benchmarks (Optional)
(Not included in code snapshot, but earlier plan considered.)
- Benchmark harness comparing Length vs Frame vs StreamingMaterialize on JSON size & chunk count distributions.
- Track allocations & throughput; guide parse mode defaults.

---

## 18. Remaining Tier‑B Actions

| Item | Status | Notes |
|------|--------|-------|
| FramingAssembler integrated | DONE | Mode selectable via ParseMode.Frame |
| Default switch to Frame (optional) | PENDING | Currently LengthGate is conservative default |
| Additional framing edge tests (very deep nesting) | PENDING | Could add explicit max-depth stress property |
| Multi-document concatenation (Frame reuse) | DEFERRED | Requires assembler reset semantics |

---

## 19. Tier‑C Roadmap Preparation

| Step | Goal | Prerequisites |
|------|------|---------------|
| PStream DU + builder | Allow token pipelines | TokenEvent confirmed stable |
| Streaming accumulation | Incremental domain coverage | Token depth / property name sequencing |
| Streaming projection | Early partial projections | Formal event ordering invariants |
| Materializer stage | Optional JToken for parity | JTokenWriter or parallel assembler path |
| Machine extension | Unified doc model supporting streaming states | Extended runtime union or second dictionary |
| Invariants upgrade | Token-level safety + domain consistency | Instrumentation approach patterned from current |
| Performance validation | Confirm streaming competitive | Add microbench posts vs existing parsers |

---

## 20. Quick Reference Summary

| Layer | Key Files |
|-------|-----------|
| Core Runtime | `CoreTests.fs`, `CoreMachine.fs`, `Generators.fs` |
| Stages | `IntegrationStages.fs` |
| Assemblers | `CoreJsonStreamProcessors.fs` (or `JsonStreamProcessors.*`) |
| Pipelines | `CoreIntegrationPipeline.fs` (plus future streaming file) |
| Machine (Model-Based) | `CoreIntegrationMachine.fs`, `CoreIntegrationTests.fs` |
| Streaming Tests | `CoreStreamingMaterializeTests.fs`, `CoreStreamingInvariantsTests.fs`, `CoreStreamingErrorTests.fs` |
| Framing Tests | `FramingTests.fs` |
| Token Streaming Prototype | `JsonTokenStreaming.fs` |
| Auxiliary Integration Props | `CoreIntegrationTestSuite` (classification) |
| Unit & Law Tests | `CoreTests.fs`, `CoreStreamProcessing.fs` |

---

## Appendix: Design Principles Recap

- **Isolation of Complexity**: Parsing polymorphism isolated behind `IJsonAssembler`.
- **Model Purity**: Mutations only inside operations; model state always functional immutable.
- **Idempotent Completion**: Assemblers safe to feed after completion.
- **Opt-In Evolution**: New modes introduced without breaking previous invariants until explicitly switched.
- **Test Layer Stratification**: Unit → Property → Model-Based → Instrumented / Negative.
- **Traceability**: Seeds & classification labels enable fast reproduction & distribution diagnostics.

---

## Suggestions for Immediate Next Steps

1. Decide if Frame should become the default `ParseMode` for chunked pipelines (flip default + add regression property).
2. Implement `PStream` DU early with a minimal token pass-through to unblock Tier‑C refactors incrementally.
3. Introduce a “finalize” method / artificial final block signal for StreamingMaterialize to tighten corruption test semantics (currently permissive).
4. Add optional benchmarking harness (BenchmarkDotNet) to measure parse mode trade-offs before expanding token streaming stages.
5. Begin streaming accumulation prototype with shallow root scalar extraction (reusing current accumulation semantics but event-driven).

---

Please refer back to this document when extending Tier‑B or entering Tier‑C phases.  
If you need a companion diagram or a performance benchmarking scaffold next, let me know and I can generate it.
