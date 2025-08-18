# StreamProcessor Enhanced Integration Test Plan

## Overview

We introduce a model-based + property-based integration test suite for the `StreamProcessor` abstraction, focused on realistic multi-stage stream topologies that exercise:

1. Incremental / chunked ingestion of JSON documents (simulated async I/O)
2. Asynchronous parsing and staged emission of JSON payloads
3. Stateful accumulation of scalar attributes (threshold–gated forwarding)
4. K/V persistence side-effects (simulated in-memory stores)
5. Interleaving of multiple in-flight documents (future extension hooks)
6. Pipeline composition invariants (preservation, completeness, determinism under pure semantics)

All scenarios terminate at a **terminal node that produces JSON data generated via `JsonGenerators`** (as required).

The suite is driven by an FsCheck model machine whose `.Next` operation **derives the next command *from the current model state*** (per the requirement `model-use-in-machine.Next`), rather than sampling completely uninformed random actions. This shrinks better, avoids illegal transitions, and increases semantic coverage.

## Scenario Classes & Weights

| Scenario Class          | Purpose / Focus                                                          | Required Weight | Applied Weight |
|-------------------------|---------------------------------------------------------------------------|-----------------|----------------|
| Chunking                | Chunk boundary correctness, deferred emission, async parsing             | ≥30%            | 35%            |
| Accumulation            | Threshold-based emission, dictionary domain integrity, dependency check  | ≥20%            | 25%            |
| Direct (Baseline)       | Control / reference (no chunking, no accumulation)                       | —               | 30%            |
| Hybrid / Stress (opt.)  | Composition of Accumulation + Chunking (extensible hook stub)            | (future)        | 10% (reserved) |

(Implementation currently activates: Chunking / Accumulation / Direct; Hybrid hook stub retained for extension.)

## Pipeline Logical Stages

Each scenario builds a logical pipeline (conceptually stages 1–4). The physical realization in the tests is *explicit stage processors chained manually* (rather than a global runtime) to stay close to core `StreamProcessor` semantics while remaining deterministic and shrink-friendly.

### Common Stage Definitions

1. Stage 1 – JSON Generation
   - Uses `JsonGenerators.genJson`
   - Produces a root JSON string (object or array), immediately parsed (for model oracle) into `Newtonsoft.Json.Linq.JToken`
   - Pre-computes:
     - Root token
     - Top-level scalar properties
     - Top-level object-valued properties
     - Set / multiset of keys (for accumulation invariants)

2. Stage 2 – Chunking Node (Chunking Scenario Only)
   - Splits raw UTF-8 bytes into variable-sized chunks
   - Each call to the chunk processor emits *at most one* chunk (`Emit`) or `Complete` once exhausted
   - No premature emission of logical JSON token(s)
   - Simulated asynchronous behavior: random micro-delays and occasional `Consume`

3. Stage 3 – Async Parser
   - Accumulates bytes
   - Attempts parse only when model predicts completeness (in test: full-document parse on final chunk)
   - Emits either:
     - `Consume` (need more bytes)
     - `Emit` of a single envelope containing the finalized JSON token
   - Artificial async delay to simulate I/O-bound parsing

4. Stage 4 – Persistence / Accumulation
   - Chunking Scenario:
     - Accepts `JToken` (usually `JObject`)
     - Visits first-level properties:
       - Scalar properties → HashMap<string,obj> (scalars store)
       - Object-valued properties → HashMap<string,string> (object store; stringified flattened objects)
     - Emits *original JSON string* (terminal payload contract)
   - Accumulation Scenario:
     - Ingestion of a full JSON token (bypassing chunking) → internal property enumeration streamed logically
     - Accumulates scalar properties into a working HashMap<string,obj>
     - Emits batches (dictionary snapshot) once threshold (#unique keys) reached
     - Downstream sink records each emitted domain (set of keys)
     - Final flush (if residual keys) still meets invariants (unless configured to disallow partial flush — property explores both)
     - Terminal stage ultimately emits canonical JSON string
   - Direct Scenario:
     - Single-pass: JSON string → parse → immediate classification + emission

5. Terminal Sink
   - Always yields an `Envelope<string>` whose payload is a JSON string (requirement compliance)
   - Collects per-document emission facts for final model reconciliation

## Model State

Per active document (identified by synthetic `DocId`):

```
type DocumentState = {
  DocId              : int
  Scenario           : ScenarioKind
  OriginalJson       : string
  RootToken          : JToken
  Utf8Bytes          : byte[]
  Chunks             : byte[] list
  NextChunkIndex     : int
  ParsedEmitted      : bool
  ScalarStore        : HashMap<string,obj>      // Stage 4 (chunking / direct)
  ObjectStore        : HashMap<string,string>
  Accumulator        : HashMap<string,obj>      // (accumulation only)
  AccumulatedBatches : HashSet<string> list     // key domains per batch
  AccumThreshold     : int option               // accumulation threshold
  FinalJsonEmitted   : bool
  Completed          : bool
}
```

Global model:

```
type Model = {
  ActiveDocs  : Map<int, DocumentState>
  NextDocId   : int
  RngSalt     : int
  Emissions   : int
}
```

## Operations (Commands)

| Operation                       | Preconditions (Derived from State)                              | Effect (Model)                                                  | Effect (SUT) |
|--------------------------------|------------------------------------------------------------------|-----------------------------------------------------------------|--------------|
| StartDocument(scenario)        | Always (probabilistic weighted selection)                       | Adds new DocumentState                                          | Alloc + parse oracle + prepare chunks |
| FeedChunk(docId)               | Scenario=Chunking; chunk(s) remaining; not ParsedEmitted        | Advances NextChunkIndex                                         | Run chunk processor + parser attempts |
| ParseIfComplete(docId)         | Chunking; last chunk consumed; not ParsedEmitted                | Marks ParsedEmitted true                                        | Parser processes accumulated bytes     |
| DirectProcess(docId)           | Direct scenario; not ParsedEmitted                              | Marks ParsedEmitted true                                        | Runs direct pipeline                   |
| AccumulateStep(docId)          | Accumulation; not FinalJsonEmitted                              | Updates Accumulator / Batches (when threshold)                  | Processes properties logically         |
| FlushAccumulation(docId)       | Accumulation; residual keys > 0; safe flush condition           | Moves residual keys to final batch                              | Emits final batch                      |
| PersistFinalize(docId)         | ParsedEmitted && not FinalJsonEmitted                           | Marks FinalJsonEmitted true                                     | Emits terminal JSON string             |
| CompleteDocument(docId)        | FinalJsonEmitted && not Completed                               | Completed=true                                                   | No-op / sentinel                       |
| RandomNoOp / TimeoutTick       | Used to vary interleavings (injects scheduling noise)           | No change                                                       | Possibly triggers async completions    |
| StartAnotherOrFinish           | Model-driven termination bias                                   | Possibly starts more docs or leads towards shrinkable finish    | —            |

## Weighted Selection Strategy in `.Next`

The machine builds a dynamic list of *enabled operations* given the current model. It then applies:

1. Scenario introduction weights (Chunking 0.35 / Accumulation 0.25 / Direct 0.30 / HybridStub 0.10)
2. Progress bias: If any document is mid-chunk, prefer `FeedChunk` or `ParseIfComplete` before adding new docs (prevents starvation & ensures forward progress)
3. Cleanup bias: If documents are near completion, push finalization ops to permit invariants to be asserted earlier
4. Diversity bias: Occasionally (low probability) spawn a new document even while others are not finished (forces interleaving coverage)

This satisfies the requirement that the *model steers operation choice*.

## Core Invariants / Properties

1. Chunk Completeness
   - No parser emission (`ParsedEmitted`) before all chunks consumed (`NextChunkIndex = Chunks.Length`)
2. Byte Integrity
   - Concatenation of all provided chunks == original `Utf8Bytes`
3. Scalar vs Object Stores (Chunking / Direct)
   - `ScalarStore` keys correspond only to non-object first-level properties
   - `ObjectStore` keys correspond only to object-valued first-level properties
   - No overlap between key sets
4. Accumulation Threshold
   - Each emitted batch domain size ≥ threshold (except possibly final flush if configured to allow partial)
   - Union of batch domains == set of scalar property keys from original JSON
   - No batch repeats a key
5. Terminal Emission
   - Exactly one terminal JSON string emission per document
   - Emitted string round-trips to same JToken (structural equality via `JToken.DeepEquals`)
6. Determinism (Pure Sections)
   - Given identical JSON string and chunk boundaries, the resulting final stores and emission ordering are invariant across runs (modulo asynchronous scheduling labels)
7. Safety
   - No operation yields `Error` or unexpected `Complete` out of semantic order
8. Progress
   - Every started document eventually reaches `Completed=true` (liveness under generated schedules)
9. Monotonicity
   - `NextChunkIndex` strictly increases; accumulation batch count monotonic; emission count monotonic

## Additional Cross-Cutting Checks

- Property counts vs emission counters
- Shrink friendliness: Model avoids storing large intermediate arrays; retains only necessary oracle structures.
- Labeling:
  - Scenario labels (Chunking / Accumulation / Direct)
  - Size metrics (#chunks, threshold, #props)
  - Phase transitions (ParseComplete, AccBatchEmit, TerminalEmit)

## Async / Interleaving Simulation

- Artificial `Task.Delay` inserted in parser & persistence processors (bounded small delays)
- RandomNoOp operations simulate scheduler ticks so FsCheck explores re-orderings of "inactive" steps vs progression
- Cancellation is *scaffolded* (extension point) but not forced unless we add explicit cancel operations (can be added later without breaking invariants)

## Negative / Edge Cases

- Empty object `{}` (zero scalar/object props) in each scenario
- Deeply nested object where only root-level classification matters
- Large property counts -> ensures multiple accumulation batches
- Single-property documents (threshold > key count) -> tests partial flush logic
- Highly fragmented chunking (1-byte chunks)
- Single-chunk whole-document path

## Extensibility Hooks

- Hybrid scenario (chunk + accumulation by streaming properties before full parse) — left as future extension
- Cancellation / timeout simulation
- Multi-document concurrent interleaving (currently serial doc progression with optional overlap)

## Summary of Provided Artefacts

| Artefact File            | Description |
|--------------------------|-------------|
| `test-plan.md`           | This document (strategy & rationale) |
| `IntegrationModel.fs`    | Pure model + operation definitions + processors & pipeline harness |
| `IntegrationTests.fs`    | Expecto / FsCheck wired properties using the model machine |

The design obeys:
- Functional purity in model state transformations
- Immutable model updates (only SUT internal refs mutate where mandated by existing `StreamProcessor.stateful`)
- Weighted scenario distribution per requirements
- Terminal JSON guarantee for all scenarios
