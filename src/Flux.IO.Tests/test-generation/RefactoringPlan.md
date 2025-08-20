# Tier‑B (Structural Framing) & Tier‑C (Incremental Token Streaming) Design Plan

Status Baseline (Current Main)
- Parser implementation: `Parser.chunkReassembly expectedLength` collects all bytes and parses once.
- Origin JSON is always available (source produces full string; chunk path only simulates fragmentation).
- Accumulation & Projection stages expect a full `JToken`.
- Machine & tests rely on a single parse emission (`Parsed = true` toggled once).
- No token streaming semantics; invariants rely on final parsed object only.

Objective
Introduce progressively richer parsing behaviors (Tier B → Tier C) with **minimal disruption**, preserving existing semantics for current tests while enabling:
- Tier B: Length‑independent structural framing to detect end of a JSON document without a priori byte length.
- Tier C: True incremental token streaming (token events as they are read) and optional early downstream processing.

---

## 1. Guiding Principles

| Principle | Rationale |
|-----------|-----------|
| Layered Intro | Avoid broad refactor; each phase keeps passing tests. |
| Interface First | Introduce a unifying abstraction so existing code compiles unchanged when switching parsers. |
| Backwards Compatibility | Default pipeline behavior should remain identical until the new modes are deliberately selected. |
| Opt‑In Complexity | Test code & invariants remain valid for old pipelines; new tests added alongside. |
| No Premature Downstream Refactor | Initially wrap streaming parser to still emit a *single* `JToken` unless a streaming mode is specifically requested. |
| Replace `expectedLength` Dependency | Provide new parser modes without requiring the preview JSON generation hack to size chunking. |

---

## 2. High-Level Architecture Evolution

### Phase Abstraction Diagram (Text)

```
Existing
  Chunker -> chunkReassembly(expectedLength) -> JToken

Phase 1 (Interface)
  Chunker -> AssemblerProcessor(IJsonAssembler<JToken>) -> JToken

Phase 2 (Framer)
  Chunker -> FramingAssembler -> JToken

Phase 3 (Hybrid Tests)
  + property tests for framing invariants

Phase 4 (Streaming Tokens Internal)
  Chunker -> StreamingTokenAssembler -> TokenEmitter -> TokenMaterializer(JTokenWriter) -> JToken

Phase 5 (Full Tier C External)
  Chunker -> StreamingTokenAssembler -> TokenPipeline...(AccumStreaming | ProjectionStreaming | DocumentMaterializer)
```

---

## 3. Core Abstractions

### 3.1 IJsonAssembler (Document Output)

```fsharp
type AssemblerStatus<'a> =
  | NeedMore
  | Complete of 'a
  | Error of exn

type IJsonAssembler<'a> =
  abstract Feed : ReadOnlyMemory<byte> -> AssemblerStatus<'a>
  abstract Completed : bool
```

### 3.2 Token-Level (Tier C)

Two staged abstractions:

```fsharp
type TokenEvent =
  { TokenType : Newtonsoft.Json.JsonToken
    Value     : obj
    Depth     : int // after applying token
    EndOfDocument : bool }

type IIncrementalJsonReader =
  abstract Feed : ReadOnlyMemory<byte> -> Result<TokenEvent list, exn>
  abstract IsDocumentComplete : bool
```

We will provide:

| Implementation | Purpose |
|----------------|---------|
| `LengthAssembler` | Wrap current length-based approach (Phase 1) |
| `FramingAssembler` | Structural framing (Phase 2) |
| `StreamingAssembler` | Emits tokens; used internally to materialize a final document initially (Phase 4) |
| `TokenPassThroughAssembler` | Directly feeds tokens to downstream streaming pipeline (Phase 5) |

---

## 4. Phased Rollout Plan

| Phase | Goal | Code Additions | Changes to Existing Code | Test Additions | Risk |
|-------|------|----------------|--------------------------|----------------|------|
| 0 | Baseline reference (already) | – | – | Current invariants | Low |
| 1 | Introduce `IJsonAssembler` & adaptor | `JsonAssembly.fs` with `LengthAssembler` + `mkAssemblerProcessor` | Replace direct calls to `chunkReassembly` in builder with adaptor (but still using old code internally) | Sanity property: behavior unchanged | Low |
| 2 | Add `FramingAssembler` | `JsonFramer.fs` | Option flag in `PipelineBuilder.chunked` to choose parser mode | New properties: framing completion, single emission without length | Medium |
| 3 | Remove length dependency from chunk builder (optional) | Use framing path by default; length path kept for regression | Builder conditional default switch | Properties check that preview JSON length not required | Low |
| 4 | Add internal token streaming (still returns final `JToken`) | `JsonStreaming.fs` (token reader using `JsonTextReader`) + `StreamingAssembler` + materializer wrapper | Add parser mode `ParseMode.StreamMaterialize` | Token sequence ordering property (internal) | Medium |
| 5 | Full streaming pipeline variant (Tier C external) | New pipeline variant & DU extension: `Pipeline.PStream` etc. New stages (AccumStreaming, ProjectionStreaming) | Extend Machine & test model with token-level states | New invariants: prefix validity, depth balancing, early accumulation correctness | High |
| 6 | Refine Accum/Projection streaming logic | Domain incremental processors | Adapt invariants for partial domain exposure | Extended properties | Medium |
| 7 | Performance / stress harness | Bench test harness | Optional | Throughput/Memory assertions | Medium |

---

## 5. Detailed Phase Specs

### 5.1 Phase 1 — Interface Layer

Add file `JsonAssembly.fs`:

```fsharp
module JsonAssembly

open System
open Newtonsoft.Json.Linq

type AssemblerStatus<'a> = NeedMore | Complete of 'a | Error of exn

type IJsonAssembler<'a> =
  abstract Feed : ReadOnlyMemory<byte> -> AssemblerStatus<'a>
  abstract Completed : bool

type LengthAssembler(expectedLength:int) =
  let acc = System.Collections.Generic.List<byte>(expectedLength)
  let mutable doneFlag = false
  interface IJsonAssembler<JToken> with
    member _.Feed chunk =
      if doneFlag then Complete (JValue(null) :> JToken) // ignored; Completed true
      else
        acc.AddRange(chunk.ToArray())
        if acc.Count = expectedLength then
          try
            let json = Text.Encoding.UTF8.GetString(acc.ToArray())
            let tok = JToken.Parse json
            doneFlag <- true
            Complete tok
          with ex -> Error ex
        else NeedMore
    member _.Completed = doneFlag
```

Adapter stage:

```fsharp
let mkAssemblerProcessor (asm: IJsonAssembler<JToken>) : StreamProcessor<ReadOnlyMemory<byte>, JToken> =
  let completed = ref false
  StreamProcessor (fun env ->
    flow {
      if !completed then return Complete else
      match asm.Feed env.Payload with
      | NeedMore -> return Consume
      | Complete tok ->
          completed := true
          return Emit (mapEnvelope (fun _ -> tok) env)
      | Error ex ->
          return Error ex
    })
```

Builder changes:
- Add a discriminated union:

```fsharp
type ParseMode = LengthGate | Frame | StreamMaterialize | StreamTokens
```

- Extend `chunked` function with optional `parseMode` parameter (default `LengthGate` to preserve behavior).
- For `LengthGate` create `LengthAssembler(totalBytes)` and adapt.
- Keep existing `chunkReassembly` temporarily for regression (or mark deprecated).

### 5.2 Phase 2 — Structural Framer

File: `JsonFramer.fs`

Framer state:

```fsharp
type FrameState = {
  Depth:int
  InString:bool
  Escaped:bool
  Started:bool
  Complete:bool
}
```

Feed algorithm: scan only newly appended chunk; update state (as in prior analysis). Completion logic:

- If `Depth = 0` and `Started = true` and not `InString` and we have consumed at least one structural boundary -> mark complete.
- Post completion: attempt parse once; return `Complete tok` or `Error ex`.

Assembler Implementation:

```fsharp
type FramingAssembler() =
  let buf = System.Buffers.ArrayBufferWriter<byte>()
  let mutable st = initialState
  let mutable doneFlag = false
  interface IJsonAssembler<JToken> with
    member _.Feed chunk =
      if doneFlag then Complete (JValue(null) :> JToken)
      else
        buf.Write chunk.Span
        st <- advance st chunk.Span
        if st.Complete then
          try
            let json = Text.Encoding.UTF8.GetString(buf.WrittenSpan)
            let tok = JToken.Parse json
            doneFlag <- true
            Complete tok
          with ex -> Error ex
        else NeedMore
    member _.Completed = doneFlag
```

Builder `ParseMode.Frame` uses `FramingAssembler()` instead of length gating; remove dependency on `expectedLength`.

### 5.3 Phase 3 — Tests for Framer

Add properties:

| Property | Description |
|----------|-------------|
| `frameCompletesExactlyOnce` | Asserts assembler returns one Complete after at least one chunk and subsequent feeds yield Completed behavior. |
| `frameStructuralBalance` | For every Complete, re-parse JSON and verify parentheses/braces balanced via a secondary scan. |
| `frameLengthAgnostic` | Ensure difference between total bytes and sum of chunk sizes does not matter (simulate unknown final length by not precomputing). |

Modify machine minimally: add a flag in model to note parse mode (optional).

### 5.4 Phase 4 — Streaming Parser (Materializing)

Implement `StreamingAssembler`: wraps a `JsonTextReader` reading from an internal growing `MemoryStream` or a custom feeder.

Simplify via:
- Append bytes to `BufferBlock<byte[]>` style memory (but synchronous): append chunk to an internal `List<byte[]>`; maintain an index pointer consumed by `JsonTextReader` via a custom `Stream` implementation (`FeedingStream`).
- Feed loop: after writing chunk, call `reader.Read()` producing zero or more `JsonToken`s; write them into a `JTokenWriter`; detect root completion when writer's internal stack unwinds to zero post first root start.

Return statuses:
- `NeedMore` until root complete.
- `Complete finalJToken` once done.

This maintains existing downstream contract (`JToken` only).

### 5.5 Phase 5 — Full Streaming Mode (Tokens Out)

Extend pipeline DU:

```fsharp
type StreamingPipeline = {
  Source : StreamProcessor<unit,string>
  ToBytes: StreamProcessor<string, ReadOnlyMemory<byte>>
  Chunker: StreamProcessor<unit, ReadOnlyMemory<byte>>
  Reader : StreamProcessor<ReadOnlyMemory<byte>, TokenEvent>
  Materialize : StreamProcessor<TokenEvent, JToken> option // optional
  AccumStreaming : StreamProcessor<TokenEvent, AccumEvent> option
  ProjectionStreaming : StreamProcessor<TokenEvent, ProjectionEvent> option
}
type Pipeline =
  | PDirect of DirectPipeline
  | PChunk  of ChunkPipeline
  | PStream of StreamingPipeline
```

Streaming variant specifics:
- `Reader` emits `TokenEvent` (can be multiple per chunk: use `EmitMany`).
- We add a thin `TokenMaterializer` stateful stage (wraps `JTokenWriter`) to rebuild the full document if user wants document-based downstream tests.
- Accum/Projection streaming modes either:
  - (First iteration) accumulate only when encountering PROPERTY_NAME + VALUE tokens at depth=1.
  - (Later) allow incremental domain exposure with invariants checking monotonic coverage.

### 5.6 Phase 6 — Adapt Machine & Model

Model Additions:
- `ParseMode` stored per doc.
- For streaming docs:
  - `TokenCount`
  - `CompletedTokenStream`
  - `RootDepth` invariants (never negative; ends at zero after completion).
  - `DomainCoverageSoFar`.

Operations:
- Add `StepStreamChunk`, `StepTokenMaterialize`, `StepTokenAccum`, etc.
- Extend invariants with:
  - Token depth never negative.
  - Final depth zero when stream complete.
  - If a materialized document exists → structural equality with accumulation of token sequence.

### 5.7 Phase 7 — Performance / Memory (Optional)

Bench harness measuring:
- Peak allocations (GC stats) between length gating vs framing vs streaming.
- Throughput (#docs per second).
(Only after correctness locked.)

---

## 6. Backward Compatibility Strategy

| Concern | Mitigation |
|---------|------------|
| Existing tests expecting `Parsed=true` once | In ParseMode.Frame & LengthGate produce single `Emit` as now. |
| Machine logic referencing only two pipeline shapes | Add optional branch; default builder calls unchanged unless `parseMode` parameter provided. |
| Invariants anchored to `ChunkingData.Parsed` | For streaming tokens w/o materialization maintain a boolean flagged true when root completed. |
| Complexity contamination of simple scenarios | Keep default `ParseMode.LengthGate` for current pipeline builder. |

---

## 7. New Invariants (Additive)

### Tier B (Framer)
1. Framer emits exactly one `Complete`.
2. Balanced braces/brackets when `Complete`.
3. `Complete` only after at least one structural closing that returns depth to zero.

### Tier C (Token Streaming)
1. Token depth progression: `depth >= 0` always, final depth == 0 at document end.
2. Property tokens inside root object: every `PropertyName` token followed by exactly one value or start (object/array) token at same depth.
3. Materialized `JToken` (if used) deep-equals parse of concatenated JSON text from token writer.
4. Accumulated root scalar keys monotonic & subset of final root keys.
5. No extra tokens after `EndObject` / `EndArray` closing the root.

---

## 8. Test Plan Expansion

| Phase | Tests |
|-------|-------|
| 1 | Regression: existing properties all pass using new assembler path. |
| 2 | Add `frameCompletesOnce`, `frameNoEarlyComplete`, `frameMatchesParsedDocument`. |
| 3 | Random chunk partitions (fuzz split) property simulating every partition of a small JSON (limit size to avoid explosion). |
| 4 | `tokenSequenceReconstructsDocument` (materialized). |
| 5 | `tokenDepthInvariant`, `tokenNoPostCompletionEmission`, `streamAccumCoverageMonotonic`. |
| 6 | Combined property: both streaming and non-streaming docs in same command script. |

---

## 9. Risk Assessment & Mitigation

| Risk | Phase | Mitigation |
|------|-------|------------|
| Off-by-one depth errors in framer | 2 | Unit tests for handcrafted JSON (strings with braces). |
| Incorrect scalar detection for streaming accum | 5 | Initially limit coverage to root depth=1; assert no deeper extraction until extended. |
| Performance regression due to repeated buffer copies | 4 | Use `ArrayBufferWriter<byte>` or pooled arrays; measure before optimizing. |
| Complexity creep in Machine | 5+ | Isolate streaming doc runtime state in a new record (do not overload existing `ChunkRuntime`). |
| Test flakiness (token order) | 4 | Rely on `JsonTextReader` which guarantees order; add deterministic seeds. |

---

## 10. Implementation Sequence (Concrete Commit Checklist)

1. Commit A: Add `JsonAssembly.fs` (interfaces + length assembler + adapter). Switch builder; keep old parser as fallback (deprecated).
2. Commit B: Add `ParseMode` DU + builder optional param; default = LengthGate.
3. Commit C: Add `FramingAssembler` + tests (frame invariants). Add new enum value `Frame`.
4. Commit D: Make `Frame` the new default for chunked pipelines (optional; behind flag).
5. Commit E: Add `StreamingAssembler` (token reader) but wrap in materializer to still yield `JToken`. Mode `StreamMaterialize`.
6. Commit F: Tests for token sequence reconstruction; add new property grouping.
7. Commit G: Extend pipeline DU with streaming variant, add token event stage & model fields, operations & invariants (mode `StreamTokens`).
8. Commit H: Add streaming accumulation & projection prototypes.
9. Commit I: Expand invariants coverage; integrate into main test list.
10. Commit J: Performance harness (optional).
11. Commit K: Documentation update (README/TestPlan) summarizing modes.

---

## 11. Minimal Code Skeletons (Illustrative Only)

### `ParseMode` Integration

```fsharp
type ParseMode =
  | LengthGate
  | Frame
  | StreamMaterialize
  | StreamTokens
```

Builder snippet:

```fsharp
let chunked (seed, withAccum, withProjection, sink, chunkSizer, ?parseMode) =
  let mode = defaultArg parseMode ParseMode.LengthGate
  // ...
  let parserStage =
    match mode with
    | LengthGate ->
        let asm = LengthAssembler(allBytes.Length) :> IJsonAssembler<JToken>
        mkAssemblerProcessor asm
    | Frame ->
        let asm = FramingAssembler() :> IJsonAssembler<JToken>
        mkAssemblerProcessor asm
    | StreamMaterialize ->
        let asm = StreamingAssembler(materialize=true) :> IJsonAssembler<JToken>
        mkAssemblerProcessor asm
    | StreamTokens ->
        failwith "Use streaming pipeline builder"
```

### Streaming Reader Simplified (Pseudo)

```fsharp
type StreamingAssembler(materialize: bool) =
  let ms = new System.IO.MemoryStream()
  let sr = new System.IO.StreamReader(ms, Text.Encoding.UTF8, false, 1024, true)
  let jr = new Newtonsoft.Json.JsonTextReader(sr)
  let writer = if materialize then Some (new Newtonsoft.Json.Linq.JTokenWriter()) else None
  let mutable complete = false

  interface IJsonAssembler<JToken> with
    member _.Feed chunk =
      if complete then Complete (writer |> Option.map (fun w -> w.Token) |> Option.defaultValue (JValue(null) :> JToken)) else
      ms.Seek(0L, System.IO.SeekOrigin.End) |> ignore
      ms.Write(chunk.Span)
      ms.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
      try
        let rec readLoop () =
          if jr.Read() then
            match writer with
            | Some w -> w.WriteToken(jr, writeChildren=true)
            | None -> ()
            if jr.Depth = 0 && jr.TokenType = Newtonsoft.Json.JsonToken.EndObject
               || jr.Depth = 0 && jr.TokenType = Newtonsoft.Json.JsonToken.EndArray then
                complete <- true
                Complete (writer |> Option.map (fun w -> w.Token) |> Option.defaultValue (JValue(null) :> JToken))
            else readLoop ()
          else NeedMore
        readLoop ()
      with ex -> Error ex
    member _.Completed = complete
```

(Refine for arrays/scalars; manage stream re-read incremental consumption by implementing a custom feeding stream to avoid rewinding.)

---

## 12. Open Design Questions (Deferred Decisions)

| Question | Defer To | Notes |
|----------|----------|-------|
| Support multi-document concatenated streams? | Post Tier C | Requires framer reset or multi-root token iteration. |
| Token backpressure (large docs) | Performance pass | Possibly emit batches of tokens with `EmitMany`. |
| Partial incremental accumulation (mid-stream invariants) | Tier C extension | Add per-token domain extraction. |
| Memory pooling strategy | Perf Harness | Only optimize if profiling justifies. |
| Error recovery (malformed JSON) | Optional resilience layer | Currently fails fast returning `Error`. |

---

## 13. Summary

The plan introduces *parsing mode polymorphism* through a narrow assembler interface which:
- Enables seamless swapping of implementations (`LengthAssembler` → `FramingAssembler` → `StreamingAssembler`).
- Isolates complexity, keeping pipeline shape stable for existing consumers.
- Allows stepping from “simulate chunking” to “real framing” to “true token streaming” without breaking incremental test investments.

By postponing external token streaming exposure until Phase 5, we amortize complexity and maintain a green test suite at each stage.

---

## 14. Next Action Recommendation

Proceed with **Phase 1 + 2** (Interface + Framer) in a single PR:
1. Add interface & length assembler.
2. Add framer assembler & mode switch.
3. Add new framing properties.

Once merged & stable, schedule Phase 4 (internal token streaming) before exposing full token pipelines.

Let me know when you are ready for the Phase 1/2 scaffolding code and I will generate the exact F# modules.

---