# Incremental JSON Parsing & Chunk Processing Analysis

## 0. Context Recap

You updated the chunk reassembly parser to:

```fsharp
let chunkReassembly (expectedLength: int) : StreamProcessor<ReadOnlyMemory<byte>, JToken> =
    let acc = System.Collections.Generic.List<byte>(expectedLength)
    let doneFlag = ref false
    StreamProcessor (fun (env: Envelope<ReadOnlyMemory<byte>>) ->
        flow {
            if not doneFlag.Value then
                env.Payload.ToArray() |> acc.AddRange
                if acc.Count = expectedLength then
                    let raw = acc.ToArray()
                    let json = Encoding.UTF8.GetString raw
                    let token = JToken.Parse json
                    doneFlag.Value <- true
                    return Emit (mapEnvelope (fun _ -> token) env)
                else
                    return Consume
            else
                return Complete
        })
```

This now compiles and behaves deterministically given an `expectedLength`.

You asked:
1. How can this know the accumulated bytes form valid JSON?
2. Is a streaming parser (e.g. `JsonTextReader`) required?
3. Would `System.IO.Pipelines` help?
4. How to adapt downstream stages (tokens vs final document)?
5. What is the design impact?

---

## 1. Is Your Assumption Correct?

### Short Answer
- For **our current test harness** (where we *already generate the whole JSON string at the origin*), using the full expected length as a completion condition is pragmatic and valid. We *do* know the document’s full bytes because we constructed them at the source.  
- For a **real streaming source** (socket/file) where total length is unknown or unbounded, this approach is insufficient. You would need either:
  - A framing mechanism (detect end of a JSON value),
  - Or a truly incremental parser (token streaming with push feed).

### When Current Approach Is Acceptable
| Criterion | Current Parser (length-gated) | Real Streaming Need |
|-----------|-------------------------------|---------------------|
| Full JSON known upfront | Yes (we generate) | Not guaranteed |
| Multi-document stream | No (single) | Often yes |
| Early validation / fail fast | No (only at end) | Often desirable |
| Memory usage for large JSON | O(N) | Might be unacceptable |
| Partial downstream processing | Not possible | Sometimes required |

**Conclusion:** Your concern is legitimate for a more realistic pipeline, but *not strictly necessary* to satisfy the present test objectives. However, adding an **optional incremental/framing layer** now can future-proof the integration tests.

---

## 2. Solution Paths (Least → Most Invasive)

| Tier | Name | Description | Pros | Cons | Refactor Impact |
|------|------|-------------|------|------|-----------------|
| A | Length-Gated Aggregation (current) | Accumulate until byte count == expected → parse once | Simple, deterministic, already working | Needs prior knowledge of full length; no early error detection | None |
| B | Structural Framer + Parse | Track JSON structural balance (braces/brackets + string/escape state). Emit complete document bytes, then parse once | No total length required; O(1) streaming memory until boundary discovered | Must implement correct string & escape handling | Moderate (replace length check with framing state) |
| C | Incremental Token Parser | Feed chunks into `JsonTextReader` over a custom streaming `TextReader` or buffer; emit tokens as they become available; build final `JToken` via `JTokenWriter` | True streaming, early error detection, can drive downstream token-wise | Highest complexity; bridging bytes→char; managing partial char sequences; state disposal | Significant; downstream types must accept token streams |
| D | Hybrid: Token Stream + Optional Materialization | Same as C, but downstream decides whether to finalize document or operate token-by-token | Maximum flexibility | Additional protocol complexity (lifecycle events) | High |

Given current goals, **Tier B** is the sweet spot for increased realism with contained scope.

---

## 3. Tier B: Structural Framer Design

### Core Idea
Implement a lightweight *framing parser* that detects when a single top-level JSON value is complete by scanning bytes and maintaining state:

State variables:
- `depth : int` (increments `{` / `[`, decrements `}` / `]`)
- `inString : bool`
- `escaped : bool`
- `started : bool` (first non-whitespace seen)
- `complete : bool`

Completion rule:
- For objects/arrays: `depth = 0` and we have *closed* the starting delimiter.
- For scalars (true/false/null/number/string): detect that token ended (preceded by start boundary and followed by whitespace / EOF). Easiest: fallback to full parse attempt when not inString and `started` and slice parses successfully.

#### Algorithm Outline (chunk feed)
1. Append chunk to buffer.
2. Scan new bytes only.
3. Update `inString`, `escaped`, `depth`.
4. If `depth = 0` and we have consumed at least one balanced top-level structure:
   - Emit a slice (copy or index boundaries).
   - Mark complete.
5. On completion → parse `JToken.Parse(Encoding.UTF8.GetString(buffer))`.

#### Advantages
- No dependency on `JsonTextReader` internals.
- Handles typical well-formed documents.
- Good enough for test environments.

#### Edge Cases To Handle
| Case | Handling Strategy |
|------|-------------------|
| Escaped quotes `\"` | Maintain `escaped` flag |
| Unicode escape `\uXXXX` | Irrelevant to structure (treated inside string) |
| Nested arrays/objects | `depth` algorithm covers |
| Leading whitespace | Skip until first structural or scalar start |
| Trailing whitespace | Allowed post-completion if desired (for multi-doc this would start next doc) |

---

## 4. Tier C: Incremental Token Parser (If Later Needed)

### Components
- A custom `FeedingStream` or a `PipeReader` + `Stream` bridge
- `StreamReader` (UTF-8) with `JsonTextReader`
- Loop:
  - Feed chunk bytes
  - While `reader.Read()` returns `true`, write token to `JTokenWriter`
  - Detect end-of-root via writer stack depth or counting first completed root
- On completion → produce final `JToken`

### Disposal
- On success or unrecoverable error: dispose `JsonTextReader` and underlying stream.
- Provide `reset()` for reuse? (Probably not needed per document parser instance.)

### Pros vs Framer
| Dimension | Framer | Incremental Reader |
|-----------|--------|--------------------|
| Correctness (full JSON compliance) | Good but DIY risks | High (delegated to library) |
| Complexity | Low-Medium | High |
| Token-level granularity | No | Yes |
| Performance for small docs | Comparable | Slight overhead |
| Memory efficiency (very large JSON) | Full copy at end | Streams and incremental build |

---

## 5. Minimal Refactor Strategy (Adopting Tier B)

| Step | Change | Code Impact |
|------|--------|-------------|
| 1 | Introduce `JsonFrameParser` module with `Feed(ReadOnlyMemory<byte>) -> FrameStatus` | New file |
| 2 | Replace current `expectedLength` parser with `framer + accumulator` | Swap implementation |
| 3 | Continue producing `JToken` only once (downstream unchanged) | No downstream changes |
| 4 | Preserve ability to still use length hint (optional optimisation) | Conditional path |
| 5 | Add invariant: “Framer completes exactly when parsed JSON is valid and structurally balanced.” | Test extension |

### FrameStatus Type
```fsharp
type FrameStatus =
  | NeedMore
  | Complete of ReadOnlyMemory<byte>
  | Error of exn
```

### Parser Stage Replacement (Pseudo)
```fsharp
let mkFramingParser () : StreamProcessor<ReadOnlyMemory<byte>, JToken> =
  let buffer = System.Buffers.ArrayBufferWriter<byte>()
  let state = Ref initialFrameState
  let completed = ref false
  StreamProcessor (fun (env: Envelope<ReadOnlyMemory<byte>>) ->
    flow {
      if completed.Value then return Complete else
      buffer.Write env.Payload.Span
      match Frame.feed state env.Payload.Span with
      | NeedMore -> return Consume
      | Error ex -> return Error ex
      | Complete _ ->
          completed.Value <- true
          let json = Encoding.UTF8.GetString(buffer.WrittenSpan)
          let token = JToken.Parse json
          return Emit (mapEnvelope (fun _ -> token) env)
    })
```

---

## 6. Impact on Downstream Design

| Component | Impact with Tier B | Impact with Tier C |
|-----------|--------------------|--------------------|
| Accumulation Stage | None (still receives full `JToken`) | Needs alternative mode: batch per partial token (optional) |
| Projection Stage | None | Might operate on partial hierarchy (if desired) |
| Model Invariants | Add “FramerDepth returns 0 only at parse completion” | Could add token sequence invariants |
| Pipeline DU | No change | Add an alternative variant for token stream pipelines |

---

## 7. Risk & Mitigation

| Risk | Cause | Mitigation |
|------|-------|------------|
| Incorrect framing on pathological strings | Incomplete escape handling | Thorough unit tests with edge-case JSON (quotes, braces inside strings) |
| Performance regression | Double scan (framer + JSON parse) | Acceptable for tests; if needed adopt streaming parse later |
| Hidden dependency on prior full generation | Source always produces full JSON | Document assumption; for production streaming, switch parser implementation |
| Memory blow-up for huge docs | Accumulates entire doc | Add optional max size guard (`if buffer.Length > limit -> Error`) |

---

## 8. Recommendation Summary

| Goal | Recommended Action |
|------|--------------------|
| Preserve simplicity now | Keep current length-gated parser for immediate stability |
| Prepare for realistic streaming | Implement Tier B structural framer as drop-in replacement |
| Allow future token-level streaming | Abstract parser behind an interface (e.g., `IJsonDocumentParser`) |
| Avoid broad refactor | Keep downstream signatures unchanged for Tier B |
| Incremental test layering | Add properties for framing invariants separately before adopting Tier C |

---

## 9. Suggested Abstraction Interface

```fsharp
type IJsonAssembler<'Out> =
    abstract Feed : ReadOnlyMemory<byte> -> Result<AssemblerStatus<'Out>, exn>

and AssemblerStatus<'Out> =
    | NeedMore
    | Complete of 'Out
```

Implementations:
- `LengthBasedAssembler(expectedLen)`
- `FramingAssembler()`
- `StreamingTokenAssembler()` (future)

Parser stage becomes a thin adapter:
```fsharp
let mkAssemblerProcessor (assembler: IJsonAssembler<JToken>) =
  let doneRef = ref false
  StreamProcessor (fun (env: Envelope<ReadOnlyMemory<byte>>) ->
    flow {
      if doneRef.Value then return Complete else
      match assembler.Feed env.Payload with
      | Ok NeedMore -> return Consume
      | Ok (Complete tok) ->
          doneRef.Value <- true
          return Emit (mapEnvelope (fun _ -> tok) env)
      | Error ex -> return Error ex
    })
```

---

## 10. Code Skeleton for Structural Framer (Illustrative Only)

```fsharp
module JsonFramer =
  type InternalState = {
     depth    : int
     inString : bool
     escaped  : bool
     started  : bool
     complete : bool
  }

  let initial = { depth=0; inString=false; escaped=false; started=false; complete=false }

  let advance (st: InternalState) (span: ReadOnlySpan<byte>) =
    let mutable s = st
    let mutable i = 0
    while i < span.Length && not s.complete do
      let b = span.[i]
      let c = char b
      if not s.started && Char.IsWhiteSpace c then () else s <- { s with started = true }
      if s.inString then
        if s.escaped then
          s <- { s with escaped=false }
        else
          match c with
          | '\\' -> s <- { s with escaped=true }
          | '"'  -> s <- { s with inString=false }
          | _ -> ()
      else
        match c with
        | '"' -> s <- { s with inString=true }
        | '{' -> s <- { s with depth = s.depth + 1 }
        | '}' -> s <- { s with depth = s.depth - 1 }
        | '[' -> s <- { s with depth = s.depth + 1 }
        | ']' -> s <- { s with depth = s.depth - 1 }
        | _ -> ()
      if s.started && s.depth = 0 && not s.inString then
        // Potential completion for object/array or scalar
        // Heuristic: if top-level closed and next chars (including current) end the structure
        s <- { s with complete = true }
      i <- i + 1
    s
```

(You would refine the scalar detection — here it's simplified.)

---

## 11. Decision Matrix

| If you… | Then |
|---------|------|
| Want **fastest path** to richer tests | Keep length-based; add properties first |
| Want **realistic chunk semantics** now | Replace with structural framer |
| Want **future-proof streaming** | Introduce `IJsonAssembler` + initial `LengthBasedAssembler`, add `FramingAssembler` second |
| Need **token-level timing assertions** | Plan Tier C (incremental reader) after Tier B stabilizes |

---

## 12. Immediate Action Plan (Pragmatic)

1. Wrap current parser in `IJsonAssembler` as `LengthBasedAssembler`.
2. Add placeholder `FramingAssembler` (returns `NeedMore` until all bytes present; stub state).
3. Add property: “Assembler emits exactly one `Complete` and only after all chunks are fed.”
4. Implement real framing logic and swap in.
5. (Optional) Add fuzz test that splits bytes into every possible chunk partition to confirm frame detection invariants.

---

## 13. Final Answers to Your Questions

| Question | Answer |
|----------|--------|
| “Can current code know JSON is valid before full length?” | No. It defers validation until full length reached. This is acceptable given we *already* know origin JSON. |
| “Do we need streaming `JsonTextReader` now?” | Not strictly. Only if you want early validation, multi-document, or token-level emission. |
| “Can `System.IO.Pipelines` help?” | Yes for Tier C (true streaming), but overkill for Tier B structural framing. |
| “Downstream refactor needed?” | None for Tier B; major for Tier C if you propagate tokens. |
| “Overall impact?” | Tier B keeps downstream stable, adds modest complexity in one stage; Tier C cascades changes. |

---

## 14. Offer

If you’d like, I can next supply:
1. `IJsonAssembler` interface + length-based implementation
2. Framing assembler skeleton
3. Updated `IntegrationStages` parser factory using the assembler

Just say the word.

---

## 15. Summary

Your intuition about limitations of the current approach is correct in a *general* streaming context, but within this controlled test harness the length-based aggregation is a legitimate pragmatic choice. Introduce a framing layer (Tier B) if you want to realistically simulate not knowing content length; reserve a streaming token parser (Tier C) for when incremental downstream processing or early error detection becomes a priority.

---

*Let me know which tier you want to proceed with and I’ll generate the code scaffolding accordingly.*