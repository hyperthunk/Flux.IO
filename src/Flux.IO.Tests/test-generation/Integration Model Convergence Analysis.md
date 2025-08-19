# Integration Model Convergence Analysis (V1 → V2 Synthesis, Refactored)

## Change Log for This Revision

The original convergence analysis assumed (based on an earlier misunderstanding of the prompt) that the **final** pipeline stage must emit the JSON string (“terminal JSON emission guarantee”).  
The clarified requirement:  
> The term “terminal node” refers to *either end* of the circuit (origin OR destination).  
> For these tests we only need to ensure the **origin node produces random JSON data** using the existing `JsonGenerators`; there is **no requirement** that the *final* stage emit JSON.

Accordingly:
- All recommendations that enforced a “final JSON string emission” contract are relaxed.
- The origin (source) stage now becomes the canonical JSON emitter (source of truth).
- Downstream stages may produce transformed types (e.g. `JToken`, batches, dictionaries, domain snapshots, metrics, etc.) without forcing re-materialization of the original JSON.
- Persistence and accumulation processors are re-framed as *transformers / side-effect simulators* rather than guardians of a terminal JSON contract.

Everything else (type discipline, pipeline packaging, accumulation semantics, chunking mechanics, scenario modeling, invariants) remains as previously recommended.

---

## 1. High-Level Comparison (Unchanged Except Terminal Row)

| Aspect | V1 (Original) | V2 (Current) | Evaluation |
|--------|---------------|--------------|------------|
| Scenario coverage | Chunking, Accumulation, Direct (+ HybridStub) | Chunking, Accumulation, Direct | Keep full set (Hybrid optional) |
| Model richness | Detailed semantic stores | Minimal invariants | Restore V1 richness |
| Stage composition | Multiple hetero dictionaries | Single DU (`Pipeline`) | Keep V2 DU concept |
| Parser lifecycle | Split feed + parse op | Parser auto-emits when complete | Adopt V2 simplicity |
| Accumulation | Mixed (processor + model replication) | Mostly model-simulated | Introduce explicit AccumEvent (see below) |
| Persistence role | Enforces final JSON output | Emits canonical original JSON | Now: optional transformer; not required for final JSON |
| Terminal emission (UPDATED) | Forced final JSON emission | Final JSON emission assumption | Replace with: **Origin emits JSON; downstream free** |
| Type safety | Generic leakage | Explicit stage types | Retain explicitness |
| Chunk representation | Arrays + implicit conversions | Explicit `ReadOnlyMemory<byte>` creation | Keep V2 approach |
| Extensibility | Higher (more hooks) | Currently simplified | Reintroduce hooks after type convergence |

---

## 2. Impact of the Revised Terminal Semantics

### Before (Misinterpreted)
- “Terminal must output JSON string”
- Persistence stage forced canonical JSON output.
- Accumulation pipeline adapted events to eventually yield JSON.

### Now (Corrected)
- Source/origin stage is the only required JSON producer (already satisfied: generator → JSON string or `JToken`).
- Downstream stages may:
  - Parse → `JToken`
  - Chunk / reassemble → `JToken`
  - Accumulate → `BatchDomain`, `AccumEvent`
  - Persist → metrics/logging side effects, optional pass-through
- Final stage can emit any suitable diagnostic / transformed artifact OR nothing (e.g. `Complete`).

### Consequences
| Area | Adjustment |
|------|-----------|
| “Persistence” Stage | Rename conceptually to “KVProjection” or “SideEffectsStage” (since it no longer must re-emit JSON) |
| Accumulation Finalization | No need to convert back to JSON; invariants rely on coverage of origin token’s scalar keys |
| Terminal Stage | Optional; if retained, can serve as sink / assertion hook rather than JSON passthrough |
| Invariants | Remove “final JSON emission” requirement; add “origin JSON stable reference / structural equality baseline” |
| Model State | Keep OriginalJson & RootToken for oracle comparisons; downstream outputs need not duplicate them |

---

## 3. What to Preserve vs Modify

| Keep Original Recommendation | Modification Required |
|------------------------------|-----------------------|
| Use `Pipeline` DU with record cases | Adjust each pipeline so last stage output type is unconstrained (can differ per scenario) |
| Distinct stage types with explicit generics | Remove enforced final `string` stage from design |
| AccumEvent modeling for accumulation | Change adapter logic: no forced mapping back to `JToken` → instead, treat `FinalToken` as end-of-stream marker |
| Rich model invariants (coverage, chunk completeness) | Drop final-json-emitted invariants; add optional structural stability checks only at source |
| Chunk parser design (emit once after full buffer) | Unchanged |
| Origin JSON generation via `JsonGenerators` | Emphasize as the sole mandated JSON emission point |
| Scenario weighting & model-driven `.Next` | Unchanged (to be reattached later) |

---

## 4. Revised Type Strategy (Terminal Neutralization)

### 4.1 Stage Signatures (Updated)

| Stage | Signature | Role After Revision |
|-------|-----------|---------------------|
| JsonSource (origin) | `unit -> Envelope<string>` OR direct `StreamProcessor<unit,string>` | Generates random JSON text using `JsonGenerators.genJson` |
| BytesChunker (optional) | `StreamProcessor<string, ReadOnlyMemory<byte>>` OR separate transformation from source bytes | Converts origin JSON string to UTF-8 bytes then emits chunk envelopes |
| Chunk Parser | `StreamProcessor<ReadOnlyMemory<byte>, JToken>` | Reassembles + parses document |
| Direct Parser | `StreamProcessor<string, JToken>` (if bypass bytes) | Parses JSON directly (direct scenario) |
| Accumulation | `StreamProcessor<JToken, AccumEvent>` | Emits batches (domain snapshots) + final marker |
| KV Projection | `StreamProcessor<JToken, ProjectionEvent>` (optional) | Extracts & stores scalar/object keys (side effects simulated) |
| Sink / Terminal (optional) | `StreamProcessor<'a, unit>` | Observes end-of-stream; may always produce `Consume` / `Complete` |

### 4.2 Suggested Domain Event Types

```fsharp
type AccumEvent =
  | Batch of HashMap<string,obj>
  | Done  of HashSet<string> // full scalar domain snapshot

type ProjectionEvent =
  | Scalars of HashMap<string,obj>
  | Objects of HashMap<string,string>
  | Completed
```

These replace forced JSON re-serialization.

---

## 5. Revised Pipeline Packaging

```fsharp
type ChunkPipeline =
  { Source  : StreamProcessor<unit, string>          // origin JSON
    ToBytes : StreamProcessor<string, ReadOnlyMemory<byte>>
    Chunker : StreamProcessor<unit, ReadOnlyMemory<byte>> // OR integrated into ToBytes
    Parser  : StreamProcessor<ReadOnlyMemory<byte>, JToken>
    Accum   : StreamProcessor<JToken, AccumEvent> option
    Project : StreamProcessor<JToken, ProjectionEvent> option
    Sink    : StreamProcessor<ProjectionEvent, unit> option } // optional terminal sink

type DirectPipeline =
  { Source  : StreamProcessor<unit, string>
    Parser  : StreamProcessor<string, JToken>
    Accum   : StreamProcessor<JToken, AccumEvent> option
    Project : StreamProcessor<JToken, ProjectionEvent> option
    Sink    : StreamProcessor<ProjectionEvent, unit> option }

type Pipeline =
  | PChunk of ChunkPipeline
  | PDirect of DirectPipeline
  | PAccumChunk of ChunkPipeline // chunking + accumulation emphasis
```

Notes:
- `Source` now explicitly exists.
- A simple variant merges `ToBytes + Chunker` but separation improves test granularity (able to test pre-chunk vs post-chunk invariants).
- `Sink` removed from obligations; purely optional.

---

## 6. Execution Flow (Per Scenario)

### 6.1 Direct Scenario (No Chunking)
1. Run `Source` (once) → JSON string.
2. Feed to `Parser` → `JToken`.
3. Optionally feed to `Accum`:
   - Collect `Batch` events.
   - On `Done domain`, update model.
4. Optionally feed to `Project` for scalar/object mapping simulation.
5. `Sink` (if present) can record metrics or mark completion.

### 6.2 Chunking Scenario
1. Run `Source` → JSON string.
2. Run `ToBytes` (string → `ReadOnlyMemory<byte>` full buffer, or produce seed for chunker).
3. Run `Chunker` repeatedly → pieces.
4. Each chunk into `Parser`:
   - `Consume` until final → `Emit parsedToken`.
5. Downstream identical to direct after parse.

### 6.3 Accumulation Over Chunking
Same as chunking but passes parsed token through accumulation stage after parse completes.

---

## 7. Adjusted Model & Invariants

### 7.1 Model Changes

Remove:
- `FinalJsonEmitted`
- Mandatory terminal string tracking

Add:
- `OriginJson : string`
- `OriginToken : JToken`
- `ObservedScalarKeys : HashSet<string>`
- `ObservedBatches : HashSet<string> list`
- `ProjectionScalars : HashMap<string,obj>`
- `ProjectionObjects : HashMap<string,string>`
- `ChunkingComplete : bool`
- `ParseComplete : bool`
- `AccumComplete : bool`

### 7.2 Invariants

| Invariant | Condition |
|-----------|-----------|
| Origin Stability | Parsing `OriginJson` again produces a `JToken` structurally equal to `OriginToken` |
| Chunk Coverage | If `ParseComplete = true` then concatenation(chunks) = UTF8(originJson) |
| Single Parse Emit | `ParseComplete` flips from false → true exactly once |
| Accumulation Coverage | If `AccumComplete = true` then union of batch domains = scalar property set of root object |
| No Overlap | Each batch domain disjoint (unless design intentionally allows duplicates — define explicitly) |
| Projection Agreement | If both Projection & Accum enabled: union of batch domains = keys(ProjectionScalars) |
| Monotonic Sets | Observed domains strictly grow (no key removal) |
| Scenario Separation | Direct scenario has zero chunk steps; chunk scenario has ≥1 chunk step |

All previous “final JSON emission” assertions are removed.

---

## 8. Stage Implementations (Key Adjustments)

### 8.1 Source Stage
```fsharp
let mkJsonSource (gen: unit -> string) : StreamProcessor<unit,string> =
  StreamProcessor (fun (env: Envelope<unit>) ->
     flow { return Emit (Envelope.create env.SeqId (gen())) })
```
(If reproducibility required, pass seed or pre-generated JSON from model.)

### 8.2 ToBytes
```fsharp
let mkToBytes () : StreamProcessor<string, ReadOnlyMemory<byte>> =
  StreamProcessor (fun (env: Envelope<string>) ->
    flow {
      let bytes = Encoding.UTF8.GetBytes env.Payload
      return Emit (mapEnvelope (fun _ -> ReadOnlyMemory<byte>(bytes)) env)
    })
```

### 8.3 Chunker
If you want fine control, give `Chunker` responsibility for slicing, not `ToBytes`:
- `ToBytes` produces the *full* memory once (or not at all).
- `Chunker` keeps internal index and slices the underlying immutable array.

### 8.4 Parser
No change, except it no longer needs to reconstitute and re-emit original JSON.

### 8.5 Accumulation
Update final event to use `Done domainSet` instead of `FinalToken`.

### 8.6 Projection
```fsharp
let mkProjection () : StreamProcessor<JToken, ProjectionEvent> =
  StreamProcessor (fun (env: Envelope<JToken>) ->
     flow {
       match env.Payload with
       | :? JObject as o ->
          let scalars =
            o.Properties()
            |> Seq.choose (fun p ->
               if p.Value :? JObject then None
               else Some (p.Name, box (p.Value.ToString())))
            |> Seq.fold (fun acc (k,v) -> HashMap.add k v acc) HashMap.empty
          let objs =
            o.Properties()
            |> Seq.choose (fun p ->
               match p.Value with
               | :? JObject as jo -> Some (p.Name, jo.ToString(Newtonsoft.Json.Formatting.None))
               | _ -> None)
            |> Seq.fold (fun acc (k,v) -> HashMap.add k v acc) HashMap.empty
          let envScalars = mapEnvelope (fun _ -> ProjectionEvent.Scalars scalars) env
          // Optionally emit scalars first; for simplicity only scalars; objects could be separate events.
          return Emit envScalars
       | _ -> return Consume
     })
```

---

## 9. Simplified Harness (Pre-Machine)

Define per-scenario “advance” functions injecting a single step, returning updated model deltas. Because we removed the enforced final JSON re-emission, less adaptation is needed after `Parser`.

---

## 10. Migration Steps (Updated)

| Step | Action | Delta from Previous Plan |
|------|--------|--------------------------|
| 1 | Introduce `mkJsonSource` | New (origin contract) |
| 2 | Remove forced final JSON emission code paths | Changed |
| 3 | Refactor accumulation final event to `Done` | Changed |
| 4 | Adjust invariants (no “FinalJsonEmitted”) | Changed |
| 5 | Replace persistence with projection stage | Renamed / semantic shift |
| 6 | Rebuild pipelines with new stage graph | Slight restructure |
| 7 | Update model for new fields (ObservedScalarKeys, etc.) | Changed |
| 8 | Write harness “advance” functions | Similar |
| 9 | Re-enable Machine using new command set (later) | Same concept |
| 10 | Add property tests verifying origin-only JSON guarantee | New test emphasis |

---

## 11. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Loss of end-to-end JSON equality checks | Keep source JSON + parsed token structural assert |
| Over-segmentation of stages inflates test surface | Provide composite convenience constructors (e.g., direct pipeline builder) |
| AccumEvent complexity | Keep event type minimal (only `Batch` + `Done`) |
| State duplication between model and processors | Flow side-effects minimized; model reconstructs expectations from origin token |

---

## 12. Example Converged Types (Illustrative)

```fsharp
type AccumEvent =
  | Batch of HashMap<string,obj>
  | Done  of HashSet<string>

type ProjectionEvent =
  | Scalars of HashMap<string,obj>
  | Objects of HashMap<string,string>

type ChunkPipeline = {
  Source  : StreamProcessor<unit,string>
  ToBytes : StreamProcessor<string, ReadOnlyMemory<byte>>
  Chunker : StreamProcessor<unit, ReadOnlyMemory<byte>>
  Parser  : StreamProcessor<ReadOnlyMemory<byte>, JToken>
  Accum   : StreamProcessor<JToken, AccumEvent> option
  Project : StreamProcessor<JToken, ProjectionEvent> option
}

type DirectPipeline = {
  Source  : StreamProcessor<unit,string>
  Parser  : StreamProcessor<string,JToken>
  Accum   : StreamProcessor<JToken, AccumEvent> option
  Project : StreamProcessor<JToken, ProjectionEvent> option
}

type Pipeline =
  | PChunk of ChunkPipeline
  | PDirect of DirectPipeline
```

---

## 13. Updated Invariant Examples

```text
Invariant: For any doc d
  classify(d.OriginToken).ScalarKeys = union( all Batch keys ∪ Done keys )
Invariant: If d.ParseComplete
  then concat(chunks) UTF8 == d.OriginJson UTF8
Invariant: Source structural stability
  JToken.DeepEquals( JToken.Parse(d.OriginJson), d.OriginToken ) = true
Invariant: No duplicate scalar keys across batches
  pairwise disjoint(batchDomains)
```

---

## 14. Next Steps

1. Implement `IntegrationStages.fs` with the revised stage constructors.
2. Refactor existing registry to hold only `Pipeline`.
3. Replace “final JSON emission” logic with origin-only assertions.
4. Add property ensuring every started document has exactly one successful Source emission.
5. Reintroduce Machine & operations (progression commands) once stage layer compiles.

---

## 15. Summary

The only conceptual change required by the clarified prompt is **moving the JSON emission obligation exclusively to the origin**. This *simplifies* downstream design, removes artificial re-serialization, and improves semantic clarity. All prior recommendations on type safety, DU packaging, accumulation events, chunk parser discipline, and model richness remain valid and are easier to implement under the refined contract.

If you’d like, I can now generate the initial scaffolding (`IntegrationStages.fs` + updated `Pipeline` file) aligned with this spec.

Just say the word and I’ll produce those code files.

---