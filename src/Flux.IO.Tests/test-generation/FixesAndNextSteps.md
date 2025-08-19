4. Modify Primary Pipeline DU to Include PStream
Edit existing CoreIntegrationPipeline.fs (show diff region only). Add a new case and builder reference.


CoreIntegrationPipeline.fs
v8
// ... existing content above ...

    type Pipeline =
        | PDirect of DirectPipeline
        | PChunk  of ChunkPipeline
        | PStream of CoreIntegrationStreamingPipeline.StreamingPipeline   // NEW

// (No changes to existing builders; PStream builder lives in CoreIntegrationStreamingPipeline.Builder)

5. Model & Machine Extensions
Add new runtime state record for streaming docs (token counts, completion).

CoreIntegrationMachine.Streaming.fs

Rather than rewriting the whole Sut, we recommend a second dictionary (pipelinesStream) to keep existing logic stable.

Additions (patch style):

```
// Inside type Sut() add:
    let streamPipelines = System.Collections.Concurrent.ConcurrentDictionary<int, CoreIntegrationMachineStreaming.ExtendedRuntimePipeline>()

    member _.RegisterStream (id:int) (pipe: CoreIntegrationStreamingPipeline.StreamingPipeline) =
        let rt = { CoreIntegrationMachineStreaming.StreamingRuntime.SourceEmitted=false
                   ToBytesRun=false; TokensSeen=0; RootCompleted=false
                   AccumDone=false; ProjectionScalars=false; ProjectionObjects=false }
        streamPipelines.TryAdd(id, CoreIntegrationMachineStreaming.RStream(pipe, rt)) |> ignore

    member this.StepStream id =
        match streamPipelines.TryGetValue id with
        | true, CoreIntegrationMachineStreaming.RStream(pl, rt) ->
            // One feed step across the streaming chain
            // Source -> ToBytes -> Chunker -> TokenReader (+ domain stages)
            // For now only advance Source once (like direct)
            if not rt.SourceEmitted then
                // run source
                ()
            else
                ()
        | _ -> ()
```

(Command & operation integration omitted for brevity; you would add a new Command variant StartStreamChunk + StepStreamToken etc.)

Given the size of this response, I’m keeping machine integration scaffolding minimal—let me know if you want the full command & operation wiring now or in a subsequent incremental PR once you settle on the exact streaming semantics.


6. New Invariants (Token Mode)
Add a test file verifying:

Depth monotonic non-negative (already via instrumentation earlier)
RootTerminal appears exactly once
Accum streaming Done implies union ⊆ final scalar keys (if materializer present later)

```
namespace Flux.IO.Tests

open System
open FsCheck
open Expecto
open TokenStreaming
open JsonStreamProcessors.JsonAssembler
open StreamingDomainStages
open Newtonsoft.Json.Linq

module StreamingTokenProperties =

    // Placeholder property skeletons to be filled once machine wiring is done.
    [<Tests>]
    let tokenStreamingProps =
        testList "Token Streaming (Phase 5 Scaffold)" [
            test "placeholder" {
                // This keeps build green until full integration.
                Expect.isTrue true "placeholder"
            }
        ]
```

7. Next Steps Checklist
Action sequence to fully realize Phase 5:

Integrate PStream DU (done in code).
Finish streaming pipeline builder (remove failwith, return PStream).
Add machine support:
New Command variants: StartStream, StepStream (source/toBytes/chunker feed), StepToken (token reader), StepAccumToken, StepProjectionToken.
Enable Next() to schedule these operations similarly to chunk docs.
Extend invariants:
Token depth never negative (reuse instrumentation or track via runtime).
RootTerminal => subsequent steps produce no new tokens.
Accum batches disjoint, union subset of discovered scalar names (mirrors existing accumulation invariants).
Add equivalence property: materializing streaming tokens (or re-parsing origin JSON) deep-equals original.
Optional: performance micro-benchmark for PStream vs StreamMaterialize.

8. Rationale for Minimal Materializer (Current Stub)
Right now materializer returns a placeholder. This keeps scope small; once token streaming semantics are correct, you can:

Replace stub with a true reconstruction (maintain a JTokenWriter inside a TokenMaterializer stage processing TokenEvent list).
Or short-circuit by reusing the original JSON text (needs capturing outside reader).
If you want a full token-based reconstruction now, ask and I can supply a JTokenWriter-based TokenMaterializer stage.

9. Recommended Incremental Merge Strategy
PR 1 (Phase 5-A):

Add TokenEvent, TokenStreamReader, PStream DU, streaming builder (without machine integration).
Add placeholder tests (green build).
PR 2 (Phase 5-B):

Machine command & runtime integration for PStream (no accumulation/projection yet).
Depth / root completion invariants.
PR 3 (Phase 5-C):

Streaming accumulation & projection (stages + invariants + parity tests).
Token materializer (real implementation).
Remove placeholder tests.
This keeps each review focused and reduces risk.

Let me know:

Do you want the full machine wiring now (bigger diff)?
Should the materializer be fully implemented in Phase 5-A?
Do you prefer accumulation to emit Domain coverage incrementally (monotonic) from the start?
I can follow up with whichever subset you want next. Just specify and I’ll produce the next patch set.

Would you like me to proceed with the full machine command wiring immediately?

