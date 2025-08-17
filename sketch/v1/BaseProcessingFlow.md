processing nodes do not continue running for the duration of the pipeline. Each envelope is processed through the pipeline one at a time, and each processor runs only for the duration of processing that single envelope.
Detailed Execution Flow
Let's trace through what happens:

When flow { ... } is used:

The flow computation expression immediately creates a Flow<'a> value
This is just a wrapped function (ExecutionEnv -> CancellationToken -> ValueTask<'a>)
No async computation is created or started yet


When Flow.run is called:

This is when the ValueTask is actually created
The function is invoked with the environment and cancellation token
If the computation is synchronous (like Flow.ret), it returns a completed ValueTask
If the computation is asynchronous, it returns a ValueTask that represents the ongoing operation


Pipeline Execution Pattern:

Looking at the example in Stages.fs:
fsharpinterface IStage<System.ReadOnlyMemory<byte>, Newtonsoft.Json.Linq.JObject> with
    member _.Process env ct input =
        asyncSeq {
            for envelope in input do  // <-- Pulls one envelope at a time
                let! result = 
                    StreamProcessor.runProcessor processor envelope
                    |> Flow.run env ct    // <-- Creates and runs ValueTask here
                    |> Async.AwaitTask    // <-- Waits for completion
                    
                match result with
                | StreamCommand.Emit outEnv -> yield outEnv
                // ...
        }
Here's what happens:

The asyncSeq pulls envelopes one at a time from the input
For each envelope, it creates a new ValueTask via Flow.run
It waits for that task to complete before processing the next envelope
The processor's task/async computation lives only for the duration of processing that single envelope

Key Insights

Short-lived computations: Each processor invocation is a short-lived computation that processes exactly one envelope and then completes.
Pull-based model: The pipeline uses a pull-based model where:

Downstream stages pull envelopes from upstream stages
Each pull triggers a cascade of processing through the pipeline
Once an envelope is processed through all stages, the computations complete


No persistent async workers: There are no long-running async workers or actors. Each stage is essentially a pure function that's invoked on demand.
Backpressure naturally emerges: Because processing is pull-based and synchronous per envelope, backpressure naturally emerges - a slow downstream stage automatically slows down upstream processing.

Example Execution Trace
Let's trace a single envelope through the pipeline:
fsharp// Pipeline: bytes -> string -> json
let pipeline = bytesToString |>> parseJson

// When we process one envelope:
let envelope = { Payload = bytes; ... }

// Step 1: pipeline is called with envelope
let resultFlow = pipeline envelope
// This returns a Flow<StreamCommand<JObject>> - no async work yet!

// Step 2: Flow.run is called
let valueTask = Flow.run env ct resultFlow
// NOW the async work happens:
// - bytesToString processes the envelope (quick, synchronous)
// - Its result is fed to parseJson
// - parseJson does its work
// - Returns a completed ValueTask with the result

// Step 3: ValueTask completes
// The async computation is done - no lingering workers
Implications
This design has several important implications:

Memory efficiency: No accumulation of state between envelopes (unless explicitly using stateful processors)
Simple concurrency model: Each envelope is processed atomically through the pipeline
Easy testing: You can test processors with individual envelopes without worrying about long-running state
Natural batching point: If you want to process multiple envelopes concurrently, you'd do it at the pipeline level, not within processors

===================== Expanding

1. No Dynamic Backend Switching
Agreed - this would add significant complexity for marginal benefit. Static backend assignment at pipeline construction time is much cleaner.

3. DSL with Explicit Backend Selection
I love this approach! It makes the backend choice explicit and clear. Here's how we could structure it:

