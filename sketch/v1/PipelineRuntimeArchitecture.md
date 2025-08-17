# Pipeline Runtime Architecture - Bridging Sync and Async Worlds

## Overview

We have two complementary execution models:

1. **Synchronous Flow Model** (what we built): Perfect for envelope-by-envelope processing with natural backpressure
2. **Asynchronous Pipeline Model** (what we need to add): Long-running stages, splitting/joining, parallel sub-pipelines

The key insight is that we don't need to change the Flow monad or StreamProcessor - we need to build a **Pipeline Runtime** that orchestrates these processors.

## Core Concepts

### 1. Stage Lifecycle

```fsharp
type StageLifecycle =
    | OnDemand      // Created per envelope, destroyed after
    | LongRunning   // Created once, reused across envelopes
    | Continuous    // Runs forever, pushing data downstream
```

### 2. Stage Execution Context

```fsharp
type StageExecutionContext =
    | SyncFlow      // Uses our Flow monad for single envelope
    | AsyncWorker   // Long-running async loop
    | ParallelCore  // Delegates to Hopac/TPL/etc
```

### 3. Pipeline Segments

A pipeline is composed of **segments**, where each segment has uniform execution characteristics:

```fsharp
type PipelineSegment<'TIn, 'TOut> =
    | Linear of StreamProcessor<'TIn, 'TOut>
    | Source of IAsyncEnumerable<Envelope<'TIn>>
    | Sink of (Envelope<'TIn> -> Async<unit>)
    | Split of (Envelope<'TIn> -> RouteKey) * Map<RouteKey, PipelineSegment<'TIn, 'TMid>>
    | Join of PipelineSegment<'TIn, 'TMid> list * ('TMid list -> 'TOut)
    | Parallel of ParallelExecutor<'TIn, 'TOut>
```

## Execution Model

### For On-Demand Stages (JSON Parser)

```
Socket Reader (continuous) 
    -> Channel<bytes>
    -> JSON Parser (on-demand per document)
    -> Channel<parsed>
    -> Next Stage
```

The JSON parser:
1. Pulls from channel until document boundary
2. Processes using our Flow monad
3. Emits result
4. **Terminates** - ready to be created again for next document

### For Long-Running Stages (Socket Reader)

```fsharp
let socketReader endpoint = async {
    let channel = Channel.CreateUnbounded<Envelope<byte[]>>()
    
    // This runs forever
    let! _ = Async.Start(async {
        while true do
            let! data = readFromSocket endpoint
            do! channel.Writer.WriteAsync(createEnvelope data)
    })
    
    return channel.Reader
}
```

### For Parallel Sub-Pipelines (ML Pipeline)

```fsharp
type ParallelExecutor<'TIn, 'TOut> = {
    Engine: ParallelEngine
    Partition: 'TIn -> PartitionKey
    Process: 'TIn -> Job<'TOut>  // or Task<'TOut>
    Combine: Map<PartitionKey, 'TOut> -> 'TOut
}
```

## Backpressure Across Boundaries

The key challenge is maintaining backpressure across different execution contexts:

1. **Channel-based**: Use bounded channels between segments
2. **Credit-based**: Long-running stages request credits before pulling
3. **Adaptive**: Monitor queue depths and adjust processing rates

## Example: Your ML Pipeline

```fsharp
// Define the pipeline structure
type MLPipeline = {
    // Continuous source
    SocketReader: Continuous<HttpListener>
    
    // On-demand splitter
    ContentSplitter: OnDemand<bytes, ParsedContent>
    
    // Parallel ML processing
    MLProcessor: ParallelCore<ParsedContent, MLResult>
    
    // Accumulator (using our existing model)
    Accumulator: OnDemand<MLResult, Dataset>
    
    // Sequential sink
    DBWriter: OnDemand<Dataset, unit>
}

// Runtime orchestration
let runMLPipeline pipeline = async {
    // Stage 1: Socket reader runs continuously
    let! sourceChannel = startContinuousSource pipeline.SocketReader
    
    // Stage 2: Content splitter processes on-demand
    let splitChannel = Channel.CreateBounded<ParsedContent>(100)
    
    let! splitterTask = Async.Start(async {
        for envelope in sourceChannel do
            // Create splitter for this envelope
            let result = 
                pipeline.ContentSplitter envelope
                |> Flow.run env ct
                |> Async.AwaitTask
                
            match result with
            | Emit parsed -> 
                do! splitChannel.Writer.WriteAsync(parsed)
            | _ -> ()
    })
    
    // Stage 3: ML processing in parallel
    let mlChannel = Channel.CreateBounded<MLResult>(1000)
    
    let! mlTask = 
        ParallelEngine.run 
            pipeline.MLProcessor 
            splitChannel.Reader 
            mlChannel.Writer
            
    // Continue with accumulation and DB writing...
}
```

## Key Design Decisions

### 1. Separation of Concerns

- **Flow/StreamProcessor**: Handles single-envelope processing logic
- **Pipeline Runtime**: Handles lifecycle, channels, and orchestration
- **Execution Engines**: Handle parallelism within stages

### 2. Explicit Boundaries

Make it explicit where execution context changes:
- `OnDemand -> Continuous`: Requires channel
- `SyncFlow -> ParallelCore`: Requires partition strategy
- `Parallel -> Sequential`: Requires synchronization

### 3. Composability

Each segment type can be composed:
```fsharp
let complexPipeline =
    Source socketReader
    >>= Split contentTypeSplitter
    >>= Join [xmlProcessor; jsonProcessor]
    >>= Parallel mlEngine
    >>= Linear accumulator
    >>= Sink dbWriter
```

## Benefits of This Approach

1. **Preserves existing code**: The Flow monad and StreamProcessor remain unchanged
2. **Clear semantics**: Explicit about what runs when and for how long
3. **Flexible composition**: Can mix and match execution styles
4. **Natural backpressure**: Channels provide buffering and backpressure between segments
5. **Testability**: Each segment can be tested independently

## Next Steps

1. Define the `PipelineSegment` discriminated union
2. Build the runtime orchestrator that connects segments
3. Create adapters between execution contexts
4. Implement channel-based backpressure coordination
5. Add lifecycle management for on-demand vs long-running stages

This layered approach gives us the best of both worlds - the simplicity of synchronous flow for processing logic, and the flexibility of async orchestration for complex topologies.