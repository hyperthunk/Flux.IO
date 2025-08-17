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

=========== Extensions recommended this iteration:

Pipeline Runtime Orchestration Architecture
Overview
The runtime orchestration layer must bridge between our pure functional processing logic (Flow/StreamProcessor) and the various execution backends (Async, Hopac, TPL, etc.) while maintaining type safety, managing lifecycle, and coordinating backpressure across heterogeneous execution contexts.
Core Design Principles

Backend Abstraction: Each execution backend provides its own implementation of core primitives (channels, synchronization, scheduling)
Boundary Management: Explicit, type-safe boundaries where execution contexts change
Pluggable Coordination: Backends can provide their own inter-stage communication mechanisms
Unified Backpressure: High-level backpressure semantics that backends implement according to their capabilities
Zero-Cost Abstraction: No overhead when using a single backend throughout

Architecture Layers
┌─────────────────────────────────────────────┐
│          Pipeline DSL & Graph               │
├─────────────────────────────────────────────┤
│         Runtime Orchestrator                │
│  • Graph interpretation                     │
│  • Boundary management                      │
│  • Lifecycle coordination                   │
├─────────────────────────────────────────────┤
│      Execution Backend Abstraction          │
│  • IExecutionBackend interface              │
│  • Backend-specific primitives              │
├─────────────────────────────────────────────┤
│        Concrete Backends                    │
│  • AsyncBackend (Channels)                  │
│  • HopacBackend (Ch, Alt)                   │
│  • TPLBackend (DataFlow)                    │
│  • SyncBackend (Direct calls)               │
└─────────────────────────────────────────────┘
Execution Backend Abstraction
fsharp// Core abstraction that each backend must implement
type IExecutionBackend =
    abstract member Name: string
    
    // Create a communication channel appropriate for this backend
    abstract member CreateChannel<'T>: 
        capacity: ChannelCapacity -> 
        IBackendChannel<'T>
    
    // Execute a processor in this backend's context
    abstract member Execute<'TIn, 'TOut>: 
        processor: StreamProcessor<'TIn, 'TOut> ->
        input: IBackendChannel<'TIn> ->
        output: IBackendChannel<'TOut> ->
        env: ExecutionEnv ->
        ct: CancellationToken ->
        IBackendTask
    
    // Wait primitives appropriate for this backend
    abstract member Wait: IBackendTask -> TimeSpan option -> WaitResult
    
    // Create a bridge to another backend
    abstract member CreateBridge<'T>: 
        target: IExecutionBackend -> 
        IBridge<'T>

and ChannelCapacity =
    | Unbounded
    | Bounded of int
    | Dropping of int * DropPolicy

and IBackendChannel<'T> =
    abstract member Backend: IExecutionBackend
    abstract member Write: Envelope<'T> -> BackendResult
    abstract member Read: unit -> BackendResult<Envelope<'T> option>
    abstract member Complete: unit -> unit
    abstract member GetMetrics: unit -> ChannelMetrics

and IBackendTask =
    abstract member Id: string
    abstract member Status: TaskStatus
    abstract member Cancel: unit -> unit

and BackendResult = 
    | Success
    | Blocked
    | Dropped
    | Error of exn

and WaitResult =
    | Completed
    | Timeout
    | Cancelled
    | Faulted of exn

and IBridge<'T> =
    abstract member Source: IExecutionBackend
    abstract member Target: IExecutionBackend
    abstract member Transfer: Envelope<'T> -> BridgeResult
Concrete Backend Implementations
AsyncBackend (ValueTask/Channel based)
fsharptype AsyncBackend() =
    interface IExecutionBackend with
        member _.Name = "Async"
        
        member _.CreateChannel<'T>(capacity) =
            match capacity with
            | Unbounded -> 
                AsyncChannel(Channel.CreateUnbounded<Envelope<'T>>())
            | Bounded n -> 
                AsyncChannel(Channel.CreateBounded<Envelope<'T>>(n))
            | Dropping(n, policy) ->
                AsyncChannel(Channel.CreateBounded<Envelope<'T>>(
                    BoundedChannelOptions(n, FullMode = policy)))
                    
        member _.Execute(processor, input, output, env, ct) =
            let task = task {
                let inputSeq = AsyncChannel.toTaskSeq input
                for envelope in inputSeq do
                    let! result = processor envelope |> Flow.run env ct
                    match result with
                    | StreamCommand.Emit e -> 
                        do! output.Write(e)
                    | _ -> ()
            }
            AsyncTask(task)

type AsyncChannel<'T>(channel: Channel<Envelope<'T>>) =
    interface IBackendChannel<'T> with
        member _.Backend = asyncBackend
        member _.Write(envelope) = 
            if channel.Writer.TryWrite(envelope) then Success
            else Blocked
        member _.Read() = 
            // ... implementation
HopacBackend
fsharptype HopacBackend() =
    interface IExecutionBackend with
        member _.Name = "Hopac"
        
        member _.CreateChannel<'T>(capacity) =
            match capacity with
            | Unbounded -> HopacChannel(Ch<Envelope<'T>>())
            | Bounded n -> HopacBoundedChannel(n)
            | _ -> // Custom Hopac implementation
            
        member _.Execute(processor, input, output, env, ct) =
            let job = job {
                let rec loop () = job {
                    let! envelope = Ch.take input.Channel
                    // Convert Flow computation to Hopac Job
                    let! result = processor envelope |> flowToJob env ct
                    match result with
                    | StreamCommand.Emit e ->
                        do! Ch.give output.Channel e
                    | _ -> ()
                    return! loop()
                }
                return! loop()
            }
            HopacTask(job)
Stage Execution Context
Each stage declares its execution requirements:
fsharptype StageExecutionRequirements = {
    PreferredBackend: BackendType
    Lifecycle: StageLifecycle
    Parallelism: ParallelismHint
    ResourceRequirements: ResourceRequirements
}

and BackendType =
    | Async              // For I/O bound
    | Hopac              // For CPU bound with high parallelism
    | TPL                // For coarse-grained parallelism
    | Any                // No preference

and StageLifecycle =
    | OnDemand           // Created per item
    | Persistent         // Reused across items
    | Continuous         // Runs forever

and ResourceRequirements = {
    EstimatedCPU: CPUIntensity
    EstimatedMemory: MemoryUsage
    IOCharacteristics: IOPattern
}
Runtime Orchestrator
The orchestrator interprets the pipeline graph and manages execution:
fsharptype RuntimeOrchestrator(graph: PipelineGraph) =
    
    // Analyze graph and determine execution plan
    member _.PlanExecution() : ExecutionPlan =
        // 1. Identify execution boundaries (where backend changes)
        // 2. Group consecutive stages with same backend
        // 3. Insert bridges at boundaries
        // 4. Optimize (fusion, etc.)
        
    // Execute the pipeline
    member _.Execute(env: ExecutionEnv, ct: CancellationToken) =
        let plan = this.PlanExecution()
        
        // Create execution segments
        let segments = plan.Segments |> List.map (fun segment ->
            let backend = selectBackend segment.Requirements
            let channels = createChannels segment backend
            let task = backend.Execute(segment.Processor, channels, env, ct)
            { Segment = segment; Task = task; Channels = channels }
        )
        
        // Connect segments with bridges
        for (source, target) in plan.Bridges do
            let bridge = source.Backend.CreateBridge(target.Backend)
            connectWithBridge source target bridge
            
        // Start execution
        segments |> List.iter (fun s -> s.Task.Start())
        
        // Monitor and adapt
        this.MonitorExecution(segments)
Execution Plan
fsharptype ExecutionPlan = {
    Segments: ExecutionSegment list
    Bridges: (SegmentId * SegmentId) list
    Optimizations: Optimization list
}

and ExecutionSegment = {
    Id: SegmentId
    Stages: StageNode list
    Requirements: AggregateRequirements
    Backend: IExecutionBackend
}

and Optimization =
    | Fusion of StageId list
    | ParallelSplit of StageId * int
    | BatchResize of StageId * BatchStrategy
Boundary Management
When crossing backend boundaries:
fsharptype BoundaryManager() =
    
    // Async -> Hopac
    member _.AsyncToHopac(source: IBackendChannel<'T>, target: IBackendChannel<'T>) =
        // Use Hopac's async integration
        let bridgeJob = job {
            let rec loop() = job {
                // Read from async channel
                let! envelope = 
                    source.Read() 
                    |> Task.ofBackendResult 
                    |> Job.fromTask
                    
                match envelope with
                | Some env ->
                    // Write to Hopac channel
                    do! Ch.give target.Channel env
                    return! loop()
                | None -> 
                    return ()
            }
            return! loop()
        }
        Hopac.start bridgeJob
        
    // Hopac -> Async
    member _.HopacToAsync(source: IBackendChannel<'T>, target: IBackendChannel<'T>) =
        // Start async task that pulls from Hopac
        let bridgeTask = task {
            while true do
                // Convert Hopac Alt to Task
                let! envelope = 
                    Ch.take source.Channel
                    |> Job.toAsync
                    |> Async.StartAsTask
                    
                do! target.Write(envelope)
        }
        bridgeTask
Backpressure Coordination
Each backend implements backpressure according to its capabilities:
fsharptype BackpressureCoordinator() =
    
    member _.CoordinateAcrossBackends(segments: ExecutionSegment list) =
        // High-level strategy that backends implement
        let strategy = {
            TargetQueueDepth = 100
            MaxMemoryPressure = 0.8
            AdaptationInterval = TimeSpan.FromSeconds(5.0)
        }
        
        // Each backend reports metrics
        let getMetrics segment =
            match segment.Backend with
            | :? AsyncBackend -> 
                // Channel queue depth
                segment.Channels |> List.map (fun c -> c.GetMetrics())
                
            | :? HopacBackend ->
                // Hopac scheduler queue depth + custom metrics
                getHopacMetrics()
                
        // Coordinate by adjusting rates
        let rec coordinationLoop() = async {
            let allMetrics = segments |> List.map getMetrics
            let adjustments = calculateAdjustments strategy allMetrics
            
            for (segment, adjustment) in List.zip segments adjustments do
                applyAdjustment segment adjustment
                
            do! Async.Sleep(strategy.AdaptationInterval)
            return! coordinationLoop()
        }
Split/Join with Mixed Backends
fsharptype SplitJoinCoordinator() =
    
    member _.CreateSplit(router: Router<'T>, branches: Branch<'T> list) =
        let routerBackend = AsyncBackend() // Router always runs in async
        
        // Create appropriate channel for each branch
        let branchChannels = 
            branches |> List.map (fun branch ->
                let backend = selectBackend branch.Requirements
                let channel = backend.CreateChannel(Bounded 100)
                branch.RouteKey, (backend, channel)
            )
            
        // Router runs in async and bridges to branch backends
        let routerTask = task {
            for envelope in input do
                let routeKey = router envelope
                let (backend, channel) = Map.find routeKey branchChannels
                
                if backend = routerBackend then
                    // Same backend, direct write
                    do! channel.Write(envelope)
                else
                    // Different backend, use bridge
                    let bridge = routerBackend.CreateBridge(backend)
                    do! bridge.Transfer(envelope)
        }
        
    member _.CreateJoin(strategy: JoinStrategy<'T>, branches: JoinBranch<'T> list) =
        // Similar but in reverse - collect from multiple backends
Example: Mixed Backend Pipeline
fsharplet mixedPipeline = {
    Stages = [
        // IO-bound: use Async
        { Stage = readSocket; Requirements = { PreferredBackend = Async; ... }}
        
        // CPU-bound parsing: still Async is fine
        { Stage = parseData; Requirements = { PreferredBackend = Async; ... }}
        
        // Split point
        { Stage = splitter; Requirements = { PreferredBackend = Async; ... }}
        
        // Validation: low CPU, stay in Async
        { Stage = validate; Requirements = { PreferredBackend = Async; ... }}
        
        // Enrichment: HIGH CPU, many parallel jobs
        { Stage = enrich; Requirements = { PreferredBackend = Hopac; ... }}
        
        // Join point: coordinate back to Async
        { Stage = joiner; Requirements = { PreferredBackend = Async; ... }}
        
        // Aggregation: stateful, single-threaded
        { Stage = aggregate; Requirements = { PreferredBackend = Async; ... }}
        
        // IO-bound output
        { Stage = writeToKafka; Requirements = { PreferredBackend = Async; ... }}
    ]
}

// Runtime creates execution plan:
// Segment 1: [readSocket, parseData, splitter, validate] -> Async
// Bridge: Async -> Hopac
// Segment 2: [enrich] -> Hopac  
// Bridge: Hopac -> Async
// Segment 3: [joiner, aggregate, writeToKafka] -> Async
Lifecycle Management
fsharptype LifecycleManager() =
    
    member _.ManageStageLifecycle(stage: StageNode, lifecycle: StageLifecycle) =
        match lifecycle with
        | OnDemand ->
            // Create new instance per envelope
            fun envelope ->
                let instance = createStageInstance stage
                let result = instance.Process envelope
                instance.Dispose()
                result
                
        | Persistent ->
            // Reuse instance with pooling
            let pool = StageInstancePool(stage, maxSize = 10)
            fun envelope ->
                let instance = pool.Rent()
                try
                    instance.Process envelope
                finally
                    pool.Return(instance)
                    
        | Continuous ->
            // Long-running task
            let instance = createStageInstance stage
            let task = instance.RunContinuously()
            // Manage task lifecycle separately
Key Benefits

Backend Independence: Stages can use the most appropriate execution model
Type Safety: No dynamic casting or type erasure at boundaries
Performance: Each backend optimizes for its use case
Composability: Mix and match backends within a single pipeline
Maintainability: Clear separation between logic and execution

Open Questions for Discussion

Should we allow dynamic backend switching based on runtime metrics?
How do we handle complex patterns like feedback loops across backends?
What's the policy for backend selection when Requirements = Any?
Should bridges be pooled/reused or created per-transfer?
How do we handle ordered processing requirements across backend boundaries?
