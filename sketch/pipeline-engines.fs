namespace AsyncPipeline.Engines

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open AsyncPipeline.Core
open AsyncPipeline.IO
open AsyncPipeline.FlowControl

// Stage runtime implementation
type StageRuntime<'In, 'Out>(
    stage: Stage<'In, 'Out>,
    backpressure: BackpressureController option,
    circuitBreaker: CircuitBreaker option,
    metrics: IMetrics,
    logger: ILogger) =
    
    let mutable processedCount = 0L
    let mutable errorCount = 0L
    let mutable lastError = None
    let startTime = DateTimeOffset.UtcNow
    
    member _.Stage = stage
    
    member _.RecordSuccess(latency: TimeSpan) =
        Interlocked.Increment(&processedCount) |> ignore
        circuitBreaker |> Option.iter (fun cb -> cb.RecordResult(true, latency))
        
        let tags = Map.ofList [("stage", stage.Id); ("status", "success")]
        metrics.RecordCounter("stage_processed", tags, 1L)
        metrics.RecordHistogram("stage_latency", tags, latency.TotalMilliseconds)
        
    member _.RecordError(ex: Exception, latency: TimeSpan) =
        Interlocked.Increment(&errorCount) |> ignore
        lastError <- Some ex
        circuitBreaker |> Option.iter (fun cb -> cb.RecordResult(false, latency))
        
        let tags = Map.ofList [("stage", stage.Id); ("status", "error"); ("error_type", ex.GetType().Name)]
        metrics.RecordCounter("stage_errors", tags, 1L)
        logger.LogError($"Stage {stage.Id} error", ex)
        
    member _.CanProcess() =
        match circuitBreaker with
        | Some cb -> cb.CanExecute()
        | None -> true
        
    member _.GetRuntimeState() : StageRuntimeState =
        let uptime = DateTimeOffset.UtcNow - startTime
        {
            StageId = stage.Id
            State = if circuitBreaker |> Option.map (fun cb -> cb.GetState()) = Some (Open DateTimeOffset.MinValue) then "Open" else "Running"
            InputQueueDepth = 0 // Would be set by engine
            OutputQueueDepth = 0 // Would be set by engine
            ProcessedCount = processedCount
            ErrorCount = errorCount
            LastError = lastError
            Metrics = Map.ofList [
                ("throughput", float processedCount / uptime.TotalSeconds)
                ("error_rate", if processedCount > 0L then float errorCount / float processedCount else 0.0)
            ]
        }

// Base execution engine with common functionality
[<AbstractClass>]
type BaseExecutionEngine(options: ExecutionOptions) =
    let stages = ConcurrentDictionary<string, obj>()
    let runtimes = ConcurrentDictionary<string, obj>()
    let channels = ConcurrentDictionary<string * string, obj>()
    let cts = new CancellationTokenSource()
    let metrics = options.Metrics
    let logger = options.Logger
    
    let createRuntime<'In, 'Out> (node: StageNode) (stage: Stage<'In, 'Out>) =
        let backpressure = 
            node.Backpressure 
            |> Option.map BackpressureFactory.create
            
        let circuitBreaker =
            options.Policies.CircuitBreakerPolicies.TryFind stage.Id
            |> Option.map (fun policy -> CircuitBreaker(policy, metrics))
            
        StageRuntime(stage, backpressure, circuitBreaker, metrics, logger)
        
    let createChannel (edge: Edge) =
        match edge.Channel.Ordering with
        | Unordered | Ordered ->
            StreamAdapters.createBoundedChannel edge.Channel.BufferSize
        | PartitionOrdered keyExtractor ->
            // Would create partitioned channel
            StreamAdapters.createBoundedChannel edge.Channel.BufferSize
            
    abstract member ConnectStages : source:obj * sink:obj * edge:Edge -> unit
    abstract member ExecuteStage : runtime:obj * input:obj * output:obj -> Task
    
    member this.BuildGraph(graph: PipelineGraph) =
        // Create all stages and their runtimes
        for node in graph.Nodes do
            let stage = node.Stage
            stages.[node.Stage.GetType().GetProperty("Id").GetValue(node.Stage) :?> string] <- stage
            
            // Create runtime wrapper based on stage type
            let stageType = stage.GetType()
            let interfaces = stageType.GetInterfaces()
            let stageInterface = 
                interfaces 
                |> Array.find (fun i -> 
                    i.IsGenericType && 
                    i.GetGenericTypeDefinition() = typedefof<Stage<_,_>>)
                    
            let genericArgs = stageInterface.GetGenericArguments()
            let runtimeType = typedefof<StageRuntime<_,_>>.MakeGenericType(genericArgs)
            let runtime = Activator.CreateInstance(runtimeType, [| stage; node.Backpressure; None; metrics; logger |])
            runtimes.[node.Stage.GetType().GetProperty("Id").GetValue(node.Stage) :?> string] <- runtime
            
        // Create channels and connect stages
        for edge in graph.Edges do
            let source, sink = createChannel edge
            channels.[(edge.From, edge.To)] <- (source, sink)
            
            if stages.ContainsKey(edge.From) && stages.ContainsKey(edge.To) then
                this.ConnectStages(stages.[edge.From], stages.[edge.To], edge)
                
    member _.GetStageRuntime(stageId: string) =
        match runtimes.TryGetValue(stageId) with
        | true, runtime -> Some runtime
        | false, _ -> None
        
    interface ExecutionEngine with
        member this.Run(graph, opts) =
            task {
                try
                    // Build the graph
                    this.BuildGraph(graph)
                    
                    // Initialize all stages
                    let! initTasks = 
                        graph.Nodes
                        |> List.map (fun node ->
                            let stage = node.Stage
                            let ctx = {
                                PipelineId = graph.Metadata.TryFind "pipelineId" |> Option.defaultValue "default"
                                StageId = stage.GetType().GetProperty("Id").GetValue(stage) :?> string
                                Config = null
                                Logger = logger
                                Tracer = options.Tracer
                            }
                            let initMethod = stage.GetType().GetMethod("Init")
                            initMethod.Invoke(stage, [| ctx |]) :?> ValueTask |> fun vt -> vt.AsTask())
                        |> Task.WhenAll
                        
                    // Start execution
                    let startTime = DateTimeOffset.UtcNow
                    let executionTasks = ResizeArray<Task>()
                    
                    // Execute each stage
                    for node in graph.Nodes do
                        let runtime = runtimes.[node.Stage.GetType().GetProperty("Id").GetValue(node.Stage) :?> string]
                        let stageId = node.Stage.GetType().GetProperty("Id").GetValue(node.Stage) :?> string
                        
                        // Find input and output channels
                        let inputChannels = 
                            graph.Edges 
                            |> List.filter (fun e -> e.To = stageId)
                            |> List.map (fun e -> channels.[(e.From, e.To)])
                            
                        let outputChannels =
                            graph.Edges
                            |> List.filter (fun e -> e.From = stageId)
                            |> List.map (fun e -> channels.[(e.From, e.To)])
                            
                        let task = this.ExecuteStage(runtime, inputChannels, outputChannels)
                        executionTasks.Add(task)
                        
                    // Wait for completion or cancellation
                    let! _ = Task.WhenAll(executionTasks)
                    
                    let duration = DateTimeOffset.UtcNow - startTime
                    let totalProcessed = 
                        runtimes.Values 
                        |> Seq.sumBy (fun r -> 
                            let state = r.GetType().GetMethod("GetRuntimeState").Invoke(r, [||]) :?> StageRuntimeState
                            state.ProcessedCount)
                            
                    let totalErrors =
                        runtimes.Values
                        |> Seq.sumBy (fun r ->
                            let state = r.GetType().GetMethod("GetRuntimeState").Invoke(r, [||]) :?> StageRuntimeState
                            state.ErrorCount)
                            
                    return {
                        Success = totalErrors = 0L
                        ProcessedCount = totalProcessed
                        ErrorCount = totalErrors
                        Duration = duration
                        FinalStates = 
                            runtimes 
                            |> Seq.map (fun kvp -> 
                                let state = kvp.Value.GetType().GetMethod("GetRuntimeState").Invoke(kvp.Value, [||]) :?> StageRuntimeState
                                kvp.Key, state)
                            |> Map.ofSeq
                    }
                    
                with ex ->
                    logger.LogError("Pipeline execution failed", ex)
                    return {
                        Success = false
                        ProcessedCount = 0L
                        ErrorCount = 1L
                        Duration = TimeSpan.Zero
                        FinalStates = Map.empty
                    }
            }
            
        member _.Inspect(stageId) =
            match runtimes.TryGetValue(stageId) with
            | true, runtime -> 
                runtime.GetType().GetMethod("GetRuntimeState").Invoke(runtime, [||]) :?> StageRuntimeState
            | false, _ ->
                {
                    StageId = stageId
                    State = "NotFound"
                    InputQueueDepth = 0
                    OutputQueueDepth = 0
                    ProcessedCount = 0L
                    ErrorCount = 0L
                    LastError = None
                    Metrics = Map.empty
                }
                
        member _.Stop() =
            task {
                cts.Cancel()
                
                // Close all stages
                let! closeTasks =
                    stages.Values
                    |> Seq.map (fun stage ->
                        let closeMethod = stage.GetType().GetMethod("Close")
                        closeMethod.Invoke(stage, [||]) :?> ValueTask |> fun vt -> vt.AsTask())
                    |> Task.WhenAll
                    
                cts.Dispose()
            }

// Synchronous execution engine for testing
type SynchronousEngine(options: ExecutionOptions) =
    inherit BaseExecutionEngine(options)
    
    override this.ConnectStages(source, sink, edge) =
        // In sync mode, we directly connect without channels
        ()
        
    override this.ExecuteStage(runtime, input, output) =
        task {
            // Synchronous execution - process all items sequentially
            let stage = runtime.GetType().GetProperty("Stage").GetValue(runtime)
            let processMethod = stage.GetType().GetMethod("Process")
            
            // Create a simple enumerable wrapper
            let inputStream = 
                if List.isEmpty (input :?> obj list) then
                    // Source stage - create empty stream
                    AsyncEnumerable.empty<Envelope<obj>>()
                else
                    // Transform/sink stage - combine inputs
                    AsyncEnumerable.empty<Envelope<obj>>()
                    
            let outputStream = processMethod.Invoke(stage, [| options.Cancellation; inputStream |]) :?> IAsyncEnumerable<Envelope<obj>>
            
            // Process all items
            let enumerator = outputStream.GetAsyncEnumerator(options.Cancellation)
            let mutable hasNext = true
            
            while hasNext do
                let! moveNext = enumerator.MoveNextAsync()
                hasNext <- moveNext
                
                if hasNext then
                    let item = enumerator.Current
                    // In sync mode, directly pass to next stage
                    ()
                    
            do! enumerator.DisposeAsync()
        }

// Async/Task based execution engine
type AsyncTaskEngine(options: ExecutionOptions) =
    inherit BaseExecutionEngine(options)
    
    let connections = ConcurrentDictionary<string * string, obj * obj>()
    
    override this.ConnectStages(source, sink, edge) =
        let sourceChannel, sinkChannel = StreamAdapters.createBoundedChannel edge.Channel.BufferSize
        connections.[(edge.From, edge.To)] <- (sourceChannel, sinkChannel)
        
    override this.ExecuteStage(runtime, input, output) =
        task {
            let stage = runtime.GetType().GetProperty("Stage").GetValue(runtime) :?> Stage<obj, obj>
            let rt = runtime :?> StageRuntime<obj, obj>
            
            // Create input stream from channels
            let inputStream = 
                match input :?> obj list with
                | [] -> AsyncEnumerable.empty<Envelope<obj>>()
                | channels -> 
                    // Merge multiple input channels
                    AsyncEnumerable.empty<Envelope<obj>>() // Simplified
                    
            // Process through stage
            let outputStream = stage.Process(options.Cancellation, inputStream)
            
            // Write to output channels
            let outputSinks = output :?> obj list
            
            let enumerator = outputStream.GetAsyncEnumerator(options.Cancellation)
            let mutable hasNext = true
            
            while hasNext && not options.Cancellation.IsCancellationRequested do
                try
                    let startTime = DateTimeOffset.UtcNow
                    let! moveNext = enumerator.MoveNextAsync()
                    hasNext <- moveNext
                    
                    if hasNext then
                        let item = enumerator.Current
                        let latency = DateTimeOffset.UtcNow - startTime
                        
                        // Check circuit breaker
                        if rt.CanProcess() then
                            // Write to all output channels
                            for sink in outputSinks do
                                let s = sink :?> ISink<obj>
                                do! s.WriteAsync(item, options.Cancellation)
                                
                            rt.RecordSuccess(latency)
                        else
                            // Circuit open, skip processing
                            ()
                            
                with ex ->
                    rt.RecordError(ex, TimeSpan.Zero)
                    // Apply error policy
                    match stage.GetType().GetProperty("ErrorPolicy") with
                    | null -> reraise()
                    | prop ->
                        match prop.GetValue(stage) with
                        | :? ErrorPolicy as policy ->
                            match policy with
                            | Retry(maxRetries, backoff) ->
                                // Retry logic would go here
                                ()
                            | DeadLetter ->
                                // Send to dead letter queue
                                ()
                            | Fail -> reraise()
                            | Ignore -> ()
                        | _ -> reraise()
                    
            do! enumerator.DisposeAsync()
        }

// Factory for creating execution engines
module ExecutionEngineFactory =
    
    let create (engineType: string) (options: ExecutionOptions) : ExecutionEngine =
        match engineType.ToLowerInvariant() with
        | "sync" | "synchronous" -> 
            SynchronousEngine(options) :> ExecutionEngine
        | "async" | "task" -> 
            AsyncTaskEngine(options) :> ExecutionEngine
        | "hopac" -> 
            // Would create HopacEngine
            AsyncTaskEngine(options) :> ExecutionEngine
        | "dataflow" ->
            // Would create DataflowEngine
            AsyncTaskEngine(options) :> ExecutionEngine
        | _ ->
            AsyncTaskEngine(options) :> ExecutionEngine
            
    let createHybrid (stageEngineMap: Map<string, string>) (options: ExecutionOptions) : ExecutionEngine =
        // Would create a hybrid engine that uses different engines per stage
        AsyncTaskEngine(options) :> ExecutionEngine

// Helper module for async enumerable operations
module AsyncEnumerable =
    
    let empty<'T>() : IAsyncEnumerable<'T> =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(_) =
                { new IAsyncEnumerator<'T> with
                    member _.Current = Unchecked.defaultof<'T>
                    member _.MoveNextAsync() = ValueTask<bool>(false)
                    member _.DisposeAsync() = ValueTask.CompletedTask
                }
        }
        
    let singleton<'T> (item: 'T) : IAsyncEnumerable<'T> =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(_) =
                let mutable consumed = false
                { new IAsyncEnumerator<'T> with
                    member _.Current = item
                    member _.MoveNextAsync() = 
                        if consumed then
                            ValueTask<bool>(false)
                        else
                            consumed <- true
                            ValueTask<bool>(true)
                    member _.DisposeAsync() = ValueTask.CompletedTask
                }
        }
        
    let ofSeq<'T> (items: 'T seq) : IAsyncEnumerable<'T> =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(_) =
                let enumerator = items.GetEnumerator()
                { new IAsyncEnumerator<'T> with
                    member _.Current = enumerator.Current
                    member _.MoveNextAsync() = ValueTask<bool>(enumerator.MoveNext())
                    member _.DisposeAsync() = 
                        enumerator.Dispose()
                        ValueTask.CompletedTask
                }
        }