namespace Flux.IO

(*
    Base Layer:

    Suited to synchronous, envelope-by-envelope processing models.

    ```fsharp
    // Pipeline: bytes -> string -> json
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
    ```

    This design has several important implications:

    - Memory efficiency (minimal accumulation of state between envelopes)
    - Simple concurrency model (each envelope is processed atomically through the pipeline)
    - Easy testing of processors (without worrying about long-running state)
    - Natural batching point for processing multiple envelopes concurrently

    Next Steps

    what we've built is perfect for the synchronous, envelope-by-envelope processing model, and we shouldn't change it. Instead, we need to layer on top of it to support the more complex scenarios you've described.

    Key Architectural Insights

    Granularity Control: We need to control when async tasks are spawned - not for every token, but for meaningful work units (complete documents, batches, etc.)
    Long-Running vs On-Demand: Some stages need to run continuously (socket readers), while others should activate on-demand (JSON parsers)
    Splitting and Joining: Content-based routing with eventual convergence
    Parallel Sub-Pipelines: Stages that internally parallelize work using different execution engines
    Lifecycle Management: Some pipelines run forever, others complete and need to be recreated

    Proposed Layered Architecture
    We need to distinguish between:

    - Processing Logic (what we built with Flow/StreamProcessor) - handles individual envelopes
    - Topology (what the DSL describes) - the shape of the pipeline with splits/joins
    - Runtime Orchestration (what we need to add) - manages channels, lifecycle, and execution

    The architecture document's PipelineGraph with StageNode and Edge is the right 
    abstraction for topology. What we need is a Runtime that can interpret this graph and:

    Create channels/queues between stages based on Edge descriptors
    Manage stage lifecycle (on-demand vs persistent vs continuous)
    Handle split/join orchestration
    Coordinate backpressure across different execution contexts
    Monitor and adapt based on performance metrics
*)

module Core =

    open FSharp.Control
    open FSharp.HashCollections
    open FSharpPlus
    open System.Collections.Generic
    open System
    open System.Threading
    open System.Threading.Tasks

    type Timestamp = int64

    type TraceContext = {
        TraceId: string
        SpanId: string
        Baggage: HashMap<string, string>
    }

    type Cost = {
        Bytes: int
        CpuHint: float
    }

    type Envelope<'T> = { 
        Payload : 'T
        Headers : HashMap<string,string>
        SeqId   : int64
        SpanCtx : TraceContext
        Ts      : Timestamp
        Attrs   : HashMap<string,obj>
        Cost    : Cost
    } with 
        static member create<'T> seqId (payload: 'T) =
            { 
                Payload = payload
                Headers = HashMap.empty
                SeqId = seqId
                SpanCtx = { TraceId = ""; SpanId = ""; Baggage = HashMap.empty }
                Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                Attrs = HashMap.empty
                Cost = { Bytes = 0; CpuHint = 0.0 }
            }

    type Batch<'T> = {
        Items: Envelope<'T> array
        WindowStart: Timestamp
        WindowEnd: Timestamp
        TotalBytes: int
        TotalItems: int
    }

    type StageKind =
        | Source
        | Transform
        | Accumulator
        | Branch
        | Merge
        | Sink

    // Stream type definitions
    type TaskAsyncSeq<'T> = FSharp.Control.TaskSeq<'T>
    type AsyncSeq<'T> = FSharp.Control.AsyncSeq<'T>

    // Execution environment
    type IMetrics =
        abstract RecordCounter: name: string * tags: HashMap<string,string> * value: int64 -> unit
        abstract RecordGauge: name: string * tags: HashMap<string,string> * value: float -> unit
        abstract RecordHistogram: name: string * tags: HashMap<string,string> * value: float -> unit

    type ITracer =
        abstract StartSpan: name: string -> parent: TraceContext option -> TraceContext
        abstract EndSpan: ctx: TraceContext -> unit
        abstract AddEvent: ctx: TraceContext -> name:string -> attrs:HashMap<string,obj> -> unit

    [<Interface>]
    type ILogger =
        abstract member Log: level:string * message: string -> unit
        abstract member LogError: message: string * exn: exn -> unit

    type IMemoryPool =
        abstract RentBuffer: size:int -> ArraySegment<byte>
        abstract ReturnBuffer: buffer: ArraySegment<byte> -> unit

    type ExecutionEnv = { 
        Metrics  : IMetrics
        Tracer   : ITracer
        Logger   : ILogger
        Memory   : IMemoryPool
    }

    // Flow monad - abstracts over async computation with environment access
    type Flow<'a> = 
        | Flow of (ExecutionEnv -> CancellationToken -> ValueTask<'a>)

    module Flow =
        // Basic operations
        let inline run env ct (Flow f) = f env ct
        
        let inline ret (x: 'a) : Flow<'a> = 
            Flow (fun _ _ -> ValueTask<'a>(x))
        
        let inline zero () : Flow<unit> = 
            Flow (fun _ _ -> ValueTask<unit>(()))
            
        (* TODO: test monadic bind *)
        let bind 
                (f: 'a -> Flow<'b>) 
                (Flow m: Flow<'a>) : Flow<'b> =
            Flow (fun env ct ->
                let va = m env ct
                if va.IsCompletedSuccessfully then
                    let (Flow g) = f va.Result
                    g env ct
                else
                    // Create a ValueTask from the async operation
                    let task = task {
                        let! a = va
                        let (Flow g) = f a
                        return! g env ct
                    }
                    ValueTask<'b> task
            )
            
        // Computation expression builder
        type FlowBuilder() =
            member _.Return(x) = ret x
            member _.ReturnFrom(m: Flow<_>) = m
            member _.Zero() = zero()
            member _.Bind(m, f) = bind f m
            member _.Delay(f) = f()
            member _.Combine(m1: Flow<unit>, m2: Flow<'a>) = 
                bind (fun () -> m2) m1
            member _.For(sequence: seq<'T>, body: 'T -> Flow<unit>) : Flow<unit> =
                Flow (fun env ct ->
                    let task = task {
                        for item in sequence do
                            do! (body item |> run env ct)
                    }
                    ValueTask<unit>(task)
                )
                
        let flow = FlowBuilder()
        
        // Utility functions
        let map (f: 'a -> 'b) (m: Flow<'a>) : Flow<'b> = 
            bind (f >> ret) m
            
        let apply (mf: Flow<'a -> 'b>) (ma: Flow<'a>) : Flow<'b> =
            bind (fun f -> map f ma) mf
            
        // Lift various async types into Flow
        let liftTask (t: Task<'a>) : Flow<'a> =
            Flow (fun _ ct ->
                if t.IsCompletedSuccessfully then 
                    ValueTask<'a> t.Result
                else 
                    task {
                        let! result = t
                        return result
                    } |> ValueTask<'a>
            )
            
        let liftValueTask (vt: ValueTask<'a>) : Flow<'a> =
            Flow (fun _ _ -> vt)
            
        let liftAsync (computation: Async<'a>) : Flow<'a> =
            Flow (fun _ ct ->
                let task = Async.StartAsTask(computation, cancellationToken = ct)
                ValueTask<'a>(task)
            )
            
        // Environment access
        let ask : Flow<ExecutionEnv> =
            Flow (fun env _ -> ValueTask<ExecutionEnv> env)
            
        let asks (f: ExecutionEnv -> 'a) : Flow<'a> =
            Flow (fun env _ -> ValueTask<'a>(f env))
            
        let local (f: ExecutionEnv -> ExecutionEnv) (m: Flow<'a>) : Flow<'a> =
            Flow (fun env ct -> 
                let (Flow g) = m
                g (f env) ct
            )
            
        // Cancellation token access
        let getCancellationToken : Flow<CancellationToken> =
            Flow (fun _ ct -> ValueTask<CancellationToken>(ct))
            
        // Timeout support
        let withTimeout (timeout: System.TimeSpan) (m: Flow<'a>) : Flow<'a option> =
            Flow (fun env ct ->
                let task = task {
                    use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
                    cts.CancelAfter(timeout)
                    try
                        let! result = run env cts.Token m
                        return Some result
                    with
                    | :? OperationCanceledException when ct.IsCancellationRequested |> not ->
                        return None
                }
                ValueTask<'a option> task
            )
            
        // Error handling
        let catch (m: Flow<'a>) : Flow<Result<'a, exn>> =
            Flow (fun env ct ->
                let task = task {
                    try
                        let! result = run env ct m
                        return Ok result
                    with ex ->
                        return Error ex
                }
                ValueTask<Result<'a, exn>> task
            )
            
        let tryFinally (m: Flow<'a>) (finalizer: unit -> unit) : Flow<'a> =
            Flow (fun env ct ->
                let task = task {
                    try
                        return! run env ct m
                    finally
                        finalizer()
                }
                ValueTask<'a> task
            )

    // Stream processing monad that builds on Flow
    type StreamProcessor<'TIn, 'TOut> = 
        | StreamProcessor of (Envelope<'TIn> -> Flow<StreamCommand<'TOut>>)
        
    and StreamCommand<'T> =
        | Emit of Envelope<'T>
        | EmitMany of Envelope<'T> list
        | Consume
        | Complete
        | Error of exn
        
    // Helper function to transform envelope payload
    let mapEnvelope (f: 'a -> 'b) (env: Envelope<'a>) : Envelope<'b> =
        { 
            Payload = f env.Payload
            Headers = env.Headers
            SeqId = env.SeqId
            SpanCtx = env.SpanCtx
            Ts = env.Ts
            Attrs = env.Attrs
            Cost = env.Cost
        }
        
    module StreamProcessor =
        open Flow
        
        // Run a processor on a single envelope
        let runProcessor (StreamProcessor f) env = f env
        
        // Lift a pure function into a processor
        let lift (f: 'a -> 'b) : StreamProcessor<'a, 'b> =
            StreamProcessor (fun env ->
                flow {
                    return Emit (mapEnvelope f env)
                }
            )
            
        // Monadic bind for stream processors
        let bind 
                (f: 'b -> StreamProcessor<'b, 'c>) 
                (StreamProcessor p: StreamProcessor<'a, 'b>) : StreamProcessor<'a, 'c> =
            StreamProcessor (fun env ->
                flow {
                    let! cmd = p env
                    match cmd with
                    | Emit outEnv ->
                        // outEnv has type Envelope<'b>, which is what we need
                        let (StreamProcessor g) = f outEnv.Payload
                        return! g outEnv
                    | EmitMany envs ->
                        let results = ResizeArray<Envelope<'c>>()
                        for outEnv in envs do
                            // Each outEnv has type Envelope<'b>
                            let (StreamProcessor g) = f outEnv.Payload
                            let! res = g outEnv
                            match res with
                            | Emit e -> results.Add(e)
                            | EmitMany es -> results.AddRange(es)
                            | _ -> ()
                        return EmitMany (results |> List.ofSeq)
                    | Consume -> return Consume
                    | Complete -> return Complete
                    | Error e -> return Error e
                }
            )
            
        // Kleisli composition
        let compose 
                (f: 'b -> StreamProcessor<'b, 'c>) 
                (g: 'a -> StreamProcessor<'a, 'b>) : 'a -> StreamProcessor<'a, 'c> =
            fun a -> bind f (g a)
            
        // Filter processor
        let filter (predicate: 'a -> bool) : StreamProcessor<'a, 'a> =
            StreamProcessor (fun env ->
                flow {
                    if predicate env.Payload then
                        return Emit env
                    else
                        return Consume
                }
            )
            
        // Stateful processor
        let stateful 
                (initial: 'state) 
                (f: 'state -> 'a -> ('state * 'b option)) : StreamProcessor<'a, 'b> =
            let state = ref initial
            StreamProcessor (fun env ->
                flow {
                    let newState, result = f state.Value env.Payload
                    state.Value <- newState
                    match result with
                    | Some value -> 
                        return Emit (mapEnvelope (fun _ -> value) env)
                    | None -> return Consume
                }
            )
            
        // Access execution environment
        let withEnv (f: ExecutionEnv -> 'a -> Flow<'b>) : StreamProcessor<'a, 'b> =
            StreamProcessor (fun env ->
                flow {
                    let! execEnv = ask
                    let! result = f execEnv env.Payload
                    return Emit (mapEnvelope (fun _ -> result) env)
                }
            )
            
        // Log processor
        let log (level: string) : StreamProcessor<'a, 'a> =
            StreamProcessor (fun env ->
                flow {
                    let! execEnv = ask
                    let _ = execEnv.Logger.Log(level, sprintf "Processing: %A" env.Payload)
                    return Emit env
                }
            )
            
        // Metrics processor
        let recordMetric (name: string) (getValue: 'a -> float) : StreamProcessor<'a, 'a> =
            StreamProcessor (fun env ->
                flow {
                    let! execEnv = ask
                    let value = getValue env.Payload
                    execEnv.Metrics.RecordHistogram(name, HashMap.empty, value)
                    return Emit env
                }
            )

    // Convert between stream types
    let toTaskSeq (asyncSeq: AsyncSeq<'T>) : TaskAsyncSeq<'T> =
        asyncSeq |> AsyncSeq.toAsyncEnum
        
    let toAsyncSeq (taskSeq: TaskAsyncSeq<'T>) : AsyncSeq<'T> =
        taskSeq |> AsyncSeq.ofAsyncEnum