namespace Flux.IO

module Core2 = 

    open System
    open System.Threading
    open FSharp.HashCollections

    // Lightweight immutable attribute map (placeholder – can swap with custom fast map later)
    type AttrMap = HashMap<string, obj>

    [<Struct>]
    type SpanCtx = { traceId: string; spanId: string }

    [<Struct>]
    type Cost = { bytes: int; cpuHint: float }

    [<Struct>]
    type Envelope<'a> =
        { seqId   : int64
          payload : 'a
          ts      : int64
          trace   : SpanCtx
          attrs   : AttrMap
          cost    : Cost }

    module Envelope =
        let inline map f (e: Envelope<'a>) =
            { e with payload = f e.payload }

    /// Latency & throughput hints
    type LatencyClass = UltraLow | Low | Normal | High
    type ThroughputClass = LowT | MediumT | HighT | ExtremeT

    type PreferredBackend = Auto | Sync | Async | Hopac | Tpl

    /// Declarative non-functional trait set
    type StageTraits =
      { needsState        : bool
        needsKeyedState   : bool
        needsWindowing    : bool
        needsMetrics      : bool
        needsBackpressure : bool
        needsAsync        : bool
        needsParallel     : bool
        preferredBackend  : PreferredBackend
        parallelHint      : int option
        fusionBarrier     : bool
        latencyHint       : LatencyClass
        throughputHint    : ThroughputClass } with
        static member Empty =
          { needsState=false; needsKeyedState=false; needsWindowing=false
            needsMetrics=false; needsBackpressure=false; needsAsync=false
            needsParallel=false; preferredBackend=Auto; parallelHint=None
            fusionBarrier=false; latencyHint=Normal; throughputHint=MediumT }

    /// Async token abstraction to hide backend specifics
    [<Struct>]
    type AsyncToken<'a> = internal AsyncToken of obj // actual handle hidden

    /// Stage instruction / result set (interpreted by runtime)
    type StageResult<'a> =
    | Emit        of Envelope<'a>
    | EmitMany    of Envelope<'a> list
    | NeedMore
    | RequestCredits of int * (unit -> StageResult<'a>)
    | ScheduleAfter  of TimeSpan * (unit -> StageResult<'a>)
    | StartAsync     of AsyncToken<'a> * ('a -> StageResult<'a>)
    | Complete
    | Fault of exn

    /// Stage context passed to logic (read-only view)
    type StageContext =
      { nowUnix      : unit -> int64
        creditsAvail : unit -> int
        takeCredit   : int -> bool
        getState     : string -> obj voption
        putState     : string -> obj -> unit
        metrics      : (string * float -> unit)
        traceLog     : (string -> unit)
        partitionKey : Envelope<obj> -> int } // simplified

    /// Stage logic delegate (pure function of context & envelope to instruction)
    type StageLogic<'In,'Out> = StageContext -> Envelope<'In> -> StageResult<'Out>

    /// Structural stage program (logic + traits)
    type StageProgram<'In,'Out> =
      { id      : string
        traits  : StageTraits
        logic   : StageLogic<'In,'Out> }

    /// Planning identifiers
    [<Struct>] type StageId = StageId of string
    [<Struct>] type SegmentId = SegmentId of string

    /// Segment execution specification
    type SegmentPlan =
      { segmentId : SegmentId
        stages    : StageId list
        traits    : StageTraits
        backend   : PreferredBackend
        parallel  : int option }

    type BridgePlan =
      { fromSeg : SegmentId
        toSeg   : SegmentId
        capacity: int
        dropping: bool }

    type ExecutionPlan =
      { segments : SegmentPlan list
        bridges  : BridgePlan list }

    /// Scheduler operations (deterministic modeling)
    type SchedulerOp =
        | ChannelRead       of SegmentId
        | ChannelWrite      of SegmentId
        | AsyncComplete     of SegmentId * int // token ordinal
        | TimerFired        of SegmentId * int
        | PartitionDispatch of SegmentId * int

    [<Struct>]
    type TraceEvent =
      { tick      : int64
        op        : SchedulerOp
        segmentId : SegmentId
        meta      : int64 }

    /// Backpressure policy skeleton
    type BackpressurePolicy =
        | CreditBased of initial:int * replenish:int
        | BoundedChannel of capacity:int
        | AdaptiveHybrid

    /// Simple channel result tags
    type ChannelWriteResult = WriteOk | WriteBlocked | WriteDropped | WriteClosed

    /// Runtime channel abstraction (backend hiding)
    type RuntimeChannel<'a> =
        abstract tryWrite : Envelope<'a> -> ChannelWriteResult
        abstract tryRead  : unit -> Envelope<'a> voption
        abstract depth    : unit -> int
        abstract complete : unit -> unit

    /// Backend interface to schedule segment work
    type ExecutionBackend =
        abstract name : string
        abstract runStep :
            SegmentId * (unit -> StageResult<obj>) -> StageResult<obj>

    /// Deterministic schedule controller
    type ScheduleController =
        abstract pickNext : ready:SchedulerOp list -> SchedulerOp
        abstract record   : TraceEvent -> unit
        abstract replay   : TraceEvent list -> unit

    /// Adaptive signal placeholder
    type AdaptiveSignal =
        | IncreaseParallelism of SegmentId
        | DecreaseParallelism of SegmentId
        | ExpandCredits of SegmentId
        | ContractCredits of SegmentId

    /// Control plane hook
    type ControlPlane =
        abstract publishSignal : AdaptiveSignal -> unit
    
    /// Classification hints (carried through traits + planning)
    type EffectClass =
        | EffectIO
        | EffectCPU
        | EffectTimer
        | EffectComposite
        | EffectExternal of string

    /// Typed external (impure) operation handle.
    /// This is a *descriptor* + lifecycle controller. Creation does NOT start the effect.
    /// A backend decides when to call Start() and how to await completion.
    /// No backend-specific types (Task / Job / etc.) leak through this surface.
    type ExternalHandle<'T> =
        abstract member Id            : int
        abstract member Class         : EffectClass
        abstract member IsStarted     : bool
        abstract member IsCompleted   : bool
        abstract member Start         : unit -> unit
        abstract member TryPoll       : unit -> 'T option          // Non-blocking check
        // abstract member Await         : CancellationToken -> Task<'T>          // Internal use
        // abstract member AwaitTimeout  : CancellationToken * TimeSpan -> Task<'T option>
        abstract member Cancel        : unit -> unit

    type FlowEnv =
        { timeUnix : unit -> int64
          log      : (string -> unit)
          metric   : (string * float -> unit)
          services : HashMap<string, obj> 
        }

    /// A specification of an external effect returning a typed handle when materialized.
    /// NOTE: Build MUST be pure and free of side effects (it just packages a handle).
    type ExternalSpec<'T> =
        { build      : FlowEnv -> ExternalHandle<'T>
          classify   : EffectClass
          debugLabel : string option }

    type Flow<'T> =
        | Pure     of 'T
        | Sync     of (FlowEnv -> 'T)
        | External of ExternalSpec<'T>
        | Map      of Flow<'T> * ('T -> 'T)
        | Bind     of Flow<'T> * ('T -> Flow<'T>)

    module Flow =
        let pure' x = Pure x
        
        let deterministic f = Sync f

        let rec map f m =
            match m with
            | Pure v     -> Pure (f v)
            | Sync g     -> Sync (fun env -> f (g env))
            | Map (x,g)  -> map (f << g) x
            | External e -> Map(m, f)   // NB: lazy
            | Bind (x,k) -> Bind(x, k >> map f)

        let rec bind k m =
            match m with
            | Pure v -> k v
            | Sync g ->
                Sync (fun env ->
                    let v = g env
                    match k v with
                    | Pure v2 -> v2
                    | Sync g2 -> g2 env
                    | _       -> failwith "External in pure sync path requires runtime lowering")
            | External _   -> Bind(m, k)
            | Map (x, f)   -> bind (k << f) x
            | Bind (x, k1) -> bind (fun v -> bind k (k1 v)) x

        type FlowBuilder() =
            member _.Return x = Pure x
            
            member _.ReturnFrom m = m
            
            member _.Bind(m,k) = bind k m
            
            member _.Zero() = Pure ()
            
            member _.Delay(f: unit -> Flow<'a>) =
                Sync (fun env ->
                    match f() with
                    | Pure v -> v
                    | Sync g -> g env
                    | _ -> failwith "Delayed external requires explicit declaration.")
            
            member _.Combine(a: Flow<unit>, b: Flow<'b>) = bind (fun () -> b) a

        let flow = FlowBuilder()

        module FlowIntrospection =
            let rec hasExternal = function
                | Pure _ | Sync _ -> false
                | External _ -> true
                | Map (m,_) | Bind (m,_) -> hasExternal m
