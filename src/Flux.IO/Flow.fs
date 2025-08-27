namespace Flux.IO

module Core2 = 

    open System
    open System.Threading
    open FSharp.HashCollections

    // Lightweight immutable attribute map (placeholder)
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

    [<Struct>]
    type AsyncResult<'a> =
        | AsyncDone      of result: 'a voption
        | AsyncFailed    of reason: exn voption
        | AsyncCancelled of reason: exn voption
        | AsyncPending

    [<AbstractClass>]
    type AsyncHandle<'a>(internal token: AsyncToken<'a>) =
        member __.Token = token

        /// Check if the async operation is completed
        abstract member IsCompleted : bool

        // NB: this would be a lot cleaner with higher kinded types...
        abstract member AsTokenSource<'t> : unit -> 't option

        /// Await the completion of the async operation (blocks the caller)
        abstract member Await : AsyncResult<'a>             // Blocking await
        
        /// Await with a timeout
        abstract member AwaitTimeout : TimeSpan -> AsyncResult<'a> // Blocking await with timeout
        
        /// Cancel the async operation - returns immediately
        abstract member Cancel : unit

        abstract member CancelWait : AsyncResult<'a> // Cancel the async operation and wait for completion

        abstract member CancelWaitTimeout : TimeSpan -> AsyncResult<'a> // Cancel and wait with timeout

    /// Classification hints (carried through traits + planning)
    type EffectClass_1 =
        | EffectIO
        | EffectCPU
        | EffectTimer
        | EffectComposite
        | EffectExternal of string

    /// Effect classification hints (authoring-level / planning-level).
    type EffectClass =
        | EffectSync
        | EffectExternal of string    // e.g. "io", "net", "cpu"
        | EffectInstrument
        | EffectFuture                // placeholder for future categories
        | EffectUserDefined of string


    /// Typed external (impure) operation handle.
    /// This is a *descriptor* + lifecycle controller. Creation does NOT start the effect.
    /// A backend decides when to call Start() and how to await completion.
    /// No backend-specific types (Task / Job / etc.) leak through this surface.
    [<Interface>]
    type ExternalHandle<'T> =
        abstract member Id            : int
        abstract member Class         : EffectClass
        abstract member IsStarted     : bool
        abstract member IsCompleted   : bool
        abstract member Start         : unit -> AsyncHandle<'T>

    /// Stage instruction / result set (interpreted by runtime)
    type StageResult<'a> =
    | Emit           of Envelope<'a>
    | EmitMany       of Envelope<'a> list
    | RequestCredit  of int * (unit -> StageResult<'a>)
    | ScheduleAfter  of TimeSpan * (unit -> StageResult<'a>)
    | StartAsync     of AsyncHandle<'a> * ('a -> StageResult<'a>)
    | Consume
    | Complete
    | Fault of exn

    /// Stage context passed to logic (read-only view)
    type StageContext =
      { nowUnix      : unit -> int64
        credits      : unit -> int
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
      { segmentId   : SegmentId
        stages      : StageId list
        traits      : StageTraits
        backend     : PreferredBackend
        parallelism : int option }

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
        | CreditBased of initial: int * replenish: int
        | BoundedChannel of capacity: int
        | AdaptiveHybrid

    /// Simple channel result tags
    type ChannelWriteResult = Ok | Blocked | Dropped | Closed

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
        abstract pickNext : ready: SchedulerOp list -> SchedulerOp
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

    let foo = 
        CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None).Token
        
    type FlowEnv =
        { nowUnix  : unit -> int64
          log      : string -> unit
          metric   : (string * float) -> unit
          services : Map<string, obj> }

    /// Type witness (runtime reflection + debugging).
    type TypeTag =
        { runtime : Type
          name    : string }

    [<AutoOpen>]
    module private TypeTagHelpers =
        let inline tag<'a>() =
            let t = typeof<'a>
            { runtime = t; name = t.FullName }

    /// A specification of an external (non-deterministic) effect producing
    /// a value of type 'T when executed by the runtime.
    /// build MUST be pure: it only *packages* the handle/token.
    type ExternalSpec<'T> =
        { build      : FlowEnv -> obj         // Opaque handle (runtime interprets)
          classify   : string                 // e.g. "io", "net", "timer"
          debugLabel : string option }

    // NB: this Functor is required to complete the free-monad implementation of Flow<'a> 
    type FlowInstr<'n> =
        /// Synchronous deterministic computation (no scheduling).
        | FSync        of (FlowEnv -> obj) * TypeTag * (obj -> 'n)
        /// External effect boundary — must be scheduled / awaited by runtime.
        | FExternal    of ExternalSpec<obj> * TypeTag * (obj -> 'n)
        /// Instrumentation (metrics/logging). Unit result, direct next.
        | FInstrument  of (FlowEnv -> unit) * 'n
        // Future examples:
        // | FRequestCredit of int * (int -> 'Next)
        // | FScheduleAfter of TimeSpan * 'Next
        // | FParallel of Flow<'a> list * (obj[] -> 'Next)
        // | FAcquire of ResourceSpec<...> * (ResourceHandle -> 'Next)

    module private FlowInstr =
        /// Functor map: ('Next -> 'Next2) -> FlowInstr<'Next> -> FlowInstr<'Next2>
        let inline map (f: 'N -> 'M) (instr: FlowInstr<'N>) : FlowInstr<'M> =
            match instr with
            | FSync (g, tt, k)        -> FSync (g, tt, k >> f)
            | FExternal (spec, tt, k) -> FExternal (spec, tt, k >> f)
            | FInstrument (act, n)    -> FInstrument (act, f n)

    (* 
    
        A Flow<'T> value is a *pure structural description of a program
        description; no effects are executed during flow construction.
    
        Key Goals:
        * Lazy authoring (no eager user-code execution).
        * Explicit external (“await”) boundaries via yield! ExternalSpec<_>.
        * Clear separation of:
            Authoring  -> Flow<'T> construction
            Planning   -> Shape & trait inference (conservative here)
            Lowering   -> Conversion to Stage/Segment IR
            Runtime    -> Execution / scheduling / async orchestration
        * Extensible: Easier to add new effect nodes later (timers,
        credit requests, parallel forks, resource acquisition).
    
        This implementation is of a free monad using a Freer-style encoding
        Flow<'A> =
            | Pure of (unit -> 'A)
            | Delay of (unit -> Flow<'A>)
            | Free of FlowInstr<Flow<'A>>
            | TryWith / TryFinally
    
        Where FlowInstr<'Next> contains effect "instructions" that
        are functorial in 'Next. Each effect variant stores:
        - A function returning a result (boxed)
        - A TypeTag for the produced value
        - A continuation (obj -> 'Next) OR a direct 'Next (if unit result)
        - Metadata for classification
    
        NOTE: Continuations are *not* invoked during planning passes;
        they are only used by the runtime interpreter when an effect
        result is available. Planning that needs deeper insight into
        post-effect structure will require a later transformation or
        effect simulation layer.
        
        TODO:
        - Parallel / fork-join instructions
        - Backpressure / credit request instructions
        - Timers / scheduling (ScheduleAfter)
        - Structured resource scopes beyond simple TryFinally / Using
        - Trait-rich ExternalSpec (timeout hints, idempotency, priority)
        - A normalization pass (fusing nested maps / instruments)

    *)
    type Flow<'A> =
        | Pure       of (unit -> 'A)
        | Delay      of (unit -> Flow<'A>)
        | Free       of FlowInstr<Flow<'A>>
        | TryWith    of Flow<'A> * (exn -> Flow<'A>)
        | TryFinally of Flow<'A> * (unit -> unit)

    module FlowInternal =

        // Lazy return
        let inline pure' (x: 'A) : Flow<'A> = Pure (fun () -> x)

        // Smart constructor: synchronous deterministic action
        let inline sync (f: FlowEnv -> 'A) : Flow<'A> =
            Free (FSync ((fun env -> box (f env)), tag<'A>(), fun o -> Pure (fun () -> unbox<'A> o)))

        // Smart constructor: instrumentation
        let inline instrument (act: FlowEnv -> unit) : Flow<unit> =
            Free (FInstrument (act, Pure (fun () -> ())))

        // Smart constructor: external boundary
        let inline externalSpec (spec: ExternalSpec<'A>) : Flow<'A> =
            // Erase the result type for storage
            let erased : ExternalSpec<obj> =
                { build = spec.build
                  classify = spec.classify
                  debugLabel = spec.debugLabel }
            Free (FExternal (erased, tag<'A>(), fun o -> Pure (fun () -> unbox<'A> o)))

        // NB: This writing of monadic bind is purely structural! 
        // we do NOT execute user thunks except for evaluating a Pure's
        // stored thunk to obtain value.
        let rec bind (k: 'A -> Flow<'B>) (m: Flow<'A>) : Flow<'B> =
            match m with
            | Pure th ->
                // Evaluate the final lazy thunk to produce the value for continuation.
                // This is the *only* forced evaluation; it is a semantic 'Pure'.
                k (th())
            | Delay thunk ->
                // Preserve laziness: wrap resulting Flow in another Delay.
                Delay (fun () -> bind k (thunk()))
            | Free instr ->
                FlowInstr.map (bind k) instr |> Free
            | TryWith (body, handler) ->
                TryWith (bind k body, fun ex -> bind k (handler ex))
            | TryFinally (body, fin) ->
                TryFinally (bind k body, fin)

        // Functor map (optional convenience / potential optimization)
        let inline map (f: 'A -> 'B) (m: Flow<'A>) : Flow<'B> =
            bind (f >> pure') m

    type FlowBuilder () =

        member _.Return (x: 'A) : Flow<'A> =
            FlowInternal.pure' x

        member _.ReturnFrom (m: Flow<'A>) : Flow<'A> =
            m

        member _.Bind (m: Flow<'A>, k: 'A -> Flow<'B>) : Flow<'B> =
            FlowInternal.bind k m

        member _.Delay (thunk: unit -> Flow<'A>) : Flow<'A> =
            // Delay structural expansion
            Delay thunk

        member _.Zero () : Flow<unit> =
            FlowInternal.pure' ()

        member this.Combine (a: Flow<unit>, b: Flow<'B>) : Flow<'B> =
            this.Bind (a, fun () -> b)

        member this.While (guard: unit -> bool, body: Flow<unit>) : Flow<unit> =
            let rec loop () =
                if guard() then this.Bind(body, fun () -> this.Delay loop)
                else this.Zero()
            this.Delay loop

        member this.For (items: seq<'A>, body: 'A -> Flow<unit>) : Flow<unit> =
            let e = items.GetEnumerator()
            let rec loop () =
                if e.MoveNext() then
                    this.Bind(body e.Current, fun () -> this.Delay loop)
                else
                    this.Zero()
            this.Delay loop

        member _.TryWith (body: Flow<'A>, handler: exn -> Flow<'A>) : Flow<'A> =
            TryWith (body, handler)

        member _.TryFinally (body: Flow<'A>, fin: unit -> unit) : Flow<'A> =
            TryFinally (body, fin)

        member this.Using (resource: #IDisposable, binder: #IDisposable -> Flow<'A>) : Flow<'A> =
            // Expand to try/finally
            TryFinally (binder resource, fun () ->
                if not (isNull (box resource)) then resource.Dispose())

        member this.Using (resFlow: Flow<#IDisposable>, binder: #IDisposable -> Flow<'A>) : Flow<'A> =
            this.Bind(resFlow, fun r -> this.Using(r, binder))

        // yield! ExternalSpec<_> (explicit external boundary)
        member _.YieldFrom (spec: ExternalSpec<'A>) : Flow<'A> =
            FlowInternal.externalSpec spec

        // yield instrumentation (side-effect at runtime)
        member _.Yield (action: FlowEnv -> unit) : Flow<unit> =
            FlowInternal.instrument action

        // Source pass-through (enables: let! x = flow { ... })
        member _.Source (m: Flow<'A>) = m

    // Builder instance
    let flow = FlowBuilder()

    [<AutoOpen>]
    module Flow =
        let inline pure' x = FlowInternal.pure' x
        let inline sync f = FlowInternal.sync f
        let inline instrument act = FlowInternal.instrument act
        let inline externalEffect spec = FlowInternal.externalSpec spec
        let inline map f m = FlowInternal.map f m
        let inline bind f m = FlowInternal.bind f m

    // ------------------------------
    // Minimal Synchronous Interpreter (Prototype Only)
    // ------------------------------------------------
    // NOTE: This interpreter is intentionally partial:
    //  * It executes Pure, Delay, FSync, FInstrument.
    //  * It RAISES if it encounters FExternal.
    //  * It does not handle scheduling / async / cancellation.
    //  * Runtime layer must supply a full implementation.
    // ------------------------------
    module Interpreter =
        exception ExternalEncountered of string

        let rec run (env: FlowEnv) (program: Flow<'A>) : 'A =
            match program with
            | Pure th -> th()
            | Delay thunk -> run env (thunk())
            | TryWith (body, handler) ->
                try run env body with ex -> run env (handler ex)
            | TryFinally (body, fin) ->
                try run env body finally fin()
            | Free instr ->
                match instr with
                | FInstrument (act, next) ->
                    act env
                    run env next
                | FSync (compute, _tag, k) ->
                    let boxed = compute env
                    run env (k boxed)
                | FExternal (spec, _tag, _k) ->
                    let label = defaultArg spec.debugLabel "<external>"
                    raise (ExternalEncountered label)

    // ------------------------------
    // Trait Inference (Conservative Stub)
    // --------------------------------------------------
    // Because continuations (obj -> Flow<'A>) are opaque without executing
    // effects, a traversal *cannot* safely inspect sub-programs beyond
    // effect boundaries. This stub marks presence & counts only up to
    // encountered instructions. A future variant may:
    //   * Reify symbolic continuations
    //   * Annotate nodes with planning-time side metadata
    //   * Build a control-flow graph summarizing effect chains
    // --------------------------------------------------
    type FlowTraits =
        { syncCount         : int
          externalCount     : int
          instrumentCount   : int
          hasTry            : bool
          maxDelayNesting   : int
        // Future: parallelHint, needsBackpressure, timerCount, etc.
        }
        static member Empty =
            { syncCount = 0; externalCount = 0; instrumentCount = 0; hasTry = false; maxDelayNesting = 0 }

    module TraitInference =

        let infer (flowProg: Flow<'A>) : FlowTraits =
            let mutable traits = FlowTraits.Empty
            let rec loop depth prog =
                let updateDepth () =
                    if depth > traits.maxDelayNesting then
                        traits <- { traits with maxDelayNesting = depth }
                match prog with
                | Pure _ -> updateDepth()
                | Delay thunk ->
                    updateDepth()
                    // Do NOT force thunk deeply; just record nesting.
                    // Optionally: limit to a shallow peek.
                    loop (depth + 1) (thunk())
                | TryWith (body, handler) ->
                    traits <- { traits with hasTry = true }
                    loop depth body
                    // Do not explore handler without executing it.
                    ()
                | TryFinally (body, _) ->
                    traits <- { traits with hasTry = true }
                    loop depth body
                | Free instr ->
                    updateDepth()
                    match instr with
                    | FSync _ ->
                        traits <- { traits with syncCount = traits.syncCount + 1 }
                    | FExternal _ ->
                        traits <- { traits with externalCount = traits.externalCount + 1 }
                    | FInstrument _ ->
                        traits <- { traits with instrumentCount = traits.instrumentCount + 1 }
                    // Do not follow continuation; would require a value for obj.
            loop 0 flowProg
            traits

// ------------------------------
// Notes for Future Extensions
// --------------------------------------------------
// 1. Parallelism:
//    Add instruction variant: FParallel of Flow<'a> list * (obj[] -> 'Next)
//    Runtime can start all subflows, gather results, then resume continuation.
//    Trait inference can then set parallel hints.
//
// 2. Backpressure / Credits:
//    Instruction: FRequestCredit of int * (unit -> 'Next)
//    Lowered to StageResult.RequestCredit; continuation fires when credit granted.
//
// 3. Timers / Scheduling:
//    Instruction: FScheduleAfter of TimeSpan * 'Next
//    Runtime registers with wheel / timer heap; resumes on expiry.
//
// 4. Resource Scopes:
//    Acquire & release instructions or richer RAII-coded pattern.
//    Might encode: FAcquire of AcquireSpec * (ResourceHandle -> 'Next)
//                  with Flow exposing use! sugar mapping to TryFinally.
//
// 5. Normalization Pass:
//    A separate module can traverse the Flow tree performing:
//      - Instrument coalescing
//      - Delay flattening
//      - Map fusion (if Map node style is reintroduced)
//    to reduce runtime interpretation overhead.
//
// 6. Logging / Tracing IDs:
//    Extend FlowInstr with a lightweight monotonic node id for
//    debugging and correlation (added during authoring or a finalize pass).
// --------------------------------------------------

// ------------------------------
// Example Usage (Documentation Only)
// --------------------------------------------------
// let program = flow {
//     let! now = sync (fun env -> env.nowUnix())
//     do! instrument (fun env -> env.log (sprintf "Now: %d" now))
//     let! bytes = yield! { build = (fun _ -> obj()); classify = "io"; debugLabel = Some "read-file" }
//     return (now, bytes)
// }
//
// Running with interpreter (only works if no external):
// let env = { nowUnix = (fun () -> DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
//             log = printfn "%s"
//             metric = (fun (n,v) -> printfn "[METRIC] %s=%f" n v)
//             services = Map.empty }
//
// let traits = TraitInference.infer program
// printfn "External nodes: %d" traits.externalCount
// --------------------------------------------------