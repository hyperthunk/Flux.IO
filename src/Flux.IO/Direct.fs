namespace Flux.IO.Pipeline

(*
    Direct Pipeline API
*)
module Direct = 
    open Flux.IO.Core.Types
    open Flux.IO.Streams
    open System
    open System.Threading
    open System.Threading.Tasks

    let inline ofProg p : Flow<'T> = { Program = p }

    let inline pure' (x: 'T) : Flow<'T> =
        { Program = FPure (fun () -> x) }
    
    let ret = pure'

    let inline liftF (x: 'T) : Flow<'T> = pure' x

    let inline sync (f: ExecutionEnv -> 'T) : Flow<'T> =
        { Program = FSync f }

    let inline externalSpec (spec: ExternalSpec<'T>) : Flow<'T> =
        { Program = FExternal spec }

    let inline delay (thunk: unit -> Flow<'T>) : Flow<'T> =
        { Program = FDelay (fun () -> (thunk()).Program) }

    let inline tryWith (m: Flow<'T>) (handler: exn -> Flow<'T>) : Flow<'T> =
        { Program = FTryWith (m.Program, fun ex -> (handler ex).Program) }

    let inline tryFinally (m: Flow<'T>) (fin: unit -> unit) : Flow<'T> =
        { Program = FTryFinally (m.Program, fin) }

    
    (* 
        Note: Single-step, flattening Interpreter primitive 

        Evaluating `runExternal` has runProg evaluate a FlowProg<'T> into an EffectResult<'T>. 
        External effects are executed by obtaining an ExternalHandle<'T> and polling/waiting 
        (currently this is a naive blocking loop).
        
        Integration with scheduler or event loop is TODO/FIXME.
    *)
    let runExternal 
            (env: ExecutionEnv)
            (spec: ExternalSpec<'T>) : EffectResult<'T> =
        // TODO: Non-blocking integration with scheduler / registration
        // TODO: Timeout / cancellation bridging
        // TODO: Backpressure gating before start
        let handle = spec.Build env
        
        if handle.IsStarted then
            raise (InvalidOperationException "External effect is already started")

        let asyncHandle = handle.Start()
        
        // Register cancellation
        (* use _ = ct.Register(fun () -> 
            try asyncHandle.Cancel()
            with _ -> ()
        ) *)
        
        let result =
            try asyncHandle.Await()
            with ex ->
                // Await should not throw; normalize to AsyncFailed
                EffectFailed ex

        match result with
        | EffectOutput (ValueSome _) ->
            result
        | EffectOutput ValueNone -> 
            // TODO: could this case + EffectEnded be handled more gracefully? Maybe return None?
            raise (InvalidOperationException "External completed with no value")
        | EffectFailed ex -> 
            raise ex
        | EffectCancelled oce ->
            // Prefer preserving the provided OCE, but ensure token is visible
            raise oce
        | EffectPending ->
            // Should not happen after Await()
            raise (InvalidOperationException "Await returned Pending")
        | EffectEnded -> raise (InvalidOperationException "External ended unexpectedly")

    let rec execute
            (env: ExecutionEnv)
            (prog: FlowProg<'T>) : EffectResult<'T> =
        match prog with
        | FPure th ->
            try th() |> ValueSome |> EffectOutput
            with ex -> EffectFailed ex
        | FSync f ->
            try f env |> ValueSome |> EffectOutput
            with ex -> EffectFailed ex
        | FExternal spec ->
            runExternal env spec
        | FDelay thunk ->
            // Do not evaluate thunk early; just proceed when executed
            try execute env (thunk())
            with ex -> EffectFailed ex
        | FTryWith (body, handler) ->
            try execute env body
            with ex -> execute env (handler ex)
        | FTryFinally (body, fin) ->
            try
                let result = execute env body
                fin()
                result
            with ex ->
                fin()
                reraise()

    (* 
        Monadic Operations 
    
        NOTE: Sequencing 
        
        We run m under the current env/ct, then run k a, all inside a 
        single FSync, so there is no structural bind node and no boxing/sentinels. 
        This is a blocking composition by design (Direct path).
        
        NOTE:
        - Explicit generic ordering: k first, then m, helps solver keep 'A and 'B distinct.
        - This preserves referential transparency at the API surface while executing eagerly. 
    *)
    let inline bind<'A,'B> (k: 'A -> Flow<'B>) (m: Flow<'A>) : Flow<'B> =
        { Program =
            FSync (fun env ->
                (* let ct = Ambient.__internal_CT.Value
                let a =
                    let vt = execute env ct m.Program
                    if vt.IsCompletedSuccessfully then vt.Result else vt.AsTask().Result
                let b =
                    let vt2 = execute env ct (k a).Program
                    if vt2.IsCompletedSuccessfully then vt2.Result else vt2.AsTask().Result
                b *)
                (* let a = execute env m.Program
                let b = execute env (k a).Program *)
                match execute env m.Program with
                | EffectOutput (ValueSome a) -> 
                    match execute env (k a).Program with
                    | EffectOutput (ValueSome b) -> b
                    | EffectOutput ValueNone -> 
                        raise (InvalidOperationException "Bind continuation completed with no value")
                    | EffectFailed ex -> 
                        raise ex
                    | EffectCancelled oce -> 
                        raise oce
                    | EffectPending -> 
                        raise (InvalidOperationException "Bind continuation returned Pending")
                    | EffectEnded ->
                        raise (InvalidOperationException "Bind continuation returned Ended")
                | EffectOutput ValueNone -> 
                    raise (InvalidOperationException "Bind continuation completed with no value")
                | EffectFailed ex -> 
                    raise ex
                | EffectCancelled oce -> 
                    raise oce
                | EffectPending -> 
                    raise (InvalidOperationException "Bind continuation returned Pending")
                | EffectEnded ->
                    raise (InvalidOperationException "Bind continuation returned Ended")
            ) }

    (* let inline bind<'A,'B> (k: 'A -> Flow<'B>) (m: Flow<'A>) : Flow<'B> =
        // We produce a Flow whose execution interprets m then k.
        // This does lose intermediate structural shape (acceptable for "Direct" mode).
        let prog =
            FDelay (fun () ->
                // We reify an execution closure:
                // 1. Evaluate m (at runtime), then run k with its value.
                // Represent as pseudo-sync wrapper capturing m & k; actual sequencing in execute below.
                // We encode it using FSync + then value-thunk; but to preserve lazy semantics we choose a Delay chain.
                FPure (fun () -> Unchecked.defaultof<'B>) // placeholder sentinel; real logic in specialized execute below
            )
        // We override execution path via a custom wrapper Flow:
        // Simpler approach: store a closure inside an FSync returning an inner program.
        // We'll adopt a specialized "composed" program removed from public exposure:
        let composed =
            // custom internal node via Delay -> run m -> run k
            FDelay (fun () ->
                // Markers: we can't store heterogenous result in DU; we run it directly inside execute via nested calls.
                // We'll embed the runtime evaluation strategy in a pseudo pure node calling both sequences.
                // NOTE: This is intentionally minimal; advanced planning requires Freer translation.
                FPure (fun () -> Unchecked.defaultof<'B>)
            )
        // Wrap in a Flow with a customized execute (decorate with metadata if needed)
        { Program =
            // Use delay so side-effects of m/k construction (should be none) deferred.
            FDelay (fun () ->
                // We create a tiny trampoline by returning a pure node that, when executed through execute,
                // calls m, then k with result.
                FSync (fun env ->
                    // Because execute returns ValueTask, we need synchronous fallback only if m is sync-fast.
                    // For general case we fallback to async bridging; handled downstream by run (Flow.run).
                    // We stash env for async path below (Flow.run handles asynchronous flattening).
                    // The direct approach here: we just indicate a sentinel; real sequencing in runBind below.
                    // This sentinel is consumed by runBind.
                    Unchecked.defaultof<'B>
                )) } *)

    let inline map<'A,'B> (f: 'A -> 'B) (m: Flow<'A>) : Flow<'B> =
        bind (f >> pure') m

    let inline apply (mf: Flow<'a -> 'b>) (ma: Flow<'a>) : Flow<'b> =
        bind (fun f -> map f ma) mf

    (* 
        Execute a Flow<'T>. Because bind flattened structure by embedding logic in run,
        we handle composed cases in a unified run function. 
    *)
    let run (env: ExecutionEnv) (* (ct: CancellationToken) *) (m: Flow<'T>) : EffectResult<'T> =
        // PREVIOUS:
        // Since we used sentinel FPure/FSync scaffolding inside bind, we bypass them by
        // directly chaining at runtime:
        // Simplify: treat FSync sentinel with default value as marker— fallback to real run logic.
       execute env (* ct *) m.Program

    type FlowBuilder () =
        member _.Return (x: 'T) : Flow<'T> = pure' x
        member _.ReturnFrom (m: Flow<'T>) = m

        member _.Bind (m: Flow<'A>, k: 'A -> Flow<'B>) : Flow<'B> =
            bind k m

        member _.Bind (spec: ExternalSpec<'T>, k: 'T -> Flow<'U>) : Flow<'U> =
            bind k (externalSpec spec)

        member _.ReturnFrom (spec: ExternalSpec<'T>) : Flow<'T> =
            externalSpec spec

        member _.Zero () : Flow<unit> = pure' ()

        member _.Delay (thunk: unit -> Flow<'T>) : Flow<'T> =
            delay thunk

        member _.Combine (m1: Flow<unit>, m2: Flow<'T>) : Flow<'T> =
            bind (fun () -> m2) m1

        member this.While (guard: unit -> bool, body: Flow<unit>) : Flow<unit> =
            let rec loop () =
                if guard() then this.Bind(body, fun () -> this.Delay loop)
                else this.Zero()
            this.Delay loop

        member this.For (items: seq<'A>, body: 'A -> Flow<unit>) : Flow<unit> =
            let e = items.GetEnumerator()
            let rec loop () =
                if e.MoveNext() then this.Bind(body e.Current, fun () -> this.Delay loop)
                else this.Zero()
            this.Delay loop

        member _.TryWith (m: Flow<'T>, handler: exn -> Flow<'T>) : Flow<'T> =
            tryWith m handler

        member _.TryFinally (m: Flow<'T>, fin: unit -> unit) : Flow<'T> =
            tryFinally m fin

        member _.Using (resource: #IDisposable, binder: #IDisposable -> Flow<'T>) : Flow<'T> =
            tryFinally (binder resource) (fun () ->
                if not (isNull (box resource)) then resource.Dispose())

        member this.Using (resFlow: Flow<#IDisposable>, binder: #IDisposable -> Flow<'T>) : Flow<'T> =
            this.Bind(resFlow, fun r -> this.Using(r, binder))

        // yield! for external effect introduction
        member _.YieldFrom (spec: ExternalSpec<'T>) : Flow<'T> =
            externalSpec spec

        // yield instrumentation (as sync effect returning unit)
        member _.Yield (act: ExecutionEnv -> unit) : Flow<unit> =
            sync (fun env -> act env)

        member _.Source (m: Flow<'T>) = m

    let flow = FlowBuilder()

    module Stream =
        let emit x = Emit x
        let emitMany xs = EmitMany xs
        let consume () = Consume
        let complete () = Complete
        let fault ex = Error ex

    module Lift =

        /// Lift a Task<'T> into the Flow monad.
        let taskF(factory: unit -> Task<'T>) : Flow<'T> =
            Flux.IO.Core.Lift.task factory |> externalSpec 

        /// Lift and Async<'T> into the Flow monad.        
        let asyncF (comp: Async<'T>) : Flow<'T> = 
            Flux.IO.Core.Lift.async comp |> externalSpec

    (* Intermediary module: offload + later emission (Phase 1 refactoring) *)

    (*
        // Example usage pattern:

        let proc =
            Direct.Intermediary.forkSingle
                (fun input -> Lift.taskHandle (fun () -> someTaskProducingInt input))
                (fun env result -> sprintf "Input:%A Result:%d" env.Payload result)

        // Driver loop:
        let mutable running = true
        while running do
            let cmd = StreamProcessor.runProcessor proc inputEnvelope 
            |> Direct.run env
            |> fun r -> r.Result
            match cmd with
            | Consume -> Thread.Sleep 1
            | Emit e -> // forward downstream
            | Complete -> running <- false
            | Error ex -> failwithf "Stage fault: %A" ex
    *)
    module Intermediary =

        open FSharp.HashCollections
        open Flux.IO.Core.Types
        open Outlets

        (* Internal state for a single in-flight offloaded effect. *)
        type private SingleForkState<'In,'Eff,'Out> =
            | Idle
            | Running of original: Envelope<'In> * handle: EffectHandle<'Eff> * startedAt: int64
            | Emitting of Envelope<'Out>
            | Faulted of exn
        
        let private mapPayload<'In,'Out> 
                (payload:'Out) 
                (e:Envelope<'In>) : Envelope<'Out> =
            { 
                Payload = payload
                Headers = e.Headers
                SeqId = e.SeqId
                SpanCtx = e.SpanCtx
                Ts = e.Ts
                Attrs = e.Attrs
                Cost = e.Cost 
            }
        
        (* Private single in-flight record (immutable; replaced on mutation) *)
        type private InFlight<'In,'Out> =
            { 
                Original  : Envelope<'In>
                Outlet    : EffectOutlet<'Out>
                StartedAt : int64 
            }

        (* Attribute key configuration (all optional; when None a key is not written) *)
        type OutletAttrKeys =
            { 
                Latency     : string option
                Kind        : string option
                BatchCount  : string option
                Error       : string option
                Complete    : string option 
            } with
            static member Default =
                { 
                    Latency    = Some "fork.latencyMs"
                    Kind       = Some "outlet.kind"
                    BatchCount = Some "outlet.batchCount"
                    Error      = Some "outlet.error"
                    Complete   = Some "outlet.complete" 
                }

        type ForkStreamConfig =
            { 
                BatchMax         : int option
                EmitEmptyBatches : bool
                MaxInFlight      : int
                AttrKeys         : OutletAttrKeys
            }
            static member Default =
                { 
                    BatchMax = Some 32
                    EmitEmptyBatches = false
                    MaxInFlight = 32
                    AttrKeys = OutletAttrKeys.Default
                }

        // Internal state container (encapsulated mutable, not exposed)
        // TODO: rework to use FSharp.Data.Adaptive
        type private StreamForkState<'In,'Out>() =
            let mutable inflight : InFlight<'In,'Out> list = []
            member __.InFlight = inflight
            member __.Add (f: InFlight<'In,'Out>) =
                inflight <- f :: inflight
            member __.Replace (fs: InFlight<'In,'Out> list) =
                inflight <- fs
            member __.Count = inflight.Length
        
        let inline private withPayload 
                (payload: 'Out) 
                (orig: Envelope<'In>) : Envelope<'Out> =
            { 
                Payload = payload
                Headers = orig.Headers
                SeqId   = orig.SeqId
                SpanCtx = orig.SpanCtx
                Ts      = orig.Ts
                Attrs   = orig.Attrs
                Cost    = orig.Cost 
            }

        let inline private addAttrs
                (baseMap: HashMap<string, Attribute>) 
                (adds: (string * Attribute) list) =
            if adds.IsEmpty then baseMap
            else
                (baseMap, adds) ||> List.fold (fun acc (k,v) -> HashMap.add k v acc)

        let private tryStartOutlet
                (cfg: ForkStreamConfig)
                (env: ExecutionEnv)
                (startOutlet: 'In -> Flow<EffectOutlet<'Out>>)
                (incoming: Envelope<'In>)
                (state: StreamForkState<'In,'Out>) : exn option =
            if state.Count >= cfg.MaxInFlight then None else
            try
                let outletFlow = startOutlet incoming.Payload
                // Minimal synchronous interpreter (for the Direct path)
                let rec run (env: ExecutionEnv) (prog: FlowProg<_>) =
                    match prog with
                    | FPure th -> th()
                    | FSync f -> f env
                    | FExternal spec ->
                        // Fallback: if a blocking external sneaks in, we will await (not ideal, but graceful).
                        let h = spec.Build env
                        let eh = h.Start()
                        match eh.Await() with
                        | EffectOutput (ValueSome v) -> v
                        | EffectEnded -> raise (InvalidOperationException "External ended unexpectedly")
                        | EffectFailed ex -> raise ex
                        | EffectCancelled oce -> raise oce
                        | EffectOutput ValueNone -> raise (InvalidOperationException "External returned no value")
                        | EffectPending -> raise (InvalidOperationException "Unexpected pending external")
                    | FDelay th -> run env (th())
                    | FTryWith (b,h) ->
                        try run env b with ex -> run env (h ex)
                    | FTryFinally (b,fin) ->
                        try
                            let r = run env b
                            fin(); r
                        with ex -> fin(); reraise()

                let outlet = run env outletFlow.Program
                state.Add { Original = incoming; Outlet = outlet; StartedAt = env.NowUnix() }
                None
            with ex ->
                Some ex

        (* 
            Drain a single inflight outlet producing:
            - updated survivor decision
            - list of newly drained envelopes (possibly empty)
            - flag whether outlet consumed/removed 
        *)
        let private drainOne
                (cfg: ForkStreamConfig)
                (env: ExecutionEnv)
                (inflight: InFlight<'In,'Out>) : bool * Envelope<'Out> list =
            match drain cfg.BatchMax inflight.Outlet with
            | Pending ->
                // Still pending, keep it
                true, []
            | Drained (items, isComplete, maybeErr) ->
                if items.IsEmpty && not cfg.EmitEmptyBatches then
                    // No emission, but may still remove if completed
                    if not isComplete then true, [] else
                    // Completed with no items (emitEmptyBatches = false) -> remove
                    false, []
                else
                    // Build envelopes
                    // Pre-calculate attributes common to all items
                    let keys = cfg.AttrKeys
                    let latencyOpt =
                        match keys.Latency with
                        | Some key when inflight.Outlet.IsCompleted ->
                            let latency = env.NowUnix() - inflight.StartedAt
                            Some (key, AttrInt64 latency)
                        | _ -> None
                    let kindOpt =
                        keys.Kind |> Option.map (fun k -> k, AttrString (inflight.Outlet.Kind.ToString()))
                    let batchOpt =
                        keys.BatchCount |> Option.map (fun k -> k, AttrInt32 items.Length)
                    let errOpt =
                        match maybeErr, keys.Error with
                        | Some e, Some key -> Some (key, AttrString e.Message)
                        | _ -> None
                    let completeOpt =
                        if isComplete then keys.Complete |> Option.map (fun k -> k, AttrBool true)
                        else None

                    let baseAdds =
                        [ latencyOpt; kindOpt; batchOpt; errOpt; completeOpt ]
                        |> List.choose id

                    let drainedEnvelopes =
                        items
                        |> List.map (fun payload ->
                            let e0 = withPayload payload inflight.Original
                            let newAttrs = addAttrs e0.Attrs baseAdds
                            { e0 with Attrs = newAttrs })

                    // Keep if not complete
                    not isComplete , drainedEnvelopes

        (* Drain all outlets respecting batch max.  Returns (survivors, drained envelopes). *)
        let private drainAll
                (cfg: ForkStreamConfig)
                (env: ExecutionEnv)
                (state: StreamForkState<'In,'Out>) : InFlight<'In,'Out> list * Envelope<'Out> list =
            let mutable survivors : InFlight<'In,'Out> list = []
            let mutable drained : Envelope<'Out> list = []
            let batchLimit = cfg.BatchMax |> Option.defaultValue Int32.MaxValue

            let mutable continueLoop = true
            for inflight in state.InFlight do
                if continueLoop then
                    let keep, produced = drainOne cfg env inflight
                    drained <- drained @ produced
                    if keep then survivors <- inflight :: survivors
                    if drained.Length >= batchLimit then
                        // Respect global batch limit across outlets
                        continueLoop <- false
                else
                    survivors <- inflight :: survivors
            // survivors currently in reverse order of processing if trimmed early; 
            // reversing not necessary functionally
            survivors, drained

        
        (* 
            Generalized outlet-based fork:
            startOutlet : 'In -> Flow<IEffectOutlet<'Out>>
            Strategy:
            - For each input envelope (if capacity allows) start a new outlet and store.
            - On every invocation also poll existing outlets; emit at most one batch of drained items.
            - When an outlet completes (and its queue drained) remove it. 
        *)
        let forkOutlet
                (cfg: ForkStreamConfig)
                (startOutlet : 'In -> Flow<EffectOutlet<'Out>>) : StreamProcessor<'In,'Out> =
            let state = StreamForkState<'In,'Out>()
            StreamProcessor (fun (envIn: Envelope<'In>) ->
                { Program =
                    FSync (fun execEnv ->

                        // 1. Attempt to start a new outlet (capture any exception)
                        let startError = tryStartOutlet cfg execEnv startOutlet envIn state

                        // 2. If starting failed, surface Error immediately (no draining this tick)
                        match startError with
                        | Some ex -> Error ex
                        | None ->
                            // 3. Drain what's available
                            let survivors, drained = drainAll cfg execEnv state
                            state.Replace survivors

                            // 4. Emit command
                            match drained with
                            | []      -> Consume
                            | [one]   -> Emit one
                            | many    -> EmitMany many
                    )
                })

        (* 
            Offload one effect per input envelope, emit result later.

            Behaviour:
                - On first receipt with Idle, start effect handle -> return Consume.
                - Subsequent calls (poll/tick) while Running:
                    * Poll() pending -> Consume
                    * Poll() done    -> Emit mapped result
                    * Failure        -> Error
                - After emission, state resets to Idle (allowing reuse for next envelope).
            
            NOTE: For now, no queuing of multiple inputs; if a new envelope arrives while
                Running, it is ignored with Consume (could be extended). 
        *)
        let forkSingle
            (start   : 'In -> Flow<EffectHandle<'Eff>>)
            (project : Envelope<'In> -> 'Eff -> 'Out) : StreamProcessor<'In,'Out> =
            // TODO: refactor `state` to use FSharp.Data.Adaptive
            let mutable state : SingleForkState<'In,'Eff,'Out> = Idle
            StreamProcessor (fun (envIn: Envelope<'In>) ->
                { Program =
                    FSync (fun execEnv ->
                        match state with
                        | Idle ->
                            // Start new effect for this envelope
                            let flowHandle = start envIn.Payload
                            // Direct.run variant (simplified) – in Direct path binds are sync
                            let rec run env (* ct *) prog =
                                match prog with
                                | FPure th -> th()
                                | FSync f -> f env
                                | FExternal spec ->
                                    (* 
                                        NOTE: Build/start returning handle (non-blocking) 
                                        is not expected here, since we rely on start returning a
                                        handle via the FSync route.

                                        We arrive at blocking semantics for FExternal and for correctness
                                        attempt to await. If we've arrived here, this is a fallback.
                                    *)
                                    let h = spec.Build env
                                    let ah = h.Start()
                                    match ah.Await() with
                                    | EffectOutput (ValueSome v) -> v
                                    | EffectFailed ex -> raise ex
                                    | EffectCancelled oce -> raise oce
                                    | _ -> failwith "Unexpected pending external in forkSingle fallback path."
                                | FDelay th -> run env (th())
                                | FTryWith (body, handler) ->
                                    try run env body
                                    with ex -> run env (handler ex)
                                | FTryFinally (body, fin) ->
                                    try
                                        let r = run env body
                                        fin()
                                        r
                                    with _ ->
                                        fin()
                                        reraise()
                            let handle = run execEnv flowHandle.Program
                            state <- Running (envIn, handle, execEnv.NowUnix())
                            Consume
                        | Running (orig, handle, startedAt) ->
                            let res = handle.Poll().Force()
                            match res with
                            | EffectPending ->
                                Consume
                            | EffectOutput (ValueSome value) ->
                                let latencyMs = execEnv.NowUnix() - startedAt
                                let outPayload = project orig value
                                let baseEnv = mapPayload outPayload orig
                                let outEnv =
                                    { baseEnv with
                                        Attrs =
                                            baseEnv.Attrs
                                            |> HashMap.add "fork.latencyMs" (AttrInt64 latencyMs)
                                            |> HashMap.add "fork.effectId" (AttrObj (handle.Token :> obj)) 
                                    }
                                state <- Idle
                                Emit outEnv
                            | EffectEnded ->
                                // TODO: FIXME: this fork will move out of direct soon anyway!!!
                                state <- Idle
                                Complete
                            | EffectFailed ex ->
                                state <- Faulted ex
                                Error ex
                            | EffectCancelled oce ->
                                state <- Faulted oce
                                Error oce
                            | EffectOutput ValueNone ->
                                // TODO: Consider treating as protocol error
                                state <- Faulted (InvalidOperationException "Effect completed without value.")
                                Error (InvalidOperationException "Empty effect result")
                        | Emitting _ ->
                            // FIXME: Transitional state not currently used – treat as Complete
                            state <- Idle
                            Complete
                        | Faulted ex ->
                            // After surfacing an error once, keep returning Error to signal terminal state
                            Error ex
                    )
                })

        /// Projects an external effect that yields a single value.
        let forkSingleValue<'In, 'Eff, 'Out>
            (start : 'In -> Flow<EffectHandle<'Eff>>)
            (mapResult : 'Eff -> 'Out)
            : StreamProcessor<'In,'Out> =
            forkSingle start (fun _ v -> mapResult v)

        /// Adapts a single EffectHandle into an outlet.
        let forkSingleOutlet
                (start : 'In -> Flow<EffectHandle<'Out>>) : StreamProcessor<'In,'Out> =
            forkOutlet
                { ForkStreamConfig.Default with BatchMax = Some 1 }
                (fun input ->
                    let handleFlow = start input
                    { Program =
                        FSync (fun env ->
                            let rec run env prog =
                                match prog with
                                | FPure th -> th()
                                | FSync f -> f env
                                | FExternal spec ->
                                    let h = spec.Build env
                                    let eh = h.Start()
                                    match eh.Await() with
                                    | EffectOutput (ValueSome v) -> v
                                    | EffectFailed ex -> raise ex
                                    | EffectCancelled oce -> raise oce
                                    | EffectOutput ValueNone -> raise (InvalidOperationException "No value")
                                    | EffectPending -> raise (InvalidOperationException "Pending external")
                                    | EffectEnded -> raise (InvalidOperationException "External ended unexpectedly")
                                | FDelay th -> run env (th())
                                | FTryWith (b,h) -> try run env b with ex -> run env (h ex)
                                | FTryFinally (b,fin) ->
                                    try 
                                        let r = run env b
                                        fin()
                                        r
                                    with ex -> fin(); reraise()
                            let h = run env handleFlow.Program
                            Outlets.ofEffectHandle h
                        )
                    })

        /// Start a single Async<'Out'> per input and emit once when it completes.
        /// Subsequent calls while running only poll; no new async is started.
        let forkAsyncSingle
                (work : 'In -> Async<'Out>) : StreamProcessor<'In,'Out> =
            forkSingle
                (fun input -> Flux.IO.Core.Lift.asyncHandle (work input))
                (fun _ v -> v)

        /// Starts an Async<'Out> thunk, obtains an EffectHandle<'Out>, then adapts it to an outlet.
        /// Handles the (unlikely) case where a Flow<EffectHandle<'Out>> contains an external node
        /// whose Start() itself returns an EffectHandle<EffectHandle<'Out>> (nested) by flattening.
        let forkAsync
                (cfg: ForkStreamConfig)
                (work : 'In -> Async<'Out>) : StreamProcessor<'In,'Out> =
            // Build: 'startOutlet : 'In -> Flow<IEffectOutlet<'Out>>
            forkOutlet cfg (fun input ->
                // Flow<EffectHandle<'Out>>
                let handleFlow = Flux.IO.Core.Lift.asyncHandle (work input)
                { Program =
                    FSync (fun env ->
                        // Local minimal interpreter returning EffectHandle<'Out>
                        let rec run 
                                (env: ExecutionEnv) 
                                (prog: FlowProg<EffectHandle<'Out>>) : EffectHandle<'Out> =
                            match prog with
                            | FPure th -> th()
                            | FSync f  -> f env
                            | FDelay th -> run env (th())
                            | FTryWith (body, handler) ->
                                try run env body
                                with ex -> run env (handler ex)
                            | FTryFinally (body, fin) ->
                                try
                                    let r = run env body
                                    fin(); r
                                with ex ->
                                    fin(); reraise()
                            | FExternal spec ->
                                // spec : ExternalSpec<EffectHandle<'Out>>
                                // Starting it yields EffectHandle<EffectHandle<'Out>> (outer handle producing inner handle)
                                let outerHandle =
                                    let h = spec.Build env
                                    h.Start()

                                // Flatten: block (synchronously) until outer completes and extract inner
                                let rec awaitOuter() =
                                    match outerHandle.Poll().Force() with
                                    | EffectPending ->
                                        // Very short spin; for direct path we accept blocking
                                        Thread.SpinWait 40
                                        awaitOuter()
                                    | EffectOutput (ValueSome inner) -> inner
                                    | EffectOutput ValueNone ->
                                        raise (InvalidOperationException "External handle produced no inner handle")
                                    | EffectEnded ->
                                        raise (InvalidOperationException "External handle ended unexpectedly")
                                    | EffectFailed ex -> raise ex
                                    | EffectCancelled oce -> raise oce
                                awaitOuter()
                        let effHandle = run env handleFlow.Program
                        // Adapt to outlet
                        Outlets.ofEffectHandle effHandle
                    )
                })

    module MBox =
            
        type private AsyncMessage<'T> =
            | Start of AsyncReplyChannel<unit>
            | Poll of AsyncReplyChannel<EffectResult<'T>>
            | Wait of AsyncReplyChannel<EffectResult<'T>>
            | WaitTimeout of TimeSpan * AsyncReplyChannel<WaitableResult<'T>>
            | Cancel
        
        let lift (comp: Async<'T>) : Flow<'T> =
            let spec =
                { 
                    Build = fun _ ->
                        let started = new ManualResetEventSlim(false)
                        let completed = new ManualResetEventSlim(false)
                        let mutable result = EffectPending
                        let cts = new CancellationTokenSource()
                        
                        let agent = MailboxProcessor<AsyncMessage<'T>>.Start(fun inbox ->
                            let rec notStarted() = async {
                                let! msg = inbox.Receive()
                                match msg with
                                | Start reply ->
                                    Async.StartWithContinuations(
                                        comp,
                                        (fun value -> 
                                            result <- EffectOutput (ValueSome value)
                                            completed.Set()
                                        ),
                                        (fun ex -> 
                                            result <- EffectFailed ex
                                            completed.Set()
                                        ),
                                        (fun _ -> 
                                            result <- EffectCancelled (OperationCanceledException())
                                            completed.Set()
                                        ),
                                        cts.Token
                                    )
                                    started.Set()
                                    reply.Reply()
                                    return! running()
                                | Poll reply ->
                                    reply.Reply EffectPending
                                    return! notStarted()
                                | Wait reply ->
                                    reply.Reply EffectPending
                                    return! notStarted()
                                | WaitTimeout (_, reply) ->
                                    reply.Reply (Result EffectPending)
                                    return! notStarted()
                                | Cancel ->
                                    return! notStarted()
                            }
                            
                            and running() = async {
                                let! msg = inbox.Receive()
                                match msg with
                                | Start reply ->
                                    reply.Reply() // Already started
                                    return! running()
                                | Poll reply ->
                                    reply.Reply (if completed.IsSet then result else EffectPending)
                                    return! running()
                                | Wait reply ->
                                    // Spawn async to wait
                                    async {
                                        completed.Wait()
                                        reply.Reply result
                                    } |> Async.Start
                                    return! running()
                                | WaitTimeout (ts, reply) ->
                                    // Spawn async to wait with timeout
                                    async {
                                        if completed.Wait(ts) then
                                            reply.Reply (Result result)
                                        else
                                            reply.Reply (Result EffectPending)
                                    } |> Async.Start
                                    return! running()
                                | Cancel ->
                                    cts.Cancel()
                                    return! running()
                            }
                            
                            notStarted()
                        )

                        let createHandle() : EffectHandle<'T> =
                            { new EffectHandle<'T>(BackendToken agent) with
                                member _.IsCompleted = completed.IsSet
                                member _.Poll() = agent.PostAndReply Poll |> Result
                                member _.Await() = agent.PostAndReply Wait
                                member _.AwaitTimeout ts = 
                                    agent.PostAndReply(fun ch -> WaitTimeout(ts, ch))
                                member _.Cancel() = agent.Post Cancel
                                member this.CancelWait() = 
                                    agent.Post Cancel
                                    this.Await()
                                member this.CancelWaitTimeout ts = 
                                    agent.Post Cancel
                                    this.AwaitTimeout ts
                            }
                        
                        { new ExternalHandle<'T> with
                            member _.Id = 123 //FIXME
                            member _.Class = EffectExternal "FSharp.Control.Async"
                            member _.IsStarted = started.IsSet
                            member _.IsCompleted = completed.IsSet
                            member _.Start() = 
                                agent.PostAndReply Start
                                createHandle()
                            member _.Dispose() = 
                                started.Dispose()
                                completed.Dispose()
                                cts.Dispose()
                                (agent :> IDisposable).Dispose()
                        }
                    Classify = EffectExternal "Async"
                    DebugLabel = Some "lift-async" 
                }
            externalSpec spec
    
    module Async =
        
        (* Represents a running async operation that can be checked multiple times *)
        type StartedAsync<'T> = 
            {
                Poll: unit -> EffectResult<'T>               // Non-blocking check
                Await: unit -> EffectResult<'T>              // Blocking wait
                AwaitTimeout: TimeSpan -> EffectResult<'T>   // Blocking wait with timeout
                Cancel: unit -> unit
                IsCompleted: unit -> bool
            }

        /// Start an async computation that can be checked multiple times.
        let startAsync (asyncWork: Async<'T>) : Async<StartedAsync<'T>> =
            async {
                let completed = new ManualResetEventSlim(false)
                let mutable result = EffectPending
                let cts = new CancellationTokenSource()
                Async.StartWithContinuations(
                    asyncWork,
                    (fun value -> 
                        result <- EffectOutput (ValueSome value)
                        completed.Set()
                    ),
                    (fun ex -> 
                        result <- EffectFailed ex
                        completed.Set()
                    ),
                    (fun _ -> 
                        result <- EffectCancelled (OperationCanceledException())
                        completed.Set()
                    ),
                    cts.Token
                )
                
                return {
                    Poll = fun () -> 
                        if completed.IsSet then result 
                        else EffectPending
                    
                    Await = fun () ->
                        completed.Wait()
                        result
                    
                    AwaitTimeout = fun timeout ->
                        if completed.Wait(timeout) then result
                        else EffectPending
                    
                    Cancel = fun () -> cts.Cancel()
                    
                    IsCompleted = fun () -> completed.IsSet
                }
            }

        /// Run async work with a timeout, returning either the result or the handle to check later.
        let withTimeout 
                (timeout: TimeSpan) 
                (asyncWork: Async<'T>) : Flow<Choice<'T, StartedAsync<'T>>> =
            flow {
                // Start the async work
                let! started = Lift.asyncF (startAsync asyncWork)
                
                // Try to get result within timeout
                match started.AwaitTimeout timeout with
                | EffectOutput (ValueSome value) -> 
                    return Choice1Of2 value
                | EffectFailed ex -> 
                    return raise ex
                | EffectCancelled ex -> 
                    return raise ex
                | _ -> 
                    // Timeout - return the handle so caller can check later
                    return Choice2Of2 started
            }

        /// Retries a previously timed-out operation
        let retryTimeout 
                (timeout: TimeSpan) 
                (started: StartedAsync<'T>) : Async<Choice<'T, StartedAsync<'T>>> =
            async {
                match started.AwaitTimeout timeout with
                | EffectOutput (ValueSome value) -> 
                    return Choice1Of2 value
                | EffectFailed ex -> 
                    return raise ex
                | EffectCancelled ex -> 
                    return raise ex
                | _ -> 
                    // Still not ready
                    return Choice2Of2 started
            }

        //TODO: rewrite this with a proper backoff ~ see link
        // https://github.com/haskell-distributed/distributed-process/blob/2edcdf0a22a968be22b7d81554ae1446c1f45f0e/packages/distributed-process-supervisor/src/Control/Distributed/Process/Supervisor.hs#L1209
        
        /// Progressive timeout with retries
        let withProgressiveTimeout 
                (timeouts: TimeSpan list) 
                (asyncWork: Async<'T>) : Flow<'T option> =
            flow {
                match timeouts with
                | [] -> return None
                | firstTimeout :: remainingTimeouts ->
                    let! result = withTimeout firstTimeout asyncWork
                    
                    match result with
                    | Choice1Of2 value -> 
                        return Some value
                    | Choice2Of2 started ->
                        // Try remaining timeouts
                        let rec tryRemaining timeouts =
                            async {
                                match timeouts with
                                | [] -> return None
                                | timeout :: rest ->
                                    let! retryResult = retryTimeout timeout started
                                    match retryResult with
                                    | Choice1Of2 value -> return Some value
                                    | Choice2Of2 _ -> return! tryRemaining rest
                            }
                        
                        return! Lift.asyncF (tryRemaining remainingTimeouts)
            }

        /// Start multiple async operations and check them with timeout
        let parWithTimeout 
                (timeout: TimeSpan) 
                (asyncWorks: Async<'T> list) : Flow<('T option * StartedAsync<'T>) list> =
            flow {
                // Start all operations
                let! startedOps = 
                    asyncWorks
                    |> List.map startAsync
                    |> Async.Parallel
                    |> Lift.asyncF
                
                // Check each with timeout
                let! results =
                    startedOps
                    |> Array.map (fun started ->
                        async {
                            match started.AwaitTimeout timeout with
                            | EffectOutput (ValueSome value) -> return (Some value, started)
                            | _ -> return (None, started)
                        }
                    )
                    |> Async.Parallel
                    |> Lift.asyncF
                
                return Array.toList results
            }

        /// Create a Flow that runs async work without blocking, with explicit continuation
        let fork (asyncWork: Async<'T>) (continuation: 'T -> Flow<'R>) : Flow<'R> =
            flow {
                // Start the async work
                let! result = Lift.asyncF asyncWork
                // Continue with the result
                return! continuation result
            }
        
        /// Create a Flow that runs multiple async operations in parallel
        let par (asyncWorks: Async<'T> list) : Flow<'T list> =
            // TODO: provides this in terms of IAsyncEnumerable via both TaskSeq and AsyncSeq
            flow {
                // Start all async operations
                let! tasks = 
                    asyncWorks 
                    |> List.map Lift.asyncF 
                    |> List.map (fun f -> flow { return! f })
                    |> List.fold (fun acc elem ->
                        flow {
                            let! results = acc
                            let! result = elem
                            return result :: results
                        }
                    ) (flow { return [] })
                return List.rev tasks
            }

    (* Provide additional API surface layer for forking, parallel, etc *)

    type FlowBuilder with
        
        /// Fork async work with continuation (non-blocking)
        member __.Fork(asyncWork: Async<'T>, continuation: 'T -> Flow<'R>) : Flow<'R> =
            Async.fork asyncWork continuation
        
        /// Run async operations in parallel
        member __.Parallel(asyncWorks: Async<'T> list) : Flow<'T list> =
            Async.par asyncWorks
        
        // Yield control back to scheduler (useful in loops)
        member __.YieldControl() : Flow<unit> =
            Lift.asyncF (Async.SwitchToThreadPool())

    module StreamProcessor =

        let ask : Flow<ExecutionEnv> = 
            flow {
                let! env = sync id
                return env
            }

        // Access execution environment
        let withEnv (f: ExecutionEnv -> 'a -> Flow<'b>) : StreamProcessor<'a, 'b> =
            StreamProcessor (fun env ->
                flow {
                    let! execEnv = ask
                    let! result = f execEnv env.Payload
                    return Emit (Envelope.map (fun _ -> result) env)
                }
            )

        /// Monadic bind for stream processors.
        let bind 
                (f: 'b -> StreamProcessor<'b, 'c>) 
                (StreamProcessor p: StreamProcessor<'a, 'b>) : StreamProcessor<'a, 'c> =
            StreamProcessor (fun env ->
                flow {
                    let! execEnv = ask
                    let! cmd = p env
                    match cmd with
                    | Emit outEnv ->
                        // outEnv has type Envelope<'b>, which is what we need
                        let (StreamProcessor g) = f outEnv.Payload
                        return! g outEnv
                    | EmitMany envs ->
                        let results : Envelope<'c> list = []
                        let ems = 
                            envs 
                            |> Seq.fold (fun acc outEnv ->
                                // Each outEnv has type Envelope<'b>
                                let (StreamProcessor g) = f outEnv.Payload

                                // TODO: FIXME - we have to force the processor in order to bind here,
                                // but does this perhaps break the laziness approach when we're in Freer mode?
                                let res = g outEnv |> run execEnv (* CancellationToken.None *)
                                match res.Result with
                                | Left (Emit e) -> e :: acc
                                | Left (EmitMany es) -> es @ acc
                                | _ -> acc
                            ) results
                        return EmitMany (ems |> List.ofSeq)
                    | Consume -> return Consume
                    | Complete -> return Complete
                    | Error e -> return Error e
                }
            )