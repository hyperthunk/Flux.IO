namespace Flux.IO.Pipeline

module Direct = 
    open Flux.IO.Core.Types
    open System
    open System.Threading
    open System.Threading.Tasks

    // Ambient state to make the current CancellationToken available to FSync bind sequencing
    // without changing DU shapes or introducing boxing/sentinels.
    module Ambient =
        let __internal_CT = new AsyncLocal<CancellationToken>()

    let inline ofProg p : Flow<'T> = { Program = p }

    let inline pure' (x: 'T) : Flow<'T> =
        { Program = FPure (fun () -> x) }

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

        Evaluating `runExternal` has runProg evaluate a FlowProg<'T> into a ValueTask<'T>. 
        External effects are executed by obtaining an ExternalHandle<'T> and polling/waiting 
        (currently this is a naive blocking loop).
        
        Integration with scheduler or event loop is TODO/FIXME.
    *)
    
    
    let runExternal 
            (env: ExecutionEnv) 
            (ct: CancellationToken) 
            (spec: ExternalSpec<'T>) : ValueTask<'T> =
        // TODO: Non-blocking integration with scheduler / registration
        // TODO: Timeout / cancellation bridging
        // TODO: Backpressure gating before start
        task {
            let handle = spec.Build env
            
            if handle.IsStarted then
                raise (InvalidOperationException "External effect is already started")

            let asyncHandle = handle.Start()
            
            // Register cancellation
            use _ = ct.Register(fun () -> 
                try asyncHandle.Cancel()
                with _ -> ()
            )
            
            // Simply wait for the result
            let result =
                try asyncHandle.Await()
                with ex ->
                    // Await should not throw; normalize to AsyncFailed
                    AsyncFailed (ValueSome ex)
            
            match result with
            | AsyncDone (ValueSome v) -> 
                return v
            | AsyncDone ValueNone -> 
                return raise (InvalidOperationException "External completed with no value")
            | AsyncFailed (ValueSome ex) -> 
                return raise ex
            | AsyncFailed ValueNone -> 
                return raise (Exception "Unknown external failure")
            | AsyncCancelled (ValueSome oce) ->
                // Prefer preserving the provided OCE, but ensure token is visible
                if ct.IsCancellationRequested then
                    return raise (OperationCanceledException("Operation cancelled", oce, ct))
                else
                    return raise oce
            | AsyncCancelled ValueNone ->
                return raise (OperationCanceledException(ct))
            | AsyncPending ->
                // Should not happen after Await()
                return raise (InvalidOperationException "Await returned Pending")
        } |> ValueTask<'T>

    let rec execute 
            (env: ExecutionEnv) 
            (ct: CancellationToken) 
            (prog: FlowProg<'T>) : ValueTask<'T> =
        if ct.IsCancellationRequested then
            ValueTask<'T>(Task.FromCanceled<'T> ct)
        else
            match prog with
            | FPure th ->
                try ValueTask<'T>(th())
                with ex -> ValueTask<'T>(Task.FromException<'T> ex)
            | FSync f ->
                try ValueTask<'T>(f env)
                with ex -> ValueTask<'T>(Task.FromException<'T> ex)
            | FExternal spec ->
                runExternal env ct spec
            | FDelay thunk ->
                // Do not evaluate thunk early; just proceed when executed
                try execute env ct (thunk())
                with ex -> ValueTask<'T>(Task.FromException<'T> ex)
            | FTryWith (body, handler) ->
                let vt = execute env ct body
                if vt.IsCompletedSuccessfully then
                    ValueTask<'T> vt.Result
                else
                    task {
                        try
                            return! vt.AsTask()
                        with ex ->
                            // Build the handler program; if handler throws, escalate
                            let hProg =
                                try handler ex
                                with hex -> 
                                    FTryWith(FPure(fun () -> raise hex), 
                                            fun _ -> FPure(fun () -> raise hex)) // escalate
                            return! execute env ct hProg |> fun v -> v.AsTask()
                    } |> ValueTask<'T>
            | FTryFinally (body, fin) ->
                let vt = execute env ct body
                if vt.IsCompletedSuccessfully then
                    try
                        fin()
                        ValueTask<'T> vt.Result
                    with exFin ->
                        ValueTask<'T>(Task.FromException<'T> exFin)
                else
                    task {
                        try
                            let! r = vt
                            fin()
                            return r
                        with ex ->
                            try fin() with _ -> ()
                            return raise ex
                    } |> ValueTask<'T>

    (* Monadic Operations (where no structural bind nodes are stored) *)
    
    
    // Sequencing: run m under the current env/ct, then run k a, all inside a single FSync,
    // so there is no structural bind node and no boxing/sentinels. This is a blocking
    // composition by design (Direct path).
    //
    // NOTE:
    //  - Explicit generic ordering: k first, then m, helps solver keep 'A and 'B distinct.
    //  - Ambient.ct is set by Direct.run at the root; we rely on it here
    //    so FSync can observe the correct CancellationToken.
    //  - This preserves referential transparency at the API surface while executing eagerly.
    let inline bind<'A,'B> (k: 'A -> Flow<'B>) (m: Flow<'A>) : Flow<'B> =
        { Program =
            FSync (fun env ->
                let ct = Ambient.__internal_CT.Value
                let a =
                    let vt = execute env ct m.Program
                    if vt.IsCompletedSuccessfully then vt.Result else vt.AsTask().Result
                let b =
                    let vt2 = execute env ct (k a).Program
                    if vt2.IsCompletedSuccessfully then vt2.Result else vt2.AsTask().Result
                b) }
    
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

    // map implemented via bind -> return
    let inline map<'A,'B> (f: 'A -> 'B) (m: Flow<'A>) : Flow<'B> =
        bind (f >> pure') m

    let inline apply (mf: Flow<'a -> 'b>) (ma: Flow<'a>) : Flow<'b> =
        bind (fun f -> map f ma) mf

    (* 
        Execute a Flow<'T>. Because bind flattened structure by embedding logic in run,
        we handle composed cases in a unified run function. 
    *)
    let run (env: ExecutionEnv) (ct: CancellationToken) (m: Flow<'T>) : ValueTask<'T> =
        // PREVIOUS:
        // Since we used sentinel FPure/FSync scaffolding inside bind, we bypass them by
        // directly chaining at runtime:
        // Simplify: treat FSync sentinel with default value as marker— fallback to real run logic.
        // CURRENT: set ambient CT for nested FSync/Bind to observe
        let prev = Ambient.__internal_CT.Value
        Ambient.__internal_CT.Value <- ct
        try
            execute env ct m.Program
        finally
            Ambient.__internal_CT.Value <- prev

    type FlowBuilder () =
        member _.Return (x: 'T) : Flow<'T> = pure' x
        member _.ReturnFrom (m: Flow<'T>) = m

        member _.Bind (m: Flow<'A>, k: 'A -> Flow<'B>) : Flow<'B> =
            bind k m

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

        // Source pass-through
        member _.Source (m: Flow<'T>) = m

    let flow = FlowBuilder()

    module Stream =
        let emit x = Emit x
        let emitMany xs = EmitMany xs
        let consume () = Consume
        let complete () = Complete
        let fault ex = Error ex

    module Lift =

        let private tryWrap body =
            try
                body()
            with
            | :? AggregateException as ae ->
                let ex =
                    if isNull ae.InnerException then (ae :> exn)
                    else ae.InnerException
                AsyncFailed (ValueSome ex)
            | :? OperationCanceledException as oce ->
                AsyncCancelled (ValueSome oce)
            | ex ->
                AsyncFailed (ValueSome ex)

        /// Lift a Task<'T> by eagerly wrapping as ExternalSpec (simple adapter).
        let task(factory: unit -> Task<'T>) : Flow<'T> =
            let spec =
                { 
                    Build = fun _ ->
                        let mutable task = Unchecked.defaultof<Task<'T>>
                        let started = new ManualResetEventSlim(false)                        
                        
                        { new ExternalHandle<'T> with
                            member _.Id = 123 // factory.GetHashCode()
                            member _.Class = EffectExternal "System.Threading.Tasks.Task"
                            member _.IsStarted = started.IsSet
                            member _.IsCompleted = 
                                started.IsSet && not (isNull task) && task.IsCompleted

                            member _.Start() =
                                if not started.IsSet then
                                    task <- factory()
                                    started.Set()
                                
                                let token = BackendToken task
                                { new EffectHandle<'T>(token) with
                                    member _.IsCompleted = task.IsCompleted
                                    
                                    member _.Poll() =
                                        if task.IsCompletedSuccessfully then 
                                            AsyncDone (ValueSome task.Result)
                                        elif task.IsFaulted then 
                                            let ex =
                                                match task.Exception with
                                                | null -> null
                                                | ae when isNull ae.InnerException -> ae :> exn
                                                | ae -> ae.InnerException
                                            if isNull ex then AsyncFailed ValueNone
                                            else AsyncFailed (ValueSome ex)
                                        elif task.IsCanceled then 
                                            AsyncCancelled ValueNone
                                        else AsyncPending
                                    
                                    member this.Await() =
                                        tryWrap (fun() -> 
                                            task.Wait()
                                            this.Poll()
                                        )
                                    
                                    member this.AwaitTimeout ts = 
                                        tryWrap (fun() ->
                                            if task.Wait(ts) then 
                                                this.Poll()
                                            else 
                                                AsyncPending
                                        )
                                    
                                    member _.Cancel() = () //TODO: actually cancel it?
                                    member this.CancelWait() = this.Await()
                                    member this.CancelWaitTimeout ts = this.AwaitTimeout ts
                                }
                            
                            member _.Dispose() = 
                                started.Dispose()
                                if not (isNull task) then task.Dispose()
                        }
                    Classify = EffectExternal "Task"
                    DebugLabel = Some "lift-task" 
                }
            externalSpec spec

        /// Lift Async<'T>
        let async (comp: Async<'T>) : Flow<'T> =
            let spec = { 
                Build = fun _ ->
                    let cts = new CancellationTokenSource()
                    let started = new ManualResetEventSlim(false)
                    let mutable task = Unchecked.defaultof<Task<'T>>
                    
                    { new ExternalHandle<'T> with
                        member _.Id = 123 // FIXME
                        member _.Class = EffectExternal "FSharp.Control.Async"
                        member _.IsStarted = started.IsSet
                        member _.IsCompleted = 
                            started.IsSet && not (isNull task) && task.IsCompleted
                        
                        member _.Start() =
                            if not started.IsSet then
                                task <- Async.StartAsTask(comp, cancellationToken = cts.Token)
                                started.Set()
                            
                            // Return handle to the RUNNING task
                            { new EffectHandle<'T>(BackendToken task) with
                                member _.IsCompleted = task.IsCompleted
                                
                                member _.Poll() =
                                    if task.IsCompletedSuccessfully then
                                        AsyncDone (ValueSome task.Result)
                                    elif task.IsFaulted then
                                        let ex =
                                            match task.Exception with
                                            | null -> null
                                            | ae when Object.ReferenceEquals(ae.InnerException, null) -> ae :> exn
                                            | ae -> ae.InnerException
                                        if isNull ex then AsyncFailed ValueNone
                                        else AsyncFailed (ValueSome ex)
                                    elif task.IsCanceled then
                                        AsyncCancelled ValueNone
                                    else
                                        AsyncPending
                                
                                member this.Await() =
                                    tryWrap (fun() -> 
                                        task.Wait()
                                        this.Poll()
                                    )
                                
                                member this.AwaitTimeout ts =
                                    tryWrap (fun() -> 
                                        if task.Wait ts then this.Poll()
                                        else AsyncPending
                                    )
                                
                                member _.Cancel() = cts.Cancel()
                                
                                member this.CancelWait() = 
                                    cts.Cancel()
                                    this.Await()
                                
                                member this.CancelWaitTimeout ts = 
                                    cts.Cancel()
                                    this.AwaitTimeout ts
                            }
                        
                        member _.Dispose() = 
                            started.Dispose()
                            cts.Dispose()
                            if not (isNull task) then task.Dispose()
                    }
                Classify = EffectExternal "Async"
                DebugLabel = Some "lift-async" 
            }
            externalSpec spec

        // Start (build + start) an ExternalSpec returning the raw EffectHandle<'T> without awaiting completion.
        // This is synchronous and allocation-light: it executes only the builder and Start().
        let effectHandle (spec: ExternalSpec<'T>) : Flow<EffectHandle<'T>> =
            { Program =
                FSync (fun env ->
                    let h = spec.Build env
                    if h.IsStarted then invalidOp "External effect already started."
                    let handle = h.Start() // side effect: effect starts
                    // NOTE: ExternalHandle 'h' is not disposed automatically here.
                    // TODO: Consider wrapping handle to dispose 'h' after completion (Phase 2).
                    handle
                )
            }

        // Non-blocking start for a Task-producing factory; returns an EffectHandle.
        let taskHandle (factory: unit -> Task<'T>) : Flow<EffectHandle<'T>> =
            // Reuse existing Task adapter spec then return the handle
            let specFlow = task factory
            // 'task' currently blocks (awaits). We need a fresh spec that does NOT block.
            // Provide a minimal spec replicating the logic but returning handle directly.
            let spec =
                {
                    Build = fun _ ->
                        let mutable taskRef : Task<'T> = Unchecked.defaultof<_>
                        let started = new ManualResetEventSlim(false)
                        { new ExternalHandle<'T> with
                            member _.Id = 123 // TODO: FIXME
                            member _.Class = EffectExternal "System.Threading.Tasks.Task"
                            member _.IsStarted = started.IsSet
                            member _.IsCompleted = started.IsSet && not (isNull taskRef) && taskRef.IsCompleted
                            member _.Start() =
                                if not started.IsSet then
                                    taskRef <- factory()
                                    started.Set()
                                { new EffectHandle<'T>(BackendToken taskRef) with
                                    member _.IsCompleted = taskRef.IsCompleted
                                    member _.Poll() =
                                        if taskRef.IsCompletedSuccessfully then AsyncDone (ValueSome taskRef.Result)
                                        elif taskRef.IsFaulted then
                                            let ex =
                                                match taskRef.Exception with
                                                | null -> null
                                                | ae when isNull ae.InnerException -> ae :> exn
                                                | ae -> ae.InnerException
                                            if isNull ex then AsyncFailed ValueNone
                                            else AsyncFailed (ValueSome ex)
                                        elif taskRef.IsCanceled then AsyncCancelled ValueNone
                                        else AsyncPending
                                    member this.Await() =
                                        taskRef.Wait()
                                        this.Poll()
                                    member this.AwaitTimeout ts =
                                        if taskRef.Wait ts then this.Poll() else AsyncPending
                                    member _.Cancel() = ()
                                    member this.CancelWait() = this.Await()
                                    member this.CancelWaitTimeout ts = this.AwaitTimeout ts
                                }
                            member _.Dispose() =
                                started.Dispose()
                                if not (isNull taskRef) then taskRef.Dispose()
                        }
                    Classify = EffectExternal "Task"
                    DebugLabel = Some "task-handle"
                }
            effectHandle spec

        // Non-blocking start for Async<'T>; returns an EffectHandle<'T>.
        let asyncHandle (comp: Async<'T>) : Flow<EffectHandle<'T>> =
            let spec =
                {
                    Build = fun env ->
                        // Delegate to existing async adapter but intercept before blocking
                        let cts = new CancellationTokenSource()
                        let mutable taskRef : Task<'T> = Unchecked.defaultof<_>
                        let started = new ManualResetEventSlim(false)
                        { new ExternalHandle<'T> with
                            member _.Id = 123 // TODO: FIXME
                            member _.Class = EffectExternal "FSharp.Async"
                            member _.IsStarted = started.IsSet
                            member _.IsCompleted = started.IsSet && not (isNull taskRef) && taskRef.IsCompleted
                            member _.Start() =
                                if not started.IsSet then
                                    taskRef <- Async.StartAsTask(comp, cancellationToken = cts.Token)
                                    started.Set()
                                { new EffectHandle<'T>(BackendToken taskRef) with
                                    member _.IsCompleted = taskRef.IsCompleted
                                    member _.Poll() =
                                        if taskRef.IsCompletedSuccessfully then AsyncDone (ValueSome taskRef.Result)
                                        elif taskRef.IsFaulted then
                                            let ex =
                                                match taskRef.Exception with
                                                | null -> null
                                                | ae when isNull ae.InnerException -> ae :> exn
                                                | ae -> ae.InnerException
                                            if isNull ex then AsyncFailed ValueNone
                                            else AsyncFailed (ValueSome ex)
                                        elif taskRef.IsCanceled then AsyncCancelled ValueNone
                                        else AsyncPending
                                    member this.Await() =
                                        taskRef.Wait()
                                        this.Poll()
                                    member this.AwaitTimeout ts =
                                        if taskRef.Wait ts then this.Poll() else AsyncPending
                                    member _.Cancel() = cts.Cancel()
                                    member this.CancelWait() =
                                        cts.Cancel()
                                        this.Await()
                                    member this.CancelWaitTimeout ts =
                                        cts.Cancel()
                                        this.AwaitTimeout ts
                                }
                            member _.Dispose() =
                                started.Dispose()
                                cts.Dispose()
                                if not (isNull taskRef) then taskRef.Dispose()
                        }
                    Classify = EffectExternal "Async"
                    DebugLabel = Some "async-handle"
                }
            effectHandle spec

    // Intermediary module: offload + later emission (Phase 1)
    module Intermediary =

        open FSharp.HashCollections
        open Flux.IO.Core.Types

        /// Internal state for a single in-flight offloaded effect.
        type private SingleForkState<'In,'Eff,'Out> =
            | Idle
            | Running of original: Envelope<'In> * handle: EffectHandle<'Eff> * startedAt: int64
            | Emitting of Envelope<'Out>
            | Faulted of exn

        /// Offload one effect per input envelope, emit result later.
        /// Behaviour:
        ///  - On first receipt with Idle, start effect handle -> return Consume.
        ///  - Subsequent calls (poll/tick) while Running:
        ///       * Poll() pending -> Consume
        ///       * Poll() done    -> Emit mapped result
        ///       * Failure        -> Error
        ///  - After emission, state resets to Idle (allowing reuse for next envelope).
        /// NOTE: For now, no queuing of multiple inputs; if a new envelope arrives while
        ///       Running, it is ignored with Consume (could be extended).
        let forkSingle
            (start   : 'In -> Flow<EffectHandle<'Eff>>)
            (project : Envelope<'In> -> 'Eff -> 'Out)
            : StreamProcessor<'In,'Out> =
            let mutable state : SingleForkState<'In,'Eff,'Out> = Idle
            StreamProcessor (fun (envIn: Envelope<'In>) ->
                { Program =
                    FSync (fun execEnv ->
                        match state with
                        | Idle ->
                            // Start new effect for this envelope
                            let flowHandle = start envIn.Payload
                            let handleVT =
                                // Direct.run variant (simplified) – in Direct path binds are sync
                                let rec run env ct prog =
                                    match prog with
                                    | FPure th -> th()
                                    | FSync f -> f env
                                    | FExternal spec ->
                                        // Build/start returning handle (non-blocking) is not expected here
                                        // Because we rely on start returning a handle via FSync route.
                                        let h = spec.Build env
                                        let ah = h.Start()
                                        // BLOCKING semantics for FExternal in Direct path remain, but
                                        // for correctness we attempt to await. If this is reached it's a fallback.
                                        match ah.Await() with
                                        | AsyncDone (ValueSome v) -> v
                                        | AsyncFailed (ValueSome ex) -> raise ex
                                        | AsyncCancelled (ValueSome oce) -> raise oce
                                        | _ -> failwith "Unexpected pending external in forkSingle fallback path."
                                    | FDelay th -> run env ct (th())
                                    | FTryWith (body, handler) ->
                                        try run env ct body
                                        with ex -> run env ct (handler ex)
                                    | FTryFinally (body, fin) ->
                                        try
                                            let r = run env ct body
                                            fin()
                                            r
                                        with _ ->
                                            fin()
                                            reraise()
                                let handle = run execEnv CancellationToken.None flowHandle.Program
                                state <- Running (envIn, handle, execEnv.NowUnix())
                            Consume
                        | Running (orig, handle, startedAt) ->
                            let res = handle.Poll()
                            match res with
                            | AsyncPending ->
                                Consume
                            | AsyncDone (ValueSome value) ->
                                let latencyMs = execEnv.NowUnix() - startedAt
                                let outPayload = project orig value
                                let outEnv =
                                    { orig with
                                        Payload = outPayload
                                        Attrs =
                                            orig.Attrs
                                            |> HashMap.add "fork.latencyMs" (AttrInt64 latencyMs)
                                            |> HashMap.add "fork.effectId" (AttrObj handle.Token) }
                                state <- Idle
                                Emit outEnv
                            | AsyncFailed (ValueSome ex) ->
                                state <- Faulted ex
                                Error ex
                            | AsyncCancelled (ValueSome oce) ->
                                state <- Faulted oce
                                Error oce
                            | AsyncDone ValueNone ->
                                // Consider treating as protocol error
                                state <- Faulted (InvalidOperationException "Effect completed without value.")
                                Error (InvalidOperationException "Empty effect result")
                            | AsyncFailed ValueNone ->
                                let ex = InvalidOperationException "Unknown effect failure"
                                state <- Faulted ex
                                Error ex
                            | AsyncCancelled ValueNone ->
                                let ex = OperationCanceledException()
                                state <- Faulted ex
                                Error ex
                        | Emitting _ ->
                            // Transitional state not currently used – treat as Complete
                            state <- Idle
                            Complete
                        | Faulted ex ->
                            // After surfacing an error once, keep returning Error to signal terminal state
                            Error ex
                    )
                })

        /// Convenience version that maps result directly (project only value)
        let forkSingleValue
            (start : 'In -> Flow<EffectHandle<'Eff>>)
            (mapResult : 'Eff -> 'Out)
            : StreamProcessor<'In,'Out> =
            forkSingle start (fun _ v -> mapResult v)

    module MBox =
            
        type private AsyncMessage<'T> =
            | Start of AsyncReplyChannel<unit>
            | Poll of AsyncReplyChannel<EffectResult<'T>>
            | Wait of AsyncReplyChannel<EffectResult<'T>>
            | WaitTimeout of TimeSpan * AsyncReplyChannel<EffectResult<'T>>
            | Cancel
        
        let lift (comp: Async<'T>) : Flow<'T> =
            let spec =
                { 
                    Build = fun _ ->
                        let started = new ManualResetEventSlim(false)
                        let completed = new ManualResetEventSlim(false)
                        let mutable result = AsyncPending
                        let cts = new CancellationTokenSource()
                        
                        let agent = MailboxProcessor<AsyncMessage<'T>>.Start(fun inbox ->
                            let rec notStarted() = async {
                                let! msg = inbox.Receive()
                                match msg with
                                | Start reply ->
                                    // Start the computation
                                    Async.StartWithContinuations(
                                        comp,
                                        (fun value -> 
                                            result <- AsyncDone (ValueSome value)
                                            completed.Set()
                                        ),
                                        (fun ex -> 
                                            result <- AsyncFailed (ValueSome ex)
                                            completed.Set()
                                        ),
                                        (fun _ -> 
                                            result <- AsyncCancelled ValueNone
                                            completed.Set()
                                        ),
                                        cts.Token
                                    )
                                    started.Set()
                                    reply.Reply()
                                    return! running()
                                | Poll reply ->
                                    reply.Reply AsyncPending
                                    return! notStarted()
                                | Wait reply ->
                                    reply.Reply AsyncPending
                                    return! notStarted()
                                | WaitTimeout (_, reply) ->
                                    reply.Reply AsyncPending
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
                                    reply.Reply (if completed.IsSet then result else AsyncPending)
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
                                            reply.Reply result
                                        else
                                            reply.Reply AsyncPending
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
                                member _.Poll() = agent.PostAndReply Poll
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
        
        // Represents a already started async operation that can be checked multiple times
        type StartedAsync<'T> = {
            Poll: unit -> EffectResult<'T>          // Non-blocking check
            Await: unit -> EffectResult<'T>         // Blocking wait
            AwaitTimeout: TimeSpan -> EffectResult<'T>  // Blocking wait with timeout
            Cancel: unit -> unit
            IsCompleted: unit -> bool
        }

        /// Start an async computation that can be checked multiple times
        let startAsync (asyncWork: Async<'T>) : Async<StartedAsync<'T>> =
            async {
                let completed = new ManualResetEventSlim(false)
                let mutable result = AsyncPending
                let cts = new CancellationTokenSource()
                
                // Start the computation with continuations
                Async.StartWithContinuations(
                    asyncWork,
                    (fun value -> 
                        result <- AsyncDone (ValueSome value)
                        completed.Set()
                    ),
                    (fun ex -> 
                        result <- AsyncFailed (ValueSome ex)
                        completed.Set()
                    ),
                    (fun _ -> 
                        result <- AsyncCancelled ValueNone
                        completed.Set()
                    ),
                    cts.Token
                )
                
                return {
                    Poll = fun () -> 
                        if completed.IsSet then result 
                        else AsyncPending
                    
                    Await = fun () ->
                        completed.Wait()
                        result
                    
                    AwaitTimeout = fun timeout ->
                        if completed.Wait(timeout) then result
                        else AsyncPending
                    
                    Cancel = fun () -> cts.Cancel()
                    
                    IsCompleted = fun () -> completed.IsSet
                }
            }

        /// Run async work with timeout, returning either the result or the handle to check later
        let withTimeout 
                (timeout: TimeSpan) 
                (asyncWork: Async<'T>) : Flow<Choice<'T, StartedAsync<'T>>> =
            flow {
                // Start the async work
                let! started = Lift.async (startAsync asyncWork)
                
                // Try to get result within timeout
                match started.AwaitTimeout timeout with
                | AsyncDone (ValueSome value) -> 
                    return Choice1Of2 value
                | AsyncFailed (ValueSome ex) -> 
                    return raise ex
                | AsyncCancelled _ -> 
                    return raise (OperationCanceledException())
                | _ -> 
                    // Timeout - return the handle so caller can check later
                    return Choice2Of2 started
            }

        /// Helper to retry a timed-out operation
        let retryTimeout 
                (timeout: TimeSpan) 
                (started: StartedAsync<'T>) : Async<Choice<'T, StartedAsync<'T>>> =
            async {
                match started.AwaitTimeout timeout with
                | AsyncDone (ValueSome value) -> 
                    return Choice1Of2 value
                | AsyncFailed (ValueSome ex) -> 
                    return raise ex
                | AsyncCancelled _ -> 
                    return raise (OperationCanceledException())
                | _ -> 
                    // Still not ready
                    return Choice2Of2 started
            }

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
                        
                        return! Lift.async (tryRemaining remainingTimeouts)
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
                    |> Lift.async
                
                // Check each with timeout
                let! results =
                    startedOps
                    |> Array.map (fun started ->
                        async {
                            match started.AwaitTimeout timeout with
                            | AsyncDone (ValueSome value) -> return (Some value, started)
                            | _ -> return (None, started)
                        }
                    )
                    |> Async.Parallel
                    |> Lift.async
                
                return Array.toList results
            }

        // Example usage showing the power of returning StartedAsync
        let startAsyncTest() = flow {
            let longRunningWork = async {
                do! Async.Sleep 5000
                return "Finally done!"
            }
            
            // Try with 1 second timeout
            let! firstTry = withTimeout (TimeSpan.FromSeconds 1.) longRunningWork
            
            match firstTry with
            | Choice1Of2 result ->
                printfn "Got result quickly: %s" result
                return result
            | Choice2Of2 started ->
                printfn "Timed out after 1 second, doing other work..."
                
                // Do some other work while waiting
                do! Lift.async (Async.Sleep 2000)
                
                // Check again with 3 second timeout
                let! secondTry = Lift.async (retryTimeout (TimeSpan.FromSeconds 3.) started)
                
                match secondTry with
                | Choice1Of2 result ->
                    printfn "Got result on retry: %s" result
                    return result
                | Choice2Of2 _ ->
                    printfn "Still not ready, giving up"
                    return "Default value"
        }

        /// Create a Flow that runs async work without blocking, with explicit continuation
        let fork (asyncWork: Async<'T>) (continuation: 'T -> Flow<'R>) : Flow<'R> =
            flow {
                // Start the async work
                let! result = Lift.async asyncWork
                // Continue with the result
                return! continuation result
            }
        
        /// Create a Flow that runs multiple async operations in parallel
        let par (asyncWorks: Async<'T> list) : Flow<'T list> =
            flow {
                // Start all async operations
                let! tasks = 
                    asyncWorks 
                    |> List.map Lift.async 
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
            Lift.async (Async.SwitchToThreadPool())

    module StreamProcessor =

        let ask : Flow<ExecutionEnv> = sync id

        // Access execution environment
        let withEnv (f: ExecutionEnv -> 'a -> Flow<'b>) : StreamProcessor<'a, 'b> =
            StreamProcessor (fun env ->
                flow {
                    let! execEnv = ask
                    let! result = f execEnv env.Payload
                    return Emit (Envelope.map (fun _ -> result) env)
                }
            )

        // Monadic bind for stream processors
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
                                let res = g outEnv |> run execEnv CancellationToken.None
                                match res.Result with
                                | Emit e -> e :: acc
                                | EmitMany es -> es @ acc
                                | _ -> acc
                            ) results
                        return EmitMany (ems |> List.ofSeq)
                    | Consume -> return Consume
                    | Complete -> return Complete
                    | Error e -> return Error e
                }
            )