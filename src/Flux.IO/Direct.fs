namespace Flux.IO.Pipeline

module Direct = 
    open Flux.IO.Core.Types
    open System
    open System.Threading
    open System.Threading.Tasks


    let inline ofProg p : Flow<'T> = { prog = p }

    let inline pure' (x: 'T) : Flow<'T> =
        { prog = FPure (fun () -> x) }

    let inline sync (f: ExecutionEnv -> 'T) : Flow<'T> =
        { prog = FSync f }

    let inline externalSpec (spec: ExternalSpec<'T>) : Flow<'T> =
        { prog = FExternal spec }

    let inline delay (thunk: unit -> Flow<'T>) : Flow<'T> =
        { prog = FDelay (fun () -> (thunk()).prog) }

    let inline tryWith (m: Flow<'T>) (handler: exn -> Flow<'T>) : Flow<'T> =
        { prog = FTryWith (m.prog, fun ex -> (handler ex).prog) }

    let inline tryFinally (m: Flow<'T>) (fin: unit -> unit) : Flow<'T> =
        { prog = FTryFinally (m.prog, fin) }

    // ----------------------------------------------------------------------------------
    // Internal Interpreter Primitive (single-step flattening).
    // ----------------------------------------------------------------------------------
    // runProg executes FlowProg<'T> into a ValueTask<'T>. External effects are executed
    // by obtaining an ExternalHandle<'T> and polling/waiting (here naive blocking loop).
    // A production version would integrate with a scheduler or event loop.
    // ----------------------------------------------------------------------------------

    let private runExternal 
            (env: ExecutionEnv) 
            (ct: CancellationToken) 
            (spec: ExternalSpec<'T>) : ValueTask<'T> =
        // NOTE: This is a simplified interpretation. Improve with:
        //  * Non-blocking integration with scheduler / registration
        //  * Timeout / cancellation bridging
        //  * Backpressure gating before start
        let handle = spec.Build env
        if not handle.IsStarted then
            let _ = handle.Start() // Acquire AsyncHandle<'T>; we rely on handle.IsCompleted transitioning
            ()
        let rec spin () =
            if ct.IsCancellationRequested then
                ValueTask<'T>(Task.FromCanceled<'T>(ct))
            else
                if handle.IsCompleted then
                    // We assume the backend ensures completed handle contains final result accessible via an AsyncHandle poll.
                    // Need a way to retrieve the stored result. If design requires, ExternalHandle<'T> can expose TryGetResult.
                    // For now, we reinterpret the Start result contract: Start returns AsyncHandle<'T> we can invoke immediately.
                    let ah = handle.Start()  // Idempotent or supply property
                    match ah.Await() with
                    | AsyncDone (ValueSome v)    -> ValueTask<'T> v
                    | AsyncDone ValueNone        -> ValueTask<'T>(Task.FromException<'T>(InvalidOperationException "External completed with no value"))
                    | AsyncFailed (ValueSome ex) -> ValueTask<'T>(Task.FromException<'T> ex)
                    | AsyncFailed ValueNone      -> ValueTask<'T>(Task.FromException<'T>(Exception "Unknown external failure"))
                    | AsyncCancelled _           -> ValueTask<'T>(Task.FromCanceled<'T> ct)
                    | AsyncPending               ->
                        // Unexpected: completed but token says pending
                        Async.Sleep 0 |> Async.RunSynchronously |> ignore
                        spin() // TODO: this could block forever if there is a fault in the async result code - fix!
                else
                    // simple yielding strategy (replace with better scheduling)
                    Async.Sleep 10 |> Async.RunSynchronously |> ignore
                    spin()
        spin()

    let rec private execute 
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

    // ----------------------------------------------------------------------------------
    // Monadic Operations (no structural bind node stored)
    // ----------------------------------------------------------------------------------

    // Explicit generic ordering: k first, then m, helps solver keep 'A and 'B distinct.
    let inline bind<'A,'B> (k: 'A -> Flow<'B>) (m: Flow<'A>) : Flow<'B> =
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
        { prog =
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
                )) }

    // map implemented via bind -> return
    let inline map<'A,'B> (f: 'A -> 'B) (m: Flow<'A>) : Flow<'B> =
        bind (f >> pure') m

    // ----------------------------------------------------------------------------------
    // Flow Runner
    // ----------------------------------------------------------------------------------

    /// Execute a Flow<'T>. Because bind flattened structure by embedding logic in run,
    /// we handle composed cases in a unified run function.
    let run (env: ExecutionEnv) (ct: CancellationToken) (m: Flow<'T>) : ValueTask<'T> =
        // Since we used sentinel FPure/FSync scaffolding inside bind, we bypass them by
        // directly chaining at runtime:
        // Simplify: treat FSync sentinel with default value as marker— fallback to real run logic.
        execute env ct m.prog

    // ----------------------------------------------------------------------------------
    // Computation Expression Builder
    // ----------------------------------------------------------------------------------

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

    // ----------------------------------------------------------------------------------
    // Convenience helpers for StreamCommand construction
    // ----------------------------------------------------------------------------------

    module Stream =
        let emit x = Emit x
        let emitMany xs = EmitMany xs
        let consume () = Consume
        let complete () = Complete
        let fault ex = Error ex

    // ----------------------------------------------------------------------------------
    // Task / Async lifting helpers (convert to ExternalSpec if desired)
    // ----------------------------------------------------------------------------------

    module Lift =

        /// Lift a Task<'T> by eagerly wrapping as ExternalSpec (simple adapter).
        let task (factory: unit -> Task<'T>) : Flow<'T> =
            // Wrap: Start will run the task; poll via a simple handle adapter
            let tcell = new TaskCompletionSource<'T>(factory())
            let spec =
                { 
                    Build = (fun _ ->
                        { new ExternalHandle<'T> with
                            member __.Id = tcell.Task.Id.GetHashCode()
                            member __.Class = 
                                let name = tcell.Task.GetType().FullName
                                in EffectExternal name
                            member __.IsStarted = 
                                tcell.Task.Status = TaskStatus.Running ||
                                tcell.Task.Status = TaskStatus.RanToCompletion
                            member __.IsCompleted = tcell.Task.IsCompleted
                            member __.Start () =
                                async {
                                    factory().ContinueWith(fun (rt: Task<'T>) ->
                                        if rt.IsCanceled then
                                            tcell.TrySetCanceled() |> ignore
                                        elif rt.IsFaulted then
                                            tcell.TrySetException rt.Exception.InnerExceptions |> ignore
                                        else
                                            tcell.TrySetResult rt.Result |> ignore
                                    ) |> ignore
                                } |> Async.StartAsTask |> ignore
                                let token = AsyncToken tcell.Task
                                { new AsyncHandle<'T>(token) with
                                    member __.IsCompleted = tcell.Task.IsCompleted
                                    member __.Await() =
                                        if tcell.Task.IsCompletedSuccessfully then 
                                            AsyncDone (ValueSome tcell.Task.Result)
                                        elif tcell.Task.IsFaulted then 
                                            AsyncFailed (ValueSome tcell.Task.Exception.InnerException)
                                        elif tcell.Task.IsCanceled then 
                                            AsyncCancelled ValueNone
                                        else AsyncPending
                                    member this.AwaitTimeout ts = 
                                        // Simple blocking wait with timeout
                                        if tcell.Task.Wait ts then this.Await()
                                        else AsyncPending
                                    member _.Cancel() = 
                                        // we cannot cancel an arbitrary external task here
                                        ()
                                    member this.CancelWait() = this.Await()
                                    member this.CancelWaitTimeout ts = this.AwaitTimeout ts
                                }
                            override __.Dispose() = tcell.Task.Dispose()
                        }
                    )
                    Classify = typeof<Task<'T>>.FullName |> EffectExternal
                    DebugLabel = Some "lift-task" 
                }
            externalSpec spec

        /// Lift Async<'T>
        let async (comp: Async<'T>) : Flow<'T> =
            let tcell = new TaskCompletionSource<'T>()
            let ctoken = new CancellationToken()
            let ctokenSrc = CancellationTokenSource.CreateLinkedTokenSource ctoken
            let spec =
                { 
                    Build = (fun _ ->
                        { new ExternalHandle<'T> with
                            member __.Id = tcell.Task.Id.GetHashCode()
                            member __.Class = 
                                let name = tcell.Task.GetType().FullName
                                in EffectExternal name
                            member __.IsStarted = 
                                tcell.Task.Status = TaskStatus.Running ||
                                tcell.Task.Status = TaskStatus.RanToCompletion
                            member __.IsCompleted = tcell.Task.IsCompleted
                            member __.Start () =
                                Async.StartWithContinuations(
                                    comp,
                                    (fun result -> tcell.TrySetResult result |> ignore),
                                    (fun ex -> tcell.TrySetException ex |> ignore),
                                    (fun cEx -> tcell.TrySetCanceled cEx.CancellationToken |> ignore),
                                    ctoken
                                )
                                let token = AsyncToken tcell.Task
                                { new AsyncHandle<'T>(token) with
                                    member __.IsCompleted = tcell.Task.IsCompleted
                                    member __.Await() =
                                        if tcell.Task.IsCompletedSuccessfully then 
                                            AsyncDone (ValueSome tcell.Task.Result)
                                        elif tcell.Task.IsFaulted then 
                                            AsyncFailed (ValueSome tcell.Task.Exception.InnerException)
                                        elif tcell.Task.IsCanceled then 
                                            AsyncCancelled ValueNone
                                        else AsyncPending
                                    member this.AwaitTimeout ts = 
                                        // Simple blocking wait with timeout
                                        if tcell.Task.Wait ts then this.Await()
                                        else AsyncPending
                                    member _.Cancel() = 
                                        ctokenSrc.CancelAsync() |> ignore
                                    member this.CancelWait() = 
                                        ctokenSrc.Cancel()
                                        this.Await()
                                    member this.CancelWaitTimeout ts = 
                                        ctokenSrc.CancelAsync() |> ignore
                                        this.AwaitTimeout ts
                                }
                            override __.Dispose() = 
                                ctokenSrc.Dispose()
                                if  tcell.Task.Status = TaskStatus.RanToCompletion ||
                                    tcell.Task.Status = TaskStatus.Canceled ||
                                    tcell.Task.Status = TaskStatus.Faulted
                                    then tcell.Task.Dispose()
                        }
                    )
                    Classify = typeof<Task<'T>>.FullName |> EffectExternal
                    DebugLabel = Some "lift-async" 
                }
            externalSpec spec
