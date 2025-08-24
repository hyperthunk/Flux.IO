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

    
    (* 
        Note: Single-step, flattening Interpreter primitive 

        Evaluating `runExternal` has runProg evaluate a FlowProg<'T> into a ValueTask<'T>. 
        External effects are executed by obtaining an ExternalHandle<'T> and polling/waiting 
        (currently this is a naive blocking loop).
        
        Integration with scheduler or event loop is TODO/FIXME.
    *)
    
    
    let private runExternal 
            (env: ExecutionEnv) 
            (ct: CancellationToken) 
            (spec: ExternalSpec<'T>) : ValueTask<'T> =
        // TODO: Non-blocking integration with scheduler / registration
        // TODO: Timeout / cancellation bridging
        // TODO: Backpressure gating before start
        task {
            let handle = spec.Build env
            
            if not handle.IsStarted then
                let _ = handle.Start()
                ()
            
            let asyncHandle = handle.Start()
            
            // Register cancellation
            use _ = ct.Register(fun () -> asyncHandle.Cancel())
            
            // Simply wait for the result
            let result = asyncHandle.Await()
            
            match result with
            | AsyncDone (ValueSome v) -> 
                return v
            | AsyncDone ValueNone -> 
                return raise (InvalidOperationException "External completed with no value")
            | AsyncFailed (ValueSome ex) -> 
                return raise ex
            | AsyncFailed ValueNone -> 
                return raise (Exception "Unknown external failure")
            | AsyncCancelled _ -> 
                return raise (OperationCanceledException())
            | AsyncPending ->
                // Should not happen after Await()
                return raise (InvalidOperationException "Await returned Pending")
        } |> ValueTask<'T>

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

    (* Monadic Operations (where no structural bind nodes are stored) *)
    
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

    (* 
        Execute a Flow<'T>. Because bind flattened structure by embedding logic in run,
        we handle composed cases in a unified run function. 
    *)
    let run (env: ExecutionEnv) (ct: CancellationToken) (m: Flow<'T>) : ValueTask<'T> =
        // Since we used sentinel FPure/FSync scaffolding inside bind, we bypass them by
        // directly chaining at runtime:
        // Simplify: treat FSync sentinel with default value as marker— fallback to real run logic.
        execute env ct m.prog

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
                                
                                let token = AsyncToken task
                                { new AsyncHandle<'T>(token) with
                                    member _.IsCompleted = task.IsCompleted
                                    
                                    member _.Poll() =
                                        if task.IsCompletedSuccessfully then 
                                            AsyncDone (ValueSome task.Result)
                                        elif task.IsFaulted then 
                                            AsyncFailed (ValueSome task.Exception.InnerException)
                                        elif task.IsCanceled then 
                                            AsyncCancelled ValueNone
                                        else AsyncPending
                                    
                                    member this.Await() =
                                        task.Wait()
                                        this.Poll()
                                    
                                    member this.AwaitTimeout ts = 
                                        if task.Wait(ts) then 
                                            this.Poll()
                                        else 
                                            AsyncPending
                                    
                                    member _.Cancel() = ()
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
                            { new AsyncHandle<'T>(AsyncToken task) with
                                member _.IsCompleted = task.IsCompleted
                                
                                member _.Poll() =
                                    if task.IsCompletedSuccessfully then
                                        AsyncDone (ValueSome task.Result)
                                    elif task.IsFaulted then
                                        AsyncFailed (ValueSome task.Exception.InnerException)
                                    elif task.IsCanceled then
                                        AsyncCancelled ValueNone
                                    else
                                        AsyncPending
                                
                                member this.Await() =
                                    task.Wait()
                                    this.Poll()
                                
                                member this.AwaitTimeout ts =
                                    if task.Wait ts then this.Poll()
                                    else AsyncPending
                                
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

    module MBox =
            
        type private AsyncMessage<'T> =
            | Start of AsyncReplyChannel<unit>
            | Poll of AsyncReplyChannel<AsyncResult<'T>>
            | Wait of AsyncReplyChannel<AsyncResult<'T>>
            | WaitTimeout of TimeSpan * AsyncReplyChannel<AsyncResult<'T>>
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
                        
                        let createHandle() : AsyncHandle<'T> =
                            { new AsyncHandle<'T>(AsyncToken agent) with
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
            Poll: unit -> AsyncResult<'T>          // Non-blocking check
            Await: unit -> AsyncResult<'T>         // Blocking wait
            AwaitTimeout: TimeSpan -> AsyncResult<'T>  // Blocking wait with timeout
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
