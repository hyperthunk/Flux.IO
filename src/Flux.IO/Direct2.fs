namespace Flux.IO.Pipeline

module Direct = 
    open Flux.IO.Core.Types
    open System
    open System.Threading
    open System.Threading.Tasks

    // Sentinel to kickoff an interpreter-driven bind chain.
    // We throw this from a private FSync node and catch it in the interpreter.
    exception private RunBindEx of obj

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

    // -----------------------------------------------------------------------------
    // Direct Interpreter for FlowProg<'T>
    // -----------------------------------------------------------------------------

    // Run a single external operation. This currently blocks the calling thread while
    // waiting for completion. Later this can be replaced with a non-blocking scheduler.
    let private runExternal 
            (env: ExecutionEnv) 
            (ct: CancellationToken) 
            (spec: ExternalSpec<'T>) : ValueTask<'T> =
        task {
            use handle = spec.Build env
            let asyncHandle = handle.Start()

            use _reg = ct.Register(fun () ->
                try asyncHandle.Cancel() with _ -> ())

            // Await the external handle and normalize results to exceptions or value
            let res =
                try asyncHandle.Await()
                with ex -> AsyncFailed (ValueSome ex)

            match res with
            | AsyncDone (ValueSome value) ->
                return value
            | AsyncDone ValueNone ->
                return raise (InvalidOperationException "External completed with no value")
            | AsyncFailed (ValueSome ex) ->
                return raise ex
            | AsyncFailed ValueNone ->
                return raise (Exception "Unknown external failure")
            | AsyncCancelled (ValueSome oce) ->
                if ct.IsCancellationRequested then
                    return raise (OperationCanceledException("Operation cancelled", oce, ct))
                else
                    return raise oce
            | AsyncCancelled ValueNone ->
                return raise (OperationCanceledException(ct))
            | AsyncPending ->
                return raise (InvalidOperationException "Await returned Pending")
        } |> ValueTask<'T>

    // The core interpreter. It executes the structural IR and returns a ValueTask<'T>.
    // It also recognizes the RunBindEx sentinel thrown from our bind wrapper to interpret
    // a composed sequence without "baking" the composition into the IR.
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
                // Normal sync function (fast path), but allow a sentinel kick-off for bind.
                try
                    let r = f env
                    ValueTask<'T>(r)
                with
                | RunBindEx o ->
                    // Unbox the interpreter thunk and run it
                    let thunk = unbox<(ExecutionEnv * CancellationToken -> ValueTask<'T>)> o
                    thunk (env, ct)
                | ex ->
                    ValueTask<'T>(Task.FromException<'T> ex)

            | FExternal spec ->
                runExternal env ct spec

            | FDelay thunk ->
                // Evaluate the delayed program and continue
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
                                             fun _ -> FPure(fun () -> raise hex))
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

    // -----------------------------------------------------------------------------
    // Monadic Operations implemented over the interpreter (no blocking shortcut)
    // -----------------------------------------------------------------------------

    // Sequencing via interpreter. We do NOT block; we compose ValueTasks using the interpreter,
    // initiated through a sentinel FSync node so that the "phantom" shape remains intact.
    let private runBind<'A,'B>
            (env: ExecutionEnv)
            (ct: CancellationToken)
            (m: Flow<'A>)
            (k: 'A -> Flow<'B>) : ValueTask<'B> =
        let vtA = execute env ct m.Program
        if vtA.IsCompletedSuccessfully then
            let a =
                try vtA.Result
                with ex -> return ValueTask<'B>(Task.FromException<'B> ex)
            execute env ct (k a).Program
        else
            task {
                let! a = vtA
                return! execute env ct (k a).AsTask()
            } |> ValueTask<'B>

    // Explicit generic ordering: k first, then m, helps solver keep 'A and 'B distinct.
    let inline bind<'A,'B> (k: 'A -> Flow<'B>) (m: Flow<'A>) : Flow<'B> =
        // We encode a tiny node that, when interpreted, throws a sentinel carrying the
        // interpreter continuation. This avoids baking blocking logic and keeps the IR minimal.
        { Program =
            FDelay (fun () ->
                FSync (fun _ ->
                    // Kick off runBind via sentinel; the interpreter will catch and run it.
                    raise (RunBindEx (box (fun (env: ExecutionEnv, ct: CancellationToken) ->
                        runBind env ct m k))))) }

    // map implemented via bind -> return
    let inline map<'A,'B> (f: 'A -> 'B) (m: Flow<'A>) : Flow<'B> =
        bind (f >> pure') m

    let inline apply (mf: Flow<'a -> 'b>) (ma: Flow<'a>) : Flow<'b> =
        bind (fun f -> map f ma) mf

    // -----------------------------------------------------------------------------
    // Public runner
    // -----------------------------------------------------------------------------
    let run (env: ExecutionEnv) (ct: CancellationToken) (m: Flow<'T>) : ValueTask<'T> =
        execute env ct m.Program

    // -----------------------------------------------------------------------------
    // Computation Expression Builder
    // -----------------------------------------------------------------------------
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

        /// Lift a Task<'T> by wrapping as ExternalSpec. Await is normalized to AsyncResult.
        let task(factory: unit -> Task<'T>) : Flow<'T> =
            let spec =
                { 
                    Build = fun _ ->
                        let mutable task = Unchecked.defaultof<Task<'T>>
                        let started = new ManualResetEventSlim(false)
                        
                        { new ExternalHandle<'T> with
                            member _.Id = 123 // TODO: better id
                            member _.Class = EffectExternal "System.Threading.Tasks.Task"
                            member _.IsStarted = started.IsSet
                            member _.IsCompleted = started.IsSet && not (isNull task) && task.IsCompleted
                            
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
                                            let ex =
                                                match task.Exception with
                                                | null -> null
                                                | ae when obj.ReferenceEquals(ae.InnerException, null) -> ae :> exn
                                                | ae -> ae.InnerException
                                            if isNull ex then AsyncFailed ValueNone
                                            else AsyncFailed (ValueSome ex)
                                        elif task.IsCanceled then 
                                            AsyncCancelled ValueNone
                                        else AsyncPending
                                    
                                    member this.Await() =
                                        try
                                            task.Wait()
                                            this.Poll()
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
                                    
                                    member this.AwaitTimeout ts = 
                                        try
                                            if task.Wait(ts) then this.Poll()
                                            else AsyncPending
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

        /// Lift Async<'T> with cancellation bridging. Await is normalized to AsyncResult.
        let async (comp: Async<'T>) : Flow<'T> =
            let spec = { 
                Build = fun _ ->
                    let cts = new CancellationTokenSource()
                    let started = new ManualResetEventSlim(false)
                    let mutable task = Unchecked.defaultof<Task<'T>>
                    
                    { new ExternalHandle<'T> with
                        member _.Id = 123 // TODO: better id
                        member _.Class = EffectExternal "FSharp.Control.Async"
                        member _.IsStarted = started.IsSet
                        member _.IsCompleted = started.IsSet && not (isNull task) && task.IsCompleted
                        
                        member _.Start() =
                            if not started.IsSet then
                                task <- Async.StartAsTask(comp, cancellationToken = cts.Token)
                                started.Set()
                            
                            { new AsyncHandle<'T>(AsyncToken task) with
                                member _.IsCompleted = task.IsCompleted
                                
                                member _.Poll() =
                                    if task.IsCompletedSuccessfully then
                                        AsyncDone (ValueSome task.Result)
                                    elif task.IsFaulted then
                                        let ex =
                                            match task.Exception with
                                            | null -> null
                                            | ae when obj.ReferenceEquals(ae.InnerException, null) -> ae :> exn
                                            | ae -> ae.InnerException
                                        if isNull ex then AsyncFailed ValueNone
                                        else AsyncFailed (ValueSome ex)
                                    elif task.IsCanceled then
                                        AsyncCancelled ValueNone
                                    else
                                        AsyncPending
                                
                                member this.Await() =
                                    try
                                        task.Wait()
                                        this.Poll()
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
                                
                                member this.AwaitTimeout ts =
                                    try
                                        if task.Wait ts then this.Poll()
                                        else AsyncPending
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
                                    async {
                                        completed.Wait()
                                        reply.Reply result
                                    } |> Async.Start
                                    return! running()
                                | WaitTimeout (ts, reply) ->
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
                            member _.Id = 123 //TODO
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
                    DebugLabel = Some "lift-async-mbox" 
                }
            externalSpec spec
    
    module Async =
        
        // Represents an already started async operation that can be checked multiple times
        type StartedAsync<'T> = {
            Poll: unit -> AsyncResult<'T>
            Await: unit -> AsyncResult<'T>
            AwaitTimeout: TimeSpan -> AsyncResult<'T>
            Cancel: unit -> unit
            IsCompleted: unit -> bool
        }

        /// Start an async computation that can be checked multiple times
        let startAsync (asyncWork: Async<'T>) : Async<StartedAsync<'T>> =
            async {
                let completed = new ManualResetEventSlim(false)
                let mutable result = AsyncPending
                let cts = new CancellationTokenSource()
                
                Async.StartWithContinuations(
                    asyncWork,
                    (fun value -> 
                        result <- AsyncDone (ValueSome value)
                        completed.Set()),
                    (fun ex -> 
                        result <- AsyncFailed (ValueSome ex)
                        completed.Set()),
                    (fun _ -> 
                        result <- AsyncCancelled ValueNone
                        completed.Set()),
                    cts.Token
                )
                
                return {
                    Poll = fun () -> if completed.IsSet then result else AsyncPending
                    Await = fun () -> completed.Wait(); result
                    AwaitTimeout = fun timeout -> if completed.Wait(timeout) then result else AsyncPending
                    Cancel = fun () -> cts.Cancel()
                    IsCompleted = fun () -> completed.IsSet
                }
            }

        /// Run async work with timeout, returning either the result or the handle to check later
        let withTimeout 
                (timeout: TimeSpan) 
                (asyncWork: Async<'T>) : Flow<Choice<'T, StartedAsync<'T>>> =
            flow {
                let! started = Lift.async (startAsync asyncWork)
                match started.AwaitTimeout timeout with
                | AsyncDone (ValueSome value) -> return Choice1Of2 value
                | AsyncFailed (ValueSome ex) -> return raise ex
                | AsyncCancelled _ -> return raise (OperationCanceledException())
                | _ -> return Choice2Of2 started
            }

        /// Helper to retry a timed-out operation
        let retryTimeout 
                (timeout: TimeSpan) 
                (started: StartedAsync<'T>) : Async<Choice<'T, StartedAsync<'T>>> =
            async {
                match started.AwaitTimeout timeout with
                | AsyncDone (ValueSome value) -> return Choice1Of2 value
                | AsyncFailed (ValueSome ex) -> return raise ex
                | AsyncCancelled _ -> return raise (OperationCanceledException())
                | _ -> return Choice2Of2 started
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
                    | Choice1Of2 value -> return Some value
                    | Choice2Of2 started ->
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
                let! startedOps = 
                    asyncWorks
                    |> List.map startAsync
                    |> Async.Parallel
                    |> Lift.async
                
                let! results =
                    startedOps
                    |> Array.map (fun started ->
                        async {
                            match started.AwaitTimeout timeout with
                            | AsyncDone (ValueSome value) -> return (Some value, started)
                            | _ -> return (None, started)
                        })
                    |> Async.Parallel
                    |> Lift.async
                
                return Array.toList results
            }

        /// Create a Flow that runs async work then continues (sequentially)
        let fork (asyncWork: Async<'T>) (continuation: 'T -> Flow<'R>) : Flow<'R> =
            flow {
                let! result = Lift.async asyncWork
                return! continuation result
            }
        
        /// Run async operations in "parallel" (sequentially in Direct; real parallel later)
        let par (asyncWorks: Async<'T> list) : Flow<'T list> =
            flow {
                let! tasks = 
                    asyncWorks 
                    |> List.map Lift.async 
                    |> List.map (fun f -> flow { return! f })
                    |> List.fold (fun acc elem ->
                        flow {
                            let! results = acc
                            let! result = elem
                            return result :: results
                        }) (flow { return [] })
                return List.rev tasks
            }

    // Extended CE helpers
    type FlowBuilder with
        member __.Fork(asyncWork: Async<'T>, continuation: 'T -> Flow<'R>) : Flow<'R> =
            Async.fork asyncWork continuation
        
        member __.Parallel(asyncWorks: Async<'T> list) : Flow<'T list> =
            Async.par asyncWorks
        
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
                })

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
                        let (StreamProcessor g) = f outEnv.Payload
                        return! g outEnv
                    | EmitMany envs ->
                        // NOTE: to preserve laziness we'd stream these; for tests we realize eagerly
                        let results : Envelope<'c> list =
                            envs
                            |> List.collect (fun outEnv ->
                                let (StreamProcessor g) = f outEnv.Payload
                                let vt = g outEnv |> run execEnv CancellationToken.None
                                match vt.Result with
                                | Emit e -> [e]
                                | EmitMany es -> es
                                | _ -> [])
                        return EmitMany results
                    | Consume -> return Consume
                    | Complete -> return Complete
                    | Error e -> return Error e
                })