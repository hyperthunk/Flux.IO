namespace Flux.IO.Core

module Lift =

    open Flux.IO.Core.Types
    open System
    open System.Threading
    open System.Threading.Tasks

    let mutable private atomicCounter = 0 

    let private tryWrap body =
        try
            body()
        with
        | :? AggregateException as ae ->
            let ex =
                if isNull ae.InnerException then (ae :> exn)
                else ae.InnerException
            EffectFailed ex
        | :? OperationCanceledException as oce ->
            EffectCancelled oce
        | ex ->
            EffectFailed ex

    /// Lift a Task<'T> by eagerly wrapping as ExternalSpec.
    let task(factory: unit -> Task<'T>) : ExternalSpec<'T> =
        let spec =
            { 
                Build = fun _ ->
                    let mutable task = Unchecked.defaultof<Task<'T>>
                    let started = new ManualResetEventSlim(false)                        
                    
                    { new ExternalHandle<'T> with
                        member _.Id = Interlocked.Increment(&atomicCounter)
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
                                        EffectOutput (ValueSome task.Result)
                                    elif task.IsFaulted then 
                                        let ex =
                                            match task.Exception with
                                            | null -> null
                                            | ae when isNull ae.InnerException -> ae :> exn
                                            | ae -> ae.InnerException
                                        if isNull ex then EffectFailed (Exception())
                                        else EffectFailed ex
                                    elif task.IsCanceled then 
                                        EffectCancelled (OperationCanceledException())
                                    else EffectPending
                                
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
                                            EffectPending
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
        in spec

    /// Lift Async<'T> by eagerly wrapping as ExternalSpec.
    let async (comp: Async<'T>) : ExternalSpec<'T> =
        let spec = { 
            Build = fun _ ->
                let cts = new CancellationTokenSource()
                let started = new ManualResetEventSlim(false)
                let mutable task = Unchecked.defaultof<Task<'T>>
                
                { new ExternalHandle<'T> with
                    member _.Id = Interlocked.Increment(&atomicCounter)
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
                                    EffectOutput (ValueSome task.Result)
                                elif task.IsFaulted then
                                    let ex =
                                        match task.Exception with
                                        | null -> null
                                        | ae when Object.ReferenceEquals(ae.InnerException, null) -> ae :> exn
                                        | ae -> ae.InnerException
                                    if isNull ex then EffectFailed (Exception())
                                    else EffectFailed ex
                                elif task.IsCanceled then
                                    EffectCancelled (OperationCanceledException())
                                else
                                    EffectPending
                            
                            member this.Await() =
                                tryWrap (fun() -> 
                                    task.Wait()
                                    this.Poll()
                                )
                            
                            member this.AwaitTimeout ts =
                                tryWrap (fun() -> 
                                    if task.Wait ts then this.Poll()
                                    else EffectPending
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
        in spec

    /// Start (build + start) an ExternalSpec returning the raw EffectHandle<'T> without awaiting completion.
    /// This is synchronous and allocation-light: it executes only the builder and Start().
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

    let buildHandle<'T>
            (factory: unit -> Task<'T>) 
            (cleanup: unit -> unit)
            (cancellation: unit -> unit)
            (env: ExecutionEnv) : ExternalHandle<'T> =
        let mutable taskRef : Task<'T> = Unchecked.defaultof<_>
        let started = new ManualResetEventSlim(false)
        { new ExternalHandle<'T> with
            member _.Id = Interlocked.Increment(&atomicCounter)
            member _.Class = EffectExternal typeof<Task<'T>>.FullName
            member _.IsStarted = started.IsSet
            member _.IsCompleted = started.IsSet && not (isNull taskRef) && taskRef.IsCompleted
            member _.Start() =
                if not started.IsSet then
                    taskRef <- factory()
                    started.Set()
                { new EffectHandle<'T>(BackendToken taskRef) with
                    member _.IsCompleted = taskRef.IsCompleted
                    member _.Poll() =
                        if taskRef.IsCompletedSuccessfully then EffectOutput (ValueSome taskRef.Result)
                        elif taskRef.IsFaulted then
                            let ex =
                                match taskRef.Exception with
                                | null -> null
                                | ae when isNull ae.InnerException -> ae :> exn
                                | ae -> ae.InnerException
                            if isNull ex then EffectFailed (Exception())
                            else EffectFailed ex
                        elif taskRef.IsCanceled then EffectCancelled (OperationCanceledException())
                        else EffectPending
                    member this.Await() =
                        taskRef.Wait()
                        this.Poll()
                    member this.AwaitTimeout ts =
                        if taskRef.Wait ts then this.Poll() else EffectPending
                    member _.Cancel() = cancellation()
                    member this.CancelWait() = 
                        this.Cancel()
                        this.Await()
                    member this.CancelWaitTimeout ts = 
                        this.Cancel()
                        this.AwaitTimeout ts
                }
            member _.Dispose() =
                started.Dispose()
                if not (isNull taskRef) then taskRef.Dispose()
                cleanup()
        }

    let buildTaskHandle<'T>
            (factory: unit -> Task<'T>) 
            (env: ExecutionEnv) : ExternalHandle<'T> =
        buildHandle factory id id env

    let buildAsyncHandle<'T>
            (comp: Async<'T>) 
            (env: ExecutionEnv) : ExternalHandle<'T> = 
        // Delegate to existing async adapter but intercept before blocking
        let cts = new CancellationTokenSource()
        buildHandle 
            (fun () -> Async.StartAsTask(comp, cancellationToken = cts.Token)) 
            (fun () -> cts.Cancel())
            (fun () -> cts.Dispose())
            env

    /// Non-blocking start for a Task-producing factories.
    let taskHandle (factory: unit -> Task<'T>) : Flow<EffectHandle<'T>> =
        // 'task' currently blocks (awaits). We need a fresh spec that does NOT block.
        // Provide a minimal spec replicating the logic but returning handle directly.
        let spec =
            {
                Build = buildTaskHandle factory
                Classify = EffectExternal "Task"
                DebugLabel = Some "task-handle"
            }
        in effectHandle spec

    /// Non-blocking start for Async<'T> work.
    let asyncHandle (comp: Async<'T>) : Flow<EffectHandle<'T>> =
        let spec =
            {
                Build = buildAsyncHandle comp
                Classify = EffectExternal "Async"
                DebugLabel = Some "async-handle"
            }
        in effectHandle spec