namespace Flux.IO.Backend

module Handles =

    open FSharp.Control
    open Flux.IO.Core.Types
    open System
    open System.Threading
    open System.Threading.Tasks

    // TODO: this won't scale infinitely
    let mutable private atomicCounter = 0

    let buildHandle<'T>
            (factory: unit -> Task<'T>) 
            (cleanup: unit -> unit)
            (cancellation: unit -> unit) : ExternalHandle<'T> =
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

module Async =

    open Flux.IO.Core
    open Flux.IO.Core.Types
    open Flux.IO.Internal.Deque
    open Flux.IO.Streams.Outlets
    open FSharp.Control
    open System
    open System.Collections.Generic
    open System.Threading
    open System.Threading.Tasks
    open Handles

    let mutable private atomicCounter = 0

    let buildTaskHandle<'T>
            (factory: unit -> Task<'T>) 
            (env: ExecutionEnv) : ExternalHandle<'T> =
        buildHandle factory id id

    let buildAsyncHandle<'T>
            (comp: Async<'T>) : ExternalHandle<'T> = 
        // Delegate to existing async adapter but intercept before blocking
        let cts = new CancellationTokenSource()
        buildHandle 
            (fun () -> Async.StartAsTask(comp, cancellationToken = cts.Token)) 
            (fun () -> cts.Cancel())
            (fun () -> cts.Dispose())

    type private AsyncQueueWorkItem<'t> =
        | Enqueue of EffectResult<'t>
        | Dequeue of AsyncReplyChannel<EffectResult<'t>>
        | Complete

    type private Stopped = Stopped
    type private State<'t> = RealTimeDeque<Either<EffectResult<'t>, Stopped>>

    type AsyncSeqEffectHandle<'t>(enum : IAsyncEnumerable<'t>) =
        inherit EffectHandle<'t>(BackendToken enum)
        
        (*********************************************************)
        (*    DO NOT WRITE OUTSIDE OF MailboxProcessor.Start     *)
        (*********************************************************)
        let mutable stopped = false
        let hResult = TaskCompletionSource<EffectResult<'t>>()
        //let task = ts.Task

        // TODO: should the number of spins be configurable?
        //let updating = new ManualResetEventSlim(true, 500) // we start signalled - no blocking

        let stop () =
            Interlocked.Exchange(&stopped, true) |> ignore

        let startListener 
                (workerToken: CancellationToken) 
                (stop: unit -> unit) = 
            // the workerToken will indicate early cancellation of the async loop 
            let mboxToken = CancellationTokenSource.CreateLinkedTokenSource workerToken
            MailboxProcessor.Start(fun inbox -> async {
                let emptyState = Deque.empty 16            
                // server loop maintains a receive queue in isolation in this thread
                let rec loop state = async {
                    if mboxToken.IsCancellationRequested then
                        return ()
                    else                    
                        let next msg : Either<State<'t>, unit> =
                            match msg with
                            | Enqueue v -> Left (Deque.snoc (Left v) state)
                            | Dequeue replyChan -> 
                                match Deque.tryUncons state with
                                | None -> 
                                    replyChan.Reply EffectPending
                                    Left state                                
                                | Some (Left v, t) ->
                                    replyChan.Reply v
                                    Left t
                                | Some (Right Stopped, t) ->
                                    if Deque.isEmpty t then
                                        // if we drained the mailbox prior to receiving this, we can shutdown
                                        replyChan.Reply EffectEnded
                                        stop () |> Right
                                    else
                                        (* 
                                            This is really an odd case, but if we saw `Enqueue` after `Stop`,
                                            we now push "stop" to the back of the queue and continue reading.
                                        *)
                                        Left (Deque.snoc (Right Stopped) t)
                                | Some _ -> 
                                    Left state
                            | Complete ->
                                if Deque.isEmpty state then
                                    // if we drained the mailbox prior to receiving this, we can shutdown
                                    stop () |> Right
                                else
                                    Left (Deque.snoc (Right Stopped) state)
                        let! msg = inbox.Receive()
                        let next = next msg
                        if next.IsRight then
                            return ()
                        else
                            let nextState = next.LeftOrDefault emptyState in
                            return! loop nextState
                }
                let hAsync = loop emptyState                
                Async.StartWithContinuations(
                    hAsync,
                    (fun () -> hResult.SetResult EffectEnded), // completed: 'stop' set by 'loop'
                    (fun ex -> hResult.SetResult (EffectFailed ex) |> stop), // error: requires 'stop'
                    (fun ex -> hResult.SetResult (EffectCancelled ex) |> stop),  // cancelled: requires 'stop'
                    mboxToken.Token    
                )
            })

        let workerToken = new CancellationTokenSource()
        (* do
            workerToken.Token.Register(Action(fun () -> stop ())) |> ignore *)

        let mbox = startListener workerToken.Token stop

        let hAsync = async {
            // let hEnum = enum.GetAsyncEnumerator workerToken.Token
            try
                for item in enum do
                    if workerToken.IsCancellationRequested then
                        raise (OperationCanceledException())
                    else
                        printfn "Posting %A" item
                        mbox.Post (Enqueue (EffectOutput (ValueSome item)))
                (* while hEnum.MoveNextAsync().AsTask().Result && 
                      not workerToken.IsCancellationRequested do
                    mbox.Post (Enqueue (EffectDone (ValueSome hEnum.Current))) *)
                mbox.Post Complete
            with ex ->
                mbox.Post (Enqueue (EffectFailed ex))
            ()
        } in do Async.Start(hAsync, workerToken.Token)

        override __.IsCompleted = 
            Interlocked.CompareExchange(&stopped, true, true) <> false

        override __.Poll() = 
            if stopped || workerToken.IsCancellationRequested then 
                hResult.Task.Result
            else
                mbox.TryPostAndReply(Dequeue, 0) |> function
                | Some result -> result
                | _ -> EffectPending

        override this.Await (): EffectResult<'t> = 
            // we make a dirty read on completed here, with a quick poll before
            if stopped then EffectEnded
            elif workerToken.IsCancellationRequested then this.Poll() 
            else
                mbox.PostAndAsyncReply Dequeue
                |> Async.RunSynchronously

        override __.AwaitTimeout (arg: TimeSpan): EffectResult<'t> = 
            mbox.TryPostAndReply(Dequeue, int arg.TotalMilliseconds) |> function
                | Some result -> result
                | _ -> EffectPending
        
        override __.Cancel (): unit = 
            workerToken.CancelAsync() |> ignore
        
        override this.CancelWait (): EffectResult<'t> = 
            workerToken.Cancel()
            this.Await()
        
        override this.CancelWaitTimeout (arg: TimeSpan): EffectResult<'t> = 
            workerToken.Cancel()
            this.AwaitTimeout arg

        member private __.Dispose disposing =
            if disposing then
                hResult.Task.Dispose()
                workerToken.Dispose()

        interface IDisposable with
            member this.Dispose() =
                this.Dispose true
                GC.SuppressFinalize this

        interface IAsyncDisposable with
            member this.DisposeAsync() =
                this.Dispose true
                ValueTask.CompletedTask

    type AsyncHandle<'t>(computation: Async<'t>) =
        let x = 10

        interface ExternalHandle<'t> with
            member _.Id = Interlocked.Increment(&atomicCounter)
            member _.Class = EffectExternal typeof<Task<'t>>.FullName
            member _.IsStarted = false
            member _.IsCompleted = false
            member _.Start() = failwith "Not implemented"
            member _.Dispose() = ()

    (* let startSingleOutlet<'T> (block: Async<'T>) : EffectOutlet<'T> = 
        let hMbox = MailboxProcessor.Start(fun mbox -> async {
            try
                let! result = block

                // Store result for repeated TryDequeue
            with ex ->
                // Store error
        })

        let hAsync = buildAsyncHandle block |> _.Start()
        { new EffectOutlet<'T> with
            member __.Kind = SingleValue
            member __.TryDequeue() =
                hAsync.Poll() |> function
                | EffectOutput (ValueSome result) -> ValueSome result
                | _ -> ValueNone
            member __.AwaitActivity ts = 
                failwith "Not implemented"

            member __.IsCompleted = false
            member __.Error = None
            member __.Dispose () = ()
        } *)
    
    let createStreamOutlet<'T> (enum: IAsyncEnumerable<'T>) : EffectOutlet<'T> = 
        { new EffectOutlet<'T> with
            member __.Kind = Unknown
            member __.TryDequeue() =
                failwith "Not implemented"
            member __.AwaitActivity ts = false
            member __.IsCompleted = false
            member __.Error = None
            member __.Dispose () = ()
        }

(* 

module Task =
    val createSingleOutlet : Task<'T> -> EffectOutlet<'T>
    val createStreamOutlet : IAsyncEnumerable<'T> -> EffectOutlet<'T>

module Hopac =
    val createSingleOutlet : Job<'T> -> EffectOutlet<'T>  
    val createStreamOutlet : Stream<'T> -> EffectOutlet<'T> 
    
*)