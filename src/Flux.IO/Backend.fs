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
                        if taskRef.IsCompletedSuccessfully then 
                            EffectOutput (ValueSome taskRef.Result)
                        elif taskRef.IsFaulted then
                            let ex =
                                match taskRef.Exception with
                                | null -> null
                                | ae when isNull ae.InnerException -> ae :> exn
                                | ae -> ae.InnerException
                            if isNull ex then EffectFailed (Exception())
                            else EffectFailed ex
                        elif taskRef.IsCanceled then 
                            EffectCancelled (OperationCanceledException())
                        else EffectPending
                    member this.Await() =
                        taskRef.Wait()
                        this.Poll()
                    member this.AwaitTimeout ts =
                        taskRef.Wait ts |> ignore 
                        this.Poll() |> Result
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
            (fun () -> cts.Dispose())
            (fun () -> cts.Cancel())

    type private AsyncQueueWorkItem<'t> =
        (* async: server will write to its state *)
        | Enqueue of EffectResult<'t> 
        (* sync: server will reply eventually, even if with EffectPending *)
        | Dequeue of AsyncReplyChannel<EffectResult<'t>> 
        (* async: server will write to the TaskCompletionSource - could still be EffectPending *)
        | TimedDequeue of TaskCompletionSource<EffectResult<'t>>
        (* sync: server will block the caller until it has a value available *)
        | BlockingDequeue of AsyncReplyChannel<EffectResult<'t>>
        (* instruction from *)
        | Complete

    type internal Stopped = WriterStopped
    type internal Waiter<'t> = 
        | AsyncWaiter of AsyncReplyChannel<EffectResult<'t>> * int
        | TaskWaiter of TaskCompletionSource<EffectResult<'t>> * int
    // type private Deque<'t> = RealTimeDeque<Either<EffectResult<'t>, Stopped>>
    type internal EffectQueue<'t> = RealTimeDeque<Either<EffectResult<'t>, Stopped>>
    type internal ReaderQueue<'t> = RealTimeDeque<Waiter<'t>>


    type internal State<'t> = {
        Queue: EffectQueue<'t>
        WaitingReaders: ReaderQueue<'t>
        NextWaiterID: int
    }

    exception AsyncStateException of string

    (* Reply to one of 3 channel types and maintain WaitingReaders:
        - Choice1Of3: normal AsyncReplyChannel
        - Choice2Of3: waiting TaskCompletionSource
        - Choice3Of3: waiting AsyncReplyChannel
    *)
    let rec inline internal reply 
            state
            (chReply : Choice<AsyncReplyChannel<EffectResult<'t>>,
                              TaskCompletionSource<EffectResult<'t>>,
                              AsyncReplyChannel<EffectResult<'t>>>) msg =
        dispatch state chReply msg false
    and internal dispatch state chReply msg cont = 
        let state' =
            match chReply, msg with
            | Choice1Of3 rc, _ -> rc.Reply msg; state
            | Choice2Of3 tcs, EffectPending -> 
                { state with 
                    WaitingReaders = Deque.snoc (TaskWaiter (tcs, state.NextWaiterID)) state.WaitingReaders
                    NextWaiterID = state.NextWaiterID + 1
                }
            | Choice2Of3 tcs, _ -> tcs.SetResult msg; state
            // TODO: EffectEnded with waiting readers...
            | Choice3Of3 rc, EffectPending -> 
                // printfn "EffectPending with 'BlockingDequeue': storing reader %d" state.NextWaiterID
                { state with 
                    WaitingReaders = Deque.snoc (AsyncWaiter (rc, state.NextWaiterID)) state.WaitingReaders
                    NextWaiterID = state.NextWaiterID + 1
                }
            | Choice3Of3 rc, effect ->
                match Deque.tryUncons state.WaitingReaders with
                | Some (AsyncWaiter (rc2, id), t) -> 
                    // we have a pending reader - satisfy it first
                    // printfn "Satisfying pending Async reader %d" id
                    rc2.Reply effect
                    { state with 
                        WaitingReaders = Deque.snoc (AsyncWaiter (rc, state.NextWaiterID)) t 
                        NextWaiterID = state.NextWaiterID + 1
                    }
                | Some (TaskWaiter (tcs, id), t) ->
                    // we have a pending TaskCompletionSource
                    // printfn "Satisfying pending Task reader %d" id
                    tcs.SetResult effect
                    { state with 
                        WaitingReaders = Deque.snoc (AsyncWaiter (rc, state.NextWaiterID)) t 
                        NextWaiterID = state.NextWaiterID + 1
                    }
                | None ->
                    // no pending readers, so we write to the current blocking channel
                    // printfn "No pending readers, replying to BlockingDequeue directly"
                    rc.Reply effect
                    state
        in if cont then drainReaders state' msg else state'
    and internal drainReaders state msg =
        // printfn "Draining readers with state: %A" state
        match Deque.tryUncons state.WaitingReaders with
        | Some (AsyncWaiter (rc, _), t) ->
            dispatch { state with WaitingReaders = t } (Choice1Of3 rc) msg true
        | Some (TaskWaiter (tcs, _), t) ->
            dispatch { state with WaitingReaders = t } (Choice2Of3 tcs) msg true
        | None ->
            // no pending readers
            state

    let inline internal tryDequeue
            state
            (chReply : Choice<AsyncReplyChannel<EffectResult<'t>>,
                                TaskCompletionSource<EffectResult<'t>>,
                                AsyncReplyChannel<EffectResult<'t>>>) =

        // printfn "Trying to dequeue with state: %A, reply: %A" state chReply
        match Deque.tryUncons state.Queue with
        | None -> 
            // printfn "Dequeue: state.Queue empty (EffectPending)"
            reply state chReply EffectPending |> Left
        | Some (Left v, t) ->
            // printfn "Dequeued: %A (from state.Queue)" v
            reply state chReply v |> fun s -> Left { s with Queue = t }
        | Some (Right WriterStopped, t) ->
            if Deque.isEmpty t then
                // printfn "Dequeued (WriterStopped): %A" WriterStopped
                // if we drained the mailbox prior to receiving this, we can shutdown
                // however we should flush the 'EffectEnded' message to any lingering clients
                dispatch state chReply EffectEnded true |> ignore
                Right ()
            else
                (* 
                    This is really an odd case, but if we saw `Enqueue` after `Stop`,
                    we now push "stop" to the back of the queue and continue reading.

                    Because we have messages waiting, we are assuming that clients are still
                    going to turn up and consume them. We may want to introduce a policy-driven
                    timeout here, to ensure that we don't wait indefinitely.
                *)
                Left { state with 
                        Queue = Deque.snoc (Right WriterStopped) t }
        | Some (Neither, _) -> 
            // compiler foo: we never cons/snoc 'Neither' to the queue
            Left state

    type AsyncSeqEffectHandle<'t>(enum : IAsyncEnumerable<'t>) =
        inherit EffectHandle<'t>(BackendToken enum)
        
        (*********************************************************)
        (*    DO NOT WRITE OUTSIDE OF MailboxProcessor.Start     *)
        (*********************************************************)
        let hResult = TaskCompletionSource<EffectResult<'t>>()

        let emptyState = 
            {
                Queue = Deque.empty 4
                WaitingReaders = Deque.empty 2
                NextWaiterID = 1
            }

        let isTerminal = function
            | EffectEnded | EffectFailed _ | EffectCancelled _ -> true
            | _ -> false

        let (|Waiting|) state =
            match Deque.uncons state.WaitingReaders with
            | AsyncWaiter (rc, _), st -> Choice1Of3 rc, { state with WaitingReaders = st }
            | TaskWaiter (tcs, _), st -> Choice2Of3 tcs, { state with WaitingReaders = st }

        let startListener 
                (mboxToken: CancellationTokenSource) = 
            // the workerToken will indicate early cancellation of the async loop 
            MailboxProcessor.Start(fun inbox -> async {                
                // server loop maintains a receive queue in isolation in this thread
                let rec loop state = async {
                    mboxToken.Token.ThrowIfCancellationRequested()                        
                    let next msg : Either<State<'t>, unit> =
                        match msg with
                        | Enqueue v when isTerminal v -> 
                            // printfn "Enqueued terminal: %A" v
                            drainReaders state v |> ignore
                            match v with
                            | EffectFailed ex -> raise ex  // raise to ensure we show the failure to clients
                            | _ -> Right ()  // simply mark the effect as ended
                        | Enqueue v when Deque.isEmpty state.WaitingReaders ->
                            // printfn "Enqueued: %A" v
                            Left { state with Queue = Deque.snoc (Left v) state.Queue }
                        | Enqueue v ->
                            // printfn "Enqueued but with waiting readers: %A" v
                            match state with
                            | Waiting (ch, st) ->
                                reply st ch v |> Left
                        | TimedDequeue tcs        -> tryDequeue state (Choice2Of3 tcs)
                        | Dequeue chReply         -> tryDequeue state (Choice1Of3 chReply)
                        | BlockingDequeue chReply -> tryDequeue state (Choice3Of3 chReply)
                        | Complete ->
                            if Deque.isEmpty state.Queue then
                                // if we drained the mailbox prior to receiving this, we can shutdown
                                if Deque.isEmpty state.WaitingReaders then
                                    // printfn "Completed with no pending messages - shutting down"
                                    Right ()
                                else
                                    // we have pending readers
                                    // printfn "Completed with pending readers - flushing with 'EffectEnded'"
                                    drainReaders state EffectEnded |> ignore
                                    Right ()
                            else
                                // printfn "Completed with pending messages - pushing 'stop' to queue"
                                let newQueue = Deque.snoc (Right WriterStopped) state.Queue
                                Left { state with Queue = newQueue }
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
                    (fun () -> hResult.SetResult EffectEnded),          // completed
                    (fun ex -> hResult.SetResult (EffectFailed ex)),    // error
                    (fun ex -> hResult.SetResult (EffectCancelled ex)), // cancelled
                    mboxToken.Token    
                )
            })

        let workerToken = new CancellationTokenSource()
        (* do
            workerToken.Token.Register(Action(fun () -> stop ())) |> ignore *)

        let mboxToken = CancellationTokenSource.CreateLinkedTokenSource workerToken.Token

        let mbox = startListener mboxToken
        
        // NOT: synchronously enumerating can drive thread-pool exhaustion!

        (* let hAsync = async {
            // let hEnum = enum.GetAsyncEnumerator workerToken.Token            
            try
                for item in enum do
                    workerToken.Token.ThrowIfCancellationRequested()
                    mbox.Post (Enqueue (EffectOutput (ValueSome item)))
                (* while hEnum.MoveNextAsync().AsTask().Result && 
                        not workerToken.IsCancellationRequested do
                    mbox.Post (Enqueue (EffectDone (ValueSome hEnum.Current))) *)
                mbox.Post Complete
            with 
            | :? OperationCanceledException as oce ->
                mbox.Post (Enqueue (EffectCancelled oce))
            | ex ->
                mbox.Post (Enqueue (EffectFailed ex))
        } in do Async.Start(hAsync, workerToken.Token) *)

        (* DO NOT touch outside of the async writer block *)
        let mutable enumerator = Unchecked.defaultof<IAsyncEnumerator<'t>>

        let hWriter = async {
            // NOTE: handle exceptions outside 'consumeNext' via 'hAsync' wrapper
            enumerator <- enum.GetAsyncEnumerator workerToken.Token
            let rec consumeNext() = async {
                workerToken.Token.ThrowIfCancellationRequested()
                    
                let! hasNext = 
                    enumerator.MoveNextAsync().AsTask() 
                    |> Async.AwaitTask
                
                if hasNext then
                    mbox.Post (Enqueue (EffectOutput (ValueSome enumerator.Current)))
                    return! consumeNext()
                else
                    mbox.Post Complete
            }            
            
            do! consumeNext()
        } 
        
        let hAsync = async {
            try 
                Async.StartWithContinuations(
                    hWriter,
                    id, // completed: 'stop' set by 'loop'
                    (fun ex -> mbox.Post (Enqueue (EffectFailed ex))), // error: requires 'stop'
                    (fun ex -> mbox.Post (Enqueue (EffectCancelled ex))),  // cancelled: requires 'stop'
                    workerToken.Token
                )
            finally 
                if not (isNull enumerator) then
                    try
                        enumerator.DisposeAsync().AsTask().Wait()
                    with _ -> ()
        } in do Async.Start(hAsync, workerToken.Token)


        override __.IsCompleted = hResult.Task.IsCompleted

        override __.Poll() = 
            if workerToken.IsCancellationRequested || 
               hResult.Task.IsCompleted || 
               hResult.Task.IsFaulted then hResult.Task.Result
            else
                mbox.PostAndReply (fun rc -> Dequeue rc)

        override __.Await (): EffectResult<'t> =
            let read =
                async {
                    let! res = mbox.PostAndAsyncReply BlockingDequeue
                    return Some res
                }

            let died =
                async {
                    // If the server loop completes (Ended/Failed/Cancelled) before the read is served,
                    // return that terminal result instead of waiting forever.
                    let! res = Async.AwaitTask hResult.Task
                    return Some res
                }

            match Async.Choice [ read; died ] |> Async.RunSynchronously with
            | Some res -> res
            | None -> raise (AsyncStateException "Invalid Async State (Await)")

        override __.AwaitTimeout (arg: TimeSpan): WaitableResult<'t> = 
            let tcs = new TaskCompletionSource<EffectResult<'t>>()
            mbox.Post (TimedDequeue tcs)
            let mkAwaiters (delay : int) = 
                [
                    async {
                        return! Async.ofTask (task {
                            if tcs.Task.Wait delay then
                                return Some tcs.Task.Result
                            else
                                return None
                        })
                    };
                    async {
                        return! Async.ofTask (task {
                            if hResult.Task.Wait delay then
                                return Some hResult.Task.Result
                            else
                                return None
                        })
                    }
                ]

            mkAwaiters (int arg.TotalMilliseconds)
            |> Async.Choice
            |> Async.RunSynchronously
            |> function
            | Some res -> Result res
            | None -> 
                WaitResult (fun () -> 
                    mkAwaiters -1
                    |> Async.Choice
                    |> Async.RunSynchronously
                    |> function 
                    | None -> failwithf "Invalid Async State"
                    | Some rs -> rs
                )
            
        override __.Cancel (): unit = 
            workerToken.CancelAsync() |> ignore
        
        override this.CancelWait (): EffectResult<'t> = 
            workerToken.Cancel()
            this.Await()

        override this.CancelWaitTimeout (arg: TimeSpan): WaitableResult<'t> = 
            workerToken.Cancel()
            this.AwaitTimeout arg

        member private __.Dispose disposing =
            if disposing then
                hResult.Task.Dispose()
                workerToken.Dispose()
                mboxToken.Dispose()

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