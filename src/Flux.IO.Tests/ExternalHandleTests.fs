namespace Flux.IO.Tests

open CoreIntegrationPipeline
open CoreIntegrationPipeline.PipelineExec
open Generators
open Flux.IO.Core.Types
open Flux.IO.Streams
open FsCheck
open FSharp.HashCollections
open System
open System.Text

module ExternalHandleTests =

    module RawHandleTests =

        open Expecto
        open Flux.IO.Backend.Async
        open FsCheck
        open FsCheck.Experimental
        open FSharp.Control
        open FSharp.HashCollections
        open System.Collections.Generic
        open System.Threading
        open System.Threading.Channels

        /// Main test model
        type Model = {
            Pos : int
            Size : int
            Multiplier : int
            Pause : int
            Expected : FSharp.HashCollections.HashSet<EffectResult<int>>
            Seen: FSharp.HashCollections.HashSet<EffectResult<int>>
            Delayed : int
            Failsafe : int
        } with
            static member Empty = 
                {
                    Pos = 0
                    Size = 10
                    Multiplier = 2
                    Pause = 5
                    Expected = HashSet.empty
                    Seen = HashSet.empty
                    Delayed = 0
                    Failsafe = 10000000
                }

        type ExecContext = { Env : ExecutionEnv }
        let mkExecContext () =
            let env,_,_,_ = TestEnv.mkEnv()
            { Env = env }

        type Command<'t> =
            | Write of 't
            | DelayedWrite of 't * int
            | Fail of exn
            | NoOp
            | Cancel

        let buildAsynEnum size multiplier pause = 
            taskSeq {
                for i in 1 .. size do
                    do! Async.Sleep(min 150 pause)
                    // printfn "Yielding %d" (i * multiplier)
                    yield i * multiplier
            }

        let mkAsyncWorker<'t> () = 
            let chCommand : Channel<Choice<Command<'t>, unit>> = Channel.CreateBounded 1
            let mutable cont = true
            let tSeq : TaskSeq<'t> = taskSeq {
                while cont do
                    let! cmd = chCommand.Reader.ReadAsync().AsTask()
                    match cmd with
                    | Choice1Of2 (Write value) -> 
                        // printfn "yielding %A" value
                        yield value
                    | Choice1Of2 (DelayedWrite (value, delay)) -> 
                        // printfn "delaying for %dms" delay
                        do! Async.Sleep delay
                        // printfn "yielding %A" value
                        yield value
                    | Choice1Of2 (Fail ex) -> 
                        // printfn "failing with %A" ex
                        raise ex
                    | Choice1Of2 NoOp -> 
                        // printfn "no-op"
                        ()
                    | Choice2Of2 () ->
                        // printfn "stopping"
                        cont <- false
                    | Choice1Of2 Cancel ->
                        // this command should never be send to the server...
                        ()
            }
            chCommand, tSeq

        type AsyncSeqCmdSut<'t when 't : equality>() =
            
            let mutable hAsync = None

            let mutable chWorker : Channel<Choice<Command<'t>, unit>> option = None

            let mutable initialized = false

            do
                let chCmd, tSeq = mkAsyncWorker()
                hAsync <- Some (new AsyncSeqEffectHandle<'t>(tSeq))
                chWorker <- Some chCmd
                initialized <- true

            member __.Initialized = initialized

            member __.Handle = hAsync.Value

            member __.Cleanup () =
                Option.map 
                    (fun (chan : Channel<Choice<Command<'t>, unit>>) -> 
                        chan.Writer.TryWrite (Choice2Of2 ())
                    ) 
                    chWorker
                |> ignore

            member __.Step cmd = 
                if cmd = Cancel then
                    hAsync.Value.CancelWait() |> Some 
                elif chWorker.IsSome then
                    let success = chWorker.Value.Writer.TryWrite (Choice1Of2 cmd) 
                    if not success then failwith "Failed to write command to channel"
                    else None
                else None

            member __.Dispose () = 
                if hAsync.IsSome then
                    (hAsync.Value :> IDisposable).Dispose()

        type AsyncSeqPropRunner<'t when 't : equality>(sut : AsyncSeqCmdSut<'t>) =

            member __.Run cmds =
                let rec check cmds = 
                    match cmds with
                    | [] -> ()
                    | cmd :: rest ->
                        let output = sut.Step cmd
                        let terminated = 
                            match cmd with
                            | Cancel ->
                                Expect.isSome output "Expected Cancel to return an EffectResult"
                                match output.Value with
                                | EffectCancelled _ -> true
                                | eff -> failwith (sprintf "Cancel Unexpected effect: %A" eff)
                            | Write value ->
                                match sut.Handle.Await() with
                                | EffectOutput (ValueSome result) ->
                                    Expect.equal result value (sprintf "Expected %A, got %A" value result)
                                    false
                                | eff -> failwith (sprintf "Write Unexpected effect: %A" eff)
                            | DelayedWrite (value, delay) ->
                                let result = sut.Handle.AwaitTimeout(TimeSpan.FromMilliseconds delay)
                                match result with
                                | Result r ->
                                    match r with
                                    | EffectOutput (ValueSome v) ->
                                        Expect.equal v value (sprintf "Expected %A, got %A" value v)
                                        false
                                    | eff -> failwith (sprintf "DelayedWrite Unexpected effect: %A" eff)
                                | WaitResult fWait ->
                                    // let timeout = TimeSpan.FromSeconds 100000L
                                    // let timedAsync comp = Async.RunSynchronously(comp, timeout.Milliseconds)
                                    let block1 = async { 
                                        do! Async.Sleep (delay + 100)
                                        let result = fWait()
                                        return Some result
                                    }
                                    let block2 = async { 
                                        do! Async.Sleep (delay + 10000)
                                        return None 
                                    }
                                    Async.Choice [block1; block2] 
                                    |> Async.RunSynchronously
                                    |> function
                                    | Some v -> 
                                        match v with
                                        | EffectOutput (ValueSome v) ->
                                            Expect.equal v value (sprintf "Expected %A, got %A" value v)
                                            false
                                        | other -> failtestf "Delayed Write (Choice) Unexpected effect from WaitResult: %A" other
                                    | None -> failtest "Expected WaitResult to complete within the timeout"
                            | Fail fEx ->
                                match sut.Handle.Await() with
                                | EffectFailed ex -> 
                                    // printfn "EffectFailed ex: %A" ex
                                    let ex' = 
                                        match ex with
                                        | :? AggregateException as ae -> ae.InnerException
                                        | _ -> ex
                                    // printfn "Fail fEx - expected: %A, seen: %A" fEx ex'
                                    Expect.equal (ex'.GetType().FullName) (fEx.GetType().FullName) 
                                        "Expected exception to match"
                                    true
                                | other ->
                                    failtestf "Unexpected effect: %A" other
                            | NoOp ->
                                sut.Handle.Poll() |> function
                                | EffectPending -> false
                                | res -> failtestf "Expected an empty response from `Poll` but received %A" res
                        if not terminated then check rest
                in check cmds

            member __.Cleanup () =
                sut.Cleanup () 
                sut.Dispose () |> ignore

        type AsyncSeqHandleSut() =

            let mutable model = Model.Empty
            let mutable handle = Unchecked.defaultof<AsyncSeqEffectHandle<int>>

            member __.Start(size, multiplier, pause) =
                model <- { model with Size = size; Multiplier = multiplier; Pause = pause }
                handle <- new AsyncSeqEffectHandle<int>(buildAsynEnum size multiplier pause)

            member __.StepModel() =
                if model.Failsafe <= 0 then failtest "Failsafe limit reached (preventing infinite/blocking test)"
                let finished = model.Pos >= model.Size - 1  // Check position vs original size
                let result = handle.Await()
                match result with
                | EffectPending -> 
                    model <- { model with 
                                Delayed = model.Delayed + 1 
                                Failsafe = model.Failsafe - 1 }                    
                    Thread.SpinWait(min 100 model.Pause)
                    true
                | _ ->
                    let newModel = 
                        if finished then
                            { model with Expected = HashSet.add EffectEnded model.Expected }
                        else
                            let exp = (model.Pos + 1) * model.Multiplier
                            let hs = HashSet.add (EffectOutput (ValueSome exp)) model.Expected
                            // printfn "Expecting %d, recieved %A" exp result.Result
                            { model with Expected = hs }                
                    model <- { newModel with 
                                Seen = HashSet.add result newModel.Seen 
                                Pos = model.Pos + 1 }  // Just track position
                    not finished
            
            member __.Verify() =
                if HashSet.isEmpty model.Seen || HashSet.isEmpty model.Expected then
                    ()
                else
                    let seen = model.Seen |> HashSet.toSeq |> List.ofSeq
                    let expected = model.Expected |> HashSet.toSeq |> List.ofSeq
                    let pairs = List.zip seen expected
                    let mismatches = List.filter (fun (s, e) -> s <> e) pairs
                    if not (List.isEmpty mismatches) then
                        printfn "Mismatching pairs: %A" pairs
                    
                    Expect.isTrue
                        (List.isEmpty mismatches)
                        "Seen effects do not match expected effects"

    module Tests =
        open Expecto
        open RawHandleTests
        open Generators

        let genRawAsyncSeqTestInputs =
            gen {
                let! a = Gen.choose(5, 100)
                let! b = Gen.choose(2, 10)
                let! c = Gen.choose(20, 149)
                return (a, b, c)
            }
        
        let genAsyncCommands =
            let genWrite =
                gen {
                    let! v = Gen.choose(1, 1000)
                    return Write v
                }
            let genDelayedWrite =
                gen {
                    let! v = Gen.choose(1, 1000)
                    let! d = Gen.choose(10, 300)
                    return DelayedWrite (v, d)
                }
            let genFail =
                gen {
                    let! msg = Gen.elements ["Failure"; "Exception"; "Error"]
                    return Fail (Exception msg)
                }
            Gen.sized (fun size ->
                Gen.listOfLength (max 20 (min 3 (size - 2))) (Gen.frequency [
                    8, genWrite
                    4, genDelayedWrite
                    2, Gen.constant NoOp
                    1, genFail
                    1, Gen.constant Cancel
                ])
            )

        type RawHandleTestArbitraries =
            static member TestAsyncCommands() = genAsyncCommands |> Arb.fromGen
            static member TestRawAsyncSeq() = genRawAsyncSeqTestInputs |> Arb.fromGen

        let rawAsyncSeqconfig =
            { FsCheckConfig.defaultConfig with
                maxTest = 5  // about 23 seconds of test runs...; 100 = 389.9s for a single test case :)
                endSize = 10 // keep the end test state small so we don't spend forever in here, --stress can pick up the slack
                arbitrary = [typeof<RawHandleTestArbitraries>] }

        [<Tests>]
        let externalHandleTests =
            testList "External Handle Tests" [
                testPropertyWithConfig 
                        { rawAsyncSeqconfig with maxTest = 25 }
                        "Async Seq Handle With Arbitrary Commands" <| fun cmds ->
                    let sut = AsyncSeqCmdSut<int>()
                    let runner = AsyncSeqPropRunner<int> sut
                    runner.Run cmds

                testPropertyWithConfig 
                        { FsCheckConfig.defaultConfig with maxTest = 10 }
                        "Async Seq Handle With Bursty Producer" <| fun () ->
                    let sut = AsyncSeqHandleSut()
                    sut.Start(10, 2, 1)  // setting delay to 1 creates a very "fast" writer
                    while sut.StepModel() do
                        sut.Verify()
                
                testPropertyWithConfig rawAsyncSeqconfig "Raw Async Seq Handles" <| fun (sz, mult, pause) ->
                    let sut = AsyncSeqHandleSut()
                    sut.Start(sz, mult, pause)
                    while sut.StepModel() do
                        sut.Verify()
            ]
