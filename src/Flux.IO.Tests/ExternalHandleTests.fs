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
        open FSharp.Control
        open FSharp.HashCollections
        open System.Collections.Generic
        open System.Threading

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

        type Command =
            | Write of int
            | Pause of int
            | NoOp

        type ExtHandleSut() =
            let mutable model = Model.Empty

        let buildAsynEnum size multiplier pause = 
            taskSeq {
                for i in 1 .. size do
                    do! Async.Sleep(min 150 pause)
                    // printfn "Yielding %d" (i * multiplier)
                    yield i * multiplier
            }

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

        type RawHandleTestArbitraries =
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
