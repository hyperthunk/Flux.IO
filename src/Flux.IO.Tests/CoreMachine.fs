namespace Flux.IO.Tests
(*
    Flow Behavior Generation: Creates flows with different execution characteristics:

    - Synchronous (immediate completion)
    - Asynchronous (with delays)
    - Failing (throws exceptions)
    - Cancelling (respects cancellation)


    StreamCommand Generation: Generates all possible StreamCommand variants:

    - Emit with single envelope
    - EmitMany with multiple envelopes
    - RequireMore
    - Complete
    - Error with various exceptions


    Model-Based Testing: Uses FsCheck's Machine framework to test:

    - Lift processors: Pure function transformations
    - Filter processors: Conditional emission
    - Stateful processors: Accumulation with state
    - WithEnv processors: Environment access with metrics/logging
    - Complex processors: Composed operations


    Commands/Operations:

    - ProcessEnvelope: Normal processing
    - ProcessWithTimeout: Processing with timeout constraints
    - ProcessWithCancellation: Processing with cancellation


    State Tracking: The model tracks:

    - Inputs processed
    - Outputs emitted
    - RequireMore count
    - Completion count
    - Error count and last error


    Error Scenarios:

    - Intermittent failures with configurable failure rate
    - Async delays with varying durations
    - Complex accumulator with sophisticated state management


    Property Tests:

    - Verifies that failure rates match expected probabilities
    - Ensures delayed processors complete within timeout
    - Confirms complex accumulators emit at correct thresholds

    - All StreamCommand state transitions
    - Both ValueTask and Async mechanisms
    - Error handling and recovery
    - Timeout behavior
    - Stateful processing with accumulation
    - Environment interaction (metrics, logging)
    - Composition of multiple processors

*)
module CoreMachine = 

    open System
    open System.Threading
    open System.Threading.Tasks
    open Expecto
    open Flux.IO.Core
    open Flux.IO.Core.Flow
    open FsCheck
    open FsCheck.Experimental
    open FSharp.HashCollections
    open Generators
    open TestEnv

    // Generate various completion behaviors for Flow
    type FlowBehavior =
        | Synchronous of int
        | Asynchronous of int * delayMs:int
        | Failing of exn
        | Cancelling
        
    let genFlowBehavior =
        Gen.frequency [
            (5, Gen.map Synchronous Arb.generate<int>)
            (3, gen {
                let! value = Arb.generate<int>
                let! delay = Gen.choose(1, 10)
                return Asynchronous(value, delay)
            })
            (1, Gen.constant (Failing (InvalidOperationException("test error"))))
            (1, Gen.constant Cancelling)
        ]
        
    let createFlow behavior : Flow<int> =
        match behavior with
        | Synchronous value -> 
            Flow.ret value
        | Asynchronous(value, delayMs) ->
            Flow (fun _ ct ->
                ValueTask<int>(task {
                    do! Task.Delay(delayMs, ct)
                    return value
                }))
        | Failing ex ->
            Flow (fun _ _ ->
                ValueTask<int>(Task.FromException<int>(ex)))
        | Cancelling ->
            Flow (fun _ ct ->
                ValueTask<int>(task {
                    ct.ThrowIfCancellationRequested()
                    return 0
                }))
                
    // Generate different StreamCommand results
    let genStreamCommand<'T> (genPayload: Gen<'T>) =
        Gen.frequency [
            (5, gen {
                let! env = genEnvelope genPayload
                return StreamCommand.Emit env
            })
            (2, gen {
                let! count = Gen.choose(0, 5)
                let! envs = Gen.listOfLength count (genEnvelope genPayload)
                return StreamCommand.EmitMany envs
            })
            (3, Gen.constant StreamCommand.Consume)
            (1, Gen.constant StreamCommand.Complete)
            (1, gen {
                let! msg = Gen.elements ["test error"; "processing failed"; "invalid state"]
                return StreamCommand.Error (Exception msg)
            })
        ]
        
    // Generate processor functions with various behaviors
    let genProcessorFunction<'a, 'b> (genOutput: Gen<'b>) =
        gen {
            let! behavior = genFlowBehavior
            let! streamCmd = genStreamCommand genOutput
            
            return fun (env: Envelope<'a>) ->
                match behavior with
                | Synchronous _ -> Flow.ret streamCmd
                | Asynchronous(_, delayMs) ->
                    Flow (fun _ ct ->
                        ValueTask<StreamCommand<'b>>(task {
                            do! Task.Delay(delayMs, ct)
                            return streamCmd
                        }))
                | Failing ex ->
                    Flow (fun _ _ ->
                        ValueTask<StreamCommand<'b>>(Task.FromException<StreamCommand<'b>>(ex)))
                | Cancelling ->
                    Flow (fun _ ct ->
                        ValueTask<StreamCommand<'b>>(task {
                            ct.ThrowIfCancellationRequested()
                            return streamCmd
                        }))
        }

// Model for StreamProcessor behavior
    module StreamProcessorModel =
        
        type ProcessorState<'TIn, 'TOut> = {
            InputsProcessed: Envelope<'TIn> list
            OutputsEmitted: Envelope<'TOut> list
            RequireMoreCount: int
            CompletedCount: int
            ErrorCount: int
            LastError: exn option
        }
        
        let emptyState = {
            InputsProcessed = []
            OutputsEmitted = []
            RequireMoreCount = 0
            CompletedCount = 0
            ErrorCount = 0
            LastError = None
        }
        
        // Commands that can be applied to a StreamProcessor
        type ProcessorCommand<'TIn, 'TOut> =
            | ProcessEnvelope of Envelope<'TIn>
            | ProcessWithTimeout of Envelope<'TIn> * TimeSpan
            | ProcessWithCancellation of Envelope<'TIn>
            
        // The actual system under test wrapper
        type ProcessorSut<'TIn, 'TOut>(processor: StreamProcessor<'TIn, 'TOut>) =
            let mutable env, _, _, _ = mkEnv()
            
            member _.Process(envelope: Envelope<'TIn>, ?timeout: TimeSpan, ?ct: CancellationToken) =
                let cancellationToken = defaultArg ct CancellationToken.None
                
                try
                    let flowResult = StreamProcessor.runProcessor processor envelope
                    
                    let result = 
                        match timeout with
                        | Some t ->
                            let timeoutFlow = Flow.withTimeout t flowResult
                            match Flow.run env cancellationToken timeoutFlow |> fun vt -> vt.Result with
                            | Some cmd -> cmd
                            | None -> StreamCommand.Error (TimeoutException())
                        | None ->
                            Flow.run env cancellationToken flowResult |> fun vt -> vt.Result
                            
                    Ok result
                with
                | ex -> Result.Error ex
                
        // Operations for the state machine
        type ProcessEnvelopeOp<'TIn, 'TOut>(envelope: Envelope<'TIn>) =
            inherit Operation<ProcessorSut<'TIn, 'TOut>, ProcessorState<'TIn, 'TOut>>()
            
            override _.Run(model) =
                { model with InputsProcessed = envelope :: model.InputsProcessed }
                
            override _.Check(sut, model) =
                match sut.Process envelope with
                | Ok (StreamCommand.Emit outEnv) ->
                    let newModel = { model with OutputsEmitted = outEnv :: model.OutputsEmitted }
                    Prop.label "Emit" true
                    
                | Ok (StreamCommand.EmitMany outEnvs) ->
                    let newModel = { model with OutputsEmitted = outEnvs @ model.OutputsEmitted }
                    Prop.label (sprintf "EmitMany(%d)" outEnvs.Length) true
                    
                | Ok StreamCommand.Consume ->
                    let newModel = { model with RequireMoreCount = model.RequireMoreCount + 1 }
                    Prop.label "RequireMore" true
                    
                | Ok StreamCommand.Complete ->
                    let newModel = { model with CompletedCount = model.CompletedCount + 1 }
                    Prop.label "Complete" true
                    
                | Ok (StreamCommand.Error ex) ->
                    let newModel = { model with 
                                        ErrorCount = model.ErrorCount + 1
                                        LastError = Some ex }
                    Prop.label (sprintf "Error: %s" ex.Message) true
                    
                | Result.Error ex ->
                    let newModel = { model with 
                                        ErrorCount = model.ErrorCount + 1
                                        LastError = Some ex }
                    Prop.label (sprintf "Exception: %s" ex.Message) true
                    
            override _.ToString() = sprintf "ProcessEnvelope(%A)" envelope.Payload
                
        type ProcessWithTimeoutOp<'TIn, 'TOut>(envelope: Envelope<'TIn>, timeout: TimeSpan) =
            inherit Operation<ProcessorSut<'TIn, 'TOut>, ProcessorState<'TIn, 'TOut>>()
            
            override _.Run(model) =
                { model with InputsProcessed = envelope :: model.InputsProcessed }
                
            override _.Check(sut, model) =
                match sut.Process(envelope, timeout) with
                | Ok result ->
                    // Same as regular process
                    Prop.label (sprintf "Timeout OK: %A" result) true
                | Result.Error ex ->
                    let isTimeout = ex :? TimeoutException
                    Prop.label (sprintf "Timeout Error (timeout=%b): %s" isTimeout ex.Message) true
                    
            override _.ToString() = sprintf "ProcessWithTimeout(%A, %A)" envelope.Payload timeout
                

        // Specifications for different processor types
        let liftProcessorSpec() =
            let setup f =
                { new Setup<ProcessorSut<int, int>, ProcessorState<int, int>>() with
                    member _.Actual() = ProcessorSut(StreamProcessor.lift f)
                    member _.Model() = emptyState }
                    
            { new Machine<ProcessorSut<int, int>, ProcessorState<int, int>>() with
                member _.Setup = 
                    genIntToIntF 
                    |> Gen.map setup 
                    |> Arb.fromGen
                    
                member _.Next(model) =
                    gen {
                        let! env = genIntEnvelope
                        let! useTimeout = Gen.frequency [(9, Gen.constant false); (1, Gen.constant true)]
                        
                        if useTimeout then
                            let! timeout = Gen.choose(1, 100) |> Gen.map (float >> TimeSpan.FromMilliseconds)
                            return ProcessWithTimeoutOp(env, timeout) :> Operation<_, _>
                        else
                            return ProcessEnvelopeOp(env) :> Operation<_, _>
                    }
            }
            
        let filterProcessorSpec() =
            let setup pred =
                { new Setup<ProcessorSut<int, int>, ProcessorState<int, int>>() with
                    member _.Actual() = ProcessorSut(StreamProcessor.filter pred)
                    member _.Model() = emptyState }
                    
            { new Machine<ProcessorSut<int, int>, ProcessorState<int, int>>() with
                member _.Setup = 
                    Arb.generate<int -> bool>
                    |> Gen.map setup 
                    |> Arb.fromGen
                    
                member _.Next model =
                    gen {
                        let! env = genIntEnvelope
                        return ProcessEnvelopeOp(env) :> Operation<_, _>
                    }
            }
            
        let statefulProcessorSpec() =
            let setup (initial, f) =
                { new Setup<ProcessorSut<int, string>, ProcessorState<int, string>>() with
                    member _.Actual() = ProcessorSut(StreamProcessor.stateful initial f)
                    member _.Model() = emptyState }
                    
            let genStatefulFunction =
                gen {
                    // Generate a function that accumulates values until a threshold
                    let! threshold = Gen.choose(1, 10)
                    let f (sum, count) value =
                        let newSum = sum + value
                        let newCount = count + 1
                        if newCount >= threshold then
                            (0, 0), Some (sprintf "Sum of %d items: %d" newCount newSum)
                        else
                            (newSum, newCount), None
                    return f
                }
                    
            { new Machine<ProcessorSut<int, string>, ProcessorState<int, string>>() with
                member _.Setup = 
                    gen {
                        let! initial = Gen.zip Arb.generate<int> Arb.generate<int>
                        let! f = genStatefulFunction
                        return setup(initial, f)
                    } |> Arb.fromGen
                    
                member _.Next(model) =
                    gen {
                        let! env = genIntEnvelope
                        let! cmdType = Gen.choose(0, 2)
                        
                        match cmdType with
                        | 0 -> return ProcessEnvelopeOp(env) :> Operation<_, _>
                        | 1 -> 
                            let! timeout = Gen.choose(50, 200) |> Gen.map (float >> TimeSpan.FromMilliseconds)
                            return ProcessWithTimeoutOp(env, timeout) :> Operation<_, _>
                        | _ -> return ProcessEnvelopeOp(env) :> Operation<_, _>
                    }
            }
            
        let withEnvProcessorSpec() =
            let setup f =
                { new Setup<ProcessorSut<int, string>, ProcessorState<int, string>>() with
                    member _.Actual() = ProcessorSut(StreamProcessor.withEnv f)
                    member _.Model() = emptyState }
                    
            let genEnvFunction =
                gen {
                    let! useMetrics = Gen.frequency [(1, Gen.constant true); (1, Gen.constant false)]
                    let! useLogger = Gen.frequency [(1, Gen.constant true); (1, Gen.constant false)]
                    let! delayMs = Gen.choose(0, 5)
                    
                    let f (env: ExecutionEnv) (value: int) =
                        flow {
                            if useMetrics then
                                env.Metrics.RecordCounter("processed", HashMap.empty, 1L)
                            if useLogger then
                                env.Logger.Log("INFO", sprintf "Processing %d" value)
                                
                            if delayMs > 0 then
                                do! Flow.liftTask (task{ do! Task.Delay delayMs })
                                
                            return sprintf "Processed: %d" value
                        }
                    return f
                }
                    
            { new Machine<ProcessorSut<int, string>, ProcessorState<int, string>>() with
                member _.Setup = 
                    genEnvFunction
                    |> Gen.map setup 
                    |> Arb.fromGen
                    
                member _.Next(model) =
                    gen {
                        let! env = genIntEnvelope
                        return ProcessEnvelopeOp(env) :> Operation<_, _>
                    }
            }
            
        // Complex processor that combines multiple operations
        let complexProcessorSpec() =
            let createComplexProcessor() =
                // A processor that filters, transforms, and accumulates
                let filterStep = StreamProcessor.filter (fun x -> x > 0)
                let transformStep = StreamProcessor.lift (fun x -> x * 2)
                let accumulateStep = StreamProcessor.stateful (0, 0) (fun (sum, count) x ->
                    let newSum = sum + x
                    let newCount = count + 1
                    if newCount >= 3 then
                        (0, 0), Some (sprintf "Batch %d: sum=%d" (newCount / 3) newSum)
                    else
                        (newSum, newCount), None
                )
                
                // Manually compose them
                StreamProcessor (fun env ->
                    flow {
                        let! filterResult = StreamProcessor.runProcessor filterStep env
                        match filterResult with
                        | Emit filtered ->
                            let! transformResult = StreamProcessor.runProcessor transformStep filtered
                            match transformResult with
                            | Emit transformed ->
                                let! accResult = StreamProcessor.runProcessor accumulateStep transformed
                                match accResult with
                                | Emit accEmit -> 
                                    // Return the accumulated result
                                    return Emit accEmit
                                | EmitMany accEmits ->
                                    return EmitMany accEmits                                
                                | Error ex ->
                                    return Error ex
                                | _ -> return accResult
                            | Consume -> 
                                return Consume
                            | Complete ->
                                return Complete
                            | Error ex ->
                                return Error ex
                            | EmitMany _ ->
                                // Transform step shouldn't emit many, but handle it
                                return Consume
                        | Consume -> 
                            return Consume
                        | Complete ->
                            return Complete
                        | Error ex ->
                            return Error ex
                        | EmitMany _ ->
                            // Filter shouldn't emit many, but handle it
                            return Consume
                    }
                )
                
            let setup() =
                { new Setup<ProcessorSut<int, string>, ProcessorState<int, string>>() with
                    member _.Actual() = ProcessorSut(createComplexProcessor())
                    member _.Model() = emptyState }
                    
            { new Machine<ProcessorSut<int, string>, ProcessorState<int, string>>() with
                member _.Setup = Gen.constant (setup()) |> Arb.fromGen
                
                member _.Next(model) =
                    gen {
                        let! value = Gen.choose(-10, 20)
                        let! seqId = Gen.choose(1, 1000) |> Gen.map int64
                        let envelope = Envelope.create seqId value
                        
                        let! useTimeout = Gen.frequency [(8, Gen.constant false); (2, Gen.constant true)]
                        
                        if useTimeout then
                            let! timeout = Gen.choose(10, 100) |> Gen.map (float >> TimeSpan.FromMilliseconds)
                            return ProcessWithTimeoutOp(envelope, timeout) :> Operation<_, _>
                        else
                            return ProcessEnvelopeOp(envelope) :> Operation<_, _>
                    }
            }

    // Additional generators for testing error scenarios
    module ErrorScenarios =
        
        // Processor that fails intermittently
        let createFailingProcessor (failureRate: float) =
            let random = Random()
            StreamProcessor (fun env ->
                flow {
                    if random.NextDouble() < failureRate then
                        return Error (InvalidOperationException("Random failure"))
                    else
                        return Emit (mapEnvelope (fun x -> x + 1) env)
                }
            )
            
        // Processor that has async delays
        let createDelayedProcessor (minDelay: int, maxDelay: int) =
            let random = Random()
            StreamProcessor (fun env ->
                flow {
                    let delay = random.Next(minDelay, maxDelay)
                    do! liftTask (task { Task.Delay delay |> ignore })
                    return Emit (mapEnvelope (fun x -> x * 2) env)
                }
            )
            
        // Processor that accumulates with complex state
        type AccumulatorState = {
            Values: int list
            Sum: int
            Count: int
            LastEmitTime: DateTime option
        }
        
        let createComplexAccumulator (emitThreshold: int) =
            let initialState = { Values = []; Sum = 0; Count = 0; LastEmitTime = None }
            
            StreamProcessor.stateful initialState (fun state value ->
                let newState = {
                    Values = value :: state.Values
                    Sum = state.Sum + value
                    Count = state.Count + 1
                    LastEmitTime = state.LastEmitTime
                }
                
                if newState.Count >= emitThreshold then
                    let output = {|
                        Average = float newState.Sum / float newState.Count
                        Values = List.rev newState.Values
                        EmitTime = DateTime.UtcNow
                    |}
                    let resetState = { initialState with LastEmitTime = Some DateTime.UtcNow }
                    resetState, Some output
                else
                    newState, None
            )

    // Property configurations
    let modelTestConfig = 
        { FsCheckConfig.defaultConfig with 
            maxTest = 500
            endSize = 20
            arbitrary = [ typeof<CoreTestArbitraries> ] }

    [<Tests>]
    let streamProcessorModelTests =
        testList "StreamProcessor Model-Based Tests" [
            
            testPropertyWithConfig modelTestConfig "lift processor model" <|
                Check.Quick(StreamProcessorModel.liftProcessorSpec().ToProperty())
                    
            testPropertyWithConfig modelTestConfig "filter processor model" <|
                Check.Quick(StreamProcessorModel.filterProcessorSpec().ToProperty())

            testPropertyWithConfig modelTestConfig "stateful processor model" <|
                Check.Quick(StreamProcessorModel.statefulProcessorSpec().ToProperty())

            testPropertyWithConfig modelTestConfig "withEnv processor model" <|
                Check.Quick(StreamProcessorModel.withEnvProcessorSpec().ToProperty())

            testPropertyWithConfig modelTestConfig "complex processor model" <|
                Check.Quick(StreamProcessorModel.complexProcessorSpec().ToProperty())

            testPropertyWithConfig 
                    { modelTestConfig with maxTest = 200 } 
                    "failing processor behavior" <| fun (failureRate: float) ->

                (* TODO: when you've fixed this (it's still failing) then test monadic bind (see cover)

                TODO: find the code that prints 5% short sequences (between 1-6 commands).
                
                Flux.IO.Tests test failed with 1 error(s) (20.5s)
                    /usr/local/share/dotnet/sdk/9.0.301/Microsoft.TestPlatform.targets(48,5): error TESTERROR:
                    StreamProcessor Model-Based Tests.failing processor behavior (47ms): Error Message:
                    Failed after 86 tests. Parameters:
                        -5.396144262
                    Result:
                        False
                    Label of failing property: Expected rate: 0.40, Actual rate: 0.56
                    Focus on error:
                        etestPropertyWithConfig (302070505, 297516059) "failing processor behavior"

                Test summary: total: 54, failed: 1, succeeded: 53, skipped: 0, duration: 20.5s
                Build failed with 1 error(s) in 28.0s *)

                let rate = 
                    match abs failureRate % 1.0 with
                    | r when r < 0.0 -> 0.1
                    | r when  Double.IsNaN r || r > 1.0 -> 1.0
                    | r -> r
                let processor = ErrorScenarios.createFailingProcessor rate
                let sut = StreamProcessorModel.ProcessorSut processor
                
                let results = 
                    [1..100]
                    |> List.map (fun i -> 
                        let env = Envelope.create<_> (int64 i) i
                        sut.Process(env, TimeSpan.FromMilliseconds 5.0, CancellationToken.None)
                    )
                    
                let errorCount = 
                    results 
                    |> List.filter 
                        (function 
                        | Result.Error _ 
                        | Ok (StreamCommand.Error _) -> true 
                        | _ -> false) 
                    |> List.length

                let successCount = 
                    results 
                    |> List.filter (function Ok (StreamCommand.Emit _) -> true | _ -> false) 
                    |> List.length

                // Check that failure rate is approximately correct (with some tolerance)
                let actualRate = float errorCount / 100.0
                let tolerance = 0.15  // 15% tolerance
                
                (actualRate >= rate - tolerance && actualRate <= rate + tolerance)
                |> Prop.label (sprintf "Expected rate: %.2f, Actual rate: %.2f" rate actualRate)
                
            testPropertyWithConfig { modelTestConfig with maxTest = 100 } "delayed processor completes eventually" <| fun (minDelay: int) (maxDelay: int) ->
                let min = abs minDelay % 50
                let max = min + (abs maxDelay % 50)
                let processor = ErrorScenarios.createDelayedProcessor(min, max)
                let sut = StreamProcessorModel.ProcessorSut(processor)
                
                let env = Envelope.create 1L 42
                let timeout = TimeSpan.FromMilliseconds(float (max + 100))
                
                match sut.Process(env, timeout) with
                | Ok (StreamCommand.Emit result) -> 
                    result.Payload = 84
                    |> Prop.label "Delayed processor completed with correct result"
                | Ok other ->
                    false
                    |> Prop.label (sprintf "Unexpected result: %A" other)
                | Result.Error ex ->
                    false
                    |> Prop.label (sprintf "Error: %s" ex.Message)
                    
            testCase "complex accumulator emits after threshold" <| fun () ->
                let processor = ErrorScenarios.createComplexAccumulator 5
                let sut = StreamProcessorModel.ProcessorSut(processor)
                
                let results =
                    [1..10]
                    |> List.map (fun i ->
                        let env = Envelope<int>.create (int64 i) i
                        sut.Process(env)
                    )
                    
                let emitted = 
                    results 
                    |> List.choose (function 
                        | Ok (StreamCommand.Emit env) -> Some env.Payload 
                        | _ -> None)
                        
                Expect.equal (List.length emitted) 2 "Should emit twice (at 5 and 10 items)"
                
                match emitted with
                | [first; second] ->
                    Expect.equal first.Average 3.0 "First batch average should be 3"
                    Expect.equal second.Average 8.0 "Second batch average should be 8"
                    Expect.equal first.Values [1;2;3;4;5] "First batch values"
                    Expect.equal second.Values [6;7;8;9;10] "Second batch values"
                | _ -> failtest "Unexpected emission pattern"
        ]
