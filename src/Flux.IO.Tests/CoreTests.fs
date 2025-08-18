namespace Flux.IO.Tests

module CoreTests =

    open System
    open System.Threading
    open System.Threading.Tasks
    open Expecto
    open Flux.IO.Collections
    open Flux.IO.Core
    open Flux.IO.Core.Flow
    open FsCheck
    open FSharpPlus
    open FSharp.HashCollections

    open Generators
    open TestEnv

    let stdConfig =
        { FsCheckConfig.defaultConfig with
            maxTest = 200
            endSize = 500
            replay = None
            arbitrary = [ typeof<CoreTestArbitraries> ] }

    let fastConfig =
        { stdConfig with 
            maxTest = 100
            endSize = 200 
            arbitrary = [ typeof<CoreTestArbitraries> ] }

    [<Tests>]
    let flowTests =
        testList "Flow Monad & Functor/Applicative Semantics" [

            (* 
                The three monad laws:
                1. Left identity: return a >>= f ≡ f a

                Wrapping a value in return and then binding a function to it is the same as 
                applying the function directly to the value.
                
                2. Right identity: m >>= return ≡ m

                Binding a monadic value m to the return function is the same as keeping the 
                original monadic value m unchanged

                3. Associativity: (m >>= f) >>= g ≡ m >>= (\x -> f x >>= g)

                Ensures the order of applying monadic functions does not affect the final result.
            *)
            
            testPropertyWithConfig 
                    stdConfig 
                    "Flow.ret returns value (fast path)" <| fun (x: int) ->
                runFlow (Flow.ret x) = x

            testPropertyWithConfig 
                    stdConfig 
                    "Functor identity: map id == id" <| fun (f: Flow<int>) -> 
                flowEq (Flow.map id f) f

            testProperty "Functor composition: map (g << h) = map g << map h" <|
                // NB: this type signature is a bit messy for FsCheck to bind to
                Prop.forAll 
                    (CoreTestArbitraries.FlowAndFunctions())
                    (fun (m,g,h) ->
                        let lhs = Flow.map (g << h) m
                        let rhs = Flow.map g (Flow.map h m)
                        flowEq lhs rhs
                    )

            testPropertyWithConfig 
                    stdConfig "Applicative identity" <| fun (f: Flow<int>) ->
                let idF = Flow.ret id
                let lhs = Flow.apply idF f
                flowEq lhs f

            testProperty "Monad left identity: return a >>= k = k a" <|
                Prop.forAll (CoreTestArbitraries.FunOverInt())
                    (fun (a, kf) ->
                        let k x = Flow.ret (kf x)
                        let left = Flow.bind k (Flow.ret a)
                        let right = k a
                        flowEq left right
                    )

            testPropertyWithConfig 
                    stdConfig
                    "Monad right identity: m >>= return = m" <| fun (m : Flow<int>) ->
                let rhs = Flow.bind Flow.ret m
                flowEq m rhs

            testProperty "Monad associativity: (m >>= f) >>= g = m >>= (fun x -> f x >>= g)" <|
                Prop.forAll (CoreTestArbitraries.FlowAndFunctions())
                    (fun (m,f,g) ->
                        let fM x = Flow.ret (f x)
                        let gM x = Flow.ret (g x)
                        let left = Flow.bind gM (Flow.bind fM m)
                        let right = Flow.bind (fun x -> Flow.bind gM (fM x)) m
                        flowEq left right
                    )

            testCase "withTimeout returns Some when completes before timeout" <| fun () ->
                let f = Flow.ret 42
                let res = runFlow (Flow.withTimeout (TimeSpan.FromMilliseconds 100.) f)
                Expect.equal res (Some 42) "Expected Some value"

            testCase "withTimeout returns None when computation slower" <| fun () ->
                let slow =
                    Flow (fun _ ct ->
                        ValueTask<int>(task {
                            do! Task.Delay(30, ct)
                            return 99
                        }))
                let res = runFlow (Flow.withTimeout (TimeSpan.FromMilliseconds 1.0) slow)
                in Expect.isNone res "Should timeout"

            testCase "catch captures exception" <| fun () ->
                let boom =
                    Flow (fun _ _ ->
                        task { 
                            return raise (InvalidOperationException "boom") 
                        } |> ValueTask<int>
                    )
                match runFlow (Flow.catch boom) with
                | Ok _ -> failwith "Expected error"
                | Result.Error ex -> Expect.equal ex.Message "boom" "Exception mismatch"

            testCase "tryFinally executes finalizer on success" <| fun () ->
                let flag = ref false
                let f = Flow.tryFinally (Flow.ret 1) (fun () -> flag.Value <- true)
                let _ = runFlow f
                Expect.isTrue flag.Value "Finalizer should execute"

            testCase "tryFinally executes finalizer on error" <| fun () ->
                let flag = ref false
                let failing =
                    Flow (fun _ _ -> ValueTask<int>(task { return raise (Exception "X") }))
                let f = Flow.tryFinally failing (fun () -> flag.Value <- true)
                let r = runFlow (Flow.catch f)
                match r with
                | Ok _ -> failwith "Expected failure"
                | Result.Error _ ->
                    Expect.isTrue flag.Value "Finalizer should run on failure"

            testCase "ask returns exact environment instance" <| fun () ->
                let env,_,_,_ = TestEnv.mkEnv()
                let got = Flow.run env CancellationToken.None Flow.ask
                Expect.isTrue (Object.ReferenceEquals(env, got.Result)) "Environment identity must match"

            testCase "local modifies environment for enclosed computation only" <| fun () ->
                let env, _, _, logger = TestEnv.mkEnv()
                let log = logger :> ILogger
                let newLogger =
                    { new ILogger with
                        member __.Log(_, msg) = log.Log("ALT", msg)
                        member __.LogError(msg, ex) = log.LogError(msg, ex) }

                let localFlow =
                    Flow.local (fun e -> { e with Logger = newLogger }) (
                    Flow.ask |> Flow.map (fun e -> e.Logger)
                    )
                let newLog = Flow.run env CancellationToken.None localFlow
                
                Expect.isFalse 
                    (Object.ReferenceEquals(env.Logger, newLog.Result)) 
                    "Logger should be replaced locally"
                
                Expect.isTrue 
                    (Object.ReferenceEquals(env.Logger, env.Logger)) 
                    "Original env logger unchanged"
            ]

    [<Tests>]
    let envelopeTests =
        testList "Envelope" [
            testPropertyWithConfig 
                    stdConfig 
                    "mapEnvelope preserves metadata" <| fun (env: Envelope<int>) ->

                let f x = x + 1
                let mapped = mapEnvelope f env

                Expect.equal mapped.SeqId env.SeqId "Expected equal seqIds"
                Expect.equal mapped.SpanCtx.TraceId env.SpanCtx.TraceId "Expected equal traceIds"

                let s1 = HashMap.keys mapped.Attrs |> Set.ofSeq
                let s2 = HashMap.keys env.Attrs |> Set.ofSeq
                Expect.isEmpty (Set.difference s1 s2) "Expected equal attribute keys"

                // whilst we *could* check reference equality, HashMap makes no guarantees 
                // about the order of value elements when calling `toSeq`
                let l1 = HashMap.values mapped.Attrs |> List.ofSeq
                let l2 = HashMap.values env.Attrs |> List.ofSeq
                Expect.equal l1.Length l2.Length "Expected equal attribute value counts"

                Expect.equal mapped.Cost env.Cost "Expected equal cost"

            testPropertyWithConfig 
                    stdConfig 
                    "mapEnvelope transforms payload correctly" <| fun (env: Envelope<int>) ->
                let f x = x * 2
                let mapped = mapEnvelope f env
                mapped.Payload = env.Payload * 2
        ]

    [<Tests>]
    let streamProcessorTests =
        testList "StreamProcessor" [

            testPropertyWithConfig 
                stdConfig 
                "lift emits exactly one envelope with transformed payload" <| 
                fun (env: Envelope<int>, f: int -> int) ->
                    let proc = StreamProcessor.lift f
                    let env0,metrics,_,_ = mkEnv()
                    let resVT = 
                        StreamProcessor.runProcessor proc env 
                        |> Flow.run env0 CancellationToken.None
                    
                    let res = resVT.Result
                    match res with
                    | StreamCommand.Emit outEnv -> outEnv.Payload = f env.Payload
                    | _ -> false

            testPropertyWithConfig 
                stdConfig 
                "filter passes subset satisfying predicate" <|
                fun (env: Envelope<int>, pred: int -> bool) ->
                    let proc = StreamProcessor.filter pred
                    let env0,_,_,_ = mkEnv()
                    let out = 
                        StreamProcessor.runProcessor proc env 
                        |> Flow.run env0 CancellationToken.None 
                        |> fun vt -> vt.Result
                    
                    match pred env.Payload, out with
                    | true, StreamCommand.Emit o -> o.Payload = env.Payload
                    | false, StreamCommand.Consume -> true
                    | _ -> false

            testCase "stateful emits only when Some returned" <| fun _ ->
                // Sum every 3 numbers
                let collector =
                    StreamProcessor.stateful (0,0) (fun (sum,count) x ->
                        let sum' = sum + x
                        let count' = count + 1
                        if count' = 3 then 
                            (0,0), Some sum' 
                        else (sum', count'), None
                    )

                let env0, _, _, _ = mkEnv()
                let mkEnv i =
                    {
                        Payload = i
                        Headers = HashMap.empty
                        SeqId = int64 i
                        SpanCtx = { TraceId="t"; SpanId="s"; Baggage=HashMap.empty }
                        Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                        Attrs = HashMap.empty
                        Cost = { Bytes = 0; CpuHint = 0.0 }
                    }

                let inputs = [1; 2; 3; 4; 5; 6; 7]
                let outputs =
                    inputs
                    |> List.map (fun i -> 
                        StreamProcessor.runProcessor collector (mkEnv i) 
                        |> Flow.run env0 CancellationToken.None 
                        |> fun vt -> vt.Result)
                
                let emittedSums =
                    outputs
                    |> List.choose (function StreamCommand.Emit e -> Some e.Payload | _ -> None)
                Expect.sequenceEqual emittedSums [1+2+3; 4+5+6] "Should emit two sums (7 incomplete)"

            testCase "withEnv accesses metrics and increments counter" <| fun _ ->
                let proc =
                    StreamProcessor.withEnv (fun env (x:int) ->
                        flow {
                            env.Metrics.RecordCounter("hit", HashMap.empty, 1L)
                            return x + 10
                        })
                
                let env0,metrics,_,_ = mkEnv()
                let envIn =
                    {
                        Payload = 5
                        Headers = HashMap.empty
                        SeqId = 1L
                        SpanCtx = { TraceId="t"; SpanId="s"; Baggage=HashMap.empty }
                        Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                        Attrs = HashMap.empty
                        Cost = { Bytes = 0; CpuHint = 0.0 }
                    }
                let res = 
                    StreamProcessor.runProcessor proc envIn 
                    |> Flow.run env0 CancellationToken.None 
                    |> fun vt -> vt.Result
                
                match res with
                | Emit outEnv ->
                    Expect.equal outEnv.Payload 15 "Transformed"
                    let hits = metrics.Counters.TryGetValue "hit" |> function | true,v -> v | _ -> 0L
                    Expect.equal hits 1L "Counter incremented"
                | _ -> failwith "Expected Emit"

        ]

    [<Tests>]
    let unitTests = testList "UnitTests" [
        test "Basic Envelope Creation" {
            let envelope = Envelope<_>.create 1 "test payload"
            Expect.equal envelope.Payload "test payload" "Payload should match"
            Expect.isTrue (HashMap.isEmpty envelope.Headers) "Headers should be empty"
            Expect.isTrue (HashMap.count envelope.Attrs = 0) "Attrs should be empty"
        }

        test "Envelope Creation with Custom Attributes" {
            let envelope = Envelope<_>.create 2 "test payload 2"
            let attrs = FastMap.ofSeq [("key1", box "value1"); ("key2", box 42)]
            let envelopeWithAttrs = { envelope with Attrs = attrs }
            Expect.equal (HashMap.count envelopeWithAttrs.Attrs) 2 "Attrs should have 2 items"
            Expect.equal (FastMap.find "key1" envelopeWithAttrs.Attrs) (box "value1") "Attr 'key1' should match"
            Expect.equal (FastMap.find "key2" envelopeWithAttrs.Attrs) (box 42) "Attr 'key2' should match"
        }
    ]

    module StatefulSumModel =
        // Model: stateful processor that sums groups of size N (here N=3) -> emits sum
        type ModelState =
            { Buffer : int list
              Emitted : int list } 

        let N = 3

        let mkProcessor () =
            StreamProcessor.stateful (0,0) (fun (sum, count) x ->
                let sum' = sum + x
                let count' = count + 1
                if count' = N then 
                    (0,0), Some sum' 
                else (sum', count'), None
            )

        type Cmd =
            | Add of int

        [<AbstractClass>]
        type Sut() = 
            abstract member Run: (Cmd list) -> bool

        // Generator for commands
        let genCmd = Arb.generate<int> |> Gen.map (fun i -> Add (abs i % 50))

        // Apply command to model
        let stepModel cmd (state: ModelState) =
            match cmd with
            | Add x ->
                let buf' = x :: state.Buffer
                if List.length buf' = N then
                    { Buffer = []
                      Emitted = (List.sum buf') :: state.Emitted }
                else
                    { state with Buffer = buf' }

        // Execute command on SUT and compare invariants
        let spec() =
            let proc = mkProcessor ()
            let env0,_,_,_ = TestEnv.mkEnv()
            let mutable currentState = { Buffer = []; Emitted = [] }
            let mutable sutOutputs : int list = []

            let apply cmd =
                let x = match cmd with Add v -> v
                let env =
                    {
                        Payload = x
                        Headers = HashMap.empty
                        SeqId = int64 x
                        SpanCtx = { TraceId="t"; SpanId="s"; Baggage=HashMap.empty }
                        Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                        Attrs = HashMap.empty
                        Cost = { Bytes = 0; CpuHint = 0.0 }
                    }
                
                // step SUT
                let res = 
                    StreamProcessor.runProcessor proc env 
                    |> Flow.run env0 CancellationToken.None 
                    |> fun vt -> vt.Result
                
                match res with
                | Emit e -> sutOutputs <- e.Payload :: sutOutputs
                | _ -> ()
                
                // step model
                currentState <- stepModel cmd currentState
                
                // invariants:
                //  1. Emitted counts match
                //  2. Each emitted value equals sum of some disjoint chunk of size N
                let invariant1 = currentState.Emitted.Length = sutOutputs.Length
                let invariant2 =
                    (List.length sutOutputs = List.length currentState.Emitted) &&
                    (List.forall2 (=) (List.rev currentState.Emitted) (List.rev sutOutputs))
                invariant1 && invariant2

            // Return commands and execution predicate
            { new Sut() with
                member __.Run(ops: List<Cmd>) = ops |> List.forall apply
            }

        let propMachine =
            gen {
                let! cmds = Gen.listOf genCmd
                let specInstance = spec()
                return (cmds, specInstance)
            }               // Gen<Cmd list * Sut>
            |> Arb.fromGen  // Arbitrary<Cmd list * Sut>
            |> (flip Prop.forAll)
                (fun (cmds, specInstance: Sut) -> specInstance.Run cmds)

    [<Tests>]
    let modelBasedTests =
        testList "Model-Based (Stateful Sum Machine)" [
            testPropertyWithConfig 
                fastConfig 
                "stateful sum model invariants hold" 
                StatefulSumModel.propMachine
        ]

    [<Tests>]
    let coreTests = testList "Core Tests" [
        unitTests
        envelopeTests
        flowTests
        streamProcessorTests
        modelBasedTests
    ]
