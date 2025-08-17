namespace Flux.IO.Tests

module CoreTests =

    open System
    open System.Collections.Concurrent
    open System.IO
    open System.Threading
    open System.Threading.Tasks
    open Expecto
    open Flux.IO.Collections
    open Flux.IO.Core
    open Flux.IO.Core.Flow
    open FsCheck
    open FSharpPlus
    open FSharp.HashCollections

    module private TestEnv =
        // Minimal metric collector (in-memory counters)
        type InMemMetrics() =
            let counters = ConcurrentDictionary<string,int64>()
            let gauges   = ConcurrentDictionary<string,float>()
            let hgrams   = ConcurrentDictionary<string,ResizeArray<float>>()
            
            interface IMetrics with
                member __.RecordCounter(name, _, value) =
                    counters.AddOrUpdate(name, value, fun _ old -> old + value) |> ignore

                member __.RecordGauge(name, _, value) =
                    gauges.AddOrUpdate(name, value, fun _ _ -> value) |> ignore

                member __.RecordHistogram(name, _, value) =
                    let bag = hgrams.GetOrAdd(name, fun _ -> ResizeArray())
                    lock bag (fun () -> bag.Add value)

            member __.Counters = counters
            member __.Histograms = hgrams

        type InMemTracer() =
            let spans = ResizeArray<string>()
            interface ITracer with
                member _.StartSpan name _parent =
                    spans.Add(name)
                    { TraceId = Guid.NewGuid().ToString()
                      SpanId  = Guid.NewGuid().ToString()
                      Baggage = HashMap.empty }
                
                member __.EndSpan _ = () // no-op
                member __.AddEvent _ _ _ = ()
            
            member __.SpanCount = spans.Count

        type InMemLogger() =
            let lines = ResizeArray<string>()
            let errors = ResizeArray<string * string>()
            
            interface ILogger with
                member __.Log(level,msg) =
                    lines.Add(sprintf "[%s] %s" level msg)
                member __.LogError(msg,ex) =
                    errors.Add(msg, ex.Message)
            
            member __.Lines = lines
            member __.Errors = errors

        type DummyPool() =
            interface IMemoryPool with
                member _.RentBuffer(size) = ArraySegment<byte>(Array.zeroCreate size)
                member _.ReturnBuffer(_b) = ()

        let mkEnv () =
            let metrics = InMemMetrics()
            let tracer  = InMemTracer()
            let logger  = InMemLogger()
            { Metrics = metrics :> IMetrics
              Tracer  = tracer  :> ITracer
              Logger  = logger  :> ILogger
              Memory  = DummyPool() :> IMemoryPool },
            metrics, tracer, logger

        let runFlow (f: Flow<'a>) =
            let env,_,_,_ = mkEnv()
            let vt = Flow.run env CancellationToken.None f
            if vt.IsCompletedSuccessfully then vt.Result
            else vt.AsTask().Result

        let flowEq1 (f1: Flow<'a>) (f2: Flow<'a>) =
            let env,_,_,_ = mkEnv()
            let r1 = Flow.run env CancellationToken.None f1
            let r2 = Flow.run env CancellationToken.None f2
            let v1 = if r1.IsCompletedSuccessfully then Choice1Of2 r1.Result else Choice2Of2 (r1.AsTask().Result)
            let v2 = if r2.IsCompletedSuccessfully then Choice1Of2 r2.Result else Choice2Of2 (r2.AsTask().Result)
            // Compare results (both succeed)
            match v1, v2 with
            | Choice1Of2 a, Choice1Of2 b -> a = b
            | _ -> false

        let flowEq (f1: Flow<'a>) (f2: Flow<'a>) =
            let env,_,_,_ = mkEnv()
            let ct = CancellationToken.None
            
            // Always get the actual result, regardless of sync/async
            let getResult (f: Flow<'a>) =
                let vt = Flow.run env ct f
                if vt.IsCompletedSuccessfully then 
                    vt.Result
                else 
                    vt.AsTask().Result
            
            try
                let v1 = getResult f1
                let v2 = getResult f2
                v1 = v2
            with
            | _ -> false  // If either throws, they're not equal

        let hasSameCompletionBehavior (f1: Flow<'a>) (f2: Flow<'a>) =
            let env,_,_,_ = mkEnv()
            let ct = CancellationToken.None
            
            let vt1 = Flow.run env ct f1
            let vt2 = Flow.run env ct f2
            
            // Both complete synchronously or both complete asynchronously
            vt1.IsCompletedSuccessfully = vt2.IsCompletedSuccessfully

    module Generators = 
        // HashMap generator from key/value list
        let genHashMap (genKey: Gen<string>) (genValue: Gen<string>) : Gen<HashMap<string,string>> =
            gen {
                let! kvs = Gen.listOf (Gen.zip genKey genValue)
                return kvs |> List.fold (fun acc (k,v) -> HashMap.add k v acc) HashMap.empty
            }

        let genShortString = 
            Gen.choose(1, 20)
            |> Gen.map (fun len -> String.replicate len "a")
            
        let genMediumString = 
            Gen.choose(30, 100)
            |> Gen.map (fun len -> String.replicate len "b")
            
        let genLongString = 
            Gen.choose(200, 1000)
            |> Gen.map (fun len -> String.replicate len "c")
        
        let genSpecialCharsString =
            Gen.elements [
                "with space"
                "with-hyphen"
                "with_underscore"
                "with.dot"
                "with{braces}"
                "with/slash"
                "with\\"
                "with\"quotes\""
                "with'apostrophes'"
                "with\nnewline"
                "with\ttab"
                "with\rreturn"
            ]
        
        let genUnicodeString =
            Gen.elements [
                "Unicode: ñáéíóú"
                "Emoji: 🔍🔎🔥💧"
                "Math: ∑∏√∂∆"
                "CJK: 你好世界"
                "Arabic: مرحبا"
                "Greek: Γειά σου"
                "Russian: Привет"
            ]
        
        // Combined string generator
        let genTestString =
            Gen.oneof [
                genShortString
                genMediumString
                genLongString
                genSpecialCharsString
                genUnicodeString
                // Some familiar strings seen in json, XML, RML, etc
                Gen.constant "http://example.org/resource"
                Gen.constant "$.people[*]"
                Gen.constant "$.transactions[*].lineItems[*]"
                Gen.constant "http://xmlns.com/foaf/0.1/name"
                Gen.constant "{id}"
                Gen.constant "name"
            ]
        
        let getKVMap = 
            let genString n= 
                Gen.choose(3, n)
                |> Gen.map (fun len -> String.replicate len "b")
            genHashMap (genString 8) genTestString

        let genTraceContext =
            gen {
                let tid = Guid.NewGuid() 
                let sid = Guid.NewGuid()
                let! bag = 
                    genHashMap 
                        (Gen.elements ["a";"b";"c";"trace";"x"]) 
                        (Gen.elements ["1";"2";"val";"xyz"])
                return { 
                    TraceId = tid.ToString() 
                    SpanId = sid.ToString()
                    Baggage = bag 
                }
            }

        let genCost =
            gen {
                let! sz  = Gen.choose(0, 1_000_000)
                let! cpu = Gen.choose(0, 100) |> Gen.map float
                return { Bytes = sz; CpuHint = cpu }
            }

        let genEnvelope (genPayload: Gen<'a>) =
            gen {
                let! p  = genPayload
                let! s  = Gen.choose(0, Int32.MaxValue) |> Gen.map int64
                let! tc = genTraceContext
                let! hdrs = 
                    genHashMap 
                        (Gen.elements ["k1"; "k2"; "foo"; "bar"; "hdr"]) 
                        (Gen.elements ["v1"; "v2"; "zzz"; "alpha"])
                let! attrs =
                    gen {
                        let! n = Gen.choose(1, 5)
                        let! keys = 
                            Gen.elements ["a"; "b"; "c"; "payload"; "seq"] |> Gen.listOfLength n
                        let! vals = 
                            Gen.elements [ box 1; box "v"; box 42; box true; box 0.5 ] |> Gen.listOfLength n
                        let pairs = List.zip keys vals
                        return pairs |> List.fold (fun acc (k,v) -> HashMap.add k v acc) HashMap.empty
                    }
                let! cost = genCost
                let ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                return {
                    Payload = p
                    Headers = hdrs
                    SeqId = s
                    SpanCtx = tc
                    Ts = ts
                    Attrs = attrs
                    Cost = cost
                }
            }

        let genIntEnvelope = genEnvelope Arb.generate<int>
        
        let genStringEnvelope = genEnvelope Arb.generate<string>

        // Simple function generator for int -> int using linear forms
        let genIntToIntF =
            gen {
                let! a = Gen.choose(-5,5)
                let! b = Gen.choose(-100,100)
                return fun (x:int) -> a * x + b
            }

        // Functions for Flow law tests (A->B, B->C)
        (* let genFunc2<'A,'B> =
            Arb.generate<'A -> 'B>
        *)
        
        // produces a flow that might be already-completed or asynchronous
        let genFlowInt =
            gen {
                let! v = Arb.generate<int>
                let! delayMs = Gen.frequency [ 3, Gen.constant 0; 1, Gen.choose(1,5) ]
                return
                    if delayMs = 0 then
                        Flow.ret v
                    else
                        Flow (fun _ ct ->
                            ValueTask<int>(
                            task {
                                do! Task.Delay(delayMs, ct)
                                return v
                            }))
            }
        
        let genFlowAndFunc = 
            gen {
                let! flow = genFlowInt
                let! f1 = genIntToIntF
                let! f2 = genIntToIntF
                return (flow, f1, f2)
            }

        let genFunOverInt = 
            gen {
                let! int = Arb.generate<int>
                let! func = genIntToIntF
                return (int, func)
            }

        // Produces the correct arbitrary (generated) test data on demand
        type CoreTestArbitraries =
            static member TestInt() = Arb.generate<int>
            static member TestMap() = Arb.fromGen getKVMap

            static member FlowInt() = Arb.fromGen genFlowInt

            static member FlowString() = Arb.fromGen genIntToIntF

            static member Cost(): Arbitrary<Cost> =
                { new Arbitrary<Cost>() with
                    member _.Generator = genCost
                    member _.Shrinker _ = Seq.empty
                }

            static member StringEnvelope() = Arb.fromGen genStringEnvelope

            static member IntEnvelope() = Arb.fromGen genIntEnvelope

            static member TraceContext() = Arb.fromGen genTraceContext

            static member FlowAndFunctions() = 
                { new Arbitrary<Flow<int> * (int->int) * (int->int)>() with
                    member __.Generator = genFlowAndFunc
                    member __.Shrinker _ = Seq.empty
                }

            static member FunOverInt() =
                { new Arbitrary<int * (int->int)>() with
                    member __.Generator = genFunOverInt
                    member __.Shrinker _ = Seq.empty
                }


    open TestEnv
    open Generators

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
                        let lhs = Flow.map (g >> h) m
                        let rhs = Flow.map h (Flow.map g m)
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
                // object the order of value elements when calling `toSeq`
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
                    | false, StreamCommand.RequireMore -> true
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
                    
                let inputs = [1;2;3;4;5;6;7]
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

        // System under test: using stateful StreamProcessor identical to code in testCase earlier
        let mkProcessor () =
            StreamProcessor.stateful (0,0) (fun (sum,count) x ->
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
                | StreamCommand.Emit e -> sutOutputs <- e.Payload :: sutOutputs
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
            }
            |> Arb.fromGen
            |> (flip Prop.forAll)
                (fun (cmds, specInstance: Sut) -> specInstance.Run cmds)

    [<Tests>]
    let modelBasedTests =
        testList "Model-Based (Stateful Sum Machine)" [
            testPropertyWithConfig fastConfig "stateful sum model invariants hold" StatefulSumModel.propMachine
        ]

    [<Tests>]
    let coreTests = testList "Core Tests" [
        unitTests
        envelopeTests
        flowTests
        streamProcessorTests
        modelBasedTests
    ]
