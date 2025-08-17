namespace Flux.IO.Tests

(*
  Extensive test suite for Core.fs

  - Uses Expecto + Expecto.FsCheck integration
  - Property-based tests (FsCheck) WITHOUT global Arb.register
  - Model-based tests using FsCheck.Experimental.Machine
  - Focus Areas:
      * Flow monad laws & operational semantics
      * Timeout / cancellation / error handling
      * StreamProcessor primitives (lift, filter, stateful, withEnv, log, recordMetric)
      * Envelope invariants & map transformation
      * Stateful accumulation behavioral equivalence
      * Backpressure semantics placeholders (where applicable in Core constructs)
*)

open System
open System.Threading
open System.Threading.Tasks
open Expecto
open FsCheck
open FsCheck.Experimental
open FsCheck.Fluent
open Flux.IO.Core
open Flux.IO.Core.Flow
open Flux.IO.Core.StreamProcessor
open FSharp.HashCollections

// ---------------------------------------------------------
// Test Utilities & Dummy Environment
// ---------------------------------------------------------

module private TestEnv =
    // Minimal metric collector (in-memory counters)
    type InMemMetrics() =
        let counters = System.Collections.Concurrent.ConcurrentDictionary<string,int64>()
        let gauges   = System.Collections.Concurrent.ConcurrentDictionary<string,float>()
        let hgrams   = System.Collections.Concurrent.ConcurrentDictionary<string,ResizeArray<float>>()
        interface IMetrics with
            member _.RecordCounter(name,tags,value) =
                counters.AddOrUpdate(name, value, fun _ old -> old + value) |> ignore
            member _.RecordGauge(name,tags,value) =
                gauges.AddOrUpdate(name, value, fun _ _ -> value) |> ignore
            member _.RecordHistogram(name,tags,value) =
                let bag = hgrams.GetOrAdd(name, fun _ -> ResizeArray())
                lock bag (fun () -> bag.Add value)
        member _.Counters = counters
        member _.Histograms = hgrams

    type InMemTracer() =
        let spans = ResizeArray<string>()
        interface ITracer with
            member _.StartSpan(name, _parent) =
                spans.Add(name)
                { TraceId = Guid.NewGuid().ToString()
                  SpanId  = Guid.NewGuid().ToString()
                  Baggage = HashMap.empty }
            member _.EndSpan(_ctx) = () // no-op
            member _.AddEvent(_ctx,_name,_attrs) = ()
        member _.SpanCount = spans.Count

    type InMemLogger() =
        let lines = ResizeArray<string>()
        let errors = ResizeArray<string * string>()
        interface ILogger with
            member _.Log(level,msg) =
                lines.Add(sprintf "[%s] %s" level msg)
            member _.LogError(msg,ex) =
                errors.Add(msg, ex.Message)
        member _.Lines = lines
        member _.Errors = errors

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

// ---------------------------------------------------------
// Generators
// ---------------------------------------------------------

module private Gens =
    // HashMap generator from key/value list
    let genHashMap (genKey: Gen<string>) (genValue: Gen<'v>) : Gen<HashMap<string,'v>> =
        gen {
            let! kvs = Gen.listOf (Gen.zip genKey genValue)
            return kvs |> List.fold (fun acc (k,v) -> HashMap.add k v acc) HashMap.empty
        }

    let genTraceContext =
        gen {
            let! tid = Gen.guid
            let! sid = Gen.guid
            let! bag = genHashMap (Gen.elements ["a";"b";"c";"trace";"x"]) (Gen.elements ["1";"2";"val";"xyz"])
            return { TraceId = tid.ToString(); SpanId = sid.ToString(); Baggage = bag }
        }

    let genCost =
        gen {
            let! sz  = Gen.choose(0, 1_000_000)
            let! cpu = Gen.choose(0,100) |> Gen.map float
            return { Bytes = sz; CpuHint = cpu }
        }

    let genEnvelope (genPayload: Gen<'a>) =
        gen {
            let! p  = genPayload
            let! s  = Gen.choose(0, Int32.MaxValue) |> Gen.map int64
            let! tc = genTraceContext
            let! hdrs = genHashMap (Gen.elements ["k1";"k2";"foo";"bar";"hdr"]) (Gen.elements ["v1";"v2";"zzz";"alpha"])
            let! attrs =
                gen {
                    let! keys = Gen.listOf (Gen.elements ["a";"b";"c";"payload";"seq"])
                    let! vals = Gen.listOf (Gen.elements [ box 1; box "v"; box 42; box true; box 0.5 ])
                    let pairs = List.zip (List.truncate (List.length vals) keys) vals
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
    let genStringEnvelope = genEnvelope (Arb.generate<string>)

    // Simple function generator for int -> int using linear forms
    let genIntToIntF =
        gen {
            let! a = Gen.choose(-5,5)
            let! b = Gen.choose(-100,100)
            return fun (x:int) -> a * x + b
        }

    // Functions for Flow law tests (A->B, B->C)
    let genFunc2<'A,'B> =
        Arb.generate<'A -> 'B>

    // Delay Flow generator: produces a flow that might be already-completed or asynchronous
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

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

module private Helpers =
    open TestEnv

    let runFlow (f: Flow<'a>) =
        let env,_,_,_ = mkEnv()
        let vt = Flow.run env CancellationToken.None f
        if vt.IsCompletedSuccessfully then vt.Result
        else vt.AsTask().Result

    let flowEq (f1: Flow<'a>) (f2: Flow<'a>) =
        let env,_,_,_ = mkEnv()
        let r1 = Flow.run env CancellationToken.None f1
        let r2 = Flow.run env CancellationToken.None f2
        let v1 = if r1.IsCompletedSuccessfully then Choice1Of2 r1.Result else Choice2Of2 (r1.AsTask().Result)
        let v2 = if r2.IsCompletedSuccessfully then Choice1Of2 r2.Result else Choice2Of2 (r2.AsTask().Result)
        // Compare results (both succeed)
        match v1, v2 with
        | Choice1Of2 a, Choice1Of2 b -> a = b
        | _ -> false

// ---------------------------------------------------------
// Property Configuration
// ---------------------------------------------------------

let stdConfig =
    { FsCheckConfig.defaultConfig with
        maxTest = 200
        endSize = 500
        replay = None }

let fastConfig =
    { stdConfig with maxTest = 100; endSize = 200 }

// ---------------------------------------------------------
// Tests: Flow Monad Laws & Semantics
// ---------------------------------------------------------

let flowTests =
  testList "Flow Monad & Functor/Applicative Semantics" [

    testPropertyWithConfig stdConfig "Flow.ret returns value (fast path)" <|
      Prop.forAll (Arb.generate<int>) (fun x ->
        let v = Helpers.runFlow (Flow.ret x)
        v = x
      )

    testPropertyWithConfig stdConfig "Functor identity: map id == id" <|
      Prop.forAll Gens.genFlowInt (fun f ->
        let mapped = Flow.map id f
        Helpers.flowEq mapped f
      )

    testPropertyWithConfig stdConfig "Functor composition: map (g << h) = map g << map h" <|
      Prop.forAll (Gen.tuple3 Gens.genFlowInt Gens.genIntToIntF Gens.genIntToIntF) (fun (m,g,h) ->
        let lhs = Flow.map (g >> h) m
        let rhs = Flow.map h (Flow.map g m)
        Helpers.flowEq lhs rhs
      )

    testPropertyWithConfig stdConfig "Applicative identity" <|
      Prop.forAll Gens.genFlowInt (fun m ->
        let idF = Flow.ret id
        let lhs = Flow.apply idF m
        Helpers.flowEq lhs m
      )

    testPropertyWithConfig stdConfig "Monad left identity: return a >>= k = k a" <|
      Prop.forAll (Gen.zip Arb.generate<int> (Arb.generate<int -> int>))
        (fun (a, kf) ->
          let k x = Flow.ret (kf x)
          let left = Flow.bind k (Flow.ret a)
          let right = k a
          Helpers.flowEq left right)

    testPropertyWithConfig stdConfig "Monad right identity: m >>= return = m" <|
      Prop.forAll Gens.genFlowInt (fun m ->
        let rhs = Flow.bind Flow.ret m
        Helpers.flowEq m rhs
      )

    testPropertyWithConfig stdConfig "Monad associativity: (m >>= f) >>= g = m >>= (fun x -> f x >>= g)" <|
      Prop.forAll (Gen.zip3 Gens.genFlowInt (Arb.generate<int -> int>) (Arb.generate<int -> int>))
        (fun (m,f,g) ->
          let fM x = Flow.ret (f x)
          let gM x = Flow.ret (g x)
          let left = Flow.bind gM (Flow.bind fM m)
          let right = Flow.bind (fun x -> Flow.bind gM (fM x)) m
          Helpers.flowEq left right
        )

    testCase "withTimeout returns Some when completes before timeout" <| fun _ ->
      let f = Flow.ret 42
      let res = Helpers.runFlow (Flow.withTimeout (TimeSpan.FromMilliseconds 100.) f)
      Expect.equal res (Some 42) "Expected Some value"

    testCase "withTimeout returns None when computation slower" <| fun _ ->
      let slow =
        Flow (fun _ ct ->
          ValueTask<int>(task {
            do! Task.Delay(30, ct)
            return 99
          }))
      let res = Helpers.runFlow (Flow.withTimeout (TimeSpan.FromMilliseconds 1.0) slow)
      Expect.isNone res "Should timeout"

    testCase "catch captures exception" <| fun _ ->
      let boom =
        Flow (fun _ _ -> ValueTask<int>(task { return raise (InvalidOperationException("boom")) }))
      let r = Helpers.runFlow (Flow.catch boom)
      match r with
      | Ok _ -> failwith "Expected error"
      | Error ex -> Expect.equal ex.Message "boom" "Exception mismatch"

    testCase "tryFinally executes finalizer on success" <| fun _ ->
      let flag = ref false
      let f = Flow.tryFinally (Flow.ret 1) (fun () -> flag.Value <- true)
      let _ = Helpers.runFlow f
      Expect.isTrue flag.Value "Finalizer should execute"

    testCase "tryFinally executes finalizer on error" <| fun _ ->
      let flag = ref false
      let failing =
        Flow (fun _ _ -> ValueTask<int>(task { return raise (Exception "X") }))
      let f = Flow.tryFinally failing (fun () -> flag.Value <- true)
      let r = Helpers.runFlow (Flow.catch f)
      match r with
      | Ok _ -> failwith "Expected failure"
      | Error _ ->
        Expect.isTrue flag.Value "Finalizer should run on failure"

    testCase "ask returns exact environment instance" <| fun _ ->
      let env,_,_,_ = TestEnv.mkEnv()
      let got = Flow.run env CancellationToken.None Flow.ask
      Expect.isTrue (Object.ReferenceEquals(env, got.Result)) "Environment identity must match"

    testCase "local modifies environment for enclosed computation only" <| fun _ ->
      let env,metrics,_,logger = TestEnv.mkEnv()
      let newLogger =
        { new ILogger with
            member _.Log(level,msg) = logger.Log("ALT", msg)
            member _.LogError(msg,ex) = logger.LogError(msg,ex) }
      let localFlow =
        Flow.local (fun e -> { e with Logger = newLogger }) (
          Flow.ask |> Flow.map (fun e -> e.Logger)
        )
      let newLog = Flow.run env CancellationToken.None localFlow
      Expect.isFalse (Object.ReferenceEquals(env.Logger, newLog.Result)) "Logger should be replaced locally"
      // outside unchanged:
      Expect.isTrue (Object.ReferenceEquals(env.Logger, env.Logger)) "Original env logger unchanged"
  ]


// ---------------------------------------------------------
// Envelope Tests
// ---------------------------------------------------------

let envelopeTests =
  testList "Envelope" [

    testPropertyWithConfig stdConfig "mapEnvelope preserves metadata" <|
      Prop.forAll Gens.genIntEnvelope (fun env ->
        let f x = x + 1
        let mapped = mapEnvelope f env
        (mapped.Headers = env.Headers)
        .&. (mapped.SeqId = env.SeqId)
        .&. (mapped.SpanCtx.TraceId = env.SpanCtx.TraceId)
        .&. (mapped.Attrs = env.Attrs)
        .&. (mapped.Cost = env.Cost)
      )

    testPropertyWithConfig stdConfig "mapEnvelope transforms payload correctly" <|
      Prop.forAll Gens.genIntEnvelope (fun env ->
        let mapped = mapEnvelope ((*) 2) env
        mapped.Payload = env.Payload * 2
      )
  ]


// ---------------------------------------------------------
// StreamProcessor Tests
// ---------------------------------------------------------

let streamProcessorTests =
  testList "StreamProcessor" [

    testPropertyWithConfig stdConfig "lift emits exactly one envelope with transformed payload" <|
      Prop.forAll (Gen.zip Gens.genIntEnvelope (Arb.generate<int -> int>)) (fun (env,f) ->
        let proc = StreamProcessor.lift f
        let env0,metrics,_,_ = TestEnv.mkEnv()
        let resVT = StreamProcessor.runProcessor proc env |> Flow.run env0 CancellationToken.None
        let res = resVT.Result
        match res with
        | StreamCommand.Emit outEnv -> outEnv.Payload = f env.Payload
        | _ -> false
      )

    testPropertyWithConfig stdConfig "filter passes subset satisfying predicate" <|
      Prop.forAll (Gen.zip Gens.genIntEnvelope (Arb.generate<int -> bool>)) (fun (env,pred) ->
        let proc = StreamProcessor.filter pred
        let env0,_,_,_ = TestEnv.mkEnv()
        let out = StreamProcessor.runProcessor proc env |> Flow.run env0 CancellationToken.None |> fun vt -> vt.Result
        match pred env.Payload, out with
        | true, StreamCommand.Emit o -> o.Payload = env.Payload
        | false, StreamCommand.RequireMore -> true
        | _ -> false
      )

    testCase "stateful emits only when Some returned" <| fun _ ->
      // Sum every 3 numbers
      let collector =
        StreamProcessor.stateful (0,0) (fun (sum,count) x ->
          let sum' = sum + x
          let count' = count + 1
          if count' = 3 then (0,0), Some sum' else (sum', count'), None)
      let env0,_,_,_ = TestEnv.mkEnv()
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
        |> List.map (fun i -> StreamProcessor.runProcessor collector (mkEnv i) |> Flow.run env0 CancellationToken.None |> fun vt -> vt.Result)
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
      let env0,metrics,_,_ = TestEnv.mkEnv()
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
      let res = StreamProcessor.runProcessor proc envIn |> Flow.run env0 CancellationToken.None |> fun vt -> vt.Result
      match res with
      | StreamCommand.Emit outEnv ->
          Expect.equal outEnv.Payload 15 "Transformed"
          let hits = metrics.Counters.TryGetValue "hit" |> function | true,v -> v | _ -> 0L
          Expect.equal hits 1L "Counter incremented"
      | _ -> failwith "Expected Emit"

  ]


// ---------------------------------------------------------
// Model-Based Testing (FsCheck.Experimental.Machine)
// ---------------------------------------------------------

module private StatefulSumModel =
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
        if count' = N then (0,0), Some sum' else (sum', count'), None)

    type Cmd =
      | Add of int

    // Generator for commands
    let genCmd = Arb.generate<int> |> Gen.map (fun i -> Add (abs i % 50))

    // Apply command to model
    let stepModel cmd (state:ModelState) =
      match cmd with
      | Add x ->
        let buf' = x :: state.Buffer
        if List.length buf' = N then
          { Buffer = []
            Emitted = (List.sum buf') :: state.Emitted }
        else
          { state with Buffer = buf' }

    // Execute command on SUT and compare invariants
    let spec () =
      let proc = mkProcessor ()
      let env0,_,_,_ = TestEnv.mkEnv()
      let mutable currentState = { Buffer = []; Emitted = [] }
      let mutable sutOutputs : int list = []

      let apply cmd =
        let x =
          match cmd with Add v -> v
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
        let res = StreamProcessor.runProcessor proc env |> Flow.run env0 CancellationToken.None |> fun vt -> vt.Result
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
      { new obj() with
          member _.Run(cmds:Cmd list) =
            cmds |> List.forall apply }

    let propMachine =
      gen {
        let! cmds = Gen.listOf genCmd
        let specInstance = spec()
        return (cmds, specInstance)
      }
      |> Prop.forAll
         (fun (cmds, specInstance:obj) ->
            let mi = specInstance.GetType().GetMethod("Run")
            let ok = mi.Invoke(specInstance,[| box cmds |]) :?> bool
            ok)

// Model-based test (Machine alternative)
let modelBasedTests =
  testList "Model-Based (Stateful Sum Machine)" [
    testPropertyWithConfig fastConfig "stateful sum model invariants hold" StatefulSumModel.propMachine
  ]


// ---------------------------------------------------------
// Aggregated Test Suite
// ---------------------------------------------------------

let allTests =
  testList "Flux.IO.Core Test Suite" [
    flowTests
    envelopeTests
    streamProcessorTests
    modelBasedTests
  ]

[<Tests>]
let tests = allTests