namespace Flux.IO.Tests

open Flux.IO
open Flux.IO.Core
open FsCheck
open FSharp.HashCollections
open Newtonsoft.Json
open System
open System.Collections.Concurrent
open System.Threading    
open System.Threading.Tasks

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

    let genJson = 
        gen {
            let! avgWidth = Gen.sized <| fun s -> Gen.choose(2, s)
            let! avgDepth = Gen.sized <| fun s -> Gen.choose(1, s)
            // let jsonRoot = JsonConvert.SerializeObject
            let! json = 
                let rec json' size =
                    
                    (* if size > 0 then
                        Gen.constant "{}"
                    else
                        let! key = Gen.stringOf (Gen.elements ['a'..'z'])
                        let! value = Gen.oneof [
                            Gen.constant "null"
                            Gen.constant "true"
                            Gen.constant "false"
                            Gen.stringOf (Gen.elements ['a'..'z'])
                            json' (size - 1)
                        ]
                        return sprintf "\"%s\":%s" key value *)
                in Gen.sized json'
            return (avgWidth, avgDepth)
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

module TestEnv =

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