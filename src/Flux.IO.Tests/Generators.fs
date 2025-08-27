namespace Flux.IO.Tests

open Flux.IO
// open Flux.IO.Core1
open Flux.IO.Core.Types
open Flux.IO.Pipeline.Direct
open FsCheck
open FSharp.HashCollections
open Newtonsoft.Json
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
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
                        Gen.elements [ AttrInt32 1; AttrString "v"; AttrInt32 42; AttrBool true; AttrFloat 0.5 ] |> Gen.listOfLength n
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
            let! a = Gen.choose(-5, 5)
            let! b = Gen.choose(-100, 100)
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
                    flow { return v }
                else
                    // TODO: re-introduce this with a proper external handle
                    flow {
                        Thread.Sleep(delayMs * 100) //, ct)
                        return v
                    }
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

    module JsonGenerators =
        open Newtonsoft.Json.Linq

        // Generate random property names
        let genPropertyName =
            gen {
                let! length = Gen.choose(3, 12)
                let! chars = Gen.arrayOfLength length (Gen.elements (['a'..'z'] @ ['A'..'Z']))
                let! suffix = Gen.elements [""; "_id"; "_name"; "_value"; "_type"; "_count"; "_date"]
                return String(chars) + suffix
            }

        // Generate scalar JSON values
        let genScalarValue =
            Gen.oneof [
                Gen.constant (JValue.CreateNull() :> JToken)  // null
                Arb.generate<bool> |> Gen.map (fun b -> JValue(b) :> JToken)  // boolean
                Arb.generate<int> |> Gen.map (fun i -> JValue(i) :> JToken)  // integer
                Arb.generate<float> |> Gen.map (fun f -> JValue(f) :> JToken)  // float

                Gen.elements [""; "test"; "value"; "data"; "example"; "foo"; "bar"] 
                |> Gen.map (fun s -> JValue(s) :> JToken)  // common strings
                
                Arb.generate<string> 
                |> Gen.filter (fun s -> s <> null) 
                |> Gen.map (fun s -> JValue(s) :> JToken)  // random strings
                
                Arb.generate<DateTime> 
                |> Gen.map (fun dt -> JValue(dt.ToString("O")) :> JToken)  // ISO date strings
            ]

        // Generate JSON arrays
        let rec genJsonArray depth size =
            gen {
                if depth <= 0 || size <= 0 then
                    // At leaf level, only scalar values
                    let! length = Gen.choose(0, min 5 size)
                    let! elements = Gen.listOfLength length genScalarValue
                    return JArray(elements) :> JToken
                else
                    // Can have nested structures
                    let! length = Gen.choose(0, min 4 size)
                    let elementSize = max 1 (size / (length + 1))
                    let! elements = Gen.listOfLength length (genJsonValue (depth - 1) elementSize)
                    return JArray(elements) :> JToken
            }

        // Generate JSON objects
        and genJsonObject depth size =
            gen {
                if depth <= 0 || size <= 0 then
                    // Leaf object with only scalar values
                    let! numProps = Gen.choose(1, min 5 (max 1 size))
                    let! props = Gen.listOfLength numProps (
                        gen {
                            let! name = genPropertyName
                            let! value = genScalarValue
                            return (name, value)
                        }
                    )
                    
                    let obj = JObject()
                    for (name, value) in props do
                        obj.[name] <- value
                    return obj :> JToken
                else
                    // Can have nested objects and arrays
                    let! numScalarProps = Gen.choose(0, min 3 size)
                    let! numNestedProps = Gen.choose(1, min 3 (max 1 (size - numScalarProps)))
                    
                    let! scalarProps = Gen.listOfLength numScalarProps (
                        gen {
                            let! name = genPropertyName
                            let! value = genScalarValue
                            return (name, value)
                        }
                    )
                    
                    let nestedPropSize = max 1 (size / (numNestedProps + 1))
                    let! nestedProps = Gen.listOfLength numNestedProps (
                        gen {
                            let! name = genPropertyName
                            let! value = genJsonValue (depth - 1) nestedPropSize
                            return (name, value)
                        }
                    )
                    
                    let obj = JObject()
                    for (name, value) in scalarProps @ nestedProps do
                        obj.[name] <- value
                    return obj :> JToken
            }

        // Generate any JSON value
        and genJsonValue depth size =
            if depth <= 0 || size <= 0 then
                genScalarValue
            else
                Gen.frequency [
                    (3, genScalarValue)  // Higher weight for scalars
                    (2, genJsonObject depth size)
                    (1, genJsonArray depth size)
                ]

        // Main JSON generator with size support for shrinking
        let genJson =
            Gen.sized (fun size ->
                gen {
                    // Scale depth based on size (smaller size = shallower depth)
                    let maxDepth = min 5 (max 1 (size / 10))
                    let! depth = Gen.choose(0, maxDepth)
                    let! json = genJsonObject depth size
                    return json.ToString Formatting.Indented
                }
            )

        // Alternative: Generate JSON with specific structure requirements
        let genJsonWithStructure minProps maxProps maxDepth =
            Gen.sized (fun size ->
                gen {
                    let rec buildObject depth currentSize =
                        gen {
                            let adjustedMax = min maxProps (max minProps currentSize)
                            let! numProps = Gen.choose(minProps, adjustedMax)
                            let propSize = max 1 (currentSize / (numProps + 1))
                            
                            let! props = Gen.listOfLength numProps (
                                gen {
                                    let! name = genPropertyName
                                    let! valueType = Gen.choose(0, 10)
                                    let! value =
                                        match valueType with
                                        | n when n < 5 -> genScalarValue  // 50% scalar
                                        | n when n < 8 && depth > 0 -> buildObject (depth - 1) propSize  // 30% nested object
                                        | _ when depth > 0 -> genJsonArray (depth - 1) propSize  // 20% array
                                        | _ -> genScalarValue  // Fallback to scalar at depth 0
                                    return (name, value)
                                }
                            )
                            
                            let obj = JObject()
                            for (name, value) in props do
                                obj.[name] <- value
                            return obj :> JToken
                        }
                        
                    let! depth = Gen.choose(0, min maxDepth (size / 5))
                    let! root = buildObject depth size
                    return root.ToString(Formatting.Indented)
                }
            )

        // Generate JSON that matches common patterns
        let genTypicalJson =
            Gen.oneof [
                // User-like object
                gen {
                    let! id = Gen.choose(1, 10000)
                    let! name = Gen.elements ["Alice", "Bob", "Charlie", "David", "Eve"]
                    let! email = Gen.map2 (sprintf "%s@%s.com") 
                                    (Gen.elements ["user"; "admin"; "test"]) 
                                    (Gen.elements ["example"; "test"; "mail"])
                    let! age = Gen.choose(18, 80)
                    let! active = Arb.generate<bool>
                    
                    let obj = JObject()
                    obj.["id"] <- JValue(id)
                    obj.["name"] <- JValue(name)
                    obj.["email"] <- JValue(email)
                    obj.["age"] <- JValue(age)
                    obj.["active"] <- JValue(active)
                    obj.["created"] <- JValue(DateTime.UtcNow.ToString("O"))
                    
                    return obj.ToString()
                }
                
                // Nested configuration object
                gen {
                    let! env = Gen.elements ["dev", "test", "prod"]
                    let! port = Gen.choose(1000, 9999)
                    let! debug = Arb.generate<bool>
                    
                    let config = JObject()
                    config.["environment"] <- JValue(env)
                    config.["server"] <- JObject([
                        JProperty("port", port)
                        JProperty("host", "localhost")
                    ])
                    config.["features"] <- JObject([
                        JProperty("debug", debug)
                        JProperty("logging", true)
                    ])
                    
                    return config.ToString()
                }
                
                // Array of items
                gen {
                    let! count = Gen.choose(1, 5)
                    let! items = Gen.listOfLength count (
                        gen {
                            let! id = Gen.choose(1, 100)
                            let! value = Gen.elements ["A", "B", "C", "D"]
                            let! quantity = Gen.choose(1, 10)
                            
                            let item = JObject()
                            item.["id"] <- JValue(id)
                            item.["value"] <- JValue(value)
                            item.["quantity"] <- JValue(quantity)
                            return item
                        }
                    )
                    
                    let root = JObject()
                    root.["items"] <- JArray(items)
                    root.["total"] <- JValue(items.Length)
                    
                    return root.ToString()
                }
            ]

        // Test the generators
        let examples() =
            printfn "Basic JSON:"
            let basic = Gen.sample 10 1 genJson
            basic |> List.iter (printfn "%s\n")
            
            printfn "\nStructured JSON (2-4 props, max depth 3):"
            let structured = Gen.sample 10 1 (genJsonWithStructure 2 4 3)
            structured |> List.iter (printfn "%s\n")
            
            printfn "\nTypical JSON:"
            let typical = Gen.sample 10 3 genTypicalJson
            typical |> List.iter (printfn "%s\n")

module TestEnv =

    // Minimal metric collector (in-memory counters)
    type InMemMetrics() =
        let counters = ConcurrentDictionary<string,int64>()
        let gauges   = ConcurrentDictionary<string,float>()
        let hgrams   = ConcurrentDictionary<string,ResizeArray<float>>()
        
        interface Metrics with
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
        interface Tracer with
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
        
        interface Logger with
            member __.Log(level,msg) =
                lines.Add(sprintf "[%s] %s" level msg)
            member __.LogError(msg,ex) =
                errors.Add(msg, ex.Message)
        
        member __.Lines = lines
        member __.Errors = errors

    type DummyPool() =
        interface MemoryPool with
            member _.RentBuffer(size) = ArraySegment<byte>(Array.zeroCreate size)
            member _.ReturnBuffer(_b) = ()

    let mkEnv () =
        let metrics = InMemMetrics()
        let tracer  = InMemTracer()
        let logger  = InMemLogger()
        { Metrics = metrics :> Metrics
          Tracer  = tracer  :> Tracer
          Logger  = logger  :> Logger
          Memory  = DummyPool() :> MemoryPool 
          NowUnix = fun () -> DateTimeOffset.UtcNow.ToUnixTimeSeconds()
          //Services = Map.empty
        },
        metrics, tracer, logger

    let runFlow (f: Flow<'a>) =
        let env,_,_,_ = mkEnv()
        let vt = run env (* CancellationToken.None *) f
        (* if vt.IsCompletedSuccessfully then vt.Result
        else vt.AsTask().Result *)
        vt.Result

    let flowEq1 (f1: Flow<'a>) (f2: Flow<'a>) =
        let env,_,_,_ = mkEnv()
        let r1 = run env (* CancellationToken.None *) f1
        let r2 = run env (* CancellationToken.None *) f2
        let v1 = if r1.IsCompleted then Choice1Of2 r1.Result else Choice2Of2 r1
        let v2 = if r2.IsCompleted then Choice1Of2 r2.Result else Choice2Of2 r2
        // Compare results (both succeed)
        match v1, v2 with
        | Choice1Of2 a, Choice1Of2 b -> a = b
        | _ -> false

    let flowEq (f1: Flow<'a>) (f2: Flow<'a>) =
        let env,_,_,_ = mkEnv()
        (* let ct = CancellationToken.None *)
        
        // Always get the actual result, regardless of sync/async
        let getResult (f: Flow<'a>) =
            let vt = run env (* ct *) f
            (* if vt.IsCompletedSuccessfully then 
                vt.Result
            else 
                vt.AsTask().Result *)
            vt.Result
        
        try
            let v1 = getResult f1
            let v2 = getResult f2
            v1 = v2
        with
        | _ -> false  // If either throws, they're not equal

    let hasSameCompletionBehavior (f1: Flow<'a>) (f2: Flow<'a>) =
        let env,_,_,_ = mkEnv()
        (* let ct = CancellationToken.None *)
        
        let vt1 = run env (* ct *) f1
        let vt2 = run env (* ct *) f2

        // Both complete synchronously or both complete asynchronously
        vt1.IsCompleted = vt2.IsCompleted