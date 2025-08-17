namespace Flux.IO.Examples

module TypeTransformationDemo =
    open Flux.IO.Core
    open FSharp.HashCollections
    open System
    open System.Text
    
    // Example showing type transformations through a pipeline:
    // bytes -> string -> json -> domain type -> output
    
    // Domain types
    type Customer = {
        Id: string
        Name: string
        Email: string
        CreditLimit: decimal
    }
    
    type ValidationResult = 
        | Valid of Customer
        | Invalid of reason: string
    
    type ProcessedOrder = {
        CustomerId: string
        OrderId: string
        Amount: decimal
        Status: string
    }
    
    // Stream processors that transform between types
    module Processors =
        open StreamProcessor
        
        // bytes -> string
        let bytesToString : StreamProcessor<ReadOnlyMemory<byte>, string> =
            lift (fun bytes -> Encoding.UTF8.GetString(bytes.Span))
            
        // string -> JObject
        let parseJson : StreamProcessor<string, Newtonsoft.Json.Linq.JObject> =
            withEnv (fun env str ->
                Flow.flow {
                    try
                        let json = Newtonsoft.Json.Linq.JObject.Parse(str)
                        env.Metrics.RecordCounter("json_parsed", HashMap.empty, 1L)
                        return json
                    with ex ->
                        env.Logger.LogError("Failed to parse JSON", ex)
                        return! Flow.Flow (fun _ _ -> raise ex)
                }
            )
            
        // JObject -> Customer
        let extractCustomer : StreamProcessor<Newtonsoft.Json.Linq.JObject, Customer> =
            lift (fun json ->
                {
                    Id = json.["id"].ToString()
                    Name = json.["name"].ToString()
                    Email = json.["email"].ToString()
                    CreditLimit = json.["creditLimit"].ToObject<decimal>()
                }
            )
            
        // Customer -> ValidationResult
        let validateCustomer : StreamProcessor<Customer, ValidationResult> =
            withEnv (fun env customer ->
                Flow.flow {
                    if String.IsNullOrWhiteSpace(customer.Email) then
                        env.Logger.Log("WARN", sprintf "Invalid customer: %s" customer.Id)
                        return Invalid "Email is required"
                    elif customer.CreditLimit < 0m then
                        return Invalid "Credit limit cannot be negative"
                    else
                        env.Metrics.RecordCounter("customers_validated", HashMap.empty, 1L)
                        return Valid customer
                }
            )
            
        // Stateful processor that accumulates orders by customer
        let accumulateOrders : StreamProcessor<ValidationResult, ProcessedOrder list> =
            stateful Map.empty (fun state validationResult ->
                match validationResult with
                | Valid customer ->
                    let orders = 
                        state 
                        |> Map.tryFind customer.Id 
                        |> Option.defaultValue []
                        
                    let newOrder = {
                        CustomerId = customer.Id
                        OrderId = Guid.NewGuid().ToString()
                        Amount = customer.CreditLimit * 0.1m // Example calculation
                        Status = "Pending"
                    }
                    
                    let updatedOrders = newOrder :: orders
                    let newState = Map.add customer.Id updatedOrders state
                    
                    // Emit when we have 5 orders for a customer
                    if List.length updatedOrders >= 5 then
                        newState, Some updatedOrders
                    else
                        newState, None
                        
                | Invalid _ ->
                    // Don't accumulate invalid customers
                    state, None
            )
    
    // Example of composing these processors
    module Pipeline =
        open StreamProcessor
        
        // Using the pipeline operator
        let fullPipeline : StreamProcessor<ReadOnlyMemory<byte>, ProcessedOrder list> =
            Processors.bytesToString
            |>> Processors.parseJson
            |>> Processors.extractCustomer
            |>> Processors.validateCustomer
            |>> Processors.accumulateOrders
            
        // Alternative: using Kleisli composition
        let fullPipeline2 =
            Processors.bytesToString >=>
            Processors.parseJson >=>
            Processors.extractCustomer >=>
            Processors.validateCustomer >=>
            Processors.accumulateOrders
            
    // Test the pipeline
    module Test =
        open System.Threading
        
        let createTestEnvelope payload seqId =
            {
                Payload = payload
                Headers = HashMap.empty
                SeqId = seqId
                SpanCtx = { TraceId = "test"; SpanId = "1"; Baggage = HashMap.empty }
                Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                Attrs = HashMap.empty
                Cost = { Bytes = 0; CpuHint = 0.0 }
            }
            
        let runExample () =
            // Create test environment
            let env = {
                Metrics = { new IMetrics with
                    member _.RecordCounter(name, tags, value) = 
                        printfn "Metric: %s = %d" name value
                    member _.RecordGauge(name, tags, value) = ()
                    member _.RecordHistogram(name, tags, value) = ()
                }
                Logger = { new ILogger with
                    member _.Log(level, msg) = printfn "[%s] %s" level msg
                    member _.LogError(msg, ex) = printfn "[ERROR] %s: %s" msg ex.Message
                }
                Tracer = { new ITracer with
                    member _.StartSpan(name, parent) = 
                        { TraceId = "test"; SpanId = Guid.NewGuid().ToString(); Baggage = HashMap.empty }
                    member _.EndSpan(ctx) = ()
                    member _.AddEvent(ctx, name, attrs) = ()
                }
                Memory = { new IMemoryPool with
                    member _.RentBuffer(size) = ArraySegment<byte>(Array.zeroCreate size)
                    member _.ReturnBuffer(buffer) = ()
                }
            }
            
            // Test data
            let customerJson = """{"id":"123","name":"John Doe","email":"john@example.com","creditLimit":5000}"""
            let bytes = Encoding.UTF8.GetBytes(customerJson) |> ReadOnlyMemory
            
            // Create envelope
            let envelope = createTestEnvelope bytes 1L
            
            // Run the pipeline
            let result = 
                Pipeline.fullPipeline envelope
                |> Flow.run env CancellationToken.None
                |> fun vt -> vt.AsTask().Result
                
            match result with
            | StreamCommand.Emit env ->
                printfn "Emitted orders: %A" env.Payload
            | StreamCommand.RequireMore ->
                printfn "Need more data to accumulate 5 orders"
            | _ ->
                printfn "Other result: %A" result