namespace Flux.IO

module Stages =
    open Core
    open FSharp.Control
    open FSharp.HashCollections
    open System.Threading
    open System.Threading.Tasks
    
    // Base interface for stages that process streams
    type IStage<'TIn, 'TOut> =
        abstract member Id: string
        abstract member Kind: StageKind
        abstract member Process: ExecutionEnv -> CancellationToken -> AsyncSeq<Envelope<'TIn>> -> AsyncSeq<Envelope<'TOut>>
        
    // Alternative interface using TaskSeq for performance-critical paths
    type ITaskStage<'TIn, 'TOut> =
        abstract member Id: string
        abstract member Kind: StageKind
        abstract member Process: ExecutionEnv -> CancellationToken -> TaskAsyncSeq<Envelope<'TIn>> -> TaskAsyncSeq<Envelope<'TOut>>
    
    // Adapter to convert between stage types
    module StageAdapters =
        let adaptToAsync (stage: ITaskStage<'TIn, 'TOut>) : IStage<'TIn, 'TOut> =
            { new IStage<'TIn, 'TOut> with
                member _.Id = stage.Id
                member _.Kind = stage.Kind
                member _.Process env ct input =
                    input 
                    |> toTaskSeq
                    |> stage.Process env ct
                    |> toAsyncSeq
            }
            
        let adaptToTask (stage: IStage<'TIn, 'TOut>) : ITaskStage<'TIn, 'TOut> =
            { new ITaskStage<'TIn, 'TOut> with
                member _.Id = stage.Id
                member _.Kind = stage.Kind
                member _.Process env ct input =
                    input 
                    |> toAsyncSeq
                    |> stage.Process env ct
                    |> toTaskSeq
            }
    
    // Example: JSON parsing stage using StreamProcessor
    type JsonParsingStage() =
        let processor = 
            StreamProcessor.withEnv (fun env (data: System.ReadOnlyMemory<byte>) ->
                Flow.flow {
                    try
                        let json = System.Text.Encoding.UTF8.GetString(data.Span)
                        let parsed = Newtonsoft.Json.Linq.JObject.Parse(json)
                        
                        env.Metrics.RecordCounter("json_parsed", HashMap.empty, 1L)
                        return parsed
                    with ex ->
                        env.Logger.LogError("JSON parsing failed", ex)
                        return! Flow.Flow (fun _ _ -> raise ex)
                }
            )
            
        interface IStage<System.ReadOnlyMemory<byte>, Newtonsoft.Json.Linq.JObject> with
            member _.Id = "json-parser"
            member _.Kind = Transform
            member _.Process env ct input =
                asyncSeq {
                    for envelope in input do
                        let! result = 
                            StreamProcessor.runProcessor processor envelope
                            |> Flow.run env ct
                            |> Async.AwaitTask
                            
                        match result with
                        | StreamCommand.Emit outEnv -> yield outEnv
                        | StreamCommand.EmitMany envs -> yield! AsyncSeq.ofSeq envs
                        | StreamCommand.RequireMore -> () // Continue to next
                        | StreamCommand.Complete -> () // End stream
                        | StreamCommand.Error ex -> raise ex
                }
    
    // Example: Accumulator stage for template variables
    type TemplateAccumulatorStage(requiredVars: Set<string>) =
        let processor =
            let initialState = HashMap.empty<string, obj>
            
            StreamProcessor.stateful initialState (fun state (json: Newtonsoft.Json.Linq.JObject) ->
                // Extract variables from JSON
                let mutable newState = state
                for prop in json.Properties() do
                    if requiredVars.Contains prop.Name then
                        newState <- HashMap.add prop.Name (prop.Value.ToObject<obj>()) newState
                
                // Check if we have all required variables
                let hasAll = requiredVars |> Set.forall (fun v -> HashMap.containsKey v newState)
                
                if hasAll then
                    // Create output with all variables
                    let output = 
                        newState 
                        |> HashMap.toSeq
                        |> dict
                        |> System.Collections.Generic.Dictionary
                    newState, Some output
                else
                    newState, None
            )
            
        interface IStage<Newtonsoft.Json.Linq.JObject, System.Collections.Generic.Dictionary<string, obj>> with
            member _.Id = "template-accumulator"
            member _.Kind = Accumulator
            member _.Process env ct input =
                asyncSeq {
                    for envelope in input do
                        let! result = 
                            StreamProcessor.runProcessor processor envelope
                            |> Flow.run env ct
                            |> Async.AwaitTask
                            
                        match result with
                        | StreamCommand.Emit outEnv -> 
                            env.Metrics.RecordCounter("templates_completed", HashMap.empty, 1L)
                            yield outEnv
                        | StreamCommand.RequireMore -> 
                            env.Metrics.RecordCounter("accumulator_waiting", HashMap.empty, 1L)
                        | _ -> ()
                }
    
    // Example: High-performance filter stage using TaskSeq
    type FilterStage<'T>(predicate: 'T -> bool) =
        interface ITaskStage<'T, 'T> with
            member _.Id = "filter"
            member _.Kind = Transform
            member _.Process env ct input =
                taskSeq {
                    for envelope in input do
                        if predicate envelope.Payload then
                            env.Metrics.RecordCounter("filter_passed", HashMap.empty, 1L)
                            yield envelope
                        else
                            env.Metrics.RecordCounter("filter_rejected", HashMap.empty, 1L)
                }
    
    // Example: Source stage that reads from a channel
    type ChannelSourceStage<'T>(channel: System.Threading.Channels.ChannelReader<'T>) =
        interface ITaskStage<unit, 'T> with
            member _.Id = "channel-source"
            member _.Kind = Source
            member _.Process env ct _ =
                taskSeq {
                    let mutable seqId = 0L
                    
                    while not ct.IsCancellationRequested do
                        match! channel.TryReadAsync(ct) with
                        | true, value ->
                            seqId <- seqId + 1L
                            yield {
                                Payload = value
                                Headers = HashMap.empty
                                SeqId = seqId
                                SpanCtx = { TraceId = ""; SpanId = ""; Baggage = HashMap.empty }
                                Ts = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                                Attrs = HashMap.empty
                                Cost = { Bytes = 0; CpuHint = 0.0 }
                            }
                        | false, _ ->
                            // Channel completed
                            ()
                }
    
    // Pipeline builder that wires stages together
    type Pipeline<'TIn, 'TOut> =
        | Single of IStage<'TIn, 'TOut>
        | Chain of IStage<'TIn, 'TMid> * Pipeline<'TMid, 'TOut>
    
    module Pipeline =
        let rec execute (env: ExecutionEnv) (ct: CancellationToken) (pipeline: Pipeline<'TIn, 'TOut>) (input: AsyncSeq<Envelope<'TIn>>) : AsyncSeq<Envelope<'TOut>> =
            match pipeline with
            | Single stage -> 
                stage.Process env ct input
            | Chain (stage, rest) ->
                let intermediate = stage.Process env ct input
                execute env ct rest intermediate
                
        // Combinators for building pipelines
        let single stage = Single stage
        
        let chain (first: IStage<'a, 'b>) (second: Pipeline<'b, 'c>) : Pipeline<'a, 'c> =
            Chain (first, second)
            
        let (>>>) = chain
        
        // Example: Build a complete pipeline
        let examplePipeline =
            let jsonParser = JsonParsingStage() :> IStage<_, _>
            let accumulator = TemplateAccumulatorStage(Set.ofList ["id"; "name"; "value"]) :> IStage<_, _>
            
            jsonParser >>> single accumulator
    
    // Example: Running a pipeline with backpressure
    module Execution =
        open System.Threading.Channels
        
        type BackpressureStrategy =
            | Bounded of capacity: int
            | Unbounded
            | Dropping of capacity: int
            
        let createChannel strategy =
            match strategy with
            | Bounded capacity ->
                Channel.CreateBounded<'T>(BoundedChannelOptions(capacity))
            | Unbounded ->
                Channel.CreateUnbounded<'T>()
            | Dropping capacity ->
                Channel.CreateBounded<'T>(
                    BoundedChannelOptions(
                        capacity,
                        FullMode = BoundedChannelFullMode.DropOldest))
        
        let runPipelineWithBackpressure 
            (pipeline: Pipeline<'TIn, 'TOut>) 
            (env: ExecutionEnv)
            (ct: CancellationToken)
            (input: AsyncSeq<Envelope<'TIn>>)
            (outputStrategy: BackpressureStrategy) =
            
            task {
                let outputChannel = createChannel outputStrategy
                
                // Process pipeline and write to output channel
                let processTask = task {
                    try
                        let output = Pipeline.execute env ct pipeline input
                        for item in output do
                            do! outputChannel.Writer.WriteAsync(item, ct)
                    finally
                        outputChannel.Writer.Complete()
                }
                
                // Return channel reader for downstream consumption
                return outputChannel.Reader, processTask
            }