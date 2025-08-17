namespace Flux.IO.Runtime

open Flux.IO.Core
open System.Threading.Channels
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open FSharp.HashCollections

// Runtime abstractions that build on top of our Flow/StreamProcessor

// Routing and splitting
type RouteKey = string

type Router<'T> = Envelope<'T> -> RouteKey

type RouterDescriptor<'T> = {
    Router: Router<'T>
    Routes: Map<RouteKey, unit> // Just tracks valid routes
}

// Join strategies
type JoinStrategy<'T> =
    | FirstWins                           // First result wins
    | AllComplete                         // Wait for all
    | Quorum of int                       // Wait for N
    | Timeout of System.TimeSpan          // Wait up to timeout
    | Custom of ('T list -> bool)         // Custom completion logic

// Pipeline segment types that compose our processors
type PipelineSegment<'TIn, 'TOut> =
    | Processor of StreamProcessor<'TIn, 'TOut>
    | Source of (ExecutionEnv -> CancellationToken -> IAsyncEnumerable<Envelope<'TIn>>)
    | Sink of (ExecutionEnv -> Envelope<'TIn> -> CancellationToken -> ValueTask)
    | Router of RouterDescriptor<'TIn> * Map<RouteKey, PipelineSegment<'TIn, 'TOut>>
    | Joiner of JoinStrategy<'TOut> * PipelineSegment<'TIn, 'TOut> list
    | Parallel of ParallelDescriptor<'TIn, 'TOut>
    | Accumulator of AccumulatorDescriptor<'TIn, 'TOut>

and ParallelDescriptor<'TIn, 'TOut> = {
    Engine: ParallelEngine
    Partitioner: 'TIn -> int
    DegreeOfParallelism: int
    Segment: PipelineSegment<'TIn, 'TOut>
}

and ParallelEngine =
    | TaskParallel
    | Hopac
    | DataFlow
    | Custom of obj

and AccumulatorDescriptor<'TIn, 'TOut> = {
    KeyExtractor: 'TIn -> string
    Policy: AccumulatorPolicy  
    Processor: StreamProcessor<'TIn, 'TOut>
}

// Runtime connectors between segments
type SegmentConnector =
    | Direct                                          // Same execution context
    | Channeled of ChannelOptions                    // Via channel
    | Queued of QueueOptions                        // Via queue
    | Broadcast                                       // Fan-out
    | RoundRobin                                      // Load balance

and ChannelOptions = {
    Capacity: int
    FullMode: BoundedChannelFullMode
    SingleReader: bool
    SingleWriter: bool
}

and QueueOptions = {
    MaxSize: int
    DropPolicy: DropPolicy
}

// Stage lifecycle management
type StageLifecycle =
    | OnDemand                                        // Created per work item
    | Persistent                                      // Reused across items
    | Continuous                                      // Runs forever

type StageDescriptor<'TIn, 'TOut> = {
    Id: string
    Lifecycle: StageLifecycle
    Segment: PipelineSegment<'TIn, 'TOut>
    Connector: SegmentConnector
}

// Example: Content-based router implementation
module RouterImpl =
    
    type RouterState<'T> = {
        Routes: Map<RouteKey, ChannelWriter<Envelope<'T>>>
        Metrics: IMetrics
    }
    
    let createRouter<'T> (router: Router<'T>) (routes: Map<RouteKey, unit>) =
        let channels = 
            routes 
            |> Map.map (fun _ _ -> 
                let ch = Channel.CreateUnbounded<Envelope<'T>>()
                ch.Writer, ch.Reader)
                
        let routerFn (env: ExecutionEnv) (ct: CancellationToken) (input: IAsyncEnumerable<Envelope<'T>>) =
            taskSeq {
                let writers = channels |> Map.map (fun _ (w, _) -> w)
                
                for envelope in input do
                    let route = router envelope
                    match Map.tryFind route writers with
                    | Some writer ->
                        do! writer.WriteAsync(envelope, ct)
                        env.Metrics.RecordCounter("router.routed", HashMap.ofList ["route", route], 1L)
                    | None ->
                        env.Metrics.RecordCounter("router.unrouted", HashMap.empty, 1L)
                        
                // Close all channels when done
                for (writer, _) in channels |> Map.toSeq |> Seq.map snd do
                    writer.Complete()
            }
            
        routerFn, channels |> Map.map (fun _ (_, r) -> r)

// Example: Join implementation
module JoinImpl =
    
    let createJoiner<'T> (strategy: JoinStrategy<'T>) (inputCount: int) =
        let results = ResizeArray<'T>(inputCount)
        let completed = ref 0
        let lock = obj()
        
        let checkComplete () =
            lock lock (fun () ->
                match strategy with
                | FirstWins -> results.Count > 0
                | AllComplete -> !completed = inputCount
                | Quorum n -> results.Count >= n
                | Timeout _ -> false // Handled externally
                | Custom check -> check (results |> List.ofSeq)
            )
            
        let addResult result =
            lock lock (fun () ->
                results.Add(result)
                checkComplete()
            )
            
        let incrementCompleted () =
            lock lock (fun () ->
                incr completed
                checkComplete()
            )
            
        let getResults () =
            lock lock (fun () ->
                results |> List.ofSeq
            )
            
        addResult, incrementCompleted, checkComplete, getResults

// Example: Building a split/join pipeline
module PipelineBuilder =
    
    type PipelineBuilder() =
        let mutable segments = []
        
        member _.Source(source) =
            segments <- Source source :: segments
            
        member _.Process(processor) =
            segments <- Processor processor :: segments
            
        member _.Split(router, routes) =
            segments <- Router(router, routes) :: segments
            
        member _.Join(strategy, branches) =
            segments <- Joiner(strategy, branches) :: segments
            
        member _.Sink(sink) =
            segments <- Sink sink :: segments
            
        member _.Build() =
            segments |> List.rev
            
    // Example: Content type splitter
    let contentTypeSplitter =
        let router (envelope: Envelope<byte[]>) =
            match Map.tryFind "Content-Type" envelope.Headers with
            | Some "application/json" -> "json"
            | Some "application/xml" -> "xml"
            | _ -> "unknown"
            
        { Router = router; Routes = Map.ofList ["json", (); "xml", (); "unknown", ()] }
        
    // Example: Building the ML pipeline
    let buildMLPipeline () =
        PipelineBuilder()
            .Source(socketSource "http://localhost:8080")
            .Split(contentTypeSplitter, 
                Map.ofList [
                    "json", Processor jsonProcessor
                    "xml", Processor xmlProcessor
                ])
            .Join(AllComplete, [])
            .Process(mlPreprocessor)
            .Parallel({
                Engine = Hopac
                Partitioner = fun data -> hash data % 8
                DegreeOfParallelism = 8
                Segment = Processor mlProcessor
            })
            .Process(resultAggregator)
            .Sink(databaseSink)
            .Build()

// Runtime execution engine
module Runtime =
    
    type RuntimeState = {
        Channels: Map<string, obj>
        Tasks: ResizeArray<Task>
        Cancellation: CancellationTokenSource
    }
    
    let rec executeSegment (state: RuntimeState) (env: ExecutionEnv) (segment: PipelineSegment<'a, 'b>) =
        match segment with
        | Processor proc ->
            // This uses our existing Flow-based processor
            fun (input: IAsyncEnumerable<Envelope<'a>>) ->
                taskSeq {
                    for envelope in input do
                        let! result = 
                            proc envelope 
                            |> Flow.run env state.Cancellation.Token
                        match result with
                        | StreamCommand.Emit e -> yield e
                        | _ -> ()
                }
                
        | Source source ->
            // Long-running source
            let output = source env state.Cancellation.Token
            // Return a function that ignores input since this is a source
            fun _ -> output
            
        | Router (desc, routes) ->
            // Create channels for each route
            let routerFn, readers = RouterImpl.createRouter desc.Router desc.Routes
            
            // Start router task
            let task = 
                fun input -> 
                    routerFn env state.Cancellation.Token input 
                    |> Task.ofAsyncSeq
            state.Tasks.Add(task input)
            
            // Return readers for downstream processing
            // This gets complex - need to manage multiple outputs
            
        | _ -> 
            // Other segment types...
            fun input -> taskSeq { yield! input }

// Key insight: We need a different abstraction for the runtime
// that manages the lifecycle and connections between our
// synchronous Flow-based processors

type PipelineRuntime<'TIn, 'TOut> = {
    Execute: ExecutionEnv -> CancellationToken -> IAsyncEnumerable<Envelope<'TIn>> -> Task<PipelineResult>
    Stop: unit -> Task
}