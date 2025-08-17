namespace Flux.IO.DSL

open Flux.IO.Core

// Core pipeline building types
type BackendMarker = interface end

type AsyncMarker() = interface BackendMarker
type HopacMarker() = interface BackendMarker
type TPLMarker() = interface BackendMarker

// Phantom type to track backend in pipeline
type PipelineSegment<'Backend, 'TIn, 'TOut when 'Backend :> BackendMarker> = 
    | Segment of (unit -> StreamProcessor<'TIn, 'TOut>)

// Base pipeline operations available to all backends
module PipelineOps =
    
    let inline lift (f: 'a -> 'b) : PipelineSegment<'Backend, 'a, 'b> =
        Segment (fun () -> StreamProcessor.lift f)
        
    let inline filter (predicate: 'a -> bool) : PipelineSegment<'Backend, 'a, 'a> =
        Segment (fun () -> StreamProcessor.filter predicate)
        
    let inline withEnv (f: ExecutionEnv -> 'a -> Flow<'b>) : PipelineSegment<'Backend, 'a, 'b> =
        Segment (fun () -> StreamProcessor.withEnv f)

// Async-specific operations
module Async =
    type Marker = AsyncMarker
    
    // Re-export common operations with Async marker
    let lift<'a, 'b> (f: 'a -> 'b) : PipelineSegment<Marker, 'a, 'b> = 
        PipelineOps.lift f
        
    let filter<'a> (predicate: 'a -> bool) : PipelineSegment<Marker, 'a, 'a> = 
        PipelineOps.filter predicate
        
    let withEnv<'a, 'b> (f: ExecutionEnv -> 'a -> Flow<'b>) : PipelineSegment<Marker, 'a, 'b> = 
        PipelineOps.withEnv f
        
    // Async-specific operations
    let stateful<'state, 'a, 'b> (initial: 'state) (f: 'state -> 'a -> Async<'state * 'b option>) : PipelineSegment<Marker, 'a, 'b> =
        Segment (fun () ->
            let asyncF state value = 
                async {
                    let! result = f state value
                    return result
                } |> Async.StartAsTask |> ValueTask
                
            StreamProcessor.stateful initial (fun s v -> 
                let task = asyncF s v
                task.Result // This would be properly async in real impl
            )
        )
        
    let parallel (degreeOfParallelism: int) (processor: PipelineSegment<Marker, 'a, 'b>) : PipelineSegment<Marker, 'a, 'b> =
        // Use Task.WhenAll or similar for parallelism
        processor

// Hopac-specific operations
module Hopac =
    open Hopac
    open Hopac.Infixes
    
    type Marker = HopacMarker
    
    // Re-export common operations with Hopac marker
    let lift<'a, 'b> (f: 'a -> 'b) : PipelineSegment<Marker, 'a, 'b> = 
        PipelineOps.lift f
        
    let filter<'a> (predicate: 'a -> bool) : PipelineSegment<Marker, 'a, 'a> = 
        PipelineOps.filter predicate
        
    // Hopac-specific stateful processor
    let stateful<'state, 'a, 'b> (initial: 'state) (f: 'state -> 'a -> Job<'state * 'b option>) : PipelineSegment<Marker, 'a, 'b> =
        Segment (fun () ->
            // This would need proper Hopac integration
            let hopacF state value =
                // Convert Job to Flow - this is where backend bridging happens
                run (f state value) // Simplified
                
            StreamProcessor.stateful initial hopacF
        )
        
    // Hopac parallel operations
    let mapPar<'a, 'b> (parallelism: int) (f: 'a -> Job<'b>) : PipelineSegment<Marker, 'a, 'b> =
        Segment (fun () ->
            // Create a processor that spawns Hopac jobs
            fun envelope ->
                Flow.flow {
                    // This would properly integrate with Hopac's job scheduling
                    let! result = 
                        f envelope.Payload 
                        |> Job.toAsync 
                        |> Async.StartAsTask 
                        |> Flow.liftTask
                    return StreamCommand.Emit (mapEnvelope (fun _ -> result) envelope)
                }
        )
        
    let scatter<'a> (partitioner: 'a -> int) (parallelism: int) : PipelineSegment<Marker, 'a, 'a> =
        // Creates parallel Hopac workers with channels
        Segment (fun () -> 
            // Implementation would create Hopac Ch channels
            StreamProcessor.lift id // Placeholder
        )

// Pipeline builder with backend tracking
type Pipeline<'Backend, 'TIn, 'TOut when 'Backend :> BackendMarker> = {
    Segments: PipelineSegment<'Backend, 'TIn, 'TOut> list
}

// Composition operators that preserve backend type
let (|>>) (p1: PipelineSegment<'Backend, 'a, 'b>) (p2: PipelineSegment<'Backend, 'b, 'c>) : PipelineSegment<'Backend, 'a, 'c> =
    Segment (fun () ->
        let proc1 = match p1 with Segment f -> f()
        let proc2 = match p2 with Segment f -> f()
        proc1 >=> proc2
    )

// Bridge operator for crossing backend boundaries
let bridge<'Backend1, 'Backend2, 'T when 'Backend1 :> BackendMarker and 'Backend2 :> BackendMarker> 
    (source: PipelineSegment<'Backend1, 'a, 'T>) 
    (target: PipelineSegment<'Backend2, 'T, 'b>) 
    : PipelineSegment<AsyncMarker, 'a, 'b> =
    // Bridges always run in Async as the coordinator
    Segment (fun () ->
        // Implementation would handle backend bridging
        StreamProcessor.lift id // Placeholder
    )

// Example usage showing explicit backend choices
module Example =
    open Async
    open Hopac
    
    let myPipeline =
        // Async for I/O
        Async.lift readBytes
        |>> Async.filter (fun bytes -> bytes.Length > 0)
        |>> Async.lift parseJson
        // Bridge to Hopac for CPU-intensive work
        |> bridge (
            Hopac.mapPar 100 enrichData
            |>> Hopac.scatter (fun x -> x.GetHashCode()) 8
        )
        // Bridge back to Async for output
        |> bridge (
            Async.withEnv writeToDatabase
        )