namespace AsyncPipeline.DSL

open System
open AsyncPipeline.Core
open AsyncPipeline.Accumulation

// Pipeline builder state
type PipelineBuilderState = {
    Nodes: StageNode list
    Edges: Edge list
    CurrentStageId: string option
    Metadata: Map<string, string>
}

// Pipeline builder computation expression
type PipelineBuilder() =
    member _.Zero() = 
        { Nodes = []; Edges = []; CurrentStageId = None; Metadata = Map.empty }
        
    member _.Yield(x) = 
        { Nodes = []; Edges = []; CurrentStageId = None; Metadata = Map.empty }
        
    member _.Combine(state1: PipelineBuilderState, state2: PipelineBuilderState) =
        { Nodes = state1.Nodes @ state2.Nodes
          Edges = state1.Edges @ state2.Edges
          CurrentStageId = state2.CurrentStageId |> Option.orElse state1.CurrentStageId
          Metadata = Map.fold (fun acc k v -> Map.add k v acc) state1.Metadata state2.Metadata }
          
    member _.Delay(f) = f()
    
    member _.Run(state: PipelineBuilderState) : Pipeline =
        let graph = {
            Nodes = state.Nodes
            Edges = state.Edges
            Metadata = state.Metadata
        }
        
        // Validate the pipeline
        let errors = PipelineValidator.validate graph
        
        { Graph = graph; ValidationErrors = errors }

// Pipeline builder operations
module PipelineOps =
    
    let source (stage: Stage<'In, 'Out>) (state: PipelineBuilderState) =
        let node = {
            Stage = stage :> obj
            Kind = StageKind.Source
            Parallelism = Sequential
            Accumulation = None
            BufferPolicy = None
            Backpressure = None
            ErrorPolicy = Fail
        }
        { state with 
            Nodes = node :: state.Nodes
            CurrentStageId = Some stage.Id }
            
    let transform (stage: Stage<'In, 'Out>) (state: PipelineBuilderState) =
        match state.CurrentStageId with
        | None -> failwith "No previous stage to connect from"
        | Some fromId ->
            let node = {
                Stage = stage :> obj
                Kind = StageKind.Transform
                Parallelism = Sequential
                Accumulation = None
                BufferPolicy = None
                Backpressure = None
                ErrorPolicy = Fail
            }
            let edge = {
                From = fromId
                To = stage.Id
                Channel = { BufferSize = 1000; Ordering = Ordered }
            }
            { state with
                Nodes = node :: state.Nodes
                Edges = edge :: state.Edges
                CurrentStageId = Some stage.Id }
                
    let accumulate (accumulator: Accumulator<'In, 'Out, 'Key, 'State>) (policy: AccumulatorPolicy) (state: PipelineBuilderState) =
        match state.CurrentStageId with
        | None -> failwith "No previous stage to connect from"
        | Some fromId ->
            let accStage = AccumulatorStage(accumulator, policy, Unchecked.defaultof<_>, Unchecked.defaultof<_>) :> Stage<'In, 'Out>
            let node = {
                Stage = accStage :> obj
                Kind = StageKind.Accumulator
                Parallelism = Sequential
                Accumulation = Some {
                    Policy = policy
                    EngineHint = Sequential
                    ProgressWeights = Map.empty
                }
                BufferPolicy = None
                Backpressure = None
                ErrorPolicy = Fail
            }
            let edge = {
                From = fromId
                To = accStage.Id
                Channel = { BufferSize = 1000; Ordering = Ordered }
            }
            { state with
                Nodes = node :: state.Nodes
                Edges = edge :: state.Edges
                CurrentStageId = Some accStage.Id }
                
    let sink (stage: Stage<'In, 'Out>) (state: PipelineBuilderState) =
        match state.CurrentStageId with
        | None -> failwith "No previous stage to connect from"
        | Some fromId ->
            let node = {
                Stage = stage :> obj
                Kind = StageKind.Sink
                Parallelism = Sequential
                Accumulation = None
                BufferPolicy = None
                Backpressure = None
                ErrorPolicy = Fail
            }
            let edge = {
                From = fromId
                To = stage.Id
                Channel = { BufferSize = 1000; Ordering = Ordered }
            }
            { state with
                Nodes = node :: state.Nodes
                Edges = edge :: state.Edges
                CurrentStageId = Some stage.Id }
                
    let branch (predicate: Envelope<'T> -> bool) (trueBranch: Stage<'T, 'Out1>) (falseBranch: Stage<'T, 'Out2>) (state: PipelineBuilderState) =
        // Branching logic implementation
        state
        
    let merge (stages: Stage<'In, 'Out> list) (state: PipelineBuilderState) =
        // Merge logic implementation
        state
        
    let parallelism (hint: ParallelismHint) (state: PipelineBuilderState) =
        match state.Nodes with
        | [] -> state
        | head :: tail ->
            let updated = { head with Parallelism = hint }
            { state with Nodes = updated :: tail }
            
    let backpressure (strategy: BackpressureStrategy) (state: PipelineBuilderState) =
        match state.Nodes with
        | [] -> state
        | head :: tail ->
            let updated = { head with Backpressure = Some strategy }
            { state with Nodes = updated :: tail }
            
    let errorPolicy (policy: ErrorPolicy) (state: PipelineBuilderState) =
        match state.Nodes with
        | [] -> state
        | head :: tail ->
            let updated = { head with ErrorPolicy = policy }
            { state with Nodes = updated :: tail }
            
    let bufferSize (size: int) (state: PipelineBuilderState) =
        match state.Edges with
        | [] -> state
        | head :: tail ->
            let updated = { head with Channel = { head.Channel with BufferSize = size } }
            { state with Edges = updated :: tail }
            
    let withMetadata (key: string) (value: string) (state: PipelineBuilderState) =
        { state with Metadata = Map.add key value state.Metadata }

// Custom operators for pipeline DSL
module PipelineOperators =
    let (|>) state f = f state
    let (>>) f g state = state |> f |> g

// Pipeline validation
module PipelineValidator =
    
    let validate (graph: PipelineGraph) : string list =
        let errors = ResizeArray<string>()
        
        // Check for orphan nodes
        let nodeIds = graph.Nodes |> List.map (fun n -> n.Stage.GetType().GetProperty("Id").GetValue(n.Stage) :?> string) |> Set.ofList
        let connectedNodes = 
            graph.Edges 
            |> List.collect (fun e -> [e.From; e.To]) 
            |> Set.ofList
            
        let orphans = Set.difference nodeIds connectedNodes
        if not (Set.isEmpty orphans) then
            errors.Add($"Orphan nodes found: {orphans}")
            
        // Check for cycles
        let rec hasCycle visited current edges =
            if Set.contains current visited then
                true
            else
                let neighbors = 
                    edges 
                    |> List.filter (fun e -> e.From = current) 
                    |> List.map (fun e -> e.To)
                let newVisited = Set.add current visited
                neighbors |> List.exists (hasCycle newVisited <| edges)
                
        let sources = 
            graph.Nodes 
            |> List.filter (fun n -> n.Kind = Source)
            |> List.map (fun n -> n.Stage.GetType().GetProperty("Id").GetValue(n.Stage) :?> string)
            
        for source in sources do
            if hasCycle Set.empty source graph.Edges then
                errors.Add($"Cycle detected starting from {source}")
                
        // Check accumulator constraints
        let accumulatorNodes = 
            graph.Nodes 
            |> List.filter (fun n -> n.Kind = Accumulator)
            
        for acc in accumulatorNodes do
            let accId = acc.Stage.GetType().GetProperty("Id").GetValue(acc.Stage) :?> string
            let feedsBackToSelf = 
                graph.Edges 
                |> List.exists (fun e -> e.From = accId && e.To = accId)
            if feedsBackToSelf then
                errors.Add($"Accumulator {accId} cannot feed back to itself")
                
        errors |> List.ofSeq

// Example stages for common operations
module CommonStages =
    
    type PassThroughStage<'T>() =
        interface Stage<'T, 'T> with
            member _.Id = "passthrough"
            member _.Kind = Transform
            member _.Init(_) = ValueTask.CompletedTask
            member _.Process(ct, input) = input
            member _.Close() = ValueTask.CompletedTask
            
    type FilterStage<'T>(predicate: 'T -> bool) =
        interface Stage<'T, 'T> with
            member _.Id = "filter"
            member _.Kind = Transform
            member _.Init(_) = ValueTask.CompletedTask
            member _.Process(ct, input) =
                { new IAsyncEnumerable<Envelope<'T>> with
                    member _.GetAsyncEnumerator(ct2) =
                        let enumerator = input.GetAsyncEnumerator(ct2)
                        { new IAsyncEnumerator<Envelope<'T>> with
                            member _.Current = enumerator.Current
                            member _.MoveNextAsync() =
                                task {
                                    let mutable found = false
                                    let mutable hasNext = true
                                    
                                    while not found && hasNext do
                                        let! moveNext = enumerator.MoveNextAsync()
                                        hasNext <- moveNext
                                        
                                        if hasNext && predicate enumerator.Current.Payload then
                                            found <- true
                                            
                                    return found
                                } |> ValueTask<_>
                            member _.DisposeAsync() = enumerator.DisposeAsync()
                        }
                }
            member _.Close() = ValueTask.CompletedTask
            
    type MapStage<'TIn, 'TOut>(mapper: 'TIn -> 'TOut) =
        interface Stage<'TIn, 'TOut> with
            member _.Id = "map"
            member _.Kind = Transform
            member _.Init(_) = ValueTask.CompletedTask
            member _.Process(ct, input) =
                { new IAsyncEnumerable<Envelope<'TOut>> with
                    member _.GetAsyncEnumerator(ct2) =
                        let enumerator = input.GetAsyncEnumerator(ct2)
                        { new IAsyncEnumerator<Envelope<'TOut>> with
                            member _.Current = 
                                let env = enumerator.Current
                                { env with Payload = mapper env.Payload }
                            member _.MoveNextAsync() = enumerator.MoveNextAsync()
                            member _.DisposeAsync() = enumerator.DisposeAsync()
                        }
                }
            member _.Close() = ValueTask.CompletedTask
            
    type BatchStage<'T>(batchSize: int) =
        interface Stage<'T, 'T array> with
            member _.Id = "batch"
            member _.Kind = Transform
            member _.Init(_) = ValueTask.CompletedTask
            member _.Process(ct, input) =
                { new IAsyncEnumerable<Envelope<'T array>> with
                    member _.GetAsyncEnumerator(ct2) =
                        let enumerator = input.GetAsyncEnumerator(ct2)
                        let batch = ResizeArray<'T>(batchSize)
                        let mutable current = Unchecked.defaultof<_>
                        
                        { new IAsyncEnumerator<Envelope<'T array>> with
                            member _.Current = current
                            member _.MoveNextAsync() =
                                task {
                                    batch.Clear()
                                    let mutable count = 0
                                    let mutable hasNext = true
                                    
                                    while count < batchSize && hasNext do
                                        let! moveNext = enumerator.MoveNextAsync()
                                        hasNext <- moveNext
                                        
                                        if hasNext then
                                            batch.Add(enumerator.Current.Payload)
                                            count <- count + 1
                                            
                                    if batch.Count > 0 then
                                        current <- {
                                            Payload = batch.ToArray()
                                            Headers = Map.empty
                                            SeqId = 0L
                                            SpanCtx = { TraceId = ""; SpanId = ""; Baggage = System.Collections.Generic.Dictionary<_,_>() :> System.Collections.Generic.IReadOnlyDictionary<_,_> }
                                            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                                            Attrs = System.Collections.Generic.Dictionary<_,_>() :> System.Collections.Generic.IReadOnlyDictionary<_,_>
                                            Cost = { Bytes = count * 100; CpuHint = 0.0 }
                                        }
                                        return true
                                    else
                                        return false
                                } |> ValueTask<_>
                            member _.DisposeAsync() = enumerator.DisposeAsync()
                        }
                }
            member _.Close() = ValueTask.CompletedTask

// Fluent API extensions
[<AutoOpen>]
module PipelineExtensions =
    
    type PipelineBuilder with
        member _.source(stage) = PipelineOps.source stage
        member _.transform(stage) = PipelineOps.transform stage
        member _.accumulate(acc, policy) = PipelineOps.accumulate acc policy
        member _.sink(stage) = PipelineOps.sink stage
        member _.parallelism(hint) = PipelineOps.parallelism hint
        member _.backpressure(strategy) = PipelineOps.backpressure strategy
        member _.errorPolicy(policy) = PipelineOps.errorPolicy policy
        
    let pipeline = PipelineBuilder()