namespace AsyncPipeline.Accumulation

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open AsyncPipeline.Core

// Accumulator stage implementation
type AccumulatorStage<'In, 'Out, 'Key, 'State when 'Key : equality>(
    accumulator: Accumulator<'In, 'Out, 'Key, 'State>,
    policy: AccumulatorPolicy,
    memoryPool: IMemoryPool,
    metrics: IMetrics) =
    
    let stateTable = ConcurrentDictionary<'Key, AccumulatorState<'Key, 'State>>()
    let spilledKeys = ConcurrentDictionary<'Key, ReadOnlyMemory<byte>>()
    let mutable context = Unchecked.defaultof<StageContext>
    let timerCts = new CancellationTokenSource()
    let mutable timerTask = Unchecked.defaultof<Task>
    
    let recordMetrics (action: string) (key: 'Key) =
        let tags = Map.ofList [("stage", accumulator.Id); ("action", action)]
        metrics.RecordCounter("accumulator_operations", tags, 1L)
        
    let checkThresholds (state: AccumulatorState<'Key, 'State>) =
        let exceedsItems = policy.MaxItems |> Option.map (fun max -> state.SizeItems >= max) |> Option.defaultValue false
        let exceedsBytes = policy.MaxBytes |> Option.map (fun max -> state.SizeBytes >= max) |> Option.defaultValue false
        exceedsItems || exceedsBytes
        
    let checkTTL (state: AccumulatorState<'Key, 'State>) (now: Timestamp) =
        match policy.CompletenessTTL with
        | Some ttl ->
            let elapsed = now - state.FirstTs
            elapsed > int64 ttl.TotalMilliseconds
        | None -> false
        
    let spillState (key: 'Key) (state: AccumulatorState<'Key, 'State>) =
        if policy.SpillAllowed then
            try
                let serialized = accumulator.SerializeSpill state
                spilledKeys.[key] <- serialized
                stateTable.TryRemove(key) |> ignore
                recordMetrics "spilled" key
                true
            with ex ->
                context.Logger.LogError($"Failed to spill state for key {key}", ex)
                false
        else
            false
            
    let reloadState (key: 'Key) =
        match spilledKeys.TryGetValue(key) with
        | true, serialized ->
            try
                let state = accumulator.DeserializeSpill serialized
                spilledKeys.TryRemove(key) |> ignore
                recordMetrics "reloaded" key
                Some state
            with ex ->
                context.Logger.LogError($"Failed to reload state for key {key}", ex)
                None
        | false, _ -> None
        
    let emitComplete (output: 'Out) (state: AccumulatorState<'Key, 'State>) (completeness: float) =
        let headers = 
            Map.ofList [
                ("acc.completeness", completeness.ToString())
                ("acc.spilled", (spilledKeys.ContainsKey(state.Key)).ToString())
            ]
        {
            Payload = output
            Headers = headers
            SeqId = 0L
            SpanCtx = { TraceId = ""; SpanId = ""; Baggage = Dictionary<_,_>() :> IReadOnlyDictionary<_,_> }
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            Attrs = Dictionary<_,_>() :> IReadOnlyDictionary<_,_>
            Cost = { Bytes = state.SizeBytes; CpuHint = 0.0 }
        }
        
    let processEnvelope (envelope: Envelope<'In>) =
        async {
            let key = accumulator.ExtractKey envelope
            let now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            
            // Get or reload state
            let currentState =
                match stateTable.TryGetValue(key) with
                | true, state -> Some state
                | false, _ -> reloadState key
                
            // Update state
            let updatedState = accumulator.UpdateState currentState envelope
            
            // Check thresholds
            let shouldCheckComplete = 
                checkThresholds updatedState || 
                checkTTL updatedState now
                
            if shouldCheckComplete then
                match accumulator.CheckComplete updatedState with
                | Complete(output, newState) ->
                    stateTable.TryRemove(key) |> ignore
                    recordMetrics "completed" key
                    return Some (emitComplete output updatedState 1.0)
                    
                | Expired(outputOpt, newState) when policy.PartialFlush ->
                    match outputOpt with
                    | Some output ->
                        stateTable.TryRemove(key) |> ignore
                        recordMetrics "partial_flush" key
                        return Some (emitComplete output updatedState updatedState.CompletenessScore)
                    | None ->
                        stateTable.[key] <- updatedState
                        return None
                        
                | Incomplete newState ->
                    stateTable.[key] <- updatedState
                    
                    // Check memory pressure and spill if needed
                    let totalMemory = stateTable.Values |> Seq.sumBy (fun s -> int64 s.SizeBytes)
                    if totalMemory > 1_000_000_000L then // 1GB threshold
                        // Spill oldest incomplete states
                        let toSpill = 
                            stateTable.ToArray()
                            |> Array.sortBy (fun kvp -> kvp.Value.FirstTs)
                            |> Array.take (min 10 (stateTable.Count / 4))
                            
                        for kvp in toSpill do
                            spillState kvp.Key kvp.Value |> ignore
                            
                    return None
            else
                stateTable.[key] <- updatedState
                return None
        }
        
    let periodicFlush () =
        async {
            while not timerCts.Token.IsCancellationRequested do
                do! Async.Sleep 1000 // Check every second
                
                let now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                let expiredStates = 
                    stateTable.ToArray()
                    |> Array.filter (fun kvp -> checkTTL kvp.Value now)
                    
                for kvp in expiredStates do
                    match accumulator.FlushForced kvp.Value with
                    | Some output when policy.PartialFlush ->
                        stateTable.TryRemove(kvp.Key) |> ignore
                        recordMetrics "ttl_flush" kvp.Key
                        // Emit would be handled by the main processing loop
                    | _ -> ()
        }
        
    interface Stage<'In, 'Out> with
        member _.Id = accumulator.Id
        member _.Kind = StageKind.Accumulator
        
        member _.Init(ctx) =
            context <- ctx
            accumulator.Init(ctx)
            
        member _.Process(ct, input) =
            // Start periodic flush timer
            timerTask <- Async.StartAsTask(periodicFlush(), TaskCreationOptions.LongRunning, timerCts.Token)
            
            { new IAsyncEnumerable<Envelope<'Out>> with
                member _.GetAsyncEnumerator(ct2) =
                    let enumerator = input.GetAsyncEnumerator(ct2)
                    let mutable current = Unchecked.defaultof<_>
                    
                    { new IAsyncEnumerator<Envelope<'Out>> with
                        member _.Current = current
                        
                        member _.MoveNextAsync() =
                            task {
                                let mutable foundOutput = false
                                let mutable continueProcessing = true
                                
                                while continueProcessing && not foundOutput do
                                    let! hasNext = enumerator.MoveNextAsync()
                                    if hasNext then
                                        let! result = processEnvelope enumerator.Current |> Async.StartAsTask
                                        match result with
                                        | Some output ->
                                            current <- output
                                            foundOutput <- true
                                        | None -> ()
                                    else
                                        // Input exhausted, flush remaining states if partial flush enabled
                                        if policy.PartialFlush then
                                            let remaining = stateTable.ToArray()
                                            if remaining.Length > 0 then
                                                let kvp = remaining.[0]
                                                match accumulator.FlushForced kvp.Value with
                                                | Some output ->
                                                    stateTable.TryRemove(kvp.Key) |> ignore
                                                    current <- emitComplete output kvp.Value kvp.Value.CompletenessScore
                                                    foundOutput <- true
                                                | None -> ()
                                            else
                                                continueProcessing <- false
                                        else
                                            continueProcessing <- false
                                            
                                return foundOutput
                            } |> ValueTask<_>
                            
                        member _.DisposeAsync() =
                            enumerator.DisposeAsync()
                    }
            }
            
        member _.Close() =
            timerCts.Cancel()
            try
                timerTask.Wait(5000) |> ignore
            with _ -> ()
            
            timerCts.Dispose()
            accumulator.Close()

// Example accumulator for template variable completion
type TemplateVariableAccumulator(requiredVars: string Set) =
    
    type VariableState = {
        CollectedVars: Map<string, obj>
        RequiredVars: string Set
    }
    
    interface Accumulator<Map<string,obj>, Map<string,obj>, string, VariableState> with
        member _.Id = "template-variable-accumulator"
        
        member _.Init(_) = ValueTask.CompletedTask
        
        member _.ExtractKey(envelope) =
            match envelope.Attrs.TryGetValue("subjectId") with
            | true, value -> value :?> string
            | false, _ -> "default"
            
        member _.UpdateState(currentState, envelope) =
            let state = 
                match currentState with
                | Some s -> s.State
                | None -> { CollectedVars = Map.empty; RequiredVars = requiredVars }
                
            let newVars = 
                envelope.Payload 
                |> Map.fold (fun acc k v -> Map.add k v acc) state.CollectedVars
                
            let newState = { state with CollectedVars = newVars }
            let completenessScore = 
                let collected = Set.ofList (Map.keys newVars |> Seq.toList)
                let intersection = Set.intersect collected requiredVars
                float intersection.Count / float requiredVars.Count
                
            match currentState with
            | Some s ->
                { s with 
                    State = newState
                    SizeItems = s.SizeItems + 1
                    SizeBytes = s.SizeBytes + 100 // Approximate
                    LastTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    CompletenessScore = completenessScore }
            | None ->
                { Key = envelope.Attrs.["subjectId"] :?> string
                  State = newState
                  SizeItems = 1
                  SizeBytes = 100
                  FirstTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                  LastTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                  CompletenessScore = completenessScore }
                  
        member _.CheckComplete(state) =
            let collected = Set.ofList (Map.keys state.State.CollectedVars |> Seq.toList)
            if Set.isSubset requiredVars collected then
                Complete(state.State.CollectedVars, state.State)
            else
                Incomplete state.State
                
        member _.FlushForced(state) =
            if state.State.CollectedVars.IsEmpty then
                None
            else
                Some state.State.CollectedVars
                
        member _.SerializeSpill(state) =
            // Simple JSON serialization for example
            let json = 
                state.State.CollectedVars 
                |> Map.toList
                |> List.map (fun (k, v) -> sprintf "\"%s\":\"%O\"" k v)
                |> String.concat ","
                |> sprintf "{%s}"
            System.Text.Encoding.UTF8.GetBytes(json) |> ReadOnlyMemory
            
        member _.DeserializeSpill(data) =
            // Simplified deserialization
            let json = System.Text.Encoding.UTF8.GetString(data.Span)
            // In real implementation, use proper JSON parser
            let vars = 
                json.Trim([|'{'; '}'|]).Split(',')
                |> Array.map (fun pair ->
                    let parts = pair.Split(':')
                    let key = parts.[0].Trim([|'"'|])
                    let value = parts.[1].Trim([|'"'|]) :> obj
                    key, value)
                |> Map.ofArray
                
            { Key = "deserialized"
              State = { CollectedVars = vars; RequiredVars = requiredVars }
              SizeItems = vars.Count
              SizeBytes = data.Length
              FirstTs = 0L
              LastTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
              CompletenessScore = 0.5 }
              
        member _.Close() = ValueTask.CompletedTask

// Spill manager for handling state serialization
type SpillManager(memoryPool: IMemoryPool) =
    let spilledData = ConcurrentDictionary<string, ReadOnlyMemory<byte>>()
    let compressionEnabled = true
    
    member _.SpillToDisk(key: string, data: ReadOnlyMemory<byte>) =
        // In production, write to memory-mapped file or disk
        spilledData.[key] <- data
        
    member _.LoadFromDisk(key: string) =
        match spilledData.TryGetValue(key) with
        | true, data -> Some data
        | false, _ -> None
        
    member _.Remove(key: string) =
        spilledData.TryRemove(key) |> ignore
        
    member _.GetStatistics() =
        { TotalKeys = spilledData.Count
          TotalBytes = spilledData.Values |> Seq.sumBy (fun d -> int64 d.Length)
          CompressionRatio = if compressionEnabled then 0.7 else 1.0 }
          
    member _.Clear() =
        spilledData.Clear()

// Completeness evaluators
module CompletenessEvaluators =
    
    // Bitset-based required variable checker
    type BitsetVariableChecker(variableNames: string array) =
        let varIndexMap = variableNames |> Array.mapi (fun i name -> name, i) |> Map.ofArray
        let requiredCount = variableNames.Length
        
        member _.UpdateBitset(current: uint64, varName: string) =
            match Map.tryFind varName varIndexMap with
            | Some index -> current ||| (1UL <<< index)
            | None -> current
            
        member _.IsComplete(bitset: uint64) =
            let allBits = (1UL <<< requiredCount) - 1UL
            (bitset &&& allBits) = allBits
            
        member _.GetProgress(bitset: uint64) =
            let setBits = System.Numerics.BitOperations.PopCount(bitset)
            float setBits / float requiredCount
            
        member _.GetMissingVars(bitset: uint64) =
            variableNames
            |> Array.mapi (fun i name -> 
                if (bitset &&& (1UL <<< i)) = 0UL then Some name else None)
            |> Array.choose id

    // Count-based completeness
    type CountBasedChecker(targetCount: int) =
        member _.IsComplete(currentCount: int) = currentCount >= targetCount
        member _.GetProgress(currentCount: int) = float currentCount / float targetCount
        
    // Size-based completeness
    type SizeBasedChecker(targetBytes: int) =
        member _.IsComplete(currentBytes: int) = currentBytes >= targetBytes
        member _.GetProgress(currentBytes: int) = float currentBytes / float targetBytes
        
    // Hybrid completeness with multiple conditions
    type HybridChecker(conditions: (unit -> bool) list) =
        member _.IsComplete() = conditions |> List.exists (fun check -> check())
        member _.GetProgress() = 
            let completed = conditions |> List.filter (fun check -> check()) |> List.length
            float completed / float conditions.Length