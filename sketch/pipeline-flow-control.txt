namespace AsyncPipeline.FlowControl

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open AsyncPipeline.Core

// Backpressure implementations
type BackpressureController =
    abstract member ShouldAccept: queueDepth:int -> pendingBytes:int64 -> bool
    abstract member OnAccepted: itemSize:int -> unit
    abstract member OnProcessed: itemSize:int -> unit
    abstract member GetMetrics: unit -> Map<string, float>

// Bounded buffer backpressure
type BoundedBufferController(capacity: int, dropPolicy: DropPolicy) =
    let mutable currentCount = 0
    let mutable droppedCount = 0L
    
    interface BackpressureController with
        member _.ShouldAccept(queueDepth, _) =
            match dropPolicy with
            | Block -> queueDepth < capacity
            | DropOldest | DropNewest -> true
            
        member _.OnAccepted(itemSize) =
            Interlocked.Increment(&currentCount) |> ignore
            
        member _.OnProcessed(itemSize) =
            Interlocked.Decrement(&currentCount) |> ignore
            
        member _.GetMetrics() =
            Map.ofList [
                ("current_count", float currentCount)
                ("dropped_count", float droppedCount)
                ("capacity", float capacity)
                ("utilization", float currentCount / float capacity)
            ]

// Credit-based flow control
type CreditBasedController(initialCredits: int, replenishThreshold: int) =
    let mutable credits = initialCredits
    let creditLock = obj()
    let creditAvailable = new ManualResetEventSlim(true)
    let mutable totalRequested = 0L
    let mutable totalGranted = 0L
    
    interface BackpressureController with
        member _.ShouldAccept(_, pendingBytes) =
            lock creditLock (fun () ->
                if credits > 0 then
                    credits <- credits - 1
                    Interlocked.Increment(&totalGranted) |> ignore
                    if credits = 0 then
                        creditAvailable.Reset()
                    true
                else
                    Interlocked.Increment(&totalRequested) |> ignore
                    false
            )
            
        member _.OnAccepted(_) = ()
        
        member _.OnProcessed(_) =
            lock creditLock (fun () ->
                credits <- credits + 1
                if credits >= replenishThreshold && not creditAvailable.IsSet then
                    creditAvailable.Set()
            )
            
        member _.GetMetrics() =
            Map.ofList [
                ("available_credits", float credits)
                ("total_requested", float totalRequested)
                ("total_granted", float totalGranted)
                ("wait_ratio", if totalRequested > 0L then float (totalRequested - totalGranted) / float totalRequested else 0.0)
            ]
            
    member _.WaitForCredit(timeout: TimeSpan) =
        creditAvailable.Wait(timeout)

// Rate shaping with token bucket
type RateShapingController(rate: int, burst: int) =
    let tokensPerMs = float rate / 1000.0
    let mutable tokens = float burst
    let mutable lastRefill = DateTimeOffset.UtcNow
    let tokenLock = obj()
    
    let refillTokens () =
        let now = DateTimeOffset.UtcNow
        let elapsed = (now - lastRefill).TotalMilliseconds
        tokens <- min (float burst) (tokens + elapsed * tokensPerMs)
        lastRefill <- now
        
    interface BackpressureController with
        member _.ShouldAccept(_, _) =
            lock tokenLock (fun () ->
                refillTokens()
                if tokens >= 1.0 then
                    tokens <- tokens - 1.0
                    true
                else
                    false
            )
            
        member _.OnAccepted(_) = ()
        member _.OnProcessed(_) = ()
        
        member _.GetMetrics() =
            lock tokenLock (fun () ->
                refillTokens()
                Map.ofList [
                    ("available_tokens", tokens)
                    ("max_burst", float burst)
                    ("rate_per_second", float rate)
                    ("fill_ratio", tokens / float burst)
                ]
            )

// Adaptive batch size controller
type AdaptiveBatchController(minBatch: int, maxBatch: int, targetLatencyMs: float) =
    let mutable currentBatchSize = (minBatch + maxBatch) / 2
    let mutable lastAdjustment = DateTimeOffset.UtcNow
    let adjustmentInterval = TimeSpan.FromSeconds(5.0)
    let latencyWindow = ConcurrentQueue<float>()
    let maxWindowSize = 100
    
    let adjustBatchSize (avgLatency: float) (queueDepth: int) =
        let now = DateTimeOffset.UtcNow
        if now - lastAdjustment > adjustmentInterval then
            lastAdjustment <- now
            
            if avgLatency < targetLatencyMs * 0.7 && queueDepth > 10 then
                // Increase batch size
                currentBatchSize <- min maxBatch (int (float currentBatchSize * 1.5))
            elif avgLatency > targetLatencyMs || queueDepth < 5 then
                // Decrease batch size
                currentBatchSize <- max minBatch (currentBatchSize / 2)
                
    member _.RecordLatency(latencyMs: float) =
        latencyWindow.Enqueue(latencyMs)
        while latencyWindow.Count > maxWindowSize do
            latencyWindow.TryDequeue() |> ignore
            
    member _.GetCurrentBatchSize() = currentBatchSize
    
    member _.UpdateBatchSize(queueDepth: int) =
        if latencyWindow.Count > 0 then
            let latencies = latencyWindow.ToArray()
            let avgLatency = Array.average latencies
            adjustBatchSize avgLatency queueDepth
            
    interface BackpressureController with
        member this.ShouldAccept(queueDepth, _) =
            this.UpdateBatchSize(queueDepth)
            true
            
        member _.OnAccepted(_) = ()
        member _.OnProcessed(_) = ()
        
        member _.GetMetrics() =
            let latencies = latencyWindow.ToArray()
            let avgLatency = if latencies.Length > 0 then Array.average latencies else 0.0
            Map.ofList [
                ("current_batch_size", float currentBatchSize)
                ("average_latency_ms", avgLatency)
                ("target_latency_ms", targetLatencyMs)
                ("min_batch", float minBatch)
                ("max_batch", float maxBatch)
            ]

// Reactive pull controller
type ReactivePullController(windowSize: int) =
    let mutable outstandingRequests = 0
    let pullRequests = Channel.CreateUnbounded<int>()
    
    member _.RequestItems(count: int) =
        pullRequests.Writer.TryWrite(count) |> ignore
        
    member _.GetPullRequests() = pullRequests.Reader
    
    interface BackpressureController with
        member _.ShouldAccept(_, _) =
            Interlocked.Increment(&outstandingRequests) <= windowSize
            
        member _.OnAccepted(_) = ()
        
        member _.OnProcessed(_) =
            let newCount = Interlocked.Decrement(&outstandingRequests)
            if newCount < windowSize / 2 then
                pullRequests.Writer.TryWrite(windowSize - newCount) |> ignore
                
        member _.GetMetrics() =
            Map.ofList [
                ("outstanding_requests", float outstandingRequests)
                ("window_size", float windowSize)
                ("window_utilization", float outstandingRequests / float windowSize)
            ]

// Circuit breaker implementation
type CircuitState =
    | Closed
    | Open of since:DateTimeOffset
    | HalfOpen

type CircuitBreaker(policy: CircuitBreakerPolicy, metrics: IMetrics) =
    let mutable state = Closed
    let stateLock = obj()
    let window = ResizeArray<bool * DateTimeOffset>() // success, timestamp
    let mutable consecutiveFailures = 0
    let mutable lastStateChange = DateTimeOffset.UtcNow
    
    let cleanWindow () =
        let cutoff = DateTimeOffset.UtcNow - policy.WindowSize
        window.RemoveAll(fun (_, ts) -> ts < cutoff) |> ignore
        
    let calculateMetrics () =
        cleanWindow()
        let total = window.Count
        let failures = window |> Seq.filter (fun (success, _) -> not success) |> Seq.length
        let errorRate = if total > 0 then float failures / float total else 0.0
        total, errorRate
        
    let checkThresholds () =
        let total, errorRate = calculateMetrics()
        total >= policy.MinimumThroughput && errorRate > policy.ErrorThreshold
        
    let changeState newState =
        let oldState = state
        state <- newState
        lastStateChange <- DateTimeOffset.UtcNow
        
        let tags = Map.ofList [("circuit", "main"); ("transition", sprintf "%A->%A" oldState newState)]
        metrics.RecordCounter("circuit_state_changes", tags, 1L)
        
    member _.RecordResult(success: bool, latency: TimeSpan) =
        lock stateLock (fun () ->
            window.Add((success, DateTimeOffset.UtcNow))
            
            match state with
            | Closed ->
                if not success then
                    consecutiveFailures <- consecutiveFailures + 1
                    if checkThresholds() || latency > policy.LatencyThreshold then
                        changeState (Open DateTimeOffset.UtcNow)
                else
                    consecutiveFailures <- 0
                    
            | Open since ->
                if DateTimeOffset.UtcNow - since > policy.CooldownPeriod then
                    changeState HalfOpen
                    
            | HalfOpen ->
                if success then
                    consecutiveFailures <- 0
                    changeState Closed
                else
                    changeState (Open DateTimeOffset.UtcNow)
        )
        
    member _.CanExecute() =
        lock stateLock (fun () ->
            match state with
            | Closed -> true
            | Open since -> 
                if DateTimeOffset.UtcNow - since > policy.CooldownPeriod then
                    changeState HalfOpen
                    true
                else
                    false
            | HalfOpen -> true
        )
        
    member _.GetState() = state
    
    member _.GetMetrics() =
        let total, errorRate = calculateMetrics()
        Map.ofList [
            ("state", match state with Closed -> 0.0 | Open _ -> 1.0 | HalfOpen -> 0.5)
            ("error_rate", errorRate)
            ("window_size", float total)
            ("consecutive_failures", float consecutiveFailures)
        ]

// Accumulator-aware backpressure
type AccumulatorAwareBackpressure(
    baseController: BackpressureController,
    bytesPerUnitWindow: int,
    spillPenaltyFactor: float) =
    
    let mutable pendingAccumulatorBytes = 0L
    let mutable activeSpillCount = 0
    
    member _.UpdateAccumulatorMetrics(pendingBytes: int64, spillCount: int) =
        Interlocked.Exchange(&pendingAccumulatorBytes, pendingBytes) |> ignore
        Interlocked.Exchange(&activeSpillCount, spillCount) |> ignore
        
    interface BackpressureController with
        member _.ShouldAccept(queueDepth, pendingBytes) =
            let effectiveQueue = 
                queueDepth + 
                int (pendingAccumulatorBytes / int64 bytesPerUnitWindow) +
                int (float activeSpillCount * spillPenaltyFactor)
                
            baseController.ShouldAccept(effectiveQueue, pendingBytes)
            
        member _.OnAccepted(itemSize) =
            baseController.OnAccepted(itemSize)
            
        member _.OnProcessed(itemSize) =
            baseController.OnProcessed(itemSize)
            
        member _.GetMetrics() =
            let baseMetrics = baseController.GetMetrics()
            baseMetrics
            |> Map.add "pending_accumulator_bytes" (float pendingAccumulatorBytes)
            |> Map.add "active_spill_count" (float activeSpillCount)
            |> Map.add "effective_pressure" (float pendingAccumulatorBytes / float bytesPerUnitWindow + float activeSpillCount * spillPenaltyFactor)

// Backpressure strategy factory
module BackpressureFactory =
    
    let create (strategy: BackpressureStrategy) : BackpressureController =
        match strategy with
        | Bounded(capacity, dropPolicy) ->
            BoundedBufferController(capacity, dropPolicy) :> BackpressureController
            
        | CreditBased(initial, replenish) ->
            CreditBasedController(initial, replenish) :> BackpressureController
            
        | RateShaping(rate, burst) ->
            RateShapingController(rate, burst) :> BackpressureController
            
        | Adaptive(minBatch, maxBatch) ->
            AdaptiveBatchController(minBatch, maxBatch, 100.0) :> BackpressureController
            
        | ReactivePull windowSize ->
            ReactivePullController(windowSize) :> BackpressureController
            
    let createAccumulatorAware (strategy: BackpressureStrategy) : AccumulatorAwareBackpressure =
        let baseController = create strategy
        AccumulatorAwareBackpressure(baseController, 1024, 2.0)