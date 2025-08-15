namespace AsyncPipeline.Memory

open System
open System.Buffers
open System.Collections.Concurrent
open System.Threading
open AsyncPipeline.Core

// Memory pool implementation
type SegmentPool(maxSegmentSize: int, maxPoolSize: int) =
    let pool = ArrayPool<byte>.Create(maxSegmentSize, maxPoolSize)
    let rentedSegments = ConcurrentDictionary<int, int64>()
    let mutable totalAllocated = 0L
    let mutable currentlyRented = 0L
    let mutable peakUsage = 0L
    
    member _.Rent(size: int) : ArraySegment<byte> =
        let buffer = pool.Rent(size)
        let segment = ArraySegment<byte>(buffer, 0, size)
        
        rentedSegments.[buffer.GetHashCode()] <- int64 size
        Interlocked.Add(&totalAllocated, int64 size) |> ignore
        let current = Interlocked.Add(&currentlyRented, int64 size)
        
        // Update peak usage
        let mutable oldPeak = peakUsage
        while current > oldPeak && Interlocked.CompareExchange(&peakUsage, current, oldPeak) <> oldPeak do
            oldPeak <- peakUsage
            
        segment
        
    member _.Return(segment: ArraySegment<byte>) =
        if segment.Array <> null then
            let hash = segment.Array.GetHashCode()
            match rentedSegments.TryRemove(hash) with
            | true, size ->
                pool.Return(segment.Array, clearArray = true)
                Interlocked.Add(&currentlyRented, -size) |> ignore
            | false, _ -> ()
            
    member _.GetStatistics() : MemoryPoolStats =
        {
            TotalAllocated = totalAllocated
            CurrentlyRented = currentlyRented
            PeakUsage = peakUsage
            FragmentationRatio = 
                if totalAllocated > 0L then
                    float currentlyRented / float totalAllocated
                else 0.0
        }
        
    interface IMemoryPool with
        member this.RentBuffer(size) = this.Rent(size)
        member this.ReturnBuffer(buffer) = this.Return(buffer)
        member this.GetStatistics() = this.GetStatistics()

// Batch object pool
type BatchPool<'T>(maxSize: int) =
    let pool = ConcurrentBag<Batch<'T>>()
    let mutable allocated = 0
    
    member _.Rent() : Batch<'T> =
        match pool.TryTake() with
        | true, batch -> batch
        | false, _ ->
            Interlocked.Increment(&allocated) |> ignore
            {
                Items = Array.empty
                WindowStart = 0L
                WindowEnd = 0L
                TotalBytes = 0
                TotalItems = 0
            }
            
    member _.Return(batch: Batch<'T>) =
        if allocated < maxSize then
            // Reset batch
            let reset = {
                Items = Array.empty
                WindowStart = 0L
                WindowEnd = 0L
                TotalBytes = 0
                TotalItems = 0
            }
            pool.Add(reset)
        else
            Interlocked.Decrement(&allocated) |> ignore
            
    member _.GetPoolSize() = pool.Count
    member _.GetAllocatedCount() = allocated

// Accumulator state pool
type AccumulatorStatePool<'Key, 'State>(maxSize: int) =
    let pool = ConcurrentBag<AccumulatorState<'Key, 'State>>()
    let mutable allocated = 0
    
    member _.Rent(key: 'Key, state: 'State) : AccumulatorState<'Key, 'State> =
        match pool.TryTake() with
        | true, accState ->
            // Reuse and update
            { accState with
                Key = key
                State = state
                SizeItems = 0
                SizeBytes = 0
                FirstTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                LastTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                CompletenessScore = 0.0
            }
        | false, _ ->
            Interlocked.Increment(&allocated) |> ignore
            {
                Key = key
                State = state
                SizeItems = 0
                SizeBytes = 0
                FirstTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                LastTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                CompletenessScore = 0.0
            }
            
    member _.Return(state: AccumulatorState<'Key, 'State>) =
        if allocated < maxSize then
            pool.Add(state)
        else
            Interlocked.Decrement(&allocated) |> ignore

// Spill buffer pool for serialization
type SpillBufferPool(segmentSize: int, maxSegments: int) =
    let segmentPool = SegmentPool(segmentSize, maxSegments)
    let activeBuffers = ConcurrentDictionary<Guid, ArraySegment<byte> list>()
    
    member _.Reserve(bytes: int) : Guid * ArraySegment<byte> list =
        let id = Guid.NewGuid()
        let segmentCount = (bytes + segmentSize - 1) / segmentSize
        let segments = 
            [1..segmentCount]
            |> List.map (fun i ->
                let size = if i = segmentCount then bytes - (segmentCount - 1) * segmentSize else segmentSize
                segmentPool.Rent(size)
            )
        activeBuffers.[id] <- segments
        id, segments
        
    member _.Release(id: Guid) =
        match activeBuffers.TryRemove(id) with
        | true, segments ->
            segments |> List.iter segmentPool.Return
        | false, _ -> ()
        
    member _.GetStatistics() =
        let stats = segmentPool.GetStatistics()
        {| 
            PoolStats = stats
            ActiveBuffers = activeBuffers.Count
            TotalActiveBytes = 
                activeBuffers.Values 
                |> Seq.sumBy (fun segments -> segments |> List.sumBy (fun s -> int64 s.Count))
        |}

// Memory pressure monitor
type MemoryPressureMonitor(thresholds: {| Low: int64; Medium: int64; High: int64 |}) =
    let mutable lastGC = GC.CollectionCount(2)
    let mutable lastCheck = DateTimeOffset.UtcNow
    let checkInterval = TimeSpan.FromSeconds(5.0)
    
    member _.GetPressureLevel() =
        let now = DateTimeOffset.UtcNow
        if now - lastCheck > checkInterval then
            lastCheck <- now
            let currentGC = GC.CollectionCount(2)
            if currentGC > lastGC then
                lastGC <- currentGC
                
        let totalMemory = GC.GetTotalMemory(false)
        
        if totalMemory > thresholds.High then
            MemoryPressureLevel.High
        elif totalMemory > thresholds.Medium then
            MemoryPressureLevel.Medium
        elif totalMemory > thresholds.Low then
            MemoryPressureLevel.Low
        else
            MemoryPressureLevel.Normal
            
    member _.ForceCollection() =
        GC.Collect(2, GCCollectionMode.Forced, true)
        GC.WaitForPendingFinalizers()
        GC.Collect(2, GCCollectionMode.Forced, true)
        
and MemoryPressureLevel =
    | Normal
    | Low
    | Medium
    | High

// Zero-copy buffer management
module ZeroCopy =
    
    type RefCountedBuffer(buffer: ArraySegment<byte>) =
        let mutable refCount = 1
        let sync = obj()
        
        member _.Buffer = buffer
        
        member _.AddRef() =
            lock sync (fun () ->
                refCount <- refCount + 1
            )
            
        member _.Release() =
            lock sync (fun () ->
                refCount <- refCount - 1
                refCount = 0
            )
            
        member _.RefCount = lock sync (fun () -> refCount)
        
    type BufferSlice = {
        Source: RefCountedBuffer
        Offset: int
        Length: int
    }
    
    let createSlice (source: RefCountedBuffer) (offset: int) (length: int) =
        source.AddRef()
        { Source = source; Offset = offset; Length = length }
        
    let releaseSlice (slice: BufferSlice) =
        slice.Source.Release()
        
    let getSpan (slice: BufferSlice) =
        ReadOnlySpan<byte>(slice.Source.Buffer.Array, slice.Source.Buffer.Offset + slice.Offset, slice.Length)
        
    let getMemory (slice: BufferSlice) =
        ReadOnlyMemory<byte>(slice.Source.Buffer.Array, slice.Source.Buffer.Offset + slice.Offset, slice.Length)

// Adaptive memory management
type AdaptiveMemoryManager(memoryPool: IMemoryPool, monitor: MemoryPressureMonitor) =
    let mutable currentStrategy = AllocationStrategy.Normal
    let strategyLock = obj()
    
    member _.UpdateStrategy() =
        let pressure = monitor.GetPressureLevel()
        lock strategyLock (fun () ->
            currentStrategy <-
                match pressure with
                | MemoryPressureLevel.Normal -> AllocationStrategy.Normal
                | MemoryPressureLevel.Low -> AllocationStrategy.Conservative
                | MemoryPressureLevel.Medium -> AllocationStrategy.Aggressive
                | MemoryPressureLevel.High -> AllocationStrategy.Critical
        )
        
    member _.GetAllocationSize(requested: int) =
        lock strategyLock (fun () ->
            match currentStrategy with
            | AllocationStrategy.Normal -> requested
            | AllocationStrategy.Conservative -> min requested (requested * 3 / 4)
            | AllocationStrategy.Aggressive -> min requested (requested / 2)
            | AllocationStrategy.Critical -> min requested 4096
        )
        
    member _.ShouldSpill() =
        lock strategyLock (fun () ->
            match currentStrategy with
            | AllocationStrategy.Normal -> false
            | AllocationStrategy.Conservative -> true
            | AllocationStrategy.Aggressive -> true
            | AllocationStrategy.Critical -> true
        )
        
    member _.GetSpillThreshold() =
        lock strategyLock (fun () ->
            match currentStrategy with
            | AllocationStrategy.Normal -> 1_000_000_000L // 1GB
            | AllocationStrategy.Conservative -> 500_000_000L // 500MB
            | AllocationStrategy.Aggressive -> 100_000_000L // 100MB
            | AllocationStrategy.Critical -> 10_000_000L // 10MB
        )
        
and AllocationStrategy =
    | Normal
    | Conservative
    | Aggressive
    | Critical

// Compression utilities for spill
module Compression =
    open System.IO
    open System.IO.Compression
    
    let compress (data: ReadOnlyMemory<byte>) : ReadOnlyMemory<byte> =
        use output = new MemoryStream()
        use gzip = new GZipStream(output, CompressionLevel.Fastest)
        gzip.Write(data.Span)
        gzip.Flush()
        output.ToArray() |> ReadOnlyMemory
        
    let decompress (compressed: ReadOnlyMemory<byte>) : ReadOnlyMemory<byte> =
        use input = new MemoryStream(compressed.ToArray())
        use gzip = new GZipStream(input, CompressionMode.Decompress)
        use output = new MemoryStream()
        gzip.CopyTo(output)
        output.ToArray() |> ReadOnlyMemory
        
    let tryCompress (data: ReadOnlyMemory<byte>) (threshold: int) =
        if data.Length > threshold then
            let compressed = compress data
            if compressed.Length < data.Length * 8 / 10 then // 20% compression ratio
                Some compressed
            else
                None
        else
            None

// Memory statistics collector
type MemoryStatsCollector(interval: TimeSpan) =
    let stats = ConcurrentQueue<MemorySnapshot>()
    let maxSnapshots = 1000
    let timer = new Timer((fun _ -> collect()), null, interval, interval)
    
    let collect() =
        let snapshot = {
            Timestamp = DateTimeOffset.UtcNow
            TotalMemory = GC.GetTotalMemory(false)
            Gen0Collections = GC.CollectionCount(0)
            Gen1Collections = GC.CollectionCount(1)
            Gen2Collections = GC.CollectionCount(2)
            AllocatedBytes = GC.GetAllocatedBytesForCurrentThread()
        }
        
        stats.Enqueue(snapshot)
        while stats.Count > maxSnapshots do
            stats.TryDequeue() |> ignore
            
    member _.GetRecentStats(duration: TimeSpan) =
        let cutoff = DateTimeOffset.UtcNow - duration
        stats
        |> Seq.filter (fun s -> s.Timestamp > cutoff)
        |> Seq.toArray
        
    member _.GetAverageMemory(duration: TimeSpan) =
        let recent = stats |> Seq.filter (fun s -> s.Timestamp > DateTimeOffset.UtcNow - duration)
        if Seq.isEmpty recent then
            0L
        else
            recent |> Seq.averageBy (fun s -> float s.TotalMemory) |> int64
            
    member _.Dispose() =
        timer.Dispose()
        
    interface IDisposable with
        member this.Dispose() = this.Dispose()

and MemorySnapshot = {
    Timestamp: DateTimeOffset
    TotalMemory: int64
    Gen0Collections: int
    Gen1Collections: int
    Gen2Collections: int
    AllocatedBytes: int64
}