namespace AsyncPipeline.IO

open System
open System.Buffers
open System.Collections.Generic
open System.IO
open System.IO.Pipelines
open System.Threading
open System.Threading.Tasks
open AsyncPipeline.Core

// Non-blocking I/O abstractions
type ISource<'T> =
    abstract member ReadAsync: ct:CancellationToken -> ValueTask<Envelope<'T> option>
    abstract member ReadBatchAsync: maxItems:int -> ct:CancellationToken -> ValueTask<Envelope<'T> array>
    abstract member Dispose: unit -> ValueTask

type ISink<'T> =
    abstract member WriteAsync: item:Envelope<'T> -> ct:CancellationToken -> ValueTask
    abstract member WriteBatchAsync: items:Envelope<'T> array -> ct:CancellationToken -> ValueTask
    abstract member FlushAsync: ct:CancellationToken -> ValueTask
    abstract member Dispose: unit -> ValueTask

// Pull-based source
type PullSource<'T>(generator: CancellationToken -> ValueTask<'T option>) =
    let mutable seqId = 0L
    
    interface ISource<'T> with
        member _.ReadAsync(ct) = 
            task {
                let! value = generator ct
                return value |> Option.map (fun payload ->
                    let id = Interlocked.Increment(&seqId)
                    {
                        Payload = payload
                        Headers = Map.empty
                        SeqId = id
                        SpanCtx = { TraceId = ""; SpanId = ""; Baggage = Dictionary<_,_>() :> IReadOnlyDictionary<_,_> }
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                        Attrs = Dictionary<_,_>() :> IReadOnlyDictionary<_,_>
                        Cost = { Bytes = 0; CpuHint = 0.0 }
                    })
            } |> ValueTask<_>
            
        member this.ReadBatchAsync(maxItems, ct) =
            task {
                let items = ResizeArray<_>(maxItems)
                let mutable cont = true
                let mutable count = 0
                
                while cont && count < maxItems do
                    let! item = (this :> ISource<_>).ReadAsync(ct)
                    match item with
                    | Some env ->
                        items.Add(env)
                        count <- count + 1
                    | None ->
                        cont <- false
                        
                return items.ToArray()
            } |> ValueTask<_>
            
        member _.Dispose() = ValueTask.CompletedTask

// Push-based source
type PushSource<'T>() =
    let buffer = System.Threading.Channels.Channel.CreateUnbounded<Envelope<'T>>()
    
    member _.Push(item: 'T) =
        let envelope = {
            Payload = item
            Headers = Map.empty
            SeqId = 0L
            SpanCtx = { TraceId = ""; SpanId = ""; Baggage = Dictionary<_,_>() :> IReadOnlyDictionary<_,_> }
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            Attrs = Dictionary<_,_>() :> IReadOnlyDictionary<_,_>
            Cost = { Bytes = 0; CpuHint = 0.0 }
        }
        buffer.Writer.TryWrite(envelope) |> ignore
        
    member _.Complete() =
        buffer.Writer.Complete()
        
    interface ISource<'T> with
        member _.ReadAsync(ct) =
            task {
                let! hasValue = buffer.Reader.WaitToReadAsync(ct)
                if hasValue then
                    let mutable item = Unchecked.defaultof<_>
                    if buffer.Reader.TryRead(&item) then
                        return Some item
                    else
                        return None
                else
                    return None
            } |> ValueTask<_>
            
        member this.ReadBatchAsync(maxItems, ct) =
            task {
                let items = ResizeArray<_>(maxItems)
                let mutable count = 0
                
                while count < maxItems && buffer.Reader.TryRead(&items) do
                    count <- count + 1
                    
                if items.Count = 0 then
                    let! item = (this :> ISource<_>).ReadAsync(ct)
                    match item with
                    | Some env -> items.Add(env)
                    | None -> ()
                    
                return items.ToArray()
            } |> ValueTask<_>
            
        member _.Dispose() = 
            buffer.Writer.TryComplete() |> ignore
            ValueTask.CompletedTask

// System.IO.Pipelines integration
type PipelineSource(pipe: PipeReader, delimiter: byte array option) =
    let mutable seqId = 0L
    
    interface ISource<ReadOnlyMemory<byte>> with
        member _.ReadAsync(ct) =
            task {
                let! result = pipe.ReadAsync(ct)
                let buffer = result.Buffer
                
                if buffer.IsEmpty && result.IsCompleted then
                    return None
                else
                    let consumed, examined =
                        match delimiter with
                        | Some delim ->
                            // Try to find delimiter
                            let mutable position = buffer.Start
                            let mutable found = false
                            let mutable data = ReadOnlyMemory<byte>.Empty
                            
                            while not found && not (buffer.End.Equals(position)) do
                                let span = buffer.Slice(position)
                                match span.PositionOf(ReadOnlySpan<byte>(delim)) with
                                | Nullable position ->
                                    let slice = buffer.Slice(buffer.Start, position)
                                    data <- slice.ToArray() |> ReadOnlyMemory
                                    found <- true
                                | _ ->
                                    position <- buffer.GetPosition(1L, position)
                                    
                            if found then
                                buffer.GetPosition(int64 delim.Length, position), position
                            else
                                buffer.Start, buffer.End
                        | None ->
                            // Read all available
                            let data = buffer.ToArray() |> ReadOnlyMemory
                            buffer.End, buffer.End
                            
                    pipe.AdvanceTo(consumed, examined)
                    
                    let id = Interlocked.Increment(&seqId)
                    Some {
                        Payload = consumed |> buffer.Slice |> (fun s -> s.ToArray()) |> ReadOnlyMemory
                        Headers = Map.empty
                        SeqId = id
                        SpanCtx = { TraceId = ""; SpanId = ""; Baggage = Dictionary<_,_>() :> IReadOnlyDictionary<_,_> }
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                        Attrs = Dictionary<_,_>() :> IReadOnlyDictionary<_,_>
                        Cost = { Bytes = consumed |> buffer.Slice |> (fun s -> int s.Length); CpuHint = 0.0 }
                    }
            } |> ValueTask<_>
            
        member this.ReadBatchAsync(maxItems, ct) =
            task {
                let items = ResizeArray<_>(maxItems)
                let mutable count = 0
                
                while count < maxItems do
                    let! item = (this :> ISource<_>).ReadAsync(ct)
                    match item with
                    | Some env ->
                        items.Add(env)
                        count <- count + 1
                    | None ->
                        count <- maxItems // Exit loop
                        
                return items.ToArray()
            } |> ValueTask<_>
            
        member _.Dispose() =
            pipe.Complete()
            ValueTask.CompletedTask

// Buffer sink
type BufferSink<'T>() =
    let items = ResizeArray<Envelope<'T>>()
    let sync = obj()
    
    member _.GetItems() = 
        lock sync (fun () -> items.ToArray())
        
    member _.Clear() =
        lock sync (fun () -> items.Clear())
        
    interface ISink<'T> with
        member _.WriteAsync(item, _) =
            lock sync (fun () -> items.Add(item))
            ValueTask.CompletedTask
            
        member _.WriteBatchAsync(batch, _) =
            lock sync (fun () -> items.AddRange(batch))
            ValueTask.CompletedTask
            
        member _.FlushAsync(_) = ValueTask.CompletedTask
        member _.Dispose() = ValueTask.CompletedTask

// Pipeline writer sink
type PipelineSink(pipe: PipeWriter, flushThreshold: int) =
    let mutable bytesWritten = 0
    
    interface ISink<ReadOnlyMemory<byte>> with
        member _.WriteAsync(item, ct) =
            task {
                let data = item.Payload
                let memory = pipe.GetMemory(data.Length)
                data.CopyTo(memory)
                pipe.Advance(data.Length)
                
                bytesWritten <- bytesWritten + data.Length
                if bytesWritten >= flushThreshold then
                    let! _ = pipe.FlushAsync(ct)
                    bytesWritten <- 0
            } |> ValueTask
            
        member this.WriteBatchAsync(items, ct) =
            task {
                for item in items do
                    do! (this :> ISink<_>).WriteAsync(item, ct)
            } |> ValueTask
            
        member _.FlushAsync(ct) =
            task {
                let! _ = pipe.FlushAsync(ct)
                bytesWritten <- 0
            } |> ValueTask
            
        member _.Dispose() =
            pipe.Complete()
            ValueTask.CompletedTask

// Duplex channel for bidirectional communication
type DuplexChannel<'TIn, 'TOut>() =
    let inbound = System.Threading.Channels.Channel.CreateUnbounded<Envelope<'TIn>>()
    let outbound = System.Threading.Channels.Channel.CreateUnbounded<Envelope<'TOut>>()
    
    member _.GetSource() = 
        { new ISource<'TIn> with
            member _.ReadAsync(ct) =
                task {
                    let! hasValue = inbound.Reader.WaitToReadAsync(ct)
                    if hasValue then
                        let mutable item = Unchecked.defaultof<_>
                        if inbound.Reader.TryRead(&item) then
                            return Some item
                        else
                            return None
                    else
                        return None
                } |> ValueTask<_>
                
            member _.ReadBatchAsync(maxItems, ct) =
                task {
                    let items = ResizeArray<_>(maxItems)
                    let mutable count = 0
                    
                    while count < maxItems && inbound.Reader.TryRead(&items) do
                        count <- count + 1
                        
                    return items.ToArray()
                } |> ValueTask<_>
                
            member _.Dispose() = ValueTask.CompletedTask
        }
        
    member _.GetSink() =
        { new ISink<'TOut> with
            member _.WriteAsync(item, _) =
                outbound.Writer.TryWrite(item) |> ignore
                ValueTask.CompletedTask
                
            member _.WriteBatchAsync(items, _) =
                for item in items do
                    outbound.Writer.TryWrite(item) |> ignore
                ValueTask.CompletedTask
                
            member _.FlushAsync(_) = ValueTask.CompletedTask
            member _.Dispose() = ValueTask.CompletedTask
        }
        
    member _.SendInbound(item: Envelope<'TIn>) =
        inbound.Writer.TryWrite(item)
        
    member _.ReceiveOutbound() =
        let mutable item = Unchecked.defaultof<_>
        if outbound.Reader.TryRead(&item) then
            Some item
        else
            None
            
    member _.Complete() =
        inbound.Writer.Complete()
        outbound.Writer.Complete()

// Stream adapters
module StreamAdapters =
    
    // Convert ISource to IAsyncEnumerable
    let sourceToStream<'T> (source: ISource<'T>) : IAsyncEnumerable<Envelope<'T>> =
        { new IAsyncEnumerable<Envelope<'T>> with
            member _.GetAsyncEnumerator(ct) =
                let mutable current = None
                { new IAsyncEnumerator<Envelope<'T>> with
                    member _.Current = current.Value
                    member _.MoveNextAsync() =
                        task {
                            let! item = source.ReadAsync(ct)
                            current <- item
                            return item.IsSome
                        } |> ValueTask<_>
                    member _.DisposeAsync() = source.Dispose()
                }
        }
    
    // Convert IAsyncEnumerable to ISink
    let streamToSink<'T> (stream: IAsyncEnumerable<Envelope<'T>>) : ISink<'T> =
        { new ISink<'T> with
            member _.WriteAsync(item, ct) = ValueTask.CompletedTask
            member _.WriteBatchAsync(items, ct) = ValueTask.CompletedTask
            member _.FlushAsync(ct) = ValueTask.CompletedTask
            member _.Dispose() = ValueTask.CompletedTask
        }
    
    // Create a bounded channel between stages
    let createBoundedChannel<'T> (capacity: int) =
        let channel = System.Threading.Channels.Channel.CreateBounded<Envelope<'T>>(
            new System.Threading.Channels.BoundedChannelOptions(capacity,
                FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait))
        
        let source = 
            { new ISource<'T> with
                member _.ReadAsync(ct) =
                    task {
                        let! hasValue = channel.Reader.WaitToReadAsync(ct)
                        if hasValue then
                            let mutable item = Unchecked.defaultof<_>
                            if channel.Reader.TryRead(&item) then
                                return Some item
                            else
                                return None
                        else
                            return None
                    } |> ValueTask<_>
                    
                member _.ReadBatchAsync(maxItems, ct) =
                    task {
                        let items = ResizeArray<_>(maxItems)
                        let mutable count = 0
                        
                        while count < maxItems do
                            let! hasValue = channel.Reader.WaitToReadAsync(ct)
                            if hasValue then
                                let mutable item = Unchecked.defaultof<_>
                                if channel.Reader.TryRead(&item) then
                                    items.Add(item)
                                    count <- count + 1
                                else
                                    count <- maxItems
                            else
                                count <- maxItems
                                
                        return items.ToArray()
                    } |> ValueTask<_>
                    
                member _.Dispose() = ValueTask.CompletedTask
            }
            
        let sink =
            { new ISink<'T> with
                member _.WriteAsync(item, ct) =
                    channel.Writer.WriteAsync(item, ct)
                    
                member _.WriteBatchAsync(items, ct) =
                    task {
                        for item in items do
                            do! channel.Writer.WriteAsync(item, ct)
                    } |> ValueTask
                    
                member _.FlushAsync(_) = ValueTask.CompletedTask
                member _.Dispose() = 
                    channel.Writer.Complete()
                    ValueTask.CompletedTask
            }
            
        source, sink