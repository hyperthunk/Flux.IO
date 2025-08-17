namespace Flux.IO

open System
open System.Threading
open System.Threading.Channels
open System.Collections.Generic

type ChannelStats =
  { Capacity   : int
    Length     : int
    Dropped    : int64
    Enqueued   : int64
    Dequeued   : int64 }

type BoundedChannel<'T> =
  { Writer : ChannelWriter<'T>
    Reader : ChannelReader<'T>
    Stats  : unit -> ChannelStats }

module BackpressureChannels =

  let createBounded<'T> (capacity:int) (drop:DropPolicy) =
    let opts = BoundedChannelOptions(capacity)
    opts.FullMode <-
      match drop with
      | DropPolicy.Block     -> BoundedChannelFullMode.Wait
      | DropPolicy.DropNewest -> BoundedChannelFullMode.DropWrite
      | DropPolicy.DropOldest -> BoundedChannelFullMode.DropOldest
    let ch = Channel.CreateBounded<'T>(opts)
    let dropped = ref 0L
    let enq = ref 0L
    let deq = ref 0L
    {
      Writer =
        { new ChannelWriter<'T>() with
            member _.TryComplete(ex) = ch.Writer.TryComplete(ex)
            member _.TryWrite v =
              let ok = ch.Writer.TryWrite v
              if ok then System.Threading.Interlocked.Increment enq |> ignore
              else System.Threading.Interlocked.Increment dropped |> ignore
              ok
            member _.WriteAsync(v,ct) =
              let w = ch.Writer.WriteAsync(v,ct)
              if w.IsCompletedSuccessfully then
                if w.Result then System.Threading.Interlocked.Increment enq |> ignore
                ValueTask<bool>(w.Result)
              else
                let t = w.AsTask().ContinueWith(fun (t:Task<bool>) ->
                  if t.IsCompletedSuccessfully && t.Result then
                    System.Threading.Interlocked.Increment enq |> ignore
                  t.Result)
                ValueTask<bool>(t) }
      Reader =
        { new ChannelReader<'T>() with
            member _.Completion = ch.Reader.Completion
            member _.CanCount = ch.Reader.CanCount
            member _.Count = ch.Reader.Count
            member _.TryRead value =
              let ok = ch.Reader.TryRead value
              if ok then System.Threading.Interlocked.Increment deq |> ignore
              ok
            member _.TryPeek value = ch.Reader.TryPeek value
            member _.WaitToReadAsync(ct) = ch.Reader.WaitToReadAsync(ct)
            member _.ReadAsync(ct) =
              let op = ch.Reader.ReadAsync(ct)
              if op.IsCompletedSuccessfully then
                System.Threading.Interlocked.Increment deq |> ignore
                ValueTask<'T>(op.Result)
              else
                let t = op.AsTask().ContinueWith(fun (t:Task<'T>) ->
                  if t.IsCompletedSuccessfully then
                    System.Threading.Interlocked.Increment deq |> ignore
                  t.Result)
                ValueTask<'T>(t) }
      Stats =
        fun () ->
          { Capacity = capacity
            Length = ch.Reader.Count
            Dropped = !dropped
            Enqueued = !enq
            Dequeued = !deq }
    }
