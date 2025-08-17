namespace Flux.IO

open System
open System.Buffers
open System.Threading

type InternalBuffer =
  { Owner  : IMemoryOwner<byte>
    Memory : Memory<byte>
    mutable Freed : bool }

type SimpleBufferPool (slabSize:int, initial:int) =
  let pool = ArrayPool<byte>.Shared
  let rented = ref 0L
  let outstanding = ref 0L
  let peak = ref 0L

  interface IBufferPool with
    member _.Rent minSize =
      let size = if minSize < slabSize then slabSize else minSize
      Interlocked.Increment(rented) |> ignore
      let arr = pool.Rent size
      Interlocked.Add(outstanding, int64 size) |> ignore
      let p = Interlocked.Read(outstanding)
      let mutable pk = Interlocked.Read(peak)
      while p > pk && Interlocked.CompareExchange(peak, p, pk) <> pk do
        pk <- Interlocked.Read(peak)
      { Memory = Memory<byte>(arr,0,size)
        Length = size
        Release = fun () ->
          if arr <> null then
            pool.Return arr
            Interlocked.Add(outstanding, - int64 size) |> ignore }

    member _.Stats () =
      { TotalRented = Interlocked.Read(rented)
        Outstanding = Interlocked.Read(outstanding)
        Peak = Interlocked.Read(peak)
        Fragmentation = 0.0 } // placeholder for future slab fragmentation calc

module BufferPool =
  let defaultPool () = SimpleBufferPool(64 * 1024, 8) :> IBufferPool