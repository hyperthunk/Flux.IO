namespace Flux.IO

open System
open System.Buffers
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FSharpPlus

// ----------- Core Primitive Types -----------

[<Struct>]
type Timestamp = int64

[<Struct>]
type TraceContext =
  { TraceId : string
    SpanId  : string
    Baggage : IReadOnlyDictionary<string,string> }

[<Struct>]
type Cost =
  { Bytes   : int
    CpuHint : float }

type Envelope<'T> =
  { Payload : 'T
    Headers : Map<string,string>
    SeqId   : int64
    SpanCtx : TraceContext
    Ts      : Timestamp
    Attrs   : IReadOnlyDictionary<string,obj>
    Cost    : Cost }

module Envelope =
  let inline map f (e:Envelope<'a>) =
    { e with Payload = f e.Payload }
  let withHeader k v (e:Envelope<'T>) =
    { e with Headers = e.Headers.Add(k,v) }
  let withAttr (k:string) (v:obj) (e:Envelope<'T>) =
    e.Attrs :?> IDictionary<string,obj> |> fun d -> d[k] <- v
    e
  let create seqId payload =
    { Payload = payload
      Headers = Map.empty
      SeqId   = seqId
      SpanCtx = { TraceId=""; SpanId=""; Baggage=Dictionary<string,string>() :> _ }
      Ts      = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
      Attrs   = Dictionary<string,obj>() :> _
      Cost    = { Bytes = 0; CpuHint = 0.0 } }

type StageId = string

[<Struct>]
type StageKind =
  | Source | Transform | Accumulator | Branch | Merge | Sink

type StageContext =
  { PipelineId : string
    StageId    : StageId
    Logger     : ILogger
    Tracer     : ITracer
    Cancellation : CancellationToken }

and ILogger =
  abstract Log      : level:string -> message:string -> unit
  abstract LogError : message:string -> exn:exn -> unit

and ITracer =
  abstract StartSpan : name:string -> parent:TraceContext option -> TraceContext
  abstract EndSpan   : ctx:TraceContext -> unit
  abstract AddEvent  : ctx:TraceContext -> name:string -> attrs:Map<string,obj> -> unit

type StreamIn<'T>  = IAsyncEnumerable<Envelope<'T>>
type StreamOut<'T> = IAsyncEnumerable<Envelope<'T>>

// ----------- Flow Monad (Effect Context) -----------

type ExecutionEnv =
  { Metrics  : IMetrics
    Tracer   : ITracer
    Logger   : ILogger
    Memory   : IBufferPool }

type Flow<'a> = internal Flow of (ExecutionEnv -> CancellationToken -> ValueTask<'a>)

module Flow =
  let inline run env ct (Flow f) = f env ct
  let inline ret x = Flow (fun _ _ -> ValueTask<_>(x))
  let inline bind (f:'a -> Flow<'b>) (Flow m) =
    Flow (fun env ct ->
      let v = m env ct
      if v.IsCompletedSuccessfully then
        let (Flow g) = f v.Result
        g env ct
      else
        let tcs = TaskCompletionSource<'b>(TaskCreationOptions.RunContinuationsAsynchronously)
        let aw = v.AsTask()
        aw.ContinueWith(fun (t:Task<'a>) ->
          if t.IsFaulted then tcs.TrySetException(t.Exception.InnerExceptions) |> ignore
          elif t.IsCanceled then tcs.TrySetCanceled() |> ignore
          else
            let (Flow g) = f t.Result
            let gv = g env ct
            if gv.IsCompletedSuccessfully then tcs.TrySetResult(gv.Result) |> ignore
            else gv.AsTask().ContinueWith(fun (tt:Task<'b>) ->
                   if tt.IsFaulted then tcs.TrySetException(tt.Exception.InnerExceptions) |> ignore
                   elif tt.IsCanceled then tcs.TrySetCanceled() |> ignore
                   else tcs.TrySetResult(tt.Result) |> ignore
                 ) |> ignore
        ) |> ignore
        ValueTask<_>(tcs.Task))

  type FlowBuilder() =
    member _.Return x = ret x
    member _.Bind (m,f) = bind f m
    member _.Zero () = ret ()
    member _.Delay f = bind id (ret ()) |> ignore; f()
  let flow = FlowBuilder()

  let map f m = bind (f >> ret) m
  let inline liftTask (t:Task<'a>) =
    Flow (fun _ _ ->
      if t.IsCompletedSuccessfully then ValueTask<_>(t.Result)
      else ValueTask<_>(t))

// ----------- Backpressure ------------

type DropPolicy = DropOldest | DropNewest | Block

type BackpressureStrategy =
  | Bounded of capacity:int * drop:DropPolicy
  | CreditBased of initial:int * replenish:int
  | AdaptiveBatch of minBatch:int * maxBatch:int
  | RateShaping of ratePerSec:int * burst:int
  | ReactivePull of window:int

// ----------- Metrics -----------

type IMetrics =
  abstract Counter   : name:string * tags:Map<string,string> * value:int64 -> unit
  abstract Gauge     : name:string * tags:Map<string,string> * value:float -> unit
  abstract Histogram : name:string * tags:Map<string,string> * value:float -> unit

// ----------- Memory Pool / Buffer -----------

type RentedBuffer =
  { Memory : Memory<byte>
    Length : int
    Release : unit -> unit }

type IBufferPool =
  abstract Rent  : minSize:int -> RentedBuffer
  abstract Stats : unit -> PoolStats

and PoolStats =
  { TotalRented : int64
    Outstanding : int64
    Peak        : int64
    Fragmentation : float }

// ----------- Error & Policies -----------

type RetryPolicy =
  { MaxRetries : int
    Backoff    : TimeSpan }

type ErrorPolicy =
  | Retry of RetryPolicy
  | DeadLetter
  | FailFast
  | Ignore

type CircuitBreakerPolicy =
  { ErrorThreshold : float
    LatencyThreshold : TimeSpan
    Window : TimeSpan
    MinThroughput : int
    Cooldown : TimeSpan }

// ----------- Accumulation -----------

type CompletenessResult<'State,'Out> =
  | Incomplete of 'State
  | Complete of 'Out * 'State
  | Expired of 'Out option * 'State

type AccumulatorPolicy =
  { MaxItems        : int option
    MaxBytes        : int option
    MaxLatency      : TimeSpan option
    CompletenessTtl : TimeSpan option
    PartialFlush    : bool
    SpillAllowed    : bool }

type AccumulatorState<'Key,'State> =
  { Key : 'Key
    State : 'State
    SizeItems : int
    SizeBytes : int
    FirstTs : Timestamp
    LastTs  : Timestamp
    CompletenessScore : float }

type Accumulator<'In,'Out,'Key,'State> =
  abstract Id             : StageId
  abstract Init           : StageContext -> ValueTask
  abstract ExtractKey     : Envelope<'In> -> 'Key
  abstract UpdateState    : AccumulatorState<'Key,'State> option -> Envelope<'In> -> AccumulatorState<'Key,'State>
  abstract CheckComplete  : AccumulatorState<'Key,'State> -> CompletenessResult<'State,'Out>
  abstract FlushForced    : AccumulatorState<'Key,'State> -> 'Out option
  abstract SerializeSpill : AccumulatorState<'Key,'State> -> ReadOnlyMemory<byte>
  abstract DeserializeSpill : ReadOnlyMemory<byte> -> AccumulatorState<'Key,'State>
  abstract Close          : unit -> ValueTask

// ----------- Stage Interface -----------

type Stage<'In,'Out> =
  abstract Id      : StageId
  abstract Kind    : StageKind
  abstract Init    : StageContext -> ValueTask
  abstract Process : ct:CancellationToken -> StreamIn<'In> -> StreamOut<'Out>
  abstract Close   : unit -> ValueTask

// ----------- Pipeline Shape (Typed) -----------

type Pipeline<'In,'Out> =
  | Terminal of Stage<'In,'Out>
  | Chain of Stage<'In,'Mid> * Pipeline<'Mid,'Out>

module Pipeline =
  let rec mapIds f = function
    | Terminal s -> Terminal s
    | Chain (s,rest) -> Chain (s, mapIds f rest)

  let inline singleton (s:Stage<'a,'b>) = Terminal s
  let inline append (s:Stage<'a,'b>) (p:Pipeline<'b,'c>) : Pipeline<'a,'c> = Chain (s,p)

  // Compose adjacent Transform stages that are pure (placeholder purity detection)
  let rec fuse (isPure:Stage<'i,'o> -> bool) (pipe:Pipeline<'a,'b>) : Pipeline<'a,'b> =
    match pipe with
    | Terminal s -> Terminal s
    | Chain (s1, Chain (s2, rest)) when s1.Kind = Transform && s2.Kind = Transform && isPure s1 && isPure s2 ->
        // FusedStage for demonstration (no obj usage; generic closure composition)
        let fused =
          { new Stage<'x,'z> with
              member _.Id = s1.Id + "+" + s2.Id
              member _.Kind = Transform
              member _.Init ctx =
                task {
                  do! s1.Init ctx
                  do! s2.Init ctx
                } |> ValueTask
              member _.Process ct input =
                let mid = s1.Process ct input
                s2.Process ct mid
              member _.Close () =
                task {
                  do! s2.Close().AsTask()
                  do! s1.Close().AsTask()
                } |> ValueTask
          } :> Stage<'a,'c> // types unify through generic constraint
          |> fun x -> x :?> Stage<'a,'c>
        Chain (fused, fuse isPure rest)
    | Chain (s,rest) -> Chain (s, fuse isPure rest)
