namespace Flux.IO

open System
open System.Threading.Tasks

type CircuitState = Closed | HalfOpen | Open

type StageMetricsSnapshot =
  { StageId : StageId
    ThroughputPerSec : float
    InputDepth : int
    OutputDepth : int
    ErrorRate : float
    AvgLatencyMs : float }

type Observer =
  abstract StageStart    : StageId * StageKind -> unit
  abstract StageComplete : StageId * int64 -> unit
  abstract OnError       : StageId * exn -> unit
  abstract OnCircuit     : StageId * CircuitState -> unit
  abstract OnAccEvent    : StageId * string * Map<string,obj> -> unit

type NoopObserver() =
  interface Observer with
    member _.StageStart(_,_) = ()
    member _.StageComplete(_,_) = ()
    member _.OnError(_,_) = ()
    member _.OnCircuit(_,_) = ()
    member _.OnAccEvent(_,_,_) = ()
