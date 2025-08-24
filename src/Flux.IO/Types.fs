namespace Flux.IO.Core.Types

open FSharp.Control
open FSharp.HashCollections
open FSharpPlus
open System.Collections.Generic
open System
open System.Threading
open System.Threading.Tasks

type Timestamp = int64

[<Struct>]
type SpanCtx = 
    { 
        TraceId: string; 
        SpanId: string 
    }


type TraceContext = 
    {
        TraceId: string
        SpanId: string
        Baggage: HashMap<string, string>
    }

type Cost = 
    {
        Bytes: int
        CpuHint: float
    }

type Envelope<'T> = { 
    Payload : 'T
    Headers : HashMap<string,string>
    SeqId   : int64
    SpanCtx : TraceContext
    Ts      : Timestamp
    Attrs   : HashMap<string,obj>
    Cost    : Cost
} with 
    static member create<'T> seqId (payload: 'T) =
        { 
            Payload = payload
            Headers = HashMap.empty
            SeqId = seqId
            SpanCtx = { TraceId = ""; SpanId = ""; Baggage = HashMap.empty }
            Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            Attrs = HashMap.empty
            Cost = { Bytes = 0; CpuHint = 0.0 }
        }

type Batch<'T> = {
    Items: Envelope<'T> array
    WindowStart: Timestamp
    WindowEnd: Timestamp
    TotalBytes: int
    TotalItems: int
}

type StageKind =
    | Source
    | Transform
    | Accumulator
    | Branch
    | Merge
    | Sink

type EffectClass =
    | EffectIO
    | EffectCPU
    | EffectTimer
    | EffectExternal of string

type AsyncToken<'a> = internal AsyncToken of obj

[<Struct>]
type AsyncResult<'a> =
    | AsyncDone      of result: 'a voption
    | AsyncFailed    of reason: exn voption
    | AsyncCancelled of reason: exn voption
    | AsyncPending

[<AbstractClass>]
type AsyncHandle<'a>(internal token: AsyncToken<'a>) =
    member __.Token = token

    /// Check if the async operation is completed
    abstract member IsCompleted : bool

    // NB: this would be a lot cleaner with higher kinded types...
    // abstract member AsTokenSource<'t> : unit -> 't option

    abstract member Poll: unit -> AsyncResult<'a>

    /// Await the completion of the async operation (blocks the caller)
    abstract member Await : unit -> AsyncResult<'a>             // Blocking await
    
    /// Await with a timeout
    abstract member AwaitTimeout : TimeSpan -> AsyncResult<'a> // Blocking await with timeout
    
    /// Cancel the async operation - returns immediately
    abstract member Cancel : unit -> unit

    abstract member CancelWait : unit -> AsyncResult<'a> // Cancel the async operation and wait for completion

    abstract member CancelWaitTimeout : TimeSpan -> AsyncResult<'a> // Cancel and wait with timeout

type AsyncState<'T> =
    | NotStarted
    | Running of CancellationTokenSource
    | Completed of AsyncResult<'T>

type AsyncMessage<'T> =
    | Start of AsyncReplyChannel<AsyncHandle<'T>>
    | Query of AsyncReplyChannel<AsyncState<'T>>
    | SetResult of AsyncResult<'T>
    | WaitForResult of TimeSpan option * AsyncReplyChannel<AsyncResult<'T>>

(* 
    Typed external (impure) operation handle.
    This is a *descriptor* + lifecycle controller. Creation does NOT start the effect.
    A backend decides when to call Start() and how to await completion.
    No backend-specific types (Task / Job / etc.) leak through this surface. 
*)
[<Interface>]
type ExternalHandle<'T> = 
    inherit IDisposable
    abstract member Id          : int
    abstract member Class       : EffectClass
    abstract member IsStarted   : bool
    abstract member IsCompleted : bool
    abstract member Start       : unit -> AsyncHandle<'T>

type ExecutionEnv =
    { log      : string -> unit
      nowUnix  : unit -> int64
      services : Map<string,obj> }

type ExternalSpec<'T> =
    { 
        Build      : ExecutionEnv -> ExternalHandle<'T>
        Classify   : EffectClass
        DebugLabel : string option 
    }

// Stream command DU (reused by pipeline processors)
type StreamCommand<'T> =
    | Emit of 'T
    | EmitMany of 'T list
    | Consume
    | Complete
    | Error of exn

// --------------------------------------------------------------------------------------
// Direct Flow Program Representation
// --------------------------------------------------------------------------------------
// We keep a lightweight representation. Instead of baking a full Freer tree we rely on
// the interpreter's continuation chaining. Only external boundaries are explicit nodes.
//
// FlowProg<'T> forms a *minimal structural* IR with NO existential storage. Each bind
// composes into a single final executable function in Flow<'T>. This keeps runtime
// overhead low, but sacrifices full introspection of intermediate binds.
// --------------------------------------------------------------------------------------
type FlowProg<'T> =
    | FPure    of (unit -> 'T)                      // Lazy pure value
    | FSync    of (ExecutionEnv -> 'T)              // Synchronous function of env
    | FExternal of ExternalSpec<'T>                 // External effect boundary
    | FDelay   of (unit -> FlowProg<'T>)            // Structural deferral
    | FTryWith of FlowProg<'T> * (exn -> FlowProg<'T>)
    | FTryFinally of FlowProg<'T> * (unit -> unit)

/// The public Flow wrapper (kept opaque to allow internal evolution).
type Flow<'T> = { prog: FlowProg<'T> }
