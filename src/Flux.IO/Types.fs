namespace Flux.IO.Core.Types

open FSharp.Control
open FSharp.HashCollections
open System
open System.Threading

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

type Attribute =
    | AttrString of string
    | AttrInt64 of int64
    | AttrUInt64 of uint64
    | AttrInt32 of int32
    | AttrUInt32 of uint32
    | AttrFloat of float
    | AttrBool of bool
    | AttrList of Attribute list
    | AttrObj of obj

type Envelope<'T> = { 
    Payload : 'T
    Headers : HashMap<string,string>
    SeqId   : int64
    SpanCtx : TraceContext
    Ts      : Timestamp
    Attrs   : HashMap<string, Attribute>
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

    static member map f (env: Envelope<'T>) : Envelope<'U> =
        {
            Payload = f env.Payload
            Headers = env.Headers
            SeqId = env.SeqId
            SpanCtx = env.SpanCtx
            Ts = env.Ts
            Attrs = env.Attrs
            Cost = env.Cost
        }
    
    static member mapHeaders f (env: Envelope<'T>) : Envelope<'T> =
        {
            env with 
                Headers = f env.Headers
        }

type Batch<'T> = 
    {
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

type BackendToken<'a> = internal BackendToken of obj

(*
    Note: on `Either<'L, 'R>` and `EffectResult<'A>` semantics.

    We allow for `EffectResult<'T>` to hold a "no-op" as its outcome, by virtue of using
    `ValueOption` semantics. This representation of the absence of a result, can be used
    to short-circuit computations across heterogeneous concurrency contexts.
*)

[<Struct>]
type Either<'L, 'R> =
    | Left  of result: 'L
    | Right of error: 'R
    | Neither with

    member this.ValueLeft =
        match this with
        | Left l  -> ValueSome l
        | Right r -> ValueNone
        | Neither -> ValueNone

    member this.ValueRight =
        match this with
        | Right r -> ValueSome r
        | Left l  -> ValueNone
        | Neither -> ValueNone

    static member map (f: 'L -> 'U) (e: Either<'L,'R>) : Either<'U,'R> =
        match e with
        | Left l -> Left (f l)
        | Right r -> Right r
        | Neither -> Neither
    
    static member mapError (f: 'R -> 'U) (e: Either<'L,'R>) : Either<'L,'U> =
        match e with
        | Left l -> Left l
        | Right r -> Right (f r)
        | Neither -> Neither
    
    static member bind (e: Either<'L,'R>, binder: 'L -> Either<'U,'R>) : Either<'U,'R> =
        match e with
        | Left l -> binder l
        | Right r -> Right r
        | Neither -> Neither

[<Struct>]
type EffectResult<'a> =
    | EffectDone      of result: 'a voption
    | EffectFailed    of reason: exn
    | EffectCancelled of reason: exn
    | EffectPending with 
        
        member this.Result = 
            match this with
            | EffectPending -> raise (InvalidOperationException "Effect is still pending")
            | EffectDone (ValueSome result) -> Left result
            | EffectDone ValueNone -> Neither
            | EffectFailed ex -> Right ex
            | EffectCancelled ex -> Right ex

        member this.IsCompleted = 
            match this with
            | EffectPending -> false
            | _ -> true
    
        member this.IsPending = not this.IsCompleted        

        member this.Failed =
            match this with
            | EffectFailed _ -> true
            | _ -> false

        member this.Succeeded = not this.Failed

module EffectResult =
    let inline map ([<InlineIfLambda>] mapping) result =
        match result with
        | EffectFailed e -> EffectFailed e
        | EffectDone (ValueSome x) -> EffectDone (ValueSome (mapping x))
        | EffectDone ValueNone -> EffectDone ValueNone
        | EffectCancelled e -> EffectCancelled e
        | EffectPending -> EffectPending

    let inline mapResult ([<InlineIfLambda>] mapping) result =
        Either.map mapping result

    let inline mapError ([<InlineIfLambda>] mapping) result =
        match result with
        | EffectFailed e -> EffectFailed (mapping e)
        | EffectDone (ValueSome x) -> EffectDone (ValueSome x)
        | EffectDone ValueNone -> EffectDone ValueNone
        | EffectCancelled e -> EffectCancelled e
        | EffectPending -> EffectPending

    let inline bind ([<InlineIfLambda>] binder) result =
        map binder result
    
    let inline pure' x = EffectDone (ValueSome x)

    let ret = pure'

    let inline unwrap (result: EffectResult<'a>) : 'a =
        match result with
        | EffectDone (ValueSome x) -> x
        | _ -> raise (InvalidOperationException "Effect does not produce a value")

[<AbstractClass>]
type EffectHandle<'a>(internal token: BackendToken<'a>) =
    member __.Token = token

    /// Check if the async operation is completed
    abstract member IsCompleted : bool

    // NB: this would be a lot cleaner with higher kinded types...
    // abstract member AsTokenSource<'t> : unit -> 't option
    abstract member Poll: unit -> EffectResult<'a>

    /// Await the completion of the async operation (blocks the caller)
    abstract member Await : unit -> EffectResult<'a>
    
    /// Await with a timeout
    abstract member AwaitTimeout : TimeSpan -> EffectResult<'a>
    
    /// Cancel the async operation - returns immediately
    abstract member Cancel : unit -> unit

    /// Cancel the async operation and wait for completion
    abstract member CancelWait : unit -> EffectResult<'a> 

    /// Cancel and wait with timeout
    abstract member CancelWaitTimeout : TimeSpan -> EffectResult<'a> 

type EffectState<'T> =
    | NotStarted
    | Running of CancellationTokenSource
    | Completed of EffectResult<'T>

type AsyncMessage<'T> =
    | Start of AsyncReplyChannel<EffectHandle<'T>>
    | Query of AsyncReplyChannel<EffectState<'T>>
    | SetResult of EffectResult<'T>
    | WaitForResult of TimeSpan option * AsyncReplyChannel<EffectResult<'T>>

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
    abstract member Start       : unit -> EffectHandle<'T>

type Metrics =
    abstract RecordCounter: name: string * tags: HashMap<string,string> * value: int64 -> unit
    abstract RecordGauge: name: string * tags: HashMap<string,string> * value: float -> unit
    abstract RecordHistogram: name: string * tags: HashMap<string,string> * value: float -> unit

[<Interface>]
type Tracer =
    abstract StartSpan: name: string -> parent: TraceContext option -> TraceContext
    abstract EndSpan: ctx: TraceContext -> unit
    abstract AddEvent: ctx: TraceContext -> name:string -> attrs:HashMap<string,obj> -> unit

[<Interface>]
type Logger =
    abstract member Log: level:string * message: string -> unit
    abstract member LogError: message: string * exn: exn -> unit

[<Interface>]
type MemoryPool =
    abstract RentBuffer: size:int -> ArraySegment<byte>
    abstract ReturnBuffer: buffer: ArraySegment<byte> -> unit

type ExecutionEnv = { 
    Metrics  : Metrics
    Tracer   : Tracer
    Logger   : Logger
    Memory   : MemoryPool
    NowUnix  : unit -> int64
    //Services : Map<string,obj> 
}

type ExternalSpec<'T> =
    { 
        Build      : ExecutionEnv -> ExternalHandle<'T>
        Classify   : EffectClass
        DebugLabel : string option 
    }

// Stream command DU (reused by pipeline processors)
type StreamCommand<'T> =
    | Emit of Envelope<'T>
    | EmitMany of Envelope<'T> list
    | Consume
    | Complete
    | Error of exn

(* 
    Direct Flow Program Representation

    We keep a lightweight representation. Instead of baking a full Freer tree we rely on
    the interpreter's continuation chaining. Only external boundaries are explicit nodes.

    FlowProg<'T> forms a *minimal structural* IR with NO existential storage. Each bind
    composes into a single final executable function in Flow<'T>. This keeps runtime
    overhead low, but sacrifices full introspection of intermediate binds. 
*)
type FlowProg<'T> =
    | FPure    of (unit -> 'T)                      // Lazy pure value
    | FSync    of (ExecutionEnv -> 'T)              // Synchronous function of env
    | FExternal of ExternalSpec<'T>                 // External effect boundary
    | FDelay   of (unit -> FlowProg<'T>)            // Structural deferral
    | FTryWith of FlowProg<'T> * (exn -> FlowProg<'T>)
    | FTryFinally of FlowProg<'T> * (unit -> unit)

// Flow wrapper (kept opaque to allow internal evolution).
type Flow<'T> = 
    { 
        Program: FlowProg<'T> 
    }

module Flow =
    let inline ret (x: 'a) : Flow<'a> = 
        { Program = FPure (fun () -> x) }

    let inline zero () : Flow<unit> = 
        { Program = FPure (fun () -> ()) }

type StreamProcessor<'TIn, 'TOut> = 
    | StreamProcessor of (Envelope<'TIn> -> Flow<StreamCommand<'TOut>>)

module StreamProcessor =

    // Run a processor on a single envelope
    let runProcessor (StreamProcessor f) env = f env
    
    // Lift a pure function into a processor
    let lift (f: 'a -> 'b) : StreamProcessor<'a, 'b> =
        StreamProcessor (fun env ->
            { Program = FSync (fun _ -> Emit (Envelope.map f env)) }
        )

    // Filter processor
    let filter (predicate: 'a -> bool) : StreamProcessor<'a, 'a> =
        StreamProcessor (fun env ->
            {
                Program = FSync (fun _ ->
                    if predicate env.Payload then
                        Emit env
                    else
                        Consume
                )
            }
        )

    // Stateful processor
    let stateful 
            (initial: 'state) 
            (f: 'state -> 'a -> ('state * 'b option)) : StreamProcessor<'a, 'b> =
        let state = ref initial
        StreamProcessor (fun env ->
            {
                Program = FSync (fun _ ->
                    let newState, result = f state.Value env.Payload
                    state.Value <- newState
                    match result with
                    | Some value -> 
                        Emit (Envelope.map (fun _ -> value) env)
                    | None -> Consume
                )
            }
        )

(*     let withEnv (f: ExecutionEnv -> 'a -> Flow<'b>) : StreamProcessor<'a, 'b> =
        StreamProcessor (fun env ->
            {
                Program = FSync (fun execEnv ->
                    let result = f execEnv env.Payload
                    Emit (Envelope.map (fun _ -> result) env)
                )
            }
        )
 *)

module Core = 
    open FSharpPlus

    let inline flip f x y = FSharpPlus.Operators.flip f x y
