namespace Flux.IO.Core.Types

open FSharp.Control
open FSharp.HashCollections
open System
open System.Collections
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

[<CustomEquality; CustomComparison>]
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

    member inline e.WithAttr (k: string, v: Attribute) =
            { e with Attrs = e.Attrs |> FSharp.HashCollections.HashMap.add k v }

    member inline e.WithAttrs (entries: (string * Attribute) seq) =
        let merged =
            entries
            |> Seq.fold (fun acc (k,v) -> FSharp.HashCollections.HashMap.add k v acc) e.Attrs
        { e with Attrs = merged }

    override this.GetHashCode() =
        this.SeqId.GetHashCode() ^^^ 
        this.Ts.GetHashCode() ^^^ 
        this.SpanCtx.GetHashCode() ^^^
        HashIdentity.Reference.GetHashCode (box this.Payload)

    interface IEquatable<Envelope<'T>> with
        member this.Equals(other: Envelope<'T>) =
            this.SeqId = other.SeqId &&
            this.Ts = other.Ts &&
            this.SpanCtx = other.SpanCtx &&
            Object.Equals(this.Payload, other.Payload)

    interface IStructuralComparable with
        member this.CompareTo(other: obj, comparer: IComparer) =
            match other with
            | :? Envelope<'T> as env ->
                let c = compare this.SeqId env.SeqId
                if c <> 0 then c else
                let c = compare this.Ts env.Ts
                if c <> 0 then c else
                let c = compare this.SpanCtx.SpanId env.SpanCtx.SpanId
                if c <> 0 then c else
                let c = compare this.SpanCtx.TraceId env.SpanCtx.TraceId
                if c <> 0 then c else
                comparer.Compare(box this.Payload, box env.Payload)
            | _ -> 0

    member this.EqualTo(other: Envelope<'T>) =
        (this :> IEquatable<Envelope<'T>>).Equals other

    override this.Equals (obj: obj): bool = 
        let other = 
            match obj with
            | :? Envelope<'T> as env -> Some env
            | _ -> None
        Option.map this.EqualTo other
        |> Option.defaultValue false

module Envelope =
    open System.Collections.Generic

    [<Literal>] 
    let ForkLatency    = "fork.latencyMs"
    [<Literal>] 
    let ForkId         = "fork.effectId"
    [<Literal>] 
    let OutletBatch    = "outlet.batchCount"
    [<Literal>] 
    let OutletKind     = "outlet.kind"
    [<Literal>] 
    let OutletError    = "outlet.error"
    [<Literal>] 
    let OutletComplete = "outlet.complete"

    let EnvelopeComparer<'T> =
        HashIdentity.FromFunctions
            (fun (x: Envelope<'T>) -> x.GetHashCode())
            (fun (x: Envelope<'T>) (y: Envelope<'T>) -> (x :> IEquatable<Envelope<'T>>).Equals y)

    type EnvelopeComparerT<'T> =
        struct end
        interface IEqualityComparer<Envelope<'T>> with
            member this.GetHashCode (x: Envelope<'T>) = EnvelopeComparer.GetHashCode x
            member this.Equals (x: Envelope<'T>, y: Envelope<'T>) = EnvelopeComparer.Equals (x, y)

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

(* 
type EquinoxClass =
    | Equinox
    | Propulsion
*)

type EffectClass =
    | EffectIO
    | EffectCPU
    | EffectTimer
    (* built-in effect classes *)
    | EffectAsync
    | EffectTask
    | EffectHopac
    | EffectTPL
    | EffectExternal of string
    (* | EffectEquinox of EquinoxClass *)

type BackendToken<'a> = BackendToken of obj

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

    member this.LeftOrDefault def =
        match this with
        | Left l -> l
        | Right _ -> def
        | Neither -> def

    member this.RightOrDefault def =
        match this with
        | Right r -> r
        | Left _  -> def
        | Neither -> def

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
    | EffectOutput    of result: 'a voption
    | EffectFailed    of reason: exn
    | EffectCancelled of reason: exn
    | EffectEnded
    | EffectPending with 
        
        member this.Result = 
            match this with
            | EffectOutput (ValueSome result) -> Left result
            | EffectEnded -> Neither
            | EffectOutput ValueNone -> Neither
            | EffectFailed ex -> Right ex
            | EffectCancelled ex -> Right ex
            | EffectPending -> raise (InvalidOperationException "Effect is still pending")

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

type WaitableResult<'a> = 
    | Result of EffectResult<'a>
    | WaitResult of (unit -> EffectResult<'a>)

    member this.IsCompleted =
        match this with
        | Result r -> r.IsCompleted
        | WaitResult _ -> false

    member this.Force() =
        match this with
        | Result r -> r
        | WaitResult f -> f ()

module EffectResult =
    let inline map ([<InlineIfLambda>] mapping) result =
        match result with
        | EffectOutput (ValueSome x) -> EffectOutput (ValueSome (mapping x))
        | _ -> result

    let inline mapResult ([<InlineIfLambda>] mapping) result =
        Either.map mapping result

    let inline mapError ([<InlineIfLambda>] mapping) result =
        match result with
        | EffectFailed e -> EffectFailed (mapping e)
        | _ -> result

    let inline bind ([<InlineIfLambda>] binder) result =
        map binder result
    
    let inline pure' x = EffectOutput (ValueSome x)

    let ret = pure'

    let inline unwrap (result: EffectResult<'a>) : 'a =
        match result with
        | EffectOutput (ValueSome x) -> x
        | _ -> raise (InvalidOperationException "Effect does not produce a value")

[<AbstractClass>]
type EffectHandle<'a>(internal token: BackendToken<'a>) =
    member __.Token = token

    /// Check if the async operation is completed
    abstract member IsCompleted : bool

    /// Poll for the result of the async operation (non-blocking)
    /// NOTE: the semantics of polling may differ between backends.
    /// In some implementations, non-blocking reads may lead to data
    /// loss in the case of timeout.
    abstract member Poll: unit -> WaitableResult<'a>

    /// Await the completion of the async operation (blocks the caller).
    /// Implementations of this method should not lead to data loss.
    abstract member Await : unit -> EffectResult<'a>
    
    /// Await with a timeout.
    /// NOTE: the semantics of timeouts may differ between backends.
    /// In some implementations, non-blocking reads may lead to data
    /// loss in the case of timeout.
    abstract member AwaitTimeout : TimeSpan -> WaitableResult<'a>
    
    /// Cancel the async operation - returns immediately
    abstract member Cancel : unit -> unit

    /// Cancel the async operation and wait for completion
    abstract member CancelWait : unit -> EffectResult<'a> 

    /// Cancel and wait with timeout
    abstract member CancelWaitTimeout : TimeSpan -> WaitableResult<'a>

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
[<CustomEquality; CustomComparison>]
type StreamCommand<'T> =
    | Emit of Envelope<'T>
    | EmitMany of Envelope<'T> list
    | Consume
    | Complete
    | Error of exn 
    with
        override this.GetHashCode() =
            match this with
            | Emit env -> HashIdentity.Reference.GetHashCode (box env)
            | EmitMany envs -> List.map HashIdentity.Reference.GetHashCode envs |> List.fold (^^^) 0
            | Consume -> 0
            | Complete -> -1
            | Error exn -> HashIdentity.Reference.GetHashCode (box exn)

        interface IEquatable<StreamCommand<'T>> with
            member this.Equals(other: StreamCommand<'T>) =
                match this, other with
                | Emit env1, Emit env2 -> env1 = env2
                | EmitMany envs1, EmitMany envs2 -> envs1 = envs2
                | Consume, Consume -> true
                | Complete, Complete -> true
                | Error exn1, Error exn2 -> exn1 = exn2
                | _ -> false

        interface IStructuralComparable with
            member this.CompareTo(other: obj, comparer: IComparer) =
                match other with
                | :? StreamCommand<'T> as cmd ->
                    compare this cmd
                | _ -> 0

        member this.EqualTo(other: StreamCommand<'T>) =
            (this :> IEquatable<StreamCommand<'T>>).Equals other

        override this.Equals (obj: obj): bool = 
            let other = 
                match obj with
                | :? StreamCommand<'T> as cmd -> Some cmd
                | _ -> None
            Option.map this.EqualTo other
            |> Option.defaultValue false

module StreamCommand =
    open System.Collections.Generic

    let StreamCommandComparer<'T> =
        HashIdentity.FromFunctions
            (fun (x: StreamCommand<'T>) -> x.GetHashCode())
            (fun (x: StreamCommand<'T>) (y: StreamCommand<'T>) -> (x :> IEquatable<StreamCommand<'T>>).Equals y)

    type StreamCommandComparerT<'T> =
        struct end
        interface IEqualityComparer<StreamCommand<'T>> with
            member this.GetHashCode (x: StreamCommand<'T>) = StreamCommandComparer.GetHashCode x
            member this.Equals (x: StreamCommand<'T>, y: StreamCommand<'T>) = StreamCommandComparer.Equals (x, y)

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

module Core = 
    open FSharpPlus

    let inline flip f x y = FSharpPlus.Operators.flip f x y

    let invalidOp msg = 
        let ex = InvalidOperationException msg
        raise ex
