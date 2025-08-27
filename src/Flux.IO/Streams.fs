namespace Flux.IO.Streams

open Flux.IO.Core.Types

type StreamProcessor<'TIn, 'TOut> = 
    | StreamProcessor of (Envelope<'TIn> -> Flow<StreamCommand<'TOut>>)

module StreamProcessor =

    /// Run a processor on a single envelope
    let runProcessor (StreamProcessor f) env = f env
    
    /// Lift a pure function into a processor
    let lift (f: 'a -> 'b) : StreamProcessor<'a, 'b> =
        StreamProcessor (fun env ->
            { Program = FSync (fun _ -> Emit (Envelope.map f env)) }
        )

    /// Filter processor
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

    /// Stateful processor
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

(*  
    Goals:
    - Backend neutrality (Task / Async / Hopac later)
    - Support for single or multi-result (finite) streams
    - Small, allocation‑light for single result case
    - Poll (pull) API – Direct path uses polling; Freer runtime can
      later translate completion into a scheduled wake-up.
*)

module Outlets =

    open System
    open System.Collections.Concurrent

    /// Classification of emission pattern (may inform planning later)
    type OutletKind =
        | SingleValue
        | FiniteStream
        | Unknown

    /// Result record used when draining outlets (keeps error distinct)
    type OutletDrainResult<'T> =
        | Drained of 'T list * isComplete: bool * error: exn option
        | Pending

    /// Abstract outlet surface
    type EffectOutlet<'T> =
        abstract Kind        : OutletKind
        abstract TryDequeue  : unit -> 'T voption      // Single item if available
        abstract IsCompleted : bool
        abstract Error       : exn option
        abstract Dispose     : unit -> unit

    /// Wrap a single EffectHandle<'T> as an outlet.
    type internal SingleValueOutlet<'T>(handle: EffectHandle<'T>) =
        let mutable completed = false
        let mutable stored : 'T option = None
        let mutable error : exn option = None
        let mutable consumedOnce = false

        interface EffectOutlet<'T> with
            member _.Kind = SingleValue
            member _.TryDequeue () =
                if consumedOnce then ValueNone
                else
                    // Poll underlying handle; capture result at first resolution
                    if not completed then
                        match handle.Poll() with
                        | EffectPending -> ValueNone
                        | EffectDone (ValueSome v) ->
                            completed <- true
                            stored <- Some v
                            ValueSome v
                        | EffectDone ValueNone ->
                            completed <- true
                            error <- Some (InvalidOperationException "Effect returned no value.")
                            ValueNone
                        | EffectFailed ex ->
                            completed <- true
                            error <- Some ex
                            ValueNone
                        | EffectCancelled ex ->
                            completed <- true
                            error <- Some ex
                            ValueNone
                    else
                        match stored with
                        | Some v when not consumedOnce ->
                            consumedOnce <- true
                            ValueSome v
                        | _ -> ValueNone
            member _.IsCompleted = completed && (stored.IsSome || error.IsSome)
            member _.Error = error
            member _.Dispose () = () // Underlying handle disposal TBD Phase 3 (wrapping ExternalHandle)

    /// Create outlet from effect handle (single)
    let ofEffectHandle (h: EffectHandle<'T>) : EffectOutlet<'T> =
        SingleValueOutlet h :> _

    /// A simple thread-safe outlet for multi-result finite streams.
    /// Producer calls `post` until `complete` or `fault`.
    type ManualStreamOutlet<'T>() =
        let queue = new ConcurrentQueue<'T>()
        let mutable completed = false
        let mutable error : exn option = None

        member _.Post (item: 'T) =
            if completed then invalidOp "Outlet already completed."
            queue.Enqueue item

        member _.Complete () =
            completed <- true

        member _.Fault (ex: exn) =
            if completed then () else
            error <- Some ex
            completed <- true

        interface EffectOutlet<'T> with
            member _.Kind = FiniteStream
            member _.TryDequeue () =
                match queue.TryDequeue() with
                | true, v -> ValueSome v
                | _ -> ValueNone
            member _.IsCompleted =
                completed && queue.IsEmpty
            member _.Error = error
            member _.Dispose () = ()

    /// Drain up to maxItems (or all currently available) – convenience for Direct runtime / processor
    let drain
            (maxItems: int option) 
            (outlet: EffectOutlet<'T>) : OutletDrainResult<'T> =
        let rec loop acc count =
            match maxItems with
            | Some m when count >= m -> List.rev acc
            | _ ->
                match outlet.TryDequeue() with
                | ValueSome v -> loop (v :: acc) (count + 1)
                | ValueNone -> List.rev acc
        let items = loop [] 0
        if items.IsEmpty && not outlet.IsCompleted && outlet.Error.IsNone then
            Pending
        else
            Drained (items, outlet.IsCompleted, outlet.Error)
