namespace Flux.IO.Tests

(*
    IntegrationStages.fs

    Purpose:
      Defines the primitive stream processing stages used to assemble
      higher-level pipelines for integration / model-based testing.

    Clarified Requirement Incorporated:
      - The "terminal node" for our tests is the ORIGIN (source) that must
        produce random JSON using JsonGenerators.
      - We NO LONGER require the final stage to emit JSON. Downstream stages
        can operate on parsed / transformed / domain-specific events.

    Design Principles:
      - Each stage has an explicit, concrete generic signature
      - No heterogeneous storage without a discriminated union at the pipeline level
      - Accumulation emits domain events (not forced JSON re-serialization)
      - Projection (KV) stage is optional and produces domain events
      - All stateful behaviour encapsulated in closure refs; model reasoning
        can still re-derive expected invariants from the origin JSON.
*)

module IntegrationStages =

    open System
    open System.Text
    open Newtonsoft.Json.Linq
    open FSharp.HashCollections
    open Flux.IO
    // open Flux.IO.Core1
    //open Flux.IO.Core1.Flow
    open Flux.IO.Core.Types
    open Flux.IO.Pipeline.Direct
    open Generators.JsonGenerators
    open Flux.IO.Streams

    /// Domain event produced by the accumulation stage.
    type AccumEvent =
        | Batch of HashMap<string,obj>    // Threshold batch snapshot
        | Done  of HashSet<string>        // All scalar keys discovered (terminal event)

    /// Domain event produced by the projection (KV extraction) stage.
    type ProjectionEvent =
        | Scalars of HashMap<string,obj>
        | Objects of HashMap<string,string>

    [<RequireQualifiedAccess>]
    module JsonSource =

        /// Build a source processor that emits exactly one JSON string produced
        /// by the supplied generator thunk, then completes.
        let create (jsonThunk: unit -> string) : StreamProcessor<unit,string> =
            let emitted = ref false
            StreamProcessor (fun (env: Envelope<unit>) ->
                flow {
                    if not !emitted then
                        emitted := true
                        let json = jsonThunk()
                        let e = Envelope.create env.SeqId json
                        return Emit e
                    else
                        return Complete
                })

    [<RequireQualifiedAccess>]
    module ToBytes =
        /// Convert JSON string to a single ReadOnlyMemory<byte> envelope.
        /// (If you need fragmentation, follow with a Chunker.)
        let create () : StreamProcessor<string, ReadOnlyMemory<byte>> =
            StreamProcessor (fun (env: Envelope<string>) ->
                flow {
                    let bytes = Encoding.UTF8.GetBytes env.Payload
                    let rom = ReadOnlyMemory<byte>(bytes)
                    return Emit (Envelope.map (fun _ -> rom) env)
                })

    [<RequireQualifiedAccess>]
    module Chunker =
        /// Takes a real array of bytes and emits slices (chunks) one at a time.
        /// The *input* to this stage is unit — the chunk array is closed over.
        /// Use this after converting the source JSON to bytes if you want multi-chunk flows.
        let fromByteArray (chunkSizeGen: int -> int) (bytes: byte[]) : StreamProcessor<unit, ReadOnlyMemory<byte>> =
            // Pre-slice the array for deterministic iteration (no per-step RNG inside)
            let slices =
                let acc = ResizeArray<ReadOnlyMemory<byte>>()
                let mutable idx = 0
                while idx < bytes.Length do
                    let remaining = bytes.Length - idx
                    let desired = chunkSizeGen remaining |> max 1 |> min remaining
                    let slice = ReadOnlyMemory<byte>(bytes, idx, desired)
                    acc.Add slice
                    idx <- idx + desired
                acc.ToArray()

            let cursor = ref 0
            StreamProcessor (fun (_env: Envelope<unit>) ->
                flow {
                    let i = !cursor
                    if i < slices.Length then
                        cursor := i + 1
                        let outEnv = Envelope.create (int64 i) slices.[i]
                        return Emit outEnv
                    else
                        return Complete
                })

    [<RequireQualifiedAccess>]
    module Parser =
        /// Parser that expects *all* bytes to flow through this processor in chunks.
        /// It accumulates until total length matches expectedLength, then emits a JToken exactly once.
        let chunkReassembly (expectedLength: int) : StreamProcessor<ReadOnlyMemory<byte>, JToken> =
            let acc = System.Collections.Generic.List<byte>(expectedLength)
            let doneFlag = ref false
            StreamProcessor (fun (env: Envelope<ReadOnlyMemory<byte>>) ->
                flow {
                    if not doneFlag.Value then
                        (* let span = env.Payload.Span
                        for i = 0 to span.Length - 1 do
                            acc.Add span.[i] *)
                        env.Payload.ToArray() |> acc.AddRange
                        if acc.Count = expectedLength then
                            let raw = acc.ToArray()
                            let json = Encoding.UTF8.GetString raw
                            let token = JToken.Parse json
                            doneFlag.Value <- true
                            return Emit (Envelope.map (fun _ -> token) env)
                        else
                            return Consume
                    else
                        return Complete
                })

        /// Direct parser (use when skipping chunking) – parses each unique string exactly once.
        let direct () : StreamProcessor<string, JToken> =
            StreamProcessor (fun (env: Envelope<string>) ->
                flow {
                    let token = JToken.Parse env.Payload
                    return Emit (Envelope.map (fun _ -> token) env)
                })

    [<RequireQualifiedAccess>]
    module Accumulation =
        (*
            Accumulation Strategy:
            - We scan ROOT-LEVEL scalar properties of a JObject.
            - We maintain a working map until threshold reached:
                -> Emit Batch(workMap); reset working map.
            - When all scalar properties have been discovered, emit Done(fullScalarKeyDomain).
            - For non-object JSON (arrays / primitives) we emit Done with a single pseudo "value" key.

            The stage processes *the entire root token* each invocation, but only
            appends unseen keys (tracked via a mutable set closure).
        *)

        let create (threshold: int) : StreamProcessor<JToken, AccumEvent> =
            let mutable working = HashMap.empty<string,obj>
            let mutable seen = HashSet.empty<string>
            let mutable doneFlag = false
            let mutable finalDomain = HashSet.empty<string>

            let captureScalars (root: JToken) =
                match root with
                | :? JObject as o ->
                    o.Properties()
                    |> Seq.choose (fun p ->
                        if p.Value :? JObject then None
                        else Some (p.Name, p.Value))
                    |> Seq.toList
                | other ->
                    // degrade gracefully: treat entire thing as single scalar "value"
                    ["value", other]

            StreamProcessor (fun (env: Envelope<JToken>) ->
                flow {
                    if doneFlag then
                        return Complete
                    else
                        let scalars = captureScalars env.Payload
                        // incorporate new keys
                        let mutable batchOut : HashMap<string,obj> option = None
                        scalars
                        |> List.iter (fun (k,v) ->
                            if not (HashSet.contains k seen) then
                                seen <- HashSet.add k seen
                                working <- HashMap.add k (box (v.ToString())) working
                                if HashMap.count working >= threshold then
                                    batchOut <- Some working
                                    working <- HashMap.empty)
                        
                        // Determine if all properties discovered
                        let totalScalarCount = scalars |> List.map fst |> Set.ofList |> Set.count
                        let discoveredCount = HashSet.count seen
                        match batchOut with
                        | Some b ->
                            return Emit (Envelope.map (fun _ -> Batch b) env)
                        | None when discoveredCount = totalScalarCount ->
                            doneFlag <- true
                            let domain = seen
                            finalDomain <- domain
                            return Emit (Envelope.map (fun _ -> Done domain) env)
                        | None ->
                            return Consume
                })

    [<RequireQualifiedAccess>]
    module Projection =
        /// Projects JToken to scalar/object KV maps; emits separate events OR a single Scalars event only.
        /// For simplicity we emit Scalars first; a second call (same token) will return Consume.
        let scalarsAndObjects () : StreamProcessor<JToken, ProjectionEvent> =
            let emittedScalars = ref false
            let emittedObjects = ref false
            StreamProcessor (fun (env: Envelope<JToken>) ->
                flow {
                    match env.Payload with
                    | :? JObject as o ->
                        if not !emittedScalars then
                            emittedScalars := true
                            let scalars =
                                o.Properties()
                                |> Seq.choose (fun p ->
                                    if p.Value :? JObject then None
                                    else Some (p.Name, box (p.Value.ToString())))
                                |> Seq.fold (fun acc (k,v) -> HashMap.add k v acc) HashMap.empty
                            return Emit (Envelope.map (fun _ -> Scalars scalars) env)
                        elif not !emittedObjects then
                            emittedObjects := true
                            let objs =
                                o.Properties()
                                |> Seq.choose (fun p ->
                                    match p.Value with
                                    | :? JObject as jo -> Some (p.Name, jo.ToString(Newtonsoft.Json.Formatting.None))
                                    | _ -> None)
                                |> Seq.fold (fun acc (k,v) -> HashMap.add k v acc) HashMap.empty
                            return Emit (Envelope.map (fun _ -> Objects objs) env)
                        else
                            return Complete
                    | _ ->
                        // Non-object: treat as scalar-only once
                        if not !emittedScalars then
                            emittedScalars := true
                            let map = HashMap.add "value" (box (env.Payload.ToString())) HashMap.empty
                            return Emit (Envelope.map (fun _ -> Scalars map) env)
                        else
                            return Complete
                })

    [<RequireQualifiedAccess>]
    module Terminal =
        /// A sink-like terminal that just records completion; always consumes input.
        let consume<'a> () : StreamProcessor<'a, unit> =
            StreamProcessor (fun (_env: Envelope<'a>) ->
                flow {
                    // Could record metrics / invariants here.
                    return Consume
                })

        /// A terminal that *completes* when first envelope arrives (useful as a sentinel).
        let completeOnFirst<'a> () : StreamProcessor<'a, unit> =
            let seen = ref false
            StreamProcessor (fun (_env: Envelope<'a>) ->
                flow {
                    if not !seen then
                        seen := true
                        return Complete
                    else
                        return Complete
                })