namespace Flux.IO.Tests

open System
open FSharp.HashCollections
open Newtonsoft.Json.Linq
open Flux.IO
open Flux.IO.Core
open Flux.IO.Core.Flow

open TokenStreaming

/// Domain events re-used (AccumEvent, ProjectionEvent) from IntegrationStages for parity.
open IntegrationStages

module StreamingAccumulation =

    /// Streaming accumulation over TokenEvent sequence (root-level object scalars).
    /// Emits Batch or Done (AccumEvent) incrementally.
    /// Limitations: only handles root object or primitive root; nested not batched until root close.
    let create (threshold:int) : StreamProcessor<TokenEvent, AccumEvent> =
        let working = ref HashMap.empty<string,obj>
        let seen    = ref (HashSet.empty<string>)
        let doneRef = ref false
        let rootScalarDomain = ref (HashSet.empty<string>)
        StreamProcessor (fun (env: Envelope<TokenEvent>) ->
            flow {
                if !doneRef then
                    return Complete
                else
                    let tok = env.Payload
                    // Track property and its scalar value
                    // We need a small look-ahead; with single-event emission we cannot see value together.
                    // For simplicity: rely on sequence order: PropertyName token followed (later feed) by primitive scalar token at same depth+1.
                    // We'll accumulate scalars only when we have already stored a "pending property".
                    // Quick approach: keep a ref for a pending property.
                    let pendingProp = env.StateTryGet<string>("pendingProp")
                    match tok.TokenType, pendingProp with
                    | System.Text.Json.JsonTokenType.PropertyName, _ ->
                        env.StateSet("pendingProp", tok.Value |> Option.map string |> Option.defaultValue "")
                        return Consume
                    | (System.Text.Json.JsonTokenType.String
                       | System.Text.Json.JsonTokenType.Number
                       | System.Text.Json.JsonTokenType.True
                       | System.Text.Json.JsonTokenType.False
                       | System.Text.Json.JsonTokenType.Null), Some propName ->
                        if HashSet.contains propName !seen |> not then
                            seen := HashSet.add propName !seen
                            working := HashMap.add propName (tok.Value |> Option.toObj) !working
                        env.StateRemove("pendingProp") |> ignore
                        if HashMap.count !working >= threshold then
                            let batch = !working
                            working := HashMap.empty
                            return Emit (mapEnvelope (fun _ -> AccumEvent.Batch batch) env)
                        else
                            return Consume
                    | _, _ ->
                        // Determine domain completion
                        if tok.RootTerminal then
                            // Emit Done with union of working + seen
                            doneRef := true
                            // Combine any residual keys into a set
                            let domain =
                                !seen
                            return Emit (mapEnvelope (fun _ -> AccumEvent.Done domain) env)
                        else
                            return Consume
            })

module StreamingProjection =

    /// Emits Scalars once (all root-level scalars when encountered / root closed) and then Objects (root-level object subtrees as compact JSON).
    let create () : StreamProcessor<TokenEvent, ProjectionEvent> =
        let emittedScalars = ref false
        let emittedObjects = ref false
        let scalars = ref HashMap.empty<string,obj>
        let objectProps = ref HashMap.empty<string,string>
        let pendingProp = ref None
        let rootClosed = ref false
        StreamProcessor (fun (env: Envelope<TokenEvent>) ->
            flow {
                if !rootClosed && !emittedScalars && !emittedObjects then
                    return Complete
                else
                    let t = env.Payload
                    match t.TokenType with
                    | System.Text.Json.JsonTokenType.PropertyName ->
                        pendingProp := t.Value |> Option.map string
                        return Consume
                    | System.Text.Json.JsonTokenType.StartObject
                    | System.Text.Json.JsonTokenType.StartArray ->
                        // If depth=1 and we have a pending prop, treat as object subtree placeholder (emit only after root close)
                        match !pendingProp, t.Depth with
                        | Some name, 1 ->
                            objectProps := HashMap.add name "<object>" !objectProps
                            pendingProp := None
                        | _ -> ()
                        return Consume
                    | System.Text.Json.JsonTokenType.String
                    | System.Text.Json.JsonTokenType.Number
                    | System.Text.Json.JsonTokenType.True
                    | System.Text.Json.JsonTokenType.False
                    | System.Text.Json.JsonTokenType.Null ->
                        match !pendingProp, t.Depth with
                        | Some name, 1 ->
                            scalars := HashMap.add name (t.Value |> Option.toObj) !scalars
                            pendingProp := None
                        | _ -> ()
                        if t.RootTerminal then
                            rootClosed := true
                        // Emit Scalars first time we detect rootClosed (or earlier if policy desired)
                        if !rootClosed && not !emittedScalars then
                            emittedScalars := true
                            return Emit (mapEnvelope (fun _ -> ProjectionEvent.Scalars !scalars) env)
                        elif !rootClosed && !emittedScalars |> not then
                            emittedScalars := true
                            return Emit (mapEnvelope (fun _ -> ProjectionEvent.Scalars !scalars) env)
                        elif !rootClosed && !emittedObjects then
                            emittedObjects := true
                            return Emit (mapEnvelope (fun _ -> ProjectionEvent.Objects !objectProps) env)
                        else
                            return Consume
                    | System.Text.Json.JsonTokenType.EndObject
                    | System.Text.Json.JsonTokenType.EndArray ->
                        if t.RootTerminal then rootClosed := true
                        if !rootClosed && not !emittedScalars then
                            emittedScalars := true
                            return Emit (mapEnvelope (fun _ -> ProjectionEvent.Scalars !scalars) env)
                        elif !rootClosed && !emittedObjects then
                            return Consume
                        elif !rootClosed then
                            return Consume
                        elif !emittedObjects then
                            emittedObjects := true
                            return Emit (mapEnvelope (fun _ -> ProjectionEvent.Objects !objectProps) env)
                        else
                            return Consume
                    | _ ->
                        if t.RootTerminal then rootClosed := true
                        return Consume
            })