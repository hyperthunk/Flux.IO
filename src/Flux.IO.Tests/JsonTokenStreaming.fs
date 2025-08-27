namespace Flux.IO.Tests

module JsonTokenStreaming =

    open System
    open System.Buffers
    open System.Collections.Generic
    open System.Text.Json
    open Flux.IO.Core.Types
    open Flux.IO.Pipeline.Direct
    open Flux.IO.Streams

    type TokenEvent = {
        TokenType    : JsonTokenType
        Value        : obj option
        Depth        : int
        RootTerminal : bool
    }

    module TokenStreamReader =

        type internal ReaderState = {
            Buffer        : ArrayBufferWriter<byte>
            Processed     : int
            JsonState     : JsonReaderState
            RootCompleted : bool
            Pending       : Queue<TokenEvent>
        }

        let private mkState () =
            { Buffer = ArrayBufferWriter<byte>()
              Processed = 0
              JsonState = JsonReaderState()
              RootCompleted = false
              Pending = System.Collections.Generic.Queue<TokenEvent>() }
        
        let private isRootTerminal (r: inref<Utf8JsonReader>) =
            if r.TokenType = JsonTokenType.EndObject || r.TokenType = JsonTokenType.EndArray then
                r.CurrentDepth = 0
            elif (r.TokenType = JsonTokenType.String
                  || r.TokenType = JsonTokenType.Number
                  || r.TokenType = JsonTokenType.True
                  || r.TokenType = JsonTokenType.False
                  || r.TokenType = JsonTokenType.Null)
                 && r.CurrentDepth = 0 then
                // Primitive document: single token forms the whole JSON value
                true
            else
                false

        let private tokenValue (r: inref<Utf8JsonReader>) =
            match r.TokenType with
            | JsonTokenType.PropertyName -> Some (r.GetString() :> obj)
            | JsonTokenType.String       -> Some (r.GetString() :> obj)
            | JsonTokenType.Number ->
                let mutable l = 0L
                if r.TryGetInt64(&l) then Some (box l)
                else
                    let mutable d = 0.0
                    if r.TryGetDouble(&d) then Some (box d)
                    else Some (box (r.GetDecimal()))
            | JsonTokenType.True  -> Some (box true)
            | JsonTokenType.False -> Some (box false)
            | JsonTokenType.Null  -> None
            | _ -> None

        /// Create a StreamProcessor that:
        ///  - Accepts ReadOnlyMemory<byte> chunks
        ///  - Emits at most one TokenEvent per invocation
        ///  - Completes after emitting the token that closes (or is) the root
        let create () =
            let st = ref (mkState())

            StreamProcessor (fun (env: Envelope<ReadOnlyMemory<byte>>) ->
                // Fast path: already done and no pending events
                let s0 = !st
                if s0.RootCompleted && s0.Pending.Count = 0 then
                    flow { return Complete }
                else
                    // Append new bytes if any
                    let chunk = env.Payload
                    if not chunk.IsEmpty then
                        s0.Buffer.Write(chunk.Span)

                    // Scan for new tokens only if queue empty and root not completed
                    if s0.Pending.Count = 0 && not s0.RootCompleted then
                        let unread = s0.Buffer.WrittenSpan.Slice(s0.Processed)
                        if unread.Length > 0 then
                            let mutable reader = Utf8JsonReader(unread, isFinalBlock=false, state=s0.JsonState)
                            let mutable localRootCompleted = s0.RootCompleted
                            let mutable scanError : exn option = None
                            try
                                while reader.Read() do
                                    let rootTerm = isRootTerminal &reader
                                    let ev = {
                                        TokenType    = reader.TokenType
                                        Value        = tokenValue &reader
                                        Depth        = int reader.CurrentDepth
                                        RootTerminal = rootTerm
                                    }
                                    s0.Pending.Enqueue ev
                                    if rootTerm then
                                        localRootCompleted <- true
                            with ex ->
                                scanError <- Some ex

                            match scanError with
                            | Some ex ->
                                // Do not mutate processed/jsonState on error
                                flow { return Error ex }
                            | None ->
                                // Commit reader progress
                                let consumed = int reader.BytesConsumed
                                st.Value <- { s0 with
                                                Processed     = s0.Processed + consumed
                                                JsonState     = reader.CurrentState
                                                RootCompleted = localRootCompleted }
                                // Fall through to emission below
                                let s1 = st.Value
                                let cmd =
                                    if s1.Pending.Count > 0 then
                                        let ev = s1.Pending.Dequeue()
                                        Emit (Envelope.map (fun _ -> ev) env)
                                    else if s1.RootCompleted then Complete
                                    else Consume
                                flow { return cmd }
                        else
                            // Nothing unread; decide based on current (still empty) queue
                            let s1 = st.Value
                            let cmd =
                                if s1.Pending.Count > 0 then
                                    let ev = s1.Pending.Dequeue()
                                    Emit (Envelope.map (fun _ -> ev) env)
                                else if s1.RootCompleted then Complete
                                else Consume
                            flow { return cmd }
                    else
                        // We already have pending tokens or root completed earlier
                        let s1 = st.Value
                        let cmd =
                            if s1.Pending.Count > 0 then
                                let ev = s1.Pending.Dequeue()
                                Emit (Envelope.map (fun _ -> ev) env)
                            else if s1.RootCompleted then Complete
                            else Consume
                        flow { return cmd })