namespace Flux.IO.Tests

module JsonStreamProcessors =

    open System
    open System.Text
    open System.Buffers
    open Newtonsoft.Json.Linq
    open Flux.IO
    (* open Flux.IO.Core1
    open Flux.IO.Core1.Flow *)
    open Flux.IO.Core.Types
    open Flux.IO.Streams
    open Flux.IO.Pipeline.Direct

    module JsonAssembler =

        /// Status returned by assemblers that aggregate incoming byte chunks.
        type AssemblerStatus<'a> =
            | StatusNeedMore
            | StatusComplete of 'a
            | StatusError of exn

        /// Interface for any JSON assembler that converts a stream of byte chunks
        /// into a final output (initially a JToken).
        type IJsonAssembler<'a> =
            abstract Feed : ReadOnlyMemory<byte> -> AssemblerStatus<'a>
            abstract Completed : bool

        /// Length-based assembler (Tier A / legacy behaviour).
        /// Assumes caller knows the full expected length of the JSON document.
        type LengthAssembler(expectedLength: int) =
            let acc = System.Collections.Generic.List<byte>(expectedLength)
            let mutable doneFlag = false
            interface IJsonAssembler<JToken> with
                member _.Feed (chunk: ReadOnlyMemory<byte>) =
                    if doneFlag then
                        StatusComplete (JValue.CreateNull() :> JToken)
                    else
                        acc.AddRange(chunk.ToArray())
                        if acc.Count = expectedLength then
                            try
                                let json = Encoding.UTF8.GetString(acc.ToArray())
                                let tok = JToken.Parse json
                                doneFlag <- true
                                StatusComplete tok
                            with ex -> StatusError ex
                        else StatusNeedMore
                member _.Completed = doneFlag

        /// Adapter: wrap any assembler into a StreamProcessor producing a single emission.
        [<RequireQualifiedAccess>]
        module AssemblerProcessor =
            let create (assembler: IJsonAssembler<JToken>) : StreamProcessor<ReadOnlyMemory<byte>, JToken> =
                StreamProcessor (fun (env: Envelope<ReadOnlyMemory<byte>>) ->
                    flow {
                        if assembler.Completed then
                            return Complete
                        else
                            match assembler.Feed env.Payload with
                            | StatusNeedMore -> return Consume
                            | StatusComplete tok ->
                                let outE = Envelope.map (fun _ -> tok) env
                                return Emit outE
                            | StatusError ex -> return Error ex
                    })

    module JsonFramer =

        open JsonAssembler
        open System.Buffers
        open System.Text

        /// Internal state for structural framing (Tier B).
        type private FrameState = {
            Depth    : int
            InString : bool
            Escaped  : bool
            Started  : bool
            Complete : bool
        }

        [<AutoOpen>]
        module private FrameLogic =

            let initialState = { Depth = 0; InString = false; Escaped = false; Started = false; Complete = false }

            let advance (st: FrameState) (span: ReadOnlySpan<byte>) =
                let mutable s = st
                let mutable i = 0
                while i < span.Length && not s.Complete do
                    let c = char span.[i]
                    if not s.Started && Char.IsWhiteSpace c then
                        ()
                    else
                        if not s.Started then
                            s <- { s with Started = true }
                        if s.InString then
                            if s.Escaped then
                                s <- { s with Escaped = false }
                            else
                                match c with
                                | '\\' -> s <- { s with Escaped = true }
                                | '"'  -> s <- { s with InString = false }
                                | _ -> ()
                        else
                            match c with
                            | '"' -> s <- { s with InString = true }
                            | '{' -> s <- { s with Depth = s.Depth + 1 }
                            | '}' ->
                                s <- { s with Depth = s.Depth - 1 }
                                if s.Depth = 0 then s <- { s with Complete = true }
                            | '[' -> s <- { s with Depth = s.Depth + 1 }
                            | ']' ->
                                s <- { s with Depth = s.Depth - 1 }
                                if s.Depth = 0 then s <- { s with Complete = true }
                            | _ -> ()
                    i <- i + 1
                s

        /// Structural framing assembler.
        type FramingAssembler(?maxBytes: int) =
            let maxBytes = defaultArg maxBytes (32 * 1024 * 1024)
            let buffer : ArrayBufferWriter<byte> = ArrayBufferWriter<byte>()  // explicit type
            let mutable st = initialState
            let mutable doneFlag = false
            interface IJsonAssembler<JToken> with
                member _.Feed (chunk: ReadOnlyMemory<byte>) =
                    if doneFlag then StatusComplete (JValue.CreateNull() :> JToken)
                    else
                        if buffer.WrittenCount + chunk.Length > maxBytes then
                            StatusError (InvalidOperationException(sprintf "Framer size limit exceeded (%d bytes)" maxBytes))
                        else
                            buffer.Write(chunk.Span)
                            st <- advance st chunk.Span
                            if st.Complete && not st.InString && st.Depth = 0 then
                                try
                                    let span : ReadOnlySpan<byte> = buffer.WrittenSpan
                                    let json = Encoding.UTF8.GetString(span)
                                    let tok = JToken.Parse json
                                    doneFlag <- true
                                    StatusComplete tok
                                with ex -> StatusError ex
                            else
                                StatusNeedMore
                member _.Completed = doneFlag

    module JsonStreaming =
        open System.Text.Json
        open JsonAssembler
        open System.Buffers
        open System.Text

        /// Streaming assembler that scans incrementally to detect root completion
        /// using Utf8JsonReader, but only materializes the final JToken by parsing
        /// the accumulated full UTF-8 buffer once root completion has been detected.
        ///
        /// Advantages:
        ///   - Early error detection while scanning tokens
        ///   - Guarantees identical final JToken to a single full parse
        ///   - Avoids JTokenWriter incremental edge cases
        ///
        /// Semantics:
        ///   - StatusNeedMore until root structure (or primitive) fully observed
        ///   - StatusComplete with parsed JToken once root completes
        ///   - Subsequent Feed calls after completion yield StatusComplete (idempotent)
        type StreamingMaterializeAssembler(?maxBytes:int) =
            let maxBytes = defaultArg maxBytes (64 * 1024 * 1024)
            let buffer = ArrayBufferWriter<byte>()
            let mutable processed = 0
            let mutable readerState = JsonReaderState()
            let mutable rootCompleted = false
            let mutable emitted = false
            let mutable cachedToken : JToken option = None

            let isRootTerminal (r: inref<Utf8JsonReader>) =
                if r.TokenType = JsonTokenType.EndObject || r.TokenType = JsonTokenType.EndArray then
                    r.CurrentDepth = 0
                elif (r.TokenType = JsonTokenType.String
                      || r.TokenType = JsonTokenType.Number
                      || r.TokenType = JsonTokenType.True
                      || r.TokenType = JsonTokenType.False
                      || r.TokenType = JsonTokenType.Null) && r.CurrentDepth = 0 then
                    true
                else
                    false

            let tryScanRootCompletion () =
                if rootCompleted then () else
                // Slice new unread portion
                let slice = buffer.WrittenSpan.Slice(processed)
                if slice.Length = 0 then ()
                else
                    let mutable r = new Utf8JsonReader(slice, isFinalBlock=false, state=readerState)
                    try
                        let mutable doneLocal = false
                        while not doneLocal && r.Read() do
                            if isRootTerminal &r then
                                rootCompleted <- true
                                doneLocal <- true
                        processed <- processed + int r.BytesConsumed
                        readerState <- r.CurrentState
                    with ex ->
                        // On read error, surface immediately by raising status error through Feed
                        raise ex

            let finalizeParse () =
                if emitted then cachedToken.Value
                else
                    let json = Encoding.UTF8.GetString(buffer.WrittenSpan)
                    let tok = JToken.Parse json
                    cachedToken <- Some tok
                    emitted <- true
                    tok

            interface IJsonAssembler<JToken> with
                member _.Feed (chunk: ReadOnlyMemory<byte>) =
                    if emitted then
                        JsonAssembler.StatusComplete (cachedToken.Value)
                    else
                        // Capacity guard
                        if buffer.WrittenCount + chunk.Length > maxBytes then
                            JsonAssembler.StatusError (
                                InvalidOperationException(
                                    sprintf "Streaming size limit exceeded (%d bytes)" maxBytes))
                        else
                            // Append incoming bytes
                            if not chunk.IsEmpty then
                                buffer.Write(chunk.Span)
                            // Attempt to scan for root completion
                            let status =
                                try
                                    tryScanRootCompletion()
                                    if rootCompleted then
                                        try
                                            let tok = finalizeParse()
                                            JsonAssembler.StatusComplete tok
                                        with exParse ->
                                            JsonAssembler.StatusError exParse
                                    else
                                        JsonAssembler.StatusNeedMore
                                with exScan ->
                                    JsonAssembler.StatusError exScan
                            status

                member _.Completed = emitted

    module JsonStreamingInstrumentation =
        open System.Text.Json
        open JsonAssembler

        type StreamingInstrumentation =
          { DepthTrace : ResizeArray<int>
            mutable StartObj : int
            mutable EndObj : int
            mutable StartArr : int
            mutable EndArr : int
            mutable PrimitiveRoot : bool
            mutable Completed : bool
            mutable TokenCount : int }

        let createInstrumentation () =
            { DepthTrace = ResizeArray()
              StartObj = 0; EndObj = 0; StartArr = 0; EndArr = 0
              PrimitiveRoot = false; Completed = false; TokenCount = 0 }

        type InstrumentedStreamingMaterializeAssembler(?maxBytes:int, ?instr:StreamingInstrumentation) =
            let instr = defaultArg instr (createInstrumentation())
            let maxBytes = defaultArg maxBytes (64 * 1024 * 1024)
            let buffer = ArrayBufferWriter<byte>()
            let mutable processed = 0
            let mutable state = JsonReaderState()
            let writer = new JTokenWriter()
            let mutable rootCompleted = false
            let mutable rootTokenEmitted = false

            let writeToken (r: byref<Utf8JsonReader>) =
                instr.TokenCount <- instr.TokenCount + 1
                instr.DepthTrace.Add(int r.CurrentDepth)
                match r.TokenType with
                | JsonTokenType.StartObject -> instr.StartObj <- instr.StartObj + 1; writer.WriteStartObject()
                | JsonTokenType.EndObject   -> instr.EndObj <- instr.EndObj + 1; writer.WriteEndObject()
                | JsonTokenType.StartArray  -> instr.StartArr <- instr.StartArr + 1; writer.WriteStartArray()
                | JsonTokenType.EndArray    -> instr.EndArr <- instr.EndArr + 1; writer.WriteEndArray()
                | JsonTokenType.PropertyName -> writer.WritePropertyName(r.GetString())
                | JsonTokenType.String -> writer.WriteValue(r.GetString())
                | JsonTokenType.Number ->
                    let mutable l = 0L
                    if r.TryGetInt64(&l) then writer.WriteValue(l)
                    else
                        let mutable d = 0.0
                        if r.TryGetDouble(&d) then writer.WriteValue(d)
                        else writer.WriteValue(r.GetDecimal())
                | JsonTokenType.True -> writer.WriteValue(true)
                | JsonTokenType.False -> writer.WriteValue(false)
                | JsonTokenType.Null -> writer.WriteNull()
                | JsonTokenType.Comment -> ()
                | _ -> ()
                ()
            let instrumentation (i: StreamingInstrumentation) = i

            interface IJsonAssembler<JToken> with
                member _.Feed(chunk: ReadOnlyMemory<byte>) =
                    if rootTokenEmitted then
                        StatusComplete writer.Token
                    else
                        if buffer.WrittenCount + chunk.Length > maxBytes then
                            StatusError (InvalidOperationException(sprintf "Streaming size limit exceeded (%d bytes)" maxBytes))
                        else
                            if not chunk.IsEmpty then buffer.Write(chunk.Span)
                            let spanAll = buffer.WrittenSpan
                            let newSlice = spanAll.Slice(processed)
                            let mutable reader = new Utf8JsonReader(newSlice, isFinalBlock = false, state=state)
                            try
                                while reader.Read() do
                                    writeToken &reader
                                    if (reader.TokenType = JsonTokenType.EndObject || reader.TokenType = JsonTokenType.EndArray) && reader.CurrentDepth = 0 then
                                        rootCompleted <- true
                                    elif (reader.TokenType = JsonTokenType.String
                                           || reader.TokenType = JsonTokenType.Number
                                           || reader.TokenType = JsonTokenType.True
                                           || reader.TokenType = JsonTokenType.False
                                           || reader.TokenType = JsonTokenType.Null)
                                         && reader.CurrentDepth = 0
                                         && not rootCompleted
                                         && writer.Token = null then
                                        // primitive root: mark completed immediately after first token
                                        instr.PrimitiveRoot <- true
                                        rootCompleted <- true

                                processed <- processed + int reader.BytesConsumed
                                state <- reader.CurrentState
                                if rootCompleted && not rootTokenEmitted then
                                    rootTokenEmitted <- true
                                    instr.Completed <- true
                                    StatusComplete writer.Token
                                else
                                    StatusNeedMore
                            with ex ->
                                StatusError ex
                member _.Completed = rootTokenEmitted
