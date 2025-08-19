namespace Flux.IO.Tests

module JsonStreamProcessors =

    open System
    open System.Text
    open System.Buffers
    open Newtonsoft.Json.Linq
    open Flux.IO
    open Flux.IO.Core
    open Flux.IO.Core.Flow

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
                    Flow.flow {
                        if assembler.Completed then
                            return Complete
                        else
                            match assembler.Feed env.Payload with
                            | StatusNeedMore -> return Consume
                            | StatusComplete tok ->
                                let outE = mapEnvelope (fun _ -> tok) env
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

        type StreamingMaterializeAssembler(?maxBytes:int) =
            let maxBytes = defaultArg maxBytes (64 * 1024 * 1024)
            let buffer = ArrayBufferWriter<byte>()
            let mutable processed = 0
            let mutable state = JsonReaderState()
            let writer = new JTokenWriter()
            let mutable rootCompleted = false
            let mutable rootTokenEmitted = false
            let mutable sawRootToken = false  // set after first root token (primitive or start container)

            let writeToken (r: byref<Utf8JsonReader>) =
                match r.TokenType with
                | JsonTokenType.StartObject -> sawRootToken <- true; writer.WriteStartObject()
                | JsonTokenType.EndObject   -> writer.WriteEndObject()
                | JsonTokenType.StartArray  -> sawRootToken <- true; writer.WriteStartArray()
                | JsonTokenType.EndArray    -> writer.WriteEndArray()
                | JsonTokenType.PropertyName -> writer.WritePropertyName(r.GetString())
                | JsonTokenType.String -> 
                    if not sawRootToken then sawRootToken <- true
                    writer.WriteValue(r.GetString())
                | JsonTokenType.Number ->
                    if not sawRootToken then sawRootToken <- true
                    let mutable l = 0L
                    if r.TryGetInt64(&l) then writer.WriteValue(l)
                    else
                        let mutable d = 0.0
                        if r.TryGetDouble(&d) then writer.WriteValue(d)
                        else writer.WriteValue(r.GetDecimal())
                | JsonTokenType.True  -> if not sawRootToken then sawRootToken <- true; writer.WriteValue(true)
                | JsonTokenType.False -> if not sawRootToken then sawRootToken <- true; writer.WriteValue(false)
                | JsonTokenType.Null  -> if not sawRootToken then sawRootToken <- true; writer.WriteNull()
                | JsonTokenType.Comment
                | JsonTokenType.None -> ()
                | _ -> ()
                ()

            interface IJsonAssembler<JToken> with
                member _.Feed (chunk: ReadOnlyMemory<byte>) =
                    if rootTokenEmitted then
                        StatusComplete writer.Token
                    else
                        if buffer.WrittenCount + chunk.Length > maxBytes then
                            StatusError (InvalidOperationException(sprintf "Streaming size limit exceeded (%d bytes)" maxBytes))
                        else
                            if not chunk.IsEmpty then buffer.Write(chunk.Span)

                            // Slice of new data
                            let spanAll = buffer.WrittenSpan
                            let newSlice = spanAll.Slice(processed)
                            let mutable reader = new Utf8JsonReader(newSlice, isFinalBlock = false, state=state)
                            try
                                while reader.Read() do
                                    writeToken &reader
                                    // Detect root completion:
                                    if (reader.TokenType = JsonTokenType.EndObject || reader.TokenType = JsonTokenType.EndArray) && reader.CurrentDepth = 0 then
                                        rootCompleted <- true
                                    elif (reader.TokenType = JsonTokenType.String
                                           || reader.TokenType = JsonTokenType.Number
                                           || reader.TokenType = JsonTokenType.True
                                           || reader.TokenType = JsonTokenType.False
                                           || reader.TokenType = JsonTokenType.Null)
                                         && reader.CurrentDepth = 0
                                         && not rootCompleted
                                         && sawRootToken
                                         then
                                        // Primitive top-level root completed immediately
                                        rootCompleted <- true

                                processed <- processed + int reader.BytesConsumed
                                state <- reader.CurrentState

                                if rootCompleted && not rootTokenEmitted then
                                    // Finalize and ensure writer.Token is materialized
                                    writer.Close()
                                    match writer.Token with
                                    | null ->
                                        // Token not yet available; wait for more data (unlikely except partial multi-byte boundary)
                                        StatusNeedMore
                                    | tok ->
                                        rootTokenEmitted <- true
                                        StatusComplete tok
                                else
                                    StatusNeedMore
                            with ex ->
                                StatusError ex

                member _.Completed = rootTokenEmitted

    module JsonStreamingInstrumentation =
        open System
        open System.Buffers
        open System.Text.Json
        open Newtonsoft.Json.Linq
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
