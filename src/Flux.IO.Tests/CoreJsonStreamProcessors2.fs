namespace Flux.IO.Pipeline

(*
Key Refactoring Changes

Removed dependency on Core1: Now uses the Direct API types from Flux.IO.Core.Types and Flux.IO.Pipeline.Direct
Changed StreamProcessor to simple functions: Instead of a StreamProcessor type, we use functions that return Flow<StreamCommand<'T>>. This fits better with the Direct API's compositional style.
Stateful processing: The createStatefulProcessor helper manages assembler state across multiple calls, which is necessary for streaming JSON assembly.
Flow-based composition: All processors now return Flow values that can be composed using the Direct API's monadic operations.
Simplified API: The high-level Processors module provides easy-to-use functions for different JSON parsing strategies.

Usage Pattern
The refactored code follows this pattern:
fsharp// Create a processor
let processor = JsonStreamProcessors.Processors.streaming()

// Process chunks
flow {
    for chunk in chunks do
        let envelope = Envelope.create seqId chunk
        let! command = processor envelope
        match command with
        | Emit outputEnv -> // Handle parsed JSON
        | Error ex -> // Handle error
        | _ -> ()
}
Note on Pipeline Composition
The pipeline function sketch shows how processors could be composed, but it faces F#'s type system limitations around heterogeneous lists. In practice, you'd need to either:

Use a more sophisticated type encoding
Compose processors manually with explicit types
Use dynamic typing (as sketched with unbox)

The refactored code maintains all the original functionality while integrating cleanly with the Direct API's Flow monad and execution model.
*)

module JsonStreamProcessors =

    open System
    open System.Text
    open System.Buffers
    open Newtonsoft.Json.Linq
    open Flux.IO.Core.Types
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

        /// Create a Flow-based processor from an assembler
        let createProcessor (assembler: IJsonAssembler<JToken>) : Envelope<ReadOnlyMemory<byte>> -> Flow<StreamCommand<JToken>> =
            fun (env: Envelope<ReadOnlyMemory<byte>>) ->
                flow {
                    if assembler.Completed then
                        return Complete
                    else
                        match assembler.Feed env.Payload with
                        | StatusNeedMore -> return Consume
                        | StatusComplete tok ->
                            let outEnv = { env with Payload = tok }
                            return Emit outEnv
                        | StatusError ex -> return Error ex
                }

        /// Stateful processor that maintains assembler across calls
        let createStatefulProcessor (createAssembler: unit -> IJsonAssembler<JToken>) =
            let mutable assembler = createAssembler()
            fun (env: Envelope<ReadOnlyMemory<byte>>) ->
                flow {
                    if assembler.Completed then
                        // Reset for next document if needed
                        assembler <- createAssembler()
                    
                    match assembler.Feed env.Payload with
                    | StatusNeedMore -> return Consume
                    | StatusComplete tok ->
                        let outEnv = { env with Payload = tok }
                        return Emit outEnv
                    | StatusError ex -> return Error ex
                }

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
            let buffer : ArrayBufferWriter<byte> = ArrayBufferWriter<byte>()
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

        /// Streaming assembler using System.Text.Json
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
                        if buffer.WrittenCount + chunk.Length > maxBytes then
                            JsonAssembler.StatusError (
                                InvalidOperationException(
                                    sprintf "Streaming size limit exceeded (%d bytes)" maxBytes))
                        else
                            if not chunk.IsEmpty then
                                buffer.Write(chunk.Span)
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

    /// High-level Flow processors for JSON streaming
    module Processors =
        open JsonAssembler
        open JsonFramer
        open JsonStreaming

        /// Process a stream of byte chunks into JSON tokens using length-based assembly
        let lengthBased (expectedLength: int) : Envelope<ReadOnlyMemory<byte>> -> Flow<StreamCommand<JToken>> =
            createStatefulProcessor (fun () -> LengthAssembler(expectedLength) :> IJsonAssembler<JToken>)

        /// Process a stream of byte chunks into JSON tokens using structural framing
        let framing (?maxBytes: int) : Envelope<ReadOnlyMemory<byte>> -> Flow<StreamCommand<JToken>> =
            createStatefulProcessor (fun () -> FramingAssembler(?maxBytes = maxBytes) :> IJsonAssembler<JToken>)

        /// Process a stream of byte chunks into JSON tokens using streaming parser
        let streaming (?maxBytes: int) : Envelope<ReadOnlyMemory<byte>> -> Flow<StreamCommand<JToken>> =
            createStatefulProcessor (fun () -> StreamingMaterializeAssembler(?maxBytes = maxBytes) :> IJsonAssembler<JToken>)

        /// Compose processors in a pipeline
        let pipeline (processors: (Envelope<'a> -> Flow<StreamCommand<'b>>) list) =
            fun (initialEnv: Envelope<'a>) ->
                flow {
                    let rec process (env: Envelope<'x>) (procs: (Envelope<'x> -> Flow<StreamCommand<'y>>) list) =
                        flow {
                            match procs with
                            | [] -> return Complete
                            | proc :: rest ->
                                let! cmd = proc env
                                match cmd with
                                | Emit outEnv ->
                                    // Type inference issue here - would need existential types
                                    // For now, this is a sketch of the pattern
                                    return! process (unbox outEnv) (unbox rest)
                                | EmitMany envs ->
                                    for e in envs do
                                        do! process (unbox e) (unbox rest) |> ignore
                                    return Consume
                                | Consume -> return Consume
                                | Complete -> return Complete
                                | Error ex -> return Error ex
                        }
                    
                    return! process initialEnv (unbox processors)
                }

    /// Example usage
    module Examples =
        open Processors

        let processJsonStream (chunks: ReadOnlyMemory<byte> list) =
            flow {
                let processor = streaming(maxBytes = 1024 * 1024)
                let mutable tokens = []
                
                for chunk in chunks do
                    let env = Envelope.create 0L chunk
                    let! cmd = processor env
                    match cmd with
                    | Emit tokenEnv ->
                        tokens <- tokenEnv.Payload :: tokens
                    | Error ex ->
                        return raise ex
                    | _ -> ()
                
                return List.rev tokens
            }