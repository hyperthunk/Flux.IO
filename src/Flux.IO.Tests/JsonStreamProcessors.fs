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