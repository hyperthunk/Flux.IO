namespace Flux.IO.Tests

open System
open System.Text
open Newtonsoft.Json.Linq
open FSharp.HashCollections
open Flux.IO
open Flux.IO.Core1
open Flux.IO.Core1.Flow
open Generators.JsonGenerators

open IntegrationStages
open JsonStreamProcessors
open JsonStreamProcessors.JsonAssembler
open JsonStreamProcessors.JsonFramer

module CoreIntegrationPipeline =

    type ParseMode =
        | LengthGate
        | Frame
        | StreamMaterialize
        | StreamTokens

    type DirectPipeline = {
        Source    : StreamProcessor<unit,string>
        Parser    : StreamProcessor<string,JToken>
        Accum     : StreamProcessor<JToken, AccumEvent> option
        Projection: StreamProcessor<JToken, ProjectionEvent> option
        Sink      : StreamProcessor<ProjectionEvent, unit> option
    }

    type ChunkPipeline = {
        Source    : StreamProcessor<unit,string>
        ToBytes   : StreamProcessor<string, ReadOnlyMemory<byte>>
        Chunker   : StreamProcessor<unit, ReadOnlyMemory<byte>>
        Parser    : StreamProcessor<ReadOnlyMemory<byte>, JToken>
        Accum     : StreamProcessor<JToken, AccumEvent> option
        Projection: StreamProcessor<JToken, ProjectionEvent> option
        Sink      : StreamProcessor<ProjectionEvent, unit> option
        TotalBytes: int option
        Mode      : ParseMode
    }

    type Pipeline =
        | PDirect of DirectPipeline
        | PChunk  of ChunkPipeline

    module PipelineBuilder =

        let private makeJsonThunk (seed: int option) =
            let rng = seed |> Option.defaultValue (Random().Next()) |> Random
            fun () ->
                FsCheck.Gen.eval 10 (FsCheck.Random.StdGen (rng.Next(), rng.Next())) genJson

        let direct (seed:int, withAccum:int option, withProjection:bool, sink:bool) : Pipeline =
            let jsonThunk = makeJsonThunk (Some seed)
            let source    = JsonSource.create jsonThunk
            let parser    = Parser.direct ()
            let accum     = withAccum |> Option.map Accumulation.create
            let proj      = if not withProjection then Some (Projection.scalarsAndObjects()) else None
            let sinkStage = if not sink then Some (IntegrationStages.Terminal.consume()) else None
            PDirect { Source=source; Parser=parser; Accum=accum; Projection=proj; Sink=sinkStage }

        let chunked (seed:int,
                     withAccum:int option,
                     withProjection:bool,
                     sink:bool,
                     chunkSizer:(int -> int),
                     parseMode: ParseMode) : Pipeline =

            let jsonThunk = makeJsonThunk (Some seed)

            // We still preview to create deterministic chunk set; in Frame mode TotalBytes just becomes None
            let previewJson = jsonThunk()
            let allBytes    = Encoding.UTF8.GetBytes previewJson
            let totalLen    = allBytes.Length

            let source  = JsonSource.create jsonThunk
            let toBytes = ToBytes.create()
            let chunker = Chunker.fromByteArray (fun rem -> chunkSizer rem) allBytes

            let parserStage =
                match parseMode with
                | ParseMode.LengthGate ->
                    let asm = LengthAssembler(totalLen) :> IJsonAssembler<JToken>
                    AssemblerProcessor.create asm
                | ParseMode.Frame ->
                    let asm = FramingAssembler() :> IJsonAssembler<JToken>
                    AssemblerProcessor.create asm
                | ParseMode.StreamMaterialize ->
                    let asm = JsonStreaming.StreamingMaterializeAssembler() :> IJsonAssembler<JToken>
                    AssemblerProcessor.create asm
                | ParseMode.StreamTokens ->
                    failwith "Streaming modes not implemented in Phase 1+2."

            let accum = withAccum |> Option.map Accumulation.create
            let proj  = if not withProjection then Some (Projection.scalarsAndObjects()) else None
            let sinkStage = if not sink then Some (IntegrationStages.Terminal.consume()) else None

            PChunk {
                Source=source; ToBytes=toBytes; Chunker=chunker; Parser=parserStage
                Accum=accum; Projection=proj; Sink=sinkStage
                TotalBytes = (if parseMode = ParseMode.LengthGate then Some totalLen else None)
                Mode = parseMode
            }

    module PipelineExec =
        open System.Threading

        type ExecContext = { Env : ExecutionEnv }
        let mkExecContext () =
            let env,_,_,_ = TestEnv.mkEnv()
            { Env = env }

        let private runProc<'a,'b> (ctx: ExecContext) (proc: StreamProcessor<'a,'b>) (payload: 'a) =
            let envl = Envelope.create 0L payload
            StreamProcessor.runProcessor proc envl
            |> Flow.run ctx.Env CancellationToken.None
            |> fun vt -> vt.Result

        type DirectRuntimeState =
            { mutable SourceDone  : bool
              mutable Parsed      : bool
              mutable AccumDone   : bool
              mutable Projection1 : bool
              mutable Projection2 : bool }

        let initDirectState () =
            { SourceDone=false; Parsed=false; AccumDone=false; Projection1=false; Projection2=false }

        let stepDirect ctx (pl:DirectPipeline) st =
            if not st.SourceDone then
                match runProc ctx pl.Source () with
                | Emit eJson ->
                    match runProc ctx pl.Parser eJson.Payload with
                    | Emit _ ->
                        st.SourceDone <- true
                        st.Parsed <- true
                        Choice1Of2 "Source+Parse"
                    | _ ->
                        st.SourceDone <- true
                        Choice1Of2 "SourceOnly"
                | _ -> Choice2Of2 "NoProgress"
            elif st.Parsed && pl.Accum.IsSome && not st.AccumDone then
                match runProc ctx pl.Accum.Value (JToken.Parse "\"_forward_token_unavailable\"") with
                | Emit _ -> Choice1Of2 "AccumEvent"
                | Complete ->
                    st.AccumDone <- true
                    Choice1Of2 "AccumComplete"
                | _ -> Choice2Of2 "AccumConsume"
            elif st.Parsed && pl.Projection.IsSome && not st.Projection1 then
                match runProc ctx pl.Projection.Value (JToken.Parse "\"_forward_token_unavailable\"") with
                | Emit _ -> st.Projection1 <- true; Choice1Of2 "Projection1"
                | Complete -> st.Projection1 <- true; Choice1Of2 "ProjectionCompleteEarly"
                | _ -> Choice2Of2 "ProjectionConsume"
            else
                Choice2Of2 "Idle"

        type ChunkRuntimeState =
            { mutable SourceDone : bool
              mutable ToBytesRun : bool
              mutable ChunkingDone : bool
              mutable Parsed : bool }

        let initChunkState () =
            { SourceDone=false; ToBytesRun=false; ChunkingDone=false; Parsed=false }

        let stepChunk ctx (pl:ChunkPipeline) st =
            if not st.SourceDone then
                match runProc ctx pl.Source () with
                | Emit eJson ->
                    match runProc ctx pl.ToBytes eJson.Payload with
                    | Emit _ -> st.SourceDone <- true; st.ToBytesRun <- true; Choice1Of2 "Source+ToBytes"
                    | _ -> st.SourceDone <- true; Choice1Of2 "SourceOnly"
                | _ -> Choice2Of2 "NoProgress"
            elif not st.ChunkingDone then
                match runProc ctx pl.Chunker () with
                | Emit chunkEnv ->
                    match runProc ctx pl.Parser chunkEnv.Payload with
                    | Emit _ ->
                        st.Parsed <- true
                        Choice1Of2 "FinalChunk+Parsed"
                    | Consume -> Choice1Of2 "MidChunk"
                    | Complete ->
                        st.ChunkingDone <- true
                        Choice1Of2 "ChunkerCompletePremature"
                    | _ -> Choice2Of2 "UnexpectedCmd"
                | Complete ->
                    st.ChunkingDone <- true
                    Choice1Of2 "ChunkingComplete"
                | _ -> Choice2Of2 "ChunkConsume"
            else
                Choice2Of2 "Idle"