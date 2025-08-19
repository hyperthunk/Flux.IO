namespace Flux.IO.Tests

(*
    IntegrationPipeline.fs

    Purpose:
      Provides pipeline discriminated unions, builders, and lightweight
      step helpers for advancing pipeline execution in tests (without yet
      wiring the FsCheck Machine layer).

    Key Clarification Incorporated:
      - The ORIGIN (source) is the required "terminal" that must emit JSON
        generated via JsonGenerators.
      - Downstream final stage is optional; no requirement to re-emit JSON.

    Pipelines:
      Direct:      Source -> Parser -> (optional Accum) -> (optional Projection)
      Chunking:    Source -> ToBytes -> Chunker -> Parser -> (optional Accum) -> (optional Projection)
      AccumChunk:  Same as Chunking but Accum forced
*)

module CoreIntegrationPipeline = 

    open System
    open System.Text
    open Newtonsoft.Json.Linq
    open FSharp.HashCollections
    open Flux.IO
    open Flux.IO.Core
    open Flux.IO.Core.Flow
    open Generators.JsonGenerators

    open IntegrationStages

    // --------- Pipeline Records ---------

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
        TotalBytes: int
    }

    type Pipeline =
        | PDirect of DirectPipeline
        | PChunk  of ChunkPipeline

    // --------- Builders ---------

    module PipelineBuilder =

        /// Helper: create a seeded JSON generator so tests can reproduce sequences if needed.
        let private makeJsonThunk (seed: int option) =
            let rng = seed |> Option.defaultValue (Random().Next()) |> Random
            fun () ->
                // sample a single JSON from existing generator; we trust fscheck sizing externally
                // You could parameterize size by rng.Next().
                let sample = FsCheck.Gen.eval 10 (FsCheck.Random.StdGen (rng.Next(), rng.Next())) genJson
                sample

        let direct
            ( seed          : int
            , withAccum     : int option
            , withProjection: bool
            , sink          : bool ) : Pipeline =

            let jsonThunk = makeJsonThunk (Some seed)
            let source    = JsonSource.create jsonThunk
            let parser    = Parser.direct ()
            let accum     = withAccum |> Option.bind (fun th -> Some (Accumulation.create th))
            let proj      = if not withProjection then Some (Projection.scalarsAndObjects()) else None
            let sinkStage =
                if not sink then Some (Terminal.consume()) else None

            PDirect {
                Source=source; Parser=parser; Accum=accum;
                Projection=proj; Sink=sinkStage
            }

        let chunked
            ( seed          : int
            , withAccum     : int option
            , withProjection: bool
            , sink          : bool
            , chunkSizer    : (int -> int) ) : Pipeline =

            let jsonThunk = makeJsonThunk (Some seed)
            let source    = JsonSource.create jsonThunk
            // We run source once to capture bytes for deterministic chunker (pragmatic; in harness we will re-run stream)
            let previewJson = jsonThunk()
            let allBytes    = Encoding.UTF8.GetBytes previewJson
            let toBytes     = ToBytes.create()
            // chunk sizing strategy
            let sizeFn      = chunkSizer // (fun remaining -> min 32 remaining)
            let chunker     = Chunker.fromByteArray sizeFn allBytes
            let parser      = Parser.chunkReassembly allBytes.Length
            let accum       = withAccum |> Option.bind (fun th -> Some (Accumulation.create th))
            let proj        = if not withProjection then Some (Projection.scalarsAndObjects()) else None
            let sinkStage   = if not sink then Some (Terminal.consume()) else None

            PChunk {
                Source=source; ToBytes=toBytes; Chunker=chunker; Parser=parser
                Accum=accum; Projection=proj; Sink=sinkStage; TotalBytes = allBytes.Length
            }

    // --------- Execution Harness Helpers ---------

    module PipelineExec =

        open System.Threading

        /// Minimal harness environment per pipeline execution
        type ExecContext =
            { Env : ExecutionEnv }

        let mkExecContext () =
            let env,_,_,_ = TestEnv.mkEnv()
            { Env = env }

        /// Run any processor with a payload and obtain the StreamCommand.
        let private runProc<'a,'b>
            (ctx: ExecContext)
            (proc: StreamProcessor<'a,'b>)
            (payload: 'a) =
            let envl = Envelope.create 0L payload
            StreamProcessor.runProcessor proc envl
            |> Flow.run ctx.Env CancellationToken.None
            |> fun vt -> vt.Result

        /// Advance a direct pipeline exactly one logical step (pull-style).
        /// Strategy:
        ///   1. If we haven't seen source emit -> run source
        ///   2. Else if parse not done -> parse
        ///   3. Else accumulate / project in priority order
        type DirectRuntimeState =
            { mutable SourceDone  : bool
              mutable Parsed      : bool
              mutable AccumDone   : bool
              mutable Projection1 : bool
              mutable Projection2 : bool } // track scalar + object events separately if needed

        let initDirectState () =
            { SourceDone=false; Parsed=false; AccumDone=false; Projection1=false; Projection2=false }

        let stepDirect
            (ctx: ExecContext)
            (pl: DirectPipeline)
            (st: DirectRuntimeState) =
            if not st.SourceDone then
                match runProc ctx pl.Source () with
                | Emit eJson ->
                    // immediately parse (simulate forward feed)
                    match runProc ctx pl.Parser eJson.Payload with
                    | Emit eTok ->
                        st.SourceDone <- true
                        st.Parsed <- true
                        Choice1Of2 "Source+Parse"
                    | _ -> st.SourceDone <- true; Choice1Of2 "SourceOnly"
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

        /// Chunk pipeline runtime state
        type ChunkRuntimeState =
            { mutable SourceDone : bool
              mutable ToBytesRun : bool
              mutable ChunkingDone : bool
              mutable Parsed : bool }

        let initChunkState () =
            { SourceDone=false; ToBytesRun=false; ChunkingDone=false; Parsed=false }

        /// One coarse step for chunk pipeline
        let stepChunk
            (ctx: ExecContext)
            (pl: ChunkPipeline)
            (st: ChunkRuntimeState) =
            if not st.SourceDone then
                match runProc ctx pl.Source () with
                | Emit eJson ->
                    // feed into ToBytes (optional)
                    match runProc ctx pl.ToBytes eJson.Payload with
                    | Emit _ -> st.SourceDone <- true; st.ToBytesRun <- true; Choice1Of2 "Source+ToBytes"
                    | _ -> st.SourceDone <- true; Choice1Of2 "SourceOnly"
                | _ -> Choice2Of2 "NoProgress"
            elif not st.ChunkingDone then
                // Pull a chunk (unit payload)
                match runProc ctx pl.Chunker () with
                | Emit chunkEnv ->
                    // feed chunk to parser
                    match runProc ctx pl.Parser chunkEnv.Payload with
                    | Emit _ -> st.Parsed <- true; Choice1Of2 "FinalChunk+Parsed"
                    | Consume -> Choice1Of2 "MidChunk"
                    | Complete -> st.ChunkingDone <- true; Choice1Of2 "ChunkerCompletePremature"
                    | _ -> Choice2Of2 "UnexpectedCmd"
                | Complete ->
                    st.ChunkingDone <- true
                    Choice1Of2 "ChunkingComplete"
                | _ -> Choice2Of2 "ChunkConsume"
            else
                Choice2Of2 "Idle"