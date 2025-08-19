namespace Flux.IO.Tests

open System
open System.Text
open Newtonsoft.Json.Linq
open Flux.IO
open Flux.IO.Core
open Flux.IO.Core.Flow

open TokenStreaming
open StreamingDomainStages
open IntegrationStages
open JsonStreamProcessors.JsonStreaming    // for materializer fallback
open JsonStreamProcessors.JsonAssembler

module CoreIntegrationStreamingPipeline =

    type StreamingPipeline = {
        Source        : StreamProcessor<unit,string>
        ToBytes       : StreamProcessor<string, ReadOnlyMemory<byte>>
        Chunker       : StreamProcessor<unit, ReadOnlyMemory<byte>>
        TokenReader   : StreamProcessor<ReadOnlyMemory<byte>, TokenEvent>
        Accum         : StreamProcessor<TokenEvent, AccumEvent> option
        Projection    : StreamProcessor<TokenEvent, ProjectionEvent> option
        Materializer  : StreamProcessor<TokenEvent, JToken> option  // optional doc builder
    }

    /// Materializer: collects TokenEvent into a final JToken (parses original JSON string for now for simplicity)
    /// Minimal implementation: buffer chunks again (simpler than reconstructing full tree from tokens now).
    module TokenMaterializer =
        let create () : StreamProcessor<TokenEvent, JToken> =
            // We'll accumulate raw bytes externally. Simpler: we cannot reconstruct raw bytes from TokenEvent alone,
            // so for correctness we mark placeholder; upgrade later with a writer-building approach.
            // For now: return Complete only when RootTerminal token arrives; re-parse from accumulated source not available here -> unsupported.
            // Stub: return Complete with JValue("materializer-unimplemented")
            let completed = ref false
            StreamProcessor (fun (env: Envelope<TokenEvent>) ->
                flow {
                    if !completed then return Complete
                    else
                        if env.Payload.RootTerminal then
                            completed := true
                            return Emit (mapEnvelope (fun _ -> (JValue("materializer-unimplemented") :> JToken)) env)
                        else
                            return Consume
                })

    module Builder =
        open CoreIntegrationPipeline.PipelineBuilder
        open Generators.JsonGenerators

        let streamingChunked
            ( seed          : int
            , withAccum     : bool
            , threshold     : int option
            , withProjection: bool
            , chunkSizer    : (int -> int)
            ) : CoreIntegrationPipeline.Pipeline =

            let rng = Random seed
            let jsonThunk =
                let r = Random seed
                fun () -> FsCheck.Gen.eval 10 (FsCheck.Random.StdGen (r.Next(), r.Next())) genJson

            let source    = IntegrationStages.JsonSource.create jsonThunk
            let preview   = jsonThunk()
            let bytes     = Encoding.UTF8.GetBytes preview
            let toBytes   = IntegrationStages.ToBytes.create()
            let chunker   = IntegrationStages.Chunker.fromByteArray chunkSizer bytes
            let tokenReader = TokenStreamReader.create()

            let accum =
                if withAccum then threshold |> Option.map StreamingAccumulation.create
                else None

            let projection =
                if withProjection then Some (StreamingProjection.create())
                else None

            let mat = None  // enable later if desired

            // Reuse existing Pipeline DU by wrapping in a new variant or extending; for now extend DU.
            // We'll add PStream in main CoreIntegrationPipeline DU (edit there).

            // Construct a runtime token pipeline through Upcast? For simplicity reuse PChunk until DU updated.
            failwith "Integrate PStream DU variant in CoreIntegrationPipeline.fs before use."