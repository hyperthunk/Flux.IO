namespace Flux.IO.Tests

open System
open System.Text
open System.Threading
open Newtonsoft.Json.Linq
open FsCheck
open FsCheck.Experimental
open FSharp.HashCollections
open Flux.IO
open Flux.IO.Core
open Flux.IO.Core.Flow
open Generators.JsonGenerators

(*
    Revised Integration Model

    This revision focuses on fixing the previous compilation issues by:
      - Giving each processor explicit concrete type parameters (no open generic envelope misuse)
      - Removing unsafe / invalid assumptions about env.Payload
      - Eliminating the previous "ParseIfComplete" separation (parser now processes each chunk as it arrives)
      - Using a unified Pipeline discriminated union to hold heterogeneously typed stage processors
      - Avoiding attempts to store processors with incompatible generic arguments in the same dictionary
      - Simplifying accumulation scenario logic to deterministic incremental ingestion
      - Removing reliance on a custom FlowBuilder `Using` (not needed here)
      - Replacing invalid HashSet.toList uses with standard Seq iteration

    Terminal payload remains a JSON string produced from the original generated JSON (satisfies requirement).
*)

module IntegrationModel =

    open Newtonsoft.Json
    open System.Collections.Generic

    // ---------------------------
    // Scenario kinds
    // ---------------------------
    type ScenarioKind =
        | Chunking
        | Accumulation
        | Direct

    // ---------------------------
    // Document state stored in the pure model (immutable)
    // ---------------------------
    type DocumentState = {
        DocId            : int
        Scenario         : ScenarioKind
        OriginalJson     : string
        RootToken        : JToken
        Utf8Bytes        : byte[]
        Chunks           : byte[] list          // Precomputed chunks (chunking scenario)
        NextChunkIndex   : int
        ParsedEmitted    : bool                 // Parser has produced the JToken
        Accumulator      : HashMap<string,obj>  // Accumulation working set
        AccumBatches     : HashSet<string> list // Emitted batch key domains
        AccumThreshold   : int option
        FinalJsonEmitted : bool
        Completed        : bool
    }

    let emptyDoc id scenario json root bytes chunks threshold =
        { DocId = id
          Scenario = scenario
          OriginalJson = json
          RootToken = root
          Utf8Bytes = bytes
          Chunks = chunks
          NextChunkIndex = 0
          ParsedEmitted = (scenario = ScenarioKind.Direct) // direct scenario parses immediately
          Accumulator = HashMap.empty
          AccumBatches = []
          AccumThreshold = threshold
          FinalJsonEmitted = false
          Completed = false }

    type Model = {
        ActiveDocs : Map<int, DocumentState>
        NextDocId  : int
    }
    with
        static member Empty = { ActiveDocs = Map.empty; NextDocId = 0 }

    // ---------------------------
    // Utility: chunking
    // ---------------------------
    let makeChunks (rng: Random) (bytes: byte[]) =
        if bytes.Length = 0 then [bytes]
        else
            let acc = ResizeArray<byte[]>()
            let mutable i = 0
            while i < bytes.Length do
                let remaining = bytes.Length - i
                let size =
                    if remaining = 1 then 1
                    else rng.Next(1, min 32 remaining + 1)
                acc.Add(bytes[i .. i + size - 1])
                i <- i + size
            acc |> Seq.toList

    // ---------------------------
    // Root classification helpers
    // ---------------------------
    type RootProps = { Scalar : (string * JToken) list; Objects : (string * JObject) list }

    let classifyRoot (t: JToken) =
        match t with
        | :? JObject as o ->
            let scalars = ResizeArray<_>()
            let objs = ResizeArray<_>()
            for p in o.Properties() do
                match p.Value with
                | :? JObject as jo -> objs.Add(p.Name, jo)
                | other -> scalars.Add(p.Name, other)
            { Scalar = List.ofSeq scalars; Objects = List.ofSeq objs }
        | other ->
            { Scalar = [("value", other)]; Objects = [] }

    // ---------------------------
    // StreamProcessors per scenario
    // ---------------------------

    // Chunking: chunker (unit -> ReadOnlyMemory<byte>)
    let mkChunker (chunks: byte[] list) =
        let idx = ref 0
        StreamProcessor (fun (_env: Envelope<unit>) ->
            flow {
                if !idx < chunks.Length then
                    let i = !idx
                    idx := i + 1
                    let rom = ReadOnlyMemory<byte>(chunks.[i])
                    let outEnv =
                        Envelope.create (int64 i) rom
                    return Emit outEnv
                else
                    return Complete
            })

    // Parser for chunking: consumes ReadOnlyMemory<byte> chunks, accumulates until total length reached, then parses
    let mkChunkingParser (expectedLen: int) =
        let collected = System.Collections.Generic.List<byte>(expectedLen)
        let parsed = ref false
        StreamProcessor (fun (env: Envelope<ReadOnlyMemory<byte>>) ->
            flow {
                if not !parsed then
                    // Append bytes
                    for b in env.Payload.Span do collected.Add b
                    if collected.Count >= expectedLen then
                        let raw = collected.ToArray()
                        let jsonStr = Encoding.UTF8.GetString raw
                        try
                            let token = JToken.Parse jsonStr
                            parsed := true
                            let outEnv = mapEnvelope (fun _ -> token) env
                            return Emit outEnv
                        with ex ->
                            return Error ex
                    else
                        return Consume
                else
                    return Complete
            })

    // Persistence: JToken -> string (final json)
    let mkPersistence (original: string) =
        StreamProcessor (fun (env: Envelope<JToken>) ->
            flow {
                // We just output the canonical original JSON to satisfy "terminal JSON" rule
                let outEnv = mapEnvelope (fun _ -> original) env
                return Emit outEnv
            })

    // Direct parser (unit -> JToken) - already parsed externally (we store RootToken in model)
    let mkDirectParser (token: JToken) =
        StreamProcessor.lift (fun (_: unit) -> token)

    // Accumulation processor: JToken -> (string) intermediate batches NOT final JSON.
    // We expose the working set snapshots when threshold reached (encoded as JSON object string),
    // final JSON emission handled by finalize command to keep the model simple & pure.
    let mkAccumulationProcessor (threshold: int) =
        let working = ref HashMap.empty<string,obj>
        StreamProcessor (fun (env: Envelope<JToken>) ->
            flow {
                let root = env.Payload
                match root with
                | :? JObject as o ->
                    // Each call we (idempotently) gather scalar keys not yet in working until threshold
                    if HashMap.count working.Value < threshold then
                        for p in o.Properties() do
                            match p.Value with
                            | :? JObject -> () // skip nested objects (per requirements)
                            | v ->
                                if HashMap.count working.Value < threshold then
                                    if not (HashMap.containsKey p.Name working.Value) then
                                        working.Value <- HashMap.add p.Name (box (v.ToString())) working.Value
                    if HashMap.count working.Value >= threshold then
                        // Emit batch snapshot and reset
                        let snapshotPairs =
                            working.Value |> HashMap.toSeq |> Seq.map (fun (k,v) -> k, string v)
                        let jObj = JObject()
                        snapshotPairs |> Seq.iter (fun (k,v) -> jObj[k] <- JValue(v))
                        working.Value <- HashMap.empty
                        let outEnv = mapEnvelope (fun _ -> jObj.ToString(Formatting.None)) env
                        return Emit outEnv
                    else
                        return Consume
                | _ ->
                    return Consume
            })

    // Terminal pass-through: string -> string (identity)
    let mkTerminal () = StreamProcessor.lift id

    // ---------------------------
    // Pipeline union to store heterogeneously typed processors in SUT
    // ---------------------------
    type Pipeline =
        | ChunkingP of chunker: StreamProcessor<unit, ReadOnlyMemory<byte>>
                      * parser:  StreamProcessor<ReadOnlyMemory<byte>, JToken>
                      * persist: StreamProcessor<JToken, string>
                      * terminal: StreamProcessor<string,string>
        | DirectP   of parser: StreamProcessor<unit, JToken>
                      * persist: StreamProcessor<JToken, string>
                      * terminal: StreamProcessor<string,string>
                      * token: JToken
        | AccumP    of parser: StreamProcessor<unit, JToken>
                      * accum: StreamProcessor<JToken, string>
                      * terminal: StreamProcessor<string,string>
                      * token: JToken

    // ---------------------------
    // Commands
    // ---------------------------
    type Command =
        | StartDocument of ScenarioKind * string * int              // scenario, json, threshold(for accum)
        | NextChunk of int                                          // docId
        | RunDirectParse of int
        | RunAccumulation of int
        | PersistAndFinalize of int
        | CompleteDoc of int
        | FlushAccumulation of int                                  // force final batch emission
        | NoOp

    // ---------------------------
    // SUT harness
    // ---------------------------
    type Sut() =
        let pipelines = System.Collections.Concurrent.ConcurrentDictionary<int, Pipeline>()
        let env,_,_,_ = TestEnv.mkEnv()

        member _.Env = env
        member _.Pipelines = pipelines

        member _.Run<'A,'B> (proc: StreamProcessor<'A,'B>) (payload: 'A) =
            let envelope = Envelope<'A>.create 0L payload
            StreamProcessor.runProcessor proc envelope
            |> Flow.run env CancellationToken.None
            |> fun vt -> vt.Result

    // ---------------------------
    // Command application (exec + model update). Returns updated model + labels
    // ---------------------------
    let apply (sut: Sut) (cmd: Command) (model: Model) : Model * string list =
        let labels = ResizeArray<string>()
        let label s = labels.Add s

        match cmd with
        | StartDocument (scenario, json, threshold) ->
            let id = model.NextDocId
            let token = JToken.Parse json
            let bytes = Encoding.UTF8.GetBytes json
            let doc =
                match scenario with
                | ScenarioKind.Chunking ->
                    let rng = Random(id + 7919)
                    let chunks = makeChunks rng bytes
                    emptyDoc id scenario json token bytes chunks None
                | ScenarioKind.Direct ->
                    emptyDoc id scenario json token bytes [bytes] None
                | ScenarioKind.Accumulation ->
                    // threshold param only relevant here
                    emptyDoc id scenario json token bytes [bytes] (Some threshold)

            // Build pipeline
            match scenario with
            | ScenarioKind.Chunking ->
                let chunker  = mkChunker doc.Chunks
                let parser   = mkChunkingParser bytes.Length
                let persist  = mkPersistence json
                let terminal = mkTerminal()
                sut.Pipelines.TryAdd(id, ChunkingP(chunker, parser, persist, terminal)) |> ignore
            | ScenarioKind.Direct ->
                let parser   = mkDirectParser token
                let persist  = mkPersistence json
                let terminal = mkTerminal()
                sut.Pipelines.TryAdd(id, DirectP(parser, persist, terminal, token)) |> ignore
            | ScenarioKind.Accumulation ->
                // parser simply supplies token (one shot)
                let parser   = mkDirectParser token
                let accum    = mkAccumulationProcessor threshold
                let terminal = mkTerminal()
                sut.Pipelines.TryAdd(id, AccumP(parser, accum, terminal, token)) |> ignore

            let model' =
                { model with
                    ActiveDocs = model.ActiveDocs.Add(id, doc)
                    NextDocId = id + 1 }
            label (sprintf "Start:%A Doc=%d" scenario id)
            model', List.ofSeq labels

        | NextChunk id ->
            match model.ActiveDocs.TryFind id, sut.Pipelines.TryGetValue id with
            | Some doc, true, ChunkingP(chunker, parser, _, _) ->
                if doc.NextChunkIndex >= doc.Chunks.Length then
                    model, ["NextChunk:Exhausted"]
                else
                    // Run chunker
                    match sut.Run chunker () with
                    | Emit chunkEnv ->
                        // Feed directly into parser
                        let parseResult = sut.Run parser chunkEnv.Payload
                        let doc' =
                            match parseResult with
                            | Emit _ -> { doc with NextChunkIndex = doc.NextChunkIndex + 1; ParsedEmitted = true }
                            | Consume -> { doc with NextChunkIndex = doc.NextChunkIndex + 1 }
                            | Complete -> { doc with NextChunkIndex = doc.NextChunkIndex + 1 }
                            | _       -> { doc with NextChunkIndex = doc.NextChunkIndex + 1 }
                        { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["NextChunk"]
                    | Complete ->
                        let doc' = { doc with NextChunkIndex = doc.Chunks.Length }
                        { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["NextChunk:Complete"]
                    | _ ->
                        model, ["NextChunk:Other"]
            | _ -> model, ["NextChunk:Invalid"]

        | RunDirectParse id ->
            match model.ActiveDocs.TryFind id, sut.Pipelines.TryGetValue id with
            | Some doc, true, DirectP(parser, _, _, _) ->
                if doc.ParsedEmitted then model, ["RunDirectParse:Already"]
                else
                    let res = sut.Run parser ()
                    let doc' =
                        match res with
                        | Emit _ -> { doc with ParsedEmitted = true }
                        | _ -> doc
                    { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["RunDirectParse"]
            | _ -> model, ["RunDirectParse:Invalid"]

        | RunAccumulation id ->
            match model.ActiveDocs.TryFind id, sut.Pipelines.TryGetValue id with
            | Some doc, true, AccumP(parser, accumProc, _, token) ->
                if doc.FinalJsonEmitted then model, ["RunAccumulation:Finalized"]
                else
                    // Ensure parsed once
                    let doc1 =
                        if doc.ParsedEmitted then doc
                        else
                            let res = sut.Run parser ()
                            match res with
                            | Emit _ -> { doc with ParsedEmitted = true }
                            | _ -> doc
                    // Run accumulation step
                    let _accRes = sut.Run accumProc doc1.RootToken
                    // Update accumulator snapshot in model logically:
                    // We rebuild working set coverage based on threshold and existing recorded batches.
                    // For invariants we only care about domains; actual emitted batch strings are opaque.
                    // The accumProc internally resets after emission; we simulate domain capture by scanning for new keys
                    // not yet covered + not already in residual accumulator.
                    let rp = classifyRoot doc1.RootToken
                    let threshold = defaultArg doc1.AccumThreshold 1
                    // Compute covered keys:
                    let covered =
                        doc1.AccumBatches
                        |> List.collect (fun hs -> hs |> Seq.toList)
                        |> Set.ofList
                    // Determine next key to add (sequentially)
                    let mutable accMap = doc1.Accumulator
                    for (k, v) in rp.Scalar do
                        if not (HashMap.containsKey k accMap) && not (Set.contains k covered) then
                            accMap <- HashMap.add k (box (v.ToString())) accMap
                            if HashMap.count accMap >= threshold then
                                // move into batch
                                let batchSet = HashSet.ofSeq (HashMap.keys accMap)
                                let doc2 = { doc1 with Accumulator = HashMap.empty; AccumBatches = batchSet :: doc1.AccumBatches }
                                let model' =
                                    { model with ActiveDocs = model.ActiveDocs.Add(id, doc2) }
                                label "RunAccumulation:Batch"
                                return model', List.ofSeq labels
                    // If no batch formed:
                    let doc3 = { doc1 with Accumulator = accMap }
                    { model with ActiveDocs = model.ActiveDocs.Add(id, doc3) }, ["RunAccumulation:Progress"]
            | _ -> model, ["RunAccumulation:Invalid"]

        | FlushAccumulation id ->
            match model.ActiveDocs.TryFind id with
            | Some doc when doc.Scenario = ScenarioKind.Accumulation ->
                if HashMap.isEmpty doc.Accumulator then model, ["FlushAccumulation:Empty"]
                else
                    let batch = HashSet.ofSeq (HashMap.keys doc.Accumulator)
                    let doc' = { doc with Accumulator = HashMap.empty; AccumBatches = batch :: doc.AccumBatches }
                    { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["FlushAccumulation"]
            | _ -> model, ["FlushAccumulation:Invalid"]

        | PersistAndFinalize id ->
            match model.ActiveDocs.TryFind id, sut.Pipelines.TryGetValue id with
            | Some doc, true, ChunkingP(_,_,persist,term) ->
                if doc.FinalJsonEmitted || not doc.ParsedEmitted then model, ["Persist:NotReady"]
                else
                    let res = sut.Run persist doc.RootToken
                    let doc' =
                        match res with
                        | Emit _ ->
                            // Pass through terminal (string -> string) for completeness
                            let _ = sut.Run term doc.OriginalJson
                            { doc with FinalJsonEmitted = true }
                        | _ -> doc
                    { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["Persist:Chunking"]
            | Some doc, true, DirectP(_,persist,term,_) ->
                if doc.FinalJsonEmitted || not doc.ParsedEmitted then model, ["Persist:NotReady"]
                else
                    let res = sut.Run persist doc.RootToken
                    let doc' =
                        match res with
                        | Emit _ ->
                            let _ = sut.Run term doc.OriginalJson
                            { doc with FinalJsonEmitted = true }
                        | _ -> doc
                    { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["Persist:Direct"]
            | Some doc, true, AccumP(_,_,term,_) ->
                // Finalize when accumulation coverage == scalar key set
                if doc.FinalJsonEmitted then model, ["Persist:Already"]
                else
                    let rp = classifyRoot doc.RootToken
                    let scalarKeys = rp.Scalar |> List.map fst |> Set.ofList
                    let batchKeys =
                        doc.AccumBatches
                        |> List.collect (fun hs -> hs |> Seq.toList)
                        |> Set.ofList
                    let residualKeys = doc.Accumulator |> HashMap.keys |> Set.ofSeq
                    let covered = Set.union batchKeys residualKeys
                    if scalarKeys = covered then
                        let _ = sut.Run term doc.OriginalJson
                        let doc' = { doc with FinalJsonEmitted = true }
                        { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["Persist:Accum"]
                    else
                        model, ["Persist:AccumNotCovered"]
            | _ -> model, ["Persist:Invalid"]

        | CompleteDoc id ->
            match model.ActiveDocs.TryFind id with
            | Some doc when doc.FinalJsonEmitted && not doc.Completed ->
                let doc' = { doc with Completed = true }
                { model with ActiveDocs = model.ActiveDocs.Add(id, doc') }, ["CompleteDoc"]
            | _ -> model, ["CompleteDoc:NotReady"]

        | NoOp ->
            model, ["NoOp"]
        |> fun (m, ls) -> m, ls

    // ---------------------------
    // Invariants
    // ---------------------------
    let invariants (model: Model) =
        let mutable ok = true
        let sb = System.Text.StringBuilder()
        for KeyValue(_, doc) in model.ActiveDocs do
            match doc.Scenario with
            | ScenarioKind.Chunking ->
                // If parser emitted, all chunks should have been processed
                if doc.ParsedEmitted && doc.NextChunkIndex < doc.Chunks.Length then
                    ok <- false
                    sb.AppendLine($"Doc{doc.DocId} parsed early") |> ignore
                // Byte integrity when done
                if doc.NextChunkIndex = doc.Chunks.Length then
                    let reassembled =
                        doc.Chunks
                        |> List.take doc.NextChunkIndex
                        |> Array.concat
                    if reassembled <> doc.Utf8Bytes then
                        ok <- false
                        sb.AppendLine($"Doc{doc.DocId} bytes mismatch") |> ignore
            | ScenarioKind.Direct -> ()
            | ScenarioKind.Accumulation ->
                // If finalized, coverage must equal scalar keys
                if doc.FinalJsonEmitted then
                    let rp = classifyRoot doc.RootToken
                    let scalarKeys = rp.Scalar |> List.map fst |> Set.ofList
                    let coveredBatches =
                        doc.AccumBatches
                        |> List.collect (fun hs -> hs |> Seq.toList)
                        |> Set.ofList
                    let residual = doc.Accumulator |> HashMap.keys |> Set.ofSeq
                    let covered = Set.union coveredBatches residual
                    if covered <> scalarKeys then
                        ok <- false
                        sb.AppendLine($"Doc{doc.DocId} accumulation coverage mismatch") |> ignore
                    // Threshold respect (each batch >= threshold)
                    match doc.AccumThreshold with
                    | Some th ->
                        for b in doc.AccumBatches do
                            if b.Count < th then
                                ok <- false
                                sb.AppendLine($"Doc{doc.DocId} batch below threshold") |> ignore
                    | None -> ()
        ok, sb.ToString()

    // ---------------------------
    // Machine
    // ---------------------------
    type IntegrationMachine() =
        inherit Machine<Sut, Model>()

        override _.Setup =
            let gen =
                gen {
                    return
                        { new Setup<Sut, Model>() with
                            member _.Actual() = Sut()
                            member _.Model() = Model.Empty }
                } |> Arb.fromGen
            gen

        override _.Next(model: Model) : Gen<Operation<Sut, Model>> =
            // Enabled commands
            let docIds = model.ActiveDocs |> Map.toList |> List.map fst

            let genScenario =
                Gen.frequency [
                    35, Gen.constant ScenarioKind.Chunking
                    25, Gen.constant ScenarioKind.Accumulation
                    40, Gen.constant ScenarioKind.Direct
                ]

            let genStart =
                gen {
                    let! scenario = genScenario
                    let! json = genJson
                    let! threshold =
                        Gen.choose(2, 8)
                    return StartDocument(scenario, json, threshold)
                }

            let existingCommands =
                [
                    for id in docIds do
                        match model.ActiveDocs[id].Scenario with
                        | ScenarioKind.Chunking ->
                            let d = model.ActiveDocs[id]
                            if d.NextChunkIndex < d.Chunks.Length then
                                yield NextChunk id
                            if d.ParsedEmitted && not d.FinalJsonEmitted then
                                yield PersistAndFinalize id
                            if d.FinalJsonEmitted && not d.Completed then
                                yield CompleteDoc id
                        | ScenarioKind.Direct ->
                            let d = model.ActiveDocs[id]
                            if not d.ParsedEmitted then
                                yield RunDirectParse id
                            if d.ParsedEmitted && not d.FinalJsonEmitted then
                                yield PersistAndFinalize id
                            if d.FinalJsonEmitted && not d.Completed then
                                yield CompleteDoc id
                        | ScenarioKind.Accumulation ->
                            let d = model.ActiveDocs[id]
                            if not d.FinalJsonEmitted then
                                yield RunAccumulation id
                                yield FlushAccumulation id
                                yield PersistAndFinalize id
                            if d.FinalJsonEmitted && not d.Completed then
                                yield CompleteDoc id
                ]

            let canStart =
                model.ActiveDocs
                |> Map.toList
                |> List.filter (fun (_,d) -> not d.Completed)
                |> List.length < 4

            let genCmd =
                if canStart then
                    Gen.frequency [
                        3, genStart
                        7, (if existingCommands.IsEmpty then Gen.constant NoOp else Gen.elements existingCommands)
                        1, Gen.constant NoOp
                    ]
                else
                    if existingCommands.IsEmpty then genStart
                    else
                        Gen.frequency [
                            9, Gen.elements existingCommands
                            1, Gen.constant NoOp
                        ]

            gen {
                let! cmd = genCmd
                return
                    { new Operation<Sut, Model>() with
                        override _.Run m =
                            let sut = base.Sut
                            let (m', _) = apply sut cmd m
                            m'
                        override _.Check(sut, m) =
                            let (m', _) = apply sut cmd m
                            let (ok, msg) = invariants m'
                            ok |> Prop.label (sprintf "Cmd=%A Docs=%d %s" cmd m'.ActiveDocs.Count msg)
                        override _.ToString() = sprintf "%A" cmd }
            }

module IntegrationProperties =
    open Expecto

    let config =
        { FsCheckConfig.defaultConfig with
            maxTest = 150
            endSize = 60 }

    [<Tests>]
    let integrationProperties =
        testList "Integration / StreamProcessor Invariants" [
            testPropertyWithConfig config "all scenarios satisfy invariants" <| fun () ->
                let m = IntegrationModel.IntegrationMachine()
                m.ToProperty()
        ]