namespace Flux.IO.Tests.Integration

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open FsCheck
open FsCheck.Experimental
open FSharpPlus
open FSharp.HashCollections
open Flux.IO
open Flux.IO.Core
open Flux.IO.Core.Flow
open Generators.JsonGenerators

// ---------------------------
// Scenario & Model Definitions
// ---------------------------
module IntegrationModel =

    // Scenario kinds tested (weights applied in machine)
    type ScenarioKind =
        | Chunking
        | Accumulation
        | Direct
        | HybridStub   // reserved future extension

    // Document lifecycle state tracked for oracle & invariants
    type DocumentState = {
        DocId              : int
        Scenario           : ScenarioKind
        OriginalJson       : string
        RootToken          : JToken
        Utf8Bytes          : byte[]
        Chunks             : byte[][]                 // precomputed chunk sequence
        NextChunkIndex     : int
        ParsedEmitted      : bool
        ScalarStore        : HashMap<string,obj>      // actual persisted scalar entries
        ObjectStore        : HashMap<string,string>   // object-valued flattened json
        Accumulator        : HashMap<string,obj>      // accumulation scenario working buffer
        AccumulatedBatches : HashSet<string> list     // domains emitted per batch
        AccumThreshold     : int option               // accumulation threshold (#keys)
        FinalJsonEmitted   : bool
        Completed          : bool
    }

    let emptyDoc docId scenario json root bytes chunks threshold =
        { DocId = docId
          Scenario = scenario
          OriginalJson = json
          RootToken = root
          Utf8Bytes = bytes
          Chunks = chunks
          NextChunkIndex = 0
          ParsedEmitted = false
          ScalarStore = HashMap.empty
          ObjectStore = HashMap.empty
          Accumulator = HashMap.empty
          AccumulatedBatches = []
          AccumThreshold = threshold
          FinalJsonEmitted = false
          Completed = false }

    type Model = {
        ActiveDocs : Map<int, DocumentState>
        NextDocId  : int
        RngSalt    : int
        Emissions  : int
    }
    with
        static member Empty =
            { ActiveDocs = Map.empty
              NextDocId = 0
              RngSalt = 0
              Emissions = 0 }

    // ---------------------------
    // Utility Helpers (Pure)
    // ---------------------------

    let randomChunking (rng: Random) (bytes: byte[]) =
        if bytes.Length = 0 then [||] |> Array.singleton
        else
            let mutable idx = 0
            let acc = ResizeArray<byte[]>()
            while idx < bytes.Length do
                let remaining = bytes.Length - idx
                let size =
                    if remaining = 1 then 1
                    else rng.Next(1, min 32 (remaining) + 1)
                acc.Add(bytes[idx .. idx + size - 1])
                idx <- idx + size
            acc.ToArray()

    // Extract first-level properties classification (for stores & thresholds)
    type RootProps =
        { Scalar : (string * JToken) list
          Objects: (string * JObject) list }

    let classifyRoot (root: JToken) =
        match root with
        | :? JObject as o ->
            let mutable scalar = []
            let mutable objs = []
            for p in o.Properties() do
                match p.Value with
                | :? JObject as jo -> objs <- (p.Name, jo) :: objs
                | other -> scalar <- (p.Name, other) :: scalar
            { Scalar = List.rev scalar; Objects = List.rev objs }
        | other ->
            // Arrays or primitives -> treat as a single object container logically
            { Scalar = [("value", other)]; Objects = [] }

    let jtokenToFlatString (t: JToken) =
        t.ToString(Formatting.None)

    // ---------------------------
    // Pipeline Stage Processors
    // ---------------------------
    // We build minimal processors necessary to satisfy contract; they are internal for test.

    // Chunk Processor:
    // Input: unit
    // Output: ReadOnlyMemory<byte>
    // Emits each chunk sequentially. Completes when done.
    let mkChunkProcessor (chunks: byte[][]) =
        let state = ref 0
        StreamProcessor (fun _env ->
            flow {
                let i = state.Value
                if i < chunks.Length then
                    let payload = chunks.[i].AsMemory()
                    state.Value <- i + 1
                    return Emit { Envelope.create (int64 i) payload with Payload = payload }
                else
                    return Complete
            })

    // Parser Processor:
    // Input: ReadOnlyMemory<byte> (chunk)
    // Output: JToken (only one emit when full document reconstructed)
    // Simple approach: accumulate all bytes until we can parse successfully (best-effort single root)
    let mkParserProcessor (expectedTotal: int) =
        let buffer = ref (Array.zeroCreate<byte> 0)
        let parsed = ref false
        StreamProcessor (fun env ->
            flow {
                let! exec = Flow.ask
                // Simulate async
                do! Flow.liftTask (task { do! Task.Delay(exec.Logger |> ignore; 1) })
                if not parsed.Value then
                    // Append chunk payload
                    // The incoming env.Payload is ReadOnlyMemory<byte>
                    let incoming = env.Payload.ToArray()
                    let merged =
                        if buffer.Value.Length = 0 then incoming
                        else Array.append buffer.Value incoming
                    buffer.Value <- merged
                    if merged.Length >= expectedTotal then
                        // Attempt parse entire content
                        try
                            use sr = new System.IO.StreamReader(new System.IO.MemoryStream(merged), Encoding.UTF8)
                            use jr = new JsonTextReader(sr, SupportMultipleContent = false)
                            let token = JToken.ReadFrom(jr)
                            parsed.Value <- true
                            return Emit { mapEnvelope (fun _ -> token) env with Payload = token }
                        with _ ->
                            return Consume
                    else
                        return Consume
                else
                    return Complete
            })

    // Persistence Processor (Chunking / Direct):
    // Input: JToken
    // Output: string (original JSON string representation)
    let mkPersistenceProcessor (originalJson: string) =
        StreamProcessor (fun env ->
            flow {
                let! exec = Flow.ask
                // Lightweight side-effects (metrics/log)
                exec.Metrics.RecordCounter("persist_invocations", HashMap.empty, 1L)
                let token = env.Payload
                match token with
                | :? JObject as obj ->
                    // We classify and "store" inside Attrs (simulated)
                    // Actual storage verification occurs in model (we re-derive classification).
                    return Emit { mapEnvelope (fun _ -> originalJson) env with Payload = originalJson }
                | _ ->
                    return Emit { mapEnvelope (fun _ -> originalJson) env with Payload = originalJson }
            })

    // Accumulation Processor:
    // Input: (JToken) root
    // Output: Batch dictionaries (string HashMap snapshot) OR final JSON (string) at terminal
    //
    // For modeling simplicity we emit each threshold batch as JSON string representing the partial dictionary,
    // then one final emit with the full original JSON.
    let mkAccumulationProcessor (threshold: int) (originalJson: string) =
        // Keep running dictionary + emitted count
        let working = ref HashMap.empty<string,obj>
        let emittedBatches = ref 0
        let finalEmitted = ref false
        StreamProcessor (fun env ->
            flow {
                let! exec = Flow.ask
                exec.Metrics.RecordCounter("accum_calls", HashMap.empty, 1L)
                if finalEmitted.Value then
                    return Complete
                else
                    match env.Payload with
                    | (:? JObject as obj) ->
                        // Add properties into working
                        for p in obj.Properties() do
                            match p.Value with
                            | :? JObject as _ -> () // skip object-valued for accumulation
                            | other ->
                                if not (HashMap.containsKey p.Name working.Value) then
                                    working.Value <- HashMap.add p.Name (box (other.ToString())) working.Value
                        if HashMap.count working.Value >= threshold then
                            // Emit batch snapshot
                            emittedBatches.Value <- emittedBatches.Value + 1
                            let snapshot =
                                working.Value
                                |> HashMap.toSeq
                                |> Seq.map (fun (k,v) -> k, string v)
                                |> dict
                                |> fun d -> JObject(d |> Seq.map (fun kvp -> JProperty(kvp.Key, JValue(kvp.Value)))))
                            // Reset working to allow further accumulation (we treat disjoint sets ideally)
                            working.Value <- HashMap.empty
                            return Emit { mapEnvelope (fun _ -> snapshot.ToString(Formatting.None)) env with Payload = snapshot.ToString(Formatting.None) }
                        else
                            // Decide probabilistically to finalize or wait
                            if HashMap.count working.Value > 0 && HashMap.count working.Value + 1 < threshold then
                                return Consume
                            else
                                return Consume
                    | _ ->
                        // On non-object, finalize immediately
                        finalEmitted.Value <- true
                        return Emit { mapEnvelope (fun _ -> originalJson) env with Payload = originalJson }
            })

    // Terminal Pass-Through: ensure final JSON string
    let mkTerminal (json: string) =
        StreamProcessor.lift (fun (_: string) -> json)

    // ---------------------------
    // Commands / Operations
    // ---------------------------

    type Command =
        | StartDocument of ScenarioKind * string * int * int            // (scenario, json, threshold, seed)
        | FeedChunk of int
        | ParseIfComplete of int
        | DirectProcess of int
        | AccumulateStep of int
        | FlushAccumulation of int
        | PersistFinalize of int
        | CompleteDoc of int
        | RandomNoOp
        | StartAnotherOrFinish

    // ---------------------------
    // Model Transition (Pure)
    // ---------------------------

    let updateDoc doc f =
        f doc

    let setDoc m doc =
        { m with ActiveDocs = m.ActiveDocs |> Map.add doc.DocId doc }

    // ---------------------------
    // SUT Harness
    // ---------------------------

    // We maintain simple registry of per-document stage processors; stateful processors hold mutation internally.
    type SutRegistry = {
        Chunkers : System.Collections.Concurrent.ConcurrentDictionary<int, StreamProcessor<unit, ReadOnlyMemory<byte>>>
        Parsers  : System.Collections.Concurrent.ConcurrentDictionary<int, StreamProcessor<ReadOnlyMemory<byte>, JToken>>
        Persists : System.Collections.Concurrent.ConcurrentDictionary<int, StreamProcessor<JToken, string>>
        Accums   : System.Collections.Concurrent.ConcurrentDictionary<int, StreamProcessor<JToken, string>>
        Terminals: System.Collections.Concurrent.ConcurrentDictionary<int, StreamProcessor<string,string>>
    }
    with
        static member Create() =
            { Chunkers = System.Collections.Concurrent.ConcurrentDictionary()
              Parsers  = System.Collections.Concurrent.ConcurrentDictionary()
              Persists = System.Collections.Concurrent.ConcurrentDictionary()
              Accums   = System.Collections.Concurrent.ConcurrentDictionary()
              Terminals= System.Collections.Concurrent.ConcurrentDictionary() }

    type Sut() =
        let registry = SutRegistry.Create()
        let env,_,_,_ = TestEnv.mkEnv()
        member _.Env = env
        member _.Registry = registry

        member _.Run<'a,'b> (proc: StreamProcessor<'a,'b>) (payload: 'a) =
            let envelope = Envelope<'a>.create (int64 0) payload
            StreamProcessor.runProcessor proc envelope
            |> Flow.run env CancellationToken.None
            |> fun vt -> vt.Result

    // ---------------------------
    // Operation Execution (Impure harness + pure model update)
    // ---------------------------

    let applyCommand (sut: Sut) (cmd: Command) (model: Model) : Model * (string list) =
        let labels = System.Collections.Generic.List<string>()
        let label s = labels.Add s

        match cmd with
        | StartDocument (scenario, json, threshold, seed) ->
            let docId = model.NextDocId
            // Prepare root token + bytes
            let token = JToken.Parse(json)
            let bytes = Encoding.UTF8.GetBytes(json)
            let chunks =
                match scenario with
                | Chunking ->
                    let rng = Random(seed)
                    randomChunking rng bytes
                | _ -> [| bytes |]
            let accumThreshold =
                match scenario with
                | Accumulation -> Some threshold
                | _ -> None
            let doc = emptyDoc docId scenario json token bytes chunks accumThreshold
            // Create processors
            match scenario with
            | Chunking ->
                let chunker = mkChunkProcessor chunks
                let parser  = mkParserProcessor bytes.Length
                let persist = mkPersistenceProcessor json
                sut.Registry.Chunkers.TryAdd(docId, chunker) |> ignore
                sut.Registry.Parsers.TryAdd(docId, parser) |> ignore
                sut.Registry.Persists.TryAdd(docId, persist) |> ignore
                sut.Registry.Terminals.TryAdd(docId, StreamProcessor.lift id) |> ignore
            | Direct ->
                let parser  = StreamProcessor.lift (fun (_: unit) -> token)
                let persist = mkPersistenceProcessor json
                sut.Registry.Parsers.TryAdd(docId, parser) |> ignore
                sut.Registry.Persists.TryAdd(docId, persist) |> ignore
                sut.Registry.Terminals.TryAdd(docId, StreamProcessor.lift id) |> ignore
            | Accumulation ->
                let parser = StreamProcessor.lift (fun (_: unit) -> token)
                let accum  = mkAccumulationProcessor threshold json
                sut.Registry.Parsers.TryAdd(docId, parser) |> ignore
                sut.Registry.Accums.TryAdd(docId, accum) |> ignore
                sut.Registry.Terminals.TryAdd(docId, StreamProcessor.lift id) |> ignore
            | HybridStub ->
                // placeholder - currently treat as chunking variant
                let chunker = mkChunkProcessor chunks
                let parser  = mkParserProcessor bytes.Length
                let persist = mkPersistenceProcessor json
                sut.Registry.Chunkers.TryAdd(docId, chunker) |> ignore
                sut.Registry.Parsers.TryAdd(docId, parser) |> ignore
                sut.Registry.Persists.TryAdd(docId, persist) |> ignore
                sut.Registry.Terminals.TryAdd(docId, StreamProcessor.lift id) |> ignore

            let model' =
                { model with
                    ActiveDocs = model.ActiveDocs.Add(docId, doc)
                    NextDocId  = docId + 1 }
            label (sprintf "Start:%A DocId=%d len=%d" scenario docId bytes.Length)
            model', List.ofSeq labels

        | FeedChunk docId ->
            match model.ActiveDocs.TryFind docId with
            | None -> model, ["FeedChunk:UnknownDoc"]
            | Some doc ->
                if doc.Scenario <> ScenarioKind.Chunking && doc.Scenario <> ScenarioKind.HybridStub then
                    model, ["FeedChunk:NotChunking"]
                else
                    let chunker = sut.Registry.Chunkers.[docId]
                    let res = sut.Run chunker ()
                    let doc' =
                        match res with
                        | Emit env ->
                            let nc = doc.NextChunkIndex + 1
                            { doc with NextChunkIndex = nc }
                        | Complete ->
                            // Already exhausted
                            doc
                        | Consume -> doc
                        | EmitMany _ -> doc
                        | Error _ -> doc
                    let m = setDoc model doc'
                    m, ["FeedChunk"; sprintf "idx=%d" doc'.NextChunkIndex]

        | ParseIfComplete docId ->
            match model.ActiveDocs.TryFind docId with
            | None -> model, ["ParseIfComplete:UnknownDoc"]
            | Some doc ->
                if doc.ParsedEmitted || doc.NextChunkIndex < doc.Chunks.Length then
                    model, ["ParseIfComplete:NotReady"]
                else
                    let parser = sut.Registry.Parsers.[docId]
                    let res = sut.Run parser doc.Chunks.[doc.Chunks.Length - 1].AsMemory()
                    let doc' =
                        match res with
                        | Emit env -> { doc with ParsedEmitted = true }
                        | _ -> doc
                    let m = setDoc model doc'
                    m, ["ParseIfComplete"]

        | DirectProcess docId ->
            match model.ActiveDocs.TryFind docId with
            | None -> model, ["DirectProcess:Unknown"]
            | Some doc ->
                if doc.Scenario <> ScenarioKind.Direct then model, ["DirectProcess:WrongScenario"]
                else
                    let parser = sut.Registry.Parsers.[docId]
                    let res = sut.Run parser ()
                    let doc' =
                        match res with
                        | Emit _ -> { doc with ParsedEmitted = true }
                        | _ -> doc
                    setDoc model doc', ["DirectProcess"]

        | AccumulateStep docId ->
            match model.ActiveDocs.TryFind docId with
            | None -> model, ["Accumulate:Unknown"]
            | Some doc ->
                if doc.Scenario <> ScenarioKind.Accumulation then model, ["Accumulate:WrongScenario"]
                else
                    let parser = sut.Registry.Parsers.[docId]
                    let resP = sut.Run parser ()
                    let doc1 =
                        match resP with
                        | Emit _ -> { doc with ParsedEmitted = true }
                        | _ -> doc
                    // Simulate accumulation by replaying root scalar props into batches logically
                    let rootProps = classifyRoot doc1.RootToken
                    // We mark accumulation as a single step update to accumulator; emission handled via threshold logic
                    let threshold = defaultArg doc1.AccumThreshold 1
                    let mutable acc = doc1.Accumulator
                    let mutable batches = doc1.AccumulatedBatches
                    for (k,v) in rootProps.Scalar do
                        if not (HashMap.containsKey k acc) then
                            acc <- HashMap.add k (box (v.ToString())) acc
                            if HashMap.count acc >= threshold then
                                batches <- (HashSet.ofSeq (HashMap.keys acc)) :: batches
                                acc <- HashMap.empty
                    let doc2 = { doc1 with Accumulator = acc; AccumulatedBatches = batches }
                    setDoc model doc2, ["AccumulateStep"]

        | FlushAccumulation docId ->
            match model.ActiveDocs.TryFind docId with
            | None -> model, ["Flush:Unknown"]
            | Some doc ->
                if doc.Scenario <> ScenarioKind.Accumulation then model, ["Flush:WrongScenario"]
                else
                    if HashMap.isEmpty doc.Accumulator then model, ["Flush:Empty"]
                    else
                        let batch = HashSet.ofSeq (HashMap.keys doc.Accumulator)
                        let doc' =
                            { doc with
                                Accumulator = HashMap.empty
                                AccumulatedBatches = batch :: doc.AccumulatedBatches }
                        setDoc model doc', ["FlushAccumulation"]

        | PersistFinalize docId ->
            match model.ActiveDocs.TryFind docId with
            | None -> model, ["PersistFinalize:Unknown"]
            | Some doc ->
                if not doc.ParsedEmitted || doc.FinalJsonEmitted then model, ["PersistFinalize:NotReady"]
                else
                    match doc.Scenario with
                    | Chunking
                    | Direct
                    | HybridStub ->
                        let persist = sut.Registry.Persists.[docId]
                        // feed a dummy token envelope - persistence extracts from doc.RootToken logically
                        let res =
                            sut.Run persist doc.RootToken
                        let doc' =
                            match res with
                            | Emit env -> { doc with FinalJsonEmitted = true }
                            | _ -> doc
                        setDoc model doc', ["PersistFinalize"]
                    | Accumulation ->
                        // For accumulation we finalize if all root scalar keys covered by batches ∪ residual accumulator
                        let rootProps = classifyRoot doc.RootToken
                        let batchKeys =
                            doc.AccumulatedBatches
                            |> List.collect HashSet.toList
                            |> Set.ofList
                        let residual = HashSet.ofSeq (HashMap.keys doc.Accumulator)
                        let covered = Set.union batchKeys (Set.ofSeq residual)
                        let scalarKeys = rootProps.Scalar |> List.map fst |> Set.ofList
                        if Set.isSubset scalarKeys covered then
                            let doc' = { doc with FinalJsonEmitted = true }
                            setDoc model doc', ["PersistFinalize"]
                        else
                            model, ["PersistFinalize:NotCovered"]

        | CompleteDoc docId ->
            match model.ActiveDocs.TryFind docId with
            | None -> model, ["Complete:Unknown"]
            | Some doc ->
                if doc.Completed || not doc.FinalJsonEmitted then
                    model, ["Complete:NotReady"]
                else
                    let doc' = { doc with Completed = true }
                    setDoc model doc', ["CompleteDoc"]

        | RandomNoOp ->
            model, ["NoOp"]

        | StartAnotherOrFinish ->
            model, ["Decision"]

    // ---------------------------
    // Invariant Checks (Pure)
    // ---------------------------

    let checkInvariants (model: Model) =
        let mutable ok = true
        let sb = System.Text.StringBuilder()

        // For each doc, verify scenario-specific invariants
        for KeyValue(_, doc) in model.ActiveDocs do
            let rootProps = classifyRoot doc.RootToken
            match doc.Scenario with
            | Chunking
            | HybridStub ->
                // Chunk completeness
                if doc.ParsedEmitted && doc.NextChunkIndex < doc.Chunks.Length then
                    ok <- false
                    sb.AppendLine(sprintf "Doc%d parsed before all chunks" doc.DocId) |> ignore
                // Byte integrity
                let reassembled =
                    doc.Chunks |> Array.take doc.NextChunkIndex |> Array.collect id
                if doc.NextChunkIndex = doc.Chunks.Length then
                    if reassembled <> doc.Utf8Bytes then
                        ok <- false
                        sb.AppendLine(sprintf "Doc%d reassembled mismatch" doc.DocId) |> ignore
            | Direct -> ()
            | Accumulation ->
                // Accumulation batches coverage:
                let scalarKeys = rootProps.Scalar |> List.map fst |> Set.ofList
                let batchKeys =
                    doc.AccumulatedBatches
                    |> List.collect HashSet.toList
                    |> Set.ofList
                let residual = Set.ofSeq (HashMap.keys doc.Accumulator)
                let covered = Set.union batchKeys residual
                if doc.FinalJsonEmitted && covered <> scalarKeys then
                    ok <- false
                    sb.AppendLine(sprintf "Doc%d accumulation coverage mismatch" doc.DocId) |> ignore
                // If threshold exists, batch sizes respect threshold (except final residual)
                match doc.AccumThreshold with
                | Some th ->
                    for b in doc.AccumulatedBatches do
                        if b.Count < th then
                            ok <- false
                            sb.AppendLine(sprintf "Doc%d batch smaller than threshold" doc.DocId) |> ignore
                | None -> ()
        ok, sb.ToString()

    // ---------------------------
    // Machine Specification
    // ---------------------------

    type IntegrationMachine() =
        inherit Machine<Sut, Model>()

        override _.Setup =
            // Provide a seeded RNG salt in Model for chunk splitting determinism
            let gen =
                gen {
                    let! salt = Arb.generate<int>
                    return
                        { new Setup<Sut, Model>() with
                            member __.Actual() = Sut()
                            member __.Model() = { Model.Empty with RngSalt = salt |> abs } }
                }
            Arb.fromGen gen

        override _.Next(model: Model) : Gen<Operation<Sut, Model>> =
            // Build enabled commands based on model state
            let docs = model.ActiveDocs |> Map.toList |> List.map fst
            let active =
                model.ActiveDocs
                |> Map.toList
                |> List.map (fun (id, d) -> id, d)

            // Are there unfinished docs?
            let unfinished = active |> List.filter (fun (_,d) -> not d.Completed)

            // Weighted new doc scenario selection
            let genScenario =
                Gen.frequency [
                    35, Gen.constant ScenarioKind.Chunking
                    25, Gen.constant ScenarioKind.Accumulation
                    30, Gen.constant ScenarioKind.Direct
                    10, Gen.constant ScenarioKind.HybridStub
                ]

            let genStartDoc =
                gen {
                    let! scenario = genScenario
                    let! json = genJson
                    // derive threshold from number of root scalar keys (ensuring feasibility)
                    let root = JToken.Parse json
                    let props = classifyRoot root
                    let scalarCount = props.Scalar.Length
                    let! threshold =
                        Gen.choose(2, max 2 (if scalarCount = 0 then 2 else scalarCount))
                        |> Gen.map (fun t -> min t (max 2 (scalarCount + 1)))
                    let! seed = Arb.generate<int>
                    return StartDocument (scenario, json, threshold, abs seed)
                }

            // Derive potential commands for existing docs
            let perDocCommands =
                [
                    for (id, d) in active do
                        match d.Scenario with
                        | ScenarioKind.Chunking
                        | ScenarioKind.HybridStub ->
                            if d.NextChunkIndex < d.Chunks.Length then
                                yield FeedChunk id
                            if d.NextChunkIndex = d.Chunks.Length && not d.ParsedEmitted then
                                yield ParseIfComplete id
                            if d.ParsedEmitted && not d.FinalJsonEmitted then
                                yield PersistFinalize id
                            if d.FinalJsonEmitted && not d.Completed then
                                yield CompleteDoc id
                        | ScenarioKind.Direct ->
                            if not d.ParsedEmitted then yield DirectProcess id
                            if d.ParsedEmitted && not d.FinalJsonEmitted then yield PersistFinalize id
                            if d.FinalJsonEmitted && not d.Completed then yield CompleteDoc id
                        | ScenarioKind.Accumulation ->
                            if not d.FinalJsonEmitted then
                                // We add accumulation step operations
                                yield AccumulateStep id
                                if HashMap.count d.Accumulator > 0 then
                                    yield FlushAccumulation id
                                yield PersistFinalize id
                            if d.FinalJsonEmitted && not d.Completed then
                                yield CompleteDoc id
                ]

            let canStartNew =
                // Limit concurrent active docs to keep shrinking effective
                unfinished.Length < 4

            // Probability to pick start vs existing
            let genCommand =
                if canStartNew then
                    Gen.frequency [
                        3, genStartDoc
                        7, Gen.elements (if perDocCommands.IsEmpty then [RandomNoOp] else perDocCommands)
                        1, Gen.constant RandomNoOp
                    ]
                else
                    if perDocCommands.IsEmpty then genStartDoc
                    else
                        Gen.frequency [
                            9, Gen.elements perDocCommands
                            1, Gen.constant RandomNoOp
                        ]

            gen {
                let! cmd = genCommand
                return
                    { new Operation<Sut, Model>() with
                        override __.Run model =
                            // Pure state update executed AFTER SUT side-effects
                            let sut = base.Sut
                            let m', _labels = applyCommand sut cmd model
                            m'
                        override __.Check(sut, model) =
                            let m', labels = applyCommand sut cmd model
                            let ok, msg = checkInvariants m'
                            let compositeLabel =
                                sprintf "Cmd=%A Docs=%d %s" cmd model.ActiveDocs.Count msg
                            ok |> Prop.label compositeLabel
                        override __.ToString() = sprintf "%A" cmd }
            }

// Exposed property factory
module IntegrationProperties =

    open Expecto

    let config =
        { FsCheckConfig.defaultConfig with
            maxTest = 150
            endSize = 50 }

    [<Tests>]
    let integrationModelProperties =
        testList "Integration Model Machine" [
            testPropertyWithConfig config "stream integration scenarios satisfy invariants" <| fun () ->
                let m = IntegrationMachine()
                m.ToProperty()
        ]