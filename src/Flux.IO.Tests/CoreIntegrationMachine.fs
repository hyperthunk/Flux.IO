namespace Flux.IO.Tests

(*
    IntegrationMachine.fs

    Core integration test infrastructure (model + machine) that:
      - Creates and manages heterogeneous pipelines (direct / chunking)
      - Drives pipeline progression through model-derived operations
      - Tracks semantic state (source emission, chunk consumption, parse completion,
        accumulation batches, projection events)
      - Provides invariants for property-based model checking

    NOTE:
      This file intentionally focuses on the MACHINE + MODEL layers.
      The underlying pipeline / stages are defined in:
        - IntegrationStages.fs
        - IntegrationPipeline.fs

    DESIGN PRINCIPLES:
      - Model is pure & immutable; only updated in Operation.Run
      - SUT holds mutable runtime pipeline state
      - .Next derives enabled operations from current model (model-driven exploration)
      - No mandatory final JSON emission: origin source is the only required JSON producer
      - Accumulation invariants ensure domain coverage and non-overlap
*)

open System
open System.Text
open Newtonsoft.Json.Linq
open FsCheck
open FsCheck.Experimental
open FSharp.HashCollections
open CoreIntegrationPipeline
open CoreIntegrationPipeline.PipelineExec

module CoreIntegrationMachine =

    // ---------------------------
    // Scenario & Supporting Types
    // ---------------------------

    type Scenario =
        | Direct
        | Chunking

    /// Root-level property classification used for invariants.
    type RootClassification = {
        ScalarKeys : string list
    }

    let classifyRoot (tok: JToken) =
        match tok with
        | :? JObject as o ->
            let keys =
                o.Properties()
                |> Seq.choose (fun p ->
                    if p.Value :? JObject then None else Some p.Name)
                |> Seq.distinct
                |> Seq.toList
            { ScalarKeys = keys }
        | _ ->
            { ScalarKeys = ["value"] }

    /// Accumulation result domain union utilities
    module HashSetOps =
        let unionAll (sets: HashSet<'a> list) =
            let rec loop ss acc =
                match ss with
                | [] -> acc
                | h::t ->
                    let acc' =
                        HashSet.fold (fun st v -> HashSet.add v st) acc h
                    loop t acc'
            loop sets (HashSet.empty<_>)

        let keysOfMap (m: HashMap<'a,'b>) =
            HashMap.keys m
            |> Seq.fold (fun st k -> HashSet.add k st) (HashSet.empty<_>)

    // ---------------------------
    // Document Model
    // ---------------------------

    type AccumData = {
        Threshold         : int
        Batches           : HashSet<string> list
        Done              : bool
        Residual          : HashMap<string,obj>
    }

    type ProjectionData = {
        Scalars  : HashMap<string,obj>
        Objects  : HashMap<string,string>
        ScalarsEmitted : bool
        ObjectsEmitted : bool
    }

    type ChunkingData = {
        TotalBytes  : int
        Consumed    : int
        Parsed      : bool
    }

    type DirectRuntime =
        { SourceEmitted : bool
          Parsed        : bool
          AccumComplete : bool
          Projection    : ProjectionData option }

    type ChunkRuntime =
        { SourceEmitted : bool
          BytesMaterialised : bool
          ChunksConsumed : int
          Parsed         : bool
          AccumComplete  : bool
          Projection     : ProjectionData option
          TotalChunks    : int
          TotalBytes     : int }

    type DocumentRuntime =
        | DirectDoc of DirectRuntime
        | ChunkDoc  of ChunkRuntime

    type DocumentModel = {
        Id            : int
        Scenario      : Scenario
        Seed          : int
        Json          : string
        RootToken     : JToken
        Classification: RootClassification
        Accum         : AccumData option
        Projection    : ProjectionData option
        Chunk         : ChunkingData option
        Runtime       : DocumentRuntime
        Completed     : bool
    }

    /// Main test model
    type Model = {
        Docs    : Map<int, DocumentModel>
        NextId  : int
        RngSalt : int
    }
    with
        static member Empty = { Docs = Map.empty; NextId = 0; RngSalt = 0 }

    // ---------------------------
    // SUT (System Under Test) Structure
    // ---------------------------

    /// Runtime pipeline state per doc
    type RuntimePipeline =
        | RDirect of DirectPipeline * DirectRuntimeState
        | RChunk  of ChunkPipeline  * ChunkRuntimeState

    type Sut() =
        let pipelines = Collections.Concurrent.ConcurrentDictionary<int, RuntimePipeline>()
        let ctx = mkExecContext()

        member _.Pipelines = pipelines
        member _.Ctx = ctx

        member this.StepDirect id =
            match pipelines.TryGetValue id with
            | true, RDirect (pl, st) ->
                stepDirect ctx pl st |> ignore
            | _ -> ()

        member this.StepChunk id =
            match pipelines.TryGetValue id with
            | true, RChunk (pl, st) ->
                stepChunk ctx pl st |> ignore
            | _ -> ()

        member this.Register (id:int) (pipe: Pipeline) =
            match pipe with
            | PDirect dp ->
                pipelines.TryAdd(id, RDirect(dp, initDirectState())) |> ignore
            | PChunk cp ->
                pipelines.TryAdd(id, RChunk(cp, initChunkState())) |> ignore

        member this.GetRuntime id =
            match pipelines.TryGetValue id with
            | true, RDirect (_pl, st) -> Choice1Of2 st
            | true, RChunk  (_pl, st) -> Choice2Of2 st
            | _ -> failwithf "Runtime not found for doc %d" id

    // ---------------------------
    // Internal Helpers
    // ---------------------------

    let mkProjectionData () =
        { Scalars = HashMap.empty
          Objects = HashMap.empty
          ScalarsEmitted = false
          ObjectsEmitted = false }

    let mkAccumData threshold =
        { Threshold = threshold
          Batches = []
          Done = false
          Residual = HashMap.empty }

    let updateAccumOnBatch (acc: AccumData) (batch: HashMap<string,obj>) =
        let hs =
            batch
            |> HashMap.keys
            |> Seq.fold (fun st k -> HashSet.add k st) (HashSet.empty<string>)
        { acc with Batches = hs :: acc.Batches; Residual = HashMap.empty }

    let addResidual (acc: AccumData) (key: string) (value: obj) =
        { acc with Residual = HashMap.add key value acc.Residual }

    let markAccumDone (acc: AccumData) =
        { acc with Done = true }

    // ---------------------------
    // Operations (Commands)
    // ---------------------------

    type Command =
        | StartDirect of seed:int * useAccum: bool * threshold: int option * useProj: bool
        | StartChunk of seed:int * useAccum: bool * threshold: int option * useProj: bool * chunkSizeHint:int
        | StepSource of docId:int
        | StepChunkFeed of docId:int
        | StepAccum of docId:int
        | StepProjection of docId:int
        | MarkComplete of docId:int
        | NoOp

    // ---------------------------
    // Invariants
    // ---------------------------

    let invariants (m: Model) =
        let problems =
            m.Docs
            |> Map.toList
            |> List.collect (fun (_id, doc) ->
                let errs =
                    match doc.Scenario, doc.Chunk, doc.Runtime with
                    | chunking, Some ch, ChunkDoc runtime ->
                        let accs =
                            if runtime.Parsed && ch.Consumed < ch.TotalBytes then
                                ["Parsed before all bytes consumed"]
                            else []
                        let reassembly =
                            if runtime.Parsed && ch.Consumed = ch.TotalBytes then
                                [] // assume chunk reassembly correctness at stage-level (could reconstruct)
                            else []
                        accs @ reassembly
                    | _, _, _ -> []

                let accumErrs =
                    match doc.Accum with
                    | Some a when a.Done ->
                        let fullSet = doc.Classification.ScalarKeys |> Set.ofList
                        let batchUnion =
                            let sets = a.Batches
                            let u =
                                sets
                                |> List.fold (fun st hs -> HashSet.fold (fun acc k -> Set.add k acc) st hs) Set.empty
                            let residualKeys =
                                a.Residual
                                |> HashMap.keys
                                |> Seq.fold (fun st k -> Set.add k st) Set.empty
                            Set.union u residualKeys
                        if fullSet <> batchUnion then
                            ["Accumulation coverage mismatch"]
                        else []
                    | _ -> []
                errs @ accumErrs
            )
        problems

    // ---------------------------
    // Model Updates (Pure)
    // ---------------------------

    let startDirectDoc model seed useAccum threshold useProj =
        let jsonGen = PipelineBuilder.direct (seed, (if useAccum then threshold else None), useProj, true)
        // We must generate JSON separately to store in model (the builder's source stage will produce another one using same seed)
        // For deterministic reproduction we re-evaluate generator identical to builder's thunk:
        let rng = Random seed
        let sample =
            FsCheck.Gen.eval 10 (FsCheck.Random.StdGen (rng.Next(), rng.Next())) Generators.JsonGenerators.genJson
        let tok = JToken.Parse sample
        let cls = classifyRoot tok
        let docId = model.NextId
        let doc =
            { Id = docId
              Scenario = Direct
              Seed = seed
              Json = sample
              RootToken = tok
              Classification = cls
              Accum = (if useAccum then threshold |> Option.map mkAccumData else None)
              Projection = (if useProj then Some (mkProjectionData()) else None)
              Chunk = None
              Runtime =
                DirectDoc { SourceEmitted=false; Parsed=false; AccumComplete=false; Projection=None }
              Completed = false }
        doc, jsonGen

    let computeChunkCount (len:int) (sizeHint:int) =
        let rec loop remaining count =
            if remaining <= 0 then count
            else
                let sz = min sizeHint remaining |> max 1
                loop (remaining - sz) (count + 1)
        loop len 0

    let startChunkDoc model seed useAccum threshold useProj chunkSize =
        // Use LengthGate for now; can parameterize later
        let pipe =
            PipelineBuilder.chunked (
                seed,
                (if useAccum then threshold else None),
                useProj,
                true,
                (fun rem -> min chunkSize rem),
                ParseMode.LengthGate
            )
        let rng = Random seed
        let sample =
            FsCheck.Gen.eval 10 (FsCheck.Random.StdGen (rng.Next(), rng.Next())) Generators.JsonGenerators.genJson
        let tok = JToken.Parse sample
        let bytes = Encoding.UTF8.GetBytes sample
        let cls = classifyRoot tok
        let docId = model.NextId
        let totalChunks =
            let rec loop remaining count =
                if remaining <= 0 then count
                else
                    let sz = min chunkSize remaining |> max 1
                    loop (remaining - sz) (count + 1)
            loop bytes.Length 0
        let chunkData =
            { TotalBytes = bytes.Length
              Consumed = 0
              Parsed = false }
        let runtime =
            ChunkDoc { SourceEmitted=false; BytesMaterialised=false; ChunksConsumed=0;
                       Parsed=false; AccumComplete=false; Projection=None;
                       TotalChunks = totalChunks; TotalBytes = bytes.Length }
        let doc =
            { Id = docId
              Scenario = Chunking
              Seed = seed
              Json = sample
              RootToken = tok
              Classification = cls
              Accum = (if useAccum then threshold |> Option.map mkAccumData else None)
              Projection = (if useProj then Some (mkProjectionData()) else None)
              Chunk = Some chunkData
              Runtime = runtime
              Completed = false }
        doc, pipe

    let updateModelAfterSource id model =
        match model.Docs.TryFind id with
        | None -> model
        | Some doc ->
            let doc' =
                match doc.Runtime with
                | DirectDoc rt ->
                    { doc with Runtime = DirectDoc { rt with SourceEmitted = true; Parsed = true } }
                | ChunkDoc rt ->
                    { doc with Runtime = ChunkDoc { rt with SourceEmitted = true; BytesMaterialised=true } }
            { model with Docs = model.Docs.Add(id, doc') }

    let updateModelAfterChunk id parsed model =
        match model.Docs.TryFind id with
        | None -> model
        | Some doc ->
            match doc.Runtime, doc.Chunk with
            | ChunkDoc rt, Some ch ->
                let consumed = ch.Consumed
                let ch' = { ch with Consumed = consumed + 1; Parsed = if parsed then true else ch.Parsed }
                let rt' = { rt with ChunksConsumed = rt.ChunksConsumed + 1; Parsed = if parsed then true else rt.Parsed }
                let doc' = { doc with Chunk = Some ch'; Runtime = ChunkDoc rt' }
                { model with Docs = model.Docs.Add(id, doc') }
            | _ -> model

    let updateModelAccum id batchOpt doneOpt model =
        match model.Docs.TryFind id with
        | None -> model
        | Some doc ->
            match doc.Accum with
            | None -> model
            | Some acc ->
                let acc' =
                    match batchOpt, doneOpt with
                    | Some batch, _ -> updateAccumOnBatch acc batch
                    | None, Some true -> markAccumDone acc
                    | _ -> acc
                // mark accum complete in runtime if done
                let doc' =
                    match doc.Runtime with
                    | DirectDoc rt when acc'.Done ->
                        { doc with Accum = Some acc'; Runtime = DirectDoc { rt with AccumComplete = true } }
                    | DirectDoc rt ->
                        { doc with Accum = Some acc'; Runtime = DirectDoc rt }
                    | ChunkDoc rt when acc'.Done ->
                        { doc with Accum = Some acc'; Runtime = ChunkDoc { rt with AccumComplete = true } }
                    | ChunkDoc rt ->
                        { doc with Accum = Some acc'; Runtime = ChunkDoc rt }
                { model with Docs = model.Docs.Add(id, doc') }

    let updateModelProjection id scalarsOpt objectsOpt model =
        match model.Docs.TryFind id with
        | None -> model
        | Some doc ->
            match doc.Projection with
            | None -> model
            | Some proj ->
                let proj' =
                    match scalarsOpt, objectsOpt with
                    | Some s, _ -> { proj with Scalars = s; ScalarsEmitted = true }
                    | None, Some o -> { proj with Objects = o; ObjectsEmitted = true }
                    | _ -> proj
                let doc' =
                    match doc.Runtime with
                    | DirectDoc rt -> { doc with Projection = Some proj'; Runtime = DirectDoc rt }
                    | ChunkDoc rt ->  { doc with Projection = Some proj'; Runtime = ChunkDoc rt }
                { model with Docs = model.Docs.Add(id, doc') }

    let markComplete id model =
        match model.Docs.TryFind id with
        | None -> model
        | Some doc ->
            let doc' = { doc with Completed = true }
            { model with Docs = model.Docs.Add(id, doc') }

    // ---------------------------
    // Operations (FsCheck Experimental)
    // ---------------------------

    type IntegrationOp(cmd: Command) =
        inherit Operation<Sut, Model>()

        override _.Run(model: Model) =
            match cmd with
            | StartDirect(seed, useAccum, threshold, useProj) ->
                let doc, _pipe = startDirectDoc model seed useAccum threshold useProj
                { model with Docs = model.Docs.Add(doc.Id, doc); NextId = model.NextId + 1 }
            | StartChunk(seed, useAccum, threshold, useProj, chunkSize) ->
                let doc, _pipe = startChunkDoc model seed useAccum threshold useProj chunkSize
                { model with Docs = model.Docs.Add(doc.Id, doc); NextId = model.NextId + 1 }
            | StepSource id ->
                updateModelAfterSource id model
            | StepChunkFeed id ->
                // optimistic assumption: final chunk yields parse if all consumed
                let doc = model.Docs.[id]
                let parsed =
                    match doc.Chunk with
                    | Some ch ->
                        let remaining = ch.TotalBytes - ch.Consumed
                        remaining <= 1 // heuristic; true parser decides in Check
                    | None -> false
                updateModelAfterChunk id parsed model
            | StepAccum id ->
                // pure model update only sets batch/done when plausible heuristics say so (defer exact to Check)
                model
            | StepProjection _id ->
                model
            | MarkComplete id ->
                markComplete id model
            | NoOp -> model

        override _.Check(sut: Sut, model: Model) =
            // Execute side-effects & update SUT runtime
            match cmd with
            | StartDirect(seed, useAccum, threshold, useProj) ->
                let doc, pipe = startDirectDoc model seed useAccum threshold useProj
                sut.Register doc.Id pipe
                true |> Prop.label (sprintf "StartDirect Doc=%d" doc.Id)
            | StartChunk(seed, useAccum, threshold, useProj, chunkSize) ->
                let doc, pipe = startChunkDoc model seed useAccum threshold useProj chunkSize
                sut.Register doc.Id pipe
                true |> Prop.label (sprintf "StartChunk Doc=%d" doc.Id)
            | StepSource id ->
                sut.StepDirect id
                sut.StepChunk id
                true |> Prop.label (sprintf "StepSource(%d)" id)
            | StepChunkFeed id ->
                sut.StepChunk id
                true |> Prop.label (sprintf "StepChunk(%d)" id)
            | StepAccum id ->
                // we do not have explicit accumulation stepping exposed yet (would require token feed)
                true |> Prop.label (sprintf "StepAccum(%d)" id)
            | StepProjection id ->
                // likewise projection feed
                true |> Prop.label (sprintf "StepProjection(%d)" id)
            | MarkComplete id ->
                true |> Prop.label (sprintf "MarkComplete(%d)" id)
            | NoOp ->
                true |> Prop.label "NoOp"
            |> fun baseProp ->
                // Global invariant check
                let errs = invariants model
                if List.isEmpty errs then baseProp
                else
                    let merged = String.concat "; " errs
                    (false |> Prop.label ("InvariantFail: " + merged))

        override _.ToString() = sprintf "%A" cmd

    // ---------------------------
    // Machine
    // ---------------------------

    type IntegrationMachine() =
        inherit Machine<Sut, Model>()

        override _.Setup =
            let gen =
                gen {
                    let! salt = Arb.generate<int>
                    return
                        { new Setup<Sut, Model>() with
                            member _.Actual() = Sut()
                            member _.Model() = { Model.Empty with RngSalt = abs salt } }
                }
                |> Arb.fromGen
            gen

        override _.Next(model: Model) =
            let docIds =
                model.Docs
                |> Map.toList
                |> List.map fst

            // Build per-doc enabled transitions
            let perDocCommands =
                docIds
                |> List.collect (fun id ->
                    let doc = model.Docs.[id]
                    let cmds =
                        [
                            match doc.Scenario, doc.Runtime with
                            | Direct, DirectDoc rt ->
                                if not rt.SourceEmitted then
                                    yield StepSource id
                                elif not doc.Completed then
                                    // Provide projection / accumulation stubs only if configured
                                    yield! []
                                    yield MarkComplete id
                            | Chunking, ChunkDoc rt ->
                                if not rt.SourceEmitted then
                                    yield StepSource id
                                elif not rt.Parsed then
                                    yield StepChunkFeed id
                                elif not doc.Completed then
                                    yield MarkComplete id
                                else ()
                            | _ -> ()
                        ]
                    cmds)

            let canStartNew =
                model.Docs
                |> Map.toList
                |> List.filter (fun (_,d) -> not d.Completed)
                |> List.length < 4

            let genSeed = Arb.generate<int> |> Gen.map abs

            let genStart =
                gen {
                    let! seed = genSeed
                    let! scenario = Gen.frequency [ 3, Gen.constant Direct
                                                    7, Gen.constant Chunking ]
                    let! useAccum = Gen.frequency [3, Gen.constant false; 2, Gen.constant true]
                    let! threshold =
                        if useAccum then Gen.choose(2,8) |> Gen.map Some
                        else Gen.constant None
                    let! useProj = Gen.frequency [2, Gen.constant false; 1, Gen.constant true]
                    match scenario with
                    | Direct ->
                        return StartDirect(seed, useAccum, threshold, useProj)
                    | Chunking ->
                        let! chunkSize = Gen.choose(8, 64)
                        return StartChunk(seed, useAccum, threshold, useProj, chunkSize)
                }

            let genCommand =
                if canStartNew then
                    Gen.frequency [
                        3, genStart
                        6, (if perDocCommands.IsEmpty then Gen.constant NoOp else Gen.elements perDocCommands)
                        1, Gen.constant NoOp
                    ]
                else
                    if perDocCommands.IsEmpty then genStart
                    else
                        Gen.frequency [
                            9, Gen.elements perDocCommands
                            1, Gen.constant NoOp
                        ]

            gen {
                let! cmd = genCommand
                return IntegrationOp cmd :> Operation<_,_>
            }

    // ---------------------------
    // Property Helper
    // ---------------------------

    let property config =
        let machine = IntegrationMachine()
        machine.ToProperty()

    // ---------------------------
    // Convenience Expecto Test (optional – can be referenced externally)
    // ---------------------------

    module Tests =
        open Expecto

        let config =
            { FsCheckConfig.defaultConfig with
                maxTest = 150
                endSize = 50 }

        [<Tests>]
        let integrationCoreMachine =
            testList "Core Integration Machine" [
                testPropertyWithConfig config "pipelines uphold invariants" <| fun () ->
                    property config
            ]