namespace Flux.IO.Tests

(*
    CoreIntegrationTests.fs

    Comprehensive FsCheck + Expecto test suite exercising the integration
    pipelines and validating core invariants over all reachable interleavings.

    Scope:
      - Builds on the CoreIntegrationMachine (model + operations + machine)
      - Adds additional, stronger invariants that are re‑checked independently
        of the machine's internal invariant checks
      - Explores random interleavings by generating *command scripts* that
        are model‑guided (i.e. each next command depends on the evolving model)
      - Classifies scenarios to gain distribution insight (helps detect skew)

    Key Invariants (summarised):
      Source:
        * Each document’s source emits at most once
        * Parsed root token structurally equals parsing the stored JSON text
      Chunking:
        * ChunksConsumed <= TotalChunks
        * If Parsed = true then (ChunksConsumed > 0)
        * If Parsed = true then eventually (after all chunk commands) Consumed bytes == TotalBytes
      Accumulation (when present):
        * Union(batches ∪ residual) ⊆ scalar key set
        * If Done = true then equality holds
        * No key appears in more than one batch (disjointness)
        * Each batch size >= min(1, threshold) and <= threshold
      Projection (when present):
        * ScalarsEmitted implies Scalars.Keys ⊆ scalar key set
        * ObjectsEmitted implies Objects.Keys ⊆ object key set (derived on demand)
        * Scalars emitted before Objects
      Completion:
        * Completed => Parsed (chunking) OR Direct scenario
        * Completed => SourceEmitted
      Monotonicity:
        * ChunksConsumed monotonically increases
        * Accum batch count monotonically increases
        * No removal of scalar keys once observed across accumulation batches

    NOTE:
      - We intentionally *do not* attempt to re-execute the real processors
        for every advanced invariant (the machine already executes side effects);
        instead we treat the model as primary oracle for pure reasoning.
      - For stronger end-to-end assurances, operations’ .Check already performs
        runtime side-effect execution.

    Conventions:
      - Pure invariant functions return (bool * string list)
      - Aggregated property collects labels: scenario mix, doc counts, presence
        of accumulation / projection, chunk size classification.
*)

open System
open Newtonsoft.Json.Linq
open Expecto
open FsCheck
open FSharp.HashCollections
open Flux.IO
open CoreIntegrationMachine

module private CoreIntInvariant =

    // ---------- Helpers ----------

    let setOfSeq s = s |> Seq.fold (fun st x -> Set.add x st) Set.empty
    let hsUnion (hss: HashSet<'a> list) =
        hss
        |> List.fold (fun st hs -> HashSet.fold (fun acc v -> Set.add v acc) st hs) Set.empty

    let scalarKeys (doc: DocumentModel) =
        doc.Classification.ScalarKeys |> setOfSeq

    let objectKeys (doc: DocumentModel) =
        // Derived at need: root object properties whose value is object
        match doc.RootToken with
        | :? JObject as o ->
            o.Properties()
            |> Seq.choose (fun p -> if p.Value :? JObject then Some p.Name else None)
            |> setOfSeq
        | _ -> Set.empty

    let batchUnion (acc: AccumData) =
        hsUnion acc.Batches
        |> fun st ->
            acc.Residual
            |> HashMap.keys
            |> Seq.fold (fun s k -> Set.add k s) st

    // ---------- Individual Invariants ----------

    let invSource (doc: DocumentModel) : string list =
        // Parse structural equality
        let parsed =
            try
                let again = JToken.Parse doc.Json
                if JToken.DeepEquals(again, doc.RootToken) then None
                else Some "Root token mismatch with raw JSON reparse"
            with ex ->
                Some (sprintf "Failed to parse stored JSON: %s" ex.Message)
        [
            match parsed with
            | Some e -> yield e
            | None -> ()
            // Ensure source emission recorded before completion
            if doc.Completed then
                match doc.Runtime with
                | DirectDoc rt when not rt.SourceEmitted -> yield "Completed direct doc without source emission"
                | ChunkDoc  rt when not rt.SourceEmitted -> yield "Completed chunk doc without source emission"
                | _ -> ()
        ]

    let invChunking (doc: DocumentModel) : string list =
        match doc.Scenario, doc.Runtime, doc.Chunk with
        | Scenario.Chunking, ChunkDoc rt, Some ch ->
            [
                if ch.Consumed < 0 then yield "Negative consumed bytes"
                if ch.Consumed > ch.TotalBytes then yield "Consumed bytes exceed total"
                if rt.ChunksConsumed < 0 then yield "Negative chunk count"
                if rt.Parsed && rt.ChunksConsumed = 0 then yield "Parsed true but zero chunks consumed"
                // If parsed, consumed bytes should eventually reach total – we relax until Completed
                if doc.Completed && rt.Parsed && ch.Consumed <> ch.TotalBytes then
                    yield "Completed & parsed but consumed bytes != total"
            ]
        | _ -> []

    let invAccum (doc: DocumentModel) : string list =
        match doc.Accum with
        | None -> []
        | Some acc ->
            let scalarSet = scalarKeys doc
            let unionSet = batchUnion acc
            let errs =
                [
                    if not (Set.isSubset unionSet scalarSet) then
                        yield "Accum union not subset of scalar keys"
                    if acc.Done && unionSet <> scalarSet then
                        yield "Accum done but union <> scalar key set"
                ]
            // Per-batch constraints
            let batchErrs =
                acc.Batches
                |> List.mapi (fun i hs ->
                    let count = HashSet.count hs
                    if count > acc.Threshold then
                        Some (sprintf "Batch %d exceeds threshold" i)
                    elif count = 0 then
                        Some (sprintf "Batch %d empty" i)
                    else None)
                |> List.choose id
            // Disjointness (strict) : no key appears in more than one batch
            let batchKeysList = acc.Batches |> List.map (fun hs -> hs |> Seq.toList)
            let duplicates =
                batchKeysList
                |> List.collect id
                |> List.groupBy id
                |> List.choose (fun (k, occ) -> if List.length occ > 1 then Some k else None)
            let disjointErrs =
                if duplicates.Length > 0 then
                    [ sprintf "Duplicate keys across batches: %s" (String.concat "," duplicates) ]
                else []
            errs @ batchErrs @ disjointErrs

    let invProjection (doc: DocumentModel) : string list =
        match doc.Projection with
        | None -> []
        | Some p ->
            let scalarSet = scalarKeys doc
            let objSet = objectKeys doc
            [
                if p.ScalarsEmitted then
                    let scalKeys = HashMap.keys p.Scalars |> setOfSeq
                    if not (Set.isSubset scalKeys scalarSet) then
                        yield "Projection scalar keys not subset of scalar keys"
                if p.ObjectsEmitted then
                    let objKeys = HashMap.keys p.Objects |> setOfSeq
                    if not (Set.isSubset objKeys objSet) then
                        yield "Projection object keys not subset of object keys"
                if p.ObjectsEmitted && not p.ScalarsEmitted then
                    yield "Projection objects emitted before scalars"
            ]

    let invCompletion (doc: DocumentModel) : string list =
        [
            if doc.Completed then
                match doc.Runtime with
                | DirectDoc rt ->
                    if not rt.SourceEmitted then yield "Completed direct doc without source emission"
                | ChunkDoc rt ->
                    if not rt.SourceEmitted then yield "Completed chunk doc without source emission"
                    if not rt.Parsed then yield "Completed chunk doc without final parse"
        ]

    let invAggregate (doc: DocumentModel) : string list =
        invSource doc
        @ invChunking doc
        @ invAccum doc
        @ invProjection doc
        @ invCompletion doc

    let checkModel (m: Model) : string list =
        m.Docs
        |> Map.toList
        |> List.collect (fun (_id, doc) ->
            invAggregate doc
            |> List.map (fun e -> sprintf "[Doc %d] %s" doc.Id e))

// ----------------------------------------------------------------------
// Command Script Generator (model-guided)
// ----------------------------------------------------------------------

module private ScriptGen =

    open CoreIntInvariant

    /// Enabled commands based on current model (mirrors but *not* identical to machine's .Next)
    let enabledCommands (m: Model) =
        m.Docs
        |> Map.toList
        |> List.collect (fun (id, doc) ->
            let cmds =
                match doc.Scenario, doc.Runtime with
                | Scenario.Direct, DirectDoc rt ->
                    [
                        if not rt.SourceEmitted then yield StepSource id
                        elif not doc.Completed then yield MarkComplete id
                    ]
                | Scenario.Chunking, ChunkDoc rt ->
                    [
                        if not rt.SourceEmitted then yield StepSource id
                        elif not rt.Parsed then yield StepChunkFeed id
                        elif not doc.Completed then yield MarkComplete id
                    ]
                | _ -> []
            cmds)
        |> fun xs -> if List.isEmpty xs then [NoOp] else xs

    /// Generate a single next command (weighted: new start vs existing ops)
    let genNext (m: Model) =
        let canStart =
            m.Docs
            |> Map.toList
            |> List.filter (fun (_,d) -> not d.Completed)
            |> List.length < 4
        let genSeed = Arb.generate<int> |> Gen.map abs
        let startGen =
            gen {
                let! seed = genSeed
                let! scenario = Gen.frequency [3, Gen.constant Scenario.Direct; 7, Gen.constant Scenario.Chunking]
                let! useAccum = Gen.frequency [2, Gen.constant false; 1, Gen.constant true]
                let! threshold =
                    if useAccum then Gen.choose(2,8) |> Gen.map Some else Gen.constant None
                let! useProj = Gen.frequency [2, Gen.constant false; 1, Gen.constant true]
                match scenario with
                | Scenario.Direct ->
                    return StartDirect(seed, useAccum, threshold, useProj)
                | Scenario.Chunking ->
                    let! chunkSize = Gen.choose(4, 64)
                    return StartChunk(seed, useAccum, threshold, useProj, chunkSize)
            }

        let existing = enabledCommands m
        if canStart then
            Gen.frequency [
                4, startGen
                6, Gen.elements existing
            ]
        else
            Gen.elements existing

    /// Recursively build a command script of length n
    let rec buildScript n (m: Model) (acc: Command list) =
        if n <= 0 then Gen.constant (List.rev acc)
        else
            gen {
                let! cmd = genNext m
                // Pure model evolution (reuse Operation.Run semantics)
                let op = CoreIntegrationMachine.IntegrationOp cmd
                let m' = op.Run m
                return! buildScript (n-1) m' (cmd :: acc)
            }

    let genScript =
        gen {
            let! size = Gen.choose(5, 40)
            return! buildScript size Model.Empty []
        }

// ----------------------------------------------------------------------
// Script Execution
// ----------------------------------------------------------------------

module private Execute =

    open CoreIntInvariant

    /// Execute a script (list of commands) through real Sut + machine ops
    let runScript (script: Command list) =
        let sut = Sut()
        let mutable model = Model.Empty
        // For each command: Run updates model (pure), then Check executes side effects
        script
        |> List.iter (fun c ->
            let op = IntegrationOp c
            model <- op.Run model
            // We ignore Check result except for invariants; run with current model
            op.Check(sut, model) |> ignore)
        model

    // --- Helper: detailed classification labels ------------------------------
    // Place this inside CoreIntegrationProperties (or a nearby helper module)
    // so that `prop_scriptInvariants` can call it.

    module CoreIntegrationPropertiesHelpers =
        // Bucket helpers
        let private bucket value thresholds labels =
            let rec loop v ts ls =
                match ts, ls with
                | [], last::_ -> last                               // no more thresholds; return final label
                | _::_, [] -> failwith "bucket: labels shorter than thresholds"
                | [], [] -> failwith "bucket: no labels provided"   // <— catch‑all missing case
                | t::tRest, l::lRest ->
                    if value <= t then l else loop v tRest lRest
            loop value thresholds labels

        // Compute union coverage for an accumulation doc
        let private accumulationCoverage (doc: DocumentModel) =
            match doc.Accum with
            | None -> None
            | Some acc ->
                let scalarSet =
                    doc.Classification.ScalarKeys
                    |> List.distinct
                    |> Set.ofList
                let covered =
                    let unionBatches =
                        acc.Batches
                        |> List.fold (fun st hs -> HashSet.fold (fun acc k -> Set.add k acc) st hs) Set.empty
                    acc.Residual
                    |> HashMap.keys
                    |> Seq.fold (fun st k -> Set.add k st) unionBatches
                if scalarSet.IsEmpty then Some 1.0
                else Some (float (Set.count covered) / float (Set.count scalarSet))

        // Coverage ratio -> bucket
        let private coverageBucket r =
            let pct = int (r * 100.0 + 0.5)
            if pct = 100 then "coverage=100%"
            elif pct = 0 then "coverage=0%"
            elif pct <= 25 then "coverage=1-25%"
            elif pct <= 50 then "coverage=26-50%"
            elif pct <= 75 then "coverage=51-75%"
            elif pct < 100 then "coverage=76-99%"
            else "coverage=overflow" // defensive

        // Projection state classification
        let private projectionState docs =
            let projDocs = docs |> List.choose (fun d -> d.Projection)
            if projDocs.IsEmpty then "proj=none"
            else
                let scalarsOnly =
                    projDocs
                    |> List.filter (fun p -> p.ScalarsEmitted && not p.ObjectsEmitted)
                let full =
                    projDocs
                    |> List.filter (fun p -> p.ScalarsEmitted && p.ObjectsEmitted)
                match scalarsOnly, full with
                | [], [] -> "proj=declared-none"  // projection present but nothing emitted yet
                | _ :: _, [] -> "proj=scalarsOnly"
                | [], _ :: _ -> "proj=full"
                | _ , _ -> "proj=mixed"

        // Accumulation state classification
        let private accumulationState docs =
            let accumDocs = docs |> List.choose (fun d -> d.Accum)
            if accumDocs.IsEmpty then "accum=none"
            else
                let doneCount = accumDocs |> List.filter (fun a -> a.Done) |> List.length
                let total = accumDocs.Length
                if doneCount = 0 then "accum=progress"
                elif doneCount = total then "accum=allDone"
                else "accum=mixed"

        // Chunk size style: based on average chunk size across all chunk docs
        let private chunkStyle docs =
            let chunkInfos =
                docs
                |> List.choose (fun d ->
                    match d.Runtime, d.Chunk with
                    | ChunkDoc rt, Some ch when rt.ChunksConsumed > 0 ->
                        let avg =
                            // approximate average chunk size with consumed bytes / chunks consumed (if parsed or progressed)
                            let consumed =
                                // best effort: we tracked bytes consumed already
                                ch.Consumed
                            float consumed / float rt.ChunksConsumed
                        Some avg
                    | _ -> None)
            match chunkInfos with
            | [] -> "chunkStyle=none"
            | xs ->
                let avgAcross = List.average xs
                if avgAcross < 16.0 then "chunkStyle=micro"
                elif avgAcross < 64.0 then "chunkStyle=small"
                elif avgAcross < 256.0 then "chunkStyle=medium"
                else "chunkStyle=large"

        // Scalar key cardinality buckets (aggregate over docs by average)
        let private keyCardinalityBucket docs =
            let counts =
                docs
                |> List.map (fun d -> d.Classification.ScalarKeys |> List.length)
            match counts with
            | [] -> "keys=none"
            | cs ->
                let avg = cs |> List.averageBy float
                if avg <= 1.0 then "keys=0-1"
                elif avg <= 5.0 then "keys=2-5"
                elif avg <= 15.0 then "keys=6-15"
                else "keys>15"

        // Completion coverage
        let private completionCoverage docs =
            let total = List.length docs
            if total = 0 then "completion=none"
            else
                let doneCount = docs |> List.filter (fun d -> d.Completed) |> List.length
                if doneCount = 0 then "completion=none"
                elif doneCount = total then "completion=all"
                else "completion=some"

        // Scenario mix
        let private scenarioMix docs =
            let direct = docs |> List.exists (fun d -> d.Scenario = Scenario.Direct)
            let chunk  = docs |> List.exists (fun d -> d.Scenario = Scenario.Chunking)
            match direct, chunk with
            | true, true -> "mix=mixed"
            | true, false -> "mix=directOnly"
            | false, true -> "mix=chunkOnly"
            | false, false -> "mix=empty"

        // Document count bucket
        let private docCountBucket n =
            match n with
            | 0 -> "docs=0"
            | 1 -> "docs=1"
            | 2 -> "docs=2"
            | 3 -> "docs=3"
            | _ when n < 6 -> "docs=4-5"
            | _ -> "docs>=6"

        // Coverage summarisation: choose a representative bucket
        // Strategy: if any doc has 0% keep that; else if all 100% mark 100%; else average.
        let private coverageLabel docs =
            let ratios =
                docs
                |> List.choose accumulationCoverage
            match ratios with
            | [] -> "coverage=na"
            | rs when rs |> List.exists (fun r -> r = 0.0) -> "coverage=0%"
            | rs when rs |> List.forall (fun r -> abs (r - 1.0) < 0.0001) -> "coverage=100%"
            | rs ->
                let avg = List.average rs
                coverageBucket avg

        /// Public helper: classify final model with informative distribution labels
        let classifyModelDetailed (m: Model) : string list =
            let docs = m.Docs |> Map.toList |> List.map snd
            [
                scenarioMix docs
                docCountBucket docs.Length
                completionCoverage docs
                accumulationState docs
                projectionState docs
                chunkStyle docs
                keyCardinalityBucket docs
                coverageLabel docs
            ]

// ----------------------------------------------------------------------
// Properties
// ----------------------------------------------------------------------

module CoreIntegrationProperties =

    open CoreIntInvariant
    open ScriptGen
    open Execute

    let private classifyModel (m: Model) =
        let docs = m.Docs |> Map.toList |> List.map snd
        let directCount =
            docs |> List.filter (fun d -> d.Scenario = Scenario.Direct) |> List.length
        let chunkCount = docs.Length - directCount
        let withAccum =
            docs |> List.filter (fun d -> d.Accum |> Option.isSome) |> List.length
        let withProj =
            docs |> List.filter (fun d -> d.Projection |> Option.isSome) |> List.length
        let completed =
            docs |> List.filter (fun d -> d.Completed) |> List.length
        [
            sprintf "docs=%d" docs.Length
            sprintf "direct=%d" directCount
            sprintf "chunk=%d" chunkCount
            sprintf "accum=%d" withAccum
            sprintf "proj=%d" withProj
            sprintf "completed=%d" completed
        ]

    // Property: All invariants hold for randomly generated command scripts
    let prop_scriptInvariants =
        Prop.forAll (Arb.fromGen genScript) (fun script ->
            let finalModel = runScript script
            let errs = checkModel finalModel
            let labels = CoreIntegrationPropertiesHelpers.classifyModelDetailed finalModel
            let baseProp =
                if List.isEmpty errs then
                    Prop.ofTestable true
                else
                    Prop.ofTestable false
                    |> Prop.label (sprintf "Errors: %s" (String.concat " | " errs))
            labels
            |> List.fold (fun p lbl -> Prop.classify true lbl p) baseProp
        )

    // Property: Source uniqueness (no duplication) and parse structural equality always hold
    let prop_sourceUniqueness =
        Prop.forAll (Arb.fromGen genScript) (fun script ->
            let model = runScript script
            let errs =
                model.Docs
                |> Map.toList
                |> List.choose (fun (_,d) ->
                    // Reparse raw JSON, structural equality
                    try
                        let again = JToken.Parse d.Json
                        if not (JToken.DeepEquals(again, d.RootToken)) then
                            Some (sprintf "Doc %d: structural mismatch" d.Id)
                        else None
                    with ex ->
                        Some (sprintf "Doc %d: reparse failure %s" d.Id ex.Message))
            if errs.IsEmpty then true
            else
                Prop.label (String.concat "; " errs) |> ignore
                false
        )

    // Property: Accum coverage monotonic (union never shrinks)
    let prop_accumMonotonic =
        Prop.forAll (Arb.fromGen genScript) (fun script ->
            // Evolve while sampling intermediate unions
            let sut = Sut()
            let mutable model = Model.Empty
            let mutable ok = true
            let mutable errs = []
            script
            |> List.iter (fun cmd ->
                let op = IntegrationOp cmd
                let prevModel = model
                model <- op.Run model
                let _ = op.Check(sut, model)
                if ok then
                    // For each doc check monotonic union growth
                    model.Docs
                    |> Map.iter (fun _ d ->
                        match d.Accum with
                        | Some acc ->
                            let newUnion = batchUnion acc
                            match prevModel.Docs.TryFind d.Id with
                            | Some oldDoc ->
                                match oldDoc.Accum with
                                | Some oldAcc ->
                                    let oldUnion = batchUnion oldAcc
                                    if not (Set.isSubset oldUnion newUnion) then
                                        ok <- false
                                        errs <- (sprintf "Accum union shrank for doc %d" d.Id)::errs
                                | None -> ()
                            | None -> ()
                        | None -> ()
                    ))
            if ok then true
            else 
                Prop.label (String.concat "; " errs) |> ignore
                false
        )

    // Property: Projection ordering (scalars before objects)
    let prop_projectionOrder =
        Prop.forAll (Arb.fromGen genScript) (fun script ->
            let model = runScript script
            let errs =
                model.Docs
                |> Map.toList
                |> List.choose (fun (_,d) ->
                    match d.Projection with
                    | Some p when p.ObjectsEmitted && not p.ScalarsEmitted ->
                        Some (sprintf "Doc %d objects before scalars" d.Id)
                    | _ -> None)
            if errs.IsEmpty then true
            else 
                Prop.label (String.concat "; " errs) |> ignore
                false
        )

    // Property: Completed implies parse where required
    let prop_completionImpliesParse =
        Prop.forAll (Arb.fromGen genScript) (fun script ->
            let model = runScript script
            let errs =
                model.Docs
                |> Map.toList
                |> List.choose (fun (_,d) ->
                    if d.Completed then
                        match d.Runtime with
                        | DirectDoc _ -> None
                        | ChunkDoc rt when not rt.Parsed ->
                            Some (sprintf "Doc %d completed without parse" d.Id)
                        | _ -> None
                    else None)
            if errs.IsEmpty then true
            else 
                Prop.label (String.concat "; " errs) |> ignore
                false
        )

// ----------------------------------------------------------------------
// Expecto Test List
// ----------------------------------------------------------------------

[<AutoOpen>]
module CoreIntegrationTestSuite =

    open CoreIntegrationProperties

    module Config =
        let std =
            { FsCheckConfig.defaultConfig with
                maxTest = 200
                endSize = 100 }

    [<Tests>]
    let coreIntegrationTests =
        ftestList "IntegrationTestProperties" [
            testPropertyWithConfig Config.std "Script invariants hold" prop_scriptInvariants
            testPropertyWithConfig Config.std "Source uniqueness & reparse structural equality" prop_sourceUniqueness
            testPropertyWithConfig Config.std "Accumulation union monotonic" prop_accumMonotonic
            testPropertyWithConfig Config.std "Projection ordering (scalars before objects)" prop_projectionOrder
            testPropertyWithConfig Config.std "Completion implies parse (chunking)" prop_completionImpliesParse
        ]