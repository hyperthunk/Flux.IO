namespace Flux.IO.Tests

open System
open System.Text
open Newtonsoft.Json.Linq
open FsCheck
open Expecto

open Flux.IO.Tests.JsonStreamProcessors
open Flux.IO.Tests.JsonStreamProcessors.JsonAssembler
open Flux.IO.Tests.JsonStreamProcessors.JsonFramer

module FramingTests =

    // Generate moderately sized JSON strings (reuse existing generator from Generators.JsonGenerators if accessible).
    let genJsonText =
        Generators.JsonGenerators.genJson
        |> Gen.map id // already string
        |> Arb.fromGen

    // Split a string's bytes into random partitions
    let genPartitions (s: string) =
        let bytes = Encoding.UTF8.GetBytes s
        let len = bytes.Length
        if len = 0 then Gen.constant [| ReadOnlyMemory<byte>(Array.empty) |]
        else
            gen {
                // number of chunks
                let! k = Gen.choose(1, min (len) (1 + len / 2))
                // random cut points
                let! cuts =
                    Gen.listOfLength (k - 1) (Gen.choose(1, len - 1))
                    |> Gen.map (List.distinct >> List.sort)
                let indices = (0 :: cuts) @ [len]
                let segments =
                    indices
                    |> List.pairwise
                    |> List.map (fun (a,b) ->
                        ReadOnlyMemory<byte>(bytes, a, b - a))
                    |> List.toArray
                return segments
            }

    let private runFramer (chunks: ReadOnlyMemory<byte>[]) =
        let asm = FramingAssembler() :> JsonAssembler.IJsonAssembler<JToken>
        let mutable result : JToken option = None
        let mutable error : exn option = None
        let mutable emits = 0
        for ch in chunks do
            match asm.Feed ch with
            | JsonAssembler.StatusNeedMore -> ()
            | JsonAssembler.StatusComplete tok ->
                emits <- emits + 1
                result <- Some tok
            | JsonAssembler.StatusError ex ->
                error <- Some ex
        emits, result, error

    [<Tests>]
    let framingProperties =
        testList "Framing / Structural Parser" [

            testProperty "frameCompletesOnce" <| Prop.forAll genJsonText (fun json ->
                let chunks = Gen.eval 10 (Random.StdGen (123,456)) (genPartitions json)
                let emits, result, error = runFramer chunks
                match error with
                | Some ex -> false |> Prop.label ("Error: " + ex.Message)
                | None ->
                    (emits = 1)
                    |> Prop.label (sprintf "emits=%d bytes=%d" emits json.Length)
                    |> Prop.collect emits
            )

            testProperty "frameDocumentParity" <| Prop.forAll genJsonText (fun json ->
                let chunks = Gen.eval 10 (Random.StdGen (42,1337)) (genPartitions json)
                let emits, result, error = runFramer chunks
                match error, result with
                | Some ex, _ -> false |> Prop.label ("Error: " + ex.Message)
                | None, None ->
                    // Possibly unsupported scalar — treat as pass if original parse also fails
                    try
                        let _ = JToken.Parse json
                        false |> Prop.label "No result but parse succeeded"
                    with ex -> true |> Prop.label ("Caught: " + ex.Message)
                | None, Some tok ->
                    try
                        let parsed = JToken.Parse json
                        JToken.DeepEquals(parsed, tok)
                        |> Prop.label (sprintf "emits=%d" emits)
                    with ex ->
                        false |> Prop.label ("Direct parse failed: " + ex.Message)
            )

            testProperty "frameNeverCompletesEarly" <| Prop.forAll genJsonText (fun json ->
                let bytes = Encoding.UTF8.GetBytes json
                // Build progressive prefixes; if framer completes at prefix p < full length, direct parse must succeed *and* structural depth closed
                let asm = FramingAssembler() :> JsonAssembler.IJsonAssembler<JToken>
                let mutable early = false
                for i in 1 .. bytes.Length - 1 do
                    if not asm.Completed then
                        let span = ReadOnlyMemory<byte>(bytes, i-1, 1)
                        let _ = asm.Feed span
                        if asm.Completed && i <> bytes.Length then
                            // early completion
                            early <- true
                (early = false)
            )

            testProperty "frameFuzzPartitions" <| Prop.forAll genJsonText (fun json ->
                // Fuzz: generate multiple different partitionings; ensure at most one completion across all
                let seeds = [0..5]
                let mutable ok = true
                let mutable labels = []
                for s in seeds do
                    if ok then
                        let chunks =
                            Gen.eval 20 (Random.StdGen (s, s+17)) (genPartitions json)
                        let emits, _, error = runFramer chunks
                        if error.IsSome then ok <- false
                        elif emits <> 1 then ok <- false
                        labels <- (sprintf "e%d" emits) :: labels
                ok |> Prop.label (String.concat "," labels)
            )
        ]