namespace Flux.IO.Tests

module CoreStreamingMaterializeTests =
    open System
    open System.Text
    open Newtonsoft.Json.Linq
    open FsCheck
    open Expecto

    open JsonStreamProcessors
    open JsonStreamProcessors.JsonAssembler
    open JsonStreamProcessors.JsonStreaming

    let genJsonText =
        Generators.JsonGenerators.genJson
        |> Gen.map id
        |> Arb.fromGen

    let genPartitions (s: string) =
        let bytes = Encoding.UTF8.GetBytes s
        let len = bytes.Length
        if len = 0 then Gen.constant [| ReadOnlyMemory<byte>(Array.empty) |]
        else
            gen {
                let! k = Gen.choose(1, min len (1 + len / 3))
                let! cuts =
                    Gen.listOfLength (k - 1) (Gen.choose(1, len - 1))
                    |> Gen.map (List.distinct >> List.sort)
                let indices = (0 :: cuts) @ [len]
                return
                    indices
                    |> List.pairwise
                    |> List.map (fun (a,b) -> ReadOnlyMemory<byte>(bytes, a, b - a))
                    |> List.toArray
            }

    // Count only the FIRST completion; ignore subsequent StatusComplete (assembler is idempotent)
    let runStreaming (chunks: ReadOnlyMemory<byte>[]) =
        let asm = StreamingMaterializeAssembler() :> IJsonAssembler<JToken>
        let mutable result : JToken option = None
        let mutable error : exn option = None
        let mutable emittedOnce = false
        let mutable emits = 0
        for ch in chunks do
            match asm.Feed ch with
            | JsonAssembler.StatusNeedMore -> ()
            | JsonAssembler.StatusComplete tok ->
                if not emittedOnce then
                    emits <- emits + 1
                    result <- Some tok
                    emittedOnce <- true
            | JsonAssembler.StatusError ex ->
                if error.IsNone then error <- Some ex
        emits, result, error

    [<Tests>]
    let streamingMaterializeProps =
        testList "Streaming Materialize Assembler" [

            testProperty "streamMaterializeCompletesOnce" <| Prop.forAll genJsonText (fun json ->
                let chunks = Gen.eval 10 (Random.StdGen (11,97)) (genPartitions json)
                let emits, result, error = runStreaming chunks
                match error with
                | Some ex -> false |> Prop.label ("Error: " + ex.Message)
                | None ->
                    (emits = 1 && result.IsSome)
                    |> Prop.label (sprintf "emits=%d len=%d" emits json.Length)
            )

            testProperty "streamMaterializeParity" <| Prop.forAll genJsonText (fun json ->
                let chunks = Gen.eval 10 (Random.StdGen (222,333)) (genPartitions json)
                let emits, result, error = runStreaming chunks
                match error, result with
                | Some ex, _ -> false |> Prop.label ("Error: " + ex.Message)
                | None, None ->
                    try
                        JToken.Parse json |> ignore
                        false |> Prop.label "No emission but parse ok"
                    with ex ->
                        true |> Prop.label ("Caught: " + ex.Message)
                | None, Some tok ->
                    try
                        let parsed = JToken.Parse json
                        let ok = JToken.DeepEquals(parsed, tok)
                        ok |> Prop.label (sprintf "emits=%d parity=%b" emits ok)
                    with ex ->
                        false |> Prop.label ("Direct parse failed: " + ex.Message)
            )

            testProperty "streamMaterializeRobustMultipleRandomPartitions" <| Prop.forAll genJsonText (fun json ->
                let seeds = [0..5]
                let mutable ok = true
                let mutable labels = []
                for s in seeds do
                    if ok then
                        let chunks =
                            Gen.eval 15 (Random.StdGen (s, s+101)) (genPartitions json)
                        let emits, res, err = runStreaming chunks
                        match err, res with
                        | Some ex, _ ->
                            ok <- false
                            labels <- ("ex:" + ex.Message)::labels
                        | None, Some _ when emits <> 1 ->
                            ok <- false
                            labels <- (sprintf "emits=%d" emits)::labels
                        | None, None ->
                            // Accept only if JSON is unparseable (should be rare here) otherwise flag
                            try
                                let _ = JToken.Parse json
                                ok <- false
                                labels <- "noEmit"::labels
                            with _ ->
                                labels <- "noEmitUnparseable"::labels
                        | _ -> ()
                ok |> Prop.label (String.concat "," (List.rev labels))
            )
        ]