namespace Flux.IO.Tests

module CoreStreamProcessingTests = 

    open Expecto
    open FsCheck
    open Generators

    [<Tests>]
    let jsonPipelineTests =
        testList "JsonPipelineTests" [
            testProperty "JsonData" <| fun () ->
                let json = JsonGenerators.genJson |> Gen.sample 100 1
                Expect.isNonEmpty json "Expected some generated JSON"
                json |> List.iter (fun s -> 
                    printfn "Generated JSON: %A" s
                    Expect.isNotEmpty s "Expected non-empty JSON"
                )
        ]