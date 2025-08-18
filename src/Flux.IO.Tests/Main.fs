namespace Flux.IO.Tests

module TestRunner =

    open Expecto
    open Flux.IO.Tests
    open Flux.IO.Tests.CoreTests
    // open Flux.IO.Tests.CoreStreamProcessingTests

    [<EntryPoint>]
    let main argv =

        let allTests = testList "Flux.IO" [
            // jsonPipelineTests
            coreTests
            CoreMachine.streamProcessorModelTests
        ]

        allTests
        |> runTestsWithCLIArgs [] argv
