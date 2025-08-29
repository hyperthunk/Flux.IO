namespace Flux.IO.Tests

module TestRunner =

    open Expecto
    open Flux.IO.Tests
    open Flux.IO.Tests.CoreTests
    open Flux.IO.Tests.FramingTests
    open Flux.IO.Tests.CoreStreamingErrorTests
    open Flux.IO.Tests.CoreStreamingInvariantsTests
    open Flux.IO.Tests.CoreStreamingMaterializeTests
    open Flux.IO.Tests.CoreIntegrationTestSuite
    open Flux.IO.Tests.IntermediaryModelTests
    open Flux.IO.Tests.ExternalHandleTests.Tests
    // open Flux.IO.Tests.CoreStreamProcessingTests

    [<EntryPoint>]
    let main argv =

        let allTests = testList "Flux.IO" [
            // jsonPipelineTests
            streamingErrorProps
            streamingInvariantProps
            framingProperties
            streamingMaterializeProps
            coreIntegrationTests
            coreTests
            intermediaryTests
            CoreMachine.streamProcessorModelTests
            externalHandleTests
        ]

        allTests
        |> runTestsWithCLIArgs [] argv
