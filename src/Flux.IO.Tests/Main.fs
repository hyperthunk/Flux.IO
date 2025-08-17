namespace Flux.IO.Tests

module TestRunner =

    open Expecto
    open Flux.IO.Tests.CoreTests

    [<EntryPoint>]
    let main argv =

        let allTests = testList "Flux.IO" [
            coreTests
        ]

        allTests
        |> runTestsWithCLIArgs [] argv
