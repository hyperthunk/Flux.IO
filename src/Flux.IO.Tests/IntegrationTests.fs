namespace Flux.IO.Tests.Integration

open Expecto
open FsCheck
open Flux.IO.Tests.Integration.IntegrationModel

module IntegrationTests =

    // Re-export the machine-based properties so they can live alongside existing test suites
    [<Tests>]
    let streamProcessorIntegrationSuite =
        IntegrationProperties.integrationModelProperties

    // Additional high-level scenario distribution smoke test (not model-based) to ensure weighting approximates targets.
    //
    // This is a lightweight statistical property (soft assertion) — it does NOT enforce strict equality,
    // only that each mandatory class appears at least once within a bounded run set.
    //
    // Rationale: Guards regressions where a refactor accidentally removes scenario generation paths.
    [<Tests>]
    let scenarioDistribution =
        testCase "Scenario generation covers required classes (smoke)" <| fun _ ->
            let machine = IntegrationMachine()
            let setup = Arb.generate<Setup<_,_>> |> Gen.sample 1 1 |> Seq.head
            let sut = setup.Actual()
            let mutable seenChunk = false
            let mutable seenAccum = false
            let mutable seenDirect = false
            let mutable iterations = 0
            let mutable model = setup.Model()
            let rnd = System.Random(1234)
            while iterations < 200 && (not (seenChunk && seenAccum && seenDirect)) do
                // sample a next op
                let genOp = machine.Next model
                let op = Gen.eval 10 (Random.StdGen (rnd.Next(), rnd.Next())) genOp
                // "Run" path: apply and update model
                let model' =
                    let sutObj = sut
                    (op :?> FsCheck.Experimental.Operation<_,_>).Run(model)
                // naive command classification by string
                let s = op.ToString()
                if s.Contains("Chunking") then seenChunk <- true
                if s.Contains("Accumul") then seenAccum <- true
                if s.Contains("Direct") then seenDirect <- true
                model <- model'
                iterations <- iterations + 1
            Expect.isTrue (seenChunk && seenAccum && seenDirect) "All mandatory scenario kinds should be generated at least once."
