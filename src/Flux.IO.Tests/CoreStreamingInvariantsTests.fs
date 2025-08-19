namespace Flux.IO.Tests

open System
open System.Text
open Newtonsoft.Json.Linq
open FsCheck
open Expecto

open JsonStreamProcessors.JsonAssembler
open JsonStreamProcessors.JsonStreamingInstrumentation

module StreamingInvariantsTests =

    let genJsonText =
        Generators.JsonGenerators.genJson
        |> Arb.fromGen

    let genPartitions (s:string) =
        let bytes = Encoding.UTF8.GetBytes s
        let len = bytes.Length
        if len = 0 then Gen.constant [| ReadOnlyMemory<byte>(Array.empty) |]
        else
            gen {
                let! k = Gen.choose(1, min len (1 + len / 3))
                let! cuts =
                    Gen.listOfLength (k - 1) (Gen.choose(1, len - 1))
                    |> Gen.map (List.distinct >> List.sort)
                let idxs = (0::cuts) @ [len]
                return
                    idxs
                    |> List.pairwise
                    |> List.map (fun (a,b) -> ReadOnlyMemory<byte>(bytes, a, b - a))
                    |> List.toArray
            }

    let runInstrumented (chunks: ReadOnlyMemory<byte>[]) =
        let instr = createInstrumentation()
        let asm = InstrumentedStreamingMaterializeAssembler(instr=instr) :> IJsonAssembler<JToken>
        let mutable result : JToken option = None
        let mutable error : exn option = None
        for ch in chunks do
            match asm.Feed ch with
            | JsonStreamProcessors.JsonAssembler.StatusNeedMore -> ()
            | JsonStreamProcessors.JsonAssembler.StatusComplete tok ->
                result <- Some tok
            | JsonStreamProcessors.JsonAssembler.StatusError ex ->
                error <- Some ex
        instr, result, error

    [<Tests>]
    let streamingInvariantProps =
        testList "Streaming Invariants" [

            testProperty "depthNeverNegativeAndEndsAtZero" <| Prop.forAll genJsonText (fun json ->
                let chunks = Gen.eval 10 (Random.StdGen (1,2)) (genPartitions json)
                let instr, result, error = runInstrumented chunks
                match error with
                | Some ex -> false |> Prop.label ("Error: " + ex.Message)
                | None ->
                    let negative = instr.DepthTrace |> Seq.exists (fun d -> d < 0)
                    let finalZero = if instr.Completed then (instr.DepthTrace.Count = 0 || instr.DepthTrace[instr.DepthTrace.Count-1] = 0) else true
                    let structuralBalanced =
                        instr.StartObj = instr.EndObj && instr.StartArr = instr.EndArr
                    (not negative && finalZero && structuralBalanced && result.IsSome)
                    |> Prop.label (sprintf "objs=%d/%d arrs=%d/%d tokens=%d" instr.StartObj instr.EndObj instr.StartArr instr.EndArr instr.TokenCount)
            )
        ]