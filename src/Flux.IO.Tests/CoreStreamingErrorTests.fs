namespace Flux.IO.Tests

open System
open System.Text
open Newtonsoft.Json.Linq
open FsCheck
open Expecto

open JsonStreamProcessors.JsonAssembler
open JsonStreamProcessors.JsonStreaming

module StreamingErrorTests =

    // Take valid JSON and corrupt it. We return original + corrupted
    let corruptionGen =
        Generators.JsonGenerators.genJson
        |> Gen.map (fun original ->
            let bytes = Encoding.UTF8.GetBytes original |> Array.copy
            let len = bytes.Length
            let rand = System.Random()
            let choice = rand.Next(0,5)
            let corrupt() =
                match choice with
                | 0 when len > 2 ->
                    // remove last non-whitespace byte (truncate)
                    bytes.[len-2] <- byte 0x7F
                | 1 when len > 3 ->
                    // insert unmatched quote
                    bytes.[len/2] <- byte '"'
                | 2 when len > 4 ->
                    // drop a closing brace/ bracket if present
                    for i = len-1 downto 0 do
                        if bytes.[i] = byte '}' || bytes.[i] = byte ']' then
                            bytes.[i] <- byte ' '
                            i <- -1
                | 3 when len > 6 ->
                    // break escape: replace backslash with plain 'x'
                    for i = 0 to len-2 do
                        if bytes.[i] = byte '\\' then
                            bytes.[i] <- byte 'x'
                            i <- len
                | _ when len > 5 ->
                    // inject control char
                    bytes.[len/3] <- byte 0x01
                | _ -> ()
            corrupt()
            let corrupted = Encoding.UTF8.GetString bytes
            (original, corrupted)
        )
        |> Arb.fromGen

    let partitionBytes (s:string) =
        let bytes = Encoding.UTF8.GetBytes s
        // Simple random small chunks
        let rand = System.Random()
        let rec loop i acc =
            if i >= bytes.Length then List.rev acc
            else
                let take = min (rand.Next(1, 16)) (bytes.Length - i)
                loop (i + take) (ReadOnlyMemory<byte>(bytes, i, take)::acc)
        loop 0 [] |> List.toArray

    let feedCorrupted (jsonValid:string) (jsonBad:string) =
        let validParseOk =
            try JToken.Parse jsonValid |> ignore; true
            with _ -> false
        let corruptedFails =
            try JToken.Parse jsonBad |> ignore; false
            with _ -> true

        let asm = StreamingMaterializeAssembler() :> IJsonAssembler<JToken>
        let chunks = partitionBytes jsonBad
        let mutable completed = false
        let mutable errorHit = false
        for ch in chunks do
            match asm.Feed ch with
            | JsonStreamProcessors.JsonAssembler.StatusComplete _ ->
                completed <- true
            | JsonStreamProcessors.JsonAssembler.StatusError _ ->
                errorHit <- true
            | _ -> ()
        validParseOk, corruptedFails, completed, errorHit

    [<Tests>]
    let streamingErrorProps =
        testList "Streaming Error Simulation" [
            testProperty "corruptedNeverCompletesAndErrorsEventually" <| Prop.forAll corruptionGen (fun (orig, bad) ->
                let valid, corruptFails, completed, errorHit = feedCorrupted orig bad
                if not valid then true // original invalid; skip classification
                else
                    // If corruption produces a still-valid JSON (rare accidental), allow pass.
                    let directStillValid =
                        try JToken.Parse bad |> ignore; true with _ -> false
                    if directStillValid then true
                    else
                        // Expect: did not complete, eventually errored
                        (corruptFails && not completed && errorHit)
                        |> Prop.label (sprintf "completed=%b error=%b" completed errorHit)
            )
        ]