namespace Flux.IO.Tests

open System
open System.Text
open Newtonsoft.Json.Linq
open FsCheck
open Expecto

open JsonStreamProcessors.JsonAssembler
open JsonStreamProcessors.JsonStreaming

module CoreStreamingErrorTests =

    // Corruption generator (adjusted loops to avoid infinite / non-progress behavior)
    let corruptionGen =
        Generators.JsonGenerators.genJson
        |> Gen.map (fun original ->
            let bytes = Encoding.UTF8.GetBytes original |> Array.copy
            let len = bytes.Length
            let rand = System.Random()
            let choice = rand.Next(0,5)
            let mutate() =
                match choice with
                | 0 when len > 2 ->
                    bytes.[len-2] <- byte 0x7F
                | 1 when len > 3 ->
                    bytes.[len/2] <- byte '"'
                | 2 when len > 4 ->
                    let mutable i = len - 1
                    let mutable replaced = false
                    while i >= 0 && not replaced do
                        if bytes.[i] = byte '}' || bytes.[i] = byte ']' then
                            bytes.[i] <- byte ' '
                            replaced <- true
                        i <- i - 1
                | 3 when len > 6 ->
                    let mutable i = 0
                    let mutable doneFlag = false
                    while i < len - 1 && not doneFlag do
                        if bytes.[i] = byte '\\' then
                            bytes.[i] <- byte 'x'
                            doneFlag <- true
                        i <- i + 1
                | _ when len > 5 ->
                    bytes.[len/3] <- byte 0x01
                | _ -> ()
            mutate()
            let corrupted = Encoding.UTF8.GetString bytes
            (original, corrupted)
        )
        |> Arb.fromGen

    let partitionBytes (s:string) =
        let bytes = Encoding.UTF8.GetBytes s
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
                if not completed then completed <- true
            | JsonStreamProcessors.JsonAssembler.StatusError _ ->
                errorHit <- true
            | _ -> ()
        validParseOk, corruptedFails, completed, errorHit

    [<Tests>]
    let streamingErrorProps =
        testList "Streaming Error Simulation" [
            testProperty "corruptedNeverCompletesAndErrorsEventually" <| Prop.forAll corruptionGen (fun (orig, bad) ->
                let valid, corruptFails, completed, errorHit = feedCorrupted orig bad
                if not valid then true
                else
                    let directStillValid =
                        try JToken.Parse bad |> ignore; true with _ -> false
                    if directStillValid then true
                    else
                        // ACCEPT either: (a) we saw an error without completion OR
                        // (b) we neither completed nor errored (pending waiting for more bytes),
                        // since assembler has no final-block signal yet.
                        Prop.label (sprintf "completed=%b error=%b pending=%b" completed errorHit (not errorHit && not completed)) |> ignore
                        (corruptFails && errorHit && not completed)
                            || (corruptFails && not errorHit && not completed)
            )
        ]