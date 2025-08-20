namespace Flux.IO.Tests.Benchmarks

open System
open System.Text
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open Newtonsoft.Json.Linq

open JsonStreamProcessors
open JsonStreamProcessors.JsonAssembler
open JsonStreamProcessors.JsonFramer
open JsonStreamProcessors.JsonStreaming

[<MemoryDiagnoser>]
type AssemblerBenchmarks () =

    [<Params("SmallFlat","MediumNested","Large","Pathological")>]
    member val Scenario = "" with get, set

    [<Params("Single","UniformSmall","Mixed","Byte")>]
    member val ChunkPattern = "" with get, set

    let mutable jsonBytes : byte[] = [||]
    let mutable chunks : ReadOnlyMemory<byte>[] = [||]

    let mkJson scenario =
        // Simplistic synthetic generator; replace with real generator if desired
        match scenario with
        | "SmallFlat" ->
            """{"a":1,"b":2,"c":"x","d":true}"""
        | "MediumNested" ->
            let inner = String.replicate 20 """{"k":123,"arr":[1,2,3,4],"s":"abcdef"}"""
            sprintf """{"root":[%s]}""" inner
        | "Large" ->
            let block = """{"n":123,"s":"abcdefghijklmnopqrstuvwxyz0123456789","deep":[{"a":1},{"b":2},{"c":3}]}"""
            let big = String.replicate 500 block
            sprintf """{"big":[%s],"tail":999}""" big
        | "Pathological" ->
            let props =
                [for i in 1..800 -> sprintf "\"k%d\":\"%s\"" i (Guid.NewGuid().ToString("N"))]
                |> String.concat ","
            sprintf "{%s}" props
        | _ -> "{}"

    let partition (pattern:string) (bytes: byte[]) =
        let len = bytes.Length
        match pattern with
        | "Single" -> [| ReadOnlyMemory<byte>(bytes) |]
        | "UniformSmall" ->
            let rec loop i acc =
                if i >= len then List.rev acc
                else
                    let take = min 64 (len - i)
                    loop (i + take) (ReadOnlyMemory<byte>(bytes, i, take)::acc)
            loop 0 [] |> List.toArray
        | "Mixed" ->
            let rand = Random 42
            let rec loop i acc =
                if i >= len then List.rev acc
                else
                    let take = min (rand.Next(8,256)) (len - i)
                    loop (i + take) (ReadOnlyMemory<byte>(bytes, i, take)::acc)
            loop 0 [] |> List.toArray
        | "Byte" ->
            [| for i in 0 .. len-1 -> ReadOnlyMemory<byte>(bytes, i, 1) |]
        | _ -> [| ReadOnlyMemory<byte>(bytes) |]

    [<GlobalSetup>]
    member this.Setup() =
        let json = mkJson this.Scenario
        jsonBytes <- Encoding.UTF8.GetBytes json
        chunks <- partition this.ChunkPattern jsonBytes

    let runAssembler (asm: IJsonAssembler<JToken>) =
        for ch in chunks do
            match asm.Feed ch with
            | JsonAssembler.StatusComplete _ -> ()
            | _ -> ()
        ()

    [<Benchmark(Baseline=true)>]
    member _.LengthGateAssembler () =
        let asm = LengthAssembler(jsonBytes.Length) :> IJsonAssembler<JToken>
        runAssembler asm

    [<Benchmark>]
    member _.FramingAssembler () =
        let asm = FramingAssembler() :> IJsonAssembler<JToken>
        runAssembler asm

    [<Benchmark>]
    member _.StreamingMaterializeAssembler () =
        let asm = StreamingMaterializeAssembler() :> IJsonAssembler<JToken>
        runAssembler asm

module Program =
    [<EntryPoint>]
    let main argv =
        BenchmarkRunner.Run<AssemblerBenchmarks>() |> ignore
        0