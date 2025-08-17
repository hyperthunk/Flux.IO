namespace Flux.IO

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FSharpPlus

open Flux.IO

type PipelineBuilder() =
  member _.Yield (s:Stage<'a,'b>) = Pipeline.singleton s
  [<CustomOperation("thenStage")>]
  member _.Then (p:Pipeline<'a,'b>, s:Stage<'b,'c>) = Pipeline.append s p
  member _.Run (p:Pipeline<'a,'b>) = p

let pipeline = PipelineBuilder()

// Convenience stage constructors

module Stages =
  open System.Runtime.CompilerServices

  let map (id:string) (f:'a -> 'b) =
    { new Stage<'a,'b> with
        member _.Id = id
        member _.Kind = Transform
        member _.Init _ = ValueTask()
        member _.Process ct (input:StreamIn<'a>) =
          let e = input.GetAsyncEnumerator(ct)
          let seq =
            let asyncSeq =
              taskSeq {
                while
                  let! mv = e.MoveNextAsync()
                  mv
                do
                  let current = e.Current
                  let mapped = Envelope.map f current
                  yield mapped
              }
            asyncSeq
          seq :> StreamOut<'b>
        member _.Close () = ValueTask()
    }

  let sinkIgnore (id:string) =
    { new Stage<'a,unit> with
        member _.Id = id
        member _.Kind = Sink
        member _.Init _ = ValueTask()
        member _.Process ct (input:StreamIn<'a>) =
          let e = input.GetAsyncEnumerator(ct)
          let seq =
            let asyncSeq =
              taskSeq {
                while
                  let! mv = e.MoveNextAsync()
                  mv
                do
                  ()
              }
            asyncSeq
          seq :> StreamOut<unit>
        member _.Close () = ValueTask()
    }
