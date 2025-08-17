namespace Flux.IO

open System
open System.Threading
open System.Threading.Tasks
open FSharpPlus

type ExecutionOptions<'In,'Out> =
  { Env : ExecutionEnv
    Observer : Observer
    Cancellation : CancellationToken }

type Compiled<'In,'Out> =
  { Run : StreamIn<'In> -> CancellationToken -> StreamOut<'Out> }

module private ExecInternal =
  let rec compose (pipe:Pipeline<'A,'B>) : (StreamIn<'A> -> CancellationToken -> StreamOut<'B>) =
    match pipe with
    | Terminal s ->
      fun input ct -> s.Process ct input
    | Chain (s,rest) ->
      let tail = compose rest
      fun input ct ->
        let mid = s.Process ct input
        tail mid ct

type SyncExecutionEngine() =
  member _.Execute<'A,'B>(pipe:Pipeline<'A,'B>, input:StreamIn<'A>, opts:ExecutionOptions<'A,'B>) =
    // For sync engine we simply drain sequentially; still using async enumerables for interface parity
    let runner = ExecInternal.compose pipe
    runner input opts.Cancellation

type AsyncExecutionEngine() =
  member _.Execute<'A,'B>(pipe:Pipeline<'A,'B>, input:StreamIn<'A>, opts:ExecutionOptions<'A,'B>) =
    let runner = ExecInternal.compose pipe
    runner input opts.Cancellation
