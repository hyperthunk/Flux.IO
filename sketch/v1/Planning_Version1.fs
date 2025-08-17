namespace Flux.IO

open System
open System.Threading

module Planning =

  type PlanStage<'In,'Out> =
    { Stage : Stage<'In,'Out>
      Fusible : bool }

  type ExecutionPlan<'In,'Out> =
    { Root : Pipeline<'In,'Out>
      Fused : int
      EstimatedMaxLatencyMs : int
      Notes : string list }

  let analyze<'A,'B> (isPure:Stage<'x,'y> -> bool) (pipe:Pipeline<'A,'B>) =
    let fused = Pipeline.fuse isPure pipe
    { Root = fused
      Fused = 0 // placeholder – implement traversal
      EstimatedMaxLatencyMs = 0
      Notes = [] }
