namespace Flux.IO

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open Flux.IO

type VariableMask =
  { Required : uint64
    Present  : uint64 }
  member inline x.IsComplete = (x.Required &&& x.Present) = x.Required
  static member addVar idx (m:VariableMask) =
    { m with Present = m.Present ||| (1UL <<< idx) }
  static member completeness (m:VariableMask) =
    if m.Required = 0UL then 1.0
    else float (System.Numerics.BitOperations.PopCount(int m.Present))
         / float (System.Numerics.BitOperations.PopCount(int m.Required))

type VariableSetAccumulator<'In,'Out>
  ( id:StageId,
    extractKey : Envelope<'In> -> string,
    extractVars : Envelope<'In> -> (int * obj) list,
    buildOutput : string * (int*obj) list -> 'Out,
    policy:AccumulatorPolicy ) =

  let states = Dictionary<string, AccumulatorState<string,(VariableMask * (int*obj) list)>>() // internal state

  interface Accumulator<'In,'Out,string,(VariableMask * (int*obj) list)> with
    member _.Id = id
    member _.Init _ = ValueTask()
    member _.ExtractKey e = extractKey e
    member _.UpdateState existing env =
      let key = extractKey env
      let now = env.Ts
      let vars = extractVars env
      let updated =
        match existing with
        | None ->
          let baseMask = { Required = fst policy.MaxItems |> ignore; Present = 0UL } // placeholder
          let mutable mask = baseMask
          let collected = ResizeArray()
          for (idx,v) in vars do
            mask <- VariableMask.addVar idx mask
            collected.Add(idx,v)
          { Key = key
            State = (mask, collected |> Seq.toList)
            SizeItems = 1
            SizeBytes = env.Cost.Bytes
            FirstTs = now
            LastTs = now
            CompletenessScore = VariableMask.completeness mask }
        | Some st ->
          let (mask, items) = st.State
          let mutable mask2 = mask
            // Add duplicates simplistically; dedupe outside scope now
          let appended =
            vars
            |> List.fold (fun (m,lst) (idx,v) ->
                (VariableMask.addVar idx m, (idx,v)::lst)) (mask, items)
          let maskf, itemsf = appended
          { st with
              State = (maskf, itemsf)
              SizeItems = st.SizeItems + 1
              SizeBytes = st.SizeBytes + env.Cost.Bytes
              LastTs = now
              CompletenessScore = VariableMask.completeness maskf }
      updated
    member _.CheckComplete st =
      let (mask, items) = st.State
      let now = st.LastTs
      let ttlExpired =
        match policy.CompletenessTtl with
        | None -> false
        | Some ttl -> (now - st.FirstTs) > int64 ttl.TotalMilliseconds
      if mask.IsComplete then
        let out = buildOutput(st.Key, items)
        Complete(out, st.State)
      elif ttlExpired && policy.PartialFlush then
        let out = buildOutput(st.Key, items)
        Expired(Some out, st.State)
      else
        Incomplete st.State
    member _.FlushForced st =
      let (_, items) = st.State
      Some (buildOutput(st.Key, items))
    member _.SerializeSpill st =
      // Placeholder (not implementing durable format yet)
      ReadOnlyMemory<byte>(Array.zeroCreate 0)
    member _.DeserializeSpill _ =
      raise (NotImplementedException "Spill restore not implemented")
    member _.Close () = ValueTask()
