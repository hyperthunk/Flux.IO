# Flux.IO Prototype (Iteration P0–P2)

This repository contains the initial implementation of the **Flux.IO** subsystem:
- Type-safe streaming pipeline (no `obj` in core graph)
- Zero-copy buffer pool
- Accumulator model (variable set + TTL + partial flush)
- Backpressure primitives (bounded channel + strategies)
- Execution engines (Sync + Async)
- Planning (basic fusion of pure transforms)

See `IO-Subsystem-Design.md` for architecture details and `TEST-PLAN.md` for validation strategy.

> NOTE: Advanced features (URI connector registry, durable state backends, checkpointing) are intentionally deferred.

## Quick Start (Sample)

```fsharp
open Flux.IO
open Flux.IO.Stages

let parse = Stages.map "parse-int" int
let sink  = Stages.sinkIgnore "noop-sink"

let pipe =
  Pipeline.Chain (parse, Pipeline.Terminal sink)

// Execute with a simple async source (example placeholder)