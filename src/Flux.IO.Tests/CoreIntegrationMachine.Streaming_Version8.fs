namespace Flux.IO.Tests

open System
open Newtonsoft.Json.Linq
open Flux.IO
open Flux.IO.Core
open Flux.IO.Core.Flow
open CoreIntegrationPipeline
open CoreIntegrationPipeline.PipelineExec
open CoreIntegrationStreamingPipeline
open TokenStreaming
open IntegrationStages
open FsCheck

module CoreIntegrationMachineStreaming =

    type StreamingRuntime = {
        mutable SourceEmitted : bool
        mutable ToBytesRun    : bool
        mutable TokensSeen    : int
        mutable RootCompleted : bool
        mutable AccumDone     : bool
        mutable ProjectionScalars : bool
        mutable ProjectionObjects : bool
    }

    /// Extend existing runtime union via wrapping inside original dictionary (keep separate dictionary optional).
    type ExtendedRuntimePipeline =
        | RStream of StreamingPipeline * StreamingRuntime

    // (Integration with existing Sut: you can either add a second dictionary
    // or unify runtime union. For minimal change, add optional second store.)