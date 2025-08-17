# Flow Monad and Stream Processing Architecture

## Overview

The Flow monad provides a foundational abstraction for the Flux.IO streaming framework that:

1. **Hides async implementation details** from node implementors
2. **Provides environment access** (metrics, logging, tracing, memory pools)
3. **Enables composition** of stream processing operations
4. **Supports multiple async backends** (ValueTask, Task, Async, AsyncSeq, TaskSeq)

## Core Design Principles

### 1. Type Safety Without `obj`

The entire system is built on strong typing with no use of `obj` in the public API. Stream processors explicitly declare their input and output types, ensuring type safety throughout the pipeline.

### 2. Monadic Abstraction

The `Flow<'a>` monad encapsulates:
- Asynchronous computation (via `ValueTask`)
- Environment access (via reader monad pattern)
- Cancellation token threading

```fsharp
type Flow<'a> = Flow of (ExecutionEnv -> CancellationToken -> ValueTask<'a>)
```

### 3. Stream Processing Layer

Built on top of `Flow`, the `StreamProcessor<'TIn, 'TOut>` provides:
- Type-safe envelope transformation
- Backpressure signals (`RequireMore`)
- Batch processing (`EmitMany`)
- Error propagation

## Architecture Layers

### Layer 1: Flow Monad (Low-Level)

The `Flow` monad is the fundamental building block that:

- **Abstracts async operations**: Developers write synchronous-looking code
- **Provides environment access**: Metrics, logging, tracing available implicitly
- **Handles cancellation**: Cancellation tokens are threaded automatically
- **Enables timeout**: Built-in timeout support without explicit async handling

Example usage:
```fsharp
let processWithLogging value = flow {
    let! env = Flow.ask
    env.Logger.Log("INFO", sprintf "Processing: %A" value)
    return value * 2
}
```

### Layer 2: StreamProcessor (Mid-Level)

The `StreamProcessor` builds on `Flow` to provide streaming semantics:

```fsharp
type StreamProcessor<'TIn, 'TOut> = 
    | StreamProcessor of (Envelope<'TIn> -> Flow<StreamCommand<'TOut>>)
```

Stream commands enable:
- **Emit**: Output a single processed envelope
- **EmitMany**: Output multiple envelopes (for expand operations)
- **RequireMore**: Signal backpressure (need more input)
- **Complete**: Signal stream completion
- **Error**: Propagate errors

### Layer 3: Pipeline DSL (High-Level)

The pipeline DSL will build on stream processors to provide intuitive composition:

```fsharp
pipeline {
    source fileSource
    |> StreamProcessor.lift parseJson
    |> StreamProcessor.filter (fun json -> json.HasProperty "customerId")
    |> StreamProcessor.stateful Map.empty accumulateByCustomer
    |> StreamProcessor.withEnv expandTemplate
    |> sink databaseSink
}
```

## Key Features

### 1. Zero-Copy by Default

The envelope structure carries payloads without copying:
- Uses `ReadOnlyMemory<byte>` for raw data
- Transforms can operate on views/slices
- Copying only occurs at branch points

### 2. Accumulation Support

Stateful processors enable accumulation patterns:

```fsharp
let accumulateUntilComplete requiredFields =
    StreamProcessor.stateful Map.empty (fun state envelope ->
        let newState = Map.add envelope.Key envelope.Value state
        if hasAllRequired requiredFields newState then
            newState, Some (createOutput newState)
        else
            newState, None
    )
```

### 3. Backpressure Integration

The `RequireMore` command integrates with the execution engine's backpressure mechanism:
- Processors can signal when they need more data
- The framework manages flow control
- Credit-based and adaptive strategies supported

### 4. Environment Access Pattern

Node implementors access the execution environment through the Flow monad:

```fsharp
let metricsProcessor = StreamProcessor.withEnv (fun env data ->
    flow {
        env.Metrics.RecordCounter("items_processed", HashMap.empty, 1L)
        let startTime = System.Diagnostics.Stopwatch.StartNew()
        let result = processData data
        env.Metrics.RecordHistogram("processing_time", HashMap.empty, startTime.Elapsed.TotalMilliseconds)
        return result
    }
)
```

## Implementation Details

### Type Inference Fix

The original implementation had type inference issues with `ValueTask` constructors. The fix:

1. **Explicit type annotations** on return types
2. **Proper task builder usage** for async paths
3. **Separation of completed vs async paths** in bind

### Async Backend Abstraction

The Flow monad can lift different async types:
- `liftTask`: For `Task<'a>`
- `liftValueTask`: For `ValueTask<'a>`
- `liftAsync`: For F# `Async<'a>`

This allows integration with existing libraries while maintaining a consistent programming model.

### Performance Considerations

1. **Fast path optimization**: When `ValueTask` is already completed, avoid allocation
2. **Minimal allocations**: Use struct-based `ValueTask` throughout
3. **Pooled buffers**: Integration with `IMemoryPool` for buffer management

## Usage Examples

### Simple Transform
```fsharp
let doubleProcessor = StreamProcessor.lift (fun x -> x * 2)
```

### Filtering
```fsharp
let evenOnly = StreamProcessor.filter (fun x -> x % 2 = 0)
```

### Stateful Accumulation
```fsharp
let sumEvery10 = StreamProcessor.stateful (0, 0) (fun (sum, count) value ->
    let newSum = sum + value
    let newCount = count + 1
    if newCount = 10 then
        (0, 0), Some newSum
    else
        (newSum, newCount), None
)
```

### Environment Access
```fsharp
let tracedProcessor = StreamProcessor.withEnv (fun env value ->
    flow {
        let span = env.Tracer.StartSpan("process_item", None)
        let result = complexProcessing value
        env.Tracer.EndSpan(span)
        return result
    }
)
```

## Future Extensions

### 1. Branching Support
Add combinators for splitting streams based on predicates.

### 2. Merge Operations
Support for combining multiple streams with ordering guarantees.

### 3. Window Operations
Time-based and count-based windowing with watermark support.

### 4. State Backends
Pluggable state storage for large-scale stateful processing.

## Testing Strategy

### 1. Deterministic Testing
The Flow monad can be run with a mock environment for deterministic testing:

```fsharp
let testEnv = {
    Metrics = TestMetrics()
    Logger = TestLogger()
    Tracer = TestTracer()
    Memory = TestMemoryPool()
}

let result = Flow.run testEnv CancellationToken.None computation
```

### 2. Property-Based Testing
Stream processors can be tested with FsCheck to verify properties like:
- Composition associativity
- Filter predicate correctness
- Stateful processor invariants

### 3. Performance Testing
Benchmark critical paths using BenchmarkDotNet with focus on:
- Allocation rates
- Throughput metrics
- Latency distributions

## Conclusion

The Flow monad provides a powerful abstraction that:
- Hides async complexity from node implementors
- Enables type-safe stream processing
- Supports accumulation and backpressure
- Integrates with the broader Flux.IO architecture

This design achieves the goal of allowing developers to write simple, composable stream processing logic without dealing with the complexities of async programming, while maintaining high performance and type safety throughout.