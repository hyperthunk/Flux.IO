namespace AsyncPipeline.Core

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

// Core data model
type Timestamp = int64

type TraceContext = {
    TraceId: string
    SpanId: string
    Baggage: IReadOnlyDictionary<string, string>
}

type Cost = {
    Bytes: int
    CpuHint: float
}

type Envelope<'T> = {
    Payload: 'T
    Headers: Map<string, string>
    SeqId: int64
    SpanCtx: TraceContext
    Timestamp: Timestamp
    Attrs: IReadOnlyDictionary<string, obj>
    Cost: Cost
}

type Batch<'T> = {
    Items: Envelope<'T> array
    WindowStart: Timestamp
    WindowEnd: Timestamp
    TotalBytes: int
    TotalItems: int
}

// Stage types
type StageKind =
    | Source
    | Transform
    | Accumulator
    | Branch
    | Merge
    | Sink

type StageContext = {
    PipelineId: string
    StageId: string
    Config: obj
    Logger: ILogger
    Tracer: ITracer
}

and ILogger =
    abstract member Log: level:string -> message:string -> unit
    abstract member LogError: message:string -> exn:Exception -> unit

and ITracer =
    abstract member StartSpan: name:string -> parent:TraceContext option -> TraceContext
    abstract member EndSpan: ctx:TraceContext -> unit
    abstract member AddEvent: ctx:TraceContext -> name:string -> attrs:Map<string,obj> -> unit

// Stream abstractions
type StreamIn<'T> = IAsyncEnumerable<Envelope<'T>>
type StreamOut<'T> = IAsyncEnumerable<Envelope<'T>>

// Core stage interface (F# style without I prefix)
type Stage<'In, 'Out> =
    abstract member Id: string
    abstract member Kind: StageKind
    abstract member Init: StageContext -> ValueTask
    abstract member Process: ct:CancellationToken -> input:StreamIn<'In> -> StreamOut<'Out>
    abstract member Close: unit -> ValueTask

// Accumulation model
type CompletenessResult<'State, 'Out> =
    | Incomplete of 'State
    | Complete of 'Out * 'State
    | Expired of 'Out option * 'State

type CompletenessChecker<'State, 'In, 'Out> =
    'State -> Envelope<'In> -> CompletenessResult<'State, 'Out>

type AccumulatorPolicy = {
    MaxItems: int option
    MaxBytes: int option
    MaxLatency: TimeSpan option
    CompletenessTTL: TimeSpan option
    PartialFlush: bool
    SpillAllowed: bool
}

type AccumulatorState<'Key, 'State> = {
    Key: 'Key
    State: 'State
    SizeItems: int
    SizeBytes: int
    FirstTs: Timestamp
    LastTs: Timestamp
    CompletenessScore: float
}

type Accumulator<'In, 'Out, 'Key, 'State> =
    abstract member Id: string
    abstract member Init: StageContext -> ValueTask
    abstract member ExtractKey: Envelope<'In> -> 'Key
    abstract member UpdateState: AccumulatorState<'Key, 'State> option -> Envelope<'In> -> AccumulatorState<'Key, 'State>
    abstract member CheckComplete: AccumulatorState<'Key, 'State> -> CompletenessResult<'State, 'Out>
    abstract member FlushForced: AccumulatorState<'Key, 'State> -> 'Out option
    abstract member SerializeSpill: AccumulatorState<'Key, 'State> -> ReadOnlyMemory<byte>
    abstract member DeserializeSpill: ReadOnlyMemory<byte> -> AccumulatorState<'Key, 'State>
    abstract member Close: unit -> ValueTask

// Execution options
type ParallelismHint =
    | Fixed of int
    | DynamicCpuBound
    | DynamicIoBound
    | Sequential

type BufferPolicy = {
    Capacity: int
    DropPolicy: DropPolicy
}

and DropPolicy =
    | DropOldest
    | DropNewest
    | Block

type BackpressureStrategy =
    | Bounded of capacity:int * dropPolicy:DropPolicy
    | CreditBased of initial:int * replenishThreshold:int
    | RateShaping of rate:int * burst:int
    | Adaptive of minBatch:int * maxBatch:int
    | ReactivePull of windowSize:int

type ErrorPolicy =
    | Retry of maxRetries:int * backoff:TimeSpan
    | DeadLetter
    | Fail
    | Ignore

type CircuitBreakerPolicy = {
    ErrorThreshold: float
    LatencyThreshold: TimeSpan
    WindowSize: TimeSpan
    MinimumThroughput: int
    CooldownPeriod: TimeSpan
}

// Metrics and monitoring
type IMetrics =
    abstract member RecordCounter: name:string -> tags:Map<string,string> -> value:int64 -> unit
    abstract member RecordGauge: name:string -> tags:Map<string,string> -> value:float -> unit
    abstract member RecordHistogram: name:string -> tags:Map<string,string> -> value:float -> unit

type StageRuntimeState = {
    StageId: string
    State: string
    InputQueueDepth: int
    OutputQueueDepth: int
    ProcessedCount: int64
    ErrorCount: int64
    LastError: Exception option
    Metrics: Map<string, float>
}

// Memory management
type IMemoryPool =
    abstract member RentBuffer: size:int -> ArraySegment<byte>
    abstract member ReturnBuffer: buffer:ArraySegment<byte> -> unit
    abstract member GetStatistics: unit -> MemoryPoolStats

and MemoryPoolStats = {
    TotalAllocated: int64
    CurrentlyRented: int64
    PeakUsage: int64
    FragmentationRatio: float
}

// Policy registry
type PolicyRegistry = {
    ErrorPolicies: Map<string, ErrorPolicy>
    BackpressurePolicies: Map<string, BackpressureStrategy>
    CircuitBreakerPolicies: Map<string, CircuitBreakerPolicy>
    BufferPolicies: Map<string, BufferPolicy>
}

// Execution engine
type ExecutionOptions = {
    Cancellation: CancellationToken
    Metrics: IMetrics
    Tracer: ITracer
    Logger: ILogger
    MemoryPool: IMemoryPool
    Policies: PolicyRegistry
}

type PipelineResult = {
    Success: bool
    ProcessedCount: int64
    ErrorCount: int64
    Duration: TimeSpan
    FinalStates: Map<string, StageRuntimeState>
}

type ExecutionEngine =
    abstract member Run: graph:PipelineGraph * options:ExecutionOptions -> Task<PipelineResult>
    abstract member Inspect: stageId:string -> StageRuntimeState
    abstract member Stop: unit -> Task

// Pipeline graph
and AccumulationDescriptor = {
    Policy: AccumulatorPolicy
    EngineHint: ParallelismHint
    ProgressWeights: Map<string, float>
}

and StageNode = {
    Stage: obj
    Kind: StageKind
    Parallelism: ParallelismHint
    Accumulation: AccumulationDescriptor option
    BufferPolicy: BufferPolicy option
    Backpressure: BackpressureStrategy option
    ErrorPolicy: ErrorPolicy
}

and Edge = {
    From: string
    To: string
    Channel: ChannelDescriptor
}

and ChannelDescriptor = {
    BufferSize: int
    Ordering: ChannelOrdering
}

and ChannelOrdering =
    | Unordered
    | Ordered
    | PartitionOrdered of keyExtractor:(obj -> string)

and PipelineGraph = {
    Nodes: StageNode list
    Edges: Edge list
    Metadata: Map<string, string>
}

// Pipeline builder result
type Pipeline = {
    Graph: PipelineGraph
    ValidationErrors: string list
}