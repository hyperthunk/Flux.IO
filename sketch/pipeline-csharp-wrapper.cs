using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;

namespace AsyncPipeline.CSharp
{
    // Core interfaces with I prefix for C# conventions
    public interface IEnvelope<T>
    {
        T Payload { get; }
        IReadOnlyDictionary<string, string> Headers { get; }
        long SeqId { get; }
        ITraceContext SpanCtx { get; }
        long Timestamp { get; }
        IReadOnlyDictionary<string, object> Attrs { get; }
        ICost Cost { get; }
    }

    public interface ITraceContext
    {
        string TraceId { get; }
        string SpanId { get; }
        IReadOnlyDictionary<string, string> Baggage { get; }
    }

    public interface ICost
    {
        int Bytes { get; }
        double CpuHint { get; }
    }

    public interface IStage<in TIn, TOut>
    {
        string Id { get; }
        StageKind Kind { get; }
        ValueTask Init(IStageContext ctx);
        IAsyncEnumerable<IEnvelope<TOut>> Process(CancellationToken ct, IAsyncEnumerable<IEnvelope<TIn>> input);
        ValueTask Close();
    }

    public interface IStageContext
    {
        string PipelineId { get; }
        string Stage