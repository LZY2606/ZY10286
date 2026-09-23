#nullable enable

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ZLogger.Tests.Harness;

/// <summary>
/// BatchingAsyncLogProcessor whose batch consumption is driven by ticks
/// (ProcessGate) instead of time. Records batch boundaries and writes formatted
/// markers into a sink so conservation invariants can be checked.
/// </summary>
public sealed class GatedBatchingProcessor : BatchingAsyncLogProcessor
{
    readonly EventLog events;
    readonly ProgrammableFormatter formatter;
    readonly MemoryStream sink = new();
    readonly List<int[]> batches = new();

    public GatedBatchingProcessor(int batchSize, EventLog events)
        : base(batchSize, new ZLoggerOptions
        {
            InternalErrorLogger = ex => events.Add(EventKind.Error, -1, ex.GetType().Name),
        })
    {
        this.events = events;
        formatter = new ProgrammableFormatter(events);
    }

    /// <summary>One Set allows exactly one batch to be processed (a "batch tick").</summary>
    public ManualResetEventSlim ProcessGate { get; } = new(false);

    /// <summary>Mutation knob: flushes every batch a second time (duplicate output).</summary>
    public bool DuplicateFlush { get; set; }

    /// <summary>When set, ProcessAsync throws this fault after the gate (before flushing).</summary>
    public Exception? ProcessFault { get; set; }

    public IReadOnlyList<int[]> Batches
    {
        get
        {
            lock (batches)
            {
                return batches.ToArray();
            }
        }
    }

    public byte[] SinkBytes
    {
        get
        {
            lock (sink)
            {
                return sink.ToArray();
            }
        }
    }

    protected override ValueTask ProcessAsync(IReadOnlyList<INonReturnableZLoggerEntry> list)
    {
        events.Add(EventKind.BatchStarted, -1, $"count={list.Count}");
        ProcessGate.Wait();
        ProcessGate.Reset();

        var ids = list.Select(x => ((TrackingEntry)x).Id).ToArray();
        lock (batches)
        {
            batches.Add(ids);
        }

        if (ProcessFault != null)
        {
            throw ProcessFault;
        }

        FlushBatch(list);
        if (DuplicateFlush)
        {
            FlushBatch(list); // mutation: duplicate batch flush
        }
        return default;
    }

    void FlushBatch(IReadOnlyList<INonReturnableZLoggerEntry> list)
    {
        var buffer = new ArrayBufferWriter<byte>();
        foreach (var entry in list)
        {
            entry.FormatUtf8(buffer, formatter);
        }
        lock (sink)
        {
            sink.Write(buffer.WrittenSpan);
        }
    }

    protected override ValueTask DisposeAsyncCore()
    {
        events.Add(EventKind.Completed);
        return default;
    }
}
