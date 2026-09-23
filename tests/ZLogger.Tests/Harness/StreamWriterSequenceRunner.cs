#nullable enable

using System;
using System.Collections.Generic;
using System.Threading;

namespace ZLogger.Tests.Harness;

/// <summary>
/// Executes a LogAction sequence against a real AsyncStreamLineMessageWriter
/// (Grow mode) wired to a programmable stream/formatter, then checks the
/// conservation invariants. All synchronization is event/barrier driven.
/// </summary>
public sealed class StreamWriterSequenceRunner
{
    readonly EventLog events = new();
    readonly ProgrammableStream stream;
    readonly ProgrammableFormatter formatter;
    readonly AsyncStreamLineMessageWriter processor;
    readonly CancellationTokenSource cancellation = new();
    readonly List<TrackingEntry> entries = new();
    readonly HashSet<int> expectedAbsentFromOutput = new();
    readonly List<string> violations = new();
    int nextId;
    bool disposed;
    bool producerCancelled;

    public StreamWriterSequenceRunner()
    {
        stream = new ProgrammableStream(events, writeGateInitiallyOpen: true);
        formatter = new ProgrammableFormatter(events);
        stream.Cancellation = cancellation.Token;

        var options = new ZLoggerOptions
        {
            FullMode = BackgroundBufferFullMode.Grow,
            InternalErrorLogger = ex => events.Add(EventKind.Error, -1, ex.GetType().Name),
        };
        options.UseFormatter(() => formatter);
        processor = new AsyncStreamLineMessageWriter(stream, options);
    }

    public EventLog Events => events;
    public IReadOnlyList<TrackingEntry> Entries => entries;

    /// <summary>Mutation hook: customize entry creation (e.g. suppress release).</summary>
    public Func<int, EventLog, TrackingEntry>? EntryFactory { get; set; }

    /// <summary>
    /// Deadlock guard for Tick barriers. Never gates progress in passing runs;
    /// mutation tests lower it so intentional leaks are detected quickly.
    /// </summary>
    public int TickTimeoutMs { get; set; } = 30000;

    public IReadOnlyList<string> Run(IReadOnlyList<LogAction> actions)
    {
        foreach (var action in actions)
        {
            switch (action)
            {
                case LogAction.Post:
                    Post();
                    break;
                case LogAction.Tick:
                    Tick();
                    break;
                case LogAction.ProducerCancel:
                    producerCancelled = true;
                    cancellation.Cancel();
                    break;
                case LogAction.Dispose:
                case LogAction.DisposeAgain:
                    DisposeProcessor();
                    break;
            }
        }

        if (!disposed)
        {
            DisposeProcessor();
        }

        violations.AddRange(Conservation.Verify(
            entries,
            stream.SinkBytes,
            expectedAbsentFromOutput,
            requireCompleteOutput: !producerCancelled));
        return violations;
    }

    void Post()
    {
        var entry = EntryFactory?.Invoke(nextId, events) ?? new TrackingEntry(nextId, events);
        nextId++;
        entries.Add(entry);
        events.Add(EventKind.Posted, entry.Id);
        try
        {
            processor.Post(entry);
            events.Add(EventKind.Accepted, entry.Id);
            if (disposed)
            {
                // Posting after completion is deterministically rejected inline:
                // the entry is released synchronously and never reaches the output.
                expectedAbsentFromOutput.Add(entry.Id);
            }
        }
        catch (Exception ex)
        {
            events.Add(EventKind.PostRejected, entry.Id, ex.GetType().Name);
            expectedAbsentFromOutput.Add(entry.Id);
        }
    }

    void Tick()
    {
        // Barrier: wait until every posted entry reached its terminal release.
        var expected = entries.Count;
        if (!events.WaitFor(() => ReleasedTotal() >= expected, TickTimeoutMs))
        {
            violations.Add($"tick: only {ReleasedTotal()}/{expected} entries released (leak or deadlock)");
        }
    }

    int ReleasedTotal()
    {
        var total = 0;
        foreach (var entry in entries)
        {
            total += entry.ReleasedCount;
        }
        return total;
    }

    void DisposeProcessor()
    {
        processor.DisposeAsync().AsTask().GetAwaiter().GetResult();
        events.Add(EventKind.Completed);
        disposed = true;
    }
}
