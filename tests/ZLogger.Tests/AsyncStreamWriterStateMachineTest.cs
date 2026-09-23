using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using ZLogger.Tests.Harness;

namespace ZLogger.Tests;

public class AsyncStreamWriterStateMachineTest
{
    static (AsyncStreamLineMessageWriter processor, ProgrammableStream stream, ProgrammableFormatter formatter, EventLog events, List<TrackingEntry> entries)
        CreateSut(BackgroundBufferFullMode fullMode, int capacity = 10_000, bool gateOpen = true)
    {
        var events = new EventLog();
        var stream = new ProgrammableStream(events, gateOpen);
        var formatter = new ProgrammableFormatter(events);
        var options = new ZLoggerOptions
        {
            FullMode = fullMode,
            BackgroundBufferCapacity = capacity,
            InternalErrorLogger = ex => events.Add(EventKind.Error, -1, ex.GetType().Name),
        };
        options.UseFormatter(() => formatter);
        return (new AsyncStreamLineMessageWriter(stream, options), stream, formatter, events, new List<TrackingEntry>());
    }

    TrackingEntry Post(AsyncStreamLineMessageWriter processor, List<TrackingEntry> entries, EventLog events, int id)
    {
        var entry = new TrackingEntry(id, events);
        entries.Add(entry);
        events.Add(EventKind.Posted, id);
        processor.Post(entry);
        events.Add(EventKind.Accepted, id);
        return entry;
    }

    static string SinkText(ProgrammableStream stream) => Encoding.UTF8.GetString(stream.SinkBytes);

    [Fact]
    public async Task BlockMode_ProducerCancelledByDispose_ReleasesEntryAndPropagates()
    {
        var (processor, stream, _, events, entries) = CreateSut(BackgroundBufferFullMode.Block, capacity: 1, gateOpen: false);

        Post(processor, entries, events, 0);
        // The write loop has dequeued entry 0 and is blocked inside the gated stream write.
        events.WaitFor(() => events.Count(EventKind.WriteEntered) == 1).Should().BeTrue();

        Post(processor, entries, events, 1); // queued; channel (capacity 1) is now full

        var entry2 = new TrackingEntry(2, events);
        entries.Add(entry2);
        var producerStarted = new ManualResetEventSlim();
        var blockedProducer = Task.Run(() =>
        {
            producerStarted.Set();
            processor.Post(entry2); // blocks in PostSlow: channel full, consumer gated
        });
        producerStarted.Wait();

        // Dispose completes the channel while the producer is blocked; the write loop
        // is still gated, so entry 2 can never be accepted.
        var disposeTask = Task.Run(async () => await processor.DisposeAsync());

        var ex = await Assert.ThrowsAsync<ChannelClosedException>(async () => await blockedProducer);
        entry2.ReleasedCount.Should().Be(1); // rejected entry released exactly once, error propagated

        stream.WriteGate.Set();
        await disposeTask.WaitAsync(TimeSpan.FromSeconds(30));

        var output = SinkText(stream);
        output.Should().Contain("[[0]]");
        output.Should().Contain("[[1]]");
        output.Should().NotContain("[[2]]");

        Conservation.Verify(entries, stream.SinkBytes, new HashSet<int> { 2 }).Should().BeEmpty();
    }

    [Fact]
    public async Task DropMode_FullBoundary_DroppedEntryIsReleasedNotWritten()
    {
        var (processor, stream, formatter, events, entries) = CreateSut(BackgroundBufferFullMode.Drop, capacity: 1, gateOpen: false);

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.WriteEntered) == 1).Should().BeTrue();

        Post(processor, entries, events, 1); // fills the single buffer slot

        var dropped = Post(processor, entries, events, 2); // overflow: dropped synchronously
        dropped.ReleasedCount.Should().Be(1); // ownership returned immediately, not leaked
        formatter.CallCount.Should().Be(1); // never formatted

        stream.WriteGate.Set();
        await processor.DisposeAsync();

        var output = SinkText(stream);
        output.Should().Contain("[[0]]");
        output.Should().Contain("[[1]]");
        output.Should().NotContain("[[2]]");

        Conservation.Verify(entries, stream.SinkBytes, new HashSet<int> { 2 }).Should().BeEmpty();
    }

    [Fact]
    public async Task GrowMode_DrainsAllEntriesInOrder()
    {
        var (processor, stream, _, events, entries) = CreateSut(BackgroundBufferFullMode.Grow, gateOpen: false);

        const int count = 500;
        for (var i = 0; i < count; i++)
        {
            Post(processor, entries, events, i);
        }
        events.WaitFor(() => events.Count(EventKind.WriteEntered) >= 1).Should().BeTrue();

        stream.WriteGate.Set();
        await processor.DisposeAsync();

        var output = SinkText(stream);
        var ids = output.Split("[[", StringSplitOptions.RemoveEmptyEntries)
            .Select(x => int.Parse(x.Trim(']', '\r', '\n')))
            .ToArray();
        ids.Should().Equal(Enumerable.Range(0, count)); // drained in FIFO order, no gaps, no duplicates

        Conservation.Verify(entries, stream.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task FormatterThrows_BeforeWrite_EntryReleasedAndErrorPropagated()
    {
        var (processor, stream, formatter, events, entries) = CreateSut(BackgroundBufferFullMode.Grow);
        formatter.ThrowBeforeWriteOnCall = 2;

        for (var i = 0; i < 3; i++) Post(processor, entries, events, i);

        events.WaitFor(() => entries.Sum(x => x.ReleasedCount) == 3).Should().BeTrue();
        events.WaitFor(() => events.Count(EventKind.Error) == 1).Should().BeTrue();
        await processor.DisposeAsync();

        var output = SinkText(stream);
        output.Should().Contain("[[0]]");
        output.Should().NotContain("[[1]]"); // faulted before writing any bytes
        output.Should().Contain("[[2]]"); // later entries still processed after the fault

        Conservation.Verify(entries, stream.SinkBytes, new HashSet<int> { 1 }).Should().BeEmpty();
    }

    [Fact]
    public async Task FormatterThrows_AfterWrite_BytesKeptEntryReleasedAndErrorPropagated()
    {
        var (processor, stream, formatter, events, entries) = CreateSut(BackgroundBufferFullMode.Grow);
        formatter.ThrowAfterWriteOnCall = 1;

        for (var i = 0; i < 3; i++) Post(processor, entries, events, i);

        events.WaitFor(() => entries.Sum(x => x.ReleasedCount) == 3).Should().BeTrue();
        events.WaitFor(() => events.Count(EventKind.Error) == 1).Should().BeTrue();
        await processor.DisposeAsync();

        // bytes written before the throw are not rolled back; every entry appears exactly once
        Conservation.Verify(entries, stream.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task WriterFails_OnNthWrite_ErrorPropagatedAndEntriesReleased()
    {
        var (processor, stream, _, events, entries) = CreateSut(BackgroundBufferFullMode.Grow);

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.Flushed) == 1).Should().BeTrue();

        stream.FailOnWriteCall = 2;
        Post(processor, entries, events, 1);

        events.WaitFor(() => entries[1].ReleasedCount == 1).Should().BeTrue();
        events.WaitFor(() => events.Count(EventKind.Error) == 1).Should().BeTrue();
        await processor.DisposeAsync();

        var output = SinkText(stream);
        output.Should().Contain("[[0]]");
        output.Should().NotContain("[[1]]"); // the failing write accepted no bytes

        Conservation.Verify(entries, stream.SinkBytes, new HashSet<int> { 1 }).Should().BeEmpty();
    }

    [Fact]
    public async Task WriterShortWrite_PartialBytesRemainWithoutRollback()
    {
        var (processor, stream, _, events, entries) = CreateSut(BackgroundBufferFullMode.Grow);
        stream.FailOnWriteCall = 1;
        stream.ShortWriteBytes = 4;

        Post(processor, entries, events, 0);

        events.WaitFor(() => entries[0].ReleasedCount == 1).Should().BeTrue();
        events.WaitFor(() => events.Count(EventKind.Error) == 1).Should().BeTrue();
        await processor.DisposeAsync();

        // partial bytes ("[[0]" of "[[0]]\n") stay in the stream; ownership and error are deterministic
        SinkText(stream).Should().Be("[[0]");
        Conservation.Verify(entries, stream.SinkBytes, new HashSet<int> { 0 }).Should().BeEmpty();
    }

    [Fact]
    public async Task WriterFails_OnCancellationWhileBlocked()
    {
        var (processor, stream, _, events, entries) = CreateSut(BackgroundBufferFullMode.Grow, gateOpen: false);
        var cancellation = new CancellationTokenSource();
        stream.Cancellation = cancellation.Token;

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.WriteEntered) == 1).Should().BeTrue();

        cancellation.Cancel(); // producer cancel: blocked write observes the token

        events.WaitFor(() => entries[0].ReleasedCount == 1).Should().BeTrue();
        events.WaitFor(() => events.Snapshot().Any(x => x.Kind == EventKind.Error && x.Detail.Contains("OperationCanceled"))).Should().BeTrue();

        stream.WriteGate.Set();
        await processor.DisposeAsync();

        Conservation.Verify(entries, stream.SinkBytes, new HashSet<int> { 0 }).Should().BeEmpty();
    }

    [Fact]
    public async Task Dispose_Twice_IsHarmless()
    {
        var (processor, stream, _, events, entries) = CreateSut(BackgroundBufferFullMode.Grow);

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.Flushed) == 1).Should().BeTrue();

        await processor.DisposeAsync();
        await processor.DisposeAsync(); // second dispose must not throw or corrupt state

        events.Count(EventKind.StreamDisposed).Should().BeGreaterThanOrEqualTo(1);
        Conservation.Verify(entries, stream.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task Dispose_RacingWithPosts_KeepsConservation()
    {
        var (processor, stream, _, events, entries) = CreateSut(BackgroundBufferFullMode.Grow);
        var entriesLock = new object();

        var poster = Task.Run(() =>
        {
            for (var i = 0; i < 50; i++)
            {
                var entry = new TrackingEntry(i, events);
                lock (entriesLock) entries.Add(entry);
                try
                {
                    processor.Post(entry);
                }
                catch
                {
                    // rejection during shutdown is acceptable; ownership must still be conserved
                }
            }
        });

        events.WaitFor(() => events.Count(EventKind.Flushed) >= 1).Should().BeTrue();
        await processor.DisposeAsync();
        await poster.WaitAsync(TimeSpan.FromSeconds(30));

        // completeness cannot be required (posts race with completion), but every
        // created entry must be released exactly once and nothing may be duplicated.
        Conservation.Verify(entries, stream.SinkBytes, requireCompleteOutput: false).Should().BeEmpty();
    }
}
