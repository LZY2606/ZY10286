using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ZLogger.Tests.Harness;

namespace ZLogger.Tests;

public class BatchingProcessorStateMachineTest
{
    static TrackingEntry Post(GatedBatchingProcessor processor, List<TrackingEntry> entries, EventLog events, int id)
    {
        var entry = new TrackingEntry(id, events);
        entries.Add(entry);
        events.Add(EventKind.Posted, id);
        processor.Post(entry);
        events.Add(EventKind.Accepted, id);
        return entry;
    }

    [Fact]
    public async Task BatchSize_ExactBoundary_BatchesNeverExceedBatchSize()
    {
        var events = new EventLog();
        var entries = new List<TrackingEntry>();
        var processor = new GatedBatchingProcessor(batchSize: 3, events);

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 1).Should().BeTrue();
        // loop is now blocked inside the first (gated) batch; queue 6 more
        for (var i = 1; i <= 6; i++) Post(processor, entries, events, i);

        processor.ProcessGate.Set(); // tick: batch 1 = [0]
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 2).Should().BeTrue();
        processor.ProcessGate.Set(); // tick: batch 2 = exactly [1,2,3]
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 3).Should().BeTrue();
        processor.ProcessGate.Set(); // tick: batch 3 = exactly [4,5,6]
        events.WaitFor(() => entries.Sum(x => x.ReleasedCount) == 7).Should().BeTrue();

        await processor.DisposeAsync();

        var batches = processor.Batches;
        batches.Should().HaveCount(3);
        batches[0].Should().Equal(0);
        batches[1].Should().Equal(1, 2, 3); // exactly batchSize when enough entries are pending
        batches[2].Should().Equal(4, 5, 6);
        batches.Should().OnlyContain(b => b.Length <= 3);

        Conservation.Verify(entries, processor.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task BatchSize_One_EveryBatchHoldsExactlyOneEntry()
    {
        var events = new EventLog();
        var entries = new List<TrackingEntry>();
        var processor = new GatedBatchingProcessor(batchSize: 1, events);

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 1).Should().BeTrue();
        Post(processor, entries, events, 1);
        Post(processor, entries, events, 2);

        processor.ProcessGate.Set();
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 2).Should().BeTrue();
        processor.ProcessGate.Set();
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 3).Should().BeTrue();
        processor.ProcessGate.Set();
        events.WaitFor(() => entries.Sum(x => x.ReleasedCount) == 3).Should().BeTrue();

        await processor.DisposeAsync();

        processor.Batches.Should().HaveCount(3);
        processor.Batches.Should().OnlyContain(b => b.Length == 1);
        Conservation.Verify(entries, processor.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task BatchTick_DrivesConsumptionWithoutWallClock()
    {
        var events = new EventLog();
        var entries = new List<TrackingEntry>();
        var processor = new GatedBatchingProcessor(batchSize: 10, events);

        Post(processor, entries, events, 0);
        Post(processor, entries, events, 1);

        // nothing is released until the batch tick arrives
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 1).Should().BeTrue();
        entries.Sum(x => x.ReleasedCount).Should().Be(0);

        processor.ProcessGate.Set();
        events.WaitFor(() => entries.Sum(x => x.ReleasedCount) == 2).Should().BeTrue();

        await processor.DisposeAsync();

        processor.Batches.Should().HaveCount(1);
        processor.Batches[0].Should().Equal(0, 1);
        events.Count(EventKind.Completed).Should().Be(1);
        Conservation.Verify(entries, processor.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task Dispose_DrainsPendingEntries()
    {
        var events = new EventLog();
        var entries = new List<TrackingEntry>();
        var processor = new GatedBatchingProcessor(batchSize: 100, events);

        for (var i = 0; i < 5; i++) Post(processor, entries, events, i);
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 1).Should().BeTrue();

        processor.ProcessGate.Set(); // allow the pending batch to drain
        await processor.DisposeAsync(); // completes the channel and waits for the loop

        entries.Sum(x => x.ReleasedCount).Should().Be(5);
        Conservation.Verify(entries, processor.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task ProcessFault_EntriesStillReleasedAndErrorPropagated()
    {
        var events = new EventLog();
        var entries = new List<TrackingEntry>();
        var processor = new GatedBatchingProcessor(batchSize: 1, events);
        processor.ProcessFault = new InvalidOperationException("process fault");

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 1).Should().BeTrue();
        Post(processor, entries, events, 1);
        processor.ProcessGate.Set();

        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 2).Should().BeTrue();
        processor.ProcessGate.Set();

        events.WaitFor(() => entries.Sum(x => x.ReleasedCount) == 2).Should().BeTrue();
        events.WaitFor(() => events.Count(EventKind.Error) == 2).Should().BeTrue();
        await processor.DisposeAsync();

        // faulting batch wrote nothing, but ownership is conserved
        Conservation.Verify(entries, processor.SinkBytes, new HashSet<int> { 0, 1 }).Should().BeEmpty();
    }

    [Fact]
    public async Task Dispose_Twice_IsHarmless()
    {
        var events = new EventLog();
        var entries = new List<TrackingEntry>();
        var processor = new GatedBatchingProcessor(batchSize: 10, events);

        Post(processor, entries, events, 0);
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 1).Should().BeTrue();
        processor.ProcessGate.Set();
        events.WaitFor(() => entries[0].ReleasedCount == 1).Should().BeTrue();

        await processor.DisposeAsync();
        await processor.DisposeAsync(); // second dispose must not throw

        Conservation.Verify(entries, processor.SinkBytes).Should().BeEmpty();
    }

    [Fact]
    public async Task Post_AfterDispose_IsRejectedAndReleasedInline()
    {
        var events = new EventLog();
        var entries = new List<TrackingEntry>();
        var processor = new GatedBatchingProcessor(batchSize: 10, events);

        await processor.DisposeAsync();

        var late = Post(processor, entries, events, 0);
        late.ReleasedCount.Should().Be(1); // not accepted, released synchronously

        Conservation.Verify(entries, processor.SinkBytes, new HashSet<int> { 0 }).Should().BeEmpty();
    }
}
