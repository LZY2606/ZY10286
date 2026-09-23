using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ZLogger.Tests.Harness;

namespace ZLogger.Tests;

public class ConservationSequenceTest
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    public void SeededSequence_SatisfiesConservation(int seed)
    {
        var sequence = LogActionSequence.Generate(seed, length: 16);
        var runner = new StreamWriterSequenceRunner();

        var violations = runner.Run(sequence);

        violations.Should().BeEmpty(
            $"seed {seed} sequence [{string.Join(", ", sequence)}] must conserve entries; events: {string.Join(", ", runner.Events.Snapshot())}");
    }

    [Fact]
    public void Mutation_MissingRelease_IsDetected()
    {
        var sequence = new[]
        {
            LogAction.Post, LogAction.Post, LogAction.Post,
            LogAction.Post, LogAction.Post, LogAction.Post,
            LogAction.Dispose,
        };
        var runner = new StreamWriterSequenceRunner
        {
            // mutation: entry 2 never releases (simulates a lost Return)
            EntryFactory = (id, events) => new TrackingEntry(id, events) { SuppressRelease = id == 2 },
        };

        var violations = runner.Run(sequence);

        violations.Should().Contain(x => x.Contains("entry 2") && x.Contains("released 0 times"));
    }

    [Fact]
    public async Task Mutation_DuplicateBatchFlush_IsDetected()
    {
        var events = new EventLog();
        var entries = Enumerable.Range(0, 3).Select(id => new TrackingEntry(id, events)).ToList();
        var processor = new GatedBatchingProcessor(batchSize: 10, events)
        {
            DuplicateFlush = true, // mutation: every batch is flushed twice
        };

        foreach (var entry in entries) processor.Post(entry);
        events.WaitFor(() => events.Count(EventKind.BatchStarted) == 1).Should().BeTrue();
        processor.ProcessGate.Set();
        await processor.DisposeAsync();

        var violations = Conservation.Verify(entries, processor.SinkBytes);

        violations.Should().Contain(x => x.Contains("duplicate"));
    }

    [Fact]
    public void Shrinker_ReducesFailingSequence_ToMinimalCore()
    {
        var sequence = LogActionSequence.Generate(seed: 42, length: 16).ToList();

        // failure predicate: entry 2 leaks its release, so any sequence creating
        // at least three entries violates conservation
        bool StillFails(List<LogAction> candidate)
        {
            var runner = new StreamWriterSequenceRunner
            {
                EntryFactory = (id, events) => new TrackingEntry(id, events) { SuppressRelease = id == 2 },
                TickTimeoutMs = 200, // the leak is intentional; don't wait on the deadlock guard
            };
            return runner.Run(candidate).Count > 0;
        }

        sequence.Count(x => x == LogAction.Post).Should().BeGreaterThanOrEqualTo(3,
            "the generated seed-42 sequence must contain the failing core");
        StillFails(sequence).Should().BeTrue();

        var shrunk = LogActionSequence.Shrink(sequence, StillFails);

        shrunk.Should().HaveCount(3);
        shrunk.Should().OnlyContain(x => x == LogAction.Post);
        StillFails(shrunk).Should().BeTrue();
    }

    [Fact]
    public void Shrinker_KeepsPassingSequencesUntouched()
    {
        var sequence = LogActionSequence.Generate(seed: 7, length: 16).ToList();

        bool StillFails(List<LogAction> candidate) => new StreamWriterSequenceRunner().Run(candidate).Count > 0;

        StillFails(sequence).Should().BeFalse(); // sanity: unmutated run passes
        var shrunk = LogActionSequence.Shrink(sequence, StillFails);
        shrunk.Should().Equal(sequence); // nothing removable when nothing fails
    }
}
