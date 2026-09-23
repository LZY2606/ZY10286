using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ZLogger.Tests.Harness;

public enum EventKind
{
    Posted,
    Accepted,
    PostRejected,
    Formatted,
    WriteEntered,
    Written,
    Flushed,
    Released,
    BatchStarted,
    Completed,
    StreamDisposed,
    Error,
}

public readonly record struct TestEvent(EventKind Kind, int EntryId, string Detail)
{
    public override string ToString() => $"{Kind}({EntryId}){Detail}";
}

/// <summary>
/// Thread-safe event recorder with event-driven waiting (no sleeps).
/// Timeouts exist only as deadlock guards; they never gate progress.
/// </summary>
public sealed class EventLog
{
    readonly object gate = new();
    readonly List<TestEvent> events = new();
    readonly AutoResetEvent changed = new(false);

    public void Add(EventKind kind, int entryId = -1, string detail = "")
    {
        lock (gate)
        {
            events.Add(new TestEvent(kind, entryId, detail));
        }
        changed.Set();
    }

    public List<TestEvent> Snapshot()
    {
        lock (gate)
        {
            return events.ToList();
        }
    }

    public int Count(EventKind kind)
    {
        lock (gate)
        {
            return events.Count(x => x.Kind == kind);
        }
    }

    public bool WaitFor(Func<bool> condition, int timeoutMs = 30000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!condition())
        {
            var remaining = deadline - Environment.TickCount64;
            if (remaining <= 0) return false;
            changed.WaitOne((int)Math.Min(remaining, 100));
        }
        return true;
    }
}
