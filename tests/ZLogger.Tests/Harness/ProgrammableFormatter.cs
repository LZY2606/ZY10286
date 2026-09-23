using System;
using System.Buffers;
using System.Text;
using System.Threading;

namespace ZLogger.Tests.Harness;

/// <summary>
/// Formatter that writes a unique [[id]] marker per entry and can be programmed
/// to throw before or after writing on the Nth format call.
/// </summary>
public sealed class ProgrammableFormatter : IZLoggerFormatter
{
    readonly EventLog events;
    int callCount;

    public ProgrammableFormatter(EventLog events)
    {
        this.events = events;
    }

    public bool WithLineBreak => true;

    /// <summary>1-based call index on which to throw before writing any bytes; -1 disables.</summary>
    public int ThrowBeforeWriteOnCall { get; set; } = -1;

    /// <summary>1-based call index on which to throw after the bytes were written; -1 disables.</summary>
    public int ThrowAfterWriteOnCall { get; set; } = -1;

    public int CallCount => Volatile.Read(ref callCount);

    public static byte[] MarkerOf(int entryId) => Encoding.UTF8.GetBytes($"[[{entryId}]]");

    public void FormatLogEntry(IBufferWriter<byte> writer, IZLoggerEntry entry)
    {
        var id = ((TrackingEntry)entry).Id;
        var call = Interlocked.Increment(ref callCount);

        if (call == ThrowBeforeWriteOnCall)
        {
            throw new InvalidOperationException($"formatter fault before write (call {call})");
        }

        var marker = MarkerOf(id);
        var span = writer.GetSpan(marker.Length);
        marker.CopyTo(span);
        writer.Advance(marker.Length);
        events.Add(EventKind.Formatted, id);

        if (call == ThrowAfterWriteOnCall)
        {
            throw new InvalidOperationException($"formatter fault after write (call {call})");
        }
    }
}
