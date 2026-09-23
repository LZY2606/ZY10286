using System;
using System.IO;
using System.Threading;

namespace ZLogger.Tests.Harness;

/// <summary>
/// Stream whose Write/Flush can be gated by barriers and programmed to fail on
/// the Nth call, past a byte budget, on cancellation, or after a short (partial) write.
/// Accepted bytes accumulate in <see cref="Sink"/> and are never rolled back.
/// </summary>
public sealed class ProgrammableStream : Stream
{
    readonly EventLog events;
    readonly MemoryStream sink = new();
    readonly object sinkGate = new();
    int writeCalls;
    int flushCalls;
    long totalBytes;

    public ProgrammableStream(EventLog events, bool writeGateInitiallyOpen)
    {
        this.events = events;
        WriteGate = new ManualResetEventSlim(writeGateInitiallyOpen);
    }

    /// <summary>Barrier driving channel consumption: Write blocks while closed.</summary>
    public ManualResetEventSlim WriteGate { get; }

    /// <summary>1-based Write call index that throws; -1 disables.</summary>
    public int FailOnWriteCall { get; set; } = -1;

    /// <summary>1-based Flush call index that throws; -1 disables.</summary>
    public int FailOnFlushCall { get; set; } = -1;

    /// <summary>Total accepted bytes beyond which Write throws; -1 disables.</summary>
    public long FailAfterTotalBytes { get; set; } = -1;

    /// <summary>On a failing Write, accept this many bytes into the sink before throwing (short write).</summary>
    public int ShortWriteBytes { get; set; } = -1;

    /// <summary>When cancelled, Write blocks/throws with OperationCanceledException.</summary>
    public CancellationToken Cancellation { get; set; }

    public int WriteCalls => Volatile.Read(ref writeCalls);
    public int FlushCalls => Volatile.Read(ref flushCalls);

    public byte[] SinkBytes
    {
        get
        {
            lock (sinkGate)
            {
                return sink.ToArray();
            }
        }
    }

    public long SinkLength
    {
        get
        {
            lock (sinkGate)
            {
                return sink.Length;
            }
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        Cancellation.ThrowIfCancellationRequested();
        events.Add(EventKind.WriteEntered, -1, $"count={count}");
        WriteGate.Wait(Cancellation);

        var call = Interlocked.Increment(ref writeCalls);
        var overBudget = FailAfterTotalBytes >= 0 && totalBytes + count > FailAfterTotalBytes;
        if (call == FailOnWriteCall || overBudget)
        {
            if (ShortWriteBytes > 0)
            {
                // short write: partial bytes land in the sink, then the write fails.
                var partial = Math.Min(ShortWriteBytes, count);
                lock (sinkGate)
                {
                    sink.Write(buffer, offset, partial);
                }
                totalBytes += partial;
            }
            throw new IOException($"programmable write failure (call {call})");
        }

        lock (sinkGate)
        {
            sink.Write(buffer, offset, count);
        }
        totalBytes += count;
        events.Add(EventKind.Written, -1, $"count={count}");
    }

    public override void Flush()
    {
        var call = Interlocked.Increment(ref flushCalls);
        if (call == FailOnFlushCall)
        {
            throw new IOException($"programmable flush failure (call {call})");
        }
        events.Add(EventKind.Flushed);
    }

    protected override void Dispose(bool disposing)
    {
        events.Add(EventKind.StreamDisposed);
        base.Dispose(disposing);
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
