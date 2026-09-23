#nullable enable

using System;
using System.Buffers;
using System.Text.Json;
using System.Threading;

namespace ZLogger.Tests.Harness;

/// <summary>
/// An IZLoggerEntry whose lifecycle is recorded into an EventLog so tests can
/// verify the conservation invariant: every created entry is released exactly once.
/// </summary>
public class TrackingEntry : IZLoggerEntry
{
    readonly EventLog events;
    int releasedCount;

    public int Id { get; }

    /// <summary>Mutation knob: simulates an implementation that forgets to release.</summary>
    public bool SuppressRelease { get; set; }

    public int ReleasedCount => Volatile.Read(ref releasedCount);

    public TrackingEntry(int id, EventLog events)
    {
        Id = id;
        this.events = events;
    }

    public LogInfo LogInfo => default;

    public void FormatUtf8(IBufferWriter<byte> writer, IZLoggerFormatter formatter)
    {
        formatter.FormatLogEntry(writer, this);
    }

    public void Return()
    {
        if (SuppressRelease) return; // mutation: leaked release
        var count = Interlocked.Increment(ref releasedCount);
        events.Add(EventKind.Released, Id, count > 1 ? "over-release" : "");
    }

    public IZLoggerEntry CreateEntry(in LogInfo info) => this;
    public int ParameterCount => 0;
    public bool IsSupportUtf8ParameterKey => true;
    public override string ToString() => $"entry-{Id}";
    public void ToString(IBufferWriter<byte> writer) { }
    public string GetOriginalFormat() => "";
    public void WriteOriginalFormat(IBufferWriter<byte> writer) { }
    public void WriteJsonParameterKeyValues(Utf8JsonWriter jsonWriter, JsonSerializerOptions jsonSerializerOptions, IKeyNameMutator? keyNameMutator = null) { }
    public ReadOnlySpan<byte> GetParameterKey(int index) => ReadOnlySpan<byte>.Empty;
    public string GetParameterKeyAsString(int index) => "";
    public object? GetParameterValue(int index) => null;
    public T? GetParameterValue<T>(int index) => default;
    public Type GetParameterType(int index) => typeof(object);
}
