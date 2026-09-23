#nullable enable

using System;
using System.Collections.Generic;
using System.Text;

namespace ZLogger.Tests.Harness;

/// <summary>
/// Verifies the conservation invariants over a finished run:
///  - every created entry reaches exactly one terminal release;
///  - successful output contains each expected marker exactly once (no duplicates,
///    none missing) unless the entry is known to be dropped/rejected.
/// </summary>
public static class Conservation
{
    public static List<string> Verify(
        IReadOnlyList<TrackingEntry> entries,
        byte[] output,
        IReadOnlySet<int>? expectedAbsentFromOutput = null,
        bool requireCompleteOutput = true)
    {
        var violations = new List<string>();
        var text = Encoding.UTF8.GetString(output);

        foreach (var entry in entries)
        {
            if (entry.ReleasedCount != 1)
            {
                violations.Add($"entry {entry.Id}: released {entry.ReleasedCount} times, expected exactly 1");
            }

            var marker = Encoding.UTF8.GetString(ProgrammableFormatter.MarkerOf(entry.Id));
            var occurrences = CountOccurrences(text, marker);
            if (occurrences > 1)
            {
                violations.Add($"entry {entry.Id}: marker appears {occurrences} times in output (duplicate write)");
            }
            else if (requireCompleteOutput && occurrences == 0 && expectedAbsentFromOutput?.Contains(entry.Id) != true)
            {
                violations.Add($"entry {entry.Id}: marker missing from output");
            }
        }

        return violations;
    }

    static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }
}
