using System;
using System.Collections.Generic;
using System.Linq;

namespace ZLogger.Tests.Harness;

public enum LogAction
{
    /// <summary>Post one entry to the processor.</summary>
    Post,

    /// <summary>Barrier: let the consumer drain and wait until all posted entries are released.</summary>
    Tick,

    /// <summary>Cancel the programmable writer's cancellation token (permanent write fault).</summary>
    ProducerCancel,

    /// <summary>Complete the channel and wait for the write loop to finish.</summary>
    Dispose,

    /// <summary>Dispose a second time; must be a harmless no-op.</summary>
    DisposeAgain,
}

public static class LogActionSequence
{
    /// <summary>Deterministically generates a short action sequence from a fixed seed.</summary>
    public static LogAction[] Generate(int seed, int length)
    {
        var random = new Random(seed);
        var actions = new LogAction[length];
        for (var i = 0; i < actions.Length; i++)
        {
            actions[i] = random.Next(100) switch
            {
                < 45 => LogAction.Post,
                < 65 => LogAction.Tick,
                < 75 => LogAction.ProducerCancel,
                < 90 => LogAction.Dispose,
                _ => LogAction.DisposeAgain,
            };
        }
        return actions;
    }

    /// <summary>
    /// Greedy chunk-removal shrinker: reduces a failing sequence towards a minimal
    /// one that still satisfies <paramref name="stillFails"/>.
    /// </summary>
    public static List<LogAction> Shrink(IReadOnlyList<LogAction> sequence, Func<List<LogAction>, bool> stillFails)
    {
        var current = sequence.ToList();
        var chunk = Math.Max(1, current.Count / 2);
        while (chunk >= 1 && current.Count > 1)
        {
            var i = 0;
            var removed = false;
            while (i + chunk <= current.Count)
            {
                var candidate = current.Where((_, index) => index < i || index >= i + chunk).ToList();
                if (candidate.Count > 0 && stillFails(candidate))
                {
                    current = candidate;
                    removed = true;
                }
                else
                {
                    i += chunk;
                }
            }
            if (!removed) chunk /= 2;
        }
        return current;
    }
}
