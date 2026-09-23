using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using ZLogger.Internal;

namespace ZLogger
{
    public sealed class AsyncStreamLineMessageWriter : IAsyncLogProcessor, IAsyncDisposable
    {
        readonly byte[] newLine;
        readonly bool crlf;
        readonly byte newLine1;
        readonly byte newLine2;

        readonly Stream stream;
        readonly Channel<IZLoggerEntry> channel;
        readonly Task writeLoop;
        readonly ZLoggerOptions options;
        int disposeStarted;

        public AsyncStreamLineMessageWriter(Stream stream, ZLoggerOptions options)
        {
            this.newLine = Encoding.UTF8.GetBytes(Environment.NewLine);
            if (newLine.Length == 1)
            {
                // cr or lf
                this.newLine1 = newLine[0];
                this.newLine2 = default;
                this.crlf = false;
            }
            else
            {
                // crlf(windows)
                this.newLine1 = newLine[0];
                this.newLine2 = newLine[1];
                this.crlf = true;
            }

            this.options = options;
            this.stream = stream;

            channel = this.options.FullMode switch
            {
                BackgroundBufferFullMode.Grow => Channel.CreateUnbounded<IZLoggerEntry>(new UnboundedChannelOptions
                {
                    AllowSynchronousContinuations = false, // always should be in async loop.
                    SingleWriter = false,
                    SingleReader = true,
                }),
                BackgroundBufferFullMode.Block => Channel.CreateBounded<IZLoggerEntry>(new BoundedChannelOptions(options.BackgroundBufferCapacity)
                {
                    AllowSynchronousContinuations = false,
                    SingleWriter = false,
                    SingleReader = true,
                    FullMode = BoundedChannelFullMode.Wait,
                }),
                BackgroundBufferFullMode.Drop => Channel.CreateBounded<IZLoggerEntry>(new BoundedChannelOptions(options.BackgroundBufferCapacity)
                {
                    AllowSynchronousContinuations = false,
                    SingleWriter = false,
                    SingleReader = true,
                    // Use Wait semantics so TryWrite reports false when full; Post turns
                    // that into a deterministic drop (release) instead of an invisible one.
                    FullMode = BoundedChannelFullMode.Wait,
                }),
                _ => throw new ArgumentOutOfRangeException()
            };
                
            this.writeLoop = Task.Run(WriteLoop);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Post(IZLoggerEntry log)
        {
            var written = channel.Writer.TryWrite(log);
            if (written) return;

            if (options.FullMode == BackgroundBufferFullMode.Block)
            {
                PostSlow(log);
                return;
            }

            // Drop mode overflow or already-completed channel: the entry is not
            // accepted by the channel, so release it here to keep exactly-once ownership.
            log.Return();
        }

        void PostSlow(IZLoggerEntry log)
        {
            try
            {
                channel.Writer.WriteAsync(log).AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // The channel was completed while waiting; the entry was never
                // accepted, so release it here before propagating the error.
                log.Return();
                throw;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AppendLine(StreamBufferWriter writer)
        {
            if (writer.TryGetForNewLine(out var buffer, out var index))
            {
                if (crlf)
                {
                    buffer[index] = newLine1;
                    buffer[index + 1] = newLine2;
                    writer.Advance(2);
                }
                else
                {
                    buffer[index] = newLine1;
                    writer.Advance(1);
                }
            }
            else
            {
                var span = writer.GetSpan(newLine.Length);
                newLine.CopyTo(span);
                writer.Advance(2);
            }
        }

        async Task WriteLoop()
        {
            var writer = new StreamBufferWriter(stream);
            var formatter = options.CreateFormatter();
            var withLineBreak = formatter.WithLineBreak;
            var reader = channel.Reader;

            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                try
                {
                    while (reader.TryRead(out var value))
                    {
                        try
                        {
                            value.FormatUtf8(writer, formatter);
                        }
                        finally
                        {
                            value.Return();
                        }

                        if (withLineBreak)
                        {
                            AppendLine(writer);
                        }
                    }

                    writer.Flush(); // flush before wait.
                }
                catch (Exception ex)
                {
                    try
                    {
                        options.InternalErrorLogger?.Invoke(ex);
                    }
                    catch { }
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref disposeStarted, 1) == 0)
            {
                try
                {
                    channel.Writer.Complete();
                    await writeLoop.ConfigureAwait(false);
                }
                finally
                {
                    this.stream.Dispose();
                }
            }
            else
            {
                // second dispose: wait for the first one to finish, do nothing else.
                await writeLoop.ConfigureAwait(false);
            }
        }
    }
}
