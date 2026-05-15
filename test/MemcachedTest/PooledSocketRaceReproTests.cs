using System;
using System.Collections.Concurrent;
using System.Net;
using System.Threading.Tasks;
using Enyim.Caching.Memcached;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MemcachedTest;

public class PooledSocketRaceReproTests
{
    [Fact]
    public async Task DisposeSocket_CalledConcurrently_DoesNotThrow()
    {
        // Arrange
        var socket = new TestablepooledSocket(new IPEndPoint(IPAddress.Loopback, 11211));
        var exceptions = new ConcurrentQueue<Exception>();
        var tasks = new Task[64];

        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                try
                {
                    socket.TriggerDisposeSocket();
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            });
        }

        // Act
        await Task.WhenAll(tasks);

        // Assert — Interlocked.Exchange ensures only one caller disposes the socket;
        // concurrent callers must not produce ObjectDisposedException or NullReferenceException.
        Assert.Empty(exceptions);
    }

    // Subclass in the same friend assembly to reach the private protected method.
    private sealed class TestablepooledSocket(IPEndPoint endpoint)
        : PooledSocket(
            endpoint,
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromMilliseconds(100),
            NullLogger<PooledSocket>.Instance,
            useSslStream: false,
            useIPv6: false,
            sslClientAuthOptions: null)
    {
        public void TriggerDisposeSocket() => DisposeSocket();
    }
}
