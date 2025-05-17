using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Enyim.Caching.Memcached;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MemcachedTest;

public class PooledSocketTest
{
    [Fact]
    public async Task ReadSync_ShouldTimeoutOrFail_WhenServerResponseIsSlow()
    {
        // Arrange
        var logger = new NullLogger<PooledSocketTest>();
        const int port = 12345;
        var server = new SlowLorisServer();
        using var cts = new CancellationTokenSource();
        await server.StartAsync(port, cts.Token);
        var endpoint = new IPEndPoint(IPAddress.Loopback, port);
        var socket = new PooledSocket(
            endpoint,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(50),
            logger,
            useSslStream: false,
            useIPv6: false,
            sslClientAuthOptions: null
        );
        await socket.ConnectAsync();
        var buffer = new byte[server.Response.Length];
        
        // Act
        var timer = Stopwatch.StartNew();
        var ex = Record.Exception(() =>
        {
            socket.Read(buffer, 0, server.Response.Length);
        });
        timer.Stop();    
        
        // Assert
        Assert.True(timer.Elapsed < TimeSpan.FromMilliseconds(500), "Read took too long");
        Assert.NotNull(ex);
        Assert.True(
            ex is TimeoutException or IOException,
            $"Expected TimeoutException or IOException, got {ex.GetType().Name}: {ex.Message}"
        );
        
        await cts.CancelAsync();
        server.Stop();
    }
    
    [Fact]
    public async Task ReadAsync_ShouldTimeoutOrFail_WhenServerResponseIsSlow()
    {
        // Arrange
        var logger = new NullLogger<PooledSocket>();
        const int port = 12345;
        var server = new SlowLorisServer();
        using var cts = new CancellationTokenSource();

        await server.StartAsync(port, cts.Token);

        var endpoint = new IPEndPoint(IPAddress.Loopback, port);
        var socket = new PooledSocket(
            endpoint,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(50),
            logger,
            useSslStream: false,
            useIPv6: false,
            sslClientAuthOptions: null
        );
        
        await socket.ConnectAsync();
        
        var buffer = new byte[server.Response.Length];
        
        // Act
        var timer = Stopwatch.StartNew();
        var ex = await Record.ExceptionAsync(async () =>
        {
            await socket.ReadAsync(buffer, 0, server.Response.Length);
        });
        timer.Stop();

        // Assert
        Assert.True(timer.Elapsed < TimeSpan.FromMilliseconds(500), "ReadAsync took too long");
        Assert.NotNull(ex);
        Assert.True(
            ex is TimeoutException or IOException,
            $"Expected TimeoutException or IOException, got {ex.GetType().Name}: {ex.Message}"
        );

        // Cleanup
        await cts.CancelAsync();
        server.Stop();
    }
}

public class SlowLorisServer
{
    private TcpListener _listener;
    private CancellationToken _token;
    public readonly byte[] Response = "Hello, I'm slow!"u8.ToArray();

    public Task StartAsync(int port, CancellationToken token)
    {
        _token = token;
        _listener = new TcpListener(IPAddress.Loopback, port);
        _listener.Start();

        _ = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(token);
                _ = Task.Run(() => HandleClientAsync(client), token);
            }
        }, token);
        return Task.CompletedTask;
    }

    private async Task HandleClientAsync(TcpClient client)
    {
        await using var stream = client.GetStream();
        for (var i = 0; i < Response.Length; i++)
        {
            await stream.WriteAsync(Response, i, 1, _token);
            await Task.Delay(100, _token);
        }
        await stream.FlushAsync(_token);
        client.Close();
    }

    public void Stop() => _listener.Stop();
}

