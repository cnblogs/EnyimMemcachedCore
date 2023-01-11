using Enyim.Caching;
using Enyim.Caching.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace MemcachedTest
{
    public class MemcachedNodeTests
    {
        [Fact]
        public async Task ConnectionIdleTimeout_reached()
        {
            IServiceCollection services = new ServiceCollection();
            var idleTimeout = TimeSpan.FromSeconds(2);
            services.AddEnyimMemcached(options =>
            {
                options.AddServer("memcached", 11211);
                options.SocketPool = new SocketPoolOptions
                {
                    ConnectionIdleTimeout = idleTimeout
                };
            });

            var originConsoleOut = Console.Out;
            using var sw = new StringWriter();
            Console.SetOut(sw);
            services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Information).AddConsole());
            IServiceProvider sp = services.BuildServiceProvider();
            var client = sp.GetRequiredService<IMemcachedClient>() as MemcachedClient;

            var logMessage = $"Connection idle timeout {idleTimeout} reached";
            await client.GetAsync(Guid.NewGuid().ToString());

            await Task.Delay(2100);
            await client.GetAsync(Guid.NewGuid().ToString());
            Assert.Contains(logMessage, sw.ToString());

            Console.SetOut(originConsoleOut);
        }
    }
}
