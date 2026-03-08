using System;
using System.Net;
using System.Threading.Tasks;
using Xunit;

namespace Enyim.Caching.Tests
{
    public class MemcachedClientStatsTests : MemcachedClientTestsBase
    {
        [Fact]
        public void When_Getting_Uptime_Is_Successful()
        {
            var uptime = _client.Stats().GetUptime(new DnsEndPoint(_memcachedHost, _memcachedPort));
            Assert.True(uptime > TimeSpan.Zero);
        }

        [Fact]
        public async Task When_Getting_Uptime_Using_Async_Stats_Is_Successful()
        {
            var uptime = (await _client.StatsAsync()).GetUptime(new DnsEndPoint(_memcachedHost, _memcachedPort));
            Assert.True(uptime > TimeSpan.Zero);
            Console.WriteLine("uptime: " + uptime);
        }
    }
}
