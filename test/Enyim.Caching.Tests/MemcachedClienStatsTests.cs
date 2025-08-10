using Enyim.Caching.Configuration;
using System;
using System.Net;
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

            var ipEndPoint = new DnsEndPoint(_memcachedHost, _memcachedPort).GetIPEndPoint(false);
            uptime = _client.Stats().GetUptime(ipEndPoint);
            Assert.True(uptime > TimeSpan.Zero);
        }
    }
}
