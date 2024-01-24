using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Xunit;

namespace Enyim.Caching.Tests
{
    public class DefaultNodeLocatorTest
    {
        [Fact]
        public void FNV1a()
        {
            var fnv = new Enyim.FNV1a();

            // FNV1a test vectors:
            // http://www.isthe.com/chongo/src/fnv/test_fnv.c
            var testVectors = new List<Tuple<string, UInt32>>
            {
                new Tuple<string, uint>("",0x811c9dc5U),
                new Tuple<string, uint>("a",0xe40c292cU),
                new Tuple<string, uint>("b",0xe70c2de5U),
                new Tuple<string, uint>("c",0xe60c2c52U),
                new Tuple<string, uint>("d",0xe10c2473U),
                new Tuple<string, uint>("e",0xe00c22e0U),
                new Tuple<string, uint>("f",0xe30c2799U),
                new Tuple<string, uint>("fo",0x6222e842U),
                new Tuple<string, uint>("foo",0xa9f37ed7U),
                new Tuple<string, uint>("foob",0x3f5076efU),
            };

            foreach (var testVector in testVectors)
            {
                byte[] data = fnv.ComputeHash(Encoding.ASCII.GetBytes(testVector.Item1));
                uint value = BitConverter.ToUInt32(data, 0);
                Assert.Equal(value, testVector.Item2);
            }
        }

        [Fact]
        public void TestLocator()
        {
            String[] servers = new[]
            {
                "10.0.1.1:11211",
                "10.0.1.2:11211",
                "10.0.1.3:11211",
                "10.0.1.4:11211",
                "10.0.1.5:11211",
                "10.0.1.6:11211",
                "10.0.1.7:11211",
                "10.0.1.8:11211",
            };
            int[] serverCount = new int[servers.Length];

            var nodes = servers.
                            Select(s => new MockNode(new IPEndPoint(IPAddress.Parse(s.Substring(0, s.IndexOf(":"))), 11211))).
                            Cast<IMemcachedNode>().
                            ToList();

            IMemcachedNodeLocator locator = new DefaultNodeLocator();
            locator.Initialize(nodes.ToList());

            var keyCheckCount = 1000000;
            var expectedKeysPerServer = keyCheckCount / nodes.Count;

            var random = new Random();
            for (int i = 0; i < keyCheckCount; i++)
            {
                var node = locator.Locate(random.NextDouble().ToString());
                for (int j = 0; j < nodes.Count; j++)
                {
                    if (nodes[j] == node)
                    {
                        serverCount[j]++;
                        break;
                    }
                }
            }

            double maxVariation = 0;
            for (int i = 0; i < serverCount.Length; i++)
            {
                var keysThisServer = serverCount[i];
                var variation = (double)Math.Abs(keysThisServer - expectedKeysPerServer) / expectedKeysPerServer;
                maxVariation = Math.Max(maxVariation, variation);
                Console.WriteLine("Expected about {0} keys per server; got {1} for server {2}; variation: {3:0.0%}", expectedKeysPerServer, keysThisServer, i, variation);
            }
            Assert.InRange(maxVariation, 0, 0.20); // variation expected to be less than 20%
        }
    }

    class MockNode : IMemcachedNode
    {
        public MockNode(IPEndPoint endpoint)
        {
            this.EndPoint = endpoint;
        }

        public EndPoint EndPoint { get; private set; }

        public bool IsAlive => true;

        public event Action<IMemcachedNode> Failed;

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public IOperationResult Execute(IOperation op)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult> ExecuteAsync(IOperation op)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExecuteAsync(IOperation op, Action<bool> next)
        {
            throw new NotImplementedException();
        }

        public bool Ping()
        {
            throw new NotImplementedException();
        }
    }
}
