using Enyim.Caching;
using Enyim.Caching.Memcached;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace MemcachedTest
{
    /// <summary>
    ///This is a test class for Enyim.Caching.MemcachedClient and is intended
    ///to contain all Enyim.Caching.MemcachedClient Unit Tests
    ///</summary>
    public class BinaryMemcachedClientTest : MemcachedClientTest
    {
        [Fact]
        public void IncrementTest()
        {
            using (MemcachedClient client = GetClient())
            {
                Assert.Equal((ulong)100, client.Increment("VALUE", 100, 2));
                Assert.Equal((ulong)124, client.Increment("VALUE", 10, 24));
            }
        }

        [Fact]
        public void DecrementTest()
        {
            using (MemcachedClient client = GetClient())
            {
                Assert.Equal((ulong)100, client.Decrement("VALUE", 100, 2));
                Assert.Equal((ulong)76, client.Decrement("VALUE", 10, 24));

                Assert.Equal((ulong)0, client.Decrement("VALUE", 100, 1000));
            }
        }

        [Fact]
        public async Task IncrementNoDefaultTest()
        {
            using (MemcachedClient client = GetClient())
            {
                Assert.Equal((ulong)2, client.Increment("VALUE", 2, 2));
                Assert.Equal((ulong)4, client.Increment("VALUE", 2, 2));

                var value = await client.GetValueAsync<string>("VALUE");
                Assert.Equal("4", value);
            }
        }

        [Fact]
        public virtual void CASTest()
        {
            using MemcachedClient client = GetClient();
            var value = new List<string> { "foo" };

            // store the item
            var r1 = client.Cas(StoreMode.Set, "CasItem1", value);

            Assert.True(r1.Result, "Initial set failed.");
            Assert.NotEqual((ulong)0, r1.Cas);

            // get back the item and check the cas value (it should match the cas from the set)
            var r2 = client.GetWithCas<List<string>>("CasItem1");

            Assert.Equal("foo", r2.Result[0]);
            Assert.Equal(r1.Cas, r2.Cas);

            value[0] = "bar";
            var r3 = client.Cas(StoreMode.Set, "CasItem1", value, r1.Cas - 1);
            Assert.False(r3.Result, "Overwriting with 'bar' should have failed.");

            value[0] = "baz";
            var r4 = client.Cas(StoreMode.Set, "CasItem1", value, r2.Cas);
            Assert.True(r4.Result, "Overwriting with 'baz' should have succeeded.");

            var r5 = client.GetWithCas<List<string>>("CasItem1");
            Assert.Equal("baz", r5.Result[0]);
        }

        [Fact]
        public void AppendCASTest()
        {
            using (MemcachedClient client = GetClient())
            {
                // store the item
                var r1 = client.Cas(StoreMode.Set, "CasAppend", "foo");

                Assert.True(r1.Result, "Initial set failed.");
                Assert.NotEqual(r1.Cas, (ulong)0);

                var r2 = client.Append("CasAppend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));

                Assert.True(r2.Result, "Append should have succeeded.");

                // get back the item and check the cas value (it should match the cas from the set)
                var r3 = client.GetWithCas<string>("CasAppend");

                Assert.Equal("fool", r3.Result);
                Assert.Equal(r2.Cas, r3.Cas);

                var r4 = client.Append("CasAppend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));
                Assert.False(r4.Result, "Append with invalid CAS should have failed.");
            }
        }

        [Fact]
        public void PrependCASTest()
        {
            using (MemcachedClient client = GetClient())
            {
                // store the item
                var r1 = client.Cas(StoreMode.Set, "CasPrepend", "ool");

                Assert.True(r1.Result, "Initial set failed.");
                Assert.NotEqual(r1.Cas, (ulong)0);

                var r2 = client.Prepend("CasPrepend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'f' }));

                Assert.True(r2.Result, "Prepend should have succeeded.");

                // get back the item and check the cas value (it should match the cas from the set)
                var r3 = client.GetWithCas<string>("CasPrepend");

                Assert.Equal("fool", r3.Result);
                Assert.Equal(r2.Cas, r3.Cas);

                var r4 = client.Prepend("CasPrepend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));
                Assert.False(r4.Result, "Prepend with invalid CAS should have failed.");
            }
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
