using Enyim.Caching.Memcached;
using Newtonsoft.Json.Linq;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Enyim.Caching.Tests
{

    public class MemcachedClientCasTests : MemcachedClientTestsBase
	{

		[Fact]
		public void When_Storing_Item_With_Valid_Cas_Result_Is_Successful()
		{
			var key = GetUniqueKey("cas");
			var value = GetRandomString();
			var storeResult = Store(StoreMode.Add, key, value);
			StoreAssertPass(storeResult);

			var casResult = _client.ExecuteCas(StoreMode.Set, key, value, storeResult.Cas);
			StoreAssertPass(casResult);
		}

		[Fact]
		public void When_Storing_Item_With_Invalid_Cas_Result_Is_Not_Successful()
		{
			var key = GetUniqueKey("cas");
			var value = GetRandomString();
			var storeResult = Store(StoreMode.Add, key, value);
			StoreAssertPass(storeResult);

			var casResult = _client.ExecuteCas(StoreMode.Set, key, value, storeResult.Cas - 1);
			StoreAssertFail(casResult);
		}


        [Fact]
        public async Task When_Storing_Item_With_Valid_Cas_Result_Is_Successful_Async()
        {
            var key = GetUniqueKey("cas");
            var value = GetRandomString();
            var comment = new Comment
            {
                Author = key,
                Text = value
            };

            var storeResult = Store(StoreMode.Add, key, comment);
            StoreAssertPass(storeResult);

            var casResult1 = await _client.GetAsync(key);
            GetAssertPass(casResult1, comment);

            var casResult2 = await _client.GetAsync<Comment>(key);
            GetAssertPass(casResult2, comment);
        }

        /// <summary>
        /// comment
        /// because <see cref="IMemcachedClient"/> use <see cref="Newtonsoft"/> as default serialization tool,
        /// so <see cref="IMemcachedClient.GetAsync(string)"/> will return <see cref="IGetOperation"/> with <see cref="JObject"/> as <see cref="IGetOperation.Result"/>'s type.
        /// </summary>
        public class Comment : IEquatable<Comment>, IEquatable<JObject>
        {
            public string Author { get; set; }

            public string Text { get; set; }

            public bool Equals(Comment other)
            {
                return other != null && other.Author == Author && other.Text == Text;
            }

            public bool Equals(JObject other)
            {
                return Equals(other?.ToObject<Comment>());
            }

            public override bool Equals(object obj)
            {
                if (obj == null)
                {
                    return false;
                }
                if (obj is Comment comment)
                {
                    return Equals(comment);
                }
                if (obj is JObject jObject)
                {
                    return Equals(jObject);
                }
                return false;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Author, Text);
            }
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2012 Couchbase, Inc.
 *    @copyright 2012 Attila Kiskó, enyim.com
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
