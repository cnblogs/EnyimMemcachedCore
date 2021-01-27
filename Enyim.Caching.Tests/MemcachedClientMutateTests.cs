using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Enyim.Caching.Tests
{
    public class MemcachedClientMutateTests : MemcachedClientTestsBase
    {
        [Fact]
        public void When_Incrementing_Value_Result_Is_Successful()
        {
            var key = GetUniqueKey("mutate");
            var mutateResult = _client.ExecuteIncrement(key, 100, 10);
            MutateAssertPass(mutateResult, 100);

            mutateResult = _client.ExecuteIncrement(key, 100, 10);
            MutateAssertPass(mutateResult, 110);
        }

        [Fact]
        public void When_Decrementing_Value_Result_Is_Successful()
        {
            var key = GetUniqueKey("mutate");
            var mutateResult = _client.ExecuteDecrement(key, 100, 10);
            MutateAssertPass(mutateResult, 100);

            mutateResult = _client.ExecuteDecrement(key, 100, 10);
            MutateAssertPass(mutateResult, 90);
        }

        [Fact]
        public async Task When_Touch_Item_Result_Is_Successful()
        {
            var key = GetUniqueKey("touch");
            await _client.AddAsync(key, "value", 1);
            var operationResult = await _client.GetAsync<string>(key);
            Assert.True(operationResult.Success);
            Assert.NotEqual(0UL, operationResult.Cas);
            var result = await _client.TouchAsync(key, TimeSpan.FromSeconds(60));
            await Task.Delay(1010);
            Assert.True(result.Success, "Success was false");
            Assert.True((result.StatusCode ?? 0) == 0, "StatusCode was not null or 0");
            operationResult = await _client.GetAsync<string>(key);
            Assert.True(operationResult.Success);
            Assert.NotEqual(0UL, operationResult.Cas);
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