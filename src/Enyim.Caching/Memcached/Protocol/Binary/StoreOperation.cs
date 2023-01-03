using System;
using System.Text;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Helpers;
using Enyim.Caching.Memcached.Results.Extensions;
using System.Threading.Tasks;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
    public class StoreOperation : BinarySingleItemOperation, IStoreOperation
    {
        private readonly StoreMode _mode;
        private CacheItem _value;
        private readonly uint _expires;

        public StoreOperation(StoreMode mode, string key, CacheItem value, uint expires) :
            base(key)
        {
            _mode = mode;
            _value = value;
            _expires = expires;
        }

        protected override BinaryRequest Build()
        {
            OpCode op;
            switch (_mode)
            {
                case StoreMode.Add: op = OpCode.Add; break;
                case StoreMode.Set: op = OpCode.Set; break;
                case StoreMode.Replace: op = OpCode.Replace; break;
                default: throw new ArgumentOutOfRangeException("mode", _mode + " is not supported");
            }

            var extra = new byte[8];

            BinaryConverter.EncodeUInt32((uint)_value.Flags, extra, 0);
            BinaryConverter.EncodeUInt32(_expires, extra, 4);

            var request = new BinaryRequest(op)
            {
                Key = Key,
                Cas = Cas,
                Extra = new ArraySegment<byte>(extra),
                Data = _value.Data
            };

            return request;
        }

        protected override IOperationResult ProcessResponse(BinaryResponse response)
        {
            var result = new BinaryOperationResult();

            StatusCode = response.StatusCode;
            if (response.StatusCode == 0)
            {
                return result.Pass();
            }
            else
            {
                var message = ResultHelper.ProcessResponseData(response.Data);
                return result.Fail(message);
            }
        }

        StoreMode IStoreOperation.Mode
        {
            get { return _mode; }
        }

        protected internal override Task<bool> ReadResponseAsync(PooledSocket socket, System.Action<bool> next)
        {
            throw new NotSupportedException();
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
