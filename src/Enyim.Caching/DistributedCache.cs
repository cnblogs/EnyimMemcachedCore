using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using Enyim.Caching.Memcached;
using Microsoft.Extensions.Caching.Memory;
using System;
using Microsoft.Extensions.Options;

namespace Enyim.Caching
{
    public partial class MemcachedClient
    {
        #region Implement IDistributedCache

        byte[] IDistributedCache.Get(string key)
        {
            var value = Get<byte[]>(key);

            if (value != null)
            {
                Refresh(key);
            }

            return value;
        }

        async Task<byte[]> IDistributedCache.GetAsync(string key, CancellationToken token = default)
        {
            var value = await GetValueAsync<byte[]>(key);

            if (value != null)
            {
                await RefreshAsync(key);
            }

            return value;
        }

        void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            ulong tmp = 0;
            var expiration = GetExpiration(options);
            PerformStore(StoreMode.Set, key, value, expiration, ref tmp, out var status);

            if (options.SlidingExpiration.HasValue)
            {
                var sldExp = options.SlidingExpiration.Value;
                Add(GetSlidingExpirationKey(key), sldExp.ToString(), sldExp);
            }
        }

        async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
        {
            var expiration = GetExpiration(options);
            await PerformStoreAsync(StoreMode.Set, key, value, expiration);

            if (options.SlidingExpiration.HasValue)
            {
                var sldExp = options.SlidingExpiration.Value;
                await AddAsync(GetSlidingExpirationKey(key), sldExp.ToString(), sldExp);
            }
        }

        public void Refresh(string key)
        {
            var sldExpKey = GetSlidingExpirationKey(key);
            var sldExpStr = Get<string>(sldExpKey);
            if (!string.IsNullOrEmpty(sldExpStr)
                && TimeSpan.TryParse(sldExpStr, out var sldExp))
            {
                var value = Get(key);
                if (value != null)
                {
                    Replace(key, value, sldExp);
                    Replace(sldExpKey, sldExpStr, sldExp);
                }
            }
        }

        public async Task RefreshAsync(string key, CancellationToken token = default)
        {
            var sldExpKey = GetSlidingExpirationKey(key);
            var sldExpStr = await GetValueAsync<string>(sldExpKey);
            if (!string.IsNullOrEmpty(sldExpStr)
                && TimeSpan.TryParse(sldExpStr, out var sldExp))
            {
                var value = (await GetAsync(key)).Value;
                if (value != null)
                {
                    await ReplaceAsync(key, value, sldExp);
                    await ReplaceAsync(sldExpKey, sldExpStr, sldExp);
                }
            }
        }

        void IDistributedCache.Remove(string key)
        {
            Remove(key);
            Remove(GetSlidingExpirationKey(key));
        }

        async Task IDistributedCache.RemoveAsync(string key, CancellationToken token = default)
        {
            await RemoveAsync(key);
            await RemoveAsync(GetSlidingExpirationKey(key));
        }

        private uint GetExpiration(DistributedCacheEntryOptions options)
        {
            if (options.SlidingExpiration.HasValue)
            {
                return GetExpiration(options.SlidingExpiration);
            }
            else if (options.AbsoluteExpirationRelativeToNow.HasValue)
            {
                return GetExpiration(null, relativeToNow: options.AbsoluteExpirationRelativeToNow.Value);
            }
            else if (options.AbsoluteExpiration.HasValue)
            {
                return GetExpiration(null, absoluteExpiration: options.AbsoluteExpiration.Value);
            }
            else
            {
                throw new ArgumentException("Invalid enum value for options", nameof(options));
            }
        }

        private string GetSlidingExpirationKey(string key) => $"{key}-sliding-expiration";

        #endregion
    }
}
