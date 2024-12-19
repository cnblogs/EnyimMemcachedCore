using Enyim.Caching.Memcached;
using Microsoft.Extensions.Caching.Distributed;
using System;
using System.Threading;
using System.Threading.Tasks;

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
            var value = await GetValueAsync<byte[]>(key).ConfigureAwait(false);

            if (value != null)
            {
                await RefreshAsync(key).ConfigureAwait(false);
            }

            return value;
        }

        void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            ulong tmp = 0;

            if (!HasSlidingExpiration(options))
            {
                PerformStore(StoreMode.Set, key, value, 0, ref tmp, out var status0);
                return;
            }

            var expiration = GetExpiration(options);
            PerformStore(StoreMode.Set, key, value, expiration, ref tmp, out var status);

            if (options != null && options.SlidingExpiration.HasValue)
            {
                var sldExp = options.SlidingExpiration.Value;
                Add(GetSlidingExpirationKey(key), sldExp.ToString(), sldExp);
            }
        }

        async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
        {
            if (!HasSlidingExpiration(options))
            {
                await PerformStoreAsync(StoreMode.Set, key, value, 0).ConfigureAwait(false);
                return;
            }

            var expiration = GetExpiration(options);
            await PerformStoreAsync(StoreMode.Set, key, value, expiration).ConfigureAwait(false);

            if (options.SlidingExpiration.HasValue)
            {
                var sldExp = options.SlidingExpiration.Value;
                await AddAsync(GetSlidingExpirationKey(key), sldExp.ToString(), sldExp).ConfigureAwait(false);
            }
        }

        private static bool HasSlidingExpiration(DistributedCacheEntryOptions options)
        {
            if (options == null)
            {
                return false;
            }

            if ((options.SlidingExpiration.HasValue == false || options.SlidingExpiration.Value == TimeSpan.Zero) &&
               options.AbsoluteExpiration.HasValue == false &&
               options.AbsoluteExpirationRelativeToNow.HasValue == false)
            {
                return false;
            }

            return true;
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
            var sldExpStr = await GetValueAsync<string>(sldExpKey).ConfigureAwait(false);
            if (!string.IsNullOrEmpty(sldExpStr)
                && TimeSpan.TryParse(sldExpStr, out var sldExp))
            {
                var value = (await GetAsync(key).ConfigureAwait(false)).Value;
                if (value != null)
                {
                    await ReplaceAsync(key, value, sldExp).ConfigureAwait(false);
                    await ReplaceAsync(sldExpKey, sldExpStr, sldExp).ConfigureAwait(false);
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
            await RemoveAsync(key).ConfigureAwait(false);
            await RemoveAsync(GetSlidingExpirationKey(key)).ConfigureAwait(false);
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
