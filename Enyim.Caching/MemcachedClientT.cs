using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching
{
    public class MemcachedClient<T> : IMemcachedClient<T>
    {
        private readonly IMemcachedClient _memcachedClient;

        public event Action<IMemcachedNode> NodeFailed;

        public MemcachedClient(ILoggerFactory loggerFactory, IMemcachedClientConfiguration configuration)
        {
            _memcachedClient = new MemcachedClient(loggerFactory, configuration);
        }

        public bool Add(string key, object value, int cacheSeconds)
        {
            return _memcachedClient.Add(key, value, cacheSeconds);
        }

        public Task<bool> AddAsync(string key, object value, int cacheSeconds)
        {
            return _memcachedClient.AddAsync(key, value, cacheSeconds);
        }

        public bool Append(string key, ArraySegment<byte> data)
        {
            return _memcachedClient.Append(key, data);
        }

        public CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data)
        {
            return _memcachedClient.Append(key, cas, data);
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value)
        {
            return _memcachedClient.Cas(mode, key, value);
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas)
        {
            return _memcachedClient.Cas(mode, key, value, cas);
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas)
        {
            return _memcachedClient.Cas(mode, key, value, expiresAt, cas);
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas)
        {
            return _memcachedClient.Cas(mode, key, value, validFor, cas);
        }

        public ulong Decrement(string key, ulong defaultValue, ulong delta)
        {
            return _memcachedClient.Decrement(key, defaultValue, delta);
        }

        public ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return _memcachedClient.Decrement(key, defaultValue, delta, expiresAt);
        }

        public ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return _memcachedClient.Decrement(key, defaultValue, delta, validFor);
        }

        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            return _memcachedClient.Decrement(key, defaultValue, delta, cas);
        }

        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            return _memcachedClient.Decrement(key, defaultValue, delta, expiresAt, cas);
        }

        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            return _memcachedClient.Decrement(key, defaultValue, delta, validFor, cas);
        }

        public void Dispose()
        {
            _memcachedClient.Dispose();
        }

        public void FlushAll()
        {
            _memcachedClient.FlushAll();
        }

        public Task FlushAllAsync()
        {
            return _memcachedClient.FlushAllAsync();
        }

        public object Get(string key)
        {
            return _memcachedClient.Get(key);
        }

        public T1 Get<T1>(string key)
        {
            return _memcachedClient.Get<T1>(key);
        }

        public IDictionary<string, T1> Get<T1>(IEnumerable<string> keys)
        {
            return _memcachedClient.Get<T1>(keys);
        }

        public Task<IGetOperationResult> GetAsync(string key)
        {
            return _memcachedClient.GetAsync(key);
        }

        public Task<IGetOperationResult<T1>> GetAsync<T1>(string key)
        {
            return _memcachedClient.GetAsync<T1>(key);
        }

        public Task<IDictionary<string, T1>> GetAsync<T1>(IEnumerable<string> keys)
        {
            return _memcachedClient.GetAsync<T1>(keys);
        }

        public Task<T1> GetValueAsync<T1>(string key)
        {
            return _memcachedClient.GetValueAsync<T1>(key);
        }

        public Task<T1> GetValueOrCreateAsync<T1>(string key, int cacheSeconds, Func<Task<T1>> generator)
        {
            return _memcachedClient.GetValueOrCreateAsync<T1>(key, cacheSeconds, generator);
        }

        public CasResult<object> GetWithCas(string key)
        {
            return _memcachedClient.GetWithCas(key);
        }

        public CasResult<T1> GetWithCas<T1>(string key)
        {
            return _memcachedClient.GetWithCas<T1>(key);
        }

        public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
        {
            return _memcachedClient.GetWithCas(keys);
        }

        public Task<IDictionary<string, CasResult<object>>> GetWithCasAsync(IEnumerable<string> keys)
        {
            return _memcachedClient.GetWithCasAsync(keys);
        }

        public ulong Increment(string key, ulong defaultValue, ulong delta)
        {
            return _memcachedClient.Increment(key, defaultValue, delta);
        }

        public ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return _memcachedClient.Increment(key, defaultValue, delta, expiresAt);
        }

        public ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return _memcachedClient.Increment(key, defaultValue, delta, validFor);
        }

        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            return _memcachedClient.Increment(key, defaultValue, delta, cas);
        }

        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            return _memcachedClient.Increment(key, defaultValue, delta, expiresAt, cas);
        }

        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            return _memcachedClient.Increment(key, defaultValue, delta, validFor, cas);
        }

        public bool Prepend(string key, ArraySegment<byte> data)
        {
            return _memcachedClient.Prepend(key, data);
        }

        public CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data)
        {
            return _memcachedClient.Prepend(key, cas, data);
        }

        public bool Remove(string key)
        {
            return _memcachedClient.Remove(key);
        }

        public Task<bool> RemoveAsync(string key)
        {
            return _memcachedClient.RemoveAsync(key);
        }

        public Task<bool> RemoveMultiAsync(params string[] keys)
        {
            return _memcachedClient.RemoveMultiAsync(keys);
        }

        public bool Replace(string key, object value, int cacheSeconds)
        {
            return _memcachedClient.Replace(key, value, cacheSeconds);
        }

        public Task<bool> ReplaceAsync(string key, object value, int cacheSeconds)
        {
            return _memcachedClient.ReplaceAsync(key, value, cacheSeconds);
        }

        public bool Set(string key, object value, int cacheSeconds)
        {
            return _memcachedClient.Set(key, value, cacheSeconds);
        }

        public Task<bool> SetAsync(string key, object value, int cacheSeconds)
        {
            return _memcachedClient.SetAsync(key, value, cacheSeconds);
        }

        public ServerStats Stats()
        {
            return _memcachedClient.Stats();
        }

        public ServerStats Stats(string type)
        {
            return _memcachedClient.Stats(type);
        }

        public bool Store(StoreMode mode, string key, object value)
        {
            return _memcachedClient.Store(mode, key, value);
        }

        public bool Store(StoreMode mode, string key, object value, DateTime expiresAt)
        {
            return _memcachedClient.Store(mode, key, value, expiresAt);
        }

        public bool Store(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            return _memcachedClient.Store(mode, key, value, validFor);
        }

        public Task<bool> StoreAsync(StoreMode mode, string key, object value, DateTime expiresAt)
        {
            return _memcachedClient.StoreAsync(mode, key, value, expiresAt);
        }

        public Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            return _memcachedClient.StoreAsync(mode, key, value, validFor);
        }

        public Task<IOperationResult> TouchAsync(string key, DateTime expiresAt)
        {
            return _memcachedClient.TouchAsync(key, expiresAt);
        }

        public Task<IOperationResult> TouchAsync(string key, TimeSpan validFor)
        {
            return _memcachedClient.TouchAsync(key, validFor);
        }

        public bool TryGet(string key, out object value)
        {
            return _memcachedClient.TryGet(key, out value);
        }

        public bool TryGet<T1>(string key, out T1 value)
        {
            return _memcachedClient.TryGet(key, out value);
        }

        public bool TryGetWithCas(string key, out CasResult<object> value)
        {
            return _memcachedClient.TryGetWithCas(key, out value);
        }

        public bool TryGetWithCas<T1>(string key, out CasResult<T1> value)
        {
            return _memcachedClient.TryGetWithCas(key, out value);
        }
    }
}
