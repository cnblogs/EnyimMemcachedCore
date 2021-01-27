using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Factories;

namespace Enyim.Caching
{
    public class NullMemcachedClient : IMemcachedClient
    {
        public event Action<IMemcachedNode> NodeFailed;

        public bool Append(string key, ArraySegment<byte> data)
        {
            return true;
        }

        public CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data)
        {
            return new CasResult<bool>();
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value)
        {
            return new CasResult<bool>();
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas)
        {
            return new CasResult<bool>();
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas)
        {
            return new CasResult<bool>();
        }

        public CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas)
        {
            return new CasResult<bool>();
        }

        public ulong Decrement(string key, ulong defaultValue, ulong delta)
        {
            return default(ulong);
        }

        public ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return default(ulong);
        }

        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            return new CasResult<ulong>();
        }

        public ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return default(ulong);
        }

        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            return new CasResult<ulong>();
        }

        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            return new CasResult<ulong>();
        }

        public void Dispose()
        {

        }

        public void FlushAll()
        {

        }

        public IDictionary<string, T> Get<T>(IEnumerable<string> keys)
        {
            return new Dictionary<string, T>();
        }

        public Task<IDictionary<string, T>> GetAsync<T>(IEnumerable<string> keys)
        {
            return Task.FromResult<IDictionary<string, T>>(new Dictionary<string, T>());
        }

        public object Get(string key)
        {
            return null;
        }

        public T Get<T>(string key)
        {
            return default(T);
        }

        public Task<IGetOperationResult> GetAsync(string key)
        {
            var result = new DefaultGetOperationResultFactory().Create();
            result.Success = false;
            return Task.FromResult(result);
        }

        public async Task<IGetOperationResult<T>> GetAsync<T>(string key)
        {
            var result = new DefaultGetOperationResultFactory<T>().Create();
            result.Success = false;
            result.Value = default(T);
            return await Task.FromResult(result);
        }

        public async Task<T> GetValueAsync<T>(string key)
        {
            return await Task.FromResult(default(T));
        }

        public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
        {
            return new Dictionary<string, CasResult<object>>();
        }

        public async Task<IDictionary<string, CasResult<object>>> GetWithCasAsync(IEnumerable<string> keys)
        {
            return await Task.FromResult(new Dictionary<string, CasResult<object>>());
        }

        public CasResult<object> GetWithCas(string key)
        {
            return new CasResult<object>();
        }

        public CasResult<T> GetWithCas<T>(string key)
        {
            return new CasResult<T>();
        }

        public ulong Increment(string key, ulong defaultValue, ulong delta)
        {
            return default(ulong);
        }

        public ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return default(ulong);
        }

        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            return new CasResult<ulong>();
        }

        public ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return default(ulong);
        }

        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            return new CasResult<ulong>();
        }

        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            return new CasResult<ulong>();
        }

        public bool Prepend(string key, ArraySegment<byte> data)
        {
            return false;
        }

        public CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data)
        {
            return new CasResult<bool>();
        }

        public bool Remove(string key)
        {
            return true;
        }

        public Task<bool> RemoveAsync(string key)
        {
            return Task.FromResult(false);
        }

        public Task<bool> RemoveMultiAsync(params string[] keys)
        {
            return Task.FromResult(false);
        }

        public ServerStats Stats()
        {
            return new ServerStats(new Dictionary<EndPoint, Dictionary<string, string>>());
        }

        public ServerStats Stats(string type)
        {
            throw new NotImplementedException();
        }

        public bool Store(StoreMode mode, string key, object value)
        {
            return false;
        }

        public bool Store(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            return false;
        }

        public async Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            return await Task.FromResult(false);
        }

        public async Task<bool> StoreAsync(StoreMode mode, string key, object value, DateTime expiresAt)
        {
            return await Task.FromResult(false);
        }

        public bool Store(StoreMode mode, string key, object value, DateTime expiresAt)
        {
            return false;
        }

        public bool TryGet(string key, out object value)
        {
            value = null;
            return false;
        }

        public bool TryGet<T>(string key, out T value)
        {
            value = default;
            return false;
        }

        public bool TryGetWithCas(string key, out CasResult<object> value)
        {
            value = new CasResult<object>();
            return false;
        }

        public bool TryGetWithCas<T>(string key, out CasResult<T> value)
        {
            value = new CasResult<T>();
            return false;
        }

        public bool Add(string key, object value, int cacheSeconds)
        {
            return true;
        }

        public Task<bool> AddAsync(string key, object value, int cacheSeconds)
        {
            return Task.FromResult(true);
        }

        public bool Set(string key, object value, int cacheSeconds)
        {
            return true;
        }

        public Task<bool> SetAsync(string key, object value, int cacheSeconds)
        {
            return Task.FromResult(true);
        }

        public bool Replace(string key, object value, int cacheSeconds)
        {
            return true;
        }

        public Task<bool> ReplaceAsync(string key, object value, int cacheSeconds)
        {
            return Task.FromResult(true);
        }

        public Task<T> GetValueOrCreateAsync<T>(string key, int cacheSeconds, Func<Task<T>> generator)
        {
            return generator?.Invoke();
        }

        public Task FlushAllAsync()
        {
            return Task.CompletedTask;
        }

        public Task<IOperationResult> TouchAsync(string key, DateTime expiresAt)
        {
            return Task.FromResult<IOperationResult>(new MutateOperationResult());
        }

        public Task<IOperationResult> TouchAsync(string key, TimeSpan validFor)
        {
            return Task.FromResult<IOperationResult>(new MutateOperationResult());
        }
    }
}
