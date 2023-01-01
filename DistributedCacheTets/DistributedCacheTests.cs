using Enyim.Caching;
using Microsoft.Extensions.Caching.Distributed;
using Xunit.Priority;

namespace ServiceStackRedisCacheTests;

[TestCaseOrderer(PriorityOrderer.Name, PriorityOrderer.Assembly)]
[Collection(nameof(DistributedCacheCollection))]
public class DistributedCacheTests
{
    private const string _value = "Coding changes the world";
    private readonly IDistributedCache _cache;

    public DistributedCacheTests(DistributedCacheFixture fixture)
    {
        _cache = fixture.DistributedCache;
    }

    [Fact]
    public async Task Cache_with_absolute_expiration()
    {
        var key = nameof(Cache_with_absolute_expiration) + "_" + Guid.NewGuid();
        var keyAsync = nameof(Cache_with_absolute_expiration) + "_async_" + Guid.NewGuid();

        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpiration = DateTimeOffset.Now.AddSeconds(2)
        };

        _cache.SetString(key, _value, options);
        await _cache.SetStringAsync(keyAsync, _value, options);

        Assert.Equal(_value, _cache.GetString(key));
        Assert.Equal(_value, await _cache.GetStringAsync(keyAsync));

        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.Null(_cache.GetString(key));
        Assert.Null(await _cache.GetStringAsync(keyAsync));
    }

    [Fact]
    public async Task Cache_with_relative_to_now()
    {
        var key = nameof(Cache_with_relative_to_now) + "_" + Guid.NewGuid();
        var keyAsync = nameof(Cache_with_relative_to_now) + "_async_" + Guid.NewGuid();

        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(2)
        };

        _cache.SetString(key, _value, options);
        await _cache.SetStringAsync(keyAsync, _value, options);

        Assert.Equal(_value, _cache.GetString(key));
        Assert.Equal(_value, await _cache.GetStringAsync(keyAsync));

        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.Null(_cache.GetString(key));
        Assert.Null(await _cache.GetStringAsync(keyAsync));
    }

    [Fact]
    public async Task Cache_with_sliding_expiration()
    {
        var key = nameof(Cache_with_sliding_expiration) + "_" + Guid.NewGuid();
        var keyAsync = nameof(Cache_with_sliding_expiration) + "_async_" + Guid.NewGuid();

        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromSeconds(3)
        };

        _cache.SetString(key, _value, options);
        await _cache.SetStringAsync(keyAsync, _value, options);

        Assert.Equal(_value, _cache.GetString(key));
        Assert.Equal(_value, await _cache.GetStringAsync(keyAsync));

        await Task.Delay(2000);
        _cache.Refresh(key);
        await _cache.RefreshAsync(keyAsync);

        await Task.Delay(2000);
        Assert.Equal(_value, _cache.GetString(key));
        Assert.Equal(_value, await _cache.GetStringAsync(keyAsync));

        await Task.Delay(3100);
        Assert.Null(_cache.GetString(key));
        Assert.Null(await _cache.GetStringAsync(keyAsync));
    }
}