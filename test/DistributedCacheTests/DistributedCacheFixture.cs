using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ServiceStackRedisCacheTests;

public class DistributedCacheFixture
{
    public IDistributedCache DistributedCache { get; private set; }

    public DistributedCacheFixture()
    {
        using IServiceScope scope = GetServiceProvider().CreateScope();
        DistributedCache = scope.ServiceProvider.GetRequiredService<IDistributedCache>();
    }

    private IServiceProvider GetServiceProvider()
    {
        IServiceCollection services = new ServiceCollection();
        IConfiguration conf = new ConfigurationBuilder().
            AddJsonFile("appsettings.json", optional: false)
            .Build();
        services.AddSingleton(conf);
        services.AddLogging();
        services.AddEnyimMemcached(asDistributedCache: true);
        return services.BuildServiceProvider();
    }
}
