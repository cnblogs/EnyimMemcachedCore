using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Markup;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;
using Xunit.Abstractions;

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
        services.AddEnyimMemcached();
        return services.BuildServiceProvider();
    }
}
