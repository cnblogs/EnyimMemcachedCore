using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class EnyimMemcachedServiceCollectionExtensions
    {
#if NET6_0_OR_GREATER
        public static IServiceCollection AddEnyimMemcached(
            this IServiceCollection services,
            string sectionKey = "enyimMemcached",
            bool asDistributedCache = true)
        {
            var config = services.BuildServiceProvider().GetRequiredService<IConfiguration>();
            return services.AddEnyimMemcached(config.GetSection(sectionKey), asDistributedCache);
        }
#endif

        public static IServiceCollection AddEnyimMemcached(
            this IServiceCollection services,
            Action<MemcachedClientOptions> setupAction,
            bool asDistributedCache = true)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (setupAction == null)
            {
                throw new ArgumentNullException(nameof(setupAction));
            }

            return services.AddEnyimMemcachedInternal(s => s.Configure(setupAction), asDistributedCache);
        }

        public static IServiceCollection AddEnyimMemcached(
            this IServiceCollection services,
            IConfigurationSection configurationSection,
            bool asDistributedCache)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (configurationSection == null)
            {
                throw new ArgumentNullException(nameof(configurationSection));
            }

            if (!configurationSection.Exists())
            {
                throw new ArgumentNullException($"{configurationSection.Key} in appsettings.json");
            }

            return services.AddEnyimMemcachedInternal(s => s.Configure<MemcachedClientOptions>(configurationSection), asDistributedCache);
        }

        public static IServiceCollection AddEnyimMemcached(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionKey = "enyimMemcached",
            bool asDistributedCache = true)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            var section = configuration.GetSection(sectionKey);
            if (!section.Exists())
            {
                throw new ArgumentNullException($"{sectionKey} in appsettings.json");
            }

            return services.AddEnyimMemcachedInternal(s => s.Configure<MemcachedClientOptions>(section), asDistributedCache);
        }

        private static IServiceCollection AddEnyimMemcachedInternal(
            this IServiceCollection services,
            Action<IServiceCollection> configure,
            bool asDistributedCache)
        {
            services.AddOptions();
            configure?.Invoke(services);

            services.TryAddSingleton<ITranscoder, DefaultTranscoder>();
            services.TryAddSingleton<IMemcachedKeyTransformer, DefaultKeyTransformer>();
            services.TryAddSingleton<IMemcachedClientConfiguration, MemcachedClientConfiguration>();
            services.TryAddSingleton<IMemcachedClient, MemcachedClient>();

            if (asDistributedCache)
            {
                services.TryAddSingleton<IDistributedCache>(sp =>
                    sp.GetRequiredService<IMemcachedClient>() as MemcachedClient);
            }

            return services;
        }

#if NET6_0_OR_GREATER
        public static IServiceCollection AddEnyimMemcached<T>(
            this IServiceCollection services,
            string sectionKey)
        {
            var config = services.BuildServiceProvider().GetRequiredService<IConfiguration>();
            return services.AddEnyimMemcached<T>(config, sectionKey);
        }
#endif

        public static IServiceCollection AddEnyimMemcached<T>(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionKey)
        {
            services.AddOptions();
            services.Configure<MemcachedClientOptions>(sectionKey, configuration.GetSection(sectionKey));
            services.TryAddSingleton<ITranscoder, DefaultTranscoder>();
            services.TryAddSingleton<IMemcachedKeyTransformer, DefaultKeyTransformer>();

            services.TryAddSingleton<IMemcachedClient<T>>(sp =>
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                var options = sp.GetRequiredService<IOptionsMonitor<MemcachedClientOptions>>();
                var conf = new MemcachedClientConfiguration(loggerFactory, options.Get(sectionKey));
                return new MemcachedClient<T>(loggerFactory, conf);
            });

            return services;
        }
    }
}
