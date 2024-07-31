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
using static System.Collections.Specialized.BitVector32;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class EnyimMemcachedServiceCollectionExtensions
    {
#if NET5_0_OR_GREATER
        public static IServiceCollection AddEnyimMemcached(
            this IServiceCollection services,
            string sectionKey = "enyimMemcached",
            bool asDistributedCache = true)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (string.IsNullOrEmpty(sectionKey))
            {
                throw new ArgumentNullException(nameof(sectionKey));
            }

            return services.AddEnyimMemcachedInternal(
                s => s.AddOptions<MemcachedClientOptions>().BindConfiguration(sectionKey), asDistributedCache);
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

            return services.AddEnyimMemcachedInternal(
                s => s.Configure(setupAction), asDistributedCache);
        }

#if NET5_0_OR_GREATER
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

            return services.AddEnyimMemcachedInternal(
                s => s.Configure<MemcachedClientOptions>(configurationSection), asDistributedCache);
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

            return services.AddEnyimMemcachedInternal(
                s => s.Configure<MemcachedClientOptions>(section), asDistributedCache);
        }
#endif

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

#if NET5_0_OR_GREATER
        public static IServiceCollection AddEnyimMemcached<T>(
            this IServiceCollection services,
            string sectionKey)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (string.IsNullOrEmpty(sectionKey))
            {
                throw new ArgumentNullException(nameof(sectionKey));
            }

            return services.AddEnyimMemcached<T>(
                s => s.AddOptions<MemcachedClientOptions>().BindConfiguration(sectionKey));
        }

        public static IServiceCollection AddEnyimMemcached<T>(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionKey)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            return services.AddEnyimMemcached<T>(
                s => s.Configure<MemcachedClientOptions>(configuration.GetSection(sectionKey)));
        }
#endif

        public static IServiceCollection AddEnyimMemcached<T>(
            this IServiceCollection services,
            Action<IServiceCollection> configure)
        {
            services.AddOptions();
            configure?.Invoke(services);

            services.TryAddSingleton<ITranscoder, DefaultTranscoder>();
            services.TryAddSingleton<IMemcachedKeyTransformer, DefaultKeyTransformer>();

            services.TryAddSingleton<IMemcachedClient<T>>(sp =>
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                var options = sp.GetRequiredService<IOptions<MemcachedClientOptions>>();
                var conf = new MemcachedClientConfiguration(loggerFactory, options);
                return new MemcachedClient<T>(loggerFactory, conf);
            });

            return services;
        }
    }
}