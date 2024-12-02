using AEPLCore.Monitoring;
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
        /// <summary>
        /// Add EnyimMemcached to the specified <see cref="IServiceCollection"/>.
        /// Read configuration via IConfiguration.GetSection("enyimMemcached")
        /// </summary>
        /// <param name="services"></param>
        /// <returns></returns>
        public static IServiceCollection AddEnyimMemcached(this IServiceCollection services)
        {
            return AddEnyimMemcachedInternal(services, null);
        }

        public static IServiceCollection AddEnyimMemcached(this IServiceCollection services, Action<MemcachedClientOptions> setupAction)
        {
            ArgumentNullException.ThrowIfNull(services);

            ArgumentNullException.ThrowIfNull(setupAction);

            return AddEnyimMemcachedInternal(services, s => s.Configure(setupAction));
        }

        public static IServiceCollection AddEnyimMemcached(this IServiceCollection services, IConfigurationSection configurationSection)
        {
            ArgumentNullException.ThrowIfNull(services);

            ArgumentNullException.ThrowIfNull(configurationSection);

            if (!configurationSection.Exists())
            {
                throw new ArgumentNullException($"{configurationSection.Key} in appsettings.json");
            }

            return AddEnyimMemcachedInternal(services, s => s.Configure<MemcachedClientOptions>(configurationSection));
        }

        public static IServiceCollection AddEnyimMemcached(this IServiceCollection services, IConfiguration configuration, string sectionKey = "enyimMemcached")
        {
            ArgumentNullException.ThrowIfNull(services);

            ArgumentNullException.ThrowIfNull(configuration);

            var section = configuration.GetSection(sectionKey);
            if (!section.Exists())
            {
                throw new ArgumentNullException($"{sectionKey} in appsettings.json");
            }

            return AddEnyimMemcachedInternal(services, s => s.Configure<MemcachedClientOptions>(section));
        }

        private static IServiceCollection AddEnyimMemcachedInternal(IServiceCollection services, Action<IServiceCollection> configure)
        {
            services.AddOptions();
            configure?.Invoke(services);

            services.TryAddSingleton<ITranscoder, DefaultTranscoder>();
            services.TryAddSingleton<IMemcachedKeyTransformer, DefaultKeyTransformer>();
            services.TryAddSingleton<IMemcachedClientConfiguration, MemcachedClientConfiguration>();
            services.AddSingleton<MemcachedClient>();

            services.AddSingleton<IMemcachedClient, MemcachedClient>();
            services.AddSingleton<IDistributedCache, MemcachedClient>();

            return services;
        }

        public static IServiceCollection AddEnyimMemcached<T>(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionKey)
        {
            services.AddOptions();
            services.Configure<MemcachedClientOptions>(sectionKey, configuration.GetSection(sectionKey));
            services.TryAddSingleton<ITranscoder, DefaultTranscoder>();
            services.TryAddSingleton<IMemcachedKeyTransformer, DefaultKeyTransformer>();

            services.AddSingleton<IMemcachedClient<T>>(sp =>
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
