using System.Collections.Generic;

namespace Enyim.Caching.Memcached.LocatorFactories
{
    /// <summary>
    /// Create DefaultNodeLocator with any ServerAddressMutations
    /// </summary>
    public class DefaultNodeLocatorFactory : IProviderFactory<IMemcachedNodeLocator>
    {
        private readonly int _serverAddressMutations;

        public DefaultNodeLocatorFactory(int serverAddressMutations)
        {
            _serverAddressMutations = serverAddressMutations;
        }

        public IMemcachedNodeLocator Create()
        {
            return new DefaultNodeLocator(_serverAddressMutations);
        }

        public void Initialize(Dictionary<string, string> parameters)
        {
        }
    }
}
