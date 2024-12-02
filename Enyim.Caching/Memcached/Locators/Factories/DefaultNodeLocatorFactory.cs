using System.Collections.Generic;

namespace Enyim.Caching.Memcached.LocatorFactories
{
    /// <summary>
    /// Create DefaultNodeLocator with any ServerAddressMutations
    /// </summary>
    public class DefaultNodeLocatorFactory(int serverAddressMutations) : IProviderFactory<IMemcachedNodeLocator>
    {
        public IMemcachedNodeLocator Create()
        {
            return new DefaultNodeLocator(serverAddressMutations);
        }

        public void Initialize(Dictionary<string, string> parameters)
        {
        }
    }
}
