using System;
using System.Collections.Generic;
using System.Linq;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// This is a simple node locator with no computation overhead, always returns the first server from the list. Use only in single server deployments.
    /// </summary>
    public sealed class SingleNodeLocator : IMemcachedNodeLocator
    {
        private IMemcachedNode _node;
        private bool _isInitialized;
        private object initLock = new Object();

        void IMemcachedNodeLocator.Initialize(IList<IMemcachedNode> nodes)
        {
            if (nodes.Count > 0)
            {
                _node = nodes[0];
            }

            _isInitialized = true;
            /*if (_isInitialized)
                return;

			// locking on this is rude but easy
			lock (initLock)
			{
                if (_isInitialized)
                    return;

                if (nodes.Count > 0)
                {
                    node = nodes[0];
                }

				_isInitialized = true;
			}*/
        }

        IMemcachedNode IMemcachedNodeLocator.Locate(string key)
        {
            if (!_isInitialized)
                throw new InvalidOperationException("You must call Initialize first");

            if (_node == null) return null;

            return _node;
        }

        IEnumerable<IMemcachedNode> IMemcachedNodeLocator.GetWorkingNodes()
        {
            return _node.IsAlive
                    ? new IMemcachedNode[] { _node }
                    : Enumerable.Empty<IMemcachedNode>();
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
