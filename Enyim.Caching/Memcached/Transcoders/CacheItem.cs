using System;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// Represents an object either being retrieved from the cache
    /// or being sent to the cache.
    /// </summary>
    /// <remarks>
    /// Initializes a new instance of <see cref="T:CacheItem"/>.
    /// </remarks>
    /// <param name="flags">Custom item data.</param>
    /// <param name="data">The serialized item.</param>
    public struct CacheItem(uint flags, ArraySegment<byte> data)
    {

        /// <summary>
        /// The data representing the item being stored/retireved.
        /// </summary>
        public ArraySegment<byte> Data
		{
			get { return data; }
			set { data = value; }
		}

		/// <summary>
		/// Flags set for this instance.
		/// </summary>
		public uint Flags
		{
			get { return flags; }
			set { flags = value; }
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
