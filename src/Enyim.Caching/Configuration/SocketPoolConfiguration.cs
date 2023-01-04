using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Enyim.Caching.Memcached;

namespace Enyim.Caching.Configuration
{
    public class SocketPoolConfiguration : ISocketPoolConfiguration
    {
        private int _minPoolSize = 5;
        private int _maxPoolSize = 100;
        private bool _useSslStream = false;
        private TimeSpan _connectionTimeout = new TimeSpan(0, 0, 10);
        private TimeSpan _receiveTimeout = new TimeSpan(0, 0, 10);
        private TimeSpan _deadTimeout = new TimeSpan(0, 0, 10);
        private TimeSpan _queueTimeout = new TimeSpan(0, 0, 0, 0, 100);
        private TimeSpan _connectionIdleTimeout = TimeSpan.Zero;
        private TimeSpan _initPoolTimeout = new TimeSpan(0, 1, 0);
        private INodeFailurePolicyFactory _failurePolicyFactory = new ThrottlingFailurePolicyFactory(5, TimeSpan.FromMilliseconds(2000));

        int ISocketPoolConfiguration.MinPoolSize
        {
            get { return _minPoolSize; }
            set { _minPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating the maximum amount of sockets per server in the socket pool.
        /// </summary>
        /// <returns>The maximum amount of sockets per server in the socket pool. The default is 20.</returns>
        /// <remarks>It should be 0.75 * (number of threads) for optimal performance.</remarks>
        int ISocketPoolConfiguration.MaxPoolSize
        {
            get { return _maxPoolSize; }
            set { _maxPoolSize = value; }
        }

        TimeSpan ISocketPoolConfiguration.ConnectionTimeout
        {
            get { return _connectionTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                _connectionTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.ReceiveTimeout
        {
            get { return _receiveTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                _receiveTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.QueueTimeout
        {
            get { return _queueTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                _queueTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.InitPoolTimeout
        {
            get { return _initPoolTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                _initPoolTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.DeadTimeout
        {
            get { return _deadTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                _deadTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.ConnectionIdleTimeout
        {
            get { return _connectionIdleTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                _connectionIdleTimeout = value;
            }
        }

        INodeFailurePolicyFactory ISocketPoolConfiguration.FailurePolicyFactory
        {
            get { return _failurePolicyFactory; }
            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");

                _failurePolicyFactory = value;
            }
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
