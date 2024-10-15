using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached
{
    [DebuggerDisplay("[ Address: {endpoint}, IsAlive = {IsAlive} ]")]
    public partial class PooledSocket : IDisposable
    {
        private readonly ILogger _logger;

        private bool _isAlive;
        private Socket _socket;
        private readonly EndPoint _endpoint;
        private readonly int _connectionTimeout;

        private NetworkStream _inputStream;
        public DateTime LastConnectionTimestamp { get; set; }

        public PooledSocket(EndPoint endpoint, TimeSpan connectionTimeout, TimeSpan receiveTimeout, ILogger logger)
        {
            _logger = logger;
            _isAlive = true;

            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.NoDelay = true;

            _connectionTimeout = connectionTimeout == TimeSpan.MaxValue
                ? Timeout.Infinite
                : (int)connectionTimeout.TotalMilliseconds;

            var rcv = receiveTimeout == TimeSpan.MaxValue
                ? Timeout.Infinite
                : (int)receiveTimeout.TotalMilliseconds;

            socket.ReceiveTimeout = rcv;
            socket.SendTimeout = rcv;

            _socket = socket;
            _endpoint = endpoint;
        }

        public void Connect()
        {
            bool success = false;

            //Learn from https://github.com/dotnet/corefx/blob/release/2.2/src/System.Data.SqlClient/src/System/Data/SqlClient/SNI/SNITcpHandle.cs#L180
            var cts = new CancellationTokenSource();
            cts.CancelAfter(_connectionTimeout);
            void Cancel()
            {
                if (_socket != null && !_socket.Connected)
                {
                    _socket.Dispose();
                    _socket = null;
                }
            }
            cts.Token.Register(Cancel);

            try
            {
                _socket.Connect(_endpoint);
            }
            catch (PlatformNotSupportedException)
            {
                var ep = GetIPEndPoint(_endpoint);
                _socket.Connect(ep.Address, ep.Port);
            }

            if (_socket != null)
            {
                if (_socket.Connected)
                {
                    LastConnectionTimestamp = DateTime.UtcNow;
                    success = true;
                }
                else
                {
                    _socket.Dispose();
                    _socket = null;
                }
            }

            if (success)
            {
                _inputStream = new NetworkStream(_socket);
            }
            else
            {
                throw new TimeoutException($"Could not connect to {_endpoint}.");
            }
        }

        public async Task ConnectAsync()
        {
            bool success = false;

            try
            {
                var connTask = _socket.ConnectAsync(_endpoint);

                if (await Task.WhenAny(connTask, Task.Delay(_connectionTimeout)) == connTask)
                {
                    await connTask;
                }
                else
                {
                    if (_socket != null)
                    {
                        _socket.Dispose();
                        _socket = null;
                    }
                    throw new TimeoutException($"Timeout to connect to {_endpoint}.");
                }
            }
            catch (PlatformNotSupportedException)
            {
                var ep = GetIPEndPoint(_endpoint);
                await _socket.ConnectAsync(ep.Address, ep.Port);
            }

            if (_socket != null)
            {
                if (_socket.Connected)
                {
                    success = true;
                    LastConnectionTimestamp = DateTime.UtcNow;
                }
                else
                {
                    _socket.Dispose();
                    _socket = null;
                }
            }

            if (success)
            {
                _inputStream = new NetworkStream(_socket);
            }
            else
            {
                throw new TimeoutException($"Could not connect to {_endpoint}.");
            }
        }

        public Action<PooledSocket> CleanupCallback { get; set; }

        public int Available
        {
            get { return _socket.Available; }
        }

        public void Reset()
        {
            // _inputStream.Flush();

            int available = _socket.Available;

            if (available > 0)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning(
                        "Socket bound to {0} has {1} unread data! This is probably a bug in the code. InstanceID was {2}.",
                        _socket.RemoteEndPoint, available, InstanceId);

                byte[] data = new byte[available];

                Read(data, 0, available);
            }

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Socket {0} was reset", InstanceId);
        }

        public async Task ResetAsync()
        {
            // await _inputStream.FlushAsync();

            int available = _socket.Available;

            if (available > 0)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                {
                    _logger.LogWarning(
                        "Socket bound to {0} has {1} unread data! This is probably a bug in the code. InstanceID was {2}.",
                        _socket.RemoteEndPoint, available, InstanceId);
                }

                byte[] data = new byte[available];

                await ReadAsync(data, 0, available);
            }

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Socket {0} was reset", InstanceId);
        }

        /// <summary>
        /// The ID of this instance. Used by the <see cref="T:MemcachedServer"/> to identify the instance in its inner lists.
        /// </summary>
        public readonly Guid InstanceId = Guid.NewGuid();

        public bool IsAlive
        {
            get { return _isAlive; }
            set { _isAlive = value; }
        }

        /// <summary>
        /// Releases all resources used by this instance and shuts down the inner <see cref="T:Socket"/>. This instance will not be usable anymore.
        /// </summary>
        /// <remarks>Use the IDisposable.Dispose method if you want to release this instance back into the pool.</remarks>
        public void Destroy()
        {
            this.Dispose(true);
        }

        ~PooledSocket()
        {
            try
            {
                this.Dispose(true);
            }
            catch
            {
            }
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                GC.SuppressFinalize(this);

                try
                {
                    if (_socket != null)
                    {
                        try { _socket.Dispose(); } catch { }
                    }

                    if (_inputStream != null)
                    {
                        _inputStream.Dispose();
                    }

                    _inputStream = null;
                    _socket = null;
                    this.CleanupCallback = null;
                }
                catch (Exception e)
                {
                    _logger.LogError(nameof(PooledSocket), e);
                }
            }
            else
            {
                Action<PooledSocket> cc = this.CleanupCallback;

                if (cc != null)
                    cc(this);
            }
        }

        void IDisposable.Dispose()
        {
            this.Dispose(false);
        }

        private void CheckDisposed()
        {
            if (_socket == null)
                throw new ObjectDisposedException("PooledSocket");
        }

        /// <summary>
        /// Reads the next byte from the server's response.
        /// </summary>
        /// <remarks>This method blocks and will not return until the value is read.</remarks>
        public int ReadByte()
        {
            this.CheckDisposed();

            try
            {
                return _inputStream.ReadByte();
            }
            catch (Exception ex)
            {
                if (ex is IOException || ex is SocketException)
                {
                    _isAlive = false;
                }

                throw;
            }
        }

        public int ReadByteAsync()
        {
            this.CheckDisposed();

            try
            {
                return _inputStream.ReadByte();
            }
            catch (Exception ex)
            {
                if (ex is IOException || ex is SocketException)
                {
                    _isAlive = false;
                }
                throw;
            }
        }

        public async Task ReadAsync(byte[] buffer, int offset, int count)
        {
            this.CheckDisposed();

            int read = 0;
            int shouldRead = count;

            while (read < count)
            {
                try
                {
                    int currentRead = await _inputStream.ReadAsync(buffer, offset, shouldRead);
                    if (currentRead == count)
                        break;
                    if (currentRead < 1)
                        throw new IOException("The socket seems to be disconnected");

                    read += currentRead;
                    offset += currentRead;
                    shouldRead -= currentRead;
                }
                catch (Exception ex)
                {
                    if (ex is IOException || ex is SocketException)
                    {
                        _isAlive = false;
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Reads data from the server into the specified buffer.
        /// </summary>
        /// <param name="buffer">An array of <see cref="T:System.Byte"/> that is the storage location for the received data.</param>
        /// <param name="offset">The location in buffer to store the received data.</param>
        /// <param name="count">The number of bytes to read.</param>
        /// <remarks>This method blocks and will not return until the specified amount of bytes are read.</remarks>
        public void Read(byte[] buffer, int offset, int count)
        {
            this.CheckDisposed();

            int read = 0;
            int shouldRead = count;

            while (read < count)
            {
                try
                {
                    int currentRead = _inputStream.Read(buffer, offset, shouldRead);
                    if (currentRead == count)
                        break;
                    if (currentRead < 1)
                        throw new IOException("The socket seems to be disconnected");

                    read += currentRead;
                    offset += currentRead;
                    shouldRead -= currentRead;
                }
                catch (Exception ex)
                {
                    if (ex is IOException || ex is SocketException)
                    {
                        _isAlive = false;
                    }
                    throw;
                }
            }
        }

        public void Write(byte[] data, int offset, int length)
        {
            this.CheckDisposed();

            SocketError status;

            _socket.Send(data, offset, length, SocketFlags.None, out status);

            if (status != SocketError.Success)
            {
                _isAlive = false;

                ThrowHelper.ThrowSocketWriteError(_endpoint, status);
            }
        }

        public void Write(IList<ArraySegment<byte>> buffers)
        {
            this.CheckDisposed();

            SocketError status;

            try
            {
                _socket.Send(buffers, SocketFlags.None, out status);
                if (status != SocketError.Success)
                {
                    _isAlive = false;
                    ThrowHelper.ThrowSocketWriteError(_endpoint, status);
                }
            }
            catch (Exception ex)
            {
                if (ex is IOException || ex is SocketException)
                {
                    _isAlive = false;
                }
                _logger.LogError(ex, nameof(PooledSocket.Write));
                throw;
            }
        }

        public async Task WriteAsync(IList<ArraySegment<byte>> buffers)
        {
            CheckDisposed();

            try
            {
                var bytesTransferred = await _socket.SendAsync(buffers, SocketFlags.None);
                if (bytesTransferred <= 0)
                {
                    _isAlive = false;
                    _logger.LogError($"Failed to {nameof(PooledSocket.WriteAsync)}. bytesTransferred: {bytesTransferred}");
                    ThrowHelper.ThrowSocketWriteError(_endpoint);
                }
            }
            catch (Exception ex)
            {
                if (ex is IOException || ex is SocketException)
                {
                    _isAlive = false;
                }
                _logger.LogError(ex, nameof(PooledSocket.WriteAsync));
                throw;
            }
        }

        private IPEndPoint GetIPEndPoint(EndPoint endpoint)
        {
            if (endpoint is DnsEndPoint)
            {
                var dnsEndPoint = (DnsEndPoint)endpoint;
                var address = Dns.GetHostAddresses(dnsEndPoint.Host).FirstOrDefault(ip =>
                    ip.AddressFamily == AddressFamily.InterNetwork);
                if (address == null)
                    throw new ArgumentException(String.Format("Could not resolve host '{0}'.", endpoint));
                return new IPEndPoint(address, dnsEndPoint.Port);
            }
            else if (endpoint is IPEndPoint)
            {
                return endpoint as IPEndPoint;
            }
            else
            {
                throw new Exception("Not supported EndPoint type");
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
