using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached.Protocol.Binary;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;
using Enyim.Collections;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Security;
using System.Threading;
using System.Threading.Tasks;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// Represents a Memcached node in the pool.
    /// </summary>
    [DebuggerDisplay("{{MemcachedNode [ Address: {EndPoint}, IsAlive = {IsAlive} ]}}")]
    public class MemcachedNode : IMemcachedNode
    {
        private readonly ILogger _logger;
        private static readonly object SyncRoot = new Object();

        private bool isDisposed;

        private readonly EndPoint _endPoint;
        private readonly ISocketPoolConfiguration _config;
        private InternalPoolImpl internalPoolImpl;
        private bool isInitialized = false;
        private SemaphoreSlim poolInitSemaphore = new SemaphoreSlim(1, 1);
        private readonly TimeSpan _initPoolTimeout;

        public MemcachedNode(
            EndPoint endpoint,
            ISocketPoolConfiguration socketPoolConfig,
            ILogger logger)
        {
            _endPoint = endpoint;
            EndPointString = endpoint?.ToString().Replace("Unspecified/", string.Empty);
            _config = socketPoolConfig;

            if (socketPoolConfig.ConnectionTimeout.TotalMilliseconds >= Int32.MaxValue)
                throw new InvalidOperationException("ConnectionTimeout must be < Int32.MaxValue");

            if (socketPoolConfig.InitPoolTimeout.TotalSeconds < 1)
            {
                _initPoolTimeout = new TimeSpan(0, 1, 0);
            }
            else
            {
                _initPoolTimeout = socketPoolConfig.InitPoolTimeout;
            }

            _logger = logger;
            this.internalPoolImpl = new InternalPoolImpl(this, socketPoolConfig, _logger);
        }

        public event Action<IMemcachedNode> Failed;
        private INodeFailurePolicy failurePolicy;

        protected INodeFailurePolicy FailurePolicy
        {
            get { return this.failurePolicy ?? (this.failurePolicy = _config.FailurePolicyFactory.Create(this)); }
        }

        /// <summary>
        /// Gets the <see cref="T:IPEndPoint"/> of this instance
        /// </summary>
        public EndPoint EndPoint
        {
            get { return _endPoint; }
        }

        public string EndPointString { get; private set; }

        /// <summary>
        /// <para>Gets a value indicating whether the server is working or not. Returns a <b>cached</b> state.</para>
        /// <para>To get real-time information and update the cached state, use the <see cref="M:Ping"/> method.</para>
        /// </summary>
        /// <remarks>Used by the <see cref="T:ServerPool"/> to quickly check if the server's state is valid.</remarks>
        public bool IsAlive
        {
            get { return this.internalPoolImpl.IsAlive; }
        }

        /// <summary>
        /// Gets a value indicating whether the server is working or not.
        ///
        /// If the server is back online, we'll ercreate the internal socket pool and mark the server as alive so operations can target it.
        /// </summary>
        /// <returns>true if the server is alive; false otherwise.</returns>
        public bool Ping()
        {
            // is the server working?
            if (this.internalPoolImpl.IsAlive)
                return true;

            // this codepath is (should be) called very rarely
            // if you get here hundreds of times then you have bigger issues
            // and try to make the memcached instaces more stable and/or increase the deadTimeout
            try
            {
                // we could connect to the server, let's recreate the socket pool
                lock (SyncRoot)
                {
                    if (this.isDisposed) return false;

                    // try to connect to the server
                    using (var socket = this.CreateSocket())
                    {
                    }

                    if (this.internalPoolImpl.IsAlive)
                        return true;

                    // it's easier to create a new pool than reinitializing a dead one
                    // rewrite-then-dispose to avoid a race condition with Acquire (which does no locking)
                    var oldPool = this.internalPoolImpl;
                    var newPool = new InternalPoolImpl(this, _config, _logger);

                    Interlocked.Exchange(ref this.internalPoolImpl, newPool);

                    try { oldPool.Dispose(); }
                    catch { }
                }

                return true;
            }
            //could not reconnect
            catch { return false; }
        }

        /// <summary>
        /// Acquires a new item from the pool
        /// </summary>
        /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
        public IPooledSocketResult Acquire()
        {
            var result = new PooledSocketResult();
            if (!this.isInitialized)
            {
                if (!poolInitSemaphore.Wait(_initPoolTimeout))
                {
                    return result.Fail("Timeout to poolInitSemaphore.Wait", _logger) as PooledSocketResult;
                }

                try
                {
                    if (!this.isInitialized)
                    {
                        var startTime = DateTime.Now;
                        this.internalPoolImpl.InitPool();
                        this.isInitialized = true;
                        _logger.LogInformation("MemcachedInitPool-cost: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                    }
                }
                finally
                {
                    poolInitSemaphore.Release();
                }
            }

            try
            {
                return this.internalPoolImpl.Acquire();
            }
            catch (Exception e)
            {
                var message = "Acquire failed. Maybe we're already disposed?";
                _logger.LogError(message, e);

                result.Fail(message, e);
                return result;
            }
        }

        /// <summary>
        /// Acquires a new item from the pool
        /// </summary>
        /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
        public async Task<IPooledSocketResult> AcquireAsync()
        {
            var result = new PooledSocketResult();
            if (!this.isInitialized)
            {
                if (!await poolInitSemaphore.WaitAsync(_initPoolTimeout))
                {
                    return result.Fail("Timeout to poolInitSemaphore.Wait", _logger) as PooledSocketResult;
                }

                try
                {
                    if (!this.isInitialized)
                    {
                        var startTime = DateTime.Now;
                        await this.internalPoolImpl.InitPoolAsync();
                        this.isInitialized = true;
                        _logger.LogInformation("MemcachedInitPool-cost: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                    }
                }
                finally
                {
                    poolInitSemaphore.Release();
                }
            }

            try
            {
                return await this.internalPoolImpl.AcquireAsync();
            }
            catch (Exception e)
            {
                var message = "Acquire failed. Maybe we're already disposed?";
                _logger.LogError(message, e);
                result.Fail(message, e);
                return result;
            }
        }

        ~MemcachedNode()
        {
            try { ((IDisposable)this).Dispose(); }
            catch { }
        }

        /// <summary>
        /// Releases all resources allocated by this instance
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed) return;

            GC.SuppressFinalize(this);

            // this is not a graceful shutdown
            // if someone uses a pooled item then it's 99% that an exception will be thrown
            // somewhere. But since the dispose is mostly used when everyone else is finished
            // this should not kill any kittens
            lock (SyncRoot)
            {
                if (this.isDisposed) return;

                this.isDisposed = true;
                this.internalPoolImpl.Dispose();
                this.poolInitSemaphore.Dispose();
            }
        }

        void IDisposable.Dispose()
        {
            this.Dispose();
        }

        #region [ InternalPoolImpl             ]

        private class InternalPoolImpl : IDisposable
        {
            private readonly ILogger _logger;
            private readonly bool _isDebugEnabled;

            /// <summary>
            /// A list of already connected but free to use sockets
            /// </summary>
            private ConcurrentStack<PooledSocket> _freeItems;

            private bool isDisposed;
            private bool isAlive;
            private DateTime markedAsDeadUtc;

            private readonly int minItems;
            private readonly int maxItems;

            private MemcachedNode ownerNode;
            private readonly EndPoint _endPoint;
            private readonly TimeSpan queueTimeout;
            private readonly TimeSpan _receiveTimeout;
            private SemaphoreSlim _semaphore;

            private readonly object initLock = new Object();

            internal InternalPoolImpl(
                MemcachedNode ownerNode,
                ISocketPoolConfiguration config,
                ILogger logger)
            {
                if (config.MinPoolSize < 0)
                    throw new InvalidOperationException("minItems must be larger >= 0", null);
                if (config.MaxPoolSize < config.MinPoolSize)
                    throw new InvalidOperationException("maxItems must be larger than minItems", null);
                if (config.QueueTimeout < TimeSpan.Zero)
                    throw new InvalidOperationException("queueTimeout must be >= TimeSpan.Zero", null);
                if (config.ReceiveTimeout < TimeSpan.Zero)
                    throw new InvalidOperationException("ReceiveTimeout must be >= TimeSpan.Zero", null);

                this.ownerNode = ownerNode;
                this.isAlive = true;
                _endPoint = ownerNode.EndPoint;
                this.queueTimeout = config.QueueTimeout;
                _receiveTimeout = config.ReceiveTimeout;

                this.minItems = config.MinPoolSize;
                this.maxItems = config.MaxPoolSize;

                _semaphore = new SemaphoreSlim(maxItems, maxItems);
                _freeItems = new ConcurrentStack<PooledSocket>();

                _logger = logger;
                _isDebugEnabled = _logger.IsEnabled(LogLevel.Debug);
            }

            internal void InitPool()
            {
                try
                {
                    if (this.minItems > 0)
                    {
                        for (int i = 0; i < this.minItems; i++)
                        {
                            try
                            {
                                _freeItems.Push(CreateSocket());
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, $"Failed to put {nameof(PooledSocket)} {i} in Pool");
                            }

                            // cannot connect to the server
                            if (!this.isAlive)
                                break;
                        }
                    }

                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Pool has been inited for {0} with {1} sockets", _endPoint, this.minItems);

                }
                catch (Exception e)
                {
                    _logger.LogError("Could not init pool.", new EventId(0), e);

                    this.MarkAsDead();
                }
            }

            internal async Task InitPoolAsync()
            {
                try
                {
                    if (this.minItems > 0)
                    {
                        for (int i = 0; i < this.minItems; i++)
                        {
                            try
                            {
                                _freeItems.Push(await CreateSocketAsync());
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, $"Failed to put {nameof(PooledSocket)} {i} in Pool");
                            }

                            // cannot connect to the server
                            if (!this.isAlive)
                                break;
                        }
                    }

                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Pool has been inited for {0} with {1} sockets", _endPoint, this.minItems);

                }
                catch (Exception e)
                {
                    _logger.LogError("Could not init pool.", new EventId(0), e);

                    this.MarkAsDead();
                }
            }

            private async Task<PooledSocket> CreateSocketAsync()
            {
                var ps = await this.ownerNode.CreateSocketAsync();
                ps.CleanupCallback = this.ReleaseSocket;

                return ps;
            }

            private PooledSocket CreateSocket()
            {
                var ps = this.ownerNode.CreateSocket();
                ps.CleanupCallback = this.ReleaseSocket;

                return ps;
            }

            public bool IsAlive
            {
                get { return this.isAlive; }
            }

            public DateTime MarkedAsDeadUtc
            {
                get { return this.markedAsDeadUtc; }
            }

            /// <summary>
            /// Acquires a new item from the pool
            /// </summary>
            /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
            public IPooledSocketResult Acquire()
            {
                var result = new PooledSocketResult();
                var message = string.Empty;

                if (_isDebugEnabled) _logger.LogDebug($"Acquiring stream from pool on node '{_endPoint}'");

                if (!this.isAlive || this.isDisposed)
                {
                    message = "Pool is dead or disposed, returning null. " + _endPoint;
                    result.Fail(message);

                    if (_isDebugEnabled) _logger.LogDebug(message);

                    return result;
                }

                PooledSocket retval = null;

                if (!_semaphore.Wait(this.queueTimeout))
                {
                    message = "Pool is full, timeouting. " + _endPoint;
                    if (_isDebugEnabled) _logger.LogDebug(message);
                    result.Fail(message, new TimeoutException());

                    // everyone is so busy
                    return result;
                }

                // maybe we died while waiting
                if (!this.isAlive)
                {
                    _semaphore.Release();

                    message = "Pool is dead, returning null. " + _endPoint;
                    if (_isDebugEnabled) _logger.LogDebug(message);
                    result.Fail(message);

                    return result;
                }

                // do we have free items?
                if (_freeItems.TryPop(out retval))
                {
                    #region [ get it from the pool         ]

                    try
                    {
                        retval.Reset();

                        message = "Socket was reset. " + retval.InstanceId;
                        if (_isDebugEnabled) _logger.LogDebug(message);

                        result.Pass(message);
                        result.Value = retval;
                        return result;
                    }
                    catch (Exception e)
                    {
                        message = "Failed to reset an acquired socket.";
                        _logger.LogError(message, e);

                        this.MarkAsDead();
                        _semaphore.Release();

                        result.Fail(message, e);
                        return result;
                    }

                    #endregion
                }

                // free item pool is empty
                message = "Could not get a socket from the pool, Creating a new item. " + _endPoint;
                if (_isDebugEnabled) _logger.LogDebug(message);

                try
                {
                    // okay, create the new item
                    var startTime = DateTime.Now;
                    retval = this.CreateSocket();
                    _logger.LogInformation("MemcachedAcquire-CreateSocket: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                    result.Value = retval;
                    result.Pass();
                }
                catch (Exception e)
                {
                    message = "Failed to create socket. " + _endPoint;
                    _logger.LogError(message, e);

                    // eventhough this item failed the failure policy may keep the pool alive
                    // so we need to make sure to release the semaphore, so new connections can be
                    // acquired or created (otherwise dead conenctions would "fill up" the pool
                    // while the FP pretends that the pool is healthy)
                    _semaphore.Release();

                    this.MarkAsDead();
                    result.Fail(message);
                    return result;
                }

                if (_isDebugEnabled) _logger.LogDebug("Done.");

                return result;
            }

            /// <summary>
            /// Acquires a new item from the pool
            /// </summary>
            /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
            public async Task<IPooledSocketResult> AcquireAsync()
            {
                var result = new PooledSocketResult();
                var message = string.Empty;

                if (_isDebugEnabled) _logger.LogDebug("Acquiring stream from pool. " + _endPoint);

                if (!this.isAlive || this.isDisposed)
                {
                    message = "Pool is dead or disposed, returning null. " + _endPoint;
                    result.Fail(message);

                    if (_isDebugEnabled) _logger.LogDebug(message);

                    return result;
                }

                PooledSocket retval = null;

                if (!await _semaphore.WaitAsync(this.queueTimeout))
                {
                    message = "Pool is full, timeouting. " + _endPoint;
                    if (_isDebugEnabled) _logger.LogDebug(message);
                    result.Fail(message, new TimeoutException());

                    // everyone is so busy
                    return result;
                }

                // maybe we died while waiting
                if (!this.isAlive)
                {
                    _semaphore.Release();

                    message = "Pool is dead, returning null. " + _endPoint;
                    if (_isDebugEnabled) _logger.LogDebug(message);
                    result.Fail(message);
                    return result;
                }

                // do we have free items?
                if (_freeItems.TryPop(out retval))
                {
                    #region [ get it from the pool         ]

                    try
                    {
                        var resetTask = retval.ResetAsync();

                        if (await Task.WhenAny(resetTask, Task.Delay(_receiveTimeout)) == resetTask)
                        {
                            await resetTask;
                        }
                        else
                        {
                            _semaphore.Release();
                            retval.IsAlive = false;

                            message = "Timeout to reset an acquired socket. InstanceId " + retval.InstanceId;
                            _logger.LogError(message);
                            result.Fail(message);
                            return result;
                        }

                        message = "Socket was reset. InstanceId " + retval.InstanceId;
                        if (_isDebugEnabled) _logger.LogDebug(message);

                        result.Pass(message);
                        result.Value = retval;
                        return result;
                    }
                    catch (Exception e)
                    {
                        MarkAsDead();
                        _semaphore.Release();

                        message = "Failed to reset an acquired socket.";
                        _logger.LogError(message, e);
                        result.Fail(message, e);
                        return result;
                    }

                    #endregion
                }

                // free item pool is empty
                message = "Could not get a socket from the pool, Creating a new item. " + _endPoint;
                if (_isDebugEnabled) _logger.LogDebug(message);


                try
                {
                    // okay, create the new item
                    var startTime = DateTime.Now;
                    retval = await this.CreateSocketAsync();
                    _logger.LogInformation("MemcachedAcquire-CreateSocket: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                    result.Value = retval;
                    result.Pass();
                }
                catch (Exception e)
                {
                    message = "Failed to create socket. " + _endPoint;
                    _logger.LogError(message, e);

                    // eventhough this item failed the failure policy may keep the pool alive
                    // so we need to make sure to release the semaphore, so new connections can be
                    // acquired or created (otherwise dead conenctions would "fill up" the pool
                    // while the FP pretends that the pool is healthy)
                    _semaphore.Release();

                    this.MarkAsDead();
                    result.Fail(message);
                    return result;
                }

                if (_isDebugEnabled) _logger.LogDebug("Done.");

                return result;
            }

            private void MarkAsDead()
            {
                if (_isDebugEnabled) _logger.LogDebug("Mark as dead was requested for {0}", _endPoint);

                var shouldFail = ownerNode.FailurePolicy.ShouldFail();

                if (_isDebugEnabled) _logger.LogDebug("FailurePolicy.ShouldFail(): " + shouldFail);

                if (shouldFail)
                {
                    if (_logger.IsEnabled(LogLevel.Warning)) _logger.LogWarning("Marking node {0} as dead", _endPoint);

                    this.isAlive = false;
                    this.markedAsDeadUtc = DateTime.UtcNow;

                    var f = this.ownerNode.Failed;

                    if (f != null)
                        f(this.ownerNode);
                }
            }

            /// <summary>
            /// Releases an item back into the pool
            /// </summary>
            /// <param name="socket"></param>
            private void ReleaseSocket(PooledSocket socket)
            {
                if (_isDebugEnabled)
                {
                    _logger.LogDebug("Releasing socket " + socket.InstanceId);
                    _logger.LogDebug("Are we alive? " + this.isAlive);
                }

                if (this.isAlive)
                {
                    // is it still working (i.e. the server is still connected)
                    if (socket.IsAlive)
                    {
                        try
                        {
                            // mark the item as free
                            _freeItems.Push(socket);
                        }
                        finally
                        {
                            // signal the event so if someone is waiting for it can reuse this item
                            if (_semaphore != null)
                            {
                                _semaphore.Release();
                            }
                        }
                    }
                    else
                    {
                        try
                        {
                            // kill this item
                            socket.Destroy();

                            // mark ourselves as not working for a while
                            this.MarkAsDead();
                        }
                        finally
                        {
                            // make sure to signal the Acquire so it can create a new conenction
                            // if the failure policy keeps the pool alive
                            if (_semaphore != null)
                            {
                                _semaphore.Release();
                            }
                        }
                    }
                }
                else
                {
                    try
                    {
                        // one of our previous sockets has died, so probably all of them
                        // are dead. so, kill the socket (this will eventually clear the pool as well)
                        socket.Destroy();
                    }
                    finally
                    {
                        if (_semaphore != null)
                        {
                            _semaphore.Release();
                        }
                    }
                }
            }


            ~InternalPoolImpl()
            {
                try { ((IDisposable)this).Dispose(); }
                catch { }
            }

            /// <summary>
            /// Releases all resources allocated by this instance
            /// </summary>
            public void Dispose()
            {
                // this is not a graceful shutdown
                // if someone uses a pooled item then 99% that an exception will be thrown
                // somewhere. But since the dispose is mostly used when everyone else is finished
                // this should not kill any kittens
                if (!this.isDisposed)
                {
                    this.isAlive = false;
                    this.isDisposed = true;

                    PooledSocket ps;

                    while (_freeItems.TryPop(out ps))
                    {
                        try { ps.Destroy(); }
                        catch { }
                    }

                    this.ownerNode = null;
                    _semaphore.Dispose();
                    _semaphore = null;
                    _freeItems = null;
                }
            }

            void IDisposable.Dispose()
            {
                this.Dispose();
            }
        }

        #endregion
        #region [ Comparer                     ]
        internal sealed class Comparer : IEqualityComparer<IMemcachedNode>
        {
            public static readonly Comparer Instance = new Comparer();

            bool IEqualityComparer<IMemcachedNode>.Equals(IMemcachedNode x, IMemcachedNode y)
            {
                return x.EndPoint.Equals(y.EndPoint);
            }

            int IEqualityComparer<IMemcachedNode>.GetHashCode(IMemcachedNode obj)
            {
                return obj.EndPoint.GetHashCode();
            }
        }
        #endregion

        protected internal virtual PooledSocket CreateSocket()
        {
            try
            {
                var ps = new PooledSocket(_endPoint, _config.ConnectionTimeout, _config.ReceiveTimeout, _logger);
                ps.Connect();
                return ps;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Create {nameof(PooledSocket)}");
                throw;
            }

        }

        protected internal virtual async Task<PooledSocket> CreateSocketAsync()
        {
            try
            {
                var ps = new PooledSocket(_endPoint, _config.ConnectionTimeout, _config.ReceiveTimeout, _logger);
                await ps.ConnectAsync();
                return ps;
            }
            catch (Exception ex)
            {
                var endPointStr = _endPoint.ToString().Replace("Unspecified/", string.Empty);
                _logger.LogError(ex, $"Failed to {nameof(CreateSocketAsync)} to {endPointStr}");
                throw;
            }
        }

        //protected internal virtual PooledSocket CreateSocket(IPEndPoint endpoint, TimeSpan connectionTimeout, TimeSpan receiveTimeout)
        //{
        //    PooledSocket retval = new PooledSocket(endPoint, connectionTimeout, receiveTimeout);

        //    return retval;
        //}

        protected virtual IPooledSocketResult ExecuteOperation(IOperation op)
        {
            var result = this.Acquire();
            if (result.Success && result.HasValue)
            {
                try
                {
                    var socket = result.Value;
                    //if Get, call BinaryRequest.CreateBuffer()
                    var b = op.GetBuffer();

                    var startTime = DateTime.Now;
                    socket.Write(b);
                    LogExecutionTime("ExecuteOperation_socket_write", startTime, 50);

                    //if Get, call BinaryResponse
                    var readResult = op.ReadResponse(socket);
                    if (readResult.Success)
                    {
                        result.Pass();
                    }
                    else
                    {
                        readResult.Combine(result);
                    }
                    return result;
                }
                catch (IOException e)
                {
                    _logger.LogError(e, $"Failed to ExecuteOperation on {EndPointString}");

                    result.Fail("Exception reading response", e);
                    return result;
                }
                finally
                {
                    ((IDisposable)result.Value).Dispose();
                }
            }
            else
            {
                var errorMsg = string.IsNullOrEmpty(result.Message) ? "Failed to acquire a socket from pool" : result.Message;
                _logger.LogError(errorMsg);
                return result;
            }

        }

        protected virtual async Task<IPooledSocketResult> ExecuteOperationAsync(IOperation op)
        {
            _logger.LogDebug($"ExecuteOperationAsync({op})");

            var result = await this.AcquireAsync();
            if (result.Success && result.HasValue)
            {
                try
                {
                    var pooledSocket = result.Value;

                    //if Get, call BinaryRequest.CreateBuffer()
                    var b = op.GetBuffer();

                    _logger.LogDebug("pooledSocket.WriteAsync...");

                    var writeSocketTask = pooledSocket.WriteAsync(b);
                    if (await Task.WhenAny(writeSocketTask, Task.Delay(_config.ConnectionTimeout)) != writeSocketTask)
                    {
                        result.Fail("Timeout to pooledSocket.WriteAsync");
                        return result;
                    }
                    await writeSocketTask;

                    //if Get, call BinaryResponse
                    _logger.LogDebug($"{op}.ReadResponseAsync...");

                    var readResponseTask = op.ReadResponseAsync(pooledSocket);
                    if (await Task.WhenAny(readResponseTask, Task.Delay(_config.ConnectionTimeout)) != readResponseTask)
                    {
                        result.Fail($"Timeout to ReadResponseAsync(pooledSocket) for {op}");
                        return result;
                    }

                    var readResult = await readResponseTask;
                    if (readResult.Success)
                    {
                        result.Pass();
                    }
                    else
                    {
                        _logger.LogInformation($"{op}.{nameof(op.ReadResponseAsync)} result: {readResult.Message}");
                        readResult.Combine(result);
                    }
                    return result;
                }
                catch (IOException e)
                {
                    _logger.LogError(e, $"IOException occurs when ExecuteOperationAsync({op}) on {EndPointString}");

                    result.Fail("IOException reading response", e);
                    return result;
                }
                catch (SocketException e)
                {
                    _logger.LogError(e, $"SocketException occurs when ExecuteOperationAsync({op}) on {EndPointString}");

                    result.Fail("SocketException reading response", e);
                    return result;
                }
                finally
                {
                    ((IDisposable)result.Value).Dispose();
                }
            }
            else
            {
                var errorMsg = string.IsNullOrEmpty(result.Message) ? "Failed to acquire a socket from pool" : result.Message;
                _logger.LogError(errorMsg);
                return result;
            }
        }

        protected virtual async Task<bool> ExecuteOperationAsync(IOperation op, Action<bool> next)
        {
            var socket = (await this.AcquireAsync()).Value;
            if (socket == null) return false;

            //key(string) to buffer(btye[])
            var b = op.GetBuffer();

            try
            {
                await socket.WriteAsync(b);

                var rrs = await op.ReadResponseAsync(socket, readSuccess =>
                {
                    ((IDisposable)socket).Dispose();

                    next(readSuccess);
                });

                return rrs;
            }
            catch (IOException e)
            {
                _logger.LogError(e, $"Failed to ExecuteOperationAsync({op}) with next action on {EndPointString}");
                ((IDisposable)socket).Dispose();

                return false;
            }
        }

        private void LogExecutionTime(string title, DateTime startTime, int thresholdMs)
        {
            var duration = (DateTime.Now - startTime).TotalMilliseconds;
            if (duration > thresholdMs)
            {
                _logger.LogWarning("MemcachedNode-{0}: {1}ms", title, duration);
            }
        }

        #region [ IMemcachedNode               ]

        EndPoint IMemcachedNode.EndPoint
        {
            get { return _endPoint; }
        }

        bool IMemcachedNode.IsAlive
        {
            get { return this.IsAlive; }
        }

        bool IMemcachedNode.Ping()
        {
            return this.Ping();
        }

        IOperationResult IMemcachedNode.Execute(IOperation op)
        {
            return ExecuteOperation(op);
        }

        async Task<IOperationResult> IMemcachedNode.ExecuteAsync(IOperation op)
        {
            return await ExecuteOperationAsync(op);
        }

        async Task<bool> IMemcachedNode.ExecuteAsync(IOperation op, Action<bool> next)
        {
            return await ExecuteOperationAsync(op, next);
        }

        event Action<IMemcachedNode> IMemcachedNode.Failed
        {
            add { this.Failed += value; }
            remove { this.Failed -= value; }
        }

        #endregion
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
