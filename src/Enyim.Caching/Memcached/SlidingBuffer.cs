using System;
using System.Collections.Generic;
using System.Threading;
using Enyim.Collections;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// Supports exactly one reader and writer, but they can access the buffer concurrently.
    /// </summary>
    internal class SlidingBuffer
    {
        private readonly InterlockedQueue<Segment> _buffers;
        private readonly int _chunkSize;
        private Segment _lastSegment;
        private int _available;

        public SlidingBuffer(int chunkSize)
        {
            _chunkSize = chunkSize;
            _buffers = new InterlockedQueue<Segment>();
        }

        public int Available { get { return _available; } }

        public int Read(byte[] buffer, int offset, int count)
        {
            var read = 0;
            Segment segment;

            while (read < count && _buffers.Peek(out segment))
            {
                var available = Math.Min(segment.WriteOffset - segment.ReadOffset, count - read);

                if (available > 0)
                {
                    System.Buffer.BlockCopy(segment.Data, segment.ReadOffset, buffer, offset + read, available);

                    read += available;
                    segment.ReadOffset += available;
                }

                // are we at the end of the segment?
                if (segment.ReadOffset == segment.WriteOffset)
                {
                    // we can dispose the current segment if it's not the last 
                    // (which is probably being written by the receiver)
                    if (_lastSegment != segment)
                    {
                        _buffers.Dequeue(out segment);
                        //Debug.Assert(success, "Could peek but could not dequeue?");

                        //bufferManager.ReturnBuffer(segment.Data);
                    }
                }
            }

            Interlocked.Add(ref _available, -read);

            return read;
        }

        public void Append(byte[] buffer, int offset, int count)
        {
            if (buffer == null || buffer.Length == 0 || count == 0) return;

            // try to append the data to the last segment
            // if the data is larger than the ChunkSize we copy it and append it as one chunk
            // if the data does not fit into the last segment we allocate a new
            // so data is never split (at the price of some wasted bytes)
            var last = _lastSegment;
            var shouldQueue = false;

            if (count > _chunkSize)
            {
                // big data, append it
                last = new Segment(new byte[count]);// bufferManager.TakeBuffer(count));
                shouldQueue = true;
            }
            else
            {
                var remaining = (last == null)
                                ? 0
                                : last.Data.Length - last.WriteOffset;

                // no space, create a new chunk
                if (remaining < count)
                {
                    last = new Segment(new byte[_chunkSize]); // bufferManager.TakeBuffer(_chunkSize));
                    shouldQueue = true;
                }
            }

            System.Buffer.BlockCopy(buffer, offset, last.Data, last.WriteOffset, count);

            // first we update the lastSegment reference, then we enque the new segment
            // this way Read can safely dequeue (discard) the last item it processed and 
            // continue  on the next one
            // doing it in reverse would make Read dequeue the current segment (the one we just inserted)
            if (shouldQueue)
            {
                Interlocked.Exchange(ref _lastSegment, last);
                _buffers.Enqueue(last);
            }

            // advertise that we have more data available for reading
            // we have to use Interlocked because in the same time the reader
            // can remove data and will decrease the value of Available
            Interlocked.Add(ref last.WriteOffset, count);
            Interlocked.Add(ref _available, count);
        }

        public void UnsafeClear()
        {
            Segment tmp;

            _lastSegment = null;
            while (_buffers.Dequeue(out tmp)) ;

            _available = 0;
        }

        #region [ Segment                      ]

        private class Segment
        {
            public Segment(byte[] data)
            {
                Data = data;
            }

            public readonly byte[] Data;
            public int WriteOffset;
            public int ReadOffset;
        }

        #endregion
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
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
