using System;
using System.Text;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
    public class BinaryResponse
    {
        private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(BinaryResponse));

        private const byte MAGIC_VALUE = 0x81;
        private const int HeaderLength = 24;

        private const int HEADER_OPCODE = 1;
        private const int HEADER_KEY = 2; // 2-3
        private const int HEADER_EXTRA = 4;
        private const int HEADER_DATATYPE = 5;
        private const int HEADER_STATUS = 6; // 6-7
        private const int HEADER_BODY = 8; // 8-11
        private const int HEADER_OPAQUE = 12; // 12-15
        private const int HEADER_CAS = 16; // 16-23

        public byte Opcode;
        public int KeyLength;
        public byte DataType;
        public int StatusCode;

        public int CorrelationId;
        public ulong CAS;

        public ArraySegment<byte> Extra;
        public ArraySegment<byte> Data;

        private string responseMessage;

        public BinaryResponse()
        {
            StatusCode = -1;
        }

        public string GetStatusMessage()
        {
            return Data.Array == null
                    ? null
                    : (responseMessage
                        ?? (responseMessage = Encoding.ASCII.GetString(Data.Array, Data.Offset, Data.Count)));
        }

        public unsafe bool Read(PooledSocket socket)
        {
            StatusCode = -1;

            if (!socket.IsAlive)
                return false;

            var header = new byte[HeaderLength];
            socket.Read(header, 0, header.Length);

            int dataLength, extraLength;

            DeserializeHeader(header, out dataLength, out extraLength);

            if (dataLength > 0)
            {
                var data = new byte[dataLength];
                socket.Read(data, 0, dataLength);

                Extra = new ArraySegment<byte>(data, 0, extraLength);
                Data = new ArraySegment<byte>(data, extraLength, data.Length - extraLength);
            }

            return StatusCode == 0;
        }

        public async Task<bool> ReadAsync(PooledSocket socket)
        {
            StatusCode = -1;

            if (!socket.IsAlive) return false;

            var header = new byte[HeaderLength];
            await socket.ReadAsync(header, 0, header.Length);

            int dataLength, extraLength;

            DeserializeHeader(header, out dataLength, out extraLength);

            if (dataLength > 0)
            {
                var data = new byte[dataLength];
                await socket.ReadAsync(data, 0, dataLength);

                Extra = new ArraySegment<byte>(data, 0, extraLength);
                Data = new ArraySegment<byte>(data, extraLength, data.Length - extraLength);
            }

            return StatusCode == 0;
        }

        private unsafe void DeserializeHeader(byte[] header, out int dataLength, out int extraLength)
        {
            fixed (byte* buffer = header)
            {
                if (buffer[0] != MAGIC_VALUE)
                    throw new InvalidOperationException("Expected magic value " + MAGIC_VALUE + ", received: " + buffer[0]);

                DataType = buffer[HEADER_DATATYPE];
                Opcode = buffer[HEADER_OPCODE];
                StatusCode = BinaryConverter.DecodeUInt16(buffer, HEADER_STATUS);

                KeyLength = BinaryConverter.DecodeUInt16(buffer, HEADER_KEY);
                CorrelationId = BinaryConverter.DecodeInt32(buffer, HEADER_OPAQUE);
                CAS = BinaryConverter.DecodeUInt64(buffer, HEADER_CAS);

                dataLength = BinaryConverter.DecodeInt32(buffer, HEADER_BODY);
                extraLength = buffer[HEADER_EXTRA];
            }
        }

        private void LogExecutionTime(string title, DateTime startTime, int thresholdMs)
        {
            var duration = (DateTime.Now - startTime).TotalMilliseconds;
            if (duration > thresholdMs)
            {
                log.WarnFormat("MemcachedBinaryResponse-{0}: {1}ms", title, duration);
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
