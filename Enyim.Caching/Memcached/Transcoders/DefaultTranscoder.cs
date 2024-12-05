using System;
using System.Linq;
using System.IO;
using System.Text;
using System.Runtime.Serialization;
using Newtonsoft.Json.Bson;
using System.Collections;
using System.Reflection;
using Newtonsoft.Json;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// Default <see cref="T:Enyim.Caching.Memcached.ITranscoder"/> implementation. Primitive types are manually serialized, the rest is serialized using <see cref="T:System.Runtime.Serialization.Formatters.Binary.BinaryFormatter"/>.
    /// </summary>
    public class DefaultTranscoder : ITranscoder
    {
        public const uint RawDataFlag = 0xfa52;
        private static readonly ArraySegment<byte> NullArray = new([]);

        CacheItem ITranscoder.Serialize(object value)
        {
            return this.Serialize(value);
        }

        object ITranscoder.Deserialize(CacheItem item)
        {
            return this.Deserialize(item);
        }

        public virtual T Deserialize<T>(CacheItem item)
        {
            if (typeof(T).GetTypeCode() != TypeCode.Object || typeof(T) == typeof(Byte[]))
            {
                var value = Deserialize(item);
                if (value != null)
                {
                    if (typeof(T) == typeof(Guid))
                    {
                       return (T)(object)new Guid((string)value);
                    }
                    else
                    {
                        return (T)value;
                    }
                }
                else
                {
                    return default;
                }
            }

            using var ms = new MemoryStream([.. item.Data]);
            using var reader = new BsonDataReader(ms);
            if (typeof(T).GetTypeInfo().ImplementedInterfaces.Contains(typeof(IEnumerable)))
            {
                reader.ReadRootValueAsArray = true;
            }
            var serializer = new JsonSerializer();
            return serializer.Deserialize<T>(reader);
        }

        protected virtual CacheItem Serialize(object value)
        {
            // raw data is a special case when some1 passes in a buffer (byte[] or ArraySegment<byte>)
            if (value is ArraySegment<byte> segment)
            {
                // ArraySegment<byte> is only passed in when a part of buffer is being 
                // serialized, usually from a MemoryStream (To avoid duplicating arrays 
                // the byte[] returned by MemoryStream.GetBuffer is placed into an ArraySegment.)
                return new CacheItem(RawDataFlag, segment);
            }


            // - or we just received a byte[]. No further processing is needed.
            if (value is byte[] tmpByteArray)
            {
                return new CacheItem(RawDataFlag, new ArraySegment<byte>(tmpByteArray));
            }

            ArraySegment<byte> data;
            // TypeCode.DBNull is 2
            TypeCode code = value == null ? (TypeCode)2 : Type.GetTypeCode(value.GetType());

            switch (code)
            {
                case (TypeCode)2: data = this.SerializeNull(); break; // TypeCode.DBNull
                case TypeCode.String: data = this.SerializeString(value.ToString()); break;
                case TypeCode.Boolean: data = this.SerializeBoolean((Boolean)value); break;
                case TypeCode.SByte: data = this.SerializeSByte((SByte)value); break;
                case TypeCode.Byte: data = this.SerializeByte((Byte)value); break;
                case TypeCode.Int16: data = this.SerializeInt16((Int16)value); break;
                case TypeCode.Int32: data = this.SerializeInt32((Int32)value); break;
                case TypeCode.Int64: data = this.SerializeInt64((Int64)value); break;
                case TypeCode.UInt16: data = this.SerializeUInt16((UInt16)value); break;
                case TypeCode.UInt32: data = this.SerializeUInt32((UInt32)value); break;
                case TypeCode.UInt64: data = this.SerializeUInt64((UInt64)value); break;
                case TypeCode.Char: data = this.SerializeChar((Char)value); break;
                case TypeCode.DateTime: data = this.SerializeDateTime((DateTime)value); break;
                case TypeCode.Double: data = this.SerializeDouble((Double)value); break;
                case TypeCode.Single: data = this.SerializeSingle((Single)value); break;
                default:
                    code = TypeCode.Object;
                    data = this.SerializeObject(value);
                    break;
            }

            return new CacheItem(TypeCodeToFlag(code), data);
        }

        public static uint TypeCodeToFlag(TypeCode code)
        {
            return (uint)((int)code | 0x0100);
        }

        public static bool IsFlagHandled(uint flag)
        {
            return (flag & 0x100) == 0x100;
        }

        protected virtual object Deserialize(CacheItem item)
        {
            if (item.Data.Array == null)
                return null;

            if (item.Flags == RawDataFlag)
            {
                var tmp = item.Data;

                if (tmp.Count == tmp.Array.Length)
                    return tmp.Array;

                // we should never arrive here, but it's better to be safe than sorry
                var retval = new byte[tmp.Count];

                Array.Copy(tmp.Array, tmp.Offset, retval, 0, tmp.Count);

                return retval;
            }

            var code = (TypeCode)(item.Flags & 0xff);

            var data = item.Data;

            return code switch
            {
                // incrementing a non-existing key then getting it
                // returns as a string, but the flag will be 0
                // so treat all 0 flagged items as string
                // this may help inter-client data management as well
                //
                // however we store 'null' as Empty + an empty array, 
                // so this must special-cased for compatibilty with 
                // earlier versions. we introduced DBNull as null marker in emc2.6
                TypeCode.Empty => (data.Array == null || data.Count == 0)
                                            ? null
                                            : DeserializeString(data),
                (TypeCode)2 => null,// TypeCode.DBNull
                TypeCode.String => this.DeserializeString(data),
                TypeCode.Boolean => this.DeserializeBoolean(data),
                TypeCode.Int16 => this.DeserializeInt16(data),
                TypeCode.Int32 => this.DeserializeInt32(data),
                TypeCode.Int64 => this.DeserializeInt64(data),
                TypeCode.UInt16 => this.DeserializeUInt16(data),
                TypeCode.UInt32 => this.DeserializeUInt32(data),
                TypeCode.UInt64 => this.DeserializeUInt64(data),
                TypeCode.Char => this.DeserializeChar(data),
                TypeCode.DateTime => this.DeserializeDateTime(data),
                TypeCode.Double => this.DeserializeDouble(data),
                TypeCode.Single => this.DeserializeSingle(data),
                TypeCode.Byte => this.DeserializeByte(data),
                TypeCode.SByte => this.DeserializeSByte(data),
                // backward compatibility
                // earlier versions serialized decimals with TypeCode.Decimal
                // even though they were saved by BinaryFormatter
                TypeCode.Decimal or TypeCode.Object => this.DeserializeObject(data),
                _ => throw new InvalidOperationException("Unknown TypeCode was returned: " + code),
            };
        }

        #region [ Typed serialization          ]

        protected virtual ArraySegment<byte> SerializeNull()
        {
            return NullArray;
        }

        protected virtual ArraySegment<byte> SerializeString(string value)
        {
            return new ArraySegment<byte>(Encoding.UTF8.GetBytes((string)value));
        }

        protected virtual ArraySegment<byte> SerializeByte(byte value)
        {
            return new ArraySegment<byte>([value]);
        }

        protected virtual ArraySegment<byte> SerializeSByte(sbyte value)
        {
            return new ArraySegment<byte>([(byte)value]);
        }

        protected virtual ArraySegment<byte> SerializeBoolean(bool value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeInt16(Int16 value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeInt32(Int32 value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeInt64(Int64 value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeUInt16(UInt16 value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeUInt32(UInt32 value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeUInt64(UInt64 value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeChar(char value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeDateTime(DateTime value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value.ToBinary()));
        }

        protected virtual ArraySegment<byte> SerializeDouble(Double value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeSingle(Single value)
        {
            return new ArraySegment<byte>(BitConverter.GetBytes(value));
        }

        protected virtual ArraySegment<byte> SerializeObject(object value)
        {
            using var ms = new MemoryStream();
            using var writer = new BsonDataWriter(ms);
            var serializer = new JsonSerializer();
            serializer.Serialize(writer, value);
            return new ArraySegment<byte>(ms.ToArray(), 0, (int)ms.Length);
        }

        #endregion
        #region [ Typed deserialization        ]

        protected virtual String DeserializeString(ArraySegment<byte> value)
        {
            return Encoding.UTF8.GetString(value.Array, value.Offset, value.Count);
        }

        protected virtual Boolean DeserializeBoolean(ArraySegment<byte> value)
        {
            return BitConverter.ToBoolean(value.Array, value.Offset);
        }

        protected virtual Int16 DeserializeInt16(ArraySegment<byte> value)
        {
            return BitConverter.ToInt16(value.Array, value.Offset);
        }

        protected virtual Int32 DeserializeInt32(ArraySegment<byte> value)
        {
            return BitConverter.ToInt32(value.Array, value.Offset);
        }

        protected virtual Int64 DeserializeInt64(ArraySegment<byte> value)
        {
            return BitConverter.ToInt64(value.Array, value.Offset);
        }

        protected virtual UInt16 DeserializeUInt16(ArraySegment<byte> value)
        {
            return BitConverter.ToUInt16(value.Array, value.Offset);
        }

        protected virtual UInt32 DeserializeUInt32(ArraySegment<byte> value)
        {
            return BitConverter.ToUInt32(value.Array, value.Offset);
        }

        protected virtual UInt64 DeserializeUInt64(ArraySegment<byte> value)
        {
            return BitConverter.ToUInt64(value.Array, value.Offset);
        }

        protected virtual Char DeserializeChar(ArraySegment<byte> value)
        {
            return BitConverter.ToChar(value.Array, value.Offset);
        }

        protected virtual DateTime DeserializeDateTime(ArraySegment<byte> value)
        {
            return DateTime.FromBinary(BitConverter.ToInt64(value.Array, value.Offset));
        }

        protected virtual Double DeserializeDouble(ArraySegment<byte> value)
        {
            return BitConverter.ToDouble(value.Array, value.Offset);
        }

        protected virtual Single DeserializeSingle(ArraySegment<byte> value)
        {
            return BitConverter.ToSingle(value.Array, value.Offset);
        }

        protected virtual Byte DeserializeByte(ArraySegment<byte> data)
        {
            return data.Array[data.Offset];
        }

        protected virtual SByte DeserializeSByte(ArraySegment<byte> data)
        {
            return (SByte)data.Array[data.Offset];
        }

        protected virtual object DeserializeObject(ArraySegment<byte> value)
        {
            using var ms = new MemoryStream(value.Array, value.Offset, value.Count);
            using var reader = new BsonDataReader(ms);
            JsonSerializer serializer = new();
            return serializer.Deserialize(reader);
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
