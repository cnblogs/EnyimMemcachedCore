using MessagePack;
using MessagePack.Resolvers;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Enyim.Caching.Memcached.Transcoders
{
    public class MessagePackTranscoder : DefaultTranscoder
    {
        private readonly MessagePackSerializerOptions _options;

        public MessagePackTranscoder() : this(CreateDefaultOptions())
        {
        }

        public MessagePackTranscoder(MessagePackSerializerOptions options)
        {
            _options = options ?? CreateDefaultOptions();
        }

        protected override ArraySegment<byte> SerializeObject(object value)
        {
            var bytes = MessagePackSerializer.Serialize(value, _options);
            return new ArraySegment<byte>(bytes, 0, bytes.Length);
        }

        protected override object DeserializeObject(ArraySegment<byte> value)
        {
            throw new NotSupportedException("Does not support typeless deserialization. Please use generic api.");
        }

        public override T Deserialize<T>(CacheItem item)
        {
            if (typeof(T).GetTypeCode() != TypeCode.Object || typeof(T) == typeof(byte[]))
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

            return MessagePackSerializer.Deserialize<T>(item.Data, _options);
        }

        private static MessagePackSerializerOptions CreateDefaultOptions()
        {
            var resolver = CompositeResolver.Create(
                NativeDateTimeResolver.Instance,
                NativeGuidResolver.Instance,
                NativeDecimalResolver.Instance,
                ContractlessStandardResolverAllowPrivate.Instance,
                ContractlessStandardResolver.Instance);
            return MessagePackSerializerOptions.Standard.WithResolver(resolver);
        }
    }
}
