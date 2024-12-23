using ZstdSharp;
using System;
using Microsoft.Extensions.Logging;
using Enyim.Caching;

public static class ZSTDCompression
{
    /// <summary>
    /// Compresses the given data using ZSTD.
    /// </summary>
    /// <param name="data">The byte array to compress.</param>
    /// <returns>Compressed data as an ArraySegment of bytes.</returns>
    public static ArraySegment<byte> Compress(ArraySegment<byte> data, ILogger<MemcachedClient> _logger)
    {
        if (data.Count == 0)
        {
            return data;
        }

        try
        {
            using var compressor = new Compressor();
            byte[] compressedData = compressor.Wrap(data.Array).ToArray();
            return new ArraySegment<byte>(compressedData);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Compression failed.");
            return data;
        }
    }


    /// <summary>
    /// Decompresses the given compressed data using Zstd.
    /// </summary>
    /// <param name="data">The compressed byte array to decompress.</param>
    /// <returns>Decompressed data as an ArraySegment of bytes.</returns>
    public static ArraySegment<byte> Decompress(ArraySegment<byte> data, ILogger<MemcachedClient> _logger)
    {
        if (data.Count == 0)
        {
            return data;
        }

        try
        {
            using var decompressor = new Decompressor();
            return new ArraySegment<byte>(
                decompressor.Unwrap(new ReadOnlySpan<byte>(data.Array, data.Offset, data.Count))
                .ToArray());
            }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Decompression failed.");
            return data;
        }
    }
}
