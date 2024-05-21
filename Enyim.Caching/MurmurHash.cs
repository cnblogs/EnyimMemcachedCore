using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Enyim.Caching
{
    //-----------------------------------------------------------------------------
    // MurmurHash3 was written by Austin Appleby, and is placed in the public
    // domain. The author hereby disclaims copyright to this source code.
    internal static class MurmurHash3
    {
        public static ulong Hash(string data)
        {
            if (string.IsNullOrEmpty(data))
            {
                throw new ArgumentNullException(nameof(data), "Input string cannot be null or empty.");
            }

            byte[] bytes = Encoding.UTF8.GetBytes(data);
            var byteSpan = new ReadOnlySpan<byte>(bytes);
            var (low, _) = ComputeHash128(byteSpan);
            return low;
        }

        public static (ulong low, ulong high) ComputeHash128(in ReadOnlySpan<byte> data, uint seed = 0)
        {
            const ulong c1 = 0x87c37b91114253d5L;
            const ulong c2 = 0x4cf5ad432745937fL;

            var length = data.Length;
            var h1 = (ulong)seed;
            var h2 = (ulong)seed;

            //----------
            // body
            ulong k1 = 0;
            ulong k2 = 0;

            var uintData = MemoryMarshal.Cast<byte, ulong>(data);

            var numBlocks = length >> 4;// length / (sizeof(uint) * 2);
            var index = 0;

            while (numBlocks-- > 0)
            {
                k1 = uintData[index++];
                k2 = uintData[index++];

                k1 *= c1; k1 = RotL64(k1, 31); k1 *= c2; h1 ^= k1;
                h1 = RotL64(h1, 27); h1 += h2; h1 = (h1 * 5) + 0x52dce729L;

                k2 *= c2; k2 = RotL64(k2, 33); k2 *= c1; h2 ^= k2;
                h2 = RotL64(h2, 31); h2 += h1; h2 = (h2 * 5) + 0x38495ab5L;
            }

            //----------
            // tail
            k1 = 0;
            k2 = 0;
            var remainder = data.Slice(length & (~15));

            switch (remainder.Length)
            {
                case 15: k2 ^= (ulong)remainder[14] << 48; goto case 14;
                case 14: k2 ^= (ulong)remainder[13] << 40; goto case 13;
                case 13: k2 ^= (ulong)remainder[12] << 32; goto case 12;
                case 12: k2 ^= (ulong)remainder[11] << 24; goto case 11;
                case 11: k2 ^= (ulong)remainder[10] << 16; goto case 10;
                case 10: k2 ^= (ulong)remainder[09] << 08; goto case 9;
                case 9:
                    k2 ^= remainder[8];
                    k2 *= c2; k2 = RotL64(k2, 33); k2 *= c1; h2 ^= k2;
                    goto case 8;
                case 8: k1 ^= (ulong)remainder[7] << 56; goto case 7;
                case 7: k1 ^= (ulong)remainder[6] << 48; goto case 6;
                case 6: k1 ^= (ulong)remainder[5] << 40; goto case 5;
                case 5: k1 ^= (ulong)remainder[4] << 32; goto case 4;
                case 4: k1 ^= (ulong)remainder[3] << 24; goto case 3;
                case 3: k1 ^= (ulong)remainder[2] << 16; goto case 2;
                case 2: k1 ^= (ulong)remainder[1] << 08; goto case 1;
                case 1:
                    k1 ^= remainder[0];
                    k1 *= c1; k1 = RotL64(k1, 31); k1 *= c2; h1 ^= k1;
                    break;
            }

            //----------
            // finalization

            h1 ^= (ulong)length; h2 ^= (ulong)length;

            h1 += h2;
            h2 += h1;

            h1 = FMix64(h1);
            h2 = FMix64(h2);

            h1 += h2;
            h2 += h1;

            return (h2, h1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong RotL64(ulong x, int r) => (x << r) | (x >> (64 - r));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong FMix64(ulong k)
        {
            k ^= k >> 33;
            k *= 0xff51afd7ed558ccd;
            k ^= k >> 33;
            k *= 0xc4ceb9fe1a85ec53;
            k ^= k >> 33;

            return k;
        }
    }
}

#region [ License information          ]

/*

Copyright (c) Attila Kiskó, enyim.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

#endregion
