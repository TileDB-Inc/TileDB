/*
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#ifdef TILEDB_FILTER_TYPE_ENUM
/** No-op filter */
TILEDB_FILTER_TYPE_ENUM(FILTER_NONE) = 0,
    /** Gzip compressor */
    TILEDB_FILTER_TYPE_ENUM(FILTER_GZIP) = 1,
    /** Zstandard compressor */
    TILEDB_FILTER_TYPE_ENUM(FILTER_ZSTD) = 2,
    /** LZ4 compressor */
    TILEDB_FILTER_TYPE_ENUM(FILTER_LZ4) = 3,
    /** Run-length encoding compressor */
    TILEDB_FILTER_TYPE_ENUM(FILTER_RLE) = 4,
    /** Bzip2 compressor */
    TILEDB_FILTER_TYPE_ENUM(FILTER_BZIP2) = 5,
    /** Double-delta compressor */
    TILEDB_FILTER_TYPE_ENUM(FILTER_DOUBLE_DELTA) = 6,
    /** Bit width reduction filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_BIT_WIDTH_REDUCTION) = 7,
    /** Bitshuffle filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_BITSHUFFLE) = 8,
    /** Byteshuffle filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_BYTESHUFFLE) = 9,
    /** Positive-delta encoding filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_POSITIVE_DELTA) = 10,
    /** MD5 checksum filter. Starts at 12 because 11 is used for encryption, see
       tiledb/sm/enums/filter_type.h */
    TILEDB_FILTER_TYPE_ENUM(FILTER_CHECKSUM_MD5) = 12,
    /** SHA256 checksum filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_CHECKSUM_SHA256) = 13,
    /** Dictionary encoding filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_DICTIONARY) = 14,
    /** Float scaling filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_SCALE_FLOAT) = 15,
    /** XOR filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_XOR) = 16,
    /** Bitsort Filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_BITSORT) = 17,
    /** WEBP filter. */
    TILEDB_FILTER_TYPE_ENUM(FILTER_WEBP) = 18,
#endif

#ifdef TILEDB_FILTER_OPTION_ENUM
    /** Compression level. Type: `int32_t`. */
    TILEDB_FILTER_OPTION_ENUM(COMPRESSION_LEVEL) = 0,
    /** Max window length for bit width reduction. Type: `uint32_t`. */
    TILEDB_FILTER_OPTION_ENUM(BIT_WIDTH_MAX_WINDOW) = 1,
    /** Max window length for positive-delta encoding. Type: `uint32_t`. */
    TILEDB_FILTER_OPTION_ENUM(POSITIVE_DELTA_MAX_WINDOW) = 2,
    /** Bit width for float-scaling filter. Type: uint64_t. */
    TILEDB_FILTER_OPTION_ENUM(SCALE_FLOAT_BYTEWIDTH) = 3,
    /** Scale factor for float-scaling filter. Type: float64. */
    TILEDB_FILTER_OPTION_ENUM(SCALE_FLOAT_FACTOR) = 4,
    /** Offset for float-scaling filter. Type: float64. */
    TILEDB_FILTER_OPTION_ENUM(SCALE_FLOAT_OFFSET) = 5,
    /** Quality for webp compression. Ranges from 0.0 to 100.0 Type: float32. */
    TILEDB_FILTER_OPTION_ENUM(WEBP_QUALITY) = 6,
    /** Set input format to for webp Type: uint8_t */
    TILEDB_FILTER_OPTION_ENUM(WEBP_INPUT_FORMAT) = 7,
    /** Enable lossless WebP compression Type: uint8_t */
    TILEDB_FILTER_OPTION_ENUM(WEBP_LOSSLESS) = 8,
#endif

#ifdef TILEDB_FILTER_WEBP_FORMAT
    TILEDB_FILTER_WEBP_FORMAT(WEBP_NONE) = 0,
    TILEDB_FILTER_WEBP_FORMAT(WEBP_RGB) = 1,
    TILEDB_FILTER_WEBP_FORMAT(WEBP_BGR) = 2,
    TILEDB_FILTER_WEBP_FORMAT(WEBP_RGBA) = 3,
    TILEDB_FILTER_WEBP_FORMAT(WEBP_BGRA) = 4,
#endif
