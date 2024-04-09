/**
 * @file gzip_wrappers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 *
 * @section DESCRIPTION
 *
 * This file declare gzip_compress and gzip_uncompress, routines
 * that slightly simplify use of the gzip compression routines.
 */

#ifndef TILEDB_GZIP_WRAPPER_H
#define TILEDB_GZIP_WRAPPER_H

#include "../gzip_compressor.h"

#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/misc/types.h"

/**
 * gzip compress data from in_buf into a simple format in out_gzipped_buf
 * that can be passed to gzip_uncompress to retrieve the uncompressed data.
 *
 * @param out_gzipped_buf The returned buffer with the compressed data.
 * @param in_buf The input buffer containing data to be compressed
 */
void gzip_compress(
    shared_ptr<tiledb::sm::Buffer>& out_gzipped_buf,
    const void* in_bytes,
    uint64_t nbytes);

/**
 * gzip decompress data from inbuf into linear series of bytesw in outbuf
 * that can be passed to gzip_uncompress to retrieve the uncompressed data.
 *
 * @param out_buf The input buffer containing data to be compressed
 * @param comp_buf The buffer with the compressed data to be decompressed
 */
void gzip_decompress(
    shared_ptr<tiledb::sm::ByteVecValue>& out_buf, const uint8_t* comp_buf);

#endif  // TILEDB_GZIP_WRAPPER_H
