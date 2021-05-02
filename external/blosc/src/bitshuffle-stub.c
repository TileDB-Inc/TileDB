/**
 * @file bitshuffle-stub.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Defines stubs for bitshuffle functions in blosc that we don't actually use,
 * but pointers to which are required to initialize an implementation structure
 * in shuffle.c
 */
#include "bitshuffle-generic.h"

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_trans_bit_elem_scal(const void* in, void* out, const size_t size,
                                         const size_t elem_size, void* tmp_buf)
{
  return 0;
}

BLOSC_NO_EXPORT int64_t
blosc_internal_bshuf_untrans_bit_elem_scal(const void* in, void* out, const size_t size,
                                         const size_t elem_size, void* tmp_buf)
{
  return 0;
}

