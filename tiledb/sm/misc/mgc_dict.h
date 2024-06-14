/**
 * @file mgc_dict.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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

#include "magic.h"

#include "tiledb/common/common.h"

namespace tiledb::sm::magic_dict {

/**
 * Have libmagic load data from our embedded version.
 *
 * @param magic - libmagic object obtained from magic_open()
 * @return the value libmgaic returns from magic_load_buffers().
 */
int magic_mgc_embedded_load(magic_t magic);

/**
 * Provides access to the internally expanded data.
 *
 * Data is stored in the library in a compressed form (approx. 270KB) and gets
 * decompressed (approx. 7MB) on the first call to this function. Subsequent
 * calls to this function will reuse the decompressed buffer.
 *
 * @return a span to the internal buffer holding the expanded data.
 */
span<const uint8_t> expanded_buffer();

}  // namespace tiledb::sm::magic_dict
