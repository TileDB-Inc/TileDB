/**
 * @file mgc_dict.h
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
 */

#include "magic.h"

#include "tiledb/common/common.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

using tiledb::common::Status;

class magic_dict {
 public:
  /** Default constructor */
  magic_dict();

  /**
   * Have libmagic load data from our embedded version.
   *
   * @param magic - libmagic object obtained from magic_open()
   * @return the value libmgaic returns from magic_load_buffers().
   */
  static int magic_mgc_embedded_load(magic_t magic);

  /**
   * Provides access to the internally expanded data.
   *
   * @return a shared pointer to the internal ByteVecValue holding
   * the expanded data.
   */
  static const shared_ptr<tiledb::sm::ByteVecValue> expanded_buffer();

 private:
  /**
   * decompress and init data items
   */
  static void prepare_data();

  /**
   * raw pointer to expanded bytes for use with magic_load_buffers(),
   * points to the data held within expanded_buffer_
   */
  static void* uncompressed_magic_dict_;

  /** holds the expanded data until application exits. */
  static shared_ptr<tiledb::sm::ByteVecValue> expanded_buffer_;
};

}  // namespace sm
}  // namespace tiledb
