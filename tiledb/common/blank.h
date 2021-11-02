/**
 * @file blank.h
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
 */

#ifndef TILEDB_COMMON_BLANK_H
#define TILEDB_COMMON_BLANK_H

namespace tiledb::common {

/**
 * `blank` is a marker class to highlight calls to constructors that are slated
 * for removal. Without specialization, `blank<T>` is not constructable. Each
 * specialization provides access to a specific constructor is not visible from
 * the class itself.
 *
 * `blank` is intended as a transition mechanism. For example, an initial use of
 * blank might hide an existing constructor as private and only make it
 * available through a blank. This does not prevent the use of the old
 * constructor in new code, but it does make the use blindingly obvious.
 */
template <class T>
struct blank {
  /**
   * All constructors are deleted.
   *
   * @tparam Args Any argument list.
   */
  template <typename... Args>
  explicit blank(Args&...) = delete;
};

}  // namespace tiledb::common

#endif  // TILEDB_COMMON_BLANK_H
