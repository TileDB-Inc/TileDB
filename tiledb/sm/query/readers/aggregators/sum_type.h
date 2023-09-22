/**
 * @file   sum_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines sum types in relation to basic types.
 *
 * TODO: This needs to be improved to remove macros (sc-33764).
 */

#ifndef TILEDB_SUM_TYPE_H
#define TILEDB_SUM_TYPE_H

namespace tiledb::sm {

#define SUM_TYPE_DATA(T, SUM_T) \
  template <>                   \
  struct sum_type_data<T> {     \
    using type = T;             \
    typedef SUM_T sum_type;     \
  };

/** Convert basic type to a sum type. **/
template <typename T>
struct sum_type_data;

SUM_TYPE_DATA(int8_t, int64_t);
SUM_TYPE_DATA(uint8_t, uint64_t);
SUM_TYPE_DATA(int16_t, int64_t);
SUM_TYPE_DATA(uint16_t, uint64_t);
SUM_TYPE_DATA(int32_t, int64_t);
SUM_TYPE_DATA(uint32_t, uint64_t);
SUM_TYPE_DATA(int64_t, int64_t);
SUM_TYPE_DATA(uint64_t, uint64_t);
SUM_TYPE_DATA(float, double);
SUM_TYPE_DATA(double, double);

}  // namespace tiledb::sm

#endif  // TILEDB_SUM_TYPE_H
