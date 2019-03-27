/**
 * @file   filter_serializer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file declares class FilterSerializer.
 */

#ifndef TILEDB_FILTER_SERIALIZER_H
#define TILEDB_FILTER_SERIALIZER_H

#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/misc/status.h"

#ifdef TILEDB_SERIALIZATION
#include "capnp/serialize.h"
#include "tiledb/rest/capnp/tiledb-rest.capnp.h"
#endif

namespace tiledb {
namespace sm {

/**
 * A serialization class for Filters that is almost-agnostic of the underlying
 * serialization library.
 */
class FilterSerializer {
 public:
  FilterSerializer();

#ifdef TILEDB_SERIALIZATION
  Status init(rest::capnp::Filter::Builder* builder);
  Status init(const rest::capnp::Filter::Reader* reader);
#else
  Status init(const void* arg);
#endif

  Status get_type(FilterType* type) const;
  Status get_option(FilterOption option, void* value) const;

  Status set_type(FilterType type);
  Status set_option(FilterOption option, const void* value);

 private:
#ifdef TILEDB_SERIALIZATION
  /** The underlying CapnProto serializer. */
  rest::capnp::Filter::Builder* capnp_builder_;

  /** The underlying CapnProto reader. */
  const rest::capnp::Filter::Reader* capnp_reader_;
#endif
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_SERIALIZER_H
