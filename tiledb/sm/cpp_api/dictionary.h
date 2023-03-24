/**
 * @file dictionary.h
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
 * This file declares the C++ API for the TileDB Dictionary object.
 */

#ifndef TILEDB_CPP_API_DICTIONARY_H
#define TILEDB_CPP_API_DICTIONARY_H

#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "object.h"
#include "tiledb.h"
#include "type.h"

#include <functional>
#include <memory>
#include <sstream>

namespace tiledb {


class Dictionary {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Dimension(const Context& ctx, tiledb_dictionary_t* dict)
      : ctx_(ctx) {
    dict_ = std::shared_ptr<tiledb_dictionary_t>(dict, deleter_);
  }

  Dictionary(const Dimension&) = default;
  Dictionary(Dimension&&) = default;
  Dictionary& operator=(const Dimension&) = default;
  Dictionary& operator=(Dimension&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */


  get/set celL_val_num
  get/set nullable
  get/set ordered
  get/set data buffer
  get/set offsets buffer
  get/set validity buffer

  /** Returns a shared pointer to the C TileDB dimension object. */
  std::shared_ptr<tiledb_dictionary_t> ptr() const {
    return dict_;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  template<T> when is_integral(T)
  create(std::vector<T>, nullable = false, ordered = false);

  template<T>
  create(Datatype type, std::vector<T>, nullable = false, ordered = false);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** A deleter wrapper. */
  impl::Deleter deleter_;

  /** The C TileDB dictionary object. */
  std::shared_ptr<tiledb_dictionary_t> dict_;

  /* ********************************* */
  /*     PRIVATE STATIC FUNCTIONS      */
  /* ********************************* */

  /**
   * Creates a dimension with the input name, datatype, domain and tile
   * extent.
   */
  static Dimension create_impl(
      const Context& ctx,
      const std::string& name,
      tiledb_datatype_t type,
      const void* domain,
      const void* tile_extent) {
    tiledb_dimension_t* d;
    ctx.handle_error(tiledb_dimension_alloc(
        ctx.ptr().get(), name.c_str(), type, domain, tile_extent, &d));
    return Dimension(ctx, d);
  }
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of a dimension for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Dimension& dim) {
  os << "Dim<" << dim.name() << "," << dim.domain_to_str() << ","
     << dim.tile_extent_to_str() << ">";
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_DIMENSION_H
