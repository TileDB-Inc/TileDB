/**
 * @file array_experimental.h
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
 * This file declares the experimental C++ API for arrays.
 */

#ifndef TILEDB_CPP_API_ARRAY_EXPERIMENTAL_H
#define TILEDB_CPP_API_ARRAY_EXPERIMENTAL_H

#include "array.h"
#include "enumeration_experimental.h"
#include "tiledb_experimental.h"

#include <optional>

namespace tiledb {
class ArrayExperimental {
 public:
  /**
   * Get the Enumeration from the attribute with name `attr_name`.
   *
   * @param ctx The context to use.
   * @param array The array containing the attribute.
   * @param attr_name The name of the attribute to get the enumeration from.
   * @return Enumeration The enumeration.
   */
  static Enumeration get_enumeration(
      const Context& ctx, const Array& array, const std::string& attr_name) {
    tiledb_enumeration_t* enmr;
    ctx.handle_error(tiledb_array_get_enumeration(
        ctx.ptr().get(), array.ptr().get(), attr_name.c_str(), &enmr));
    return Enumeration(ctx, enmr);
  }

  /**
   * Load all enumerations for the array.
   *
   * @param ctx The context to use.
   * @param array The array to load enumerations for.
   * @param all_schemas Whether or not to load enumerations on all schemas.
   */
  static void load_all_enumerations(
      const Context& ctx, const Array& array, bool all_schemas = false) {
    ctx.handle_error(tiledb_array_load_all_enumerations(
        ctx.ptr().get(), array.ptr().get(), all_schemas ? 1 : 0));
  }
};

}  // namespace tiledb

#endif
