/**
 * @file attribute_experimental.h
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
 * This file declares the experimental C++ API for attributes.
 */

#ifndef TILEDB_CPP_API_ATTRIBUTE_EXPERIMENTAL_H
#define TILEDB_CPP_API_ATTRIBUTE_EXPERIMENTAL_H

#include "attribute.h"
#include "context.h"
#include "enumeration_experimental.h"
#include "tiledb_experimental.h"

#include <optional>

namespace tiledb {
class AttributeExperimental {
 public:
  /**
   * Set the enumeration name for an attribute.
   *
   * @param ctx TileDB context.
   * @param attribute Target attribute.
   * @param enumeration_name The name of the enumeration to set.
   */
  static void set_enumeration_name(
      const Context& ctx,
      Attribute& attribute,
      const std::string& enumeration_name) {
    ctx.handle_error(tiledb_attribute_set_enumeration_name(
        ctx.ptr().get(), attribute.ptr().get(), enumeration_name.c_str()));
  }

  /**
   * Get the attribute's enumeration name.
   *
   * @param ctx TileDB context.
   * @param attribute Target attribute.
   * @return std::optional<std::string> The enumeration name if one exists.
   */
  static std::optional<std::string> get_enumeration_name(
      const Context& ctx, const Attribute& attribute) {
    // Get the enumeration name as a string handle
    tiledb_string_t* enmr_name;
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_attribute_get_enumeration_name(
        c_ctx, attribute.ptr().get(), &enmr_name));

    if (enmr_name == nullptr) {
      return std::nullopt;
    }

    // Convert string handle to a std::string
    const char* name_ptr;
    size_t name_len;
    ctx.handle_error(tiledb_string_view(enmr_name, &name_ptr, &name_len));
    std::string ret(name_ptr, name_len);

    // Release the string handle
    ctx.handle_error(tiledb_string_free(&enmr_name));

    return ret;
  }
};

}  // namespace tiledb

#endif
