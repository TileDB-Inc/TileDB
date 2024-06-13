/**
 * @file current_domain_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This defines the TileDB CurrentDomainType enum that maps to
 * tiledb_current_domain_type_t C-API enum.
 */

#ifndef TILEDB_CURRENT_DOMAIN_TYPE_H
#define TILEDB_CURRENT_DOMAIN_TYPE_H

#include <cassert>
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** A current domain type. */
enum class CurrentDomainType : uint8_t {
#define TILEDB_CURRENT_DOMAIN_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_CURRENT_DOMAIN_TYPE_ENUM
};

/** Returns the string representation of the input current domain type. */
inline const std::string& current_domain_type_str(
    CurrentDomainType current_domain_type) {
  switch (current_domain_type) {
    case CurrentDomainType::NDRECTANGLE:
      return constants::current_domain_ndrectangle_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the current domain type given a string representation. */
inline Status current_domain_type_enum(
    const std::string& current_domain_type_str,
    CurrentDomainType* current_domain_type) {
  if (current_domain_type_str == constants::current_domain_ndrectangle_str)
    *current_domain_type = CurrentDomainType::NDRECTANGLE;
  else {
    return Status_Error(
        "Invalid CurrentDomain type " + current_domain_type_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CURRENT_DOMAIN_TYPE_H
