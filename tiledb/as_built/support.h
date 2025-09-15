/**
 * @file   support.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file describes the support parameters of the already-built TileDB
 * build configuration.
 */

#ifndef TILEDB_SUPPORT_H
#define TILEDB_SUPPORT_H

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace tiledb::as_built::parameters::support {

/* ********************************* */
/*              ATTRIBUTES           */
/* ********************************* */
#ifdef TILEDB_SERIALIZATION
static constexpr bool serialization = true;
#else
static constexpr bool serialization = false;
#endif  // TILEDB_SERIALIZATION

/* ********************************* */
/*                API                */
/* ********************************* */
class support {};

void to_json(json& j, const support&) {
  j = {{"serialization", {{"enabled", serialization}}}};
}

}  // namespace tiledb::as_built::parameters::support
#endif  // TILEDB_SUPPORT_H
