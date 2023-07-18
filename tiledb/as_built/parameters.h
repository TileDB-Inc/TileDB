/**
 * @file   parameters.h
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
 * This file describes the parameters of the already-built TileDB
 * build configuration.
 */

#ifndef TILEDB_PARAMETERS_H
#define TILEDB_PARAMETERS_H

#include "tiledb/as_built/storage_backends.h"
#include "tiledb/as_built/support.h"

using json = nlohmann::json;

namespace tiledb::as_built::parameters {

/* ********************************* */
/*                API                */
/* ********************************* */
class parameters {};

void to_json(json& j, const parameters&) {
  j = {
      {"storage_backends", storage_backends::storage_backends()},
      {"support", support::support()}};
}

}  // namespace tiledb::as_built::parameters
#endif  // TILEDB_PARAMETERS_H
