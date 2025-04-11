/**
 * @file   storage_backends.h
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
 * This file describes the storage backend parameters of the already-built
 * TileDB build configuration.
 */

#ifndef TILEDB_STORAGE_BACKENDS_H
#define TILEDB_STORAGE_BACKENDS_H

#include "external/include/nlohmann/json.hpp"

using json = nlohmann::json;

namespace tiledb::as_built::parameters::storage_backends {

/* ********************************* */
/*              ATTRIBUTES           */
/* ********************************* */
#ifdef HAVE_AZURE
static constexpr bool azure = true;
#else
static constexpr bool azure = false;
#endif  // HAVE_AZURE

#ifdef HAVE_GCS
static constexpr bool gcs = true;
#else
static constexpr bool gcs = false;
#endif  // HAVE_GCS

static constexpr bool hdfs = false;

#ifdef HAVE_S3
static constexpr bool s3 = true;
#else
static constexpr bool s3 = false;
#endif  // HAVE_S3

/* ********************************* */
/*                API                */
/* ********************************* */
class storage_backends {};

void to_json(json& j, const storage_backends&) {
  j = {
      {"azure", {{"enabled", azure}}},
      {"gcs", {{"enabled", gcs}}},
      {"hdfs", {{"enabled", hdfs}}},
      {"s3", {{"enabled", s3}}}};
}

}  // namespace tiledb::as_built::parameters::storage_backends
#endif  // TILEDB_STORAGE_BACKENDS_H
