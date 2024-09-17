/**
 * @file tiledb/api/c_api/array_schema/array_schema_evolution_api_experimental.h
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
 * This file declares the array schema evolution section of the experimental
 * TileDB C API.
 */

#ifndef TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_EXPERIMENTAL_H
#define TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_EXPERIMENTAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB array schema evolution */
typedef struct tiledb_array_schema_evolution_t tiledb_array_schema_evolution_t;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_EXPERIMENTAL_H
