// TBD: probably better name needed?

/**
 * @file   tiledb_tile_sizes.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file contains the definitions for array tile size operations.
 */

#ifndef TILEDB_C_TILE_SIZES_H
#define TILEDB_C_TILE_SIZES_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tiledb_fragment_max_tile_sizes_t {
  uint64_t max_in_memory_tile_size;
  uint64_t max_persisted_tile_size;

  uint64_t max_persisted_basic_tile_size;
  uint64_t max_in_memory_basic_tile_size;
  uint64_t max_persisted_tile_size_var;
  uint64_t max_in_memory_tile_size_var;
  // in_memory_validity_size doesn't currently appear to have method to
  // obtain... is persisted/in_memory the same?
  uint64_t max_persisted_tile_size_validity;
  uint64_t max_in_memory_tile_size_validity;

} tiledb_fragment_max_tile_sizes_t;

#ifdef __cplusplus
}
#endif

#endif
