/**
 * @file   heap_memory.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Defines TileDB-variants of dynamic (heap) memory allocation routines. When
 * the global `heap_profiler` is enabled, these routines will record memory
 * stats. Should allocation fail, stats will print and exit the program.
 */

#ifndef TILEDB_HEAP_MEMORY_H
#define TILEDB_HEAP_MEMORY_H

#include <string>

namespace tiledb {
namespace common {

/** TileDB variant of `malloc`. */
void* tiledb_malloc(size_t size, const std::string& label);

/** TileDB variant of `calloc`. */
void* tiledb_calloc(size_t num, size_t size, const std::string& label);

/** TileDB variant of `realloc`. */
void* tiledb_realloc(void* p, size_t size, const std::string& label);

/** TileDB variant of `free`. */
void tiledb_free(void* p);

}  // namespace common
}  // namespace tiledb

#define TILEDB_HEAP_MEM_LABEL \
  std::string(__FILE__) + std::string(":") + std::to_string(__LINE__)

#define tdb_malloc(size) \
  tiledb::common::tiledb_malloc(size, TILEDB_HEAP_MEM_LABEL)

#define tdb_calloc(num, size) \
  tiledb::common::tiledb_calloc(num, size, TILEDB_HEAP_MEM_LABEL)

#define tdb_realloc(p, size) \
  tiledb::common::tiledb_realloc(p, size, TILEDB_HEAP_MEM_LABEL)

#define tdb_free(p) tiledb::common::tiledb_free(p)

#endif  // TILEDB_HEAP_MEMORY_H
