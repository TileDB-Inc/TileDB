/**
 * @file   tiledb/common/common.h
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
 * Common facilities of the TileDB library.
 *
 * The common facilities here are common by policy, not by convenience. The
 * elements here are expected to be used as ordinary language features. Each
 * element originates in some included file and then is incorporated into the
 * default namespace with a `using` declaration. The expectation is that common
 * elements be used *without* explicit namespace qualification.
 */

#ifndef TILEDB_COMMON_COMMON_H
#define TILEDB_COMMON_COMMON_H

#include <limits>

/*
 * All the standard library commonality is declared in "common-std.h".
 */
#include "common-std.h"

namespace tiledb::common {}
namespace tdb = tiledb::common;

/*
 * Dynamic memory
 */
#include "dynamic_memory/dynamic_memory.h"
using std::shared_ptr;
using tiledb::common::allocator;
using tiledb::common::make_shared;

/*
 * Exception
 *
 * The exception header also put `class Status` in scope.
 */
#include "exception/exception.h"
using tiledb::common::Status;
using tiledb::common::StatusException;
using tiledb::common::throw_if_not_ok;

/*
 * Experimental build
 */
#ifdef TILEDB_EXPERIMENTAL_FEATURES
constexpr bool is_experimental_build = true;
#else
constexpr bool is_experimental_build = false;
#endif  // TILEDB_EXPERIMENTAL_FEATURES

/*
 * Platform/machine config
 */
#include "platform.h"

#if 0
typedef struct tiledb_fragment_tile_size_extremes {
    uint64_t max_persisted_tile_size = 0, max_in_memory_tile_size = 0;
    uint64_t max_persisted_tile_size_var = 0, max_in_memory_tile_size_var = 0;
    // in_memory_validity_size doesn't currently appear to have method to obtain...
    // is persisted/in_memory the same?
    uint64_t max_persisted_tile_size_validity = 0, max_in_memory_tile_size_validity = 0;

#if _WIN32
#pragma push_macro("max")
#undef max
#endif
    uint64_t min_persisted_tile_size = std::numeric_limits<uint64_t>::max(),
             min_in_memory_tile_size = std::numeric_limits<uint64_t>::max();
    uint64_t min_persisted_tile_size_var = std::numeric_limits<uint64_t>::max(),
             min_in_memory_tile_size_var = std::numeric_limits<uint64_t>::max();
    uint64_t min_persisted_tile_size_validity = std::numeric_limits<uint64_t>::max(),
             min_in_memory_tile_size_validity = std::numeric_limits<uint64_t>::max();
#if _WIN32
#pragma pop_macro("max")
#endif
} tiledb_fragment_tile_size_extremes_t;
#endif

#endif  // TILEDB_COMMON_COMMON_H
