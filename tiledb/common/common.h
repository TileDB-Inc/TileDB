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
// using tiledb::common::make_unique;

#endif  // TILEDB_COMMON_COMMON_H
