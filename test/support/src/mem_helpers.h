/**
 * @file mem_helpers.h
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
 * This file declares some test suite helper functions specific to memory
 * tracking.
 */

#ifndef TILEDB_MEM_HELPERS_H
#define TILEDB_MEM_HELPERS_H

#include "tiledb/common/memory_tracker.h"

namespace tiledb::test {

/**
 * Helper function get the test instance of a shared_ptr<MemoryTracker>
 *
 * This is the preferred function. The create_test_memory_tracker will be
 * replaced shortly and only serves as a proxy to this function while we
 * transition the first few PRs to use this new function.
 *
 * The reasoning here is that creating memory trackers has turned out to be a
 * bit of a footgun with lifetime issues.
 */
shared_ptr<sm::MemoryTracker> get_test_memory_tracker();

/**
 * Helper function to create test instances of shared_ptr<MemoryTracker>
 */
shared_ptr<sm::MemoryTracker> create_test_memory_tracker();

}  // namespace tiledb::test

#endif  //  TILEDB_MEM_HELPERS_H
