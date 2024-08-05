/**
 * @file   tiledb/common/evaluator/test/unit_evaluator.cc
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
 */

#include "test/support/tdb_catch.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/evaluator/evaluator.h"

using namespace tiledb::common;

TEST_CASE("constructor", "[evaluator]") {
  auto f = [](std::string key) {
    return std::make_tuple(key.size(), std::make_shared<std::string>(key));
  };

  Evaluator<MaxEntriesCache<std::string, std::string, 3>, decltype(f)>
      max_entries_eval;
  max_entries_eval(f, "key");   // miss
  max_entries_eval(f, "key");   // hit
  max_entries_eval(f, "key2");  // miss
  max_entries_eval(f, "key3");  // miss
  max_entries_eval(f, "key4");  // evict

  Evaluator<NoCachePolicy<std::string, std::string>, decltype(f)> no_cache_eval;
  no_cache_eval(f, "key");  // miss
  no_cache_eval(f, "key");  // miss

  Evaluator<
      MemoryBudgetedCache<std::string, std::string, 1024 * 1024>,
      decltype(f)>
      memory_budgeted_cache;
  memory_budgeted_cache(f, "key");  // miss
}
