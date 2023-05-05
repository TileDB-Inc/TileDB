/**
 * @file   unit_uuid.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * Tests the UUID utility functions.
 */

#include <test/support/tdb_catch.h>
#include <set>
#include <thread>
#include <vector>

#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/uuid.h"

using namespace tiledb::sm;

std::mutex catch2_macro_mutex;

// A thread-safe variant of the REQUIRE macro.
#define REQUIRE_SAFE(a)                                   \
  {                                                       \
    std::lock_guard<std::mutex> lock(catch2_macro_mutex); \
    REQUIRE(a);                                           \
  }

// A thread-safe variant of the REQUIRE_NOTHROW macro.
#define REQUIRE_NOTHROW_SAFE(a)                                   \
  {                                                       \
    std::lock_guard<std::mutex> lock(catch2_macro_mutex); \
    REQUIRE_NOTHROW(a);                                           \
  }

void cancel_all_tasks(StorageManager*) {
}

TEST_CASE("UUID: Test generate", "[uuid]") {
  SECTION("- Serial") {
    std::string uuid0, uuid1, uuid2;
    REQUIRE_NOTHROW(uuid0 = uuid::generate_uuid());
    REQUIRE(uuid0.length() == 36);
    REQUIRE_NOTHROW(uuid1 = uuid::generate_uuid());
    REQUIRE(uuid1.length() == 36);
    REQUIRE(uuid0 != uuid1);

    REQUIRE_NOTHROW(uuid2 = uuid::generate_uuid(false));
    REQUIRE(uuid2.length() == 32);
  }

  SECTION("- Threaded") {
    const unsigned nthreads = 20;
    std::vector<std::string> uuids(nthreads);
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < nthreads; i++) {
      threads.emplace_back([&uuids, i]() {
        std::string& uuid = uuids[i];
        REQUIRE_NOTHROW_SAFE(uuid = uuid::generate_uuid());
        REQUIRE_SAFE(uuid.length() == 36);
      });
    }
    for (auto& t : threads) {
      t.join();
    }
    // Check uniqueness
    std::set<std::string> uuid_set;
    uuid_set.insert(uuids.begin(), uuids.end());
    REQUIRE(uuid_set.size() == uuids.size());
  }
}
