/**
 * @file   unit-uuid.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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

#include <catch.hpp>
#include <set>
#include <thread>
#include <vector>

#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/uuid.h"

using namespace tiledb::sm;

TEST_CASE("UUID: Test generate", "[uuid]") {
  // Initialize global OpenSSL state if required.
  REQUIRE(global_state::GlobalState::GetGlobalState().initialize(nullptr).ok());

  SECTION("- Serial") {
    std::string uuid0, uuid1, uuid2;
    REQUIRE(uuid::generate_uuid(&uuid0).ok());
    REQUIRE(uuid0.length() == 36);
    REQUIRE(uuid::generate_uuid(&uuid1).ok());
    REQUIRE(uuid1.length() == 36);
    REQUIRE(uuid0 != uuid1);

    REQUIRE(uuid::generate_uuid(&uuid2, false).ok());
    REQUIRE(uuid2.length() == 32);
  }

  SECTION("- Threaded") {
    const unsigned nthreads = 20;
    std::vector<std::string> uuids(nthreads);
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < nthreads; i++) {
      threads.emplace_back([&uuids, i]() {
        std::string& uuid = uuids[i];
        REQUIRE(uuid::generate_uuid(&uuid).ok());
        REQUIRE(uuid.length() == 36);
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