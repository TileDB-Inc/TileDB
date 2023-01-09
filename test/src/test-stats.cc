/**
 * @file   test-stats.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Home for tests focused on some aspect of stats functionality.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/context/context_api_internal.h"
#include <tiledb/common/dynamic_memory/dynamic_memory.h>
#include <tiledb/common/common.h>
#include <tiledb/sm/cpp_api/tiledb>
#include <tiledb/sm/filesystem/vfs.h>
#include <tiledb/sm/stats/global_stats.h>

#include <string>

using namespace tiledb::sm::stats;
// redefine the globally defined name normally used in effort to have 
// compiler alert if referenced in this module.
int all_stats = 0;
// tests here are to use a local definition of GlobalStats rather than the global one.
static tiledb::sm::stats::GlobalStats pseudo_all_stats;

static std::string empty_dumped_stats = { "[\n\n]\n" };
// Currently there are no stats that are always present.
// Can, however, envision a time when something (stats calls themselves?) might be
// accumulated, thus having a forever outstanding stats item, the base
// version of which might be represented in this base_dumped_stats, rather
// than the output being totally 'empty'.
static std::string base_dumped_stats = empty_dumped_stats;

TEST_CASE("Stats registration handling via direct method calls", "[stats]") {
  // Examine stats output as a reflection of whether allocations might be held beyond a destruction.

  std::string dumped_stats;

  SECTION(" - baseline of no stats") {
    // If this fails, look for something else in overall test program that may
    // have generated stats in a fashion that they were not cleaned up, most
    // likely something creating and keeping a Context active outside of these
    // tests.
    pseudo_all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);
  }

  SECTION(" - create local stats, verify that its released") {
    {
      std::shared_ptr<Stats> stats{make_shared<Stats>(HERE(), "Test")};

      pseudo_all_stats.register_stats(stats);

      pseudo_all_stats.dump(&dumped_stats);
      CHECK(dumped_stats == base_dumped_stats);

      stats->create_child("childstats");
      pseudo_all_stats.dump(&dumped_stats);
      CHECK(dumped_stats == base_dumped_stats);

      stats->add_counter("testcounter", 1);
      pseudo_all_stats.dump(&dumped_stats);
      CHECK(dumped_stats != base_dumped_stats);

      // 'stats' destroyed, should no longer be active as registered item.
    }

    pseudo_all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);
  }
}

TEST_CASE("Stats registration handling, indirect via Context", "[stats][context]") {
  // Examine stats output when class Context active as reflection of
  // whether data allocations might be held beyond a reset and/or registrant destruction.
  
  std::string dumped_stats;

  SECTION(" - baseline of no stats") {
    pseudo_all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);
  
    // Nothing has been done to generate stats, should still be base.
    pseudo_all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);
  }

  // Similar to above, but this do something that 
  // should populate some stats.
  SECTION(" - verify stats generated and then released") {
    // Nothing has been done to generate stats, should still be base.
    pseudo_all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    {
      // Now set up for and performs stats generating actions.

      std::shared_ptr<Stats> stats = make_shared<Stats>(HERE(), "test_stats");
      Stats* stats_{stats->create_child("TestStats")};
      pseudo_all_stats.register_stats(stats);

      // Stats still base.
      pseudo_all_stats.dump(&dumped_stats);
      CHECK(dumped_stats == base_dumped_stats);

      stats_->add_counter("file_size_num", 1);
      stats_->add_counter("is_object_num", 1);

      // Stats should no longer be base.
      pseudo_all_stats.dump(&dumped_stats);
      CHECK(dumped_stats != base_dumped_stats);

      // Perform reset of any remaining stats and remove any
      // previously registered stats for already destructed registrants.
      pseudo_all_stats.reset();
      pseudo_all_stats.dump(&dumped_stats);
      CHECK(dumped_stats == base_dumped_stats);

      // Populate it again, to be sure it's missing after we exit block and
      // original (Context ctx) registered stats were destroyed.
      stats_->add_counter("is_object_num", 1);
      stats_->add_counter("file_size_num", 1);

      // check again that it's not at base level.
      pseudo_all_stats.dump(&dumped_stats);
      CHECK(dumped_stats != base_dumped_stats);

      // 'stats' destroyed, should no longer be active as registered item.
    }

    // Registered stats only knows about weak_ptr, original registered stats
    // should now be gone and output again at base level.
    pseudo_all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

  // Perform reset of any remaining stats (none in this test) and remove
  // previously registered stats for already destructed registrants.
  pseudo_all_stats.reset();

  // Stats should still be base level.
  pseudo_all_stats.dump(&dumped_stats);
  CHECK(dumped_stats == base_dumped_stats);
  }
  
}
