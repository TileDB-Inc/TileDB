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
 * Some tests for the Stats functionality.
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

static std::string empty_dumped_stats = { "[\n\n]\n" };
// Currently there are no stats that are always present.
// Can, however, envision a time when stats calls themselves might be
// accumulated, thus having a forever outstanding stats item, the empty
// version of which might be represented in this base_dumped_stats, rather
// than the output being totally 'empty'.
static std::string base_dumped_stats = empty_dumped_stats;

TEST_CASE("Stats inferred registration handling, direct method calls", "[stats]") {
  // Examine stats output as reflection of
  // whether data might be held beyond a reset.

  std::string dumped_stats;
  all_stats.dump(&dumped_stats);
  CHECK(dumped_stats == base_dumped_stats);

  {
    std::shared_ptr<Stats> stats { make_shared<Stats>(HERE(), "Test") };
    
    all_stats.register_stats(stats);

    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    stats->create_child("childstats");
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    stats->add_counter("testcounter", 1);
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats != base_dumped_stats);
  }  

  all_stats.dump(&dumped_stats);
  CHECK(dumped_stats == base_dumped_stats);
}

TEST_CASE("Stats registration handling, indirect via Context", "[stats][context]") {
  // Examine stats output when class Context active as reflection of
  // whether data might be held beyond a reset.
  
  std::string dumped_stats;
  all_stats.dump(&dumped_stats);
  CHECK(dumped_stats == base_dumped_stats);
  
  {
    // class Context registers a GlobalStats entity.
    tiledb::Context ctx;
    // Nothing has been done to generate stats, should still be base.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);
  }

  // should still be base after Context gone
  all_stats.dump(&dumped_stats);
  CHECK(dumped_stats == base_dumped_stats);

  // Similar to above, but this time exerciase another item that 
  // should populate some stats.
  {

    // class Context registers a GlobalStats entity.
    tiledb::Context ctx;
    // Nothing has been done to generate stats, should still be base.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    // Now set up for and performs stats generating action.

    // ...
    auto stats = ctx.ptr()->storage_manager()->stats();
    auto compute_tp = ctx.ptr()->storage_manager()->compute_tp();
    auto io_tp = ctx.ptr()->storage_manager()->io_tp();
    auto ctx_config = ctx.ptr()->storage_manager()->config();
    // construction invokes a stats->create_child()
    tiledb::sm::VFS vfs(stats, compute_tp, io_tp, ctx_config);
    
    // Stats still base.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    bool does_it_exist = false;
    std::string irrelevant_filename =
        "not.expected.to.exist.but.doesnt.matter.if.does";
    tiledb::sm::URI uri(irrelevant_filename);
    // .is_file() actually adds a counter to the stats.
    CHECK(vfs.is_file(uri, &does_it_exist).ok());
    // Stats should no longer be base.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats != base_dumped_stats);

  // Perform reset of any remaining stats (none in this test) and remove
    // previously registered stats for already destructed registrants.
    all_stats.reset();
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    // Populate it again, to be sure it's missing after we exit block and
    // original (Context ctx) registered stats were destroyed.
    CHECK(vfs.is_file(uri, &does_it_exist).ok());

    // check again that it's not at base level.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats != base_dumped_stats);
  }

  // Registered stats only knows about weak_ptr, original registered stats
  // is gone and output is now again base level.
  all_stats.dump(&dumped_stats);
  CHECK(dumped_stats == base_dumped_stats);

  // Perform reset of any remaining stats (none in this test) and remove
  // previously registered stats for already destructed registrants.
  all_stats.reset();

  // Stats should still be base level.
  all_stats.dump(&dumped_stats);
  CHECK(dumped_stats == base_dumped_stats);
  
}
