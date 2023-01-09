#include <iostream>
#include "tiledb/api/c_api/context/context_api_internal.h"
#include <tiledb/sm/stats/global_stats.h>
#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

using namespace tiledb::sm::stats;

static std::string empty_dumped_stats = { "[\n\n]\n" };
// Currently there are no stats that are always present.
// Can, however, envision a time when something (stats calls themselves?) might be
// accumulated, thus having a forever outstanding stats item, the base
// version of which might be represented in this base_dumped_stats, rather
// than the output being totally 'empty'.
static std::string base_dumped_stats = empty_dumped_stats;

// Using C_API routines, exercise a routine registering stats
// to exercise the paths (before bug fix) causing the leakage
// problem even when involved Context(s) were destructed.
TEST_CASE("Stats registration handling, indirect via Context", "[stats][context]") {
  // Examine stats output when class Context active as reflection of
  // whether data allocations might be held beyond a reset and/or registrant destruction.
  
  auto check_stats_is = [](std::string s) -> void {
    char *ptr_dumped_stats{nullptr};
    tiledb_stats_raw_dump_str(&ptr_dumped_stats); 
    CHECK(std::string(ptr_dumped_stats) == s);
    tiledb_stats_free_str(&ptr_dumped_stats);
  };
  check_stats_is(base_dumped_stats);

  auto check_stats_is_not = [](std::string s) -> void {
    char *ptr_dumped_stats{nullptr};
    tiledb_stats_raw_dump_str(&ptr_dumped_stats); 
    CHECK(std::string(ptr_dumped_stats) != s);
    tiledb_stats_free_str(&ptr_dumped_stats);
  };
  
  {
    // class Context registers a GlobalStats entity.
    tiledb::Context ctx;
    // Nothing has been done to generate stats, should still be base.
    check_stats_is(base_dumped_stats);
  }

  // should still be base after Context gone
  check_stats_is(base_dumped_stats);

  // Similar to above, but this time exercise another item that 
  // should populate some stats.
  {

    // class Context registers a GlobalStats entity.
    tiledb::Context ctx;
    // Nothing has been done to generate stats, should still be base.
    check_stats_is(base_dumped_stats);

    // Now set up for and performs stats generating action.

    tiledb::VFS vfs(ctx);
    
    // Stats still base.
    check_stats_is(base_dumped_stats);

    bool does_it_exist = false;
    std::string irrelevant_filename =
        "not.expected.to.exist.but.doesnt.matter.if.does";
    does_it_exist = vfs.is_file(irrelevant_filename);
    // Stats should no longer be base.
    check_stats_is_not(base_dumped_stats);

    // Perform reset of any remaining stats and remove any
    // previously registered stats for already destructed registrants.
    tiledb_stats_reset();
    check_stats_is(base_dumped_stats);

    // Populate it again, to be sure it's missing after we exit block and
    // original (Context ctx) registered stats were destroyed.
    does_it_exist = vfs.is_file(irrelevant_filename);

    // check again that it's not at base level.
    check_stats_is_not(base_dumped_stats);
    
    //'ctx' is destructed at end of block and registered stats should be released.
  }

  // Registered stats only knows about weak_ptr, original registered stats
  // is gone and output is now again base level.
  check_stats_is(base_dumped_stats);

  // Perform reset of any remaining stats (none in this test) and remove
  // previously registered stats for already destructed registrants.
  tiledb_stats_reset();

  // Stats should still be base level.
  check_stats_is(base_dumped_stats);
}
