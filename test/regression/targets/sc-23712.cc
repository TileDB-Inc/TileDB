#include <catch2/catch_test_macros.hpp>

#include <tiledb/sm/stats/global_stats.h>
#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb::sm::stats;

static std::string empty_dumped_stats = {"[\n\n]\n"};
// Currently there are no stats that are always present.
// Can, however, envision a time when something (stats calls themselves?) might
// be accumulated, thus having a forever outstanding stats item, the base
// version of which might be represented in this base_dumped_stats, rather
// than the output being totally 'empty'.
static std::string base_dumped_stats = empty_dumped_stats;

// Using CPP_API (which invokes C_API) routines, exercise a routine registering
// stats to exercise the paths (before bug fix) causing the leakage
// problem even when involved Context(s) were destructed.
TEST_CASE(
    "Stats registration handling, indirect via Context", "[stats][context]") {
  // Examine stats output as reflection of whether registered data allocations
  // might be held beyond a reset and/or registrant destruction.

  auto check_stats_is = [](std::string s) -> void {
    char* ptr_dumped_stats{nullptr};
    tiledb_stats_raw_dump_str(&ptr_dumped_stats);
    CHECK(std::string(ptr_dumped_stats) == s);
    tiledb_stats_free_str(&ptr_dumped_stats);
  };
  auto check_stats_is_not = [](std::string s) -> void {
    char* ptr_dumped_stats{nullptr};
    tiledb_stats_raw_dump_str(&ptr_dumped_stats);
    CHECK(std::string(ptr_dumped_stats) != s);
    tiledb_stats_free_str(&ptr_dumped_stats);
  };

  SECTION(" - baseline of no stats") {
    // If this fails, look for something else in overall test program that may
    // have generated stats in a fashion that they were not cleaned up, most
    // likely something creating and keeping a Context active outside of these
    // tests.
    check_stats_is(base_dumped_stats);
  }

  SECTION(" - creation/destruction of context should leave no stats") {
    {
      // local block to enclose construction/destruction of Context ctx.
      tiledb::Context ctx;
      // Nothing has been done to generate stats, should still be base.
      check_stats_is(base_dumped_stats);
    }
    // should still be base after Context gone
    check_stats_is(base_dumped_stats);
  }

  SECTION(" - create stats, be sure they are released") {
    {  // local block to enclose construction/destruction of Context ctx.

      // class Context registers data with the global GlobalStats entity.
      tiledb::Context ctx;
      // Nothing has been done to generate stats, should still be base.
      check_stats_is(base_dumped_stats);

      // Now set up for and performs stats generating action.

      tiledb::VFS vfs(ctx);

      // Stats still base.
      check_stats_is(base_dumped_stats);

      std::string irrelevant_filename =
          "not.expected.to.exist.but.doesnt.matter.if.does";
      // Need the side effect of stats generation from this call,
      // actual results of the call irrelevant.
      (void)vfs.is_file(irrelevant_filename);
      // Stats should no longer be base.
      check_stats_is_not(base_dumped_stats);

      // Perform reset of any remaining stats and remove any
      // previously registered stats for already destructed registrants.
      tiledb_stats_reset();
      check_stats_is(base_dumped_stats);

      // Populate it again, to be sure it's missing after we exit block and
      // original (Context ctx) registered stats were destroyed.
      // After the side effect of stats generation from this call,
      // actual results irrelevant.
      (void)vfs.is_file(irrelevant_filename);

      // check again that it's not at base level.
      check_stats_is_not(base_dumped_stats);

      // 'ctx' is destructed at end of block and items it registered should be
      // released.
    }

    // Registered stats only knows about weak_ptr, original registered stats
    // is gone and output should be base level.
    check_stats_is(base_dumped_stats);
  }

  // verify once more
  check_stats_is(base_dumped_stats);

  // Perform reset of any remaining stats (none in this test) to remove
  // previously registered stats for already destructed registrants.
  tiledb_stats_reset();

  // Stats should still be base level.
  check_stats_is(base_dumped_stats);
}
