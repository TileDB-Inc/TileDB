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

#if 0
//TBD: Which version to use, should linkage be changed to tiledb_static with other required 
// (CI at least) changes?
// This version might be desireable as it tries to mimic the errant situation without
// invoking 'in-between' routines that actually created it, however the 'tiledb_regression'
// linking is currently setup to use shared, and internal routines needed are not visible
// externally from shared library version of tiledb.
// Those routines can be accessed if linked with tiledb_static, but this would require
// any attempt to exercise tiledb_regression to have been built with the currently optional
// 'static' build feature.  
TEST_CASE(
    "Test stats leakage with Context",
    "[regression]") {
  //tiledb::Context context;
  std::string dumped_stats;

  // Similar to above, but this time exerciase another item that 
  // should populate some stats.
  {

    // class Context registers stats entity with GlobalStats all_stats.
    tiledb::Context ctx;
    // Nothing has been done to generate stats, should still be base.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    // Now set up for and performs stats generating action.

    // ...
    auto stats = ctx.ptr()->storage_manager()->stats();
    //auto compute_tp = ctx.ptr()->storage_manager()->compute_tp();
    //auto io_tp = ctx.ptr()->storage_manager()->io_tp();
    //auto ctx_config = ctx.ptr()->storage_manager()->config();
    // construction invokes a stats->create_child()
    //tiledb::sm::VFS vfs(stats, compute_tp, io_tp, ctx_config);
    Stats* child_stats{stats->create_child("TestStats")};
    //all_stats.register_stats(stats);    
    
    // Stats still base.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    //~ bool does_it_exist = false;
    //~ std::string irrelevant_filename =
        //~ "not.expected.to.exist.but.doesnt.matter.if.does";
    //~ tiledb::sm::URI uri(irrelevant_filename);
    //~ // .is_file() actually adds a counter to the stats.
    //~ CHECK(vfs.is_file(uri, &does_it_exist).ok());
    child_stats->add_counter("counter_statistic", 1);
    // Stats should no longer be base.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats != base_dumped_stats);

  // Perform reset of any remaining stats and remove any
    // previously registered stats for already destructed registrants.
    all_stats.reset();
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats == base_dumped_stats);

    // Populate it again, to be sure it's missing after we exit block and
    // original (Context ctx) registered stats were destroyed.
    //~ CHECK(vfs.is_file(uri, &does_it_exist).ok());
    child_stats->add_counter("counter_statistic", 1);

    // check again that it's not at base level.
    all_stats.dump(&dumped_stats);
    CHECK(dumped_stats != base_dumped_stats);
    
    // end of scope for variable ctx, with bug children statistics remained.
    // After fix, should no longer remain.
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
#else
// Version that does, via C_API routines, exercise a routine registering stats
// to exercise the paths causing the leakage problem even when involved Context(s)
// are destructed (before bug fix).
TEST_CASE("Stats registration handling, indirect via Context", "[stats][context]") {
  // Examine stats output when class Context active as reflection of
  // whether data allocations might be held beyond a reset and/or registrant destruction.
  
  //std::string dumped_stats;
  //all_stats.dump(&dumped_stats);
  //CHECK(dumped_stats == base_dumped_stats);
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
    //all_stats.dump(&dumped_stats);
    //CHECK(dumped_stats == base_dumped_stats);
    check_stats_is(base_dumped_stats);
  }

  // should still be base after Context gone
  //all_stats.dump(&dumped_stats);
  //CHECK(dumped_stats == base_dumped_stats);
  check_stats_is(base_dumped_stats);

  // Similar to above, but this time exercise another item that 
  // should populate some stats.
  {

    // class Context registers a GlobalStats entity.
    tiledb::Context ctx;
    // Nothing has been done to generate stats, should still be base.
    //all_stats.dump(&dumped_stats);
    //CHECK(dumped_stats == base_dumped_stats);
    check_stats_is(base_dumped_stats);

    // Now set up for and performs stats generating action.

    // ...
//    auto stats = ctx.ptr()->storage_manager()->stats();
//    auto compute_tp = ctx.ptr()->storage_manager()->compute_tp();
//    auto io_tp = ctx.ptr()->storage_manager()->io_tp();
//    auto ctx_config = ctx.ptr()->storage_manager()->config();
    // construction invokes a stats->create_child()
//    tiledb::sm::VFS vfs(stats, compute_tp, io_tp, ctx_config);
    tiledb::VFS vfs(ctx);
    
    // Stats still base.
    //all_stats.dump(&dumped_stats);
    //CHECK(dumped_stats == base_dumped_stats);
    check_stats_is(base_dumped_stats);

    bool does_it_exist = false;
    std::string irrelevant_filename =
        "not.expected.to.exist.but.doesnt.matter.if.does";
//    tiledb::sm::URI uri(irrelevant_filename);
    // .is_file() actually adds a counter to the stats.
//    CHECK(vfs.is_file(uri, &does_it_exist).ok());
    does_it_exist = vfs.is_file(irrelevant_filename);
    // Stats should no longer be base.
    //all_stats.dump(&dumped_stats);
    //CHECK(dumped_stats != base_dumped_stats);
    check_stats_is_not(base_dumped_stats);

  // Perform reset of any remaining stats and remove any
    // previously registered stats for already destructed registrants.
    //all_stats.reset();
    tiledb_stats_reset();
    //all_stats.dump(&dumped_stats);
    //CHECK(dumped_stats == base_dumped_stats);
    check_stats_is(base_dumped_stats);

    // Populate it again, to be sure it's missing after we exit block and
    // original (Context ctx) registered stats were destroyed.
//    CHECK(vfs.is_file(uri, &does_it_exist).ok());
    does_it_exist = vfs.is_file(irrelevant_filename);

    // check again that it's not at base level.
    //all_stats.dump(&dumped_stats);
    //CHECK(dumped_stats != base_dumped_stats);
    check_stats_is_not(base_dumped_stats);
    
    //'ctx' is destructed at end of block and registered stats should be released.
  }

  // Registered stats only knows about weak_ptr, original registered stats
  // is gone and output is now again base level.
  //all_stats.dump(&dumped_stats);
  //CHECK(dumped_stats == base_dumped_stats);
  check_stats_is(base_dumped_stats);

  // Perform reset of any remaining stats (none in this test) and remove
  // previously registered stats for already destructed registrants.
  //all_stats.reset();
  tiledb_stats_reset();

  // Stats should still be base level.
  //all_stats.dump(&dumped_stats);
  //CHECK(dumped_stats == base_dumped_stats);
  check_stats_is(base_dumped_stats);
  
}
#endif
