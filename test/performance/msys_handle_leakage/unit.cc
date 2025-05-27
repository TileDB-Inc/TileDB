// cmake --build tiledb/test/performance --target
// tiledb_explore_msys_handle_leakage --config RelWithDebInfo

/* (from
 * https://app.shortcut.com/tiledb-inc/story/19067/rtools40-msys-builds-of-tiledb-unit-leaking-handles)
 *
 * (Event) Handle leakage in mingw builds (rtools40, rtools42) seems to be issue
 * within those cpp std libraries that tiledb usage aggravates, as visual studio
 * builds are not exhibiting this leakage.
 *
 * The leakage itself does not seem to be completely deterministic, as
 * repeatedly running with the same parameters (available with modified unit
 * test) multiple times generally results in different numbers of handles leaked
 * on any given run.  Leakage can be demonstrated with tiledb_unit filtering on
 * "CPP API: Test consolidation with timestamps, full and partial read with
 * dups"
 *
 * Modifications to unit-cppapi-consolidation-with-timestamps.cc which
 * implements that TEST_CASE indicate that in particular the activity of the
 * .query .submit()d in read_sparse() routine seems to be at least the largest
 * culprit, without the query there seems little/no leakage.
 *
 * The handle leakage may be causing some of the 32bit unit test failures in
 * mingw, as some of the tests that fail in an unfiltered run work acceptably
 * when run individually, but it seems 32bit builds are not going to be
 * supported with rtools42 so this may not be important.
 *
 * The ('recent') msys2 mingw versions I have installed of both gdb and lldb
 * seem to have (differing) issues of their own making it difficult to try and
 * gather a complete picture of exactly what/where all may be involved in calls
 * to the CreateEventA (windows api) routine presumed to be at least responsible
 * for the direct creation of the handles being leaked.
 *
 * See notes on building this module and running some trials in the top of
 * unit.cc along with some previously observed results.
 */

/*
 * WARNING:
 *   [2024/05/13] This file has succumbed to bit rot. It no longer compiles.
 */

// clang-format off
//.\tiledb\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=1 --consolidate-sparse-iters=1 --wait-for-keypress both
//./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=1 --consolidate-sparse-sparse-iters=1 --wait-for-keypress both

/* msys (from root of build tree, see <build_root>/test/performance/msys_handle_leakge/trials.sh
$ . ../test/performance/msys_handle_leakage/trials.sh
++ ./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=0 --consolidate-sparse-iters=1
===============================================================================
All tests passed (32 assertions in 1 test case)

handle_count
before 65
after  305
++ ./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=1 --consolidate-sparse-iters=1
===============================================================================
All tests passed (64 assertions in 1 test case)

handle_count
before 65
after  314
++ ./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=150 --perform-query=1 --consolidate-sparse-iters=1
===============================================================================
All tests passed (4832 assertions in 1 test case)

handle_count
before 65
after  1140
++ ./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=300 --perform-query=1 --consolidate-sparse-iters=1
===============================================================================
All tests passed (9632 assertions in 1 test case)

handle_count
before 65
after  1966
 */
 
 /* visual studio from root of build tree, see <build_tree>\test\performance\msys_handle_leakage\trials.bat
d:\dev\tiledb\gh.sc-19067-msys-handle-leakage.git\bld.vs19.A>rem .\tiledb\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=0 --consolidate-sparse-iters=1 --wait-for-keypress both

d:\dev\tiledb\gh.sc-19067-msys-handle-leakage.git\bld.vs19.A>.\tiledb\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=0 --consolidate-sparse-iters=1
===============================================================================
All tests passed (32 assertions in 1 test case)

handle_count
before 52
after  70

d:\dev\tiledb\gh.sc-19067-msys-handle-leakage.git\bld.vs19.A>.\tiledb\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=1 --consolidate-sparse-iters=1
===============================================================================
All tests passed (64 assertions in 1 test case)

handle_count
before 52
after  70

d:\dev\tiledb\gh.sc-19067-msys-handle-leakage.git\bld.vs19.A>.\tiledb\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=150 --perform-query=1 --consolidate-sparse-iters=1
===============================================================================
All tests passed (4832 assertions in 1 test case)

handle_count
before 52
after  70

d:\dev\tiledb\gh.sc-19067-msys-handle-leakage.git\bld.vs19.A>.\tiledb\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=300 --perform-query=1 --consolidate-sparse-iters=1
===============================================================================
All tests passed (9632 assertions in 1 test case)

handle_count
before 52
after  70
  */
// clang-format on

#define CATCH_CONFIG_RUNNER
#include <test/support/tdb_catch.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

extern unsigned int read_sparse_iters;
extern unsigned int perform_query;
extern unsigned int consolidate_sparse_iters;

namespace tiledb {
namespace test {

// Command line arguments.
int store_g_vfs(std::string&& vfs, std::vector<std::string> vfs_fs);

}  // namespace test
}  // namespace tiledb

int main(const int argc, char** const argv) {
  Catch::Session session;

  // Define acceptable VFS values.
  const std::vector<std::string> vfs_fs = {"native", "s3", "gcs", "azure"};

  // Build a pipe-separated string of acceptable VFS values.
  std::ostringstream vfs_fs_oss;
  for (size_t i = 0; i < vfs_fs.size(); ++i) {
    vfs_fs_oss << vfs_fs[i];
    if (i != (vfs_fs.size() - 1)) {
      vfs_fs_oss << "|";
    }
  }

  // Add a '--vfs' command line argument to override the default VFS.
  std::string vfs;
  Catch::Clara::Parser cli =
      session.cli() |
      Catch::Clara::Opt(vfs, vfs_fs_oss.str())["--vfs"](
          "Override the VFS filesystem to use for generic tests") |
      Catch::Clara::Opt(
          read_sparse_iters, "read_sparse_iters")["--read-sparse-iters"](
          "specify read_sparse_iters (default 1)") |
      Catch::Clara::Opt(perform_query, "perform_query")["--perform-query"](
          "specify whether to perform query (default non-zero)") |
      Catch::Clara::Opt(
          consolidate_sparse_iters,
          "consolidate_sparse_iters")["--consolidate-sparse-iters"](
          "how many read_sparse iters to perform (default 1)");

  session.cli(cli);

  int rc = session.applyCommandLine(argc, argv);
  if (rc != 0)
    return rc;

  // Validate and store the VFS command line argument.
  rc = tiledb::test::store_g_vfs(std::move(vfs), std::move(vfs_fs));
  if (rc != 0)
    return rc;

  DWORD handle_count_before = 0, handle_count_after;
  BOOL got_before, got_after;
  got_before = GetProcessHandleCount(GetCurrentProcess(), &handle_count_before);
  // return session.run();
  auto retval = session.run();
  got_after = GetProcessHandleCount(GetCurrentProcess(), &handle_count_after);
  std::cout << "handle_count" << std::endl
            << "before " << handle_count_before << std::endl
            << "after  " << handle_count_after << std::endl;
  return retval;
}

struct CICompletionStatusListener : Catch::EventListenerBase {
  using EventListenerBase::EventListenerBase;  // inherit constructor

  // Successful completion hook
  void testRunEnded(Catch::TestRunStats const& testRunStats) override {
    if (std::getenv("GITHUB_RUN_ID") != nullptr ||
        std::getenv("AGENT_NAME") != nullptr) {
      if (testRunStats.totals.testCases.allOk() == 1) {
        // set TILEDB_CI_SUCCESS job-level variable in azure pipelines
        // note: this variable is only set in subsequest tasks.
        printf("::set-output name=TILEDB_CI_SUCCESS::1\n");
        printf("##vso[task.setvariable variable=TILEDB_CI_SUCCESS]1\n");
      }
    }
  }
};
CATCH_REGISTER_LISTENER(CICompletionStatusListener)
