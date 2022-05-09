#define CATCH_CONFIG_RUNNER
#include <catch.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace tiledb {
namespace test {

// Command line arguments.
int store_g_vfs(std::string&& vfs, std::vector<std::string> vfs_fs);

}  // namespace test
}  // namespace tiledb

int main(const int argc, char** const argv) {
  Catch::Session session;

  // Define acceptable VFS values.
  const std::vector<std::string> vfs_fs = {"native", "s3", "hdfs", "azure"};

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
  Catch::clara::Parser cli =
      session.cli() |
      Catch::clara::Opt(vfs, vfs_fs_oss.str())["--vfs"](
          "Override the VFS filesystem to use for generic tests");

  session.cli(cli);

  int rc = session.applyCommandLine(argc, argv);
  if (rc != 0)
    return rc;

  // Validate and store the VFS command line argument.
  rc = tiledb::test::store_g_vfs(std::move(vfs), std::move(vfs_fs));
  if (rc != 0)
    return rc;

  return session.run();
}

struct CICompletionStatusListener : Catch::TestEventListenerBase {
  using TestEventListenerBase::TestEventListenerBase;  // inherit constructor

  // Successful completion hook
  void testRunEnded(Catch::TestRunStats const& testRunStats) override {
//    if (std::getenv("GITHUB_RUN_ID") != nullptr ||
//        std::getenv("AGENT_NAME") != nullptr) {
      if (testRunStats.totals.testCases.allOk() == 1) {
        // set TILEDB_CI_SUCCESS job-level variable in azure pipelines
        // note: this variable is only set in subsequest tasks.
        printf("::set-output name=TILEDB_CI_SUCCESS::1\n");
        printf("##vso[task.setvariable variable=TILEDB_CI_SUCCESS]1\n");
      }
//    }
  }
};
CATCH_REGISTER_LISTENER(CICompletionStatusListener)
