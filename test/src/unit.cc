#define CATCH_CONFIG_RUNNER
#include <test/support/tdb_catch.h>

#include <cstdlib>
#include <fstream>
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
  const std::vector<std::string> vfs_fs = {
      "native", "s3", "hdfs", "azure", "gcs", "rest-s3"};

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

struct CICompletionStatusListener : Catch::EventListenerBase {
  using Catch::EventListenerBase::EventListenerBase;  // inherit constructor

  // Successful completion hook:
  // This is a secondary validation that the tests ran and succeeded.
  // - For Github actions, we write into step output
  // - For Azure Pipelines, we set an environment variable
  // These outputs are validated in a separate CI step.

  void testRunEnded(Catch::TestRunStats const& testRunStats) override {
    if (testRunStats.totals.testCases.allOk() != 1) {
      // Test failed, *don't* set success variable.
      return;
    }

    if (const char* gh_state_filename_p = std::getenv("GITHUB_OUTPUT");
        gh_state_filename_p != nullptr) {
      std::string state_filename(gh_state_filename_p);
      if (std::ofstream output{state_filename}) {
        output << "TILEDB_CI_SUCCESS=1"
               << "\n";
      } else {
        std::cerr << "Failed to open GITHUB_STATE file for CI_SUCCESS tracking!"
                  << std::endl;
      }
    } else if (std::getenv("AGENT_NAME") != nullptr) {
      // set TILEDB_CI_SUCCESS job-level variable in azure pipelines
      // note: this variable is only set in subsequest tasks.
      printf("##vso[task.setvariable variable=TILEDB_CI_SUCCESS]1\n");
    }
  }
};
CATCH_REGISTER_LISTENER(CICompletionStatusListener)
