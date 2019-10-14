#define CATCH_CONFIG_MAIN
#include <catch.hpp>

// Successful completion hook
#include <cstdlib>
#include <iostream>

struct CICompletionStatusListener : Catch::TestEventListenerBase {
  using TestEventListenerBase::TestEventListenerBase;  // inherit constructor

  void testRunEnded(Catch::TestRunStats const& testRunStats) override {
    if (std::getenv("AGENT_NAME") != nullptr) {
      if (testRunStats.totals.testCases.allOk() == 1) {
        // set TILEDB_CI_SUCCESS job-level variable in azure pipelines
        // note: this variable is only set in subsequest tasks.
        std::cout << "##vso[task.setvariable variable=TILEDB_CI_SUCCESS]1"
                  << std::endl;
      }
    }
  }
};
CATCH_REGISTER_LISTENER(CICompletionStatusListener)
