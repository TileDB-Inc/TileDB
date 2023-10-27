#define CATCH_CONFIG_RUNNER
#include <test/support/tdb_catch.h>

int main(const int argc, char** const argv) {
  Catch::Session session;

  int rc = session.applyCommandLine(argc, argv);
  if (rc != 0)
    return rc;

  return session.run();
}
