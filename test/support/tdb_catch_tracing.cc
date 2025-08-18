#ifdef HAVE_TRACING
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include "tiledb.h"

class TracingListener : public Catch::EventListenerBase {
 public:
  using Catch::EventListenerBase::EventListenerBase;

  void testRunStarting(Catch::TestRunInfo const&) override {
    // TODO: can we use the argument to configure?
    tiledb_tracing_init("localhost:4317");
  }
};

CATCH_REGISTER_LISTENER(TracingListener)
#endif
