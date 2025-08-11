#include "tiledb/common/tracing.h"

#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer_provider.h>

namespace tiledb::tracing {

opentelemetry::trace::Tracer& get_tracer() {
  static opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
      singleton;

  if (!singleton) {
    singleton = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
        "tiledb");
  }
  return *singleton.get();
}

}  // namespace tiledb::tracing
