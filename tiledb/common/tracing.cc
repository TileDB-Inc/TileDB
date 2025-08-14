#include "tiledb/common/tracing.h"

#include <opentelemetry/trace/noop.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer_provider.h>

namespace tiledb::tracing {

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> get_tracer() {
  return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      "tiledb");
}

}  // namespace tiledb::tracing
