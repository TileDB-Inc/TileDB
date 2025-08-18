#include "tiledb/common/tracing.h"

#include <opentelemetry/exporters/ostream/span_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/noop.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer_provider.h>

namespace tiledb::tracing {

static void init_stdout(void) {
  auto exporter =
      opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();
  auto processor =
      opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(
          std::move(exporter));
  std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
      opentelemetry::sdk::trace::TracerProviderFactory::Create(
          std::move(processor));
  // set the global trace provider
  trace_api::Provider::SetTracerProvider(provider);
}

static void init_otlp(const char* uri) {
  opentelemetry::exporter::otlp::OtlpHttpExporterOptions opts;
  opts.url = std::string(uri);

  auto exporter =
      opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create(opts);
  auto processor =
      opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(
          std::move(exporter));
  std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
      opentelemetry::sdk::trace::TracerProviderFactory::Create(
          std::move(processor));
  // set the global trace provider
  trace_api::Provider::SetTracerProvider(provider);
}

void init(const char* uri) {
  if (uri) {
    init_otlp(uri);
  } else {
    init_stdout();
  }
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> get_tracer() {
  return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      "tiledb");
}

}  // namespace tiledb::tracing
