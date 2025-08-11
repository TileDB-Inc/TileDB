#ifdef HAVE_TRACING
#include <opentelemetry/exporters/ostream/span_exporter_factory.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

static void initTracing() {
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

class TracingListener : public Catch::EventListenerBase {
 public:
  using Catch::EventListenerBase::EventListenerBase;

  void testRunStarting(Catch::TestRunInfo const&) override {
    // TODO: can we use the argument to configure?
    initTracing();
  }
};

CATCH_REGISTER_LISTENER(TracingListener)
#endif
