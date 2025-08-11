#ifndef TILEDB_COMMON_TRACING_H
#define TILEDB_COMMON_TRACING_H

#ifdef HAVE_TRACING

#include <opentelemetry/trace/tracer.h>

namespace tiledb::tracing {

opentelemetry::trace::Tracer& get_tracer();

class EventBuilder : public opentelemetry::common::KeyValueIterable {
 public:
  EventBuilder() {
  }

  EventBuilder& attribute(
      std::string_view key, opentelemetry::common::AttributeValue value) {
    attributes_[std::string(key)] = value;
    return *this;
  }

 protected:
  size_t size() const noexcept override {
    return attributes_.size();
  }

  bool ForEachKeyValue(opentelemetry::nostd::function_ref<bool(
                           opentelemetry::nostd::string_view,
                           opentelemetry::common::AttributeValue)> callback)
      const noexcept override {
    for (const auto& kv : attributes_) {
      if (!callback(kv.first, kv.second)) {
        return false;
      }
    }
    return true;
  }

 private:
  std::map<std::string, opentelemetry::common::AttributeValue> attributes_;
};

}  // namespace tiledb::tracing

#endif

#endif
