#ifndef TILEDB_COMMON_TRACING_H
#define TILEDB_COMMON_TRACING_H

#ifdef HAVE_TRACING

#include <opentelemetry/trace/tracer.h>

namespace tiledb::tracing {

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> get_tracer();

class AttributeSet : public opentelemetry::common::KeyValueIterable {
 public:
  AttributeSet() {
  }

  AttributeSet& add(
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

class Scope {
 private:
  Scope(opentelemetry::trace::Scope&& scope)
      : otel_(std::move(scope)) {
  }

  opentelemetry::trace::Scope otel_;

  friend class ScopeBuilder;
};

class ScopeBuilder {
 public:
  ScopeBuilder(const char* name)
      : name_(name) {
  }

  ScopeBuilder& with_function_arguments(
      std::unordered_map<size_t, std::string> args) {
    for (const auto& arg : args) {
      const std::string key = "arg" + std::to_string(arg.first);
      attributes_.add(key, arg.second);
    }
    return *this;
  }

  Scope finish() {
    auto span = get_tracer()->StartSpan(name_, attributes_);
    return Scope(opentelemetry::trace::Scope(span));
  }

 private:
  const char* name_;
  AttributeSet attributes_;
};

}  // namespace tiledb::tracing

#endif

#endif
