#ifndef TILEDB_COMMON_TRACING_H
#define TILEDB_COMMON_TRACING_H

#ifdef HAVE_TRACING

#include <opentelemetry/trace/tracer.h>

template <typename T>
using otel_ptr = opentelemetry::nostd::shared_ptr<T>;

namespace tiledb::tracing {

otel_ptr<opentelemetry::trace::Tracer> get_tracer();

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
  Scope(otel_ptr<opentelemetry::trace::Span> span)
      : span_(span)
      , scope_(span) {
  }

  otel_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::trace::Scope scope_;

  friend class ScopeBuilder;

 public:
  Scope(const char* name)
      : span_(get_tracer()->StartSpan(name))
      , scope_(span_) {
  }

  opentelemetry::trace::Span* operator->() {
    return span_.get();
  }
};

class ScopeBuilder {
 public:
  ScopeBuilder(const char* name)
      : name_(name) {
  }

  ScopeBuilder& with_attribute(
      std::string_view key, opentelemetry::common::AttributeValue value) {
    attributes_.add(key, value);
    return *this;
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
    return Scope(get_tracer()->StartSpan(name_, attributes_));
  }

 private:
  const char* name_;
  AttributeSet attributes_;
};

}  // namespace tiledb::tracing

#endif

#endif
