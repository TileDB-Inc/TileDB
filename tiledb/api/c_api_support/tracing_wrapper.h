#ifndef TILEDB_C_API_SUPPORT_TRACING_WRAPPER_H
#define TILEDB_C_API_SUPPORT_TRACING_WRAPPER_H

#ifdef HAVE_TRACING

#include "tiledb/common/tracing.h"

/**
 * Trait for statically dispatching a CAPI function pointer into the function
 * name.
 *
 * This is only defined as a specialization via the `CAPI_PREFIX` macro.
 */
template <auto Fn>
struct NameTrait {
  static constexpr bool exists = false;
};

namespace tiledb::api::tracing {

/**
 * Formats C API arguments for reporting as a span attribute
 */
struct ArgumentTrace {
  template <typename T, std::enable_if_t<std::is_arithmetic_v<T>, bool> = true>
  ArgumentTrace(T value)
      : fmt_(std::to_string(value)) {
  }

  template <typename T, std::enable_if_t<std::is_enum_v<T>, bool> = true>
  ArgumentTrace(T value)
      : fmt_(std::to_string(value)) {
  }

  /*
  ArgumentTrace(const char* cstr)
      : fmt_(cstr) {
  }
  */

  template <typename T, std::enable_if_t<std::is_pointer_v<T>, bool> = true>
  ArgumentTrace(T value) {
    std::stringstream s;
    s << value;
    fmt_ = s.str();
  }

  std::string fmt_;
};

std::unordered_map<size_t, std::string> format_arguments(
    std::convertible_to<ArgumentTrace> auto&&... args) {
  std::unordered_map<size_t, std::string> ret;

  size_t i = 0;
  for (auto v : std::initializer_list<ArgumentTrace>{args...}) {
    ret[i] = v.fmt_;
    i++;
  }

  return ret;
}

template <auto Fn>
struct TracingAspect;

template <typename R, typename... Args, R (*func)(Args...)>
struct TracingAspect<func> {
  static tiledb::tracing::Scope call(Args... args) {
    static_assert(NameTrait<func>::exists);
    static constexpr const char* funcname = NameTrait<func>::name;
    return tiledb::tracing::ScopeBuilder(funcname)
        .with_function_arguments(format_arguments(args...))
        .finish();
  }
};

}  // namespace tiledb::api::tracing

#undef CAPI_PREFIX
#define CAPI_PREFIX(root)                                \
  template <>                                            \
  struct NameTrait<tiledb::api::tiledb_##root> {         \
    static constexpr bool exists = true;                 \
    static constexpr const char* name = "tiledb_" #root; \
  };

template <auto f>
struct tiledb::api::CAPIFunctionSelector<f, void> {
  using aspect_type = tiledb::api::tracing::TracingAspect<f>;
};

#else
#endif

#endif
