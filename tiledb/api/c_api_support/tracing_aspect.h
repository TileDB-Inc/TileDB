#ifndef TILEDB_C_API_SUPPORT_TRACING_ASPECT_H
#define TILEDB_C_API_SUPPORT_TRACING_ASPECT_H

#ifdef HAVE_TRACING

#include "tiledb/api/c_api_support/tracing_wrapper.h"

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

#endif

#endif
