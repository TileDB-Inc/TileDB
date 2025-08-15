#include "tiledb/api/c_api_support/tracing_wrapper.h"

namespace tiledb::api::tracing {

CApiTrace::CApiTrace(
    const char* funcname, std::unordered_map<size_t, std::string> args)
    : scope_(tiledb::tracing::ScopeBuilder(funcname)
                 .with_function_arguments(args)
                 .finish()) {
}

}  // namespace tiledb::api::tracing
