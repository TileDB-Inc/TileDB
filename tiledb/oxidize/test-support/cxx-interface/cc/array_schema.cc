#include "tiledb/oxidize/test-support/cxx-interface/cc/array_schema.h"

using namespace tiledb::common;
using namespace tiledb::sm;

namespace tiledb::test::oxidize {

std::unique_ptr<Attribute> new_attribute(
    const std::string& name, Datatype dt, bool nullable) {
  return std::make_unique<Attribute>(name, dt, nullable);
}

std::unique_ptr<Dimension> new_dimension(
    const std::string& name,
    Datatype dt,
    std::shared_ptr<MemoryTracker> memory_tracker) {
  return std::make_unique<Dimension>(name, dt, memory_tracker);
}

std::unique_ptr<Domain> new_domain(
    std::shared_ptr<MemoryTracker> memory_tracker) {
  return std::make_unique<Domain>(memory_tracker);
}

std::unique_ptr<ArraySchema> new_array_schema(
    ArrayType array_type, std::shared_ptr<MemoryTracker> memory_tracker) {
  return std::make_unique<ArraySchema>(array_type, memory_tracker);
}

}  // namespace tiledb::test::oxidize
