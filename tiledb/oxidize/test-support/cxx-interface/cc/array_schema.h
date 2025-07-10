#ifndef TILEDB_OXIDIZE_TEST_SUPPORT_ARRAY_SCHEMA_H
#define TILEDB_OXIDIZE_TEST_SUPPORT_ARRAY_SCHEMA_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"

namespace tiledb::test::oxidize {

using namespace tiledb::common;
using namespace tiledb::sm;

std::unique_ptr<Attribute> new_attribute(
    const std::string& name, Datatype dt, bool nullable);

std::unique_ptr<Dimension> new_dimension(
    const std::string& name,
    Datatype dt,
    std::shared_ptr<MemoryTracker> memory_tracker);

std::unique_ptr<Domain> new_domain(
    std::shared_ptr<MemoryTracker> memory_tracker);

std::unique_ptr<ArraySchema> new_array_schema(
    ArrayType array_type, std::shared_ptr<MemoryTracker> memory_tracker);

[[maybe_unused]] static std::shared_ptr<Attribute> attribute_to_shared(
    std::unique_ptr<Attribute> attr) {
  return attr;
}

[[maybe_unused]] static std::shared_ptr<Dimension> dimension_to_shared(
    std::unique_ptr<Dimension> dim) {
  return dim;
}

[[maybe_unused]] static std::shared_ptr<Domain> domain_to_shared(
    std::unique_ptr<Domain> domain) {
  return domain;
}

[[maybe_unused]] static std::shared_ptr<ArraySchema> array_schema_to_shared(
    std::unique_ptr<ArraySchema> array_schema) {
  return array_schema;
}

[[maybe_unused]] static std::shared_ptr<const Attribute> as_const_attribute(
    std::shared_ptr<Attribute> attr) {
  return attr;
}

}  // namespace tiledb::test::oxidize

#endif
