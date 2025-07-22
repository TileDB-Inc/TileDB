#include "tiledb/oxidize/cxx-interface/cc/array_schema.h"

using namespace tiledb::sm;

namespace tiledb::oxidize::sm {

namespace attribute {

const std::string* enumeration_name_cxx(const Attribute& attribute) {
  std::optional<std::reference_wrapper<const std::string>> e =
      attribute.get_enumeration_name();
  if (e.has_value()) {
    return &e.value().get();
  } else {
    return nullptr;
  }
}

}  // namespace attribute

namespace dimension {

void set_domain(Dimension& dimension, rust::Slice<const uint8_t> domain) {
  dimension.set_domain(static_cast<const void*>(domain.data()));
}

void set_tile_extent(Dimension& dimension, rust::Slice<const uint8_t> domain) {
  dimension.set_tile_extent(static_cast<const void*>(domain.data()));
}

}  // namespace dimension

namespace enumeration {

rust::Slice<const uint8_t> data_cxx(const Enumeration& enumeration) {
  std::span<const uint8_t> span = enumeration.data();
  return rust::Slice(span.data(), span.size());
}

rust::Slice<const uint8_t> offsets_cxx(const Enumeration& enumeration) {
  std::span<const uint8_t> span = enumeration.offsets();
  return rust::Slice(span.data(), span.size());
}

}  // namespace enumeration

namespace array_schema {

std::unique_ptr<std::vector<MaybeEnumeration>> enumerations(
    const ArraySchema& schema) {
  std::unique_ptr<std::vector<MaybeEnumeration>> e(
      new std::vector<MaybeEnumeration>(schema.enumeration_map().size()));

  for (const auto& enmr : schema.enumeration_map()) {
    if (enmr.second == nullptr) {
      e->push_back(MaybeEnumeration::not_loaded(enmr.first));
    } else {
      e->push_back(MaybeEnumeration::loaded(enmr.second));
    }
  }

  return e;
}

}  // namespace array_schema

}  // namespace tiledb::oxidize::sm
