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

}  // namespace tiledb::oxidize::sm
