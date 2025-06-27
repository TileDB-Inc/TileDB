#include "tiledb/oxidize/cxx-interface/cc/array_schema.h"

namespace tiledb::oxidize {

using namespace tiledb::sm;

void set_domain(Dimension& dimension, rust::Slice<const uint8_t> domain) {
  dimension.set_domain(static_cast<const void*>(domain.data()));
}

void set_tile_extent(Dimension& dimension, rust::Slice<const uint8_t> domain) {
  dimension.set_tile_extent(static_cast<const void*>(domain.data()));
}

}  // namespace tiledb::oxidize
