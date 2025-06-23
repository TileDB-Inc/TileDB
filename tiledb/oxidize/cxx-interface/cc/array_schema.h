#include "tiledb/oxidize/rust.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"

namespace tiledb::oxidize {

using namespace tiledb::sm;

using ConstAttribute = const Attribute;
using ConstDimension = const Dimension;

void set_domain(Dimension& dimension, rust::Slice<const uint8_t> domain);
void set_tile_extent(Dimension& dimension, rust::Slice<const uint8_t> domain);

}  // namespace tiledb::oxidize
