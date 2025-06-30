#include "tiledb/oxidize/rust.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"

namespace tiledb::oxidize::sm {

using namespace tiledb::sm;

namespace attribute {

using ConstAttribute = const Attribute;

const std::string* enumeration_name_cxx(const Attribute& attribute);

}  // namespace attribute

namespace dimension {

using namespace tiledb::sm;

using ConstDimension = const tiledb::sm::Dimension;

void set_domain(Dimension& dimension, rust::Slice<const uint8_t> domain);
void set_tile_extent(Dimension& dimension, rust::Slice<const uint8_t> domain);

}  // namespace dimension

}  // namespace tiledb::oxidize::sm
