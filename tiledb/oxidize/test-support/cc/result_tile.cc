#include "tiledb/oxidize/test-support/cc/result_tile.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::sm;

namespace tiledb::test::oxidize {

std::shared_ptr<ResultTile> new_result_tile(
    std::shared_ptr<MemoryTracker> memory_tracker) {
  return std::make_shared<ResultTile>(memory_tracker);
}

void init_attr_tile(
    std::shared_ptr<ResultTile> result_tile,
    const ArraySchema& array_schema,
    const std::string& field,
    rust::Slice<const uint8_t> values,
    rust::Slice<const uint64_t> offsets) {
  if (offsets.empty()) {
    ResultTileSizes sizes(
        values.length(),
        0,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt);
    ResultTileData data(const_cast<uint8_t*>(values.data()), nullptr, nullptr);

    result_tile->init_attr_tile(
        constants::format_version, array_schema, field, sizes, data);
  } else {
    ResultTileSizes sizes(
        offsets.length() * sizeof(uint64_t),
        0,
        values.length(),
        0,
        std::nullopt,
        std::nullopt);
    ResultTileData data(
        const_cast<uint64_t*>(offsets.data()),
        const_cast<uint8_t*>(values.data()),
        nullptr);

    result_tile->init_attr_tile(
        constants::format_version, array_schema, field, sizes, data);
  }
}

}  // namespace tiledb::test::oxidize
