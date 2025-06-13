#include "tiledb/oxidize/test-support/cc/result_tile.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::sm;

namespace tiledb::test::oxidize {

std::shared_ptr<ResultTile> new_result_tile(
    uint64_t cell_num,
    const ArraySchema& array_schema,
    std::shared_ptr<MemoryTracker> memory_tracker) {
  return std::make_shared<ResultTile>(array_schema, cell_num, memory_tracker);
}

static std::pair<ResultTileSizes, ResultTileData> make_tile_initializers(
    rust::Slice<const uint8_t> values, rust::Slice<const uint64_t> offsets) {
  if (offsets.empty()) {
    ResultTileSizes sizes(
        values.length(),
        0,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt);
    ResultTileData data(const_cast<uint8_t*>(values.data()), nullptr, nullptr);

    return std::make_pair(sizes, data);
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
    return std::make_pair(sizes, data);
  }
}

void init_coord_tile(
    std::shared_ptr<ResultTile> result_tile,
    const ArraySchema& array_schema,
    const std::string& field,
    rust::Slice<const uint8_t> values,
    rust::Slice<const uint64_t> offsets,
    uint32_t dim_num) {
  const auto init = make_tile_initializers(values, offsets);
  result_tile->init_coord_tile(
      constants::format_version,
      array_schema,
      field,
      init.first,
      init.second,
      dim_num);
}

void init_attr_tile(
    std::shared_ptr<ResultTile> result_tile,
    const ArraySchema& array_schema,
    const std::string& field,
    rust::Slice<const uint8_t> values,
    rust::Slice<const uint64_t> offsets) {
  const auto init = make_tile_initializers(values, offsets);
  result_tile->init_attr_tile(
      constants::format_version, array_schema, field, init.first, init.second);
}

}  // namespace tiledb::test::oxidize
