#include "tiledb/oxidize/test-support/cxx-interface/cc/result_tile.h"
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
    uint64_t num_cells,
    rust::Slice<const uint8_t> values,
    rust::Slice<const uint64_t> offsets,
    const uint8_t* validity) {
  if (offsets.empty()) {
    ResultTileSizes sizes(
        values.length(),
        0,
        std::nullopt,
        std::nullopt,
        (validity == nullptr ? std::nullopt : std::optional{num_cells}),
        (validity == nullptr ? std::nullopt : std::optional{0}));
    ResultTileData data(
        const_cast<uint8_t*>(values.data()),
        nullptr,
        const_cast<uint8_t*>(validity));

    return std::make_pair(sizes, data);
  } else {
    ResultTileSizes sizes(
        offsets.length() * sizeof(uint64_t),
        0,
        values.length(),
        0,
        (validity == nullptr ? std::nullopt : std::optional{num_cells}),
        (validity == nullptr ? std::nullopt : std::optional{0}));
    ResultTileData data(
        const_cast<uint64_t*>(offsets.data()),
        const_cast<uint8_t*>(values.data()),
        const_cast<uint8_t*>(validity));
    return std::make_pair(sizes, data);
  }
}

static void write_tiles(
    ResultTile& result_tile,
    const std::string& field,
    const ResultTileSizes& sizes,
    const ResultTileData& data) {
  auto* tuple = result_tile.tile_tuple(field);

  tuple->fixed_tile().write(data.fixed_filtered_data(), 0, sizes.tile_size());

  if (sizes.has_var_tile()) {
    tuple->var_tile().write(data.var_filtered_data(), 0, sizes.tile_var_size());
  }
  if (sizes.has_validity_tile()) {
    tuple->validity_tile().write(
        data.validity_filtered_data(), 0, sizes.tile_validity_size());
  }
}

void init_coord_tile(
    std::shared_ptr<ResultTile> result_tile,
    const ArraySchema& array_schema,
    const std::string& field,
    rust::Slice<const uint8_t> values,
    rust::Slice<const uint64_t> offsets,
    uint32_t dim_num) {
  const auto init =
      make_tile_initializers(result_tile->cell_num(), values, offsets, nullptr);
  result_tile->init_coord_tile(
      constants::format_version,
      array_schema,
      field,
      init.first,
      init.second,
      dim_num);
  write_tiles(*result_tile, field, init.first, init.second);
}

void init_attr_tile(
    std::shared_ptr<ResultTile> result_tile,
    const ArraySchema& array_schema,
    const std::string& field,
    rust::Slice<const uint8_t> values,
    rust::Slice<const uint64_t> offsets,
    const uint8_t* validity) {
  const auto init = make_tile_initializers(
      result_tile->cell_num(), values, offsets, validity);
  result_tile->init_attr_tile(
      constants::format_version, array_schema, field, init.first, init.second);
  write_tiles(*result_tile, field, init.first, init.second);
}

}  // namespace tiledb::test::oxidize
