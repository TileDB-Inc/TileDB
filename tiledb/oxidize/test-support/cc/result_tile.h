#ifndef TILEDB_OXIDIZE_TEST_SUPPORT_RESULT_TILE_H
#define TILEDB_OXIDIZE_TEST_SUPPORT_RESULT_TILE_H

#include "tiledb/sm/query/readers/result_tile.h"

namespace tiledb::test::oxidize {

using namespace tiledb::sm;

std::shared_ptr<ResultTile> new_result_tile(
    std::shared_ptr<MemoryTracker> memory_tracker);

void init_attr_tile(
    std::shared_ptr<ResultTile> result_tile,
    const ArraySchema& array_schema,
    const std::string& field,
    rust::Slice<const uint8_t> data,
    rust::Slice<const uint64_t> offsets);

}  // namespace tiledb::test::oxidize

#endif
