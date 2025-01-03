#include <test/support/src/array_helpers.h>

namespace tiledb::test {

tiledb_error_t* SparseGlobalOrderReaderMemoryBudget::apply(
    tiledb_config_t* config) {
  tiledb_error_t* error;

  if (tiledb_config_set(
          config, "sm.mem.total_budget", total_budget_.c_str(), &error) !=
      TILEDB_OK) {
    return error;
  }

  if (tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_tile_ranges",
          ratio_tile_ranges_.c_str(),
          &error) != TILEDB_OK) {
    return error;
  }

  if (tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_array_data",
          ratio_array_data_.c_str(),
          &error) != TILEDB_OK) {
    return error;
  }

  if (tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_coords",
          ratio_coords_.c_str(),
          &error) != TILEDB_OK) {
    return error;
  }

  return nullptr;
}

}  // namespace tiledb::test
