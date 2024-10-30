/**
 * @file   array_operations.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements I/O operations which support class `Array`.
 *
 * Note that these functions are _non-members_ of class `Array`, and therefore
 * do not need access to class member variables.
 *
 * These operations have been intentionally segregated from class `Array`
 * to support a future standalone object library, `array_operations`,
 * eliminating cyclic dependendencies in the build.
 *
 * This object library cannot yet be added, however, due to a dependence
 * of `load_delete_and_update_conditions` on `StrategyBase::throw_if_cancelled`.
 * This function relies on `StorageManagerCanonical`, which is slated for
 * removal. Once all `Query` code is overhauled to remove this dependence,
 * the `array_operations` module can be created.
 *
 */

#include "tiledb/sm/array/array_operations.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/deletes_and_updates/serialization.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/tile/generic_tile_io.h"

namespace tiledb::sm {

class ArrayOperationsException : public StatusException {
 public:
  explicit ArrayOperationsException(const std::string& message)
      : StatusException("ArrayOperations", message) {
  }
};

/* ********************************* */
/*                API                */
/* ********************************* */

tuple<std::vector<QueryCondition>, std::vector<std::vector<UpdateValue>>>
load_delete_and_update_conditions(
    ContextResources& resources, const OpenedArray& opened_array) {
  auto& locations =
      opened_array.array_directory().delete_and_update_tiles_location();
  auto conditions = std::vector<QueryCondition>(locations.size());
  auto update_values = std::vector<std::vector<UpdateValue>>(locations.size());

  auto status =
      parallel_for(&resources.compute_tp(), 0, locations.size(), [&](size_t i) {
        // Get condition marker.
        auto& uri = locations[i].uri();

        // Read the condition from storage.
        auto tile = GenericTileIO::load(
            resources,
            uri,
            locations[i].offset(),
            *(opened_array.encryption_key()),
            resources.ephemeral_memory_tracker());

        if (tiledb::sm::utils::parse::ends_with(
                locations[i].condition_marker(),
                tiledb::sm::constants::delete_file_suffix)) {
          conditions[i] = tiledb::sm::deletes_and_updates::serialization::
              deserialize_condition(
                  i,
                  locations[i].condition_marker(),
                  tile->data(),
                  tile->size());
        } else if (tiledb::sm::utils::parse::ends_with(
                       locations[i].condition_marker(),
                       tiledb::sm::constants::update_file_suffix)) {
          auto&& [cond, uvs] = tiledb::sm::deletes_and_updates::serialization::
              deserialize_update_condition_and_values(
                  i,
                  locations[i].condition_marker(),
                  tile->data(),
                  tile->size());
          conditions[i] = std::move(cond);
          update_values[i] = std::move(uvs);
        } else {
          throw ArrayOperationsException("Unknown condition marker extension");
        }

        throw_if_not_ok(
            conditions[i].check(opened_array.array_schema_latest()));
        return Status::Ok();
      });
  throw_if_not_ok(status);

  return {conditions, update_values};
}

}  // namespace tiledb::sm
