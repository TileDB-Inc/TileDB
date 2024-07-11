/**
 * @file   array_schema_operations.cc
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
 * This file implements I/O operations which support the ArraySchema class.
 */

#include "tiledb/sm/array_schema/array_schema_operations.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb::sm {

/* ********************************* */
/*                API                */
/* ********************************* */

void store_array_schema(
    ContextResources& resources,
    const shared_ptr<ArraySchema>& array_schema,
    const EncryptionKey& encryption_key) {
  const URI schema_uri = array_schema->uri();

  // Serialize
  SizeComputationSerializer size_computation_serializer;
  array_schema->serialize(size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(),
      resources.ephemeral_memory_tracker())};
  Serializer serializer(tile->data(), tile->size());
  array_schema->serialize(serializer);
  resources.stats().add_counter("write_array_schema_size", tile->size());

  // Delete file if it exists already
  bool exists;
  throw_if_not_ok(resources.vfs().is_file(schema_uri, &exists));
  if (exists) {
    throw_if_not_ok(resources.vfs().remove_file(schema_uri));
  }

  // Check if the array schema directory exists
  // If not create it, this is caused by a pre-v10 array
  bool schema_dir_exists = false;
  URI array_schema_dir_uri =
      array_schema->array_uri().join_path(constants::array_schema_dir_name);
  throw_if_not_ok(
      resources.vfs().is_dir(array_schema_dir_uri, &schema_dir_exists));

  if (!schema_dir_exists) {
    throw_if_not_ok(resources.vfs().create_dir(array_schema_dir_uri));
  }

  GenericTileIO::store_data(resources, schema_uri, tile, encryption_key);

  // Create the `__enumerations` directory under `__schema` if it doesn't
  // exist. This might happen if someone tries to add an enumeration to an
  // array created before version 19.
  bool enumerations_dir_exists = false;
  URI array_enumerations_dir_uri =
      array_schema_dir_uri.join_path(constants::array_enumerations_dir_name);
  throw_if_not_ok(resources.vfs().is_dir(
      array_enumerations_dir_uri, &enumerations_dir_exists));

  if (!enumerations_dir_exists) {
    throw_if_not_ok(resources.vfs().create_dir(array_enumerations_dir_uri));
  }

  // Serialize all enumerations into the `__enumerations` directory
  for (auto& enmr_name : array_schema->get_loaded_enumeration_names()) {
    auto enmr = array_schema->get_enumeration(enmr_name);
    if (enmr == nullptr) {
      throw std::runtime_error(
          "Error serializing enumeration; Loaded enumeration is null");
    }

    SizeComputationSerializer enumeration_size_serializer;
    enmr->serialize(enumeration_size_serializer);

    auto tile{WriterTile::from_generic(
        enumeration_size_serializer.size(),
        resources.ephemeral_memory_tracker())};
    Serializer serializer(tile->data(), tile->size());
    enmr->serialize(serializer);

    auto abs_enmr_uri = array_enumerations_dir_uri.join_path(enmr->path_name());
    GenericTileIO::store_data(resources, abs_enmr_uri, tile, encryption_key);
  }
}

}  // namespace tiledb::sm
