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
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb::sm {

/* ********************************* */
/*                API                */
/* ********************************* */

/**
 * Note: This function currently implements defective behavior.
 * Storing an array schema that does not have a URI attached to it should
 * _not_ succeed. Users should be aware of this behavior and avoid storage of
 * schemas with empty URIs.
 * This defect is scheduled for fix asap, but must be documented in the interim.
 */
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

shared_ptr<ArraySchema> handle_load_uri(
    const Context& ctx, const URI& uri, const Config* config) {
  // Check array name
  if (uri.is_invalid()) {
    throw std::runtime_error("Failed to load array schema; Invalid array URI");
  }

  if (uri.is_tiledb()) {
    auto& rest_client = ctx.rest_client();
    auto array_schema_response = rest_client.post_array_schema_from_rest(
        config ? *config : ctx.resources().config(), uri, 0, UINT64_MAX);
    return std::move(std::get<0>(array_schema_response));
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    throw_if_not_ok(
        key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));

    // Load URIs from the array directory
    optional<tiledb::sm::ArrayDirectory> array_dir;
    array_dir.emplace(
        ctx.resources(),
        uri,
        0,
        UINT64_MAX,
        tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY);

    auto tracker = ctx.resources().ephemeral_memory_tracker();
    // Load latest array schema
    auto&& array_schema_latest =
        array_dir->load_array_schema_latest(key, tracker);

    bool incl_enums = config->get<bool>(
        "rest.load_enumerations_on_array_open", Config::must_find);
    if (config && incl_enums) {
      std::vector<std::string> enmr_paths_to_load;
      auto enmr_names = array_schema_latest->get_enumeration_names();
      for (auto& name : enmr_names) {
        if (!array_schema_latest->is_enumeration_loaded(name)) {
          auto& path = array_schema_latest->get_enumeration_path_name(name);
          enmr_paths_to_load.emplace_back(path);
        }
      }

      auto enmrs_loaded = array_dir->load_enumerations_from_paths(
          enmr_paths_to_load, key, tracker);
      for (auto& enmr : enmrs_loaded) {
        array_schema_latest->store_enumeration(enmr);
      }
    }

    return std::move(array_schema_latest);
  }
}

}  // namespace tiledb::sm
