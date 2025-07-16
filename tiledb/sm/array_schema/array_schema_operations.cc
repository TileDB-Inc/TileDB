/**
 * @file   array_schema_operations.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024-2025 TileDB, Inc.
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
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/integral_type_casts.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb::sm {

/** Class for locally generated status exceptions. */
class ArraySchemaOperationsException : public StatusException {
 public:
  explicit ArraySchemaOperationsException(const std::string& msg)
      : StatusException("ArraySchemaOperations", msg) {
  }
};

/* ********************************* */
/*                API                */
/* ********************************* */

// ===== FORMAT =====
// version (uint32_t)
// allow_dups (bool)
// array_type (uint8_t)
// tile_order (uint8_t)
// cell_order (uint8_t)
// capacity (uint64_t)
// coords_filters (see FilterPipeline::serialize)
// cell_var_offsets_filters (see FilterPipeline::serialize)
// cell_validity_filters (see FilterPipeline::serialize)
// domain
// attribute_num (uint32_t)
//   attribute #1
//   attribute #2
//   ...
// dimension_label_num (uint32_t)
//   dimension_label #1
//   dimension_label #2
//   ...
// current_domain
void serialize_array_schema(
    Serializer& serializer, const ArraySchema& array_schema) {
  // Write version, which is always the current version. Despite
  // the in-memory `version_`, we will serialize every array schema
  // as the latest version.
  const format_version_t version = constants::format_version;
  serializer.write<format_version_t>(version);

  // Write allows_dups
  serializer.write<uint8_t>(array_schema.allows_dups());

  // Write array type
  serializer.write<uint8_t>((uint8_t)array_schema.array_type());

  // Write tile and cell order
  serializer.write<uint8_t>((uint8_t)array_schema.tile_order());
  serializer.write<uint8_t>((uint8_t)array_schema.cell_order());

  // Write capacity
  serializer.write<uint64_t>(array_schema.capacity());

  // Write coords filters
  array_schema.coords_filters().serialize(serializer);

  // Write offsets filters
  array_schema.cell_var_offsets_filters().serialize(serializer);

  // Write validity filters
  array_schema.cell_validity_filters().serialize(serializer);

  // Write domain
  array_schema.domain().serialize(serializer, version);

  // Write attributes
  auto attribute_num = (uint32_t)array_schema.attributes().size();
  serializer.write<uint32_t>(attribute_num);
  for (auto& attr : array_schema.attributes()) {
    attr->serialize(serializer, version);
  }

  // Write dimension labels
  auto dimension_labels = array_schema.dimension_labels();
  auto label_num = static_cast<uint32_t>(dimension_labels.size());
  if (label_num != dimension_labels.size()) {
    throw ArraySchemaOperationsException(
        "Overflow when attempting to serialize label number.");
  }
  serializer.write<uint32_t>(label_num);
  for (auto& label : dimension_labels) {
    label->serialize(serializer, version);
  }

  // Write Enumeration path map
  auto enmr_num = utils::safe_integral_cast<size_t, uint32_t>(
      array_schema.enumeration_map().size());

  serializer.write<uint32_t>(enmr_num);
  for (auto& [enmr_name, enmr_uri] : array_schema.enumeration_path_map()) {
    auto enmr_name_size = static_cast<uint32_t>(enmr_name.size());
    serializer.write<uint32_t>(enmr_name_size);
    serializer.write(enmr_name.data(), enmr_name_size);

    auto enmr_uri_size = static_cast<uint32_t>(enmr_uri.size());
    serializer.write<uint32_t>(enmr_uri_size);
    serializer.write(enmr_uri.data(), enmr_uri_size);
  }

  // Serialize array current domain information
  array_schema.current_domain().serialize(serializer);
}

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
  serialize_array_schema(size_computation_serializer, *array_schema);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(),
      resources.ephemeral_memory_tracker())};
  Serializer serializer(tile->data(), tile->size());
  serialize_array_schema(serializer, *array_schema);
  resources.stats().add_counter("write_array_schema_size", tile->size());

  // Delete file if it exists already
  if (resources.vfs().is_file(schema_uri)) {
    resources.vfs().remove_file(schema_uri);
  }

  // Check if the array schema directory exists
  // If not create it, this is caused by a pre-v10 array
  URI array_schema_dir_uri =
      array_schema->array_uri().join_path(constants::array_schema_dir_name);
  if (!resources.vfs().is_dir(array_schema_dir_uri)) {
    resources.vfs().create_dir(array_schema_dir_uri);
  }

  GenericTileIO::store_data(resources, schema_uri, tile, encryption_key);

  // Create the `__enumerations` directory under `__schema` if it doesn't
  // exist. This might happen if someone tries to add an enumeration to an
  // array created before version 19.
  URI array_enumerations_dir_uri =
      array_schema_dir_uri.join_path(constants::array_enumerations_dir_name);
  if (!resources.vfs().is_dir(array_enumerations_dir_uri)) {
    resources.vfs().create_dir(array_enumerations_dir_uri);
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

shared_ptr<ArraySchema> load_array_schema(
    const Context& ctx, const URI& uri, const Config& config) {
  // Check array name
  if (uri.is_invalid()) {
    throw std::runtime_error("Failed to load array schema; Invalid array URI");
  }

  // Load enumerations if config option is set.
  const bool incl_enums = config.get<bool>(
      "rest.load_enumerations_on_array_open", Config::must_find);

  if (uri.is_tiledb()) {
    auto& rest_client = ctx.rest_client();
    auto&& [st, array_schema_response] =
        rest_client.get_array_schema_from_rest(uri);
    throw_if_not_ok(st);
    auto array_schema = std::move(array_schema_response).value();

    if (incl_enums) {
      auto tracker = ctx.resources().ephemeral_memory_tracker();
      // Pass an empty list of enumeration names. REST will use timestamps to
      // load all enumerations on all schemas for the array within that range.
      auto ret = rest_client.post_enumerations_from_rest(
          uri,
          array_schema->timestamp_start(),
          array_schema->timestamp_end(),
          config,
          *array_schema,
          array_schema->get_enumeration_names(),
          tracker);

      for (auto& enmr : ret[array_schema->name()]) {
        array_schema->store_enumeration(enmr);
      }
    }
    return array_schema;
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

    if (incl_enums) {
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
