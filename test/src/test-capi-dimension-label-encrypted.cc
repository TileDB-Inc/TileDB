/**
 * @file test-capi-dimension_labels.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests the DimensionLabel API with an encrypted array
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"

using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "C-API: create encrypted array with a dimension label",
    "[capi][ArraySchema][DimensionLabel][encryption]") {
  // Create an array schema and add a dimension label.
  uint64_t x_domain[2]{0, 63};
  uint64_t x_tile_extent{64};
  uint64_t y_domain[2]{0, 63};
  uint64_t y_tile_extent{64};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_DENSE,
      {"x", "y"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&x_domain[0], &y_domain[0]},
      {&x_tile_extent, &y_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);
  require_tiledb_ok(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "label", TILEDB_INCREASING_DATA, TILEDB_FLOAT64));
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));

  // Set the encryption type and key on a new context.
  std::string encryption_type{"AES_256_GCM"};
  std::string encryption_key{"0123456789abcdeF0123456789abcdeF"};
  tiledb_ctx_t* ctx_encrypt;
  alloc_encrypted_ctx(encryption_type, encryption_key, &ctx_encrypt);

  // Create array.
  auto array_name = fullpath("encrypted_array_with_label");
  SECTION("Create array with current API") {
    require_tiledb_ok(
        tiledb_array_create(ctx_encrypt, array_name.c_str(), array_schema));
    tiledb_array_schema_free(&array_schema);
  }
  SECTION("Create array with deprecated API") {
    tiledb_encryption_type_t encryption_type{TILEDB_AES_256_GCM};
    std::string encryption_key{"0123456789abcdeF0123456789abcdeF"};
    const uint32_t encryption_key_length{
        static_cast<uint32_t>(strlen(encryption_key.c_str()))};
    require_tiledb_ok(tiledb_array_create_with_key(
        ctx,
        array_name.c_str(),
        array_schema,
        encryption_type,
        encryption_key.c_str(),
        encryption_key_length));
    tiledb_array_schema_free(&array_schema);
  }

  // Check the array schema cannot be loaded without the encryption key.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  check_tiledb_error_with(
      tiledb_array_schema_load(ctx, array_name.c_str(), &loaded_array_schema),
      "[TileDB::TileIO] Error: Error reading generic tile; tile is encrypted "
      "with AES_256_GCM but given key is for NO_ENCRYPTION");

  // Check the array schema can be loaded with the encryption key.
  tiledb::test::require_tiledb_ok(
      ctx_encrypt,
      tiledb_array_schema_load(
          ctx_encrypt, array_name.c_str(), &loaded_array_schema));

  // Get the URI for the dimension label array schema.
  auto dim_label_ref =
      loaded_array_schema->array_schema_->dimension_label_reference("label");
  auto dim_label_uri = dim_label_ref.uri(URI(array_name));

  // Check the dimension label array cannot be loaded without the encryption
  // key.
  tiledb_array_schema_t* loaded_label_array_schema{nullptr};
  require_tiledb_error_with(
      tiledb_array_schema_load(
          ctx, dim_label_uri.c_str(), &loaded_label_array_schema),
      "[TileDB::TileIO] Error: Error reading generic tile; tile is encrypted "
      "with AES_256_GCM but given key is for NO_ENCRYPTION");

  // Open the dimension label array schema and check the tile extent.
  tiledb::test::require_tiledb_ok(
      ctx_encrypt,
      tiledb_array_schema_load(
          ctx_encrypt, dim_label_uri.c_str(), &loaded_label_array_schema));

  // Free remaining resources.
  tiledb_array_schema_free(&loaded_array_schema);
  tiledb_array_schema_free(&loaded_label_array_schema);
  tiledb_ctx_free(&ctx_encrypt);
}
