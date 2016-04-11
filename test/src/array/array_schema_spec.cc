/**
 * Copyright (c) 2016  Massachusetts Institute of Technology and Intel Corp.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * Tests to check array schema are serialized/deserialized correctly
 * to/from the array storage
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include "c_api.h"

class ArraySchemaTest: public testing::Test {

public:
  const std::string WORKSPACE = ".__workspace/";
  const std::string ARRAYNAME = "dense_test_100x100_10x10";

  // Array schema object under test
  TileDB_ArraySchema test_schema;
  // TileDB context
  TileDB_CTX* tiledb_ctx;
  // Array name is initialized with the workspace folder
  std::string array_name;

  int create_dense_array();

  virtual void SetUp() {
    // Initialize context with the default configuration parameters
    tiledb_ctx_init(&tiledb_ctx, NULL);

    if (tiledb_workspace_create(
    	  tiledb_ctx,
		  WORKSPACE.c_str()) != TILEDB_OK) {
      exit(EXIT_FAILURE);
    }

    array_name.append(WORKSPACE);
    array_name.append(ARRAYNAME);
  }

  virtual void TearDown() {
    // Finalize TileDB context
    tiledb_ctx_finalize(tiledb_ctx);

    // Remove the temporary workspace
    std::string command = "rm -rf ";
    command.append(WORKSPACE);
    int rc = system(command.c_str());
    ASSERT_EQ(rc, 0);
  }
};

int ArraySchemaTest::create_dense_array() {
  const char* attributes[] = { "ATTR_INT32" };
  const char* dimensions[] = { "X", "Y" };
  int64_t domain[] = { 0, 99, 0, 99 };
  int64_t tile_extents[] = { 10, 10 };
  const int types[] = { TILEDB_INT32, TILEDB_INT64 };
  const int compression[] =
  { TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION };

  tiledb_array_set_schema(
      // The array schema structure
      &test_schema,
      // Array name
      array_name.c_str(),
      // Attributes
      attributes,
      // Number of attributes
      1,
      // Capacity
      1000,
      // Cell order
      TILEDB_COL_MAJOR,
      // Number of cell values per attribute (NULL means 1 everywhere)
      NULL,
      // Compression
      compression,
      // Dense array
      1,
      // Dimensions
      dimensions,
      // Number of dimensions
      2,
      // Domain
      domain,
      // Domain length in bytes
      100*sizeof(int64_t),
      // Tile extents (no regular tiles defined)
      tile_extents,
      // Tile extents in bytes
      10*sizeof(int64_t),
      // Tile order (0 means ignore in sparse arrays and default in dense)
      0,
      // Types
      types
  );

  /* Create the array. */
  return tiledb_array_create(tiledb_ctx, &test_schema);
}

/***************************/
/********** TESTS **********/
/***************************/
TEST_F(ArraySchemaTest, DenseSchemaTest) {

  if (create_dense_array() != TILEDB_OK) {
    EXPECT_EQ(0, 1);
  }

  TileDB_ArraySchema schemaFromDisk;
  tiledb_array_load_schema(tiledb_ctx, array_name.c_str(), &schemaFromDisk);

  const int size = 1024;
  char *cwd = new char [size];
  char *ptr = getcwd(cwd, size);

  // If error reading the current working directory, fail the test
  if (ptr == NULL) {
	ASSERT_EQ(0, 1);
  }

  std::string full_array_path;
  full_array_path.append(cwd);
  full_array_path.append("/");
  full_array_path.append(test_schema.array_name_);

  ASSERT_STREQ(schemaFromDisk.array_name_, full_array_path.c_str());

  ASSERT_EQ(schemaFromDisk.attribute_num_, test_schema.attribute_num_);
  ASSERT_EQ(schemaFromDisk.dim_num_, test_schema.dim_num_);
  ASSERT_EQ(schemaFromDisk.capacity_, test_schema.capacity_);
  ASSERT_EQ(schemaFromDisk.cell_order_, test_schema.cell_order_);
  ASSERT_EQ(schemaFromDisk.tile_order_, test_schema.tile_order_);
  ASSERT_EQ(schemaFromDisk.dense_, test_schema.dense_);
  ASSERT_STREQ(schemaFromDisk.attributes_[0], test_schema.attributes_[0]);

  ASSERT_EQ(schemaFromDisk.compression_[0], test_schema.compression_[0]);
  ASSERT_EQ(schemaFromDisk.compression_[1], test_schema.compression_[1]);

  ASSERT_EQ(schemaFromDisk.types_[0], test_schema.types_[0]);
  ASSERT_EQ(schemaFromDisk.types_[1], test_schema.types_[1]);

  int* lhs_tile_extents = static_cast<int*>(schemaFromDisk.tile_extents_);
  int* rhs_tile_extents = static_cast<int*>(test_schema.tile_extents_);

  ASSERT_EQ(lhs_tile_extents[0], rhs_tile_extents[0]);

  // Free array schema
  tiledb_array_free_schema(&schemaFromDisk);
}


