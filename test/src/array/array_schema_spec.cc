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

#include "gtest/gtest.h"
#include "c_api.h"

class ArraySchemaTest: public testing::Test {

	const std::string WORKSPACE	= "/tmp/.__workspace/";
	const std::string ARRAYNAME = "dense_test_100x100_10x10";

public:
	// Array schema object under test
	TileDB_ArraySchema testSchema;
	// TileDB context
	TileDB_CTX* tiledbCtx;
	// Array name is initialized with the workspace folder
	std::string arrayName;

	int create_dense_array();

	virtual void SetUp() {
		// Initialize context with the default configuration parameters
		tiledb_ctx_init(&tiledbCtx, NULL);
		if (tiledb_workspace_create(tiledbCtx, WORKSPACE.c_str()) != TILEDB_OK) {
			exit(EXIT_FAILURE);
		}

		arrayName.append(WORKSPACE);
		arrayName.append(ARRAYNAME);
	}

	virtual void TearDown() {
		// Finalize TileDB context
		tiledb_ctx_finalize(tiledbCtx);

		// Remove the temporary workspace
		std::string command = "rm -rf ";
		command.append(WORKSPACE);
		int ret = system(command.c_str());
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
			&testSchema,
			// Array name
			arrayName.c_str(),
			// Attributes
			attributes,
			// Number of attributes
			1,
			// Dimensions
			dimensions,
			// Number of dimensions
			2,
			// Dense array
			1,
			// Domain
			domain,
			// Domain length in bytes
			100*sizeof(int64_t),
			// Tile extents (no regular tiles defined)
			tile_extents,
			// Tile extents in bytes
			10*sizeof(int64_t),
			// Types
			types,
			// Number of cell values per attribute (NULL means 1 everywhere)
			NULL,
			// Cell order
			TILEDB_COL_MAJOR,
			// Tile order (0 means ignore in sparse arrays and default in dense)
			0,
			// Capacity
			1000,
			// Compression
			compression
	);

	/* Create the array. */
	return tiledb_array_create(tiledbCtx, &testSchema);
}

/***************************/
/********** TESTS **********/
/***************************/
TEST_F(ArraySchemaTest, DenseSchemaTest) {

	if (create_dense_array() != TILEDB_OK) {
		EXPECT_EQ(0, 1);
	}

	TileDB_ArraySchema schemaFromDisk;
	tiledb_array_load_schema(tiledbCtx, arrayName.c_str(), &schemaFromDisk);

	ASSERT_STREQ(schemaFromDisk.array_name_, testSchema.array_name_);

	ASSERT_EQ(schemaFromDisk.attribute_num_, testSchema.attribute_num_);
	ASSERT_EQ(schemaFromDisk.dim_num_, testSchema.dim_num_);
	ASSERT_EQ(schemaFromDisk.capacity_, testSchema.capacity_);
	ASSERT_EQ(schemaFromDisk.cell_order_, testSchema.cell_order_);
	ASSERT_EQ(schemaFromDisk.tile_order_, testSchema.tile_order_);
	ASSERT_EQ(schemaFromDisk.dense_, testSchema.dense_);
	ASSERT_STREQ(schemaFromDisk.attributes_[0], testSchema.attributes_[0]);

	ASSERT_EQ(schemaFromDisk.compression_[0], testSchema.compression_[0]);
	ASSERT_EQ(schemaFromDisk.compression_[1], testSchema.compression_[1]);

	ASSERT_EQ(schemaFromDisk.types_[0], testSchema.types_[0]);
	ASSERT_EQ(schemaFromDisk.types_[1], testSchema.types_[1]);

	int* lhs_tile_extents = static_cast<int*>(schemaFromDisk.tile_extents_);
	int* rhs_tile_extents = static_cast<int*>(testSchema.tile_extents_);

	ASSERT_EQ(lhs_tile_extents[0], rhs_tile_extents[0]);

	// Free array schema
	tiledb_array_free_schema(&schemaFromDisk);
}


