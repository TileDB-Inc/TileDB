#include "gtest/gtest.h"
#include "c_api.h"

#define ARRAYNAME	"workspace/dense_test_100x100_10x10"

class ArrayTest: public testing::Test {

	virtual void SetUp() {
		int ret = 0;
		ret = system("mkdir -p workspace");
		ret = system("touch workspace/__tiledb_group.tdb");
		ret = system("touch workspace/__tiledb_workspace.tdb");
	}

	virtual void TearDown() {
		int ret = system("rm -rf workspace");
	}

	//ArrayConstCellIterator<int>* it_;
};

/**
 * Test whether all private class members
 * are initialized by the default constructor
 */
TEST_F(ArrayTest, DefaultConstructor)
{
	/* Initialize context with the default configuration parameters. */
	  TileDB_CTX* tiledb_ctx;
	  tiledb_ctx_init(&tiledb_ctx, NULL);

	 /*
	  * Prepare the array schema struct, initializing all numeric members to 0
	  * and pointers to NULL.
	  */
	  TileDB_ArraySchema array_schema = {};

	  /* Set array name to "dense_A", inside (existing) workspace "my_workspace". */
	  array_schema.array_name_  = ARRAYNAME;

	  /* Set attributes and number of attributes. */
	  const char* attributes[] = { "a1" };
	  array_schema.attributes_ = attributes;
	  array_schema.attribute_num_ = 1;

	  /* Set capacity. */
	  //array_schema.capacity_ = 20000;

	  /* Set cell order. */
	  array_schema.cell_order_ = "row-major";

	  /* Set dimensions and number of dimensions. */
	  const char* dimensions[] = { "d1", "d2" };
	  array_schema.dimensions_ = dimensions;
	  array_schema.dim_num_ = 2;

	  /*
	   * Set types: **int32** for "a1", **float32** for "a2" and **int64** for the
	   * coordinates.
	   */
	  const char* types[] = { "int32", "int64" };
	  array_schema.types_ = types;

	  /* Set domain to [0,99], [0,99]. */
	  const int64_t domain[] = { 0, 99, 0, 99 };
	  array_schema.domain_ = domain;

	  /* The array has regular, 10x10 tiles. */
	  const int64_t tile_extents[] = { 10, 10 };
	  array_schema.tile_extents_ = tile_extents;

	  /* The array is dense. */
	  array_schema.dense_ = 1;

	  /* Compression for "a1" is GZIP, none for the rest. */

	  const char* compression[] = { "NONE", "NONE" };
	  array_schema.compression_ = compression;

	  /*
	   * NOTE: The rest of the array schema members will be set to default values.
	   * This implies that the array has "row-major" tile order, no compression,
	   * and consolidation step equal to 1.
	   */

	  /* Create the array. */
	  tiledb_array_create(tiledb_ctx, &array_schema);

	  /* Finalize context. */
	  tiledb_ctx_finalize(tiledb_ctx);

//	EXPECT_EQ(it_->array, NULL);
//	EXPECT_EQ(it_->cell_, NULL);
//	EXPECT_EQ(it_->cell_its_, NULL);
//	EXPECT_EQ(it_->end_, false);
//	EXPECT_EQ(it_->range_, NULL);
//	EXPECT_EQ(it_->tile_its_, NULL);
//	EXPECT_EQ(it_->full_overlap_, NULL);
//	EXPECT_EQ(it_->is_del_, false);
//	EXPECT_EQ(it_->var_size_, false);
//	EXPECT_EQ(it_->cell_size_, 0);
}

/**
 * Test the increment operator ++
 */
TEST_F(ArrayTest, IncrementOperator) {

	//it_ = new ArrayConstCellIterator<int>();


}
