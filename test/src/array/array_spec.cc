#include "gtest/gtest.h"
#include "array/array.h"

class ArrayTest: public testing::Test {

	virtual void SetUp() {
		 it_ = NULL;
	}

	virtual void TearDown() {
		delete it_;
	}

	ArrayConstCellIterator<int>* it_;
};

/**
 * Test whether all private class members
 * are initialized by the default constructor
 */
TEST_F(ArrayTest, DefaultConstructor)
{
	it_ = new ArrayConstCellIterator<int>();

	EXPECT_EQ(it_->array, NULL);
	EXPECT_EQ(it_->cell_, NULL);
	EXPECT_EQ(it_->cell_its_, NULL);
	EXPECT_EQ(it_->end_, false);
	EXPECT_EQ(it_->range_, NULL);
	EXPECT_EQ(it_->tile_its_, NULL);
	EXPECT_EQ(it_->full_overlap_, NULL);
	EXPECT_EQ(it_->is_del_, false);
	EXPECT_EQ(it_->var_size_, false);
	EXPECT_EQ(it_->cell_size_, 0);
}

/**
 * Test the increment operator ++
 */
TEST_F(ArrayTest, IncrementOperator) {

	it_ = new ArrayConstCellIterator<int>();


}
