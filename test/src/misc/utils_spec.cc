/**
 * @file   utils_spec.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for the utility functions spec.
 */

#include "utils_spec.h"


/* ****************************** */
/*        GTEST FUNCTIONS         */
/* ****************************** */

void UtilsTestFixture::SetUp() {
}

void UtilsTestFixture::TearDown() {
}




/* ****************************** */
/*             TESTS              */
/* ****************************** */

/** Tests RLE compression (attribute). */
TEST_F(UtilsTestFixture, test_RLE) {
  // Initializations
  unsigned char input[1000000];
  size_t input_size = 0;
  unsigned char compressed[1000000];
  size_t compressed_size = 0;
  unsigned char decompressed[1000000];
  size_t decompressed_size = 0;
  size_t value_size;
  size_t run_size = 6;
  size_t compress_bound;
  int64_t rc;

  // === Attribute compression (value_size = sizeof(int)) === //
  value_size = sizeof(int);

  // Test empty bufffer


  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  ASSERT_EQ(rc, 0);

  // Test input buffer invalid format
  input_size = 5; 
  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  ASSERT_EQ(rc, TILEDB_UT_ERR);

  // Test output buffer overflow
  input_size = 16; 
  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  ASSERT_EQ(rc, TILEDB_UT_ERR);

  // Test compress bound 
  compress_bound = RLE_compress_bound(input_size, value_size);
  ASSERT_EQ(compress_bound, input_size + ((input_size/value_size) * 2));

  // Test all values unique (many unitary runs)
  for(int i=0; i<100; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  input_size = 100*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  ASSERT_EQ(rc, compressed_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));

  // Test all values the same (a single long run)
  int v = 111;
  for(int i=0; i<100; ++i) 
    memcpy(input + i*value_size, &v, value_size); 
  input_size = 100*value_size;
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  ASSERT_EQ(compressed_size, run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));

  // Test a mix of short and long runs
  for(int i=0; i<10; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  for(int i=10; i<100; ++i) 
    memcpy(input + i*value_size, &v, value_size); 
  for(int i=100; i<110; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  input_size = 110*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  ASSERT_EQ(compressed_size, 21*run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));
  
  // Test when a run exceeds max run length
  for(int i=0; i<10; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  for(int i=10; i<70010; ++i) 
    memcpy(input + i*value_size, &v, value_size); 
  for(int i=70010; i<70030; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  input_size = 70030*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  ASSERT_EQ(compressed_size, 32*run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));

  // === Attribute compression (value_size = 2*sizeof(double)) === //
  value_size = 2*sizeof(double);
  run_size = value_size + 2;

  // Test a mix of short and long runs
  double j = 0.1, k = 0.2;
  for(int i=0; i<10; ++i) { 
    j+= 10000.12;
    memcpy(input + 2*i*sizeof(double), &j, value_size); 
    k+= 1000.12;
    memcpy(input + (2*i+1)*sizeof(double), &k, value_size); 
  }
  j+= 10000.12;
  k+= 1000.12;
  for(int i=10; i<100; ++i) { 
    memcpy(input + 2*i*sizeof(double), &j, value_size); 
    memcpy(input + (2*i+1)*sizeof(double), &k, value_size); 
  }
  for(int i=100; i<110; ++i) { 
    j+= 10000.12;
    memcpy(input + 2*i*sizeof(double), &j, value_size); 
    k+= 1000.12;
    memcpy(input + (2*i+1)*sizeof(double), &k, value_size); 
  }
  input_size = 110*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  ASSERT_EQ(compressed_size, 21*run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));
}

/** Tests RLE compression (coordinates, row-major cell order). */
TEST_F(UtilsTestFixture, test_RLE_coords_row) {
  // Initializations
  unsigned char input[1000000];
  unsigned char compressed[1000000];
  unsigned char decompressed[1000000];
  size_t input_size = 0;
  size_t compressed_size = 0;
  size_t decompressed_size = 0;
  size_t value_size;
  size_t coords_size;
  size_t run_size;
  size_t compress_bound;
  int dim_num = 2;
  int rc;

  // === Coordinates compression (row-major) === //
  value_size = sizeof(int);
  coords_size = dim_num*value_size;
  run_size = sizeof(int) + 2*sizeof(char);

  // Test empty bufffer
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, 0);

  // Test input buffer invalid format
  input_size = 5; 
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_ERR);

  // Test output buffer overflow
  input_size = 16; 
  compressed_size = 0;
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_ERR);

  // Test compress bound 
  input_size = 64;
  compress_bound = RLE_compress_bound_coords(input_size, value_size, dim_num);
  int64_t cell_num = input_size / coords_size;
  size_t compress_bound_expected = 
      input_size + cell_num*(dim_num-1)*2 + sizeof(int64_t);
  ASSERT_EQ(compress_bound, compress_bound_expected);

  // Test all values unique (many unitary runs)
  int v;
  for(int i=0; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, compressed_size);
  decompressed_size = input_size;
  rc = RLE_decompress_coords_row(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));

  // Test all values the same (a single long run)
  v = 111;
  for(int i=0; i<100; ++i) { 
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_row(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  ASSERT_EQ(compressed_size, 100*value_size + run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_row(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));

  // Test a mix of short and long runs
  for(int i=0; i<10; ++i) {
    v = i;
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  v = 111;
  for(int i=10; i<90; ++i) { 
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  for(int i=90; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_row(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  ASSERT_EQ(compressed_size, 100*value_size + 21*run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_row(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));
}

/** Tests RLE compression (coordinates, column-major cell order). */
TEST_F(UtilsTestFixture, test_RLE_coords_col) {
  // Initializations
  unsigned char input[1000000];
  unsigned char compressed[1000000];
  unsigned char decompressed[1000000];
  size_t input_size = 0;
  size_t compressed_size = 0;
  size_t decompressed_size = 0;
  size_t value_size;
  size_t coords_size;
  size_t run_size;
  size_t compress_bound;
  int dim_num = 2;
  int rc;

  // === Coordinates compression (row-major) === //
  value_size = sizeof(int);
  coords_size = dim_num*value_size;
  run_size = sizeof(int) + 2*sizeof(char);

  // Test empty bufffer
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, 0);

  // Test input buffer invalid format
  input_size = 5; 
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_ERR);

  // Test output buffer overflow
  input_size = 16; 
  compressed_size = 0;
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_ERR);

  // Test compress bound 
  input_size = 64;
  compress_bound = RLE_compress_bound_coords(input_size, value_size, dim_num);
  int64_t cell_num = input_size / coords_size;
  size_t compress_bound_expected = 
      input_size + cell_num*(dim_num-1)*2 + sizeof(int64_t);
  ASSERT_EQ(compress_bound, compress_bound_expected);

  // Test all values unique (many unitary runs)
  int v;
  for(int i=0; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  ASSERT_EQ(rc, compressed_size);
  decompressed_size = input_size;
  rc = RLE_decompress_coords_col(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));

  // Test all values the same (a single long run)
  v = 111;
  for(int i=0; i<100; ++i) { 
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_col(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  ASSERT_EQ(compressed_size, 100*value_size + run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_col(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));

  // Test a mix of short and long runs
  for(int i=0; i<10; ++i) {
    v = i;
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  v = 111;
  for(int i=10; i<90; ++i) { 
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  for(int i=90; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_col(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  ASSERT_EQ(compressed_size, 100*value_size + 21*run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_col(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  ASSERT_EQ(rc, TILEDB_UT_OK);
  ASSERT_FALSE(memcmp(input, decompressed, input_size));
}
