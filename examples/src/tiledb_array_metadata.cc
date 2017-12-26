/**
 * @file   tiledb_array_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This example explores the C API for the array metadata.
 *
 * Simply run the following to make it work.
 *
 * $ ./tiledb_array_metadata
 */

#include <tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Create array metadata
  tiledb_array_metadata_t* array_metadata;
  tiledb_array_metadata_create(ctx, &array_metadata, "my_array");

  // Print array metadata contents
  printf("First dump:\n");
  tiledb_array_metadata_dump(ctx, array_metadata, stdout);

  // Set some values
  tiledb_array_metadata_set_array_type(ctx, array_metadata, TILEDB_SPARSE);
  tiledb_array_metadata_set_tile_order(ctx, array_metadata, TILEDB_ROW_MAJOR);
  tiledb_array_metadata_set_cell_order(ctx, array_metadata, TILEDB_COL_MAJOR);
  tiledb_array_metadata_set_capacity(ctx, array_metadata, 10);
  tiledb_array_metadata_set_coords_compressor(
      ctx, array_metadata, TILEDB_ZSTD, 4);
  tiledb_array_metadata_set_offsets_compressor(
      ctx, array_metadata, TILEDB_BLOSC, 5);

  // Print array metadata contents again
  printf("\nSecond dump:\n");
  tiledb_array_metadata_dump(ctx, array_metadata, stdout);

  // Create dimensions
  int d1_domain[] = {0, 1000};
  int d1_extent = 10;
  tiledb_dimension_t* d1;
  tiledb_dimension_create(ctx, &d1, "", TILEDB_INT32, d1_domain, &d1_extent);
  uint64_t d2_domain[] = {100, 10000};
  uint64_t d2_extent = 100;
  tiledb_dimension_t* d2;
  tiledb_dimension_create(ctx, &d2, "d2", TILEDB_UINT64, d2_domain, &d2_extent);

  // Set domain
  tiledb_domain_t* domain;
  tiledb_domain_create(ctx, &domain, TILEDB_UINT64);
  tiledb_domain_add_dimension(ctx, domain, d1);
  tiledb_domain_add_dimension(ctx, domain, d2);
  tiledb_array_metadata_set_domain(ctx, array_metadata, domain);

  // Add attributes
  tiledb_attribute_t *a1, *a2;
  tiledb_attribute_create(ctx, &a1, "", TILEDB_INT32);
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_FLOAT32);
  tiledb_attribute_set_cell_val_num(ctx, a1, 3);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_GZIP, -1);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a1);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a2);

  // Print array metadata contents again
  printf("\nThird dump:\n");
  tiledb_array_metadata_dump(ctx, array_metadata, stdout);

  // Get some values using getters
  const char* array_name;
  tiledb_array_type_t array_type;
  uint64_t capacity;
  tiledb_compressor_t coords_compressor, offsets_compressor;
  int coords_compression_level, offsets_compression_level;
  tiledb_layout_t tile_order, cell_order;
  tiledb_array_metadata_get_array_name(ctx, array_metadata, &array_name);
  tiledb_array_metadata_get_array_type(ctx, array_metadata, &array_type);
  tiledb_array_metadata_get_capacity(ctx, array_metadata, &capacity);
  tiledb_array_metadata_get_tile_order(ctx, array_metadata, &tile_order);
  tiledb_array_metadata_get_cell_order(ctx, array_metadata, &cell_order);
  tiledb_array_metadata_get_coords_compressor(
      ctx, array_metadata, &coords_compressor, &coords_compression_level);
  tiledb_array_metadata_get_offsets_compressor(
      ctx, array_metadata, &offsets_compressor, &offsets_compression_level);

  // Print from getters
  printf("\nFrom getters:\n");
  printf("- Array name: %s\n", array_name);
  printf(
      "- Array type: %s\n", (array_type == TILEDB_DENSE) ? "dense" : "sparse");
  printf(
      "- Cell order: %s\n",
      (cell_order == TILEDB_ROW_MAJOR) ? "row-major" : "col-major");
  printf(
      "- Tile order: %s\n",
      (tile_order == TILEDB_ROW_MAJOR) ? "row-major" : "col-major");
  printf("- Capacity: %llu\n", (unsigned long long)capacity);
  printf(
      "- Coordinates compressor: %s\n",
      (coords_compressor == TILEDB_ZSTD) ? "ZSTD" : "error");
  printf("- Coordinates compression level: %d\n", coords_compression_level);
  printf(
      "- Offsets compressor: %s\n",
      (offsets_compressor == TILEDB_BLOSC) ? "BLOSC" : "error");
  printf("- Offsets compression level: %d\n", offsets_compression_level);

  // Print the attribute names using iterators
  printf("\nArray metadata attribute names: \n");

  unsigned int nattr = 0;
  tiledb_array_metadata_get_num_attributes(ctx, array_metadata, &nattr);

  tiledb_attribute_t* attr = nullptr;
  const char* attr_name = nullptr;
  for (unsigned int i = 0; i < nattr; i++) {
    tiledb_attribute_from_index(ctx, array_metadata, i, &attr);
    tiledb_attribute_get_name(ctx, attr, &attr_name);
    printf("* %s\n", attr_name);
    tiledb_attribute_free(ctx, attr);
  }
  printf("\n");

  // Get and print domain
  tiledb_domain_t* got_domain;
  tiledb_array_metadata_get_domain(ctx, array_metadata, &got_domain);
  tiledb_domain_dump(ctx, got_domain, stdout);

  // Print the dimension names using iterators
  printf("\nArray metadata dimension names: \n");
  unsigned int rank = 0;
  tiledb_domain_get_rank(ctx, domain, &rank);

  tiledb_dimension_t* dim = nullptr;
  const char* dim_name = nullptr;
  for (unsigned int i = 0; i < rank; i++) {
    tiledb_dimension_from_index(ctx, domain, i, &dim);
    tiledb_dimension_get_name(ctx, dim, &dim_name);
    printf("* %s\n", dim_name);
    tiledb_dimension_free(ctx, dim);
  }
  printf("\n");

  // Clean up
  tiledb_attribute_free(ctx, a1);
  tiledb_attribute_free(ctx, a2);
  tiledb_dimension_free(ctx, d1);
  tiledb_dimension_free(ctx, d2);
  tiledb_domain_free(ctx, domain);
  tiledb_domain_free(ctx, got_domain);
  tiledb_array_metadata_free(ctx, array_metadata);
  tiledb_ctx_free(ctx);
  return 0;
}
