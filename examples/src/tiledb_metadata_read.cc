/* 
 * File: tiledb_metadata_read.cc
 *
 * Reading from a TileDB metadata structure. Follows the example of
 * "tiledb_metadata_write.cc"
 */

#include "tiledb.h"
#include <stdlib.h>
#include <iostream>

int main() {
  /* Intialize context. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx);

  /* Open metadata. */
  int md = tiledb_metadata_open(
               tiledb_ctx, 
               "my_workspace/A/meta", 
               TILEDB_METADATA_MODE_READ);

  /* 
   * Prepare to read only the first attribute of the entry with key "stavros".
   * The returned value will have a fixed size equal to 3 integers.
   */
  size_t value_size = 3*sizeof(int);
  void* value = malloc(value_size);
  int attribute_num = 1;
  const char** attributes = new const char*[attribute_num];
  attributes[0] = "a1";

  /* Read the entry. */
  tiledb_metadata_read(
      tiledb_ctx, 
      md,
      "stavros", 
      attributes, 
      attribute_num,
      value,
      &value_size);

  /* Print to stdout. */
  std::cout << "For key \"stavros\", a1 = (" 
            << ((int*) value)[0] << "," << 
               ((int*) value)[1] << "," << 
               ((int*) value)[2] << ")\n";

  /* Read a non-existing entry with key "stavros papapadopoulos". */
  tiledb_metadata_read(
      tiledb_ctx, 
      md,
      "stavros papadopoulos", 
      attributes, 
      attribute_num,
      value,
      &value_size);

  /* The entry does not exist. */
  if(value_size == 0) 
    std::cout << "Entry with key \"stavros papadopoulos\" does not exist!\n";

  /* Close metadata. */
  tiledb_metadata_close(tiledb_ctx, md);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  /* Clean up. */
  free(value);
  delete [] attributes;

  return 0;
}
