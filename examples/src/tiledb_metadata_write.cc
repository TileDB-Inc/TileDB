/*
 * File: tiledb_metadata_write.cc 
 *
 * Writing to a TileDB metadata structure. 
 */

#include "tiledb.h"
#include <stdlib.h>
#include <string.h>

int main() {
  /* Intialize context. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx);

  /* Open metadata. */
  int md = tiledb_metadata_open(
               tiledb_ctx, 
               "my_workspace/A/meta", 
               TILEDB_METADATA_MODE_WRITE);

  /* 
   * Prepare a metadata entry. 
   * We assume that the metadata has two attributes:
   * a1 that is "int32:3" and a2 that is "float32:var".
   * We will add the following metadata entry: ("stavros", ((1,2,3), (0.1,0.2)))
   * See the TileDB Mechanics 101 tutorial for detailed information
   * on the binary format of the value of a metadata entry.
   */
  const char* key = "stavros";
  int a1[3] = {1, 2, 3};
  float a2[2] = {0.1, 0.2};
  size_t value_size = 3*sizeof(int) + 2*sizeof(float) + sizeof(int) + sizeof(size_t);
  int a2_val_num = 2;
  void* value = malloc(value_size);
  memcpy(value, &value_size, sizeof(size_t));
  memcpy((char*) value + sizeof(size_t), a1, 3*sizeof(int));
  memcpy((char*) value + sizeof(size_t) + 3*sizeof(int), &a2_val_num, sizeof(int));
  memcpy((char*) value + sizeof(size_t) + 4*sizeof(int), a2, 2*sizeof(float));

  /* Write to metadata. */
  tiledb_metadata_write(tiledb_ctx, md, key, value);

  /* Close metadata. */
  tiledb_metadata_close(tiledb_ctx, md);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  /* Clean up. */
  free(value);

  return 0;
}
