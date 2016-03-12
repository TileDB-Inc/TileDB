/*
 * Demonstrates how to read from dense array "workspace/A".
 */

#include "c_api.h"

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  tiledb_delete(tiledb_ctx, "workspace_2");

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
