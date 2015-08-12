#include "tiledb.h"
#include <iostream>
#include <string.h>

int main() {
  TileDB_CTX* ctx;
  tiledb_ctx_init("example/", ctx);

  int ad = tiledb_array_open(ctx, "IREG", "r"); 

  TileDB_ConstReverseCellIterator* it;

  int64_t range[4] = { 0,50, 0,50 };
  tiledb_const_reverse_cell_iterator_init_in_range(ctx, ad, NULL, 0, range, it);

  const void* cell;
  int64_t c;
  tiledb_const_reverse_cell_iterator_next(it, cell);

  while(cell != NULL) {
    // Print the first coordinate of each cell
    memcpy(&c, cell, sizeof(int64_t));
    std::cout << c << "\n";
    tiledb_const_reverse_cell_iterator_next(it, cell);
  }

  tiledb_const_reverse_cell_iterator_finalize(it);  

  tiledb_array_close(ctx, ad);

  tiledb_ctx_finalize(ctx);

  return 0;
}
