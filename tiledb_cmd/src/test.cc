#include "tiledb.h"
#include <iostream>
#include <string.h>

int main() {
  TileDB_CTX* ctx;
  tiledb_init("example/", ctx);

  int ad = tiledb_open_array(ctx, "IREG", "r"); 

  TileDB_ConstCellIterator* it;
  tiledb_const_cell_iterator_init(ctx, ad, NULL, 0, it);  

  const void* cell;
  int64_t c;
  tiledb_const_cell_iterator_next(it, cell);
  while(cell != NULL) {
    // Print the first coordinate of each cell
    memcpy(&c, cell, sizeof(int64_t));
    std::cout << c << "\n";
    tiledb_const_cell_iterator_next(it, cell);
  }

  tiledb_const_cell_iterator_finalize(it);  
  tiledb_close_array(ctx, ad);

  tiledb_finalize(ctx);

  return 0;
}
