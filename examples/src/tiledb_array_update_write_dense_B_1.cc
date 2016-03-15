/*
 * Demonstrates how to write to dense array "workspace/A", in dense mode.
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_B",
      TILEDB_ARRAY_WRITE,
      NULL,            // No range - entire domain
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[64];
  size_t buffer_a2[] = 
  {
    0,   2,   5,   9,   14,  20,  27,  35,
    44,  46,  49,  53,  58,  64,  71,  79, 
    88,  90,  93,  97,  102, 108, 115, 123, 
    132, 134, 137, 141, 146, 152, 159, 167,
    176, 178, 181, 185, 190, 196, 203, 211,
    220, 222, 225, 229, 234, 240, 247, 255,
    264, 266, 269, 273, 278, 284, 291, 299,
    308, 310, 313, 317, 322, 328, 335, 343
  };
  char buffer_a2_var[] = 
  {
    "a\0bb\0ccc\0dddd\0eeeee\0ffffff\0ggggggg\0hhhhhhhh\0"
    "i\0jj\0kkk\0llll\0mmmmm\0nnnnnn\0ooooooo\0pppppppp\0"
    "q\0rr\0sss\0tttt\0uuuuu\0vvvvvv\0wwwwwww\0xxxxxxxx\0"
    "y\0zz\0!!!\0@@@@\0#####\0$$$$$$\0^^^^^^^\0********\0"
    "a\0bb\0ccc\0dddd\0eeeee\0ffffff\0ggggggg\0hhhhhhhh\0"
    "i\0jj\0kkk\0llll\0mmmmm\0nnnnnn\0ooooooo\0pppppppp\0"
    "q\0rr\0sss\0tttt\0uuuuu\0vvvvvv\0wwwwwww\0xxxxxxxx\0"
    "y\0zz\0!!!\0@@@@\0#####\0$$$$$$\0^^^^^^^\0********"
  };
  float buffer_a3[128];
  const void* buffers[] = { buffer_a1, buffer_a2, buffer_a2_var, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
    sizeof(buffer_a1) , 
    sizeof(buffer_a2), 
    sizeof(buffer_a2_var),
    sizeof(buffer_a3)
  };

  /* Populate buffers with some arbitrary values. */
  for(int i=0; i<64; ++i) {
    buffer_a1[i] = i;
    buffer_a3[2*i] = i + 0.1;
    buffer_a3[2*i+1] = i + 0.2;
  }

  /* Write to array. */
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
