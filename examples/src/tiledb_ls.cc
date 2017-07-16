/**
 * @file   tiledb_list.cc
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
 * It shows how to explore the contents of a TileDB directory.
 */

#include "tiledb.h"
#include <cstdio>
#include <cstdlib>

int main(int argc, char** argv) {
  /* TODO: this will be refactored

  // Sanity check
  if(argc != 2) {
    fprintf(stderr, "Usage: ./tiledb_list parent_dir\n");
    return -1;
  }

  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Retrieve number of directories
  int dir_num;
  tiledb_ls_c(ctx, argv[1], &dir_num);

  // Exit if there are not TileDB objects in the input directory_
  if(dir_num == 0)
    return 0;

  // Initialize variables
  char** dirs = new char*[dir_num];
  tiledb_object_t* dir_types = new tiledb_object_t[dir_num];
  for(int i=0; i<dir_num; ++i)
    dirs[i] = (char*) malloc(TILEDB_NAME_MAX_LEN);

  // List TileDB objects
  tiledb_ls(
      ctx,                                    // Context
      argv[1],                                       // Parent directory
      dirs,                                          // Directories
      dir_types,                                     // Directory types
      &dir_num);                                     // Directory number

  // Print TileDB objects
  for(int i=0; i<dir_num; ++i) {
    printf("%s ", dirs[i]);
    if(dir_types[i] == TILEDB_ARRAY)
      printf("ARRAY\n");
    else if(dir_types[i] == TILEDB_METADATA)
      printf("METADATA\n");
    else if(dir_types[i] == TILEDB_GROUP)
      printf("GROUP\n");
  }
 
  // Clean up
  for(int i=0; i<dir_num; ++i)
    free(dirs[i]);
  free(dirs);
  free(dir_types);

  // Finalize context
  tiledb_ctx_free(ctx);

   */

  return 0;
}
