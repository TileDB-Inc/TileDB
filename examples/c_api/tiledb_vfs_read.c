/**
 * @file   tiledb_vfs_read.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Read from a file with VFS.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_vfs_write_c
 * $ ./tiledb_vfs_read_c
 */

#include <tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Create TileDB VFS
  tiledb_vfs_t* vfs;
  tiledb_vfs_create(ctx, &vfs, NULL);

  // Read binary data
  tiledb_vfs_fh_t* fh;
  tiledb_vfs_open(ctx, vfs, "tiledb_vfs.bin", TILEDB_VFS_READ, &fh);
  float f1;
  char s1[13];
  s1[12] = '\0';
  tiledb_vfs_read(ctx, fh, 0, &f1, sizeof(float));
  tiledb_vfs_read(ctx, fh, sizeof(float), s1, 12);
  printf("Binary read:\n%.1f\n%s\n", f1, s1);

  // Clean up
  tiledb_vfs_fh_free(ctx, fh);
  tiledb_vfs_free(ctx, vfs);
  tiledb_ctx_free(ctx);

  return 0;
}