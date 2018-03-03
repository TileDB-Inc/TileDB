/**
 * @file   tiledb_vfs_write.c
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
 * Write to a file with VFS. Simply run:
 *
 * $ ./tiledb_vfs_write_c
 *
 */

#include <string.h>
#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Create TileDB VFS
  tiledb_vfs_t* vfs;
  tiledb_vfs_create(ctx, &vfs, NULL);

  // Write binary data
  tiledb_vfs_fh_t* fh;
  tiledb_vfs_open(ctx, vfs, "tiledb_vfs.bin", TILEDB_VFS_WRITE, &fh);
  float f1 = 153.0;
  const char* s1 = "abcd";
  tiledb_vfs_write(ctx, fh, &f1, sizeof(float));
  tiledb_vfs_write(ctx, fh, s1, strlen(s1));
  tiledb_vfs_close(ctx, fh);
  tiledb_vfs_fh_free(ctx, &fh);

  // Write binary data again - this will overwrite the previous file
  tiledb_vfs_open(ctx, vfs, "tiledb_vfs.bin", TILEDB_VFS_WRITE, &fh);
  const char* s2 = "abcdef";
  f1 = 153.1;
  tiledb_vfs_write(ctx, fh, &f1, sizeof(float));
  tiledb_vfs_write(ctx, fh, s2, strlen(s2));
  tiledb_vfs_close(ctx, fh);
  tiledb_vfs_fh_free(ctx, &fh);

  // Append binary data to existing file
  tiledb_vfs_open(ctx, vfs, "tiledb_vfs.bin", TILEDB_VFS_APPEND, &fh);
  const char* s3 = "ghijkl";
  tiledb_vfs_write(ctx, fh, s3, strlen(s3));
  tiledb_vfs_close(ctx, fh);
  tiledb_vfs_fh_free(ctx, &fh);

  // Clean up
  tiledb_vfs_free(ctx, &vfs);
  tiledb_ctx_free(&ctx);

  return 0;
}