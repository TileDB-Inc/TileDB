/**
 * @file   vfs.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This program explores the various TileDB VFS tools.
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

static void do_dirs_files() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create TileDB VFS
  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, NULL, &vfs);

  // Create directory
  int is_dir = 0;
  tiledb_vfs_is_dir(ctx, vfs, "dir_A", &is_dir);
  if (!is_dir) {
    tiledb_vfs_create_dir(ctx, vfs, "dir_A");
    printf("Created 'dir_A'\n");
  } else {
    printf("'dir_A' already exists\n");
  }

  // Creating an (empty) file
  int is_file = 0;
  tiledb_vfs_is_file(ctx, vfs, "dir_A/file_A", &is_file);
  if (!is_file) {
    tiledb_vfs_touch(ctx, vfs, "dir_A/file_A");
    printf("Created empty file 'dir_A/file_A'\n");
  } else {
    printf("'dir_A/file_A' already exists\n");
  }

  // Getting the file size
  uint64_t file_size;
  tiledb_vfs_file_size(ctx, vfs, "dir_A/file_A", &file_size);
  char format_buf[256];
  snprintf(
      format_buf,
      sizeof(format_buf),
      "File size for 'dir_A/file_A': %%%s\n",
      PRIu64);
  printf(format_buf, file_size);

  // Moving files (moving directories is similar)
  printf("Moving file 'dir_A/file_A' to 'dir_A/file_B'\n");
  tiledb_vfs_move_file(ctx, vfs, "dir_A/file_A", "dir_A/file_B");

  // Deleting files and directories. Note that, in the case of directories,
  // the function will delete all the contents of the directory (i.e., it
  // works even for non-empty directories).
  printf("Deleting 'dir_A/file_B' and 'dir_A'\n");
  tiledb_vfs_remove_file(ctx, vfs, "dir_A/file_B");
  tiledb_vfs_remove_dir(ctx, vfs, "dir_A");

  // Clean up
  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);
}

static void do_write() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create TileDB VFS
  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, NULL, &vfs);

  // Write binary data
  tiledb_vfs_fh_t* fh;
  tiledb_vfs_open(ctx, vfs, "tiledb_vfs.bin", TILEDB_VFS_WRITE, &fh);
  float f1 = 153.0;
  const char* s1 = "abcd";
  tiledb_vfs_write(ctx, fh, &f1, sizeof(float));
  tiledb_vfs_write(ctx, fh, s1, strlen(s1));
  tiledb_vfs_close(ctx, fh);
  tiledb_vfs_fh_free(&fh);

  // Write binary data again - this will overwrite the previous file
  tiledb_vfs_open(ctx, vfs, "tiledb_vfs.bin", TILEDB_VFS_WRITE, &fh);
  const char* s2 = "abcdef";
  f1 = 153.1;
  tiledb_vfs_write(ctx, fh, &f1, sizeof(float));
  tiledb_vfs_write(ctx, fh, s2, strlen(s2));
  tiledb_vfs_close(ctx, fh);
  tiledb_vfs_fh_free(&fh);

  // Append binary data to existing file
  tiledb_vfs_open(ctx, vfs, "tiledb_vfs.bin", TILEDB_VFS_APPEND, &fh);
  const char* s3 = "ghijkl";
  tiledb_vfs_write(ctx, fh, s3, strlen(s3));
  tiledb_vfs_close(ctx, fh);
  tiledb_vfs_fh_free(&fh);

  // Clean up
  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);
}

static void do_read() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create TileDB VFS
  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, NULL, &vfs);

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
  tiledb_vfs_fh_free(&fh);
  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);
}

int main() {
  do_dirs_files();
  do_write();
  do_read();
}
