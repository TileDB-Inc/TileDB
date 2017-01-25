/**
 * @file   tiledb_metadata_read.cc
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
 * It shows how to read from metadata.
 */

#include "tiledb.h"
#include <cstdio>

int main(int argc, char** argv) {
  // Sanity check
  if(argc != 2) {
    fprintf(stderr, "Usage: ./tiledb_metadata_read key\n");
    return -1;
  }

  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Subset over attributes
  const char* attributes[] = { "a1", "a2" };

  // Initialize metadata
  TileDB_Metadata* tiledb_metadata;
  tiledb_metadata_init(
      tiledb_ctx,                                    // Context
      &tiledb_metadata,                              // Metadata object
      "my_workspace/sparse_arrays/my_array_B/meta",  // Metadata name
      TILEDB_METADATA_READ,                          // Mode
      attributes,                                    // Attributes
      2);                                            // Number of attributes

  // Prepare cell buffers
  int buffer_a1[1];
  size_t buffer_a2[1];
  char buffer_var_a2[2];
  void* buffers[] = 
  { 
      buffer_a1,                                     // a1 
      buffer_a2, buffer_var_a2                       // a2
  };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),                             // a1 
      sizeof(buffer_a2), sizeof(buffer_var_a2)       // a2
  };

  // Read from metadata
  tiledb_metadata_read(tiledb_metadata, argv[1], buffers, buffer_sizes); 
 
  // Check existence
  if(buffer_sizes[0] == 0 && !tiledb_metadata_overflow(tiledb_metadata, 0)) {
    fprintf(stderr, "Key '%s' does not exist in the metadata!\n", argv[1]);
  }  else if(buffer_sizes[2] == 0 && 
             tiledb_metadata_overflow(tiledb_metadata, 1)) {
    // Check overflow for a2 
    fprintf(stderr, "Reading value on attribute 'a2' for key '%s' resulted in "
            "a buffer overflow!\n", argv[1]);
  } else if(static_cast<int*>(buffers[0])[0] == TILEDB_EMPTY_INT32) {
    // Check if deleted
    fprintf(stderr, "Key '%s' has been deleted!\n", argv[1]);
  } else {
    // Print attribute values
    printf(
        "%s: a1=%d, a2=%.*s\n", 
        argv[1], 
        static_cast<int*>(buffers[0])[0],
        int(buffer_sizes[2]),
        static_cast<char*>(buffers[2]));
  }

  /* Finalize the array. */
  tiledb_metadata_finalize(tiledb_metadata);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
