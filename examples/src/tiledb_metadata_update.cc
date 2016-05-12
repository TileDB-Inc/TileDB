/**
 * @file   tiledb_metadata_update.cc
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
 * It shows how to modify or delete a metadata item.
 */

#include "c_api.h"

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize metadata
  TileDB_Metadata* tiledb_metadata;
  tiledb_metadata_init(
      tiledb_ctx,                                    // Context
      &tiledb_metadata,                              // Metadata object
      "my_workspace/sparse_arrays/my_array_B/meta",  // Metadata name
      TILEDB_METADATA_WRITE,                         // Mode
      NULL,                                          // All attributes
      0);                                            // Number of attributes 

  // Prepare cell buffers
  int buffer_a1[] = { 100, TILEDB_EMPTY_INT32 };
  size_t buffer_a2[] = { 0, 1 };
  char buffer_var_a2[] = { 'A', TILEDB_EMPTY_CHAR };
  char keys[] = "k1\0k2";
  size_t buffer_keys[] = { 0, 3 };
  char buffer_var_keys[] = 
  {
       'k', '1', '\0',                               // k1 modified key value
        TILEDB_EMPTY_CHAR                            // k2 deleted value
  };
  const void* buffers[] = 
  { 
      buffer_a1,                                     // a1
      buffer_a2, buffer_var_a2,                      // a2
      buffer_keys, buffer_var_keys                   // TILEDB_KEY
  };         
  size_t buffer_sizes[] = { 
      sizeof(buffer_a1),                             // a1
      sizeof(buffer_a2), sizeof(buffer_var_a2),      // a2 
      sizeof(buffer_keys), sizeof(buffer_var_keys)   // TILEDB_KEY
  };
  size_t keys_size = sizeof(keys);

  // Write metadata
  tiledb_metadata_write(
      tiledb_metadata,                               // Metadata object
      keys,                                          // Keys
      keys_size,                                     // Keys size
      buffers,                                       // Attribute buffers
      buffer_sizes);                                 // Attribute buffer sizes

  // Finalize the metadata
  tiledb_metadata_finalize(tiledb_metadata);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
