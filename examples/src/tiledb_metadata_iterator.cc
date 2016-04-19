/**
 * @file   tiledb_metadata_iterator.cc
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
 * It shows how to use a metadata iterator.
 */

#include "c_api.h"
#include <cstdio>

int main() {
  /* Initialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Subset over the attributes
  const char* attributes[] = { TILEDB_KEY };

  // Prepare cell buffers
  size_t buffer_key[8];
  char buffer_key_var[500];
  void* buffers[] = { buffer_key, buffer_key_var };
  size_t buffer_sizes[] = { sizeof(buffer_key), sizeof(buffer_key_var) };

  // Initialize metadata iterator
  TileDB_MetadataIterator* tiledb_metadata_iterator;
  tiledb_metadata_iterator_init(
      tiledb_ctx,                                    // Context
      &tiledb_metadata_iterator,                     // Metadata iterator
      "my_workspace/sparse_arrays/my_array_B/meta",  // Metadata name
      attributes,                                    // Attributes
      1,                                             // Number of attributes
      buffers,                                       // Buffers for internal use
      buffer_sizes);                                 // Sizes of buffers

  // Iterate over the metadata
  const char* key;
  size_t key_size;
  while(!tiledb_metadata_iterator_end(tiledb_metadata_iterator)) {
    // Get value 
    tiledb_metadata_iterator_get_value(
        tiledb_metadata_iterator,                    // Metadata iterator
        0,                                           // Attribute id
        (const void**) &key,                         // Key
        &key_size);                                  // Key size

    // Print only if it is not empty
    if(key[0] != TILEDB_EMPTY_CHAR)
      printf("%s\n", key);

    // Advance iterator
    tiledb_metadata_iterator_next(tiledb_metadata_iterator);
  }

  // Finalize metadata iterator
  tiledb_metadata_iterator_finalize(tiledb_metadata_iterator);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
