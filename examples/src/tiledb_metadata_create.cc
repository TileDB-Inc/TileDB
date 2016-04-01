/**
 * @file   tiledb_metadata_create.cc
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
 * It creates a metadata object.
 */

#include "c_api.h"

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Prepare parameters for metadata schema
  const char* metadata_name = "my_workspace/sparse_arrays/my_array_B/meta";
  const char* attributes[] = { "a1", "a2" }; // Two attributes
  const int cell_val_num[] = 
  {
      1,                           // a1
      TILEDB_VAR_NUM               // a2
  };
  const int compression[] = 
  { 
        TILEDB_GZIP,              // a1 
        TILEDB_GZIP,              // a2
        TILEDB_NO_COMPRESSION     // TILEDB_KEY
  };
  const int types[] = 
  { 
      TILEDB_INT32,                // a1
      TILEDB_CHAR                  // a2
  };

  // Set metadata schema
  TileDB_MetadataSchema metadata_schema;
  tiledb_metadata_set_schema(
      &metadata_schema,            // Metadata schema struct
      metadata_name,               // Metadata name
      attributes,                  // Attributes
      2,                           // Number of attributes
      4,                           // Capacity
      cell_val_num,                // Number of cell values per attribute  
      compression,                 // Compression
      types                        // Types
  );

  // Create metadata
  tiledb_metadata_create(tiledb_ctx, &metadata_schema); 

  // Free metadata schema
  tiledb_metadata_free_schema(&metadata_schema);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
