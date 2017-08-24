/**
 * @file   array.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class Array.
 */

#ifndef TILEDB_ARRAY_H
#define TILEDB_ARRAY_H

#include <queue>

#include "array_schema.h"
#include "config.h"
#include "fragment.h"
#include "fragment_metadata.h"
#include "query.h"

namespace tiledb {

class AIORequest;
class Query;
class StorageManager;

/** Manages a TileDB array object. */
class Array {
 public:
  // TODO: will be remove from here
  Query* query_;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Array();

  /** Destructor. */
  ~Array();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Handles an AIO request. */
  Status aio_handle_request(AIORequest* aio_request);

  /** Returns the storage manager object. */
  StorageManager* storage_manager() const;

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /** Returns the configuration parameters. */
  const Config* config() const;

  Status read(Query* query, void** buffers, size_t* buffer_sizes);

  Status read_default(Query* query, void** buffers, size_t* buffer_sizes);

  Status consolidate(
      Fragment*& new_fragment, std::vector<uri::URI>* old_fragments);

  Status consolidate(Fragment* new_fragment, int attribute_id);

  Status finalize();

  Status init(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      const std::vector<std::string>& fragment_names,
      const std::vector<FragmentMetadata*>& book_keeping,
      QueryMode mode,
      const char** attributes,
      int attribute_num,
      const void* subarray,
      const Config* config);

  Status write(Query* query, const void** buffers, const size_t* buffer_sizes);

  Status write_default(
      Query* query, const void** buffers, const size_t* buffer_sizes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  StorageManager* storage_manager_;
  const Config* config_;
  const ArraySchema* array_schema_;
};

}  // namespace tiledb

#endif  // TILEDB_ARRAY_H
