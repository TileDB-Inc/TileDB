/**
 * @file   file_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class FileSchema.
 */

#ifndef TILEDB_FILE_SCHEMA_H
#define TILEDB_FILE_SCHEMA_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class CompressionFilter;

enum class ArrayType : uint8_t;

/** Specifies the file array schema. */
class FileSchema : public ArraySchema {
  static const uint64_t default_extent = 1024;

  constexpr static const std::array<uint64_t, 2> default_domain = {
      0, std::numeric_limits<uint64_t>::max() - 1};

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  FileSchema();

  /** Constructor. */
  FileSchema(ArrayType array_type);

  /**
   * Constructor. Clones the input.
   *
   * @param array_schema The array schema to copy.
   */
  explicit FileSchema(const FileSchema* file_schema);

  /** Destructor. */
  ~FileSchema();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Set the array schema details based on heuristics from the files
   *
   * @param file_size size of the original file used to determine tile_extent
   * size
   * @param file_compressed bool for if the original file is compressed, sets
   * noop for attribute if so
   */
  void set_schema_based_on_file_details(
      const uint64_t file_size, const bool file_compressed);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * Create domain to store position
   * @param tile_extent
   * @return Domain
   */
  static Domain create_domain(const uint64_t tile_extent = default_extent);

  /**
   * Create attribute to store file data
   * @param fp
   * @return attribute
   */
  static tiledb_shared_ptr<Attribute> create_attribute(
      const FilterPipeline& fp);

  /**
   * Compute tile extents based on the size of the file
   * @param file_size
   * @return extent based on defined heuristics
   */
  static uint64_t compute_tile_extent_based_on_file_size(
      const uint64_t file_size);

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILE_SCHEMA_H
