/**
 * @file status_code.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 */
#ifndef TILEDB_STATUS_CODE_H
#define TILEDB_STATUS_CODE_H

#include <string>

namespace tiledb::common {

/**
 * StatusCode is a legacy identifier for the area of code where an error arises.
 *
 * This class has exactly one use: to identify a string constant used to print
 * status messages. It could have been `const char*` for all it does.
 */
enum class StatusCode : char {
  Ok,
  Error,
  StorageManager,
  FragmentMetadata,
  ArraySchema,
  ArraySchemaEvolution,
  Metadata,
  IO,
  Mem,
  GZip,
  Compression,
  Tile,
  TileIO,
  Buffer,
  Query,
  ValidityVector,
  VFS,
  ConstBuffer,
  Dimension,
  Domain,
  Consolidator,
  LRUCache,
  KV,
  KVItem,
  KVIter,
  Config,
  Utils,
  FS_S3,
  FS_AZURE,
  FS_GCS,
  FS_HDFS,
  FS_MEM,
  Attribute,
  WriteCellSlabIter,
  SparseGlobalOrderReaderError,
  SparseUnorderedWithDupsReaderError,
  DenseReaderError,
  Reader,
  Writer,
  PreallocatedBuffer,
  Filter,
  Encryption,
  Array,
  VFSFileHandleError,
  ContextError,
  SubarrayError,
  SubarrayPartitionerError,
  RTreeError,
  CellSlabIterError,
  RestError,
  SerializationError,
  ChecksumError,
  ThreadPoolError,
  FragmentInfoError,
  DenseTilerError,
  QueryConditionError
};

std::string to_string(const StatusCode& sc);
std::string_view to_string_view(const StatusCode& sc);

}  // namespace tiledb::common

#endif  // TILEDB_STATUS_CODE_H
