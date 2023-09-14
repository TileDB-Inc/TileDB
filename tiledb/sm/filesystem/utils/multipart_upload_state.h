/**
 * @file buffered_chunk.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the MultiPartUploadState class.
 */

#ifndef TILEDB_FILESYSTEM_MULTIPART_UPLOAD_STATE_H
#define TILEDB_FILESYSTEM_MULTIPART_UPLOAD_STATE_H

namespace tiledb::sm::filesystem {

/**
 * Multipart upload state definition used in the serialization of remote
 * global order writes. This state is a generalization of
 * the multipart upload state types currently defined independently by each
 * backend implementation.
 */
struct MultiPartUploadState {
  struct CompletedParts {
    optional<std::string> e_tag;
    uint64_t part_number;
  };

  uint64_t part_number;
  optional<std::string> upload_id;
  optional<std::vector<BufferedChunk>> buffered_chunks;
  std::vector<CompletedParts> completed_parts;
  Status status;
};

#endif // TILEDB_FILESYSTEM_MULTIPART_UPLOAD_STATE_H
