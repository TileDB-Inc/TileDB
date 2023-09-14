/**
 * @file read_ahead_cache.h
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
 * This file declares the ReadAheadCache class.
 */

#ifndef TILEDB_FILESYSTEM_READ_AHEAD_CACHE_H
#define TILEDB_FILESYSTEM_READ_AHEAD_CACHE_H

namespace tiledb::sm::filesystem {

struct ReadAheadBuffer {
  /* ********************************* */
  /*            CONSTRUCTORS           */
  /* ********************************* */

  /** Value Constructor. */
  ReadAheadBuffer(const uint64_t offset, Buffer&& buffer)
      : offset_(offset)
      , buffer_(std::move(buffer)) {
  }

  /** Move Constructor. */
  ReadAheadBuffer(ReadAheadBuffer&& other)
      : offset_(other.offset_)
      , buffer_(std::move(other.buffer_)) {
  }

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Move-Assign Operator. */
  ReadAheadBuffer& operator=(ReadAheadBuffer&& other) {
    offset_ = other.offset_;
    buffer_ = std::move(other.buffer_);
    return *this;
  }

  DISABLE_COPY_AND_COPY_ASSIGN(ReadAheadBuffer);

  /* ********************************* */
  /*             ATTRIBUTES            */
  /* ********************************* */

  /** The offset within the associated URI. */
  uint64_t offset_;

  /** The buffered data at `offset`. */
  Buffer buffer_;
};

/**
 * An LRU cache of `ReadAheadBuffer` objects keyed by a URI string.
 */
class ReadAheadCache : public LRUCache<std::string, ReadAheadBuffer> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ReadAheadCache(const uint64_t max_cached_buffers)
      : LRUCache(max_cached_buffers) {
  }

  /** Destructor. */
  virtual ~ReadAheadCache() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Attempts to read a buffer from the cache.
   *
   * @param uri The URI associated with the buffer to cache.
   * @param offset The offset that buffer starts at within the URI.
   * @param buffer The buffer to cache.
   * @param nbytes The number of bytes within the buffer.
   * @param success True if `buffer` was read from the cache.
   * @return Status
   */
  Status read(
      const URI& uri,
      const uint64_t offset,
      void* const buffer,
      const uint64_t nbytes,
      bool* const success) {
    assert(success);
    *success = false;

    // Store the URI's string representation.
    const std::string uri_str = uri.to_string();

    // Protect access to the derived LRUCache routines.
    std::lock_guard<std::mutex> lg(lru_mtx_);

    // Check that a cached buffer exists for `uri`.
    if (!has_item(uri_str))
      return Status::Ok();

    // Store a reference to the cached buffer.
    const ReadAheadBuffer* const ra_buffer = get_item(uri_str);

    // Check that the read offset is not below the offset of
    // the cached buffer.
    if (offset < ra_buffer->offset_)
      return Status::Ok();

    // Calculate the offset within the cached buffer that corresponds
    // to the requested read offset.
    const uint64_t offset_in_buffer = offset - ra_buffer->offset_;

    // Check that both the start and end positions of the requested
    // read range reside within the cached buffer.
    if (offset_in_buffer + nbytes > ra_buffer->buffer_.size())
      return Status::Ok();

    // Copy the subrange of the cached buffer that satisfies the caller's
    // read request back into their output `buffer`.
    std::memcpy(
        buffer,
        static_cast<uint8_t*>(ra_buffer->buffer_.data()) + offset_in_buffer,
        nbytes);

    // Touch the item to make it the most recently used item.
    touch_item(uri_str);

    *success = true;
    return Status::Ok();
  }

  /**
   * Writes a cached buffer for the given uri.
   *
   * @param uri The URI associated with the buffer to cache.
   * @param offset The offset that buffer starts at within the URI.
   * @param buffer The buffer to cache.
   * @return Status
   */
  Status insert(const URI& uri, const uint64_t offset, Buffer&& buffer) {
    // Protect access to the derived LRUCache routines.
    std::lock_guard<std::mutex> lg(lru_mtx_);

    const uint64_t size = buffer.size();
    ReadAheadBuffer ra_buffer(offset, std::move(buffer));
    return LRUCache<std::string, ReadAheadBuffer>::insert(
        uri.to_string(), std::move(ra_buffer), size);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  // Protects LRUCache routines.
  std::mutex lru_mtx_;
};

}  // namespace tiledb::sm::filesystem

#endif  // TILEDB_FILESYSTEM_READ_AHEAD_CACHE_H
