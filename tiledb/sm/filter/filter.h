/**
 * @file   filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file declares class Filter.
 */

#ifndef TILEDB_FILTER_H
#define TILEDB_FILTER_H

#include <ostream>
#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

// Forward declarations
namespace tiledb::sm {
class Filter;
}

/**
 * Converts the filter into a string representation.
 *
 * @param os Output stream
 * @param filter Filter to represent as a string
 * @return the output stream argument
 */
std::ostream& operator<<(std::ostream& os, const tiledb::sm::Filter& filter);

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class FilterBuffer;
class Tile;
class WriterTile;

enum class FilterOption : uint8_t;
enum class FilterType : uint8_t;
enum class Datatype : uint8_t;

class FilterStatusException : public StatusException {
 public:
  explicit FilterStatusException(const std::string& msg)
      : StatusException("Filter", msg) {
  }
};

/**
 * A Filter processes or modifies a byte region, modifying it in place, or
 * producing output in new buffers.
 *
 * Every filter implements both forward direction (used during write queries)
 * and a reverse direction (for reads).
 */
class Filter {
  friend std::ostream& ::operator<<(std::ostream&, const tiledb::sm::Filter&);

 public:
  /**
   * Constructor.
   *
   * @param filter_data_type Datatype the filter will operate on.
   */
  explicit Filter(FilterType type, Datatype filter_data_type);

  /** Destructor. */
  virtual ~Filter() = default;

  /**
   * Returns a newly allocated clone of this Filter. The clone does not belong
   * to any pipeline. Caller is responsible for deletion of the clone.
   */
  Filter* clone() const;

  /**
   * Returns a newly allocated clone of this Filter. The clone does not belong
   * to any pipeline. Caller is responsible for deletion of the clone.
   *
   * @param data_type Data type for the new filter.
   */
  Filter* clone(Datatype data_type) const;

  /**
   * Returns the filter output type
   *
   * @param input_type Expected type used for input. Used for filters which
   * change output type based on input data. e.g. XORFilter output type is
   * based on byte width of input type.
   */
  virtual Datatype output_datatype(Datatype input_type) const;

  /**
   * Throws if given data type *cannot* be handled by this filter.
   *
   * @param datatype Input datatype to check filter compatibility.
   */
  void ensure_accepts_datatype(Datatype datatype) const;

  /**
   * Checks if the filter is applicable to the input datatype.
   *
   * @param type Input datatype to check filter compatibility.
   */
  virtual bool accepts_input_datatype(Datatype type) const;

  /**
   * Gets an option from this filter.
   *
   * @param option Option whose value to get
   * @param value Buffer to store the value.
   * @return Status
   */
  Status get_option(FilterOption option, void* value) const;

  /**
   * Runs this filter in the "forward" direction (i.e. during write queries).
   *
   * Note: the input buffers should not be modified directly. The only reason it
   * is not a const pointer is to allow its offset field to be modified while
   * reading from it.
   *
   * Implemented by filter subclass.
   *
   * @param tile Current tile on which the filter is being run
   * @param offsets_tile Offsets tile of the current tile on which the filter is
   * being run
   * @param input_metadata Buffer with metadata for `input`
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data
   * @param output Buffer with filtered data (unused by in-place filters).
   */
  virtual void run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const = 0;

  /**
   * Runs this filter in the "reverse" direction (i.e. during read queries).
   *
   * Note: the input buffers should not be modified directly. The only reason it
   * is not a const pointer is to allow its offset field to be modified while
   * reading from it.
   *
   * Implemented by filter subclass.
   *
   * @param tile Current tile on which the filter is being run
   * @param offsets_tile Offsets tile of the current tile on which the filter is
   * being run
   * @param input_metadata Buffer with metadata for `input`
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data
   * @param output Buffer with filtered data (unused by in-place filters).
   * @param config Config object for query-level parameters
   * @return
   */
  virtual Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const = 0;

  /**
   * Initializes the filter compression resource pool if any
   *
   * @param size the size of the resource pool to initiliaze
   *
   * */
  virtual void init_compression_resource_pool(uint64_t size);

  /**
   * Initializes the filter decompression resource pool if any
   *
   * @param size the size of the resource pool to initiliaze
   *
   * */
  virtual void init_decompression_resource_pool(uint64_t size);

  /**
   * Sets an option on this filter.
   *
   * @param option Option whose value to get
   * @param value Buffer holding the value.
   * @return Status
   */
  Status set_option(FilterOption option, const void* value);

  /**
   * Serializes the filter metadata into a binary buffer.
   *
   * @param serializer The object to serialized into.
   * @return Status
   */
  void serialize(Serializer& serializer) const;

  /** Returns the filter type. */
  FilterType type() const;

 protected:
  /** The filter type. */
  FilterType type_;

  /** The datatype this filter will operate on within the pipeline. */
  Datatype filter_data_type_;

  /**
   * Clone function must implemented by each specific Filter subclass. This is
   * used instead of copy constructors to allow for base-typed Filter instances
   * to be cloned without knowing their derived types.
   */
  virtual Filter* clone_impl() const = 0;

  /** Optional subclass specific get_option method. */
  virtual Status get_option_impl(FilterOption option, void* value) const;

  /** Optional subclass specific get_option method. */
  virtual Status set_option_impl(FilterOption option, const void* value);

  /**
   * Serialization function that can be implemented by a specific Filter
   * subclass for filter-specific metadata.
   *
   * If a filter subclass has no specific metadata, it's not necessary to
   * implement this method.
   *
   * @param buff The buffer to serialize the data into.
   */
  virtual void serialize_impl(Serializer& serializer) const;

  /** Dumps the filter details in ASCII format in the selected output string. */
  virtual std::ostream& output(std::ostream& os) const = 0;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_H
