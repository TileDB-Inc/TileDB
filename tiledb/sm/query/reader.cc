/**
 * @file   reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file implements class Reader.
 */

#include "tiledb/sm/query/reader.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

#include <iostream>

namespace tiledb {
namespace sm {

namespace {
/**
 * If the given iterator points to an "invalid" element, advance it until the
 * pointed-to element is valid, or `end`. Validity is determined by calling
 * `it->valid()`.
 *
 * Example:
 *
 * @code{.cpp}
 * std::vector<T> vec = ...;
 * // Get an iterator to the first valid vec element, or vec.end() if the
 * // vector is empty or only contains invalid elements.
 * auto it = skip_invalid_elements(vec.begin(), vec.end());
 * // If there was a valid element, now advance the iterator to the next
 * // valid element (or vec.end() if there are no more).
 * it = skip_invalid_elements(++it, vec.end());
 * @endcode
 *
 *
 * @tparam IterT The iterator type
 * @param it The iterator
 * @param end The end iterator value
 * @return Iterator pointing to a valid element, or `end`.
 */
template <typename IterT>
inline IterT skip_invalid_elements(IterT it, const IterT& end) {
  while (it != end && !it->valid()) {
    ++it;
  }
  return it;
}
}  // namespace

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Reader::Reader() {
  array_ = nullptr;
  array_schema_ = nullptr;
  storage_manager_ = nullptr;
  layout_ = Layout::ROW_MAJOR;
  read_state_.cur_subarray_partition_ = nullptr;
  read_state_.subarray_ = nullptr;
  read_state_.initialized_ = false;
  read_state_.overflowed_ = false;
}

Reader::~Reader() {
  clear_read_state();
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Reader::array_schema() const {
  return array_schema_;
}

std::vector<std::string> Reader::attributes() const {
  return attributes_;
}

AttributeBuffer Reader::buffer(const std::string& attribute) const {
  auto attrbuf = attr_buffers_.find(attribute);
  if (attrbuf == attr_buffers_.end())
    return AttributeBuffer{};
  return attrbuf->second;
}

Status Reader::capnp(::QueryReader::Builder* queryReaderBuilder) const {
  if (!this->fragment_metadata_.empty()) {
    capnp::List<::FragmentMetadata>::Builder fragmentMetadataBuilder =
        queryReaderBuilder->initFragmentMetadata(
            this->fragment_metadata_.size());
    for (size_t i = 0; i < this->fragment_metadata_.size(); i++) {
      ::FragmentMetadata::Builder builder = fragmentMetadataBuilder[i];
      Status st = this->fragment_metadata_[i]->capnp(&builder);
      if (!st.ok())
        return st;
    }
  }

  if (this->read_state_.initialized_ == true) {
    ::ReadState::Builder readStateBuilder = queryReaderBuilder->initReadState();

    readStateBuilder.setInitialized(this->read_state_.initialized_);
    readStateBuilder.setOverflowed(this->read_state_.overflowed_);

    auto coords_type = array_schema_->coords_type();
    auto subarray_size = 2 * this->array_schema()->coords_size();
    if (this->read_state_.cur_subarray_partition_ != nullptr) {
      ::DomainArray::Builder curSubarrayPartitionBuilder =
          readStateBuilder.initCurSubarrayPartition();
      // Allocate subarray
      switch (coords_type) {
        case Datatype::INT8: {
          curSubarrayPartitionBuilder.setInt8(kj::arrayPtr(
              static_cast<int8_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::UINT8: {
          curSubarrayPartitionBuilder.setUint8(kj::arrayPtr(
              static_cast<uint8_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::INT16: {
          curSubarrayPartitionBuilder.setInt16(kj::arrayPtr(
              static_cast<int16_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::UINT16: {
          curSubarrayPartitionBuilder.setUint16(kj::arrayPtr(
              static_cast<uint16_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::INT32: {
          curSubarrayPartitionBuilder.setInt32(kj::arrayPtr(
              static_cast<int32_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::UINT32: {
          curSubarrayPartitionBuilder.setUint32(kj::arrayPtr(
              static_cast<uint32_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::INT64: {
          curSubarrayPartitionBuilder.setInt64(kj::arrayPtr(
              static_cast<int64_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::UINT64: {
          curSubarrayPartitionBuilder.setUint64(kj::arrayPtr(
              static_cast<uint64_t*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::FLOAT32: {
          curSubarrayPartitionBuilder.setFloat32(kj::arrayPtr(
              static_cast<float*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        case Datatype::FLOAT64: {
          curSubarrayPartitionBuilder.setFloat64(kj::arrayPtr(
              static_cast<double*>(this->read_state_.cur_subarray_partition_),
              subarray_size));
          break;
        }
        default: {
          return Status::ReaderError(
              "Unknown datatype for current subarray partition in capnp");
        }
      }
    }

    if (!this->read_state_.subarray_partitions_.empty()) {
      auto subarray_length = 2 * this->array_schema()->dim_num();
      // SubarrayPartitions
      ::ReadState::SubarrayPartitions::Builder subarrayPartitionsBuilder =
          readStateBuilder.initSubarrayPartitions();
      size_t subarrayPartitionsSize =
          this->read_state_.subarray_partitions_.size();
      switch (coords_type) {
        case Datatype::INT8: {
          capnp::List<capnp::List<int8_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initInt8(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<int8_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<int8_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::UINT8: {
          capnp::List<capnp::List<uint8_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initUint8(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<uint8_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<uint8_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::INT16: {
          capnp::List<capnp::List<int16_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initInt16(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<int16_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<int16_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::UINT16: {
          capnp::List<capnp::List<uint16_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initUint16(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<uint16_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<uint16_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::INT32: {
          capnp::List<capnp::List<int32_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initInt32(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<int32_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<int32_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::UINT32: {
          capnp::List<capnp::List<uint32_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initUint32(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<uint32_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<uint32_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::INT64: {
          capnp::List<capnp::List<int64_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initInt64(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<int64_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<int64_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::UINT64: {
          capnp::List<capnp::List<uint64_t>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initUint64(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<uint64_t>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<uint64_t*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::FLOAT32: {
          capnp::List<capnp::List<float>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initFloat32(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<float>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<float*>(subarray)[j]);
            i++;
          }
          break;
        }
        case Datatype::FLOAT64: {
          capnp::List<capnp::List<double>>::Builder partitionsBuilder =
              subarrayPartitionsBuilder.initFloat64(subarrayPartitionsSize);
          size_t i = 0;
          for (auto subarray : this->read_state_.subarray_partitions_) {
            capnp::List<double>::Builder list = partitionsBuilder[i];
            for (size_t j = 0; j < subarray_length; j++)
              list.set(j, static_cast<double*>(subarray)[j]);
            i++;
          }
          break;
        }
        default: {
          return Status::ReaderError(
              "Unknown datatype for subarray partitions in capnp");
        }
      }
    }
  }
  return Status::Ok();
}

bool Reader::incomplete() const {
  return read_state_.overflowed_ ||
         read_state_.cur_subarray_partition_ != nullptr;
}

unsigned Reader::fragment_num() const {
  return (unsigned int)fragment_metadata_.size();
}

std::vector<URI> Reader::fragment_uris() const {
  std::vector<URI> uris;
  for (auto meta : fragment_metadata_)
    uris.emplace_back(meta->fragment_uri());

  return uris;
}

Status Reader::get_buffer(
    const std::string& attribute, void** buffer, uint64_t** buffer_size) const {
  auto it = attr_buffers_.find(attribute);
  if (it == attr_buffers_.end()) {
    *buffer = nullptr;
    *buffer_size = nullptr;
  } else {
    *buffer = it->second.buffer_;
    *buffer_size = it->second.buffer_size_;
  }

  return Status::Ok();
}

Status Reader::get_buffer(
    const std::string& attribute,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  auto it = attr_buffers_.find(attribute);
  if (it == attr_buffers_.end()) {
    *buffer_off = nullptr;
    *buffer_off_size = nullptr;
    *buffer_val = nullptr;
    *buffer_val_size = nullptr;
  } else {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    *buffer_val = it->second.buffer_var_;
    *buffer_val_size = it->second.buffer_var_size_;
  }
  return Status::Ok();
}

Status Reader::from_capnp(::QueryReader::Reader* queryReader) {
  if (queryReader->hasFragmentMetadata()) {
    capnp::List<::FragmentMetadata>::Reader fragmentMetadataReader =
        queryReader->getFragmentMetadata();
    // Clear existing fragmentMetadata so we can use deserialized data
    this->fragment_metadata_.clear();
    for (auto fragmentReader : fragmentMetadataReader) {
      // TODO: Memory leak! Fragment metadata is never deleted.. would be nice
      // to change these to shared ptr's
      FragmentMetadata* fragment = new FragmentMetadata(
          this->array_schema(),
          this->array_schema()->array_type() == ArrayType::DENSE,
          URI(""),
          this->array_->timestamp());
      Status st = fragment->from_capnp(&fragmentReader);
      if (!st.ok())
        return st;
      this->fragment_metadata_.push_back(fragment);
    }
  }

  if (queryReader->hasReadState()) {
    ::ReadState::Reader readStateReader = queryReader->getReadState();

    this->read_state_.initialized_ = readStateReader.getInitialized();
    this->read_state_.overflowed_ = readStateReader.getOverflowed();

    ::DomainArray::Reader curSubarrayPartitionReader =
        readStateReader.getCurSubarrayPartition();
    // Allocate subarray
    auto subarray_size = 2 * this->array_schema_->coords_size();
    this->read_state_.cur_subarray_partition_ = std::malloc(subarray_size);
    if (this->read_state_.cur_subarray_partition_ == nullptr)
      return Status::ReaderError(
          "Cannot initialize read state current subarray partition; Memory "
          "allocation failed");
    auto coords_type = array_schema_->coords_type();
    switch (coords_type) {
      case Datatype::INT8: {
        if (curSubarrayPartitionReader.hasInt8()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getInt8();
          // Vector to temporary store current sub array before copying
          std::vector<int8_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::UINT8: {
        if (curSubarrayPartitionReader.hasUint8()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getUint8();
          // Vector to temporary store current sub array before copying
          std::vector<uint8_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::INT16: {
        if (curSubarrayPartitionReader.hasInt16()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getInt16();
          // Vector to temporary store current sub array before copying
          std::vector<int16_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::UINT16: {
        if (curSubarrayPartitionReader.hasUint16()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getUint16();
          // Vector to temporary store current sub array before copying
          std::vector<uint16_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::INT32: {
        if (curSubarrayPartitionReader.hasInt32()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getInt32();
          // Vector to temporary store current sub array before copying
          std::vector<int32_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::UINT32: {
        if (curSubarrayPartitionReader.hasUint32()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getUint32();
          // Vector to temporary store current sub array before copying
          std::vector<uint32_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::INT64: {
        if (curSubarrayPartitionReader.hasInt64()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getInt64();
          // Vector to temporary store current sub array before copying
          std::vector<int64_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::UINT64: {
        if (curSubarrayPartitionReader.hasUint64()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getUint64();
          // Vector to temporary store current sub array before copying
          std::vector<uint64_t> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::FLOAT32: {
        if (curSubarrayPartitionReader.hasFloat32()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getFloat32();
          // Vector to temporary store current sub array before copying
          std::vector<float> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      case Datatype::FLOAT64: {
        if (curSubarrayPartitionReader.hasFloat64()) {
          // Get current subarray from reader
          auto currSubArray = curSubarrayPartitionReader.getFloat64();
          // Vector to temporary store current sub array before copying
          std::vector<double> curSubarrayVecTmp(currSubArray.size());
          for (size_t i = 0; i < currSubArray.size(); i++)
            curSubarrayVecTmp[i] = currSubArray[i];
          std::memcpy(
              this->read_state_.cur_subarray_partition_,
              curSubarrayVecTmp.data(),
              subarray_size);
        } else {
          std::free(this->read_state_.cur_subarray_partition_);
          this->read_state_.cur_subarray_partition_ = nullptr;
        }
        break;
      }
      default: {
        return Status::ReaderError(
            "Unknown datatype for current subarray partition in from_capnp");
      }
    }

    // SubarrayPartitions
    ::ReadState::SubarrayPartitions::Reader subarrayPartitionsReader =
        readStateReader.getSubarrayPartitions();
    switch (coords_type) {
      case Datatype::INT8: {
        if (subarrayPartitionsReader.hasInt8()) {
          capnp::List<capnp::List<int8_t>>::Reader list =
              subarrayPartitionsReader.getInt8();
          for (capnp::List<int8_t>::Reader subarrayPartition : list) {
            int8_t* subarray = static_cast<int8_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::UINT8: {
        if (subarrayPartitionsReader.hasUint8()) {
          capnp::List<capnp::List<uint8_t>>::Reader list =
              subarrayPartitionsReader.getUint8();
          for (capnp::List<uint8_t>::Reader subarrayPartition : list) {
            uint8_t* subarray = static_cast<uint8_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::INT16: {
        if (subarrayPartitionsReader.hasInt16()) {
          capnp::List<capnp::List<int16_t>>::Reader list =
              subarrayPartitionsReader.getInt16();
          for (capnp::List<int16_t>::Reader subarrayPartition : list) {
            int16_t* subarray = static_cast<int16_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::UINT16: {
        if (subarrayPartitionsReader.hasUint16()) {
          capnp::List<capnp::List<uint16_t>>::Reader list =
              subarrayPartitionsReader.getUint16();
          for (capnp::List<uint16_t>::Reader subarrayPartition : list) {
            uint16_t* subarray = static_cast<uint16_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::INT32: {
        if (subarrayPartitionsReader.hasInt32()) {
          capnp::List<capnp::List<int32_t>>::Reader list =
              subarrayPartitionsReader.getInt32();
          for (capnp::List<int32_t>::Reader subarrayPartition : list) {
            int32_t* subarray = static_cast<int32_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::UINT32: {
        if (subarrayPartitionsReader.hasUint32()) {
          capnp::List<capnp::List<uint32_t>>::Reader list =
              subarrayPartitionsReader.getUint32();
          for (capnp::List<uint32_t>::Reader subarrayPartition : list) {
            uint32_t* subarray = static_cast<uint32_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::INT64: {
        if (subarrayPartitionsReader.hasInt64()) {
          capnp::List<capnp::List<int64_t>>::Reader list =
              subarrayPartitionsReader.getInt64();
          for (capnp::List<int64_t>::Reader subarrayPartition : list) {
            int64_t* subarray = static_cast<int64_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::UINT64: {
        if (subarrayPartitionsReader.hasUint64()) {
          capnp::List<capnp::List<uint64_t>>::Reader list =
              subarrayPartitionsReader.getUint64();
          for (capnp::List<uint64_t>::Reader subarrayPartition : list) {
            uint64_t* subarray = static_cast<uint64_t*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::FLOAT32: {
        if (subarrayPartitionsReader.hasFloat32()) {
          capnp::List<capnp::List<float>>::Reader list =
              subarrayPartitionsReader.getFloat32();
          for (capnp::List<float>::Reader subarrayPartition : list) {
            float* subarray = static_cast<float*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      case Datatype::FLOAT64: {
        if (subarrayPartitionsReader.hasFloat64()) {
          capnp::List<capnp::List<double>>::Reader list =
              subarrayPartitionsReader.getFloat64();
          for (capnp::List<double>::Reader subarrayPartition : list) {
            double* subarray = static_cast<double*>(std::malloc(
                subarrayPartition.size() * datatype_size(coords_type)));
            if (subarray == nullptr)
              return Status::ReaderError(
                  "Cannot initialize read state subarray partitions; Memory "
                  "allocation failed");
            for (size_t i = 0; i < subarrayPartition.size(); i++)
              subarray[i] = subarrayPartition[i];

            this->read_state_.subarray_partitions_.push_back(subarray);
          }
        }
        break;
      }
      default: {
        return Status::ReaderError(
            "Unknown datatype for subarray partitions in from_capnp");
      }
    }
  }
  return Status::Ok();
}

Status Reader::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize query; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize query; Array metadata not set"));
  if (attr_buffers_.empty())
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize query; Buffers not set"));
  if (attributes_.empty())
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize query; Attributes not set"));

  if (read_state_.subarray_ == nullptr)
    RETURN_NOT_OK(set_subarray(nullptr));

  optimize_layout_for_1D();

  if (!fragment_metadata_.empty())
    RETURN_NOT_OK(init_read_state());

  return Status::Ok();
}

URI Reader::last_fragment_uri() const {
  if (fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.back()->fragment_uri();
}

Layout Reader::layout() const {
  return layout_;
}

Status Reader::next_subarray_partition() {
  STATS_FUNC_IN(reader_next_subarray_partition);

  if (read_state_.subarray_partitions_.empty()) {
    std::free(read_state_.cur_subarray_partition_);
    read_state_.cur_subarray_partition_ = nullptr;
    return Status::Ok();
  }

  // Prepare buffer sizes map
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      buffer_sizes_map;
  for (const auto& it : attr_buffers_) {
    buffer_sizes_map[it.first] = std::pair<uint64_t, uint64_t>(
        it.second.original_buffer_size_, it.second.original_buffer_var_size_);
  }

  // Loop until a new partition whose result fit in the buffers is found
  std::unordered_map<std::string, std::pair<double, double>> est_buffer_sizes;
  bool found = false;
  void* next_partition = nullptr;
  auto domain = array_schema_->domain();
  do {
    // Pop next partition
    next_partition = read_state_.subarray_partitions_.front();
    read_state_.subarray_partitions_.pop_front();

    // Get estimated buffer sizes
    for (const auto& attr_it : buffer_sizes_map)
      est_buffer_sizes[attr_it.first] = std::pair<double, double>(0, 0);
    auto st = storage_manager_->array_compute_est_read_buffer_sizes(
        array_schema_, fragment_metadata_, next_partition, &est_buffer_sizes);

    if (!st.ok()) {
      std::free(next_partition);
      clear_read_state();
      return st;
    }

    // Handle case of no results
    auto no_results = true;
    for (auto& item : est_buffer_sizes) {
      if (item.second.first != 0) {
        no_results = false;
        break;
      }
    }
    if (no_results) {
      std::free(next_partition);
      next_partition = nullptr;
      continue;
    }

    // Handle case of split
    auto must_split = false;
    for (auto& item : est_buffer_sizes) {
      auto buffer_size = buffer_sizes_map.find(item.first)->second.first;
      auto buffer_var_size = buffer_sizes_map.find(item.first)->second.second;
      auto var_size = array_schema_->var_size(item.first);
      if (uint64_t(round(item.second.first)) > buffer_size ||
          (var_size && uint64_t(round(item.second.second)) > buffer_var_size)) {
        must_split = true;
        break;
      }
    }
    if (must_split) {
      void *subarray_1 = nullptr, *subarray_2 = nullptr;
      st = domain->split_subarray(
          next_partition, layout_, &subarray_1, &subarray_2);
      if (!st.ok()) {
        std::free(next_partition);
        clear_read_state();
        return st;
      }

      // Not splittable, return the original subarray as result
      if (subarray_1 == nullptr || subarray_2 == nullptr) {
        found = true;
      } else {
        read_state_.subarray_partitions_.push_front(subarray_2);
        read_state_.subarray_partitions_.push_front(subarray_1);
      }
    } else {
      found = true;
    }
  } while (!found && !read_state_.subarray_partitions_.empty());

  // Set the current subarray
  if (found) {
    assert(read_state_.cur_subarray_partition_ != nullptr);
    std::memcpy(
        read_state_.cur_subarray_partition_,
        next_partition,
        2 * array_schema_->coords_size());
  } else {
    std::free(read_state_.cur_subarray_partition_);
    read_state_.cur_subarray_partition_ = nullptr;
  }

  std::free(next_partition);

  return Status::Ok();

  STATS_FUNC_OUT(reader_next_subarray_partition);
}

bool Reader::no_results() const {
  for (const auto& it : attr_buffers_) {
    if (*(it.second.buffer_size_) != 0)
      return false;
  }
  return true;
}

Status Reader::read() {
  STATS_FUNC_IN(reader_read);

  if (fragment_metadata_.empty() ||
      read_state_.cur_subarray_partition_ == nullptr) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  bool no_results = false;
  read_state_.overflowed_ = false;

  do {
    reset_buffer_sizes();

    // Perform dense or sparse read if there are fragments
    if (array_schema_->dense()) {
      RETURN_NOT_OK(dense_read());
    } else {
      RETURN_NOT_OK(sparse_read());
    }

    // Return if the buffers could not fit the results.
    // Do not advance to the next partition. This is equivalent to
    // having no results.
    if (read_state_.overflowed_) {
      zero_out_buffer_sizes();
      return Status::Ok();
    }

    // Advance to the next subarray partition
    RETURN_NOT_OK(next_subarray_partition());
    no_results = this->no_results();
  } while (no_results && read_state_.cur_subarray_partition_ != nullptr);

  if (no_results)
    zero_out_buffer_sizes();

  return Status::Ok();

  STATS_FUNC_OUT(reader_read);
}

void Reader::set_array(const Array* array) {
  array_ = array;
}

void Reader::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
  if (array_schema->is_kv())
    layout_ = Layout::GLOBAL_ORDER;
}

Status Reader::set_buffer(
    const std::string& attribute, void* buffer, uint64_t* buffer_size) {
  // Check buffer
  if (buffer == nullptr || buffer_size == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffer; Array schema not set"));

  // Check that attribute exists
  if (attribute != constants::coords &&
      array_schema_->attribute(attribute) == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Invalid attribute"));

  // Check that attribute is fixed-sized
  bool var_size =
      (attribute != constants::coords && array_schema_->var_size(attribute));
  if (var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute '") + attribute +
        "' is var-sized"));

  // Error if setting a new attribute after initialization
  bool attr_exists = attr_buffers_.find(attribute) != attr_buffers_.end();
  if (read_state_.initialized_ && !attr_exists)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer for new attribute '") + attribute +
        "' after initialization"));

  // Append to attributes only if buffer not set before
  if (!attr_exists)
    attributes_.emplace_back(attribute);

  // Set attribute buffer
  attr_buffers_[attribute] =
      AttributeBuffer(buffer, nullptr, buffer_size, nullptr);

  return Status::Ok();
}

Status Reader::set_buffer(
    const std::string& attribute,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size) {
  // Check buffer
  if (buffer_off == nullptr || buffer_off_size == nullptr ||
      buffer_val == nullptr || buffer_val_size == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffer; Array schema not set"));

  // Check that attribute exists
  if (attribute != constants::coords &&
      array_schema_->attribute(attribute) == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Invalid attribute"));

  // Check that attribute is var-sized
  bool var_size =
      (attribute != constants::coords && array_schema_->var_size(attribute));
  if (!var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute '") + attribute +
        "' is fixed-sized"));

  // Error if setting a new attribute after initialization
  bool attr_exists = attr_buffers_.find(attribute) != attr_buffers_.end();
  if (read_state_.initialized_ && !attr_exists)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer for new attribute '") + attribute +
        "' after initialization"));

  // Append to attributes only if buffer not set before
  if (!attr_exists)
    attributes_.emplace_back(attribute);

  // Set attribute buffer
  attr_buffers_[attribute] =
      AttributeBuffer(buffer_off, buffer_val, buffer_off_size, buffer_val_size);

  return Status::Ok();
}

void Reader::set_fragment_metadata(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  fragment_metadata_ = fragment_metadata;
}

Status Reader::set_layout(Layout layout) {
  // Check if the array is a key-value store
  if (array_schema_->is_kv())
    return LOG_STATUS(Status::ReaderError(
        "Cannot set layout; The array is defined as a key-value store"));

  layout_ = layout;

  return Status::Ok();
}

void Reader::set_storage_manager(StorageManager* storage_manager) {
  storage_manager_ = storage_manager;
}

Status Reader::set_subarray(const void* subarray) {
  if (read_state_.subarray_ != nullptr)
    clear_read_state();

  auto subarray_size = 2 * array_schema_->coords_size();
  read_state_.subarray_ = std::malloc(subarray_size);
  if (read_state_.subarray_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Memory allocation for read state subarray failed"));

  if (subarray != nullptr)
    std::memcpy(read_state_.subarray_, subarray, subarray_size);
  else
    std::memcpy(
        read_state_.subarray_,
        array_schema_->domain()->domain(),
        subarray_size);

  return Status::Ok();
}

void* Reader::subarray() const {
  return read_state_.subarray_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Reader::clear_read_state() {
  for (auto p : read_state_.subarray_partitions_)
    std::free(p);
  read_state_.subarray_partitions_.clear();

  std::free(read_state_.subarray_);
  read_state_.subarray_ = nullptr;
  std::free(read_state_.cur_subarray_partition_);
  read_state_.cur_subarray_partition_ = nullptr;

  read_state_.initialized_ = false;
  read_state_.overflowed_ = false;
}

template <class T>
Status Reader::compute_cell_ranges(
    const OverlappingCoordsList<T>& coords,
    OverlappingCellRangeList* cell_ranges) const {
  STATS_FUNC_IN(reader_compute_cell_ranges);

  // Trivial case
  auto coords_num = (uint64_t)coords.size();
  if (coords_num == 0)
    return Status::Ok();

  // Initialize the first range
  auto coords_end = coords.end();
  auto it = skip_invalid_elements(coords.begin(), coords_end);
  if (it == coords_end) {
    return LOG_STATUS(Status::ReaderError("Unexpected empty cell range."));
  }
  uint64_t start_pos = it->pos_;
  uint64_t end_pos = start_pos;
  auto tile = it->tile_;

  // Scan the coordinates and compute ranges
  it = skip_invalid_elements(++it, coords_end);
  while (it != coords_end) {
    if (it->tile_ == tile && it->pos_ == end_pos + 1) {
      // Same range - advance end position
      end_pos = it->pos_;
    } else {
      // New range - append previous range
      cell_ranges->emplace_back(tile, start_pos, end_pos);
      start_pos = it->pos_;
      end_pos = start_pos;
      tile = it->tile_;
    }
    it = skip_invalid_elements(++it, coords_end);
  }

  // Append the last range
  cell_ranges->emplace_back(tile, start_pos, end_pos);

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_cell_ranges);
}

template <class T>
Status Reader::compute_dense_cell_ranges(
    const T* tile_coords,
    std::vector<DenseCellRangeIter<T>>& frag_its,
    uint64_t start,
    uint64_t end,
    std::list<DenseCellRange<T>>* dense_cell_ranges) {
  STATS_FUNC_IN(reader_compute_dense_cell_ranges);

  // NOTE: `start` will always get updated as results are inserted
  // in `dense_cell_ranges`.

  // For easy reference
  auto fragment_num = fragment_metadata_.size();
  auto layout =
      (layout_ == Layout::GLOBAL_ORDER) ? array_schema_->cell_order() : layout_;
  auto same_layout = (layout == array_schema_->cell_order());
  DenseCellRangeCmp<T> comp(array_schema_->domain(), layout);

  // Populate queue - stores pairs of (start, fragment_num-fragment_id)
  std::priority_queue<
      DenseCellRange<T>,
      std::vector<DenseCellRange<T>>,
      DenseCellRangeCmp<T>>
      pq((comp));
  for (unsigned i = 0; i < fragment_num; ++i) {
    if (!frag_its[i].end())
      pq.emplace(
          i,
          tile_coords,
          frag_its[i].range_start(),
          frag_its[i].range_end(),
          (same_layout) ? nullptr : frag_its[i].coords_start(),
          (same_layout) ? nullptr : frag_its[i].coords_end());
  }

  // Iterate over the queue and create dense cell ranges
  while (!pq.empty()) {
    // Pop top range, and get new top
    auto popped = pq.top();
    auto fidx = popped.fragment_idx_;
    pq.pop();

    // Popped must be ignored and a new range must be fetched
    if (comp.precedes(popped, start, DenseCellRangeCmp<T>::RANGE_END)) {
      ++frag_its[fidx];
      if (!frag_its[fidx].end())
        pq.emplace(
            fidx,
            tile_coords,
            frag_its[fidx].range_start(),
            frag_its[fidx].range_end(),
            (same_layout) ? nullptr : frag_its[fidx].coords_start(),
            (same_layout) ? nullptr : frag_its[fidx].coords_end());
      continue;
    }

    // The search needs to stop - add current range as empty result
    if (comp.succeeds(popped, end, DenseCellRangeCmp<T>::RANGE_START)) {
      dense_cell_ranges->emplace_back(
          -1, tile_coords, start, end, nullptr, nullptr);
      return Status::Ok();
    }

    // ----------------------------------------------------------------
    // At this point, there is intersection between popped
    // and the input range. We need to create dense range results.
    // ----------------------------------------------------------------

    // Need to pad an empty range
    if (popped.start_ > start) {
      auto new_end = MIN(end, popped.start_ - 1);
      dense_cell_ranges->emplace_back(
          -1, tile_coords, start, new_end, nullptr, nullptr);
      start = new_end + 1;
      if (start > end)
        break;
    }

    // Check if popped intersects with top.
    if (!pq.empty()) {
      auto top = pq.top();

      // Keep on ignoring ranges that belong to older fragments
      // and are fully contained in the popped range
      while (popped.fragment_idx_ > top.fragment_idx_ &&
             popped.start_ <= top.start_ && popped.end_ >= top.end_) {
        pq.pop();
        if (pq.empty())
          break;
        top = pq.top();
      }

      // Make partial result, and then split and re-insert popped to pq.
      if (!pq.empty() && top.start_ <= end && top.start_ > popped.start_ &&
          top.start_ <= popped.end_) {
        auto new_end = top.start_ - 1;
        dense_cell_ranges->emplace_back(
            fidx, tile_coords, start, new_end, nullptr, nullptr);
        start = new_end + 1;
        if (start > end)
          break;
        popped.start_ = top.start_;
        pq.emplace(popped);
        continue;
      }
    }

    // Make result
    auto new_end = MIN(end, popped.end_);
    dense_cell_ranges->emplace_back(
        fidx, tile_coords, start, new_end, nullptr, nullptr);
    start = new_end + 1;

    // Check if a new range must be fetched in place of popped
    if (new_end == popped.end_) {
      ++frag_its[fidx];
      if (!frag_its[fidx].end())
        pq.emplace(
            fidx,
            tile_coords,
            frag_its[fidx].range_start(),
            frag_its[fidx].range_end(),
            (same_layout) ? nullptr : frag_its[fidx].coords_start(),
            (same_layout) ? nullptr : frag_its[fidx].coords_end());
    }

    if (start > end)
      break;
  }

  // Insert an empty cell range if the input range has not been filled
  if (start <= end) {
    dense_cell_ranges->emplace_back(
        -1, tile_coords, start, end, nullptr, nullptr);
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_dense_cell_ranges);
}

template <class T>
Status Reader::compute_dense_overlapping_tiles_and_cell_ranges(
    const std::list<DenseCellRange<T>>& dense_cell_ranges,
    const OverlappingCoordsList<T>& coords,
    OverlappingTileVec* tiles,
    OverlappingCellRangeList* overlapping_cell_ranges) {
  STATS_FUNC_IN(reader_compute_dense_overlapping_tiles_and_cell_ranges);

  // Trivial case = no dense cell ranges
  if (dense_cell_ranges.empty())
    return Status::Ok();

  // For easy reference
  auto domain = array_schema_->domain();
  auto dim_num = array_schema_->dim_num();
  auto coords_size = array_schema_->coords_size();

  // This maps a (fragment, tile coords) pair to an overlapping tile position
  std::map<std::pair<unsigned, const T*>, uint64_t> tile_coords_map;

  // Prepare first range
  auto cr_it = dense_cell_ranges.begin();
  const OverlappingTile* cur_tile = nullptr;
  const T* cur_tile_coords = cr_it->tile_coords_;
  if (cr_it->fragment_idx_ != -1) {
    auto tile_idx = fragment_metadata_[cr_it->fragment_idx_]->get_tile_pos(
        cr_it->tile_coords_);
    auto cur_tile_ptr = std::unique_ptr<OverlappingTile>(new OverlappingTile(
        (unsigned)cr_it->fragment_idx_, tile_idx, attributes_));
    tile_coords_map[std::pair<unsigned, const T*>(
        (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_)] =
        (uint64_t)tiles->size();
    cur_tile = cur_tile_ptr.get();
    tiles->push_back(std::move(cur_tile_ptr));
  }
  auto start = cr_it->start_;
  auto end = cr_it->end_;

  // Initialize coords info
  auto coords_end = coords.end();
  auto coords_it = skip_invalid_elements(coords.begin(), coords_end);
  std::vector<T> coords_tile_coords;
  coords_tile_coords.resize(dim_num);
  uint64_t coords_pos = 0;
  unsigned coords_fidx = 0;
  const OverlappingTile* coords_tile = nullptr;
  if (coords_it != coords_end) {
    domain->get_tile_coords(coords_it->coords_, &coords_tile_coords[0]);
    RETURN_NOT_OK(domain->get_cell_pos<T>(coords_it->coords_, &coords_pos));
    coords_fidx = coords_it->tile_->fragment_idx_;
    coords_tile = coords_it->tile_;
  }

  // Compute overlapping tiles and cell ranges
  for (++cr_it; cr_it != dense_cell_ranges.end(); ++cr_it) {
    // Find tile
    const OverlappingTile* tile = nullptr;
    if (cr_it->fragment_idx_ != -1) {  // Non-empty
      auto tile_coords_map_it =
          tile_coords_map.find(std::pair<unsigned, const T*>(
              (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_));
      if (tile_coords_map_it != tile_coords_map.end()) {
        tile = (*tiles)[tile_coords_map_it->second].get();
      } else {
        auto tile_idx = fragment_metadata_[cr_it->fragment_idx_]->get_tile_pos(
            cr_it->tile_coords_);
        auto tile_ptr = std::unique_ptr<OverlappingTile>(new OverlappingTile(
            (unsigned)cr_it->fragment_idx_, tile_idx, attributes_));
        tile_coords_map[std::pair<unsigned, const T*>(
            (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_)] =
            (uint64_t)tiles->size();
        tile = tile_ptr.get();
        tiles->push_back(std::move(tile_ptr));
      }
    }

    // Check if the range must be appended to the current one
    // The second condition is to impose constraint "if both ranges
    // are empty, then they should belong to the same dense tile".
    if (tile == cur_tile &&
        (tile != nullptr ||
         !memcmp(cur_tile_coords, cr_it->tile_coords_, coords_size)) &&
        cr_it->start_ == end + 1) {
      end = cr_it->end_;
      continue;
    }

    // Handle the coordinates that fall between `start` and `end`.
    // This function will either skip the coordinates if they belong to an
    // older fragment, or include them as results and split the dense cell
    // range.
    RETURN_NOT_OK(handle_coords_in_dense_cell_range(
        cur_tile,
        cur_tile_coords,
        &start,
        end,
        coords_size,
        coords,
        &coords_it,
        coords_tile,
        &coords_pos,
        &coords_fidx,
        &coords_tile_coords,
        overlapping_cell_ranges));

    // Push remaining range to the result
    if (start <= end)
      overlapping_cell_ranges->emplace_back(cur_tile, start, end);

    // Update state
    cur_tile = tile;
    start = cr_it->start_;
    end = cr_it->end_;
    cur_tile_coords = cr_it->tile_coords_;
  }

  // Handle the coordinates that fall between `start` and `end`.
  // This function will either skip the coordinates if they belong to an
  // older fragment, or include them as results and split the dense cell
  // range.
  RETURN_NOT_OK(handle_coords_in_dense_cell_range(
      cur_tile,
      cur_tile_coords,
      &start,
      end,
      coords_size,
      coords,
      &coords_it,
      coords_tile,
      &coords_pos,
      &coords_fidx,
      &coords_tile_coords,
      overlapping_cell_ranges));

  // Push remaining range to the result
  if (start <= end)
    overlapping_cell_ranges->emplace_back(cur_tile, start, end);

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_dense_overlapping_tiles_and_cell_ranges);
}  // namespace sm

template <class T>
Status Reader::compute_overlapping_coords(
    const OverlappingTileVec& tiles, OverlappingCoordsList<T>* coords) const {
  STATS_FUNC_IN(reader_compute_overlapping_coords);

  for (const auto& tile : tiles) {
    if (tile->full_overlap_) {
      RETURN_NOT_OK(get_all_coords<T>(tile.get(), coords));
    } else {
      RETURN_NOT_OK(compute_overlapping_coords<T>(tile.get(), coords));
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_overlapping_coords);
}

template <class T>
Status Reader::compute_overlapping_coords(
    const OverlappingTile* tile, OverlappingCoordsList<T>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto coords_num = t.cell_num();
  auto subarray = (T*)read_state_.cur_subarray_partition_;
  auto c = (T*)t.data();

  for (uint64_t i = 0, pos = 0; i < coords_num; ++i, pos += dim_num) {
    if (utils::geometry::coords_in_rect<T>(&c[pos], &subarray[0], dim_num))
      coords->emplace_back(tile, &c[pos], i);
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_overlapping_tiles(OverlappingTileVec* tiles) const {
  STATS_FUNC_IN(reader_compute_overlapping_tiles);

  // For easy reference
  auto subarray = (T*)read_state_.cur_subarray_partition_;
  auto dim_num = array_schema_->dim_num();
  auto fragment_num = fragment_metadata_.size();
  bool full_overlap;

  // Find overlapping tile indexes for each fragment
  tiles->clear();
  for (unsigned i = 0; i < fragment_num; ++i) {
    // Applicable only to sparse fragments
    if (fragment_metadata_[i]->dense())
      continue;

    auto mbrs = fragment_metadata_[i]->mbrs();
    auto mbr_num = (uint64_t)mbrs.size();
    for (uint64_t j = 0; j < mbr_num; ++j) {
      if (utils::geometry::overlap(
              &subarray[0], (const T*)(mbrs[j]), dim_num, &full_overlap)) {
        auto tile = std::unique_ptr<OverlappingTile>(
            new OverlappingTile(i, j, attributes_, full_overlap));
        tiles->push_back(std::move(tile));
      }
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_overlapping_tiles);
}

template <class T>
Status Reader::compute_tile_coords(
    std::unique_ptr<T[]>* all_tile_coords,
    OverlappingCoordsList<T>* coords) const {
  STATS_FUNC_IN(reader_compute_tile_coords);

  if (coords->empty() || array_schema_->domain()->tile_extents() == nullptr)
    return Status::Ok();

  auto domain = reinterpret_cast<const T*>(array_schema_->domain()->domain());
  auto tile_extents =
      reinterpret_cast<const T*>(array_schema_->domain()->tile_extents());
  auto dim_num = array_schema_->dim_num();
  const auto num_coords = (uint64_t)coords->size();

  // Allocate space for all OverlappingCoords' tile coordinate tuples.
  all_tile_coords->reset(new (std::nothrow) T[num_coords * dim_num]);
  if (all_tile_coords == nullptr) {
    return LOG_STATUS(
        Status::ReaderError("Could not allocate tile coords array."));
  }

  // Compute the tile coordinates for each OverlappingCoords.
  for (uint64_t i = 0; i < num_coords; i++) {
    auto& c = (*coords)[i];
    T* tile_coords = all_tile_coords->get() + i * dim_num;
    for (unsigned j = 0; j < dim_num; j++) {
      tile_coords[j] = (c.coords_[j] - domain[2 * j]) / tile_extents[j];
    }
    c.tile_coords_ = tile_coords;
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_tile_coords);
}

Status Reader::copy_cells(
    const std::string& attribute, const OverlappingCellRangeList& cell_ranges) {
  // Early exit for empty cell range list.
  if (cell_ranges.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  if (array_schema_->var_size(attribute))
    return copy_var_cells(attribute, cell_ranges);
  return copy_fixed_cells(attribute, cell_ranges);
}

Status Reader::copy_fixed_cells(
    const std::string& attribute, const OverlappingCellRangeList& cell_ranges) {
  STATS_FUNC_IN(reader_copy_fixed_cells);

  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(attribute);
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  // Precompute the cell range destination offsets in the buffer.
  auto num_cr = cell_ranges.size();
  uint64_t buffer_offset = 0;
  std::vector<uint64_t> cr_offsets(num_cr);
  for (uint64_t i = 0; i < num_cr; i++) {
    const auto& cr = cell_ranges[i];
    auto bytes_to_copy = (cr.end_ - cr.start_ + 1) * cell_size;
    cr_offsets[i] = buffer_offset;
    buffer_offset += bytes_to_copy;
  }

  // Handle overflow
  if (buffer_offset > *buffer_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy cell ranges in parallel.
  auto statuses = parallel_for(0, num_cr, [&](uint64_t i) {
    const auto& cr = cell_ranges[i];
    uint64_t offset = cr_offsets[i];
    // Check for overflow
    auto bytes_to_copy = (cr.end_ - cr.start_ + 1) * cell_size;
    assert(offset + bytes_to_copy <= *buffer_size);

    // Copy
    if (cr.tile_ == nullptr) {  // Empty range
      auto fill_num = bytes_to_copy / fill_size;
      for (uint64_t j = 0; j < fill_num; ++j) {
        std::memcpy(buffer + offset, fill_value, fill_size);
        offset += fill_size;
      }
    } else {  // Non-empty range
      const auto& tile = cr.tile_->attr_tiles_.find(attribute)->second.first;
      auto data = (unsigned char*)tile.data();
      std::memcpy(buffer + offset, data + cr.start_ * cell_size, bytes_to_copy);
    }

    return Status::Ok();
  });

  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(attr_buffers_[attribute].buffer_size_) = buffer_offset;
  STATS_COUNTER_ADD(reader_num_fixed_cell_bytes_copied, buffer_offset);

  return Status::Ok();

  STATS_FUNC_OUT(reader_copy_fixed_cells);
}

Status Reader::copy_var_cells(
    const std::string& attribute, const OverlappingCellRangeList& cell_ranges) {
  STATS_FUNC_IN(reader_copy_var_cells);

  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  uint64_t offset_size = constants::cell_var_offset_size;
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  // Compute the destinations of offsets and var-len data in the buffers.
  std::vector<std::vector<uint64_t>> offset_offsets_per_cr;
  std::vector<std::vector<uint64_t>> var_offsets_per_cr;
  uint64_t total_offset_size, total_var_size;
  RETURN_NOT_OK(compute_var_cell_destinations(
      attribute,
      cell_ranges,
      &offset_offsets_per_cr,
      &var_offsets_per_cr,
      &total_offset_size,
      &total_var_size));

  // Check for overflow and return early (without copying) in that case.
  if (total_offset_size > *buffer_size || total_var_size > *buffer_var_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy cell ranges in parallel.
  const auto num_cr = cell_ranges.size();
  auto statuses = parallel_for(0, num_cr, [&](uint64_t cr_idx) {
    const auto& cr = cell_ranges[cr_idx];
    const auto& offset_offsets = offset_offsets_per_cr[cr_idx];
    const auto& var_offsets = var_offsets_per_cr[cr_idx];

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    unsigned char* tile_var_data = nullptr;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cr.tile_ != nullptr) {
      const auto& tile_pair = cr.tile_->attr_tiles_.find(attribute)->second;
      const auto& tile = tile_pair.first;
      const auto& tile_var = tile_pair.second;
      tile_offsets = (uint64_t*)tile.data();
      tile_var_data = (unsigned char*)tile_var.data();
      tile_cell_num = tile.cell_num();
      tile_var_size = tile_var.size();
    }

    // Copy each cell in the range
    for (auto cell_idx = cr.start_; cell_idx <= cr.end_; cell_idx++) {
      uint64_t dest_vec_idx = cell_idx - cr.start_;
      auto offset_dest = buffer + offset_offsets[dest_vec_idx];
      auto var_offset = var_offsets[dest_vec_idx];
      auto var_dest = buffer_var + var_offset;

      // Copy offset
      std::memcpy(offset_dest, &var_offset, offset_size);

      // Copy variable-sized value
      if (cr.tile_ == nullptr) {
        std::memcpy(var_dest, &fill_value, fill_size);
      } else {
        uint64_t cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var_size - (tile_offsets[cell_idx] - tile_offsets[0]);
        std::memcpy(
            var_dest,
            &tile_var_data[tile_offsets[cell_idx] - tile_offsets[0]],
            cell_var_size);
      }
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(attr_buffers_[attribute].buffer_size_) = total_offset_size;
  *(attr_buffers_[attribute].buffer_var_size_) = total_var_size;
  STATS_COUNTER_ADD(
      reader_num_var_cell_bytes_copied, total_offset_size + total_var_size);

  return Status::Ok();

  STATS_FUNC_OUT(reader_copy_var_cells);
}

Status Reader::compute_var_cell_destinations(
    const std::string& attribute,
    const OverlappingCellRangeList& cell_ranges,
    std::vector<std::vector<uint64_t>>* offset_offsets_per_cr,
    std::vector<std::vector<uint64_t>>* var_offsets_per_cr,
    uint64_t* total_offset_size,
    uint64_t* total_var_size) const {
  // For easy reference
  auto num_cr = cell_ranges.size();
  auto offset_size = constants::cell_var_offset_size;
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);

  // Resize the output vectors
  offset_offsets_per_cr->resize(num_cr);
  var_offsets_per_cr->resize(num_cr);

  // Compute the destinations for all cell ranges.
  *total_offset_size = 0;
  *total_var_size = 0;
  for (uint64_t cr_idx = 0; cr_idx < num_cr; cr_idx++) {
    const auto& cr = cell_ranges[cr_idx];
    auto cell_num_in_range = cr.end_ - cr.start_ + 1;
    (*offset_offsets_per_cr)[cr_idx].resize(cell_num_in_range);
    (*var_offsets_per_cr)[cr_idx].resize(cell_num_in_range);

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cr.tile_ != nullptr) {
      const auto& tile_pair = cr.tile_->attr_tiles_.find(attribute)->second;
      const auto& tile = tile_pair.first;
      const auto& tile_var = tile_pair.second;
      tile_offsets = (uint64_t*)tile.data();
      tile_cell_num = tile.cell_num();
      tile_var_size = tile_var.size();
    }

    // Compute the destinations for each cell in the range.
    for (auto cell_idx = cr.start_; cell_idx <= cr.end_; cell_idx++) {
      uint64_t dest_vec_idx = cell_idx - cr.start_;
      // Get size of variable-sized cell
      uint64_t cell_var_size = 0;
      if (cr.tile_ == nullptr) {
        cell_var_size = fill_size;
      } else {
        cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var_size - (tile_offsets[cell_idx] - tile_offsets[0]);
      }

      // Record destination offsets.
      (*offset_offsets_per_cr)[cr_idx][dest_vec_idx] = *total_offset_size;
      (*var_offsets_per_cr)[cr_idx][dest_vec_idx] = *total_var_size;
      *total_offset_size += offset_size;
      *total_var_size += cell_var_size;
    }
  }

  return Status::Ok();
}

template <class T>
Status Reader::dedup_coords(OverlappingCoordsList<T>* coords) const {
  STATS_FUNC_IN(reader_dedup_coords);

  auto coords_size = array_schema_->coords_size();
  auto coords_end = coords->end();
  auto it = skip_invalid_elements(coords->begin(), coords_end);
  while (it != coords_end) {
    auto next_it = skip_invalid_elements(std::next(it), coords_end);
    if (next_it != coords_end &&
        !std::memcmp(it->coords_, next_it->coords_, coords_size)) {
      if (it->tile_->fragment_idx_ < next_it->tile_->fragment_idx_) {
        it->invalidate();
        it = skip_invalid_elements(++it, coords_end);
      } else {
        next_it->invalidate();
      }
    } else {
      it = skip_invalid_elements(++it, coords_end);
    }
  }
  return Status::Ok();

  STATS_FUNC_OUT(reader_dedup_coords);
}

Status Reader::dense_read() {
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return dense_read<int8_t>();
    case Datatype::UINT8:
      return dense_read<uint8_t>();
    case Datatype::INT16:
      return dense_read<int16_t>();
    case Datatype::UINT16:
      return dense_read<uint16_t>();
    case Datatype::INT32:
      return dense_read<int>();
    case Datatype::UINT32:
      return dense_read<unsigned>();
    case Datatype::INT64:
      return dense_read<int64_t>();
    case Datatype::UINT64:
      return dense_read<uint64_t>();
    default:
      return LOG_STATUS(
          Status::ReaderError("Cannot read; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Reader::dense_read() {
  STATS_FUNC_IN(reader_dense_read);

  // For easy reference
  auto domain = array_schema_->domain();
  auto subarray_len = 2 * array_schema_->dim_num();
  std::vector<T> subarray;
  subarray.resize(subarray_len);
  for (size_t i = 0; i < subarray_len; ++i)
    subarray[i] = ((T*)read_state_.cur_subarray_partition_)[i];

  // Get overlapping sparse tile indexes
  OverlappingTileVec sparse_tiles;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_tiles<T>(&sparse_tiles));

  // Read sparse tiles
  RETURN_CANCEL_OR_ERROR(read_all_tiles(&sparse_tiles));

  // Filter sparse tiles
  RETURN_CANCEL_OR_ERROR(filter_all_tiles(&sparse_tiles));

  // Compute the read coordinates for all sparse fragments
  OverlappingCoordsList<T> coords;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_coords<T>(sparse_tiles, &coords));

  // Compute the tile coordinates for all overlapping coordinates (for sorting).
  std::unique_ptr<T[]> tile_coords(nullptr);
  RETURN_CANCEL_OR_ERROR(compute_tile_coords<T>(&tile_coords, &coords));

  // Sort and dedup the coordinates (not applicable to the global order
  // layout for a single fragment)
  if (!(fragment_metadata_.size() == 1 && layout_ == Layout::GLOBAL_ORDER)) {
    RETURN_CANCEL_OR_ERROR(sort_coords<T>(&coords));
    RETURN_CANCEL_OR_ERROR(dedup_coords<T>(&coords));
  }
  tile_coords.reset(nullptr);

  // For each tile, initialize a dense cell range iterator per
  // (dense) fragment
  std::vector<std::vector<DenseCellRangeIter<T>>> dense_frag_its;
  std::unordered_map<uint64_t, std::pair<uint64_t, std::vector<T>>>
      overlapping_tile_idx_coords;
  RETURN_CANCEL_OR_ERROR(init_tile_fragment_dense_cell_range_iters(
      &dense_frag_its, &overlapping_tile_idx_coords));

  // Get the cell ranges
  std::list<DenseCellRange<T>> dense_cell_ranges;
  DenseCellRangeIter<T> it(domain, subarray, layout_);
  RETURN_CANCEL_OR_ERROR(it.begin());
  while (!it.end()) {
    auto o_it = overlapping_tile_idx_coords.find(it.tile_idx());
    assert(o_it != overlapping_tile_idx_coords.end());
    RETURN_CANCEL_OR_ERROR(compute_dense_cell_ranges<T>(
        &(o_it->second.second)[0],
        dense_frag_its[o_it->second.first],
        it.range_start(),
        it.range_end(),
        &dense_cell_ranges));
    ++it;
  }

  // Compute overlapping dense tile indexes
  OverlappingTileVec dense_tiles;
  OverlappingCellRangeList overlapping_cell_ranges;
  RETURN_CANCEL_OR_ERROR(compute_dense_overlapping_tiles_and_cell_ranges<T>(
      dense_cell_ranges, coords, &dense_tiles, &overlapping_cell_ranges));
  coords.clear();
  dense_cell_ranges.clear();
  overlapping_tile_idx_coords.clear();

  // Read dense tiles
  RETURN_CANCEL_OR_ERROR(read_all_tiles(&dense_tiles, false));

  // Filter dense tiles
  RETURN_CANCEL_OR_ERROR(filter_all_tiles(&dense_tiles, false));

  // Copy cells
  for (const auto& attr : attributes_) {
    if (read_state_.overflowed_)
      break;

    if (attr != constants::coords)  // Skip coordinates
      RETURN_CANCEL_OR_ERROR(copy_cells(attr, overlapping_cell_ranges));
  }

  // Fill coordinates if the user requested them
  if (!read_state_.overflowed_ && has_coords())
    RETURN_CANCEL_OR_ERROR(fill_coords<T>());

  return Status::Ok();

  STATS_FUNC_OUT(reader_dense_read);
}

template <class T>
Status Reader::fill_coords() {
  STATS_FUNC_IN(reader_fill_coords);

  // For easy reference
  auto it = attr_buffers_.find(constants::coords);
  assert(it != attr_buffers_.end());
  auto coords_buff = it->second.buffer_;
  auto coords_buff_offset = (uint64_t)0;
  auto coords_buff_size = *(it->second.buffer_size_);
  auto domain = array_schema_->domain();
  auto cell_order = array_schema_->cell_order();
  auto subarray_len = 2 * array_schema_->dim_num();
  auto coords_size = array_schema_->coords_size();
  std::vector<T> subarray;
  subarray.resize(subarray_len);
  for (size_t i = 0; i < subarray_len; ++i)
    subarray[i] = ((T*)read_state_.cur_subarray_partition_)[i];

  // Iterate over all coordinates, retrieved in cell slabs
  DenseCellRangeIter<T> cell_it(domain, subarray, layout_);
  RETURN_CANCEL_OR_ERROR(cell_it.begin());
  while (!cell_it.end()) {
    auto coords_num = cell_it.range_end() - cell_it.range_start() + 1;

    // Check for overflow
    if (coords_num * coords_size + coords_buff_offset > coords_buff_size) {
      read_state_.overflowed_ = true;
      return Status::Ok();
    }

    if (layout_ == Layout::ROW_MAJOR ||
        (layout_ == Layout::GLOBAL_ORDER && cell_order == Layout::ROW_MAJOR))
      fill_coords_row_slab(
          cell_it.coords_start(), coords_num, coords_buff, &coords_buff_offset);
    else
      fill_coords_col_slab(
          cell_it.coords_start(), coords_num, coords_buff, &coords_buff_offset);
    ++cell_it;
  }

  // Update the coords buffer size
  *(it->second.buffer_size_) = coords_buff_offset;

  return Status::Ok();

  STATS_FUNC_OUT(reader_fill_coords);
}

template <class T>
void Reader::fill_coords_row_slab(
    const T* start, uint64_t num, void* buff, uint64_t* offset) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  assert(dim_num > 0);
  auto c_buff = (char*)buff;

  // Fill coordinates
  for (uint64_t i = 0; i < num; ++i) {
    // First dim-1 dimensions are copied as they are
    if (dim_num > 1) {
      auto bytes_to_copy = (dim_num - 1) * sizeof(T);
      std::memcpy(c_buff + *offset, start, bytes_to_copy);
      *offset += bytes_to_copy;
    }

    // Last dimension is incremented by `i`
    auto new_coord = start[dim_num - 1] + i;
    std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
    *offset += sizeof(T);
  }
}

template <class T>
void Reader::fill_coords_col_slab(
    const T* start, uint64_t num, void* buff, uint64_t* offset) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  assert(dim_num > 0);
  auto c_buff = (char*)buff;

  // Fill coordinates
  for (uint64_t i = 0; i < num; ++i) {
    // First dimension is incremented by `i`
    auto new_coord = start[0] + i;
    std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
    *offset += sizeof(T);

    // Last dim-1 dimensions are copied as they are
    if (dim_num > 1) {
      auto bytes_to_copy = (dim_num - 1) * sizeof(T);
      std::memcpy(c_buff + *offset, &start[1], bytes_to_copy);
      *offset += bytes_to_copy;
    }
  }
}

Status Reader::filter_all_tiles(
    OverlappingTileVec* tiles, bool ensure_coords) const {
  if (tiles->empty())
    return Status::Ok();

  // Prepare attributes
  std::set<std::string> all_attributes;
  for (const auto& attr : attributes_) {
    if (array_schema_->dense() && attr == constants::coords)
      continue;  // Skip coords in dense case - no actual tiles to filter
    all_attributes.insert(attr);
  }

  // Make sure the coordinate tiles are filtered if specified.
  if (ensure_coords)
    all_attributes.insert(constants::coords);

  // Filter the tiles in parallel over the attributes.
  auto statuses = parallel_for_each(
      all_attributes.begin(),
      all_attributes.end(),
      [this, &tiles](const std::string& attr) {
        RETURN_CANCEL_OR_ERROR(filter_tiles(attr, tiles));
        return Status::Ok();
      });

  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

Status Reader::filter_tiles(
    const std::string& attribute, OverlappingTileVec* tiles) const {
  STATS_FUNC_IN(reader_filter_tiles);

  auto var_size = array_schema_->var_size(attribute);
  auto num_tiles = static_cast<uint64_t>(tiles->size());
  auto statuses = parallel_for(0, num_tiles, [&, this](uint64_t i) {
    auto& tile = (*tiles)[i];
    auto it = tile->attr_tiles_.find(attribute);
    // Skip non-existent attributes (e.g. coords in the dense case).
    if (it == tile->attr_tiles_.end())
      return Status::Ok();

    // Get information about the tile in its fragment
    auto& fragment = fragment_metadata_[tile->fragment_idx_];
    auto tile_attr_uri = fragment->attr_uri(attribute);
    auto tile_attr_offset = fragment->file_offset(attribute, tile->tile_idx_);

    auto& tile_pair = it->second;
    auto& t = tile_pair.first;
    auto& t_var = tile_pair.second;

    if (!t.filtered()) {
      // Decompress, etc.
      RETURN_NOT_OK(filter_tile(attribute, &t, var_size));
      RETURN_NOT_OK(storage_manager_->write_to_cache(
          tile_attr_uri, tile_attr_offset, t.buffer()));
    }

    if (var_size && !t_var.filtered()) {
      auto tile_attr_var_uri = fragment->attr_var_uri(attribute);
      auto tile_attr_var_offset =
          fragment->file_var_offset(attribute, tile->tile_idx_);

      // Decompress, etc.
      RETURN_NOT_OK(filter_tile(attribute, &t_var, false));
      RETURN_NOT_OK(storage_manager_->write_to_cache(
          tile_attr_var_uri, tile_attr_var_offset, t_var.buffer()));
    }

    return Status::Ok();
  });

  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();

  STATS_FUNC_OUT(reader_filter_tiles);
}

Status Reader::filter_tile(
    const std::string& attribute, Tile* tile, bool offsets) const {
  uint64_t orig_size = tile->buffer()->size();

  // Get a copy of the appropriate filter pipeline.
  FilterPipeline filters;
  if (tile->stores_coords()) {
    filters = *array_schema_->coords_filters();
  } else if (offsets) {
    filters = *array_schema_->cell_var_offsets_filters();
  } else {
    filters = *array_schema_->filters(attribute);
  }

  // Append an encryption filter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  RETURN_NOT_OK(filters.run_reverse(tile));

  tile->set_filtered(true);
  tile->set_pre_filtered_size(orig_size);

  return Status::Ok();
}

template <class T>
Status Reader::get_all_coords(
    const OverlappingTile* tile, OverlappingCoordsList<T>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto coords_num = t.cell_num();
  auto c = (T*)t.data();

  for (uint64_t i = 0; i < coords_num; ++i)
    coords->emplace_back(tile, &c[i * dim_num], i);

  return Status::Ok();
}

template <class T>
Status Reader::handle_coords_in_dense_cell_range(
    const OverlappingTile* cur_tile,
    const T* cur_tile_coords,
    uint64_t* start,
    uint64_t end,
    uint64_t coords_size,
    const OverlappingCoordsList<T>& coords,
    typename OverlappingCoordsList<T>::const_iterator* coords_it,
    const OverlappingTile* coords_tile,
    uint64_t* coords_pos,
    unsigned* coords_fidx,
    std::vector<T>* coords_tile_coords,
    OverlappingCellRangeList* overlapping_cell_ranges) const {
  auto domain = array_schema_->domain();
  auto coords_end = coords.end();

  // While the coords are within the same dense cell range
  while (*coords_it != coords_end &&
         !memcmp(&(*coords_tile_coords)[0], cur_tile_coords, coords_size) &&
         *coords_pos >= *start && *coords_pos <= end) {
    // Check if the coords must be skipped
    if (cur_tile != nullptr && *coords_fidx < cur_tile->fragment_idx_) {
      *coords_it = skip_invalid_elements(++(*coords_it), coords_end);
      if (*coords_it != coords_end) {
        domain->get_tile_coords(
            (*coords_it)->coords_, &(*coords_tile_coords)[0]);
        RETURN_NOT_OK(
            domain->get_cell_pos<T>((*coords_it)->coords_, coords_pos));
        *coords_fidx = (*coords_it)->tile_->fragment_idx_;
        coords_tile = (*coords_it)->tile_;
      }
      continue;
    } else {  // Break dense range
      // Left range
      if (*coords_pos > *start) {
        overlapping_cell_ranges->emplace_back(
            cur_tile, *start, *coords_pos - 1);
      }
      // Coords unary range
      overlapping_cell_ranges->emplace_back(
          coords_tile, (*coords_it)->pos_, (*coords_it)->pos_);
      // Update start
      *start = *coords_pos + 1;

      // Advance coords
      *coords_it = skip_invalid_elements(++(*coords_it), coords_end);
      if (*coords_it != coords_end) {
        domain->get_tile_coords(
            (*coords_it)->coords_, &(*coords_tile_coords)[0]);
        RETURN_NOT_OK(
            domain->get_cell_pos<T>((*coords_it)->coords_, coords_pos));
        *coords_fidx = (*coords_it)->tile_->fragment_idx_;
        coords_tile = (*coords_it)->tile_;
      }
    }
  }

  return Status::Ok();
}

bool Reader::has_coords() const {
  return attr_buffers_.find(constants::coords) != attr_buffers_.end();
}

Status Reader::init_read_state() {
  auto subarray_size = 2 * array_schema_->coords_size();
  read_state_.cur_subarray_partition_ = std::malloc(subarray_size);
  if (read_state_.cur_subarray_partition_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize read state; Memory allocation failed"));

  auto first_partition = std::malloc(subarray_size);
  if (first_partition == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize read state; Memory allocation failed"));

  std::memcpy(first_partition, read_state_.subarray_, subarray_size);
  read_state_.subarray_partitions_.push_back(first_partition);

  RETURN_NOT_OK(next_subarray_partition());

  // If there is no next subarray partition, then the original subarray is
  // not splittable. Set the current subarray to the original subarray
  if (read_state_.cur_subarray_partition_ == nullptr) {
    read_state_.cur_subarray_partition_ = std::malloc(subarray_size);
    if (read_state_.cur_subarray_partition_ == nullptr)
      return LOG_STATUS(Status::ReaderError(
          "Cannot initialize read state; Memory allocation failed"));
    std::memcpy(
        read_state_.cur_subarray_partition_,
        read_state_.subarray_,
        subarray_size);
  }

  read_state_.initialized_ = true;

  return Status::Ok();
}

Status Reader::init_tile(const std::string& attribute, Tile* tile) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto cell_size = array_schema_->cell_size(attribute);
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto is_coords = (attribute == constants::coords);
  auto dim_num = (is_coords) ? array_schema_->dim_num() : 0;
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init(type, tile_size, cell_size, dim_num));

  return Status::Ok();
}

Status Reader::init_tile(
    const std::string& attribute, Tile* tile, Tile* tile_var) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init(type, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

template <class T>
Status Reader::init_tile_fragment_dense_cell_range_iters(
    std::vector<std::vector<DenseCellRangeIter<T>>>* iters,
    std::unordered_map<uint64_t, std::pair<uint64_t, std::vector<T>>>*
        overlapping_tile_idx_coords) {
  STATS_FUNC_IN(reader_init_tile_fragment_dense_cell_range_iters);

  // For easy reference
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  auto fragment_num = fragment_metadata_.size();
  std::vector<T> subarray;
  subarray.resize(2 * dim_num);
  for (unsigned i = 0; i < 2 * dim_num; ++i)
    subarray[i] = ((T*)read_state_.cur_subarray_partition_)[i];

  // Compute tile domain and current tile coords
  std::vector<T> tile_domain, tile_coords;
  tile_domain.resize(2 * dim_num);
  tile_coords.resize(dim_num);
  domain->get_tile_domain(&subarray[0], &tile_domain[0]);
  for (unsigned i = 0; i < dim_num; ++i)
    tile_coords[i] = tile_domain[2 * i];
  auto tile_num = domain->tile_num<T>(&subarray[0]);

  // Iterate over all tiles in the tile domain
  iters->clear();
  overlapping_tile_idx_coords->clear();
  std::vector<T> tile_subarray, subarray_in_tile;
  std::vector<T> frag_subarray, frag_subarray_in_tile;
  tile_subarray.resize(2 * dim_num);
  subarray_in_tile.resize(2 * dim_num);
  frag_subarray_in_tile.resize(2 * dim_num);
  frag_subarray.resize(2 * dim_num);
  bool tile_overlap, in;
  uint64_t tile_idx;
  for (uint64_t i = 0; i < tile_num; ++i) {
    // Compute subarray overlap with tile
    domain->get_tile_subarray(&tile_coords[0], &tile_subarray[0]);
    utils::geometry::overlap(
        &subarray[0],
        &tile_subarray[0],
        dim_num,
        &subarray_in_tile[0],
        &tile_overlap);
    tile_idx = domain->get_tile_pos(&tile_coords[0]);
    (*overlapping_tile_idx_coords)[tile_idx] =
        std::pair<uint64_t, std::vector<T>>(i, tile_coords);

    // Initialize fragment iterators. For sparse fragments, the constructed
    // iterator will always be at its end.
    std::vector<DenseCellRangeIter<T>> frag_iters;
    for (unsigned j = 0; j < fragment_num; ++j) {
      if (!fragment_metadata_[j]->dense()) {  // Sparse fragment
        frag_iters.emplace_back();
      } else {  // Dense fragment
        auto frag_domain = (T*)fragment_metadata_[j]->non_empty_domain();
        for (unsigned k = 0; k < 2 * dim_num; ++k)
          frag_subarray[k] = frag_domain[k];
        utils::geometry::overlap(
            &subarray_in_tile[0],
            &frag_subarray[0],
            dim_num,
            &frag_subarray_in_tile[0],
            &tile_overlap);

        if (tile_overlap) {
          frag_iters.emplace_back(domain, frag_subarray_in_tile, layout_);
          RETURN_NOT_OK(frag_iters.back().begin());
        } else {
          frag_iters.emplace_back();
        }
      }
    }
    iters->push_back(std::move(frag_iters));

    // Get next tile coordinates
    domain->get_next_tile_coords(&tile_domain[0], &tile_coords[0], &in);
    assert((i != tile_num - 1 && in) || (i == tile_num - 1 && !in));
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_init_tile_fragment_dense_cell_range_iters);
}

void Reader::optimize_layout_for_1D() {
  if (array_schema_->dim_num() == 1)
    layout_ = Layout::GLOBAL_ORDER;
}

Status Reader::read_all_tiles(
    OverlappingTileVec* tiles, bool ensure_coords) const {
  STATS_FUNC_IN(reader_read_all_tiles);

  // Shortcut for empty tile vec
  if (tiles->empty())
    return Status::Ok();

  // Prepare attributes
  std::set<std::string> all_attributes;
  for (const auto& attr : attributes_) {
    if (array_schema_->dense() && attr == constants::coords)
      continue;  // Skip coords in dense case - no actual tiles to read
    all_attributes.insert(attr);
  }

  // Make sure the coordinate tiles are read if specified.
  if (ensure_coords)
    all_attributes.insert(constants::coords);

  // Read the tiles asynchronously.
  std::vector<std::future<Status>> tasks;
  for (const auto& attr : all_attributes)
    RETURN_CANCEL_OR_ERROR(read_tiles(attr, tiles, &tasks));

  // Wait for the reads to finish and check statuses.
  auto statuses =
      storage_manager_->reader_thread_pool()->wait_all_status(tasks);
  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();

  STATS_FUNC_OUT(reader_read_all_tiles);
}

Status Reader::read_tiles(
    const std::string& attribute,
    OverlappingTileVec* tiles,
    std::vector<std::future<Status>>* tasks) const {
  // For each tile, read from its fragment.
  bool var_size = array_schema_->var_size(attribute);
  auto num_tiles = static_cast<uint64_t>(tiles->size());

  for (uint64_t i = 0; i < num_tiles; i++) {
    auto& tile = (*tiles)[i];
    auto it = tile->attr_tiles_.find(attribute);
    if (it == tile->attr_tiles_.end()) {
      return LOG_STATUS(
          Status::ReaderError("Invalid tile map for attribute " + attribute));
    }

    // Initialize the tile(s)
    auto& tile_pair = it->second;
    auto& t = tile_pair.first;
    auto& t_var = tile_pair.second;
    if (!var_size) {
      RETURN_NOT_OK(init_tile(attribute, &t));
    } else {
      RETURN_NOT_OK(init_tile(attribute, &t, &t_var));
    }

    // Enqueue the read task in the Reader thread pool.
    auto task = storage_manager_->reader_thread_pool()->enqueue([&attribute,
                                                                 &tile,
                                                                 &t,
                                                                 &t_var,
                                                                 var_size,
                                                                 this]() {
      // Get information about the tile in its fragment
      auto& fragment = fragment_metadata_[tile->fragment_idx_];
      auto tile_attr_uri = fragment->attr_uri(attribute);
      auto tile_attr_offset = fragment->file_offset(attribute, tile->tile_idx_);
      auto tile_size = fragment->tile_size(attribute, tile->tile_idx_);
      auto tile_persisted_size =
          fragment->persisted_tile_size(attribute, tile->tile_idx_);

      // Try the cache first.
      bool cache_hit;
      RETURN_NOT_OK(storage_manager_->read_from_cache(
          tile_attr_uri, tile_attr_offset, t.buffer(), tile_size, &cache_hit));
      if (cache_hit) {
        t.set_filtered(true);
        STATS_COUNTER_ADD(reader_attr_tile_cache_hits, 1);
      } else {
        // Read from disk if it missed.
        RETURN_NOT_OK(storage_manager_->read(
            tile_attr_uri, tile_attr_offset, t.buffer(), tile_persisted_size));
        STATS_COUNTER_ADD(reader_num_tile_bytes_read, tile_persisted_size);
      }

      if (var_size) {
        auto tile_attr_var_uri = fragment->attr_var_uri(attribute);
        auto tile_attr_var_offset =
            fragment->file_var_offset(attribute, tile->tile_idx_);
        auto tile_var_size =
            fragment->tile_var_size(attribute, tile->tile_idx_);
        auto tile_var_persisted_size =
            fragment->persisted_tile_var_size(attribute, tile->tile_idx_);

        RETURN_NOT_OK(storage_manager_->read_from_cache(
            tile_attr_var_uri,
            tile_attr_var_offset,
            t_var.buffer(),
            tile_var_size,
            &cache_hit));

        if (cache_hit) {
          t_var.set_filtered(true);
          STATS_COUNTER_ADD(reader_attr_tile_cache_hits, 1);
        } else {
          RETURN_NOT_OK(storage_manager_->read(
              tile_attr_var_uri,
              tile_attr_var_offset,
              t_var.buffer(),
              tile_var_persisted_size));
          STATS_COUNTER_ADD(
              reader_num_tile_bytes_read, tile_var_persisted_size);
          STATS_COUNTER_ADD(reader_num_var_cell_bytes_read, t.size());
          STATS_COUNTER_ADD(reader_num_var_cell_bytes_read, t_var.size());
        }
      } else {
        STATS_COUNTER_ADD_IF(
            !cache_hit, reader_num_fixed_cell_bytes_read, t.size());
      }

      return Status::Ok();
    });

    tasks->push_back(std::move(task));
  }

  STATS_COUNTER_ADD(reader_num_attr_tiles_touched, num_tiles);

  return Status::Ok();
}

void Reader::reset_buffer_sizes() {
  for (auto& it : attr_buffers_) {
    *(it.second.buffer_size_) = it.second.original_buffer_size_;
    if (it.second.buffer_var_size_ != nullptr)
      *(it.second.buffer_var_size_) = it.second.original_buffer_var_size_;
  }
}

template <class T>
Status Reader::sort_coords(OverlappingCoordsList<T>* coords) const {
  STATS_FUNC_IN(reader_sort_coords);

  if (layout_ == Layout::GLOBAL_ORDER) {
    auto domain = array_schema_->domain();
    parallel_sort(coords->begin(), coords->end(), GlobalCmp<T>(domain));
  } else {
    auto dim_num = array_schema_->dim_num();
    if (layout_ == Layout::ROW_MAJOR)
      parallel_sort(coords->begin(), coords->end(), RowCmp<T>(dim_num));
    else if (layout_ == Layout::COL_MAJOR)
      parallel_sort(coords->begin(), coords->end(), ColCmp<T>(dim_num));
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_sort_coords);
}

Status Reader::sparse_read() {
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return sparse_read<int8_t>();
    case Datatype::UINT8:
      return sparse_read<uint8_t>();
    case Datatype::INT16:
      return sparse_read<int16_t>();
    case Datatype::UINT16:
      return sparse_read<uint16_t>();
    case Datatype::INT32:
      return sparse_read<int>();
    case Datatype::UINT32:
      return sparse_read<unsigned>();
    case Datatype::INT64:
      return sparse_read<int64_t>();
    case Datatype::UINT64:
      return sparse_read<uint64_t>();
    case Datatype::FLOAT32:
      return sparse_read<float>();
    case Datatype::FLOAT64:
      return sparse_read<double>();
    default:
      return LOG_STATUS(
          Status::ReaderError("Cannot read; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Reader::sparse_read() {
  STATS_FUNC_IN(reader_sparse_read);

  // Get overlapping tile indexes
  OverlappingTileVec tiles;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_tiles<T>(&tiles));

  // Read tiles
  RETURN_CANCEL_OR_ERROR(read_all_tiles(&tiles));

  // Filter tiles
  RETURN_CANCEL_OR_ERROR(filter_all_tiles(&tiles));

  // Compute the read coordinates for all fragments
  OverlappingCoordsList<T> coords;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_coords<T>(tiles, &coords));

  // Compute the tile coordinates for all overlapping coordinates (for sorting).
  std::unique_ptr<T[]> tile_coords(nullptr);
  RETURN_CANCEL_OR_ERROR(compute_tile_coords<T>(&tile_coords, &coords));

  // Sort and dedup the coordinates (not applicable to the global order
  // layout for a single fragment)
  if (!(fragment_metadata_.size() == 1 && layout_ == Layout::GLOBAL_ORDER)) {
    RETURN_CANCEL_OR_ERROR(sort_coords<T>(&coords));
    RETURN_CANCEL_OR_ERROR(dedup_coords<T>(&coords));
  }
  tile_coords.reset(nullptr);

  // Compute the maximal cell ranges
  OverlappingCellRangeList cell_ranges;
  RETURN_CANCEL_OR_ERROR(compute_cell_ranges(coords, &cell_ranges));
  coords.clear();

  // Copy cells
  for (const auto& attr : attributes_) {
    if (read_state_.overflowed_)
      break;
    RETURN_CANCEL_OR_ERROR(copy_cells(attr, cell_ranges));
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_sparse_read);
}

void Reader::zero_out_buffer_sizes() {
  for (auto& attr_buffer : attr_buffers_) {
    if (attr_buffer.second.buffer_size_ != nullptr)
      *(attr_buffer.second.buffer_size_) = 0;
    if (attr_buffer.second.buffer_var_size_ != nullptr)
      *(attr_buffer.second.buffer_var_size_) = 0;
  }
}

}  // namespace sm
}  // namespace tiledb
