/**
 * @file enumeration.h
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
 * This file defines class Enumeration.
 */

#ifndef TILEDB_ENUMERATION_H
#define TILEDB_ENUMERATION_H

#include <iostream>

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/storage_format/serialization/serializers.h"

namespace tiledb::sm {

class MemoryTracker;

/** Defines an array enumeration */
class Enumeration {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** No default constructor. Use the create factory method. */
  Enumeration() = delete;

  DISABLE_COPY(Enumeration);
  DISABLE_MOVE(Enumeration);

  /** Destructor. */
  ~Enumeration() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  DISABLE_COPY_ASSIGN(Enumeration);
  DISABLE_MOVE_ASSIGN(Enumeration);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Create a new Enumeration
   *
   * @param name The name of this Enumeration as referenced by attributes.
   * @param type The datatype of the enumeration values.
   * @param cell_val_num The cell_val_num of the enumeration.
   * @param ordered Whether the enumeration should be considered as ordered.
   *        If false, prevents inequality operators in QueryConditons from
   *        being used with this enumeration.
   * @param data A pointer to the enumerations values.
   * @param data_size The length of the buffer pointed to by data.
   * @param offsets If cell_var_num is constants::var_num a pointer to the
   *        offsets buffer. Must be null if cell_var_num is not var_num.
   * @param offsets_size The size of the buffer pointed to by offsets. Must be
   *        zero of cell_var_num is not var_num.
   * @param memory_tracker The memory tracker associated with this Enumeration.
   * @return shared_ptr<Enumeration> The created enumeration.
   */
  static shared_ptr<const Enumeration> create(
      const std::string& name,
      Datatype type,
      uint32_t cell_val_num,
      bool ordered,
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size,
      shared_ptr<MemoryTracker> memory_tracker) {
    return create(
        name,
        "",
        type,
        cell_val_num,
        ordered,
        data,
        data_size,
        offsets,
        offsets_size,
        memory_tracker);
  }

  /** Create a new Enumeration
   *
   * @param name The name of this Enumeration as referenced by attributes.
   * @param path_name The last URI path component of the Enumeration.
   * @param type The datatype of the enumeration values.
   * @param cell_val_num The cell_val_num of the enumeration.
   * @param ordered Whether the enumeration should be considered as ordered.
   *        If false, prevents inequality operators in QueryConditons from
   *        being used with this enumeration.
   * @param data A pointer to the enumerations values.
   * @param data_size The length of the buffer pointed to by data.
   * @param offsets If cell_var_num is constants::var_num a pointer to the
   *        offsets buffer. Must be null if cell_var_num is not var_num.
   * @param offsets_size The size of the buffer pointed to by offsets. Must be
   *        zero of cell_var_num is not var_num.
   * @param memory_tracker The memory tracker associated with this Enumeration.
   * @return shared_ptr<Enumeration> The created enumeration.
   */
  static shared_ptr<const Enumeration> create(
      const std::string& name,
      const std::string& path_name,
      Datatype type,
      uint32_t cell_val_num,
      bool ordered,
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size,
      shared_ptr<MemoryTracker> memory_tracker) {
    return make_shared_enumeration(
        HERE(),
        name,
        path_name,
        type,
        cell_val_num,
        ordered,
        data,
        data_size,
        offsets,
        offsets_size,
        memory_tracker);
  }

  /** Create a new Enumeration
   *
   * @param name The name of this Enumeration as referenced by attributes.
   * @param path_name The last URI path component of the Enumeration.
   * @param type The datatype of the enumeration values.
   * @param cell_val_num The cell_val_num of the enumeration.
   * @param ordered Whether the enumeration should be considered as ordered.
   *        If false, prevents inequality operators in QueryConditons from
   *        being used with this enumeration.
   * @param data A pointer to the enumerations values.
   * @param offsets If cell_var_num is constants::var_num a pointer to the
   *        offsets buffer. Must be null if cell_var_num is not var_num.
   * @param memory_tracker The memory tracker associated with this Enumeration.
   * @return shared_ptr<Enumeration> The created enumeration.
   */
  static shared_ptr<const Enumeration> create(
      const std::string& name,
      const std::string& path_name,
      Datatype type,
      uint32_t cell_val_num,
      bool ordered,
      Buffer&& data,
      Buffer&& offsets,
      shared_ptr<MemoryTracker> memory_tracker) {
    return make_shared_enumeration(
        HERE(),
        name,
        path_name,
        type,
        cell_val_num,
        ordered,
        std::move(data),
        std::move(offsets),
        memory_tracker);
  }

  /**
   * Deserialize an enumeration
   *
   * @param deserializer The deserializer to deserialize from.
   * @param memory_tracker The memory tracker associated with this Enumeration.
   * @return A new Enumeration.
   */
  static shared_ptr<const Enumeration> deserialize(
      Deserializer& deserializer, shared_ptr<MemoryTracker> memory_tracker);

  /**
   * Create a new enumeration by extending an existing enumeration's
   * list of values.
   *
   * The returned Enumeration can then be used by the
   * ArraySchemaEvolution::extend_enumeration to update the schema.
   *
   * @param data A pointer to the enumerations values to add.
   * @param data_size The length of the buffer pointed to by data.
   * @param offsets A pointer to a buffer of offsets data. Must be provided
   *        if and only if the enumeration being extended is var sized.
   * @param offsets_size The size of the offsets buffer or zero if no buffer
   *        is supplied.
   * @return shared_ptr<Enumeration> The extended enumeration.
   */
  shared_ptr<const Enumeration> extend(
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size) const;

  /**
   * Check if this enumeration is an extension of the provided Enumeration.
   *
   * @return bool Whether this enumeration is an extension or not.
   */
  bool is_extension_of(shared_ptr<const Enumeration> other) const;

  /**
   * Serializes the enumeration into a buffer.
   *
   * @param serializer The object the array schema is serialized into.
   */
  void serialize(Serializer& serializer) const;

  /**
   * The name of this Enumeration referenced by attributes.
   *
   * @return The name of this Enumeration.
   */
  const std::string& name() const {
    return name_;
  }

  /**
   * The path name for this Enumeration on disk.
   *
   * @return The path name of this Enumeration.
   */
  const std::string& path_name() const {
    return path_name_;
  }

  /**
   * The type of the enumeration values
   *
   * @return Datatype The datatype of the enumeration values.
   */
  Datatype type() const {
    return type_;
  }

  /**
   * The cell_val_num of the enumeration
   *
   * @return uint32_t The cell_val_num of the enumeration.
   */
  uint32_t cell_val_num() const {
    return cell_val_num_;
  }

  /**
   * Get the value map of the enumeration
   * @return const tdb::pmr::unordered_map<std::string_view, uint64_t>& The
   *        value map of the enumeration.
   */
  const tdb::pmr::unordered_map<std::string_view, uint64_t>& value_map() const {
    return value_map_;
  }

  /**
   * Get the cell_size of the enumeration
   *
   * This returns constants::var_size when cell_val_num is var_num, otherwise
   * it returns the cell_val_num * datatype_size(type())
   *
   * @return uint64_t The cell size of this enumeration.
   */
  uint64_t cell_size() const {
    if (var_size()) {
      return constants::var_size;
    }

    return cell_val_num_ * datatype_size(type_);
  }

  /**
   * Get the number of elements in the Enumeration.
   *
   * @return uint64_t The number of elements.
   */
  uint64_t elem_count() const {
    if (var_size()) {
      return offsets_.size() / sizeof(uint64_t);
    } else {
      return data_.size() / cell_size();
    }
  }

  /**
   * Returns whether this enumeration is variable sized.
   *
   * @return bool Whether this enumerations is variable sized.
   */
  bool var_size() const {
    return cell_val_num_ == constants::var_num;
  }

  /**
   * Returns whether this enumeration is considered ordered or not.
   *
   * If an enumeration is not ordered then inequality operators in
   * QueryConditions against this Enumeration are disabled.
   *
   * @return bool Whether this enumeration is considered ordered.
   */
  bool ordered() const {
    return ordered_;
  }

  /**
   * Returns the data buffer
   *
   * @return tuple<const void*, uint64_t> A pointer to the data buffer and the
             size of the buffer pointed to.
   */
  span<uint8_t> data() const {
    return {
        static_cast<uint8_t*>(data_.data()), static_cast<size_t>(data_.size())};
  }

  /**
   * Returns the offsets buffer
   *
   * @return tuple<const void*, uint64_t> A pointer to the offsets buffer and
   *         the size of the buffer pointed to.
   */
  span<uint8_t> offsets() const {
    return {
        static_cast<uint8_t*>(offsets_.data()),
        static_cast<size_t>(offsets_.size())};
  }

  /**
   * Return the index of a value in the enumeration
   *
   * @param data A pointer to the data buffer
   * @param size The size of of data buffer
   * @return uint64_t The index of the value represented by data or
   *         constants::missing_enumeration_value if not found.
   */
  uint64_t index_of(const void* data, uint64_t size) const;

 private:
  /* ********************************* */
  /*        PRIVATE CONSTRUCTORS       */
  /* ********************************* */

  /** Constructor
   *
   * @param name The name of this Enumeration as referenced by attributes.
   * @param path_name The last URI path component of the Enumeration.
   * @param type The datatype of the enumeration values.
   * @param cell_val_num The cell_val_num of the enumeration.
   * @param ordered Whether the enumeration should be considered as ordered.
   *        If false, prevents inequality operators in QueryConditons from
   *        being used with this enumeration.
   * @param data A pointer to the enumerations values.
   * @param data_size The length of the buffer pointed to by data.
   * @param offsets If cell_var_num is constants::var_num a pointer to the
   *        offsets buffer. Must be null if cell_var_num is not var_num.
   * @param offsets_size The size of the buffer pointed to by offsets. Must be
   *        zero of cell_var_num is not var_num.
   * @param memory_tracker The memory tracker.
   */
  Enumeration(
      const std::string& name,
      const std::string& path_name,
      Datatype type,
      uint32_t cell_val_num,
      bool ordered,
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size,
      shared_ptr<MemoryTracker> memory_tracker);

  /** Constructor
   *
   * @param name The name of this Enumeration as referenced by attributes.
   * @param path_name The last URI path component of the Enumeration.
   * @param type The datatype of the enumeration values.
   * @param cell_val_num The cell_val_num of the enumeration.
   * @param ordered Whether the enumeration should be considered as ordered.
   *        If false, prevents inequality operators in QueryConditons from
   *        being used with this enumeration.
   * @param data An rvalue reference to a Buffer containing the enumerations
   * values.
   * @param offsets An rvalue reference to a Buffer containing the enumerations
   * value offsets. Must be empty if cell_var_num is not var_num.
   * @param memory_tracker The memory tracker.
   */
  Enumeration(
      const std::string& name,
      const std::string& path_name,
      Datatype type,
      uint32_t cell_val_num,
      bool ordered,
      Buffer&& data,
      Buffer&& offsets,
      shared_ptr<MemoryTracker> memory_tracker);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The memory tracker of the Enumeration.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The name of this Enumeration stored in the enumerations directory. */
  std::string name_;

  /** The path name of this Enumeration as stored on disk. */
  std::string path_name_;

  /** The type of enumerated values */
  Datatype type_;

  /** Number of values per enumeration value */
  uint32_t cell_val_num_;

  /** A flag which enables or disables inequality comparisons */
  bool ordered_;

  /** The list of enumeration values */
  Buffer data_;

  /** The offsets of each enumeration value if var sized. */
  Buffer offsets_;

  /** Map of values to indices */
  tdb::pmr::unordered_map<std::string_view, uint64_t> value_map_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Helper function to create shared_ptr<Enumeration> objects.
   * @tparam Args Argument types for the Enumeration constructor
   * @param origin A string containing the palce from where this function is
   * called
   * @param args Arguments for the Enumeration constructor
   */
  template <typename... Args>
  static std::shared_ptr<Enumeration> make_shared_enumeration(
      const std::string_view& origin, Args&&... args) {
    struct shared_helper : public Enumeration {
      shared_helper(Args&&... args)
          : Enumeration(std::forward<Args>(args)...) {
      }
    };

    return make_shared<shared_helper>(origin, std::forward<Args>(args)...);
  }

  /** Populate the value_map_ */
  void generate_value_map();

  /**
   * Add a value to value_map_
   *
   * @param sv A string view of the data to add.
   * @param index The index of the data in the Enumeration.
   */
  void add_value_to_map(std::string_view& sv, uint64_t index);

  /**
   * Validate input data.
   */
  void validate(
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size);
};

}  // namespace tiledb::sm

#endif  // TILEDB_DOMAIN_H

/** Converts the filter into a string representation. */
std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::Enumeration& enumeration);
