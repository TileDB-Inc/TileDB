/**
 * @file   attribute.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class Attribute.
 */

#ifndef TILEDB_ATTRIBUTE_H
#define TILEDB_ATTRIBUTE_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::type {
class Range;
}

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class Enumeration;

enum class Compressor : uint8_t;
enum class Datatype : uint8_t;

/**
 * Manipulates a TileDB attribute.
 *
 * @invariant
 *
 * A valid `cell_val_num` depends on the Attribute datatype and ordering.
 * For `Datatype::ANY`, the only valid value is `constants::var_num`.
 * If the attribute is unordered, then all other datatypes support any value.
 * If the attribute is ordered, then an Attribute of `Datatype::STRING_ASCII`
 * must have `constants::var_num`, and all other datatypes must have 1.
 */
class Attribute {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Attribute() = delete;

  /**
   * Constructor.
   *
   * @param name The name of the attribute.
   * @param type The type of the attribute.
   *
   * @note The default number of values per cell is 1 for all datatypes except
   *     `ANY`, which is always variable-sized.
   */
  Attribute(const std::string& name, Datatype type, bool nullable = false);

  /**
   * Constructor.
   *
   * @param name The name of the attribute.
   * @param type The type of the attribute.
   * @param cell_val_num The cell number of the attribute.
   * @param order The ordering of the attribute.
   *
   * @throws if `cell_val_num` is invalid. See `set_cell_val_num`.
   */
  Attribute(
      const std::string& name,
      Datatype type,
      uint32_t cell_val_num,
      DataOrder order);

  /**
   * Constructor.
   *
   * @param name The name of the attribute.
   * @param type The type of the attribute.
   * @param nullable The nullable of the attribute.
   * @param cell_val_num The cell number of the attribute.
   * @param filter_pipeline The filters of the attribute.
   * @param fill_value The fill value of the attribute.
   * @param fill_value_validity The validity of fill_value.
   * @param order The order of the data stored in the attribute.
   *
   * @throws if `cell_val_num` is invalid. See `set_cell_val_num`.
   */
  Attribute(
      const std::string& name,
      Datatype type,
      bool nullable,
      uint32_t cell_val_num,
      const FilterPipeline& filter_pipeline,
      const ByteVecValue& fill_value,
      uint8_t fill_value_validity,
      DataOrder order = DataOrder::UNORDERED_DATA,
      std::optional<std::string> enumeration_name = nullopt);

  /**
   * Copy constructor is default.
   */
  Attribute(const Attribute&) = default;

  /**
   * Copy assignment is default.
   */
  Attribute& operator=(const Attribute&) = default;

  /**
   * Move constructor is default
   */
  Attribute(Attribute&&) = default;

  /**
   * Move assignment is default.
   */
  Attribute& operator=(Attribute&&) = default;

  /** Destructor. */
  ~Attribute() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * The attribute name.
   */
  [[nodiscard]] inline const std::string& name() const {
    return name_;
  }

  /**
   * The attribute type.
   */
  [[nodiscard]] inline Datatype type() const {
    return type_;
  }

  /** Returns the number of values per cell. */
  [[nodiscard]] inline unsigned int cell_val_num() const {
    return cell_val_num_;
  }

  /**
   * Returns `true` if this is a nullable attribute, and `false` otherwise.
   */
  [[nodiscard]] inline bool nullable() const {
    return nullable_;
  }

  /**
   * Returns *true* if this is a variable-sized attribute, and *false*
   * otherwise.
   */
  [[nodiscard]] inline bool var_size() const {
    return cell_val_num_ == constants::var_num;
  }

  /**
   * Returns the size in bytes of one cell for this attribute. If the attribute
   * is variable-sized, this function returns the size in bytes of an offset.
   */
  [[nodiscard]] inline uint64_t cell_size() const {
    if (var_size()) {
      return constants::var_size;
    }
    return cell_val_num_ * datatype_size(type_);
  }

  /** Returns the fill value. */
  [[nodiscard]] inline const ByteVecValue& fill_value() const {
    return fill_value_;
  }

  /** Returns the fill value validity. */
  [[nodiscard]] inline uint8_t fill_value_validity() const {
    return fill_value_validity_;
  }

  /**
   * Gets the fill value for the attribute. Applicable to
   * fixed-sized and var-sized attributes.
   */
  void get_fill_value(const void** value, uint64_t* size) const;

  /**
   * Gets the fill value for the nullable attribute. Applicable to
   * fixed-sized and var-sized attributes.
   */
  void get_fill_value(const void** value, uint64_t* size, uint8_t* valid) const;

  /** Returns the order of the data stored in this attribute. */
  [[nodiscard]] inline DataOrder order() const {
    return order_;
  }

  /** Returns the filter pipeline of this attribute. */
  const FilterPipeline& filters() const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param version The format spec version.
   * @return Attribute
   */
  static Attribute deserialize(Deserializer& deserializer, uint32_t version);

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param serializer The object the attribute is serialized into.
   * @param version The format spec version.
   */
  void serialize(Serializer& serializer, uint32_t version) const;

  /**
   * Sets the attribute number of values per cell.
   *
   * @throws AttributeException if `cell_val_num` is invalid. See class
   * documentation.
   *
   * @post `this->cell_val_num() == cell_val_num` if `cell_val_num` is
   * valid, and `this->cell_val_num()` is unchanged otherwise.
   */
  void set_cell_val_num(unsigned int cell_val_num);

  /** Sets the nullability for this attribute. */
  void set_nullable(bool nullable);

  /** Sets the filter pipeline for this attribute. */
  void set_filter_pipeline(const FilterPipeline& pipeline);

  /**
   * Sets the fill value for the attribute. Applicable to
   * both fixed-sized and var-sized attributes.
   */
  void set_fill_value(const void* value, uint64_t size);

  /**
   * Sets the fill value for the nullable attribute. Applicable to
   * both fixed-sized and var-sized attributes.
   */
  void set_fill_value(const void* value, uint64_t size, uint8_t valid);

  /** The default fill value. */
  static ByteVecValue default_fill_value(
      Datatype datatype, uint32_t cell_val_num);

  /** Set an enumeration for this attribute. */
  void set_enumeration_name(std::optional<std::string> enmr_name);

  /** Get the enumeration for this attribute. */
  std::optional<std::reference_wrapper<const std::string>>
  get_enumeration_name() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The attribute number of values per cell. */
  unsigned cell_val_num_;

  /** True if this attribute may be null. */
  bool nullable_;

  /** The attribute filter pipeline. */
  FilterPipeline filters_;

  /** The attribute name. */
  std::string name_;

  /** The attribute type. */
  Datatype type_;

  /** The fill value. */
  ByteVecValue fill_value_;

  /** The fill value validity, applicable only to nullable attributes. */
  uint8_t fill_value_validity_;

  /** The required order of the data stored in the attribute. */
  DataOrder order_;

  /** The name of the Enumeration to use for this attribute. */
  std::optional<std::string> enumeration_name_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
  /** Called to validate a cell val num for this attribute */
  void validate_cell_val_num(unsigned cell_val_num) const;

  /** Sets the default fill value. */
  void set_default_fill_value();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ATTRIBUTE_H

/** Converts the filter into a string representation. */
std::ostream& operator<<(std::ostream& os, const tiledb::sm::Attribute& a);
