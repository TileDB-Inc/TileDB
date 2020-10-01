/**
 * @file   attribute.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "tiledb/common/status.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;

enum class Compressor : uint8_t;
enum class Datatype : uint8_t;

/** Manipulates a TileDB attribute. */
class Attribute {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Attribute();

  /**
   * Constructor.
   *
   * @param name The name of the attribute.
   * @param type The type of the attribute.
   *
   * @note The default number of values per cell is 1 for all datatypes except
   *     `ANY`, which is always variable-sized.
   */
  Attribute(const std::string& name, Datatype type);

  /**
   * Constructor. It clones the input attribute.
   *
   * @param attr The attribute to be cloned.
   */
  explicit Attribute(const Attribute* attr);

  /** Destructor. */
  ~Attribute();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Returns the size in bytes of one cell for this attribute. If the attribute
   * is variable-sized, this function returns the size in bytes of an offset.
   */
  uint64_t cell_size() const;

  /** Returns the number of values per cell. */
  unsigned int cell_val_num() const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The format spec version.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff, uint32_t version);

  /** Dumps the attribute contents in ASCII form in the selected output. */
  void dump(FILE* out) const;

  /** Returns the filter pipeline of this attribute. */
  const FilterPipeline& filters() const;

  /** Returns the attribute name. */
  const std::string& name() const;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  Status serialize(Buffer* buff);

  /**
   * Sets the attribute number of values per cell. Note that if the attribute
   * datatype is `ANY` this function returns an error, since `ANY` datatype
   * must always be variable-sized.
   *
   * @return Status
   */
  Status set_cell_val_num(unsigned int cell_val_num);

  /** Sets the filter pipeline for this attribute. */
  Status set_filter_pipeline(const FilterPipeline* pipeline);

  /** Sets the attribute name. */
  void set_name(const std::string& name);

  /**
   * Sets the fill value for the attribute. Applicable to
   * both fixed-sized and var-sized attributes.
   */
  Status set_fill_value(const void* value, uint64_t size);

  /**
   * Gets the fill value for the attribute. Applicable to
   * fixed-sized and var-sized attributes.
   */
  Status get_fill_value(const void** value, uint64_t* size) const;

  /** Returns the fill value. */
  const ByteVecValue& fill_value() const;

  /** Returns the attribute type. */
  Datatype type() const;

  /**
   * Returns *true* if this is a variable-sized attribute, and *false*
   * otherwise.
   */
  bool var_size() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The attribute number of values per cell. */
  unsigned cell_val_num_;

  /** The attribute filter pipeline. */
  FilterPipeline filters_;

  /** The attribute name. */
  std::string name_;

  /** The attribute type. */
  Datatype type_;

  /** The fill value. */
  ByteVecValue fill_value_;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Sets the default fill value. */
  void set_default_fill_value();

  /** Returns the fill value in string form. */
  std::string fill_value_str() const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ATTRIBUTE_H
