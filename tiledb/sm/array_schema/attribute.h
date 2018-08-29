/**
 * @file   attribute.h
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
 * This file defines class Attribute.
 */

#ifndef TILEDB_ATTRIBUTE_H
#define TILEDB_ATTRIBUTE_H

#include <string>

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

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

  /** Returns the compressor. */
  Compressor compressor() const;

  /** Returns the compression level. */
  int compression_level() const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff);

  /** Dumps the attribute contents in ASCII form in the selected output. */
  void dump(FILE* out) const;

  /** Returns the filter pipeline of this attribute. */
  const FilterPipeline* filters() const;

  /** Returns the attribute name. */
  const std::string& name() const;

  /** Returns true if this is an anonymous (unlabeled) attribute **/
  bool is_anonymous() const;

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

  /** Sets the attribute compressor. */
  void set_compressor(Compressor compressor);

  /** Sets the attribute compression level. */
  void set_compression_level(int compression_level);

  /** Sets the filter pipeline for this attribute. */
  Status set_filter_pipeline(const FilterPipeline* pipeline);

  /** Sets the attribute name. */
  void set_name(const std::string& name);

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
  unsigned int cell_val_num_;

  /** The attribute filter pipeline. */
  FilterPipeline filters_;

  /** The attribute name. */
  std::string name_;

  /** The attribute type. */
  Datatype type_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ATTRIBUTE_H
