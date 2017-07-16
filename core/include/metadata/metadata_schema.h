/**
 * @file   metadata_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class MetadataSchema.
 */

#ifndef TILEDB_METADATA_SCHEMA_H
#define TILEDB_METADATA_SCHEMA_H

#include "array_schema.h"
#include "configurator.h"

namespace tiledb {

/** The metadata schema class. */
class MetadataSchema {
 public:
  /* ********************************* */
  /*    CONSTRUCTORS & DESTRUCTORS     */
  /* ********************************* */

  /** Constructor. */
  MetadataSchema();

  /**
   * Constructor. Clones the input.
   *
   * @param array_schema The array schema to copy.
   */
  MetadataSchema(const ArraySchema* array_schema);

  /**
   * Constructor. Clones the input.
   *
   * @param metadata_schema The metadata schema to copy.
   */
  MetadataSchema(const MetadataSchema* metadata_schema);

  /**
   * Constructor.
   *
   * @param metadata_name The metadata name.
   */
  MetadataSchema(const char* metadata_name);

  /** Destructor. */
  ~MetadataSchema();

  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the metadata name. */
  const std::string& metadata_name() const;

  /** Returns the underlying array schema. */
  const ArraySchema* array_schema() const;

  /** Returns a constant pointer to the selected attribute (NULL if error). */
  const Attribute* attr(int id) const;

  /** Returns the number of attributes. */
  unsigned int attr_num() const;

  /** Returns the capacity. */
  uint64_t capacity() const;

  /** Returns the cell order. */
  Layout cell_order() const;

  /** Checks if the metadata schema has been set correctly. */
  Status check() const;

  /** Dumps in ASCII format the metadata schema. */
  void dump(FILE* out) const;

  /** Returns the tile order. */
  Layout tile_order() const;

  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /** Adds an attribute, cloning the input. */
  void add_attribute(const Attribute* attr);

  /**
   * Loads the metadata schema from the disk.
   *
   * @param metadata_name The name of the metadata whose scheme will be loaded.
   * @return Status
   */
  Status load(const char* metadata_name);

  /** Sets the capacity. */
  void set_capacity(uint64_t capacity);

  /** Sets the cell order. */
  void set_cell_order(Layout cell_order);

  /** Sets the tile order. */
  void set_tile_order(Layout tile_order);

  /**
   * Stores the metadata schema in a file inside directory *dir*.
   *
   * @param dir The directory where the metadata schema file will be stored.
   * @return Status
   */
  Status store(const std::string& dir);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The underlying array schema. */
  ArraySchema* array_schema_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Adds the dimensions that correspond to the hash key to the underlying
   * array schema.
   */
  void add_dimensions();
};

}  // namespace tiledb

#endif  // TILEDB_METADATA_SCHEMA_H
