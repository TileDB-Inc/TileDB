/**
 * @file   hyperspace.h
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
 * This file defines class Hyperspace.
 */

#ifndef TILEDB_HYPERSPACE_H
#define TILEDB_HYPERSPACE_H

#include "buffer.h"
#include "datatype.h"
#include "dimension.h"
#include "status.h"

#include <vector>

namespace tiledb {

/** Defines an array hyperspace, which consists of dimensions. */
class Hyperspace {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Empty constructor. */
  Hyperspace();

  /**
   * Constructor.
   *
   * @param type The type of dimensions.
   */
  explicit Hyperspace(Datatype type);

  /**
   * Constructor that clones the input hyperspace.
   *
   * @param hyperspace The object to clone.
   */
  explicit Hyperspace(const Hyperspace* hyperspace);

  /** Destructor. */
  ~Hyperspace();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Adds a dimension to the hyperspace.
   *
   * @param name The dimension name.
   * @param domain The dimension domain.
   * @param tile_extent The dimension tile extent.
   * @return Status
   */
  Status add_dimension(
      const char* name, const void* domain, const void* tile_extent);

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff);

  /** Returns the number of dimensions. */
  unsigned int dim_num() const;

  /** returns the domain along the i-th dimension (nullptr upon error). */
  const void* domain(unsigned int i) const;

  /** Returns the i-th dimensions (nullptr upon error). */
  const Dimension* dimension(unsigned int i) const;

  /** Dumps the hyperspace in ASCII format in the selected output. */
  void dump(FILE* out) const;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  Status serialize(Buffer* buff);

  /** returns the tile extent along the i-th dimension (nullptr upon error). */
  const void* tile_extent(unsigned int i) const;

  /** Returns the dimensions type. */
  Datatype type() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The hyperspace dimensions. */
  std::vector<Dimension*> dimensions_;

  /** The number of dimensions. */
  unsigned int dim_num_;

  /** The type of dimensions. */
  Datatype type_;
};

}  // namespace tiledb

#endif  // TILEDB_HYPERSPACE_H
