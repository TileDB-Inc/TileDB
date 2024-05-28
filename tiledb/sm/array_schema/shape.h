/**
 * @file shape.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines class Shape.
 */

#ifndef TILEDB_SHAPE_H
#define TILEDB_SHAPE_H

#include <iostream>

#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/ndrectangle.h"
#include "tiledb/sm/enums/shape_type.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/storage_format/serialization/serializers.h"

namespace tiledb::sm {

class MemoryTracker;
class ArraySchema;
class Domain;

/** Defines an array shape */
class Shape {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** No default constructor*/
  Shape() = delete;

  DISABLE_COPY(Shape);
  DISABLE_MOVE(Shape);

  /** Constructor
   *
   * @param memory_tracker The memory tracker.
   * @param version the disk version of this shape
   */
  Shape(shared_ptr<MemoryTracker> memory_tracker, format_version_t version);

  /** Destructor. */
  ~Shape() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  DISABLE_COPY_ASSIGN(Shape);
  DISABLE_MOVE_ASSIGN(Shape);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */
  /**
   * Deserialize a Shape
   *
   * @param deserializer The deserializer to deserialize from.
   * @param memory_tracker The memory tracker associated with this Shape.
   * @param domain The array schema domain.
   * @return A new Shape.
   */
  static shared_ptr<const Shape> deserialize(
      Deserializer& deserializer,
      shared_ptr<MemoryTracker> memory_tracker,
      shared_ptr<Domain> domain);

  /**
   * Serializes the Shape into a buffer.
   *
   * @param serializer The object the array schema is serialized into.
   */
  void serialize(Serializer& serializer) const;

  /**
   * @return Returns the type of shape stored in this instance
   */
  ShapeType type() const {
    return type_;
  }

  /**
   * @return Returns whether this shape is empty or not.
   */
  bool empty() const {
    return empty_;
  }

  /**
   * Dump a textual representation of the Shape to the FILE
   *
   * @param out A file pointer to write to. If out is nullptr, use stdout
   */
  void dump(FILE* out) const;

  /**
   * Sets a NDRectangle to this shape and adjusts its type to reflect that.
   * Throws if the shape is not empty.
   *
   * @param ndr A NDRectangle to be set on this Shape object.
   */
  void set_ndrectangle(std::shared_ptr<const NDRectangle> ndr);

  /**
   * Throws if the shape doesn't have a NDRectangle set
   *
   * @return Returns the ndrectangle set on a shape.
   */
  shared_ptr<const NDRectangle> ndrectangle() const;

  /**
   * Checks if the argument fully contains this shape.
   *
   * @param expanded_shape The shape we want to compare against
   * @return True if the argument is a superset of the current instance
   */
  bool covered(shared_ptr<Shape> expanded_shape) const;

  /**
   * Checks if the arg fully contains this shape.
   *
   * @param expanded_range The ndrange we want to compare against
   * @return True if the argument is a superset of the current instance
   */
  bool covered(const NDRange& expanded_range) const;

  /**
   * Perform various checks to ensure the shape is coherent with the array
   * schema
   *
   * @param schema The array schema
   */
  void check_schema_sanity(const ArraySchema& schema) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The memory tracker of the Shape.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The type of the shape */
  ShapeType type_;

  /** A flag which enables or disables inequality comparisons */
  bool empty_;

  /** The ndrectangle shape */
  shared_ptr<const NDRectangle> ndrectangle_;

  /** The format version of this Shape */
  bool version_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_SHAPE_H
