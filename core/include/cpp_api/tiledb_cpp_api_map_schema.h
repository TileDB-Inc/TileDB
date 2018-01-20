/**
 * @file  tiledb_cpp_api_map_schema.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for the TileDB MapSchema object.
 */

#ifndef TILEDB_CPP_API_MAP_SCHEMA_H
#define TILEDB_CPP_API_MAP_SCHEMA_H

#include "tiledb.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_attribute.h"
#include "tiledb_cpp_api_domain.h"
#include "tiledb_cpp_api_object.h"
#include "tiledb_cpp_api_schema_base.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace tdb {

/** Implements the array schema functionality. */
  class MapSchema : public Schema {
  public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    /** Creates a new array schema. */
    explicit MapSchema(const Context &ctx);

    /** Loads the schema of an existing array with the input URI. */
    MapSchema(const Context &ctx, const std::string &uri);

    MapSchema(const MapSchema &) = default;
    MapSchema(MapSchema &&o) = default;
    MapSchema &operator=(const MapSchema &) = default;
    MapSchema &operator=(MapSchema &&o) = default;

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /** Auxiliary operator for getting the underlying C TileDB object. */
    operator tiledb_kv_schema_t *() const {
      return schema_.get();
    }

    /** Dumps the array schema in an ASCII representation to an output. */
    void dump(FILE *out = stdout) const override;

    /** Adds an attribute to the array. */
    MapSchema &add_attribute(const Attribute &attr) override;

    /** Returns a shared pointer to the C TileDB domain object. */
    std::shared_ptr<tiledb_kv_schema_t> ptr() const {
      return schema_;
    }

    /** Validates the schema. */
    void check() const override;

    /** Gets all attributes in the array. */
    const std::unordered_map<std::string, Attribute> attributes() const override;;

    /** Get an attribute by name. **/
    Attribute attribute(const std::string &name) const override {
    auto &ctx = ctx_.get();
      tiledb_attribute_t *attr;
      ctx.handle_error(tiledb_kv_schema_get_attribute_from_name(ctx, schema_.get(), name.c_str(), &attr));
      return {ctx, attr};
    }

    /** Get an attribute by index **/
    Attribute attribute(unsigned int i) const override;

  private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /** The pointer to the C TileDB KV schema object. */
    std::shared_ptr<tiledb_kv_schema_t> schema_;
  };

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Converts the array schema into a string representation. */
  std::ostream &operator<<(std::ostream &os, const MapSchema &schema);

/** Adds an attribute to the array schema. */
  MapSchema &operator<<(MapSchema &schema, const Attribute &attr);

}  // namespace tdb

#endif  // TILEDB_CPP_API_MAP_SCHEMA_H
