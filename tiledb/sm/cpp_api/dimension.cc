/**
 * @file   tiledb_cpp_api_dimension.cc
 *
 * @author Ravi Gaddipati
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
 * This file defines the C++ API for the TileDB Dimension object.
 */

#include "dimension.h"

#include <cassert>
#include <sstream>

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dimension::Dimension(const Context& ctx, tiledb_dimension_t* dim)
    : ctx_(ctx)
    , deleter_(ctx) {
  dim_ = std::shared_ptr<tiledb_dimension_t>(dim, deleter_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

const std::string Dimension::name() const {
  const char* name;
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_dimension_get_name(ctx, dim_.get(), &name));
  return name;
}

std::shared_ptr<tiledb_dimension_t> Dimension::ptr() const {
  return dim_;
}

Dimension::operator tiledb_dimension_t*() const {
  return dim_.get();
}

tiledb_datatype_t Dimension::type() const {
  tiledb_datatype_t type;
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_dimension_get_type(ctx, dim_.get(), &type));
  return type;
}

std::string Dimension::domain_to_str() const {
  auto domain = _domain();
  auto type = this->type();
  int8_t* di8;
  uint8_t* dui8;
  int16_t* di16;
  uint16_t* dui16;
  int32_t* di32;
  uint32_t* dui32;
  int64_t* di64;
  uint64_t* dui64;
  float* df32;
  double* df64;

  std::stringstream ss;
  ss << "[";

  switch (type) {
    case TILEDB_INT8:
      di8 = static_cast<int8_t*>(domain);
      ss << di8[0] << "," << di8[1];
      break;
    case TILEDB_UINT8:
      dui8 = static_cast<uint8_t*>(domain);
      ss << dui8[0] << "," << dui8[1];
      break;
    case TILEDB_INT16:
      di16 = static_cast<int16_t*>(domain);
      ss << di16[0] << "," << di16[1];
      break;
    case TILEDB_UINT16:
      dui16 = static_cast<uint16_t*>(domain);
      ss << dui16[0] << "," << dui16[1];
      break;
    case TILEDB_INT32:
      di32 = static_cast<int32_t*>(domain);
      ss << di32[0] << "," << di32[1];
      break;
    case TILEDB_UINT32:
      dui32 = static_cast<uint32_t*>(domain);
      ss << dui32[0] << "," << dui32[1];
      break;
    case TILEDB_INT64:
      di64 = static_cast<int64_t*>(domain);
      ss << di64[0] << "," << di64[1];
      break;
    case TILEDB_UINT64:
      dui64 = static_cast<uint64_t*>(domain);
      ss << dui64[0] << "," << dui64[1];
      break;
    case TILEDB_FLOAT32:
      df32 = static_cast<float*>(domain);
      ss << df32[0] << "," << df32[1];
      break;
    case TILEDB_FLOAT64:
      df64 = static_cast<double*>(domain);
      ss << df64[0] << "," << df64[1];
      break;
    case TILEDB_CHAR:
    case TILEDB_STRING_ASCII:
    case TILEDB_STRING_UTF8:
    case TILEDB_STRING_UTF16:
    case TILEDB_STRING_UTF32:
    case TILEDB_STRING_UCS2:
    case TILEDB_STRING_UCS4:
    case TILEDB_ANY:
      // Not supported domain types
      assert(0);
      break;
  }

  ss << "]";

  return ss.str();
}

std::string Dimension::tile_extent_to_str() const {
  auto tile_extent = _tile_extent();
  auto type = this->type();
  int8_t* ti8;
  uint8_t* tui8;
  int16_t* ti16;
  uint16_t* tui16;
  int32_t* ti32;
  uint32_t* tui32;
  int64_t* ti64;
  uint64_t* tui64;
  float* tf32;
  double* tf64;

  std::stringstream ss;

  switch (type) {
    case TILEDB_INT8:
      ti8 = static_cast<int8_t*>(tile_extent);
      ss << *ti8;
      break;
    case TILEDB_UINT8:
      tui8 = static_cast<uint8_t*>(tile_extent);
      ss << *tui8;
      break;
    case TILEDB_INT16:
      ti16 = static_cast<int16_t*>(tile_extent);
      ss << *ti16;
      break;
    case TILEDB_UINT16:
      tui16 = static_cast<uint16_t*>(tile_extent);
      ss << *tui16;
      break;
    case TILEDB_INT32:
      ti32 = static_cast<int32_t*>(tile_extent);
      ss << *ti32;
      break;
    case TILEDB_UINT32:
      tui32 = static_cast<uint32_t*>(tile_extent);
      ss << *tui32;
      break;
    case TILEDB_INT64:
      ti64 = static_cast<int64_t*>(tile_extent);
      ss << *ti64;
      break;
    case TILEDB_UINT64:
      tui64 = static_cast<uint64_t*>(tile_extent);
      ss << *tui64;
      break;
    case TILEDB_FLOAT32:
      tf32 = static_cast<float*>(tile_extent);
      ss << *tf32;
      break;
    case TILEDB_FLOAT64:
      tf64 = static_cast<double*>(tile_extent);
      ss << *tf64;
      break;
    case TILEDB_CHAR:
    case TILEDB_STRING_ASCII:
    case TILEDB_STRING_UTF8:
    case TILEDB_STRING_UTF16:
    case TILEDB_STRING_UTF32:
    case TILEDB_STRING_UCS2:
    case TILEDB_STRING_UCS4:
    case TILEDB_ANY:
      // Not supported domain (and, hence, extent) types
      assert(0);
      break;
  }

  return ss.str();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void* Dimension::_domain() const {
  auto& ctx = ctx_.get();
  void* domain;
  ctx.handle_error(tiledb_dimension_get_domain(ctx, dim_.get(), &domain));
  return domain;
}

void* Dimension::_tile_extent() const {
  void* tile_extent;
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_dimension_get_tile_extent(ctx, dim_.get(), &tile_extent));
  return tile_extent;
}

/* ********************************* */
/*     PRIVATE STATIC FUNCTIONS      */
/* ********************************* */

Dimension Dimension::create_impl(
    const Context& ctx,
    const std::string& name,
    tiledb_datatype_t type,
    const void* domain,
    const void* tile_extent) {
  tiledb_dimension_t* d;
  ctx.handle_error(tiledb_dimension_create(
      ctx, &d, name.c_str(), type, domain, tile_extent));
  return Dimension(ctx, d);
}

/* ********************************* */
/*               MISC                */
/* ********************************* */

std::ostream& operator<<(std::ostream& os, const tiledb::Dimension& dim) {
  os << "Dim<" << dim.name() << "," << dim.domain_to_str() << ","
     << dim.tile_extent_to_str() << ">";
  return os;
}

}  // namespace tiledb
