/**
 * @file tiledb/api/c_api/object/object_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the object section of the C API for TileDB.
 */

#include "../context/context_api_internal.h"
#include "object_api_external.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/walk_order.h"

namespace tiledb::api {

using ObjectCallback =
    std::function<int32_t(const char*, tiledb_object_t, void*)>;

inline void ensure_callback_argument_is_valid(ObjectCallback cb) {
  if (cb == nullptr) {
    throw CAPIStatusException("argument `callback` may not be nullptr");
  }
}

capi_return_t tiledb_object_type_to_str(
    tiledb_object_t object_type, const char** str) {
  auto otype = static_cast<tiledb::sm::ObjectType>(object_type);
  const auto& strval = tiledb::sm::object_type_str(otype);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_object_type_from_str(
    const char* str, tiledb_object_t* object_type) {
  tiledb::sm::ObjectType val = tiledb::sm::ObjectType::INVALID;
  if (!tiledb::sm::object_type_enum(str, &val).ok()) {
    return TILEDB_ERR;
  }
  *object_type = static_cast<tiledb_object_t>(val);
  return TILEDB_OK;
}

capi_return_t tiledb_walk_order_to_str(
    tiledb_walk_order_t walk_order, const char** str) {
  auto wo = static_cast<tiledb::sm::WalkOrder>(walk_order);
  const auto& strval = tiledb::sm::walkorder_str(wo);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_walk_order_from_str(
    const char* str, tiledb_walk_order_t* walk_order) {
  tiledb::sm::WalkOrder val = tiledb::sm::WalkOrder::PREORDER;
  if (!tiledb::sm::walkorder_enum(str, &val).ok()) {
    return TILEDB_ERR;
  }
  *walk_order = static_cast<tiledb_walk_order_t>(val);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    object_type_to_str, tiledb_object_t object_type, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_object_type_to_str>(
      object_type, str);
}

CAPI_INTERFACE(
    object_type_from_str, const char* str, tiledb_object_t* object_type) {
  return api_entry_plain<tiledb::api::tiledb_object_type_from_str>(
      str, object_type);
}

CAPI_INTERFACE(
    walk_order_to_str, tiledb_walk_order_t walk_order, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_walk_order_to_str>(
      walk_order, str);
}

CAPI_INTERFACE(
    walk_order_from_str, const char* str, tiledb_walk_order_t* walk_order) {
  return api_entry_plain<tiledb::api::tiledb_walk_order_from_str>(
      str, walk_order);
}
