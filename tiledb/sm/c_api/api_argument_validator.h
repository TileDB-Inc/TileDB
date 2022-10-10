/**
 * @file   api_argument_validator.h
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
 * This file declares argument validator functions used in the c-api
 * implementation
 */

#ifndef TILEDB_CAPI_HELPERS_H
#define TILEDB_CAPI_HELPERS_H

#include "tiledb/api/c_api/filter/filter_api_internal.h"
#include "tiledb/api/c_api_support/argument_validation.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/filter_type.h"

/* ********************************* */
/*         AUXILIARY FUNCTIONS       */
/* ********************************* */

namespace tiledb::api {

/**
 * Returns after successfully validating an array.
 *
 * @param array Possibly-valid pointer to array
 */
inline void ensure_array_is_valid(const tiledb_array_t* array) {
  if (array == nullptr) {
    action_invalid_object("array");
  }
}

/**
 * Returns after successfully validating a filter list.
 *
 * @param filter_list Possibly-valid pointer to filter list
 */
inline void ensure_filter_list_is_valid(
    const tiledb_filter_list_t* filter_list) {
  if (filter_list == nullptr) {
    action_invalid_object("filter list");
  }
}

}  // namespace tiledb::api

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_array_t* array) {
  if (array == nullptr || array->array_ == nullptr) {
    auto st = Status_Error("Invalid TileDB array object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_subarray_t* subarray) {
  if (subarray == nullptr || subarray->subarray_ == nullptr ||
      subarray->subarray_->array() == nullptr) {
    auto st = Status_Error("Invalid TileDB subarray object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_buffer_t* buffer) {
  if (buffer == nullptr || buffer->buffer_ == nullptr) {
    auto st = Status_Error("Invalid TileDB buffer object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_buffer_list_t* buffer_list) {
  if (buffer_list == nullptr || buffer_list->buffer_list_ == nullptr) {
    auto st = Status_Error("Invalid TileDB buffer list object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_config_t* config, tiledb_error_t** error) {
  if (config == nullptr || config->config_ == nullptr) {
    auto st = Status_Error("Cannot set config; Invalid config object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    create_error(error, st);
    return TILEDB_ERR;
  }

  *error = nullptr;
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, tiledb_config_t* config) {
  if (config == nullptr || config->config_ == nullptr) {
    auto st = Status_Error("Cannot set config; Invalid config object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_config_t* config) {
  if (config == nullptr || config->config_ == nullptr) {
    auto st = Status_Error("Cannot set config; Invalid config object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_config_iter_t* config_iter, tiledb_error_t** error) {
  if (config_iter == nullptr || config_iter->config_iter_ == nullptr) {
    auto st = Status_Error("Cannot set config; Invalid config iterator object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    create_error(error, st);
    return TILEDB_ERR;
  }

  *error = nullptr;
  return TILEDB_OK;
}

/**
 * This function is dead code. Validity of the context is now checked in the
 * exception wrapper.
 */
inline int32_t sanity_check(tiledb_ctx_t*) {
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_attribute_t* attr) {
  if (attr == nullptr || attr->attr_ == nullptr) {
    auto st = Status_Error("Invalid TileDB attribute object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_filter_list_t* filter_list) {
  if (filter_list == nullptr || filter_list->pipeline_ == nullptr) {
    auto st = Status_Error("Invalid TileDB filter list object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_dimension_t* dim) {
  if (dim == nullptr || dim->dim_ == nullptr) {
    auto st = Status_Error("Invalid TileDB dimension object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema) {
  if (array_schema == nullptr || array_schema->array_schema_ == nullptr) {
    auto st = Status_Error("Invalid TileDB array schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_evolution_t* schema_evolution) {
  if (schema_evolution == nullptr ||
      schema_evolution->array_schema_evolution_ == nullptr) {
    auto st = Status_Error("Invalid TileDB array schema evolution object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_domain_t* domain) {
  if (domain == nullptr || domain->domain_ == nullptr) {
    auto st = Status_Error("Invalid TileDB domain object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_query_t* query) {
  if (query == nullptr || query->query_ == nullptr) {
    auto st = Status_Error("Invalid TileDB query object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_query_condition_t* cond) {
  if (cond == nullptr || cond->query_condition_ == nullptr) {
    auto st = Status_Error("Invalid TileDB query condition object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_vfs_t* vfs) {
  if (vfs == nullptr || vfs->vfs_ == nullptr) {
    auto st = Status_Error("Invalid TileDB virtual filesystem object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_vfs_fh_t* fh) {
  if (fh == nullptr || fh->vfs_fh_ == nullptr) {
    auto st = Status_Error("Invalid TileDB virtual filesystem file handle");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_fragment_info_t* fragment_info) {
  if (fragment_info == nullptr || fragment_info->fragment_info_ == nullptr) {
    auto st = Status_Error("Invalid TileDB fragment info object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_group_t* group) {
  if (group == nullptr || group->group_ == nullptr) {
    auto st = Status_Error("Invalid TileDB group object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

/**
 * Helper macro similar to save_error() that catches all exceptions when
 * executing 'stmt'.
 *
 * @param ctx TileDB context
 * @param stmt Statement to execute
 */
#define SAVE_ERROR_CATCH(ctx, stmt)                                        \
  [&]() {                                                                  \
    auto _s = Status::Ok();                                                \
    try {                                                                  \
      _s = (stmt);                                                         \
    } catch (const std::exception& e) {                                    \
      auto st = Status_Error(                                              \
          std::string("Internal TileDB uncaught exception; ") + e.what()); \
      LOG_STATUS_NO_RETURN_VALUE(st);                                      \
      save_error(ctx, st);                                                 \
      return true;                                                         \
    }                                                                      \
    return save_error(ctx, _s);                                            \
  }()

#endif  // TILEDB_CAPI_HELPERS_H