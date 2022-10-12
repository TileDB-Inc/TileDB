/**
 * @file   fragment_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB FragmentInfo object.
 */

#ifndef TILEDB_CPP_API_FRAGMENT_INFO_H
#define TILEDB_CPP_API_FRAGMENT_INFO_H

#include "array_schema.h"
#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "object.h"
#include "tiledb.h"
#include "type.h"

#include <functional>
#include <memory>
#include <sstream>

namespace tiledb {

/** Describes fragment info objects. **/
class FragmentInfo {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  FragmentInfo(const Context& ctx, const std::string& array_uri)
      : ctx_(ctx) {
    tiledb_fragment_info_t* fragment_info;
    int rc = tiledb_fragment_info_alloc(
        ctx_.get().ptr().get(), array_uri.c_str(), &fragment_info);
    if (rc != TILEDB_OK)
      throw std::runtime_error(
          "[TileDB::C++API] Error: Failed to create FragmentInfo object");
    fragment_info_ =
        std::shared_ptr<tiledb_fragment_info_t>(fragment_info, deleter_);
  }

  FragmentInfo(const FragmentInfo&) = default;
  FragmentInfo(FragmentInfo&&) = default;
  FragmentInfo& operator=(const FragmentInfo&) = default;
  FragmentInfo& operator=(FragmentInfo&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Loads the fragment info. */
  void load() const {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_fragment_info_load(ctx.ptr().get(), fragment_info_.get()));
  }

  /** Loads the fragment info from an encrypted array. */
  TILEDB_DEPRECATED
  void load(
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_fragment_info_load_with_key(
        ctx.ptr().get(),
        fragment_info_.get(),
        encryption_type,
        encryption_key.data(),
        (uint32_t)encryption_key.size()));
  }

  /** Returns the URI of the fragment with the given index. */
  std::string fragment_uri(uint32_t fid) const {
    auto& ctx = ctx_.get();
    const char* uri_c;
    ctx.handle_error(tiledb_fragment_info_get_fragment_uri(
        ctx.ptr().get(), fragment_info_.get(), fid, &uri_c));
    return std::string(uri_c);
  }

  /** Returns the name of the fragment with the given index. */
  std::string fragment_name(uint32_t fid) const {
    auto& ctx = ctx_.get();
    const char* name_c;
    ctx.handle_error(tiledb_fragment_info_get_fragment_name(
        ctx.ptr().get(), fragment_info_.get(), fid, &name_c));
    return std::string(name_c);
  }

  /**
   * Retrieves the non-empty domain of the fragment with the given index
   * on the given dimension index.
   */
  void get_non_empty_domain(uint32_t fid, uint32_t did, void* domain) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_fragment_info_get_non_empty_domain_from_index(
        ctx.ptr().get(), fragment_info_.get(), fid, did, domain));
  }

  /**
   * Retrieves the non-empty domain of the fragment with the given index
   * on the given dimension name.
   */
  void get_non_empty_domain(
      uint32_t fid, const std::string& dim_name, void* domain) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_fragment_info_get_non_empty_domain_from_name(
        ctx.ptr().get(), fragment_info_.get(), fid, dim_name.c_str(), domain));
  }

  /**
   * Returns the non-empty domain of the fragment with the given index
   * on the given dimension index. Applicable to string dimensions.
   */
  std::pair<std::string, std::string> non_empty_domain_var(
      uint32_t fid, uint32_t did) const {
    auto& ctx = ctx_.get();
    uint64_t start_size, end_size;
    std::string start, end;
    ctx.handle_error(
        tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
            ctx.ptr().get(),
            fragment_info_.get(),
            fid,
            did,
            &start_size,
            &end_size));
    start.resize(start_size);
    end.resize(end_size);
    ctx.handle_error(tiledb_fragment_info_get_non_empty_domain_var_from_index(
        ctx.ptr().get(), fragment_info_.get(), fid, did, &start[0], &end[0]));
    return std::make_pair(start, end);
  }

  /**
   * Returns the non-empty domain of the fragment with the given index
   * on the given dimension name. Applicable to string dimensions.
   */
  std::pair<std::string, std::string> non_empty_domain_var(
      uint32_t fid, const std::string& dim_name) const {
    auto& ctx = ctx_.get();
    uint64_t start_size, end_size;
    std::string start, end;
    ctx.handle_error(
        tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
            ctx.ptr().get(),
            fragment_info_.get(),
            fid,
            dim_name.c_str(),
            &start_size,
            &end_size));
    start.resize(start_size);
    end.resize(end_size);
    ctx.handle_error(tiledb_fragment_info_get_non_empty_domain_var_from_name(
        ctx.ptr().get(),
        fragment_info_.get(),
        fid,
        dim_name.c_str(),
        &start[0],
        &end[0]));
    return std::make_pair(start, end);
  }

  /** Returns the number of MBRs in the fragment with the given index. */
  uint64_t mbr_num(uint32_t fid) const {
    auto& ctx = ctx_.get();
    uint64_t ret;
    ctx.handle_error(tiledb_fragment_info_get_mbr_num(
        ctx.ptr().get(), fragment_info_.get(), fid, &ret));
    return ret;
  }

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension index.
   */
  void get_mbr(uint32_t fid, uint32_t mid, uint32_t did, void* mbr) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_fragment_info_get_mbr_from_index(
        ctx.ptr().get(), fragment_info_.get(), fid, mid, did, mbr));
  }

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension name.
   */
  void get_mbr(
      uint32_t fid,
      uint32_t mid,
      const std::string& dim_name,
      void* mbr) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_fragment_info_get_mbr_from_name(
        ctx.ptr().get(),
        fragment_info_.get(),
        fid,
        mid,
        dim_name.c_str(),
        mbr));
  }

  /**
   * Returns the MBR of the fragment with the given index on the given
   * dimension index. Applicable to string dimensions.
   */
  std::pair<std::string, std::string> mbr_var(
      uint32_t fid, uint32_t mid, uint32_t did) const {
    auto& ctx = ctx_.get();
    uint64_t start_size, end_size;
    std::string start, end;
    ctx.handle_error(tiledb_fragment_info_get_mbr_var_size_from_index(
        ctx.ptr().get(),
        fragment_info_.get(),
        fid,
        mid,
        did,
        &start_size,
        &end_size));
    start.resize(start_size);
    end.resize(end_size);
    ctx.handle_error(tiledb_fragment_info_get_mbr_var_from_index(
        ctx.ptr().get(),
        fragment_info_.get(),
        fid,
        mid,
        did,
        &start[0],
        &end[0]));
    return std::make_pair(start, end);
  }

  /**
   * Returns the MBR of the fragment with the given index on the given
   * dimension name. Applicable to string dimensions.
   */
  std::pair<std::string, std::string> mbr_var(
      uint32_t fid, uint32_t mid, const std::string& dim_name) const {
    auto& ctx = ctx_.get();
    uint64_t start_size, end_size;
    std::string start, end;
    ctx.handle_error(tiledb_fragment_info_get_mbr_var_size_from_name(

        ctx.ptr().get(),
        fragment_info_.get(),
        fid,
        mid,
        dim_name.c_str(),
        &start_size,
        &end_size));
    start.resize(start_size);
    end.resize(end_size);
    ctx.handle_error(tiledb_fragment_info_get_mbr_var_from_name(
        ctx.ptr().get(),
        fragment_info_.get(),
        fid,
        mid,
        dim_name.c_str(),
        &start[0],
        &end[0]));
    return std::make_pair(start, end);
  }

  /** Returns the number of fragments. */
  uint32_t fragment_num() const {
    auto& ctx = ctx_.get();
    uint32_t ret;
    ctx.handle_error(tiledb_fragment_info_get_fragment_num(
        ctx.ptr().get(), fragment_info_.get(), &ret));
    return ret;
  }

  /** Returns the size of the fragment with the given index. */
  uint64_t fragment_size(uint32_t fid) const {
    auto& ctx = ctx_.get();
    uint64_t ret;
    ctx.handle_error(tiledb_fragment_info_get_fragment_size(
        ctx.ptr().get(), fragment_info_.get(), fid, &ret));
    return ret;
  }

  /** Returns true if the fragment with the given index is dense. */
  bool dense(uint32_t fid) const {
    auto& ctx = ctx_.get();
    int32_t ret;
    ctx.handle_error(tiledb_fragment_info_get_dense(
        ctx.ptr().get(), fragment_info_.get(), fid, &ret));
    return (bool)ret;
  }

  /** Returns true if the fragment with the given index is sparse. */
  bool sparse(uint32_t fid) const {
    auto& ctx = ctx_.get();
    int32_t ret;
    ctx.handle_error(tiledb_fragment_info_get_sparse(
        ctx.ptr().get(), fragment_info_.get(), fid, &ret));
    return (bool)ret;
  }

  /** Returns the timestamp range of the fragment with the given index. */
  std::pair<uint64_t, uint64_t> timestamp_range(uint32_t fid) const {
    auto& ctx = ctx_.get();
    uint64_t start, end;
    ctx.handle_error(tiledb_fragment_info_get_timestamp_range(
        ctx.ptr().get(), fragment_info_.get(), fid, &start, &end));
    return std::make_pair(start, end);
  }

  /** Returns the number of cells of the fragment with the given index. */
  uint64_t cell_num(uint32_t fid) const {
    auto& ctx = ctx_.get();
    uint64_t ret;
    ctx.handle_error(tiledb_fragment_info_get_cell_num(
        ctx.ptr().get(), fragment_info_.get(), fid, &ret));
    return ret;
  }

  /** Returns the total number of cells written in the loaded fragments. */
  uint64_t total_cell_num() const {
    auto& ctx = ctx_.get();
    uint64_t ret;
    ctx.handle_error(tiledb_fragment_info_get_total_cell_num(
        ctx.ptr().get(), fragment_info_.get(), &ret));
    return ret;
  }

  /** Returns the version of the fragment with the given index. */
  uint32_t version(uint32_t fid) const {
    auto& ctx = ctx_.get();
    uint32_t ret;
    ctx.handle_error(tiledb_fragment_info_get_version(
        ctx.ptr().get(), fragment_info_.get(), fid, &ret));
    return ret;
  }

  /** Returns the array schema of the fragment with the given index. */
  ArraySchema array_schema(uint32_t fid) const {
    auto& ctx = ctx_.get();
    tiledb_array_schema_t* schema;
    ctx.handle_error(tiledb_fragment_info_get_array_schema(
        ctx.ptr().get(), fragment_info_.get(), fid, &schema));
    return ArraySchema(ctx, schema);
  }

  /** Returns the array schema name of the fragment with the given index. */
  std::string array_schema_name(uint32_t fid) const {
    auto& ctx = ctx_.get();
    const char* schema_name;
    ctx.handle_error(tiledb_fragment_info_get_array_schema_name(
        ctx.ptr().get(), fragment_info_.get(), fid, &schema_name));
    return std::string(schema_name);
  }

  /**
   * Returns true if the fragment with the given index has
   * consolidated metadata.
   */
  bool has_consolidated_metadata(uint32_t fid) const {
    auto& ctx = ctx_.get();
    int32_t ret;
    ctx.handle_error(tiledb_fragment_info_has_consolidated_metadata(
        ctx.ptr().get(), fragment_info_.get(), fid, &ret));
    return (bool)ret;
  }

  /** Returns the number of fragments with unconsolidated metadata. */
  uint32_t unconsolidated_metadata_num() const {
    auto& ctx = ctx_.get();
    uint32_t ret;
    ctx.handle_error(tiledb_fragment_info_get_unconsolidated_metadata_num(
        ctx.ptr().get(), fragment_info_.get(), &ret));
    return ret;
  }

  /** Returns the number of fragments to vacuum. */
  uint32_t to_vacuum_num() const {
    auto& ctx = ctx_.get();
    uint32_t ret;
    ctx.handle_error(tiledb_fragment_info_get_to_vacuum_num(
        ctx.ptr().get(), fragment_info_.get(), &ret));
    return ret;
  }

  /** Returns the URI of the fragment to vacuum with the given index. */
  std::string to_vacuum_uri(uint32_t fid) const {
    auto& ctx = ctx_.get();
    const char* uri_c;
    ctx.handle_error(tiledb_fragment_info_get_to_vacuum_uri(
        ctx.ptr().get(), fragment_info_.get(), fid, &uri_c));
    return std::string(uri_c);
  }

  /**
   * Dumps the fragment info in an ASCII representation to an output.
   *
   * @param out (Optional) File to dump output to. Defaults to `nullptr`
   * which will lead to selection of `stdout`.
   */
  void dump(FILE* out = nullptr) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_fragment_info_dump(ctx.ptr().get(), fragment_info_.get(), out));
  }

  /** Returns the C TileDB context object. */
  std::shared_ptr<tiledb_fragment_info_t> ptr() const {
    return fragment_info_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** A deleter wrapper. */
  impl::Deleter deleter_;

  /** The C TileDB fragment info object. */
  std::shared_ptr<tiledb_fragment_info_t> fragment_info_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_FRAGMENT_INFO_H
