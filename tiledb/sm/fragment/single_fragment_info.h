/**
 * @file  single_fragment_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines struct SingleFragmentInfo.
 */

#ifndef TILEDB_SINGLE_FRAGMENT_INFO_H
#define TILEDB_SINGLE_FRAGMENT_INFO_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/types.h"

#include <cinttypes>
#include <sstream>
#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Stores basic information about one fragment. */
class SingleFragmentInfo {
 public:
  /** Constructor. */
  SingleFragmentInfo() {
    uri_ = URI("");
    name_ = "";
    version_ = 0;
    cell_num_ = 0;
    sparse_ = false;
    timestamp_range_ = {0, 0};
    fragment_size_ = 0;
    array_schema_name_ = "";
  }

  /** Constructor. */
  SingleFragmentInfo(
      const URI& uri,
      bool sparse,
      const std::pair<uint64_t, uint64_t>& timestamp_range,
      uint64_t fragment_size,
      const NDRange& non_empty_domain,
      const NDRange& expanded_non_empty_domain,
      shared_ptr<FragmentMetadata> meta)
      : uri_(uri)
      , name_(meta->fragment_uri().remove_trailing_slash().last_path_part())
      , version_(meta->format_version())
      , sparse_(sparse)
      , timestamp_range_(timestamp_range)
      , cell_num_(meta->cell_num())
      , fragment_size_(fragment_size)
      , has_consolidated_footer_(meta->has_consolidated_footer())
      , non_empty_domain_(non_empty_domain)
      , expanded_non_empty_domain_(expanded_non_empty_domain)
      , array_schema_name_(meta->array_schema_name())
      , meta_(meta) {
  }

  /** Copy constructor. */
  SingleFragmentInfo(const SingleFragmentInfo& info)
      : SingleFragmentInfo() {
    auto clone = info.clone();
    swap(clone);
  }

  /** Move constructor. */
  SingleFragmentInfo(SingleFragmentInfo&& info)
      : SingleFragmentInfo() {
    swap(info);
  }

  /** Copy assignment operator. */
  SingleFragmentInfo& operator=(const SingleFragmentInfo& info) {
    auto clone = info.clone();
    swap(clone);
    return *this;
  }

  /** Move assignment operator. */
  SingleFragmentInfo& operator=(SingleFragmentInfo&& info) {
    swap(info);
    return *this;
  }

  /** Returns the number of cells written in the fragment. */
  uint64_t cell_num() const {
    return cell_num_;
  }

  /** Dumps the single fragment info in ASCII format in the selected output. */
  std::string dump_single_fragment_info(
      const std::vector<Datatype>& dim_types) const {
    std::stringstream ss;
    ss << "  > URI: " << uri_.c_str() << "\n";
    ss << "  > Schema name: " << array_schema_name_ << "\n";
    ss << "  > Type: " << (sparse_ ? "sparse" : "dense") << "\n";
    ss << "  > Non-empty domain: " << non_empty_domain_str(dim_types).c_str()
       << "\n";
    ss << "  > Size: " << fragment_size_ << "\n";
    ss << "  > Cell num: " << cell_num_ << "\n";
    ss << "  > Timestamp range: [" << timestamp_range_.first << ", "
       << timestamp_range_.second << "]\n";
    ss << "  > Format version: " << version_ << "\n";
    ss << "  > Has consolidated metadata: "
       << (has_consolidated_footer_ ? "yes" : "no") << "\n";

    return ss.str();
  }

  /** Returns `true` if the fragment is sparse. */
  bool sparse() const {
    return sparse_;
  }

  /** Returns the fragment URI. */
  const URI& uri() const {
    return uri_;
  }

  /** Returns the fragment name. */
  const std::string& name() const {
    return name_;
  }

  /** Returns the timestamp range. */
  const std::pair<uint64_t, uint64_t>& timestamp_range() const {
    return timestamp_range_;
  }

  format_version_t format_version() const {
    return version_;
  }

  /** Returns the fragment size. */
  uint64_t fragment_size() const {
    return fragment_size_;
  }

  /** Returns true if the fragment has consolidated footer. */
  bool has_consolidated_footer() const {
    return has_consolidated_footer_;
  }

  /** Returns the non-empty domain. */
  const NDRange& non_empty_domain() const {
    return non_empty_domain_;
  }

  /** Returns the expanded non-empty domain. */
  const NDRange& expanded_non_empty_domain() const {
    return expanded_non_empty_domain_;
  }

  /** Returns a pointer to the fragment's metadata. */
  shared_ptr<FragmentMetadata> meta() const {
    return meta_;
  }

  /** Returns the array schema name. */
  const std::string& array_schema_name() const {
    return array_schema_name_;
  }

  /** Returns the non-empty domain in string format. */
  std::string non_empty_domain_str(
      const std::vector<Datatype>& dim_types) const {
    std::stringstream ss;
    for (uint32_t d = 0; d < (uint32_t)dim_types.size(); ++d) {
      ss << type::range_str(non_empty_domain_[d], dim_types[d]);
      if (d != (uint32_t)dim_types.size() - 1) {
        ss << " x ";
      }
    }

    return ss.str();
  }

  /** Accessor to the fragment size. */
  uint64_t& fragment_size() {
    return fragment_size_;
  }

  /** Accessor to the metadata pointer. */
  shared_ptr<FragmentMetadata>& meta() {
    return meta_;
  }

  /** Accessor to the metadata pointer. */
  NDRange& non_empty_domain() {
    return non_empty_domain_;
  }

 private:
  /** The fragment URI. */
  URI uri_;

  /**
   * The fragment name.
   *
   * #TODO: Remove upon removal of tiledb_fragment_info_get_fragment_name.
   */
  std::string name_;

  /** The format version of the fragment. */
  format_version_t version_;

  /** True if the fragment is sparse, and false if it is dense. */
  bool sparse_;

  /** The timestamp range of the fragment. */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** The number of cells written in the fragment. */
  uint64_t cell_num_;

  /** The size of the entire fragment directory. */
  uint64_t fragment_size_;

  /** Returns true if the fragment metadata footer is consolidated. */
  bool has_consolidated_footer_;

  /** The fragment's non-empty domain. */
  NDRange non_empty_domain_;

  /**
   * The fragment's expanded non-empty domain (in a way that
   * it coincides with tile boundaries. Applicable only to
   * dense fragments. For sparse fragments, the expanded
   * domain is the same as the non-empty domain.
   */
  NDRange expanded_non_empty_domain_;

  /** The name of array schema */
  std::string array_schema_name_;

  /** The fragment metadata. **/
  shared_ptr<FragmentMetadata> meta_;

  /**
   * Returns a deep copy of this FragmentInfo.
   * @return New FragmentInfo
   */
  SingleFragmentInfo clone() const {
    SingleFragmentInfo clone;
    clone.uri_ = uri_;
    clone.name_ = name_;
    clone.version_ = version_;
    clone.cell_num_ = cell_num_;
    clone.sparse_ = sparse_;
    clone.timestamp_range_ = timestamp_range_;
    clone.fragment_size_ = fragment_size_;
    clone.has_consolidated_footer_ = has_consolidated_footer_;
    clone.non_empty_domain_ = non_empty_domain_;
    clone.expanded_non_empty_domain_ = expanded_non_empty_domain_;
    clone.array_schema_name_ = array_schema_name_;
    clone.meta_ = meta_;
    return clone;
  }

  /** Swaps the contents (all field values) of this info with the given info. */
  void swap(SingleFragmentInfo& info) {
    std::swap(uri_, info.uri_);
    std::swap(name_, info.name_);
    std::swap(version_, info.version_);
    std::swap(cell_num_, info.cell_num_);
    std::swap(sparse_, info.sparse_);
    std::swap(timestamp_range_, info.timestamp_range_);
    std::swap(fragment_size_, info.fragment_size_);
    std::swap(has_consolidated_footer_, info.has_consolidated_footer_);
    std::swap(non_empty_domain_, info.non_empty_domain_);
    std::swap(expanded_non_empty_domain_, info.expanded_non_empty_domain_);
    std::swap(array_schema_name_, info.array_schema_name_);
    std::swap(meta_, info.meta_);
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SINGLE_FRAGMENT_INFO_H
