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

#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/misc/uri.h"

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
    version_ = 0;
    cell_num_ = 0;
    sparse_ = false;
    timestamp_range_ = {0, 0};
    fragment_size_ = 0;
  }

  /** Constructor. */
  SingleFragmentInfo(
      const URI& uri,
      uint32_t version,
      bool sparse,
      const std::pair<uint64_t, uint64_t>& timestamp_range,
      uint64_t cell_num,
      uint64_t fragment_size,
      bool has_consolidated_footer,
      const NDRange& non_empty_domain,
      const NDRange& expanded_non_empty_domain,
      const EncryptionKey* encryption_key,
      FragmentMetadata* meta)
      : uri_(uri)
      , version_(version)
      , sparse_(sparse)
      , timestamp_range_(timestamp_range)
      , cell_num_(cell_num)
      , fragment_size_(fragment_size)
      , has_consolidated_footer_(has_consolidated_footer)
      , non_empty_domain_(non_empty_domain)
      , expanded_non_empty_domain_(expanded_non_empty_domain)
      , encryption_key_(encryption_key)
      , meta_(meta) {
    // encryption_key_ = tdb_make_shared(EncryptionKey);
    // encryption_key_->set_key(
    //     encryption_key->encryption_type(),
    //     encryption_key->key().data(),
    //     encryption_key->key().size());
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
  void dump(const std::vector<Datatype>& dim_types, FILE* out) const {
    if (out == nullptr)
      out = stdout;

    std::stringstream ss;
    ss << "  > URI: " << uri_.c_str() << "\n";
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
    fprintf(out, "%s", ss.str().c_str());
  }

  /** Returns `true` if the fragment is sparse. */
  bool sparse() const {
    return sparse_;
  }

  /** Returns the fragment URI. */
  const URI& uri() const {
    return uri_;
  }

  /** Returns the timestamp range. */
  const std::pair<uint64_t, uint64_t>& timestamp_range() const {
    return timestamp_range_;
  }

  uint32_t format_version() const {
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

  /** Returns a pointer to the encryption key. */
  const EncryptionKey* encryption_key() const {
    return encryption_key_;
  }

  /** Returns a pointer to the fragment's metadata. */
  FragmentMetadata* meta() const {
    return meta_;
  }

  /** Returns the non-empty domain in string format. */
  std::string non_empty_domain_str(
      const std::vector<Datatype>& dim_types) const {
    std::stringstream ss;
    for (uint32_t d = 0; d < (uint32_t)dim_types.size(); ++d) {
      switch (dim_types[d]) {
        case Datatype::INT8:
          ss << "[" << ((int8_t*)non_empty_domain_[d].data())[0] << ", "
             << ((int8_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::UINT8:
          ss << "[" << ((uint8_t*)non_empty_domain_[d].data())[0] << ", "
             << ((uint8_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::INT16:
          ss << "[" << ((int16_t*)non_empty_domain_[d].data())[0] << ", "
             << ((int16_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::UINT16:
          ss << "[" << ((uint16_t*)non_empty_domain_[d].data())[0] << ", "
             << ((uint16_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::INT32:
          ss << "[" << ((int32_t*)non_empty_domain_[d].data())[0] << ", "
             << ((int32_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::UINT32:
          ss << "[" << ((uint32_t*)non_empty_domain_[d].data())[0] << ", "
             << ((uint32_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::INT64:
          ss << "[" << ((int64_t*)non_empty_domain_[d].data())[0] << ", "
             << ((int64_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::UINT64:
          ss << "[" << ((uint64_t*)non_empty_domain_[d].data())[0] << ", "
             << ((uint64_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::FLOAT32:
          ss << "[" << ((float*)non_empty_domain_[d].data())[0] << ", "
             << ((float*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::FLOAT64:
          ss << "[" << ((double*)non_empty_domain_[d].data())[0] << ", "
             << ((double*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::DATETIME_YEAR:
        case Datatype::DATETIME_MONTH:
        case Datatype::DATETIME_WEEK:
        case Datatype::DATETIME_DAY:
        case Datatype::DATETIME_HR:
        case Datatype::DATETIME_MIN:
        case Datatype::DATETIME_SEC:
        case Datatype::DATETIME_MS:
        case Datatype::DATETIME_US:
        case Datatype::DATETIME_NS:
        case Datatype::DATETIME_PS:
        case Datatype::DATETIME_FS:
        case Datatype::DATETIME_AS:
        case Datatype::TIME_HR:
        case Datatype::TIME_MIN:
        case Datatype::TIME_SEC:
        case Datatype::TIME_MS:
        case Datatype::TIME_US:
        case Datatype::TIME_NS:
        case Datatype::TIME_PS:
        case Datatype::TIME_FS:
        case Datatype::TIME_AS:
          ss << "[" << ((int64_t*)non_empty_domain_[d].data())[0] << ", "
             << ((int64_t*)non_empty_domain_[d].data())[1] << "]";
          break;
        case Datatype::STRING_ASCII:
          ss << "[" << non_empty_domain_[d].start_str() << ", "
             << non_empty_domain_[d].end_str() << "]";
          break;
        default:
          assert(false);
          break;
      }
      if (d != (uint32_t)dim_types.size() - 1)
        ss << " x ";
    }

    return ss.str();
  }

 private:
  /** The fragment URI. */
  URI uri_;

  /** The format version of the fragment. */
  uint32_t version_;

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

  /** The private encryption key used to encrypt the array.
   *
   * Note: The Array and SingleFragmentInfo classes are the only two places in
   * TileDB where the user's private key bytes should be stored. Whenever a key
   * is needed, a pointer to this memory region should be passed instead of a
   * copy of the bytes.
   */
  const EncryptionKey* encryption_key_;

  /** The fragment metadata. **/
  FragmentMetadata* meta_;

  /**
   * Returns a deep copy of this FragmentInfo.
   * @return New FragmentInfo
   */
  SingleFragmentInfo clone() const {
    SingleFragmentInfo clone;
    clone.uri_ = uri_;
    clone.version_ = version_;
    clone.cell_num_ = cell_num_;
    clone.sparse_ = sparse_;
    clone.timestamp_range_ = timestamp_range_;
    clone.fragment_size_ = fragment_size_;
    clone.has_consolidated_footer_ = has_consolidated_footer_;
    clone.non_empty_domain_ = non_empty_domain_;
    clone.expanded_non_empty_domain_ = expanded_non_empty_domain_;
    clone.encryption_key_ = encryption_key_;
    clone.meta_ = meta_;
    return clone;
  }

  /** Swaps the contents (all field values) of this info with the given info. */
  void swap(SingleFragmentInfo& info) {
    std::swap(uri_, info.uri_);
    std::swap(version_, info.version_);
    std::swap(cell_num_, info.cell_num_);
    std::swap(sparse_, info.sparse_);
    std::swap(timestamp_range_, info.timestamp_range_);
    std::swap(fragment_size_, info.fragment_size_);
    std::swap(has_consolidated_footer_, info.has_consolidated_footer_);
    std::swap(non_empty_domain_, info.non_empty_domain_);
    std::swap(expanded_non_empty_domain_, info.expanded_non_empty_domain_);
    std::swap(encryption_key_, info.encryption_key_);
    std::swap(meta_, info.meta_);
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SINGLE_FRAGMENT_INFO_H
