/**
 * @file   file.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
 * This file implements class BlobArray.
 */

#include "tiledb/appl/blob_array/blob_array.h"
#include <magic.h>
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/mgc_dict.h"
#include "tiledb/sm/misc/time.h"
#include "tiledb/sm/query/query.h"

#include <filesystem>

using namespace tiledb::common;

namespace tiledb {
namespace appl {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

BlobArray::BlobArray(const URI& array_uri, StorageManager* storage_manager)
    : Array(array_uri, storage_manager)
    //, blob_array_schema_() {
    , blob_array_schema_sp_(tdb::make_shared<BlobArraySchema>(HERE()))
    , blob_array_schema_(*(blob_array_schema_sp_.get())) {
  // We want to default these incase the user doesn't set it.
  // This is required for writes to the query and the metadata get the same
  // timestamp
  // TBD: what to do about fragment timestamp collisions it also causes
  // with same timestamp being re-used for each open of same
  // array (object instance), leading timestamp collision issues if/when
  // there are re-uses of this array, which can manifest as bad data
  // (stored size in metadata does not match 'contents' attribute
  // data being retrieved from fragment)...
  // at least I think that's what's happening...
  //  timestamp_end_ = utils::time::timestamp_now_ms();
  //  timestamp_end_opened_at_ = timestamp_end_;
}

BlobArray::BlobArray(const BlobArray& rhs)
    : Array(rhs)
    //, blob_array_schema_(&rhs.blob_array_schema_) {
    , blob_array_schema_sp_(rhs.blob_array_schema_sp_)
    , blob_array_schema_(*(rhs.blob_array_schema_sp_.get())) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status BlobArray::create([[maybe_unused]] const Config* config) {
  try {
    auto cfg = config ? config : &config_;
    auto encryption_key = get_encryption_key_from_config(*cfg);
    RETURN_NOT_OK(storage_manager_->array_create(
        array_uri_, blob_array_schema_sp_, *encryption_key));

  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }
  return Status::Ok();
}

Status BlobArray::to_array_from_uri(const URI& file, const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status_BlobArrayError(
          "Can not save file; File opened in read mode; Reopen in write mode");

    VFS vfs;
    // Initialize VFS object
    auto stats = storage_manager_->stats();
    auto compute_tp = storage_manager_->compute_tp();
    auto io_tp = storage_manager_->io_tp();
    auto vfs_config = config ? config : nullptr;
    auto ctx_config = storage_manager_->config();
    RETURN_NOT_OK(vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config));

    VFSFileHandle vfsfh(file, &vfs, VFSMode::VFS_READ);

    RETURN_NOT_OK(to_array_from_vfs_fh(&vfsfh, config));
    RETURN_NOT_OK(vfsfh.close());
    RETURN_NOT_OK(vfs.terminate());
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::to_array_from_vfs_fh(
    VFSFileHandle* file, const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status_BlobArrayError(
          "Can not save file; File opened in read mode; Reopen in write mode");

    if (file->mode() != VFSMode::VFS_READ)
      return Status_BlobArrayError("File must be open in READ mode");

    // TODO: Add config option to let the user control how much of the file we
    // we read
    // We can support partial writes either global order (single fragment)
    // or row-major with multiple fragment but same timestamp
    uint64_t size = file->size();
    Buffer buffer;
    buffer.realloc(size);
    RETURN_NOT_OK(file->read(0, buffer.data(), size));
    RETURN_NOT_OK(to_array_from_buffer(buffer.data(), size, config));

    std::string uri_basename = file->uri().last_path_part();
    RETURN_NOT_OK(put_metadata(
        constants::blob_array_metadata_original_file_name_key.c_str(),
        Datatype::STRING_ASCII,
        static_cast<uint32_t>(uri_basename.size()),
        uri_basename.c_str()));

    Buffer file_metadata;
    uint64_t metadata_read_size = std::min<uint64_t>(1024, size);
    file_metadata.realloc(metadata_read_size);
    RETURN_NOT_OK(file->read(0, file_metadata.data(), metadata_read_size));

    // Save metadata
    std::filesystem::path uri_string = file->uri().last_path_part();
    const std::string extension = uri_string.extension().string();
    RETURN_NOT_OK(put_metadata(
        constants::blob_array_metadata_ext_key.c_str(),
        Datatype::STRING_ASCII,
        static_cast<uint32_t>(extension.size()),
        extension.c_str()));
    RETURN_NOT_OK(store_mime_type(file_metadata, metadata_read_size));
    RETURN_NOT_OK(store_mime_encoding(file_metadata, metadata_read_size));
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::to_array_from_buffer(
    void* data, uint64_t size, [[maybe_unused]] const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status_BlobArrayError(
          "Can not save file; File opened in read mode; Reopen in write mode");

    // We want timestamp_end_ and timestamp_end_opened_at_ set
    // This is required for writes to the query and the metadata to get the same
    // timestamp.
    // But, since the array may not be exclusively "ours", make sure they are
    // reset to whatever they were.
    auto current_timestamp_end = this->timestamp_end();
    auto current_timestamp_opened_at = this->timestamp_end_opened_at_;
    auto reset_timestamp_end = [&]() {
      this->timestamp_end_ = current_timestamp_end;
      this->timestamp_end_opened_at_ = current_timestamp_opened_at;
      // canNOT .reset() the metadta timestamp here as that has side-effect of
      // clear()ing the metadata whose gen'd uri and data must be retained
      // until the array is closed.
    };
    ScopedExecutor onexit(reset_timestamp_end);

    // If a timestamp_end_ has not been set, set it to now...
    if (current_timestamp_end == UINT64_MAX) {
      timestamp_end_ = utils::time::timestamp_now_ms();
      // static decltype(utils::time::timestamp_now_ms()) faux_time = 23;
      // timestamp_end_ = ++faux_time;
    }
    // ...and make sure this matches, whether set by above code or from user.
    timestamp_end_opened_at_ = timestamp_end_;

    // set timestamp to be used for metadata uri
    // this->metadata_.reset(timestamp_end_);
    decltype(timestamp_end_opened_at_) timestamp_end_opened_at_to_set;
    if (this->timestamp_end_opened_at_ == UINT64_MAX) {
      timestamp_end_opened_at_to_set = 0;
    } else {
      timestamp_end_opened_at_to_set = this->timestamp_end_opened_at_;
    }
    this->metadata_.reset(timestamp_end_opened_at_to_set);
    // make sure this array's metadata uri gen'd with that timestamp.
    this->metadata_.generate_uri(this->array_uri_);

    Query query(storage_manager_, this);

    // Set write buffer and aux items when data attr is variable length blob
    RETURN_NOT_OK(query.set_data_buffer(
        constants::blob_array_attribute_name, data, &size));
    uint64_t ofs_buf[] = {0};
    uint64_t sizeof_ofs_buf = sizeof(ofs_buf);
    RETURN_NOT_OK(query.set_offsets_buffer(
        constants::blob_array_attribute_name, ofs_buf, &sizeof_ofs_buf));
    std::array<uint64_t, 2> v_subarray = {0, 0};

    // Set subarray
    RETURN_NOT_OK(query.set_subarray(&v_subarray));
    RETURN_NOT_OK(query.submit());

    // note that on exit (via ScopedExecutor above) 'this' routine will
    // reset timestamp used in generating the metadata uri, -however-
    // as the code elsewhere currently exists the generated uri itself (having
    // been generated with the locally generated timestamp .reset() just above)
    // will still be used when metadata is written on close.

    RETURN_NOT_OK(put_metadata(
        constants::blob_array_metadata_size_key.c_str(),
        Datatype::UINT64,
        1,
        &size));

  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::export_to_uri(const URI& file, const Config* config) {
  try {
    if (query_type_ != QueryType::READ)
      return Status_BlobArrayError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");
    VFS vfs;
    // Initialize VFS object
    auto stats = storage_manager_->stats();
    auto compute_tp = storage_manager_->compute_tp();
    auto io_tp = storage_manager_->io_tp();
    auto vfs_config = config ? config : nullptr;
    auto ctx_config = storage_manager_->config();
    RETURN_NOT_OK(vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config));

    VFSFileHandle vfsfh(file, &vfs, VFSMode::VFS_WRITE);

    RETURN_NOT_OK(export_to_vfs_fh(&vfsfh, config));
    RETURN_NOT_OK(vfsfh.close());
    RETURN_NOT_OK(vfs.terminate());
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }
  return Status::Ok();
}

Status BlobArray::export_to_vfs_fh(
    VFSFileHandle* file, [[maybe_unused]] const Config* config) {
  try {
    // TODO: handle partial exports
    // uint64_t offset = 0;
    if (query_type_ != QueryType::READ)
      return Status_BlobArrayError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");

    if (file->mode() != VFSMode::VFS_WRITE &&
        file->mode() != VFSMode::VFS_APPEND)
      return Status_BlobArrayError("File must be open in WRITE OR APPEND mode");

    const uint64_t* ptr_file_size;
    if (auto s = size(ptr_file_size); !s.ok()) {
      return s;
    }
    if (ptr_file_size == nullptr) {
      return Status_BlobArrayError(
          "Unable to export file, file size metadata not found.");
    }
    // Handle empty file
    if (*ptr_file_size == 0) {
      return Status::Ok();
    }
    uint64_t buffer_size = *ptr_file_size;

    Buffer data;
    data.realloc(buffer_size);

    Query query(storage_manager_, this);

    // Set read buffer
    // TODO: Add config option to let the user control how much of the file we
    // we read
    // Set buffer and aux items when data attr is variable length blob
    RETURN_NOT_OK(query.set_data_buffer(
        constants::blob_array_attribute_name, data.data(), &buffer_size));
    uint64_t ofs_buf[] = {0};
    uint64_t sizeof_ofs_buf = sizeof(ofs_buf);
    RETURN_NOT_OK(query.set_offsets_buffer(
        constants::blob_array_attribute_name, ofs_buf, &sizeof_ofs_buf));
    std::array<uint64_t, 2> subarray = {0, 0};

    // Set subarray
    RETURN_NOT_OK(query.set_subarray(&subarray));
    do {
      RETURN_NOT_OK(query.submit());

      // Check if query could not be completed
      if (buffer_size == 0) {
        if (auto qstat = query.status(); qstat != QueryStatus::COMPLETED) {
          std::stringstream msg;
          msg << "export_to_vfs_fh, query.status() == "
              << query_status_str(qstat);
          //<< " retry_cnt " << retry_cnt;
          // LOG_STATUS(msg.str());
          LOG_STATUS(Status_BlobArrayError(msg.str()));
        }
        return Status_BlobArrayError(
            "Unable to export entire file; Query not able to complete with "
            "records");
      }

      file->write(data.data(), buffer_size);

    } while (query.status() != QueryStatus::COMPLETED);

  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }
  return Status::Ok();
}

Status BlobArray::size(const uint64_t*& size) {
  Datatype datatype = Datatype::UINT64;
  uint32_t val_num = 1;
  THROW_NOT_OK(get_metadata(
      constants::blob_array_metadata_size_key.c_str(),
      &datatype,
      &val_num,
      reinterpret_cast<const void**>(&size)));

  if (size == nullptr) {
    return Status_BlobArrayError("file size metadata not found");
  }

  return Status::Ok();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

tdb_unique_ptr<EncryptionKey> BlobArray::get_encryption_key_from_config(
    const Config& config) const {
  std::string encryption_key_from_cfg;
  const char* encryption_key_cstr = nullptr;
  EncryptionType encryption_type = EncryptionType::NO_ENCRYPTION;
  tdb_unique_ptr<EncryptionKey> encryption_key =
      tdb_unique_ptr<EncryptionKey>(tdb_new(EncryptionKey));
  uint64_t key_length = 0;
  bool found = false;
  encryption_key_from_cfg = config.get("sm.encryption_key", &found);
  assert(found);
  if (!encryption_key_from_cfg.empty()) {
    encryption_key_cstr = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    found = false;
    encryption_type_from_cfg = config.get("sm.encryption_type", &found);
    assert(found);
    auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
    THROW_NOT_OK(st);
    encryption_type = et.value();
    if (EncryptionKey::is_valid_key_length(
            encryption_type,
            static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
      const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
      if (unit_test_cfg.array_encryption_key_length.is_set()) {
        key_length = unit_test_cfg.array_encryption_key_length.get();
      } else {
        key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      }
    } else {
      encryption_key_cstr = nullptr;
      key_length = 0;
    }
  }
  // Copy the key bytes.
  THROW_NOT_OK(encryption_key->set_key(
      encryption_type, encryption_key_cstr, key_length));
  return encryption_key;
}

// const char*
Status BlobArray::libmagic_get_mime_type(
    const char** mime_type, void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_TYPE);
  // NOTE: windows - contradiction in return values...
  //...have seen magic_load() return -1 indicating error, then magic_error()
  // returning 0 indicating no error
  //...apparent cause being that it could not find one of its .mgc databases to
  // load try set MAGIC=<path-of-build-tree-dir>/externals/install/bin/magic.mgc
  if (auto rval = magic_dict::magic_load(magic); rval != 0) {
    auto str_rval = std::to_string(rval);
    auto err = magic_error(magic);
    if (!err) {
      err =
          "(magic_error() returned 0, try setting env var 'MAGIC' to location "
          "of magic.mgc or "
          "alternate!)";
    }
    magic_close(magic);
    return LOG_STATUS(Status_BlobArrayError(
        std::string("cannot load magic database - ") + str_rval + err));
  }
  *mime_type = magic_buffer(magic, data, size);
  return Status::Ok();
}

Status BlobArray::libmagic_get_mime_encoding(
    const char** mime_encoding, void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_ENCODING);
  if (auto rval = magic_dict::magic_load(magic); rval != 0) {
    auto str_rval = std::to_string(rval);
    auto err = magic_error(magic);
    if (!err) {
      err =
          "(magic_error() returned 0, try setting env var 'MAGIC' to location "
          "of magic.mgc or "
          "alternate!)";
    }
    magic_close(magic);
    return LOG_STATUS(Status_BlobArrayError(
        std::string("cannot load magic database - ") + str_rval + err));
  }
  *mime_encoding = magic_buffer(magic, data, size);
  return Status::Ok();
}

Status BlobArray::store_mime_type(
    const Buffer& file_metadata, uint64_t metadata_read_size) {
  const char* mime_type = nullptr;
  auto status = libmagic_get_mime_type(
      &mime_type, file_metadata.data(), metadata_read_size);
  RETURN_NOT_OK(status);
  uint64_t mime_size = 0;
  if (mime_type != nullptr) {
    mime_size = strlen(mime_type);
  }
  return put_metadata(
      constants::blob_array_metadata_mime_type_key.c_str(),
      Datatype::STRING_ASCII,
      mime_size,
      mime_type);
}

Status BlobArray::store_mime_encoding(
    const Buffer& file_metadata, uint64_t metadata_read_size) {
  const char* mime_encoding = nullptr;
  auto status = libmagic_get_mime_encoding(
      &mime_encoding, file_metadata.data(), metadata_read_size);
  RETURN_NOT_OK(status);
  uint64_t mime_size = 0;
  if (mime_encoding != nullptr) {
    mime_size = strlen(mime_encoding);
  }
  return put_metadata(
      constants::blob_array_metadata_mime_encoding_key.c_str(),
      Datatype::STRING_ASCII,
      mime_size,
      mime_encoding);
}

}  // namespace appl
}  // namespace tiledb
