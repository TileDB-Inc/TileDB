/**
 * @file   file.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements class File.
 */

#include "tiledb/appl/blob_array/blob_array.h"
#include <magic.h>
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/misc/time.h"

#include <filesystem>

using namespace tiledb::common;

namespace tiledb {
namespace appl {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

BlobArray::BlobArray(const URI& array_uri, StorageManager* storage_manager)
    : Array(array_uri, storage_manager)
    , blob_array_schema_() {
  // We want to default these incase the user doesn't set it.
  // This is required for writes to the query and the metadata get the same
  // timestamp
  timestamp_end_ = utils::time::timestamp_now_ms();
  timestamp_end_opened_at_ = timestamp_end_;
};

/* ********************************* */
/*                API                */
/* ********************************* */

Status BlobArray::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  //  Array::open(query_type, encryption_type, encryption_key, key_length);
  return Array::open(
      query_type,
      timestamp_start_,
      timestamp_end_,
      encryption_type,
      encryption_key,
      key_length);
}

Status BlobArray::open(
    QueryType query_type,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  return Array::open(
      query_type,
      timestamp_start,
      timestamp_end,
      encryption_type,
      encryption_key,
      key_length);
}

Status BlobArray::create([[maybe_unused]] const Config* config) {
  try {
    auto encryption_key = get_encryption_key_from_config(config_);
    RETURN_NOT_OK(storage_manager_->array_create(
        array_uri_, &blob_array_schema_, *encryption_key));
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::create_from_uri(const URI& file, const Config* config) {
  try {
    VFS vfs;
    // Initialize VFS object
    auto stats = storage_manager_->stats();
    auto compute_tp = storage_manager_->compute_tp();
    auto io_tp = storage_manager_->io_tp();
    auto vfs_config = config ? config : nullptr;
    auto ctx_config = storage_manager_->config();
    RETURN_NOT_OK(vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config));

    VFSFileHandle vfsfh(file, &vfs, VFSMode::VFS_READ);

    RETURN_NOT_OK(create_from_vfs_fh(&vfsfh, config));
    RETURN_NOT_OK(vfsfh.close());
    RETURN_NOT_OK(vfs.terminate());
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }
  return Status::Ok();
}

#if 01
Status BlobArray::create_from_vfs_fh(
    const VFSFileHandle* file, [[maybe_unused]] const Config* config) {
  try {
    if (file->mode() != VFSMode::VFS_READ)
      return Status_BlobArrayError("File must be open in READ mode");

    auto cfg = config ? config : &config_;
    uint64_t size = file->size();
    blob_array_schema_.set_schema_based_on_file_details(size, false);
    auto encrpytion_key = get_encryption_key_from_config(*cfg); //config_);
    RETURN_NOT_OK(storage_manager_->array_create(
        array_uri_, &blob_array_schema_, *encrpytion_key));
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::save_from_file_handle(FILE* in, const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status_BlobArrayError(
          "Can not save file; File opened in read mode not write mode");

    // Get file size
    fseek(in, 0L, SEEK_END);
    uint64_t size = ftell(in);
    rewind(in);

    // TODO: Add config option to let the user control how much of the file we
    // we read
    // We can support partial writes either global order (single fragment)
    // or row-major with multiple fragment but same timestamp
    Buffer buffer;
    buffer.realloc(size);
    size = fread(buffer.data(), 1, size, in);
    rewind(in);

    RETURN_NOT_OK(save_from_buffer(buffer.data(), size, config));

    // TODO: Maybe we don't want FILE* since cross platform its hard to handle
    // finding the name of the opened file
    //    std::string uri_string = file->uri().to_string();
    //    put_metadata(
    //        constants::file_metadata_original_file_name_key.c_str(),
    //        Datatype::STRING_ASCII,
    //        uri_string.size(),
    //        uri_string.c_str());
    // TODO: add these
    //    put_metadata(constants::file_metadata_ext_key.c_str(),
    //    Datatype::STRING_ASCII, uri_string.size(), uri_string.c_str());

    // Get MIME and Encodings
    Buffer file_metadata;
    uint64_t metadata_read_size = std::min<uint64_t>(1024, size);
    file_metadata.realloc(metadata_read_size);
    metadata_read_size = fread(file_metadata.data(), 1, metadata_read_size, in);
    store_mime_type(file_metadata, metadata_read_size);
    store_mime_encoding(file_metadata, metadata_read_size);
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::save_from_uri(const URI& file, const Config* config) {
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

    RETURN_NOT_OK(save_from_vfs_fh(&vfsfh, config));
    RETURN_NOT_OK(vfsfh.close());
    RETURN_NOT_OK(vfs.terminate());
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::save_from_vfs_fh(VFSFileHandle* file, const Config* config) {
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
    RETURN_NOT_OK(save_from_buffer(buffer.data(), size, config));

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
    store_mime_type(file_metadata, metadata_read_size);
    store_mime_encoding(file_metadata, metadata_read_size);
  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}

Status BlobArray::save_from_buffer(
    void* data, uint64_t size, [[maybe_unused]] const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status_BlobArrayError(
          "Can not save file; File opened in read mode; Reopen in write mode");

    Query query(storage_manager_, this);

    // Set write buffer
    // (when attribute was fixed size byte and contents were spread across cells
//    RETURN_NOT_OK(
//        query.set_buffer(constants::blob_array_attribute_name, data, &size));
    // std::array<uint64_t, 2> subarray = {0, size - 1};

    // Set write buffer and aux items when data attr is variable length blob
    RETURN_NOT_OK(
        query.set_data_buffer(constants::blob_array_attribute_name, data, &size));
    uint64_t ofs_buf[] = {0};
    uint64_t sizeof_ofs_buf = sizeof(ofs_buf);
    RETURN_NOT_OK(query.set_offsets_buffer(constants::blob_array_attribute_name, ofs_buf, &sizeof_ofs_buf));
    std::array<uint64_t, 2> subarray = {0, 0};

    // Set subarray
    RETURN_NOT_OK(query.set_subarray(&subarray));
    RETURN_NOT_OK(query.submit());

    RETURN_NOT_OK(put_metadata(
        constants::blob_array_metadata_size_key.c_str(), Datatype::UINT64, 1, &size));

  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }

  return Status::Ok();
}
#endif

Status BlobArray::export_to_file_handle(
    FILE* out, [[maybe_unused]] const Config* config) {
  try {
    //uint64_t offset = 0;
    if (query_type_ != QueryType::READ)
      return Status_BlobArrayError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");

    uint64_t file_size = size();
    uint64_t buffer_size = file_size;
    Buffer data;
    data.realloc(buffer_size);

    Query query(storage_manager_, this);

    // Set read buffer
    // TODO: Add config option to let the user control how much of the file we
    // we read
    // TODO: handle offset reading
//    RETURN_NOT_OK(query.set_buffer(
//        constants::blob_array_attribute_name, data.data(), &buffer_size));
//    std::array<uint64_t, 2> subarray = {offset, offset + file_size - 1};
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
      if (buffer_size == 0)
        return Status_BlobArrayError(
            "Unable to export entire file; Query not able to complete with "
            "records");

      uint64_t written_bytes = fwrite(data.data(), 1, buffer_size, out);
      if (written_bytes != buffer_size)
        global_logger().warn(
            "File export wrote " + std::to_string(written_bytes) +
            " but file size is " + std::to_string(file_size) +
            ". The export likely is incomplete.");

    } while (query.status() != QueryStatus::COMPLETED);

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
    //uint64_t offset = 0;
    if (query_type_ != QueryType::READ)
      return Status_BlobArrayError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");

    if (file->mode() != VFSMode::VFS_WRITE &&
        file->mode() != VFSMode::VFS_APPEND)
      return Status_BlobArrayError("File must be open in WRITE OR APPEND mode");

    uint64_t file_size = size();
    // Handle empty file
    if (file_size == 0) {
      return Status::Ok();
    }
    uint64_t buffer_size = file_size;
    Buffer data;
    data.realloc(buffer_size);

    Query query(storage_manager_, this);

    // Set read buffer
    // TODO: Add config option to let the user control how much of the file we
    // we read
    // TODO: handle offset reading
//    RETURN_NOT_OK(query.set_buffer(
//        constants::blob_array_attribute_name, data.data(), &buffer_size));
//    std::array<uint64_t, 2> subarray = {offset, offset + file_size - 1};
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
      if (buffer_size == 0)
        return Status_BlobArrayError(
            "Unable to export entire file; Query not able to complete with "
            "records");

      file->write(data.data(), buffer_size);

    } while (query.status() != QueryStatus::COMPLETED);

  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }
  return Status::Ok();
}

Status BlobArray::export_to_buffer(
    void* data,
    uint64_t* size,
    uint64_t file_offset,
    [[maybe_unused]] const Config* config) {
  (void)file_offset;
  try {
    if (query_type_ != QueryType::READ)
      return Status_BlobArrayError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");

    Query query(storage_manager_, this);

    //    RETURN_NOT_OK(query.set_buffer(constants::file_attribute_name, data,
    //    size));
//    std::array<uint64_t, 2> subarray = {file_offset, file_offset + *size - 1};
    // Set buffer and aux items when data attr is variable length blob
    RETURN_NOT_OK(query.set_data_buffer(
        constants::blob_array_attribute_name, data, size));
    uint64_t ofs_buf[] = {0};
    uint64_t sizeof_ofs_buf = sizeof(ofs_buf);
    RETURN_NOT_OK(query.set_offsets_buffer(
        constants::blob_array_attribute_name, ofs_buf, &sizeof_ofs_buf));
    std::array<uint64_t, 2> subarray = {0, 0};

    // Set subarray
    RETURN_NOT_OK(query.set_subarray(&subarray));
    // Set read buffer
    uint64_t original_buffer_size = *size;
    uint64_t size_read = 0;
//    RETURN_NOT_OK(query.set_buffer(constants::blob_array_attribute_name, data, size));
    do {
      RETURN_NOT_OK(query.submit());

      // Check if query could not be completed
      if (*size == 0)
        return Status_BlobArrayError(
            "Unable to export file; Query not able to complete with "
            "records");

      size_read += *size;
      if (query.status() == QueryStatus::INCOMPLETE) {
        *size = original_buffer_size - size_read;
        RETURN_NOT_OK(query.set_buffer(
            constants::blob_array_attribute_name,
            (static_cast<char*>(data) + *size),
            size));
      }

    } while (query.status() != QueryStatus::COMPLETED);

    *size = size_read;

  } catch (const std::exception& e) {
    return Status_BlobArrayError(e.what());
  }
  return Status::Ok();
}

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

uint64_t BlobArray::size() {
  const uint64_t* size = nullptr;
  Datatype datatype = Datatype::UINT64;
  uint32_t val_num = 1;
  THROW_NOT_OK(get_metadata(
      constants::blob_array_metadata_size_key.c_str(),
      &datatype,
      &val_num,
      reinterpret_cast<const void**>(&size)));

  if (size == nullptr)
    return 0;

  return *size;
}

Status BlobArray::size(uint64_t* size) {
  Datatype datatype = Datatype::UINT64;
  uint32_t val_num = 1;
  *size = 0;
  const uint64_t* size_meta;
  RETURN_NOT_OK(get_metadata(
      constants::blob_array_metadata_size_key.c_str(),
      &datatype,
      &val_num,
      reinterpret_cast<const void**>(&size_meta)));
  *size = *size_meta;

  return Status::Ok();
}

Status BlobArray::mime_type(const char** mime_type, uint32_t* size) {
  Datatype datatype = Datatype::STRING_ASCII;
  *mime_type = nullptr;
  *size = 0;
  RETURN_NOT_OK(get_metadata(
      constants::blob_array_metadata_mime_encoding_key.c_str(),
      &datatype,
      size,
      reinterpret_cast<const void**>(mime_type)));

  return Status::Ok();
}

Status BlobArray::mime_encoding(const char** mime_encoding, uint32_t* size) {
  Datatype datatype = Datatype::STRING_ASCII;
  *mime_encoding = nullptr;
  *size = 0;
  RETURN_NOT_OK(get_metadata(
      constants::blob_array_metadata_mime_encoding_key.c_str(),
      &datatype,
      size,
      reinterpret_cast<const void**>(mime_encoding)));

  return Status::Ok();
}

Status BlobArray::original_name(const char** original_name, uint32_t* size) {
  Datatype datatype = Datatype::STRING_ASCII;
  *original_name = nullptr;
  *size = 0;
  RETURN_NOT_OK(get_metadata(
      constants::blob_array_metadata_original_file_name_key.c_str(),
      &datatype,
      size,
      reinterpret_cast<const void**>(original_name)));

  return Status::Ok();
}

Status BlobArray::file_extension(const char** ext, uint32_t* size) {
  Datatype datatype = Datatype::STRING_ASCII;
  *ext = nullptr;
  *size = 0;
  RETURN_NOT_OK(get_metadata(
      constants::blob_array_metadata_ext_key.c_str(),
      &datatype,
      size,
      reinterpret_cast<const void**>(ext)));

  return Status::Ok();
}

const char* BlobArray::libmagic_get_mime(void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_TYPE);
  //TBD: windows - contradiction in return values...
  //...have seen magic_load() return -1 indicating error, then magic_error() returning 0 indicating no error
  //...apparent cause being that it could not find one of igs .mgc databases to load
  //try set MAGIC=<path-of-build-tree-dir>/externals/install/bin/magic.mgc
  if (auto rval = magic_load(magic, nullptr); rval != 0) {
    auto str_rval = std::to_string(rval);
    auto err = magic_error(magic);
    LOG_STATUS(Status_BlobArrayError(
        std::string("cannot load magic database - ") + str_rval + err)); //magic_error(magic)));
    magic_close(magic);
    return nullptr;
  }
  return magic_buffer(magic, data, size);
}

const char* BlobArray::libmagic_get_mime_encoding(void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_ENCODING);
  if (magic_load(magic, nullptr) != 0) {
    LOG_STATUS(Status_BlobArrayError(
        std::string("cannot load magic database - ") + magic_error(magic)));
    magic_close(magic);
    return nullptr;
  }
  return magic_buffer(magic, data, size);
}

Status BlobArray::store_mime_type(
    const Buffer& file_metadata, uint64_t metadata_read_size) {
  const char* mime =
      libmagic_get_mime(file_metadata.data(), metadata_read_size);
  uint64_t mime_size = 0;
  if (mime != nullptr) {
    mime_size = strlen(mime);
  }
  return put_metadata(
      constants::blob_array_metadata_mime_type_key.c_str(),
      Datatype::STRING_ASCII,
      mime_size,
      mime);
}

Status BlobArray::store_mime_encoding(
    const Buffer& file_metadata, uint64_t metadata_read_size) {
  const char* mime =
      libmagic_get_mime_encoding(file_metadata.data(), metadata_read_size);
  uint64_t mime_size = 0;
  if (mime != nullptr) {
    mime_size = strlen(mime);
  }
  return put_metadata(
      constants::blob_array_metadata_mime_encoding_key.c_str(),
      Datatype::STRING_ASCII,
      mime_size,
      mime);
}

}  // namespace sm
}  // namespace tiledb
