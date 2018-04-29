/**
 * @file   kv.cc
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
 * This file implements class KV.
 */

#include "tiledb/sm/kv/kv.h"
#include "tiledb/sm/misc/logger.h"

#include <cassert>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

KV::KV(StorageManager* storage_manager)
    : storage_manager_(storage_manager) {
  read_attribute_num_ = 0;
  read_attributes_ = nullptr;
  schema_ = nullptr;
  write_attribute_num_ = 0;
  write_attributes_ = nullptr;
  max_items_ = constants::kv_max_items;
  write_buffers_ = nullptr;
  write_buffer_sizes_ = nullptr;
  write_buffer_num_ = 0;
  read_buffers_ = nullptr;
  read_buffer_sizes_ = nullptr;
  read_buffer_num_ = 0;
}

KV::~KV() {
  finalize();
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status KV::init(
    const std::string& kv_uri,
    const char** attributes,
    unsigned attribute_num,
    bool include_keys) {
  kv_uri_ = URI(kv_uri);
  RETURN_NOT_OK(storage_manager_->load_array_schema(kv_uri_, &schema_));

  if (attributes == nullptr || attribute_num == 0) {  // Load all attributes
    write_good_ = true;
    auto attr_num = schema_->attribute_num();
    unsigned i = (include_keys) ? 0 : 2;
    for (; i < attr_num; ++i) {
      attributes_.emplace_back(schema_->attribute_name(i));
      types_.emplace_back(schema_->type(i));
    }
  } else {  // Load input attributes
    write_good_ = false;
    if (include_keys) {
      attributes_.emplace_back(constants::key_attr_name);
      types_.emplace_back(constants::key_attr_type);
      attributes_.emplace_back(constants::key_type_attr_name);
      types_.emplace_back(constants::key_type_attr_type);
    }
    unsigned attr_id;
    for (unsigned i = 0; i < attribute_num; ++i) {
      auto attr_name = attributes[i];
      RETURN_NOT_OK(schema_->attribute_id(attr_name, &attr_id));
      attributes_.emplace_back(attr_name);
      types_.emplace_back(schema_->type(attr_id));
    }
  }

  assert(attributes_.size() == types_.size());
  RETURN_NOT_OK(prepare_query_attributes());
  RETURN_NOT_OK(schema_->buffer_num(
      (const char**)read_attributes_, read_attribute_num_, &read_buffer_num_));
  RETURN_NOT_OK(init_read_buffers());
  RETURN_NOT_OK(schema_->buffer_num(
      (const char**)write_attributes_,
      write_attribute_num_,
      &write_buffer_num_));

  return Status::Ok();
}

void KV::clear() {
  attributes_.clear();
  read_attribute_var_sizes_.clear();
  read_attribute_types_.clear();
  write_attribute_var_sizes_.clear();
  delete schema_;
  schema_ = nullptr;
  kv_uri_ = URI("");

  clear_items();
  clear_query_attributes();
  clear_query_buffers();
}

Status KV::finalize() {
  RETURN_NOT_OK(flush());
  clear();

  return Status::Ok();
}

Status KV::add_item(const KVItem* kv_item) {
  // Check if we are good for writes
  if (!write_good_) {
    return LOG_STATUS(
        Status::KVError("Cannot add item; Key-value store was not opened "
                        "properly for writes"));
  }

  if (items_.size() >= max_items_)
    RETURN_NOT_OK(flush());

  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);

  if (!kv_item->good(attributes_, types_)) {
    return LOG_STATUS(Status::KVError("Cannot add item; Invalid item"));
  }

  auto new_item = new KVItem();
  *new_item = *kv_item;
  items_[new_item->key()->hash_] = new_item;

  return Status::Ok();
}

Status KV::get_item(
    const void* key, Datatype key_type, uint64_t key_size, KVItem** kv_item) {
  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);

  // Create key-value item
  *kv_item = new (std::nothrow) KVItem();
  if (*kv_item == nullptr)
    return LOG_STATUS(
        Status::KVError("Cannot get item; Memory allocation failed"));

  // Set key
  auto st = (*kv_item)->set_key(key, key_type, key_size);
  if (!st.ok()) {
    delete *kv_item;
    *kv_item = nullptr;
    return st;
  }

  // If the item is buffered, copy and return
  auto it = items_.find((*kv_item)->key()->hash_);
  if (it != items_.end()) {
    **kv_item = *(it->second);
    return Status::Ok();
  }

  // Query
  bool found = false;
  st = read_item((*kv_item)->hash(), &found);
  if (!st.ok() || !found) {
    delete *kv_item;
    *kv_item = nullptr;
    return st;
  }

  // Set values
  unsigned bid = 0, aid = 0;
  const void* value = nullptr;
  uint64_t value_size = 0;
  for (const auto& attr : attributes_) {
    auto var = read_attribute_var_sizes_[aid];
    value = read_buffers_[bid + int(var)];
    value_size = read_buffer_sizes_[bid + (int)var];
    st = (*kv_item)->set_value(
        attr, value, read_attribute_types_[aid++], value_size);
    if (!st.ok()) {
      delete *kv_item;
      *kv_item = nullptr;
      return st;
    }
    bid += 1 + (int)var;
  }

  return Status::Ok();
}

Status KV::get_item(const KVItem::Hash& hash, KVItem** kv_item) {
  // Create key-value item
  *kv_item = new (std::nothrow) KVItem();
  if (*kv_item == nullptr)
    return LOG_STATUS(
        Status::KVError("Cannot get item; Memory allocation failed"));

  // Query
  bool found = false;
  auto st = read_item(hash, &found);
  if (!st.ok() || !found) {
    delete *kv_item;
    *kv_item = nullptr;
    return st;
  }

  // Set key and values
  unsigned bid = 0, aid = 0;
  const void* value = nullptr;
  const void* key = nullptr;
  uint64_t key_size = 0;
  uint64_t value_size = 0;
  Datatype key_type = Datatype::CHAR;
  bool key_found = false;
  bool key_type_found = false;
  for (const auto& attr : attributes_) {
    // Set key
    if (attr == constants::key_attr_name) {
      key = read_buffers_[bid + 1];
      key_size = read_buffer_sizes_[bid + 1];
      aid++;
      bid += 2;
      key_found = true;
      continue;
    }

    // Set key type
    if (attr == constants::key_type_attr_name) {
      key_type = static_cast<Datatype>(((char*)read_buffers_[bid])[0]);
      aid++;
      bid++;
      key_type_found = true;
      continue;
    }

    // Set values
    auto var = read_attribute_var_sizes_[aid];
    value = read_buffers_[bid + int(var)];
    value_size = read_buffer_sizes_[bid + (int)var];
    st = (*kv_item)->set_value(
        attr, value, read_attribute_types_[aid++], value_size);
    if (!st.ok()) {
      delete *kv_item;
      *kv_item = nullptr;
      return st;
    }
    bid += 1 + (int)var;
  }

  // Set key
  assert(key_found && key_type_found);
  if (key_found && key_type_found) {
    auto st = (*kv_item)->set_key(key, key_type, key_size, hash);
    if (!st.ok()) {
      delete *kv_item;
      *kv_item = nullptr;
      return st;
    }
  }

  return Status::Ok();
}

Status KV::flush() {
  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);

  // No items to flush
  if (items_.empty()) {
    return Status::Ok();
  }

  auto st = prepare_write_buffers();
  if (!st.ok()) {
    return st;
  }

  st = submit_write_query();
  if (st.ok())
    clear_items();

  return st;
}

void KV::clear_items() {
  for (auto& i : items_)
    delete i.second;
  items_.clear();
}

Status KV::set_max_buffered_items(uint64_t max_items) {
  if (max_items == 0)
    return LOG_STATUS(Status::KVError(
        "Cannot set maximum buffered items; Maximum items cannot be 0"));

  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);
  max_items_ = max_items;
  return Status::Ok();
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

Status KV::add_key(const KVItem::Key& key) {
  assert(key.key_ != nullptr && key.key_size_ > 0);

  // Aliases
  auto& buff_coords = *write_buff_vec_[0];
  auto& buff_key_offsets = *write_buff_vec_[1];
  auto& buff_keys = *write_buff_vec_[2];
  auto& buff_key_types = *write_buff_vec_[3];

  RETURN_NOT_OK(buff_coords.write(&(key.hash_.first), sizeof(key.hash_.first)));
  RETURN_NOT_OK(
      buff_coords.write(&(key.hash_.second), sizeof(key.hash_.second)));
  uint64_t offset = buff_keys.size();
  RETURN_NOT_OK(buff_key_offsets.write(&offset, sizeof(offset)));
  RETURN_NOT_OK(buff_keys.write(key.key_, key.key_size_));
  auto key_type_c = static_cast<char>(key.key_type_);
  RETURN_NOT_OK(buff_key_types.write(&key_type_c, sizeof(key_type_c)));

  return Status::Ok();
}

Status KV::add_value(const KVItem::Value& value, unsigned bid, bool var) {
  assert(bid < write_buffer_num_);

  auto idx = bid;

  // Add offset only for variable-sized attributes
  if (var) {
    uint64_t offset = write_buff_vec_[idx + 1]->size();
    RETURN_NOT_OK(write_buff_vec_[idx++]->write(&offset, sizeof(offset)));
  }

  // Add value
  RETURN_NOT_OK(write_buff_vec_[idx]->write(value.value_, value.value_size_));

  return Status::Ok();
}

void KV::clear_query_attributes() {
  // Read attributes
  if (read_attributes_ != nullptr) {
    for (unsigned int i = 0; i < read_attribute_num_; ++i)
      delete[] read_attributes_[i];
    delete[] read_attributes_;
    read_attributes_ = nullptr;
    read_attribute_num_ = 0;
  }

  // Write attributes
  if (write_attributes_ != nullptr) {
    for (unsigned int i = 0; i < write_attribute_num_; ++i)
      delete[] write_attributes_[i];
    delete[] write_attributes_;
    write_attributes_ = nullptr;
    write_attribute_num_ = 0;
  }
}

void KV::clear_query_buffers() {
  clear_read_buffers();
  clear_read_buff_vec();
  clear_write_buffers();
  clear_write_buff_vec();
}

void KV::clear_read_buff_vec() {
  for (auto& buff : read_buff_vec_)
    delete buff;
  read_buff_vec_.clear();
}

void KV::clear_read_buffers() {
  delete[] read_buffers_;
  read_buffers_ = nullptr;
  delete[] read_buffer_sizes_;
  read_buffer_sizes_ = nullptr;
}

void KV::clear_write_buff_vec() {
  for (auto& buff : write_buff_vec_)
    delete buff;
  write_buff_vec_.clear();
}

void KV::clear_write_buffers() {
  delete[] write_buffers_;
  write_buffers_ = nullptr;
  delete[] write_buffer_sizes_;
  write_buffer_sizes_ = nullptr;
}

Status KV::init_read_buffers() {
  for (unsigned i = 0; i < read_buffer_num_; ++i) {
    auto buff = new Buffer();
    read_buff_vec_.emplace_back(buff);
  }

  return Status::Ok();
}

Status KV::populate_write_buffers() {
  // Reset buffers
  for (auto buff : write_buff_vec_)
    buff->reset_size();

  // If this is the first time to populate the write buffers
  if (write_buff_vec_.empty()) {
    for (unsigned i = 0; i < write_buffer_num_; ++i) {
      auto buff = new Buffer();
      write_buff_vec_.emplace_back(buff);
    }
  }

  for (const auto& item : items_) {
    auto key = (item.second)->key();
    assert(key != nullptr);
    RETURN_NOT_OK(add_key(*key));
    unsigned bid = 4;  // Skip the buffers of the first 3 attributes

    for (unsigned aid = 3; aid < write_attribute_num_; ++aid) {
      auto value = (item.second)->value(write_attributes_[aid]);
      assert(value != nullptr);
      RETURN_NOT_OK(add_value(*value, bid, write_attribute_var_sizes_[aid]));
      bid += (write_attribute_var_sizes_[aid]) ? 2 : 1;
    }
    assert(bid == write_buff_vec_.size());
  }

  return Status::Ok();
}

Status KV::prepare_read_buffers() {
  if (read_buffers_ != nullptr)
    return Status::Ok();

  auto buffer_num = read_buff_vec_.size();
  read_buffers_ = new (std::nothrow) void*[buffer_num];
  read_buffer_sizes_ = new (std::nothrow) uint64_t[buffer_num];

  if (read_buffers_ == nullptr || read_buffer_sizes_ == nullptr)
    return LOG_STATUS(Status::KVError(
        "Cannot prepare read buffers; Memory allocation failed"));

  for (unsigned i = 0; i < buffer_num; ++i) {
    read_buffers_[i] = read_buff_vec_[i]->data();
    read_buffer_sizes_[i] = read_buff_vec_[i]->alloced_size();
  }

  return Status::Ok();
}

Status KV::prepare_write_buffers() {
  clear_write_buffers();

  RETURN_NOT_OK(populate_write_buffers());

  auto buffer_num = write_buff_vec_.size();
  write_buffers_ = new (std::nothrow) void*[buffer_num];
  write_buffer_sizes_ = new (std::nothrow) uint64_t[buffer_num];

  if (write_buffers_ == nullptr || write_buffer_sizes_ == nullptr)
    return LOG_STATUS(Status::KVError(
        "Cannot prepare read buffers; Memory allocation failed"));

  for (unsigned i = 0; i < buffer_num; ++i) {
    write_buffers_[i] = write_buff_vec_[i]->data();
    write_buffer_sizes_[i] = write_buff_vec_[i]->size();
  }

  return Status::Ok();
}

Status KV::prepare_query_attributes() {
  RETURN_NOT_OK(prepare_read_attributes());
  return prepare_write_attributes();
}

Status KV::prepare_read_attributes() {
  read_attribute_num_ = (unsigned)attributes_.size();
  read_attributes_ = new (std::nothrow) char*[read_attribute_num_];
  if (read_attributes_ == nullptr)
    return LOG_STATUS(Status::KVItemError(
        "Cannot prepare read attributes; Memory allocation failed"));
  read_attribute_var_sizes_.resize(read_attribute_num_);
  read_attribute_types_.resize(read_attribute_num_);

  unsigned int i = 0, aid;
  for (const auto& attr : attributes_) {
    read_attributes_[i] = new (std::nothrow) char[attr.size() + 1];
    strcpy(read_attributes_[i], attr.data());
    RETURN_NOT_OK(schema_->attribute_id(attr, &aid));
    read_attribute_types_[i] = schema_->attribute(aid)->type();
    read_attribute_var_sizes_[i++] = schema_->var_size(aid);
  }

  return Status::Ok();
}

Status KV::prepare_write_attributes() {
  // +3 because of the two special key attributes and coordinates
  write_attribute_num_ = (unsigned)attributes_.size() + 3;
  write_attributes_ = new (std::nothrow) char*[write_attribute_num_];
  if (write_attributes_ == nullptr)
    return LOG_STATUS(Status::KVItemError(
        "Cannot prepare write attributes; Memory allocation failed"));
  write_attribute_var_sizes_.resize(write_attribute_num_);

  // Coords
  write_attributes_[0] = new (std::nothrow) char[strlen(constants::coords) + 1];
  if (write_attributes_[0] == nullptr)
    return LOG_STATUS(Status::KVItemError(
        "Cannot prepare write attributes; Memory allocation failed"));
  strcpy(write_attributes_[0], constants::coords);
  write_attribute_var_sizes_[0] = false;

  // Key
  write_attributes_[1] =
      new (std::nothrow) char[strlen(constants::key_attr_name) + 1];
  if (write_attributes_[1] == nullptr)
    return LOG_STATUS(Status::KVItemError(
        "Cannot prepare write attributes; Memory allocation failed"));
  strcpy(write_attributes_[1], constants::key_attr_name);
  write_attribute_var_sizes_[1] = true;

  // Key type
  write_attributes_[2] =
      new (std::nothrow) char[strlen(constants::key_type_attr_name) + 1];
  if (write_attributes_[2] == nullptr)
    return LOG_STATUS(Status::KVItemError(
        "Cannot prepare write attributes; Memory allocation failed"));
  strcpy(write_attributes_[2], constants::key_type_attr_name);
  write_attribute_var_sizes_[2] = false;

  // User attributes
  unsigned int i = 3, aid;
  for (const auto& attr : attributes_) {
    write_attributes_[i] = new (std::nothrow) char[attr.size() + 1];
    strcpy(write_attributes_[i], attr.data());
    RETURN_NOT_OK(schema_->attribute_id(attr, &aid));
    write_attribute_var_sizes_[i++] = schema_->var_size(aid);
  }

  return Status::Ok();
}

Status KV::read_item(const KVItem::Hash& hash, bool* found) {
  RETURN_NOT_OK(prepare_read_buffers());

  // Prepare subarray
  uint64_t subarray[4];
  subarray[0] = hash.first;
  subarray[1] = hash.first;
  subarray[2] = hash.second;
  subarray[3] = hash.second;

  // Compute max buffer sizes
  RETURN_NOT_OK(storage_manager_->array_compute_max_read_buffer_sizes(
      kv_uri_.c_str(),
      subarray,
      (const char**)read_attributes_,
      read_attribute_num_,
      read_buffer_sizes_));

  // Potentially reallocate read buffers
  RETURN_NOT_OK(realloc_read_buffers());

  // Submit query
  RETURN_NOT_OK(submit_read_query(subarray));

  // Check if item exists
  *found = true;
  for (unsigned i = 0; i < read_buffer_num_; ++i) {
    if (read_buffer_sizes_[i] == 0) {
      *found = false;
      break;
    }
  }

  return Status::Ok();
}

Status KV::realloc_read_buffers() {
  for (unsigned i = 0; i < read_buffer_num_; ++i) {
    if (read_buffer_sizes_[i] > read_buff_vec_[i]->alloced_size()) {
      RETURN_NOT_OK(read_buff_vec_[i]->realloc(read_buffer_sizes_[i]));
      read_buffers_[i] = read_buff_vec_[i]->data();
      read_buffer_sizes_[i] = read_buff_vec_[i]->alloced_size();
    }
  }

  return Status::Ok();
}

Status KV::submit_read_query(const uint64_t* subarray) {
  // Create and send query
  auto query = (Query*)nullptr;
  RETURN_NOT_OK(
      storage_manager_->query_init(&query, kv_uri_.c_str(), QueryType::READ));
  RETURN_NOT_OK_ELSE(
      query->set_buffers(
          (const char**)read_attributes_,
          read_attribute_num_,
          read_buffers_,
          read_buffer_sizes_),
      delete query);

  RETURN_NOT_OK_ELSE(query->set_subarray(subarray), delete query);
  RETURN_NOT_OK_ELSE(storage_manager_->query_submit(query), delete query);
  RETURN_NOT_OK_ELSE(storage_manager_->query_finalize(query), delete query);

  delete query;

  return Status::Ok();
}

Status KV::submit_write_query() {
  auto query = (Query*)nullptr;
  RETURN_NOT_OK(
      storage_manager_->query_init(&query, kv_uri_.c_str(), QueryType::WRITE));
  RETURN_NOT_OK_ELSE(
      query->set_buffers(
          (const char**)write_attributes_,
          write_attribute_num_,
          write_buffers_,
          write_buffer_sizes_),
      delete query);
  RETURN_NOT_OK_ELSE(storage_manager_->query_submit(query), delete query);
  RETURN_NOT_OK_ELSE(storage_manager_->query_finalize(query), delete query);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
