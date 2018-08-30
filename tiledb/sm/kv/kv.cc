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
#include <iostream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

KV::KV(StorageManager* storage_manager)
    : storage_manager_(storage_manager) {
  schema_ = nullptr;
  max_items_ = constants::kv_max_items;
  open_array_for_reads_ = nullptr;
  open_array_for_writes_ = nullptr;
  snapshot_ = 0;
}

KV::~KV() {
  clear();
}

/* ********************************* */
/*                API                */
/* ********************************* */

uint64_t KV::capacity() const {
  return (schema_ != nullptr) ? schema_->capacity() : 0;
}

bool KV::dirty() const {
  return !items_.empty();
}

Status KV::init(const URI& kv_uri, const std::vector<std::string>& attributes) {
  kv_uri_ = kv_uri;

  // Open the array for reads
  if (open_array_for_reads_ == nullptr)
    RETURN_NOT_OK(storage_manager_->array_open(
        kv_uri_, QueryType::READ, &open_array_for_reads_, &snapshot_));

  // Open the array for writes
  uint64_t snapshot;  // will be ignored for writes
  if (open_array_for_writes_ == nullptr)
    RETURN_NOT_OK(storage_manager_->array_open(
        kv_uri_, QueryType::WRITE, &open_array_for_writes_, &snapshot));

  schema_ = open_array_for_reads_->array_schema();

  // Get attributes
  if (attributes.empty()) {  // Load all attributes
    write_good_ = true;
    auto schema_attributes = schema_->attributes();
    attributes_.push_back(constants::coords);
    for (const auto& attr : schema_attributes)
      attributes_.push_back(attr->name());
  } else {  // Load input attributes
    write_good_ = false;
    attributes_.push_back(constants::coords);
    attributes_.push_back(constants::key_attr_name);
    attributes_.push_back(constants::key_type_attr_name);
    for (const auto& attr : attributes) {
      if (attr == constants::coords || attr == constants::key_type_attr_name ||
          attr == constants::key_attr_name)
        continue;
      attributes_.push_back(attr);
    }
  }

  // Prepare attribute types and read buffer sizes
  for (const auto& attr : attributes_) {
    attribute_types_.push_back(schema_->type(attr));
    read_buffer_sizes_[attr] = std::pair<uint64_t, uint64_t>(0, 0);
  }

  return Status::Ok();
}

void KV::clear() {
  schema_ = nullptr;
  kv_uri_ = URI("");
  attributes_.clear();
  attribute_types_.clear();

  clear_items();
  clear_read_buffers();
  clear_write_buffers();

  open_array_for_reads_ = nullptr;
  open_array_for_writes_ = nullptr;
}

Status KV::finalize() {
  RETURN_NOT_OK(flush());
  std::unique_lock<std::mutex> lck(mtx_);
  RETURN_NOT_OK(storage_manager_->array_close(kv_uri_, QueryType::READ));
  RETURN_NOT_OK(storage_manager_->array_close(kv_uri_, QueryType::WRITE));
  clear();

  return Status::Ok();
}

Status KV::add_item(const KVItem* kv_item) {
  // Check if we are good for writes
  if (!write_good_)
    return LOG_STATUS(
        Status::KVError("Cannot add item; Key-value store was not opened "
                        "properly for writes"));

  if (items_.size() >= max_items_)
    RETURN_NOT_OK(flush());

  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);

  RETURN_NOT_OK(kv_item->good(attributes_, attribute_types_));

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
  const void* value = nullptr;
  uint64_t value_size = 0;
  for (const auto& attr : attributes_) {
    auto var = schema_->var_size(attr);
    value = var ? read_buffers_[attr].second : read_buffers_[attr].first;
    value_size =
        var ? read_buffer_sizes_[attr].second : read_buffer_sizes_[attr].first;
    st = (*kv_item)->set_value(attr, value, schema_->type(attr), value_size);
    if (!st.ok()) {
      delete *kv_item;
      *kv_item = nullptr;
      return st;
    }
  }

  return Status::Ok();
}

Status KV::get_item(const KVItem::Hash& hash, KVItem** kv_item) {
  // Take the lock
  std::unique_lock<std::mutex> lck(mtx_);

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
      key = read_buffers_[attr].second;
      key_size = read_buffer_sizes_[attr].second;
      key_found = true;
      continue;
    }

    // Set key type
    if (attr == constants::key_type_attr_name) {
      key_type = static_cast<Datatype>(((char*)read_buffers_[attr].first)[0]);
      key_type_found = true;
      continue;
    }

    // Set values
    auto var = schema_->var_size(attr);
    value = var ? read_buffers_[attr].second : read_buffers_[attr].first;
    value_size =
        var ? read_buffer_sizes_[attr].second : read_buffer_sizes_[attr].first;
    st = (*kv_item)->set_value(attr, value, schema_->type(attr), value_size);
    if (!st.ok()) {
      delete *kv_item;
      *kv_item = nullptr;
      return st;
    }
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

Status KV::has_key(
    const void* key, Datatype key_type, uint64_t key_size, bool* has_key) {
  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);

  // Create key-value item
  KVItem kv_item;

  // Set key
  RETURN_NOT_OK(kv_item.set_key(key, key_type, key_size));

  // If the item is buffered, copy and return
  auto it = items_.find(kv_item.key()->hash_);
  if (it != items_.end()) {
    *has_key = true;
    return Status::Ok();
  }

  // Query
  RETURN_NOT_OK(read_item(kv_item.hash(), has_key));

  return Status::Ok();
}

Status KV::flush() {
  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);

  // No items to flush
  if (items_.empty())
    return Status::Ok();

  clear_write_buffers();
  RETURN_NOT_OK(populate_write_buffers());
  RETURN_NOT_OK(submit_write_query());

  clear_items();

  RETURN_NOT_OK(
      storage_manager_->array_reopen(open_array_for_reads_, &snapshot_));

  return Status::Ok();
}

void KV::clear_items() {
  for (auto& i : items_)
    delete i.second;
  items_.clear();
}

void KV::clear_read_buffers() {
  for (auto b_it : read_buffers_) {
    std::free(b_it.second.first);
    std::free(b_it.second.second);
  }
  read_buffers_.clear();
  read_buffer_sizes_.clear();
  read_buffer_alloced_sizes_.clear();
}

void KV::clear_write_buffers() {
  for (auto b_it : write_buffers_) {
    delete b_it.second.first;
    delete b_it.second.second;
  }
  write_buffers_.clear();
}

OpenArray* KV::open_array_for_reads() const {
  return open_array_for_reads_;
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

uint64_t KV::snapshot() const {
  return snapshot_;
}

Status KV::reopen() {
  std::unique_lock<std::mutex> lck(mtx_);
  return storage_manager_->array_reopen(open_array_for_reads_, &snapshot_);
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

Status KV::add_key(const KVItem::Key& key) {
  assert(key.key_ != nullptr && key.key_size_ > 0);

  // Aliases
  auto& buff_coords = *write_buffers_[constants::coords].first;
  auto& buff_key_offsets = *write_buffers_[constants::key_attr_name].first;
  auto& buff_keys = *write_buffers_[constants::key_attr_name].second;
  auto& buff_key_types = *write_buffers_[constants::key_type_attr_name].first;

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

Status KV::add_value(const std::string& attribute, const KVItem::Value& value) {
  if (schema_->var_size(attribute)) {
    uint64_t offset = write_buffers_[attribute].second->size();
    RETURN_NOT_OK(
        write_buffers_[attribute].first->write(&offset, sizeof(offset)));
    RETURN_NOT_OK(write_buffers_[attribute].second->write(
        value.value_, value.value_size_));
  } else {
    RETURN_NOT_OK(write_buffers_[attribute].first->write(
        value.value_, value.value_size_));
  }

  return Status::Ok();
}

Status KV::populate_write_buffers() {
  // Reset buffers
  for (const auto& buff_it : write_buffers_) {
    buff_it.second.first->reset_size();
    if (buff_it.second.second != nullptr)
      buff_it.second.second->reset_size();
  }

  // Create buffers if this is the first time to populate the write buffers
  if (write_buffers_.empty()) {
    for (const auto& attr : attributes_) {
      auto var = schema_->var_size(attr);
      auto buff_1 = new Buffer();
      auto buff_2 = var ? new Buffer() : (Buffer*)nullptr;
      write_buffers_[attr] = std::pair<Buffer*, Buffer*>(buff_1, buff_2);
    }
  }

  for (const auto& item : items_) {
    auto key = (item.second)->key();
    assert(key != nullptr);
    RETURN_NOT_OK(add_key(*key));
    for (const auto& attr : attributes_) {
      // Skip the special attributes
      if (attr == constants::coords || attr == constants::key_type_attr_name ||
          attr == constants::key_attr_name)
        continue;
      auto value = (item.second)->value(attr);
      assert(value != nullptr);
      RETURN_NOT_OK(add_value(attr, *value));
    }
  }

  return Status::Ok();
}

Status KV::read_item(const KVItem::Hash& hash, bool* found) {
  // Prepare subarray
  uint64_t subarray[4];
  subarray[0] = hash.first;
  subarray[1] = hash.first;
  subarray[2] = hash.second;
  subarray[3] = hash.second;

  // Compute max buffer sizes
  RETURN_NOT_OK(storage_manager_->array_compute_max_buffer_sizes(
      open_array_for_reads_,
      snapshot_,
      subarray,
      attributes_,
      &read_buffer_sizes_));

  // If the max buffer sizes are 0, then the item is not found
  if (read_buffer_sizes_.begin()->second.first == 0) {
    *found = false;
    return Status::Ok();
  }

  // Potentially reallocate read buffers
  RETURN_NOT_OK(realloc_read_buffers());

  // Submit query
  RETURN_NOT_OK(submit_read_query(subarray));

  // Check if item exists
  *found = true;
  for (const auto& s_it : read_buffer_sizes_) {
    if (s_it.second.first == 0) {
      *found = false;
      break;
    }
  }

  return Status::Ok();
}

Status KV::realloc_read_buffers() {
  for (const auto& attr : attributes_) {
    auto alloced_size_1 = read_buffer_alloced_sizes_[attr].first;
    auto alloced_size_2 = read_buffer_alloced_sizes_[attr].second;
    auto var = schema_->var_size(attr);
    auto new_size_1 = read_buffer_sizes_[attr].first;
    auto new_size_2 = read_buffer_sizes_[attr].second;

    // First time the buffers are alloc'ed
    if (alloced_size_1 == 0) {
      auto buff_1 = std::malloc(new_size_1);
      if (buff_1 == nullptr)
        return LOG_STATUS(Status::KVError(
            "Cannot reallocate read buffers; Memory allocation failed"));

      auto buff_2 = (void*)nullptr;
      if (var) {
        buff_2 = std::malloc(new_size_2);
        if (buff_2 == nullptr)
          return LOG_STATUS(Status::KVError(
              "Cannot reallocate read buffers; Memory allocation failed"));
      }

      read_buffers_[attr] = std::pair<void*, void*>(buff_1, buff_2);
      read_buffer_alloced_sizes_[attr] =
          std::pair<uint64_t, uint64_t>(new_size_1, new_size_2);

      continue;
    }

    if (new_size_1 > alloced_size_1) {
      std::free(read_buffers_[attr].first);
      read_buffers_[attr].first = std::malloc(new_size_1);
      if (read_buffers_[attr].first == nullptr)
        return LOG_STATUS(Status::KVError(
            "Cannot reallocate read buffers; Memory allocation failed"));
      read_buffer_alloced_sizes_[attr].first = new_size_1;
    }

    if (var && new_size_2 > alloced_size_2) {
      std::free(read_buffers_[attr].second);
      read_buffers_[attr].second = std::malloc(new_size_2);
      if (read_buffers_[attr].second == nullptr)
        return LOG_STATUS(Status::KVError(
            "Cannot reallocate read buffers; Memory allocation failed"));
      read_buffer_alloced_sizes_[attr].second = new_size_2;
    }
  }

  return Status::Ok();
}

Status KV::set_read_query_buffers(Query* query) {
  for (const auto& attr : attributes_) {
    if (!schema_->var_size(attr)) {
      RETURN_NOT_OK(query->set_buffer(
          attr, read_buffers_[attr].first, &read_buffer_sizes_[attr].first));
    } else {
      RETURN_NOT_OK(query->set_buffer(
          attr,
          (uint64_t*)read_buffers_[attr].first,
          &read_buffer_sizes_[attr].first,
          read_buffers_[attr].second,
          &read_buffer_sizes_[attr].second));
    }
  }

  return Status::Ok();
}

Status KV::set_write_query_buffers(Query* query) {
  for (const auto& attr : attributes_) {
    if (!schema_->var_size(attr)) {
      write_buffer_sizes_[attr] =
          std::pair<uint64_t, uint64_t>(write_buffers_[attr].first->size(), 0);
      RETURN_NOT_OK(query->set_buffer(
          attr,
          write_buffers_[attr].first->data(),
          &write_buffer_sizes_[attr].first));
    } else {
      auto size_off = write_buffers_[attr].first->size();
      auto size_val = write_buffers_[attr].second->size();
      write_buffer_sizes_[attr] =
          std::pair<uint64_t, uint64_t>(size_off, size_val);
      RETURN_NOT_OK(query->set_buffer(
          attr,
          (uint64_t*)write_buffers_[attr].first->data(),
          &write_buffer_sizes_[attr].first,
          write_buffers_[attr].second->data(),
          &write_buffer_sizes_[attr].second));
    }
  }

  return Status::Ok();
}

Status KV::submit_read_query(const uint64_t* subarray) {
  // Create and send query
  auto query = (Query*)nullptr;
  RETURN_NOT_OK(
      storage_manager_->query_create(&query, open_array_for_reads_, snapshot_));
  RETURN_NOT_OK_ELSE(set_read_query_buffers(query), delete query);
  RETURN_NOT_OK_ELSE(query->set_subarray(subarray), delete query);
  RETURN_NOT_OK_ELSE(storage_manager_->query_submit(query), delete query);
  delete query;

  return Status::Ok();
}

Status KV::submit_write_query() {
  auto query = (Query*)nullptr;
  RETURN_NOT_OK(storage_manager_->query_create(
      &query, open_array_for_writes_, snapshot_));
  RETURN_NOT_OK_ELSE(set_write_query_buffers(query), delete query);
  RETURN_NOT_OK_ELSE(storage_manager_->query_submit(query), delete query);
  delete query;

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
