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

KV::KV(const URI& kv_uri, StorageManager* storage_manager)
    : kv_uri_(kv_uri)
    , storage_manager_(storage_manager) {
  array_ = new Array(kv_uri, storage_manager);
}

KV::~KV() {
  clear();
  delete array_;
}

/* ********************************* */
/*                API                */
/* ********************************* */

uint64_t KV::capacity() const {
  if (!is_open())
    return 0;
  auto schema = array_->array_schema();
  return (schema != nullptr) ? schema->capacity() : 0;
}

Status KV::is_dirty(bool* dirty) const {
  QueryType query_type;
  RETURN_NOT_OK(array_->get_query_type(&query_type));
  if (query_type != QueryType::WRITE)
    return LOG_STATUS(Status::KVError(
        "Cannot check if dirty; Key-value store was not opened in write mode"));

  *dirty = !items_.empty();

  return Status::Ok();
}

Status KV::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  if (is_open())
    return LOG_STATUS(Status::KVError(
        "Cannot open key-value store; Key-value store already open"));

  RETURN_NOT_OK(
      array_->open(query_type, encryption_type, encryption_key, key_length));

  prepare_attributes_and_read_buffer_sizes();

  return Status::Ok();
}

Status KV::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    uint64_t timestamp) {
  if (is_open())
    return LOG_STATUS(
        Status::KVError("Cannot open key-value store at timestamp; Key-value "
                        "store already open"));

  if (query_type != QueryType::READ)
    return LOG_STATUS(
        Status::KVError("Cannot open key-value store at timestamp; This is "
                        "applicable only to reads"));

  RETURN_NOT_OK(array_->open(
      query_type, timestamp, encryption_type, encryption_key, key_length));

  prepare_attributes_and_read_buffer_sizes();

  return Status::Ok();
}

Status KV::close() {
  if (!is_open())
    return Status::Ok();

  std::unique_lock<std::mutex> lck(mtx_);
  RETURN_NOT_OK(array_->close());
  clear();

  return Status::Ok();
}

bool KV::is_open() const {
  return array_ != nullptr && array_->is_open();
}

Status KV::add_item(const KVItem* kv_item) {
  // Take the lock.
  std::unique_lock<std::mutex> lck(mtx_);

  QueryType query_type;
  RETURN_NOT_OK(array_->get_query_type(&query_type));
  if (query_type != QueryType::WRITE)
    return LOG_STATUS(Status::KVError(
        "Cannot add item; Key-value store was not opened in write mode"));

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

  QueryType query_type;
  RETURN_NOT_OK(array_->get_query_type(&query_type));
  if (query_type != QueryType::READ)
    return LOG_STATUS(Status::KVError(
        "Cannot get item; Key-value store was not opened in read mode"));

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

  assert(array_->is_open());
  auto schema = array_->array_schema();

  // Set values
  const void* value = nullptr;
  uint64_t value_size = 0;
  for (const auto& attr : attributes_) {
    auto var = schema->var_size(attr);
    value = var ? read_buffers_[attr].second : read_buffers_[attr].first;
    value_size =
        var ? read_buffer_sizes_[attr].second : read_buffer_sizes_[attr].first;
    st = (*kv_item)->set_value(attr, value, schema->type(attr), value_size);
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
  for (const auto& attr : attributes_) {
    // Set key type and key
    if (attr == constants::key_attr_name) {
      auto key_and_type = static_cast<char*>(read_buffers_[attr].second);
      key_type = static_cast<Datatype>(*key_and_type);
      key = key_and_type + sizeof(char);
      key_size = read_buffer_sizes_[attr].second - sizeof(char);
      key_found = true;
      continue;
    }

    assert(array_->is_open());
    auto schema = array_->array_schema();

    // Set values
    auto var = schema->var_size(attr);
    value = var ? read_buffers_[attr].second : read_buffers_[attr].first;
    value_size =
        var ? read_buffer_sizes_[attr].second : read_buffer_sizes_[attr].first;
    st = (*kv_item)->set_value(attr, value, schema->type(attr), value_size);
    if (!st.ok()) {
      delete *kv_item;
      *kv_item = nullptr;
      return st;
    }
  }

  // Set key
  assert(key_found);
  if (key_found) {
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

  QueryType query_type;
  RETURN_NOT_OK(array_->get_query_type(&query_type));
  if (query_type != QueryType::READ)
    return LOG_STATUS(Status::KVError(
        "Cannot check key; Key-value store was not opened in read mode"));

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

  QueryType query_type;
  RETURN_NOT_OK(array_->get_query_type(&query_type));
  if (query_type != QueryType::WRITE)
    return LOG_STATUS(
        Status::KVError("Cannot flush key-value store; Key-value store was not "
                        "opened in write mode"));

  // No items to flush
  if (items_.empty())
    return Status::Ok();

  clear_write_buffers();
  RETURN_NOT_OK(populate_write_buffers());
  RETURN_NOT_OK(submit_write_query());

  clear_items();

  return Status::Ok();
}

Status KV::get_query_type(QueryType* query_type) const {
  if (array_ == nullptr || !array_->is_open())
    return LOG_STATUS(
        Status::KVError("Cannot get query type; Key-value store is not open"));

  QueryType type;
  RETURN_NOT_OK(array_->get_query_type(&type));
  *query_type = type;

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

Array* KV::array() const {
  return array_;
}

Status KV::reopen() {
  std::unique_lock<std::mutex> lck(mtx_);

  QueryType query_type;
  RETURN_NOT_OK(array_->get_query_type(&query_type));
  if (query_type != QueryType::READ)
    return LOG_STATUS(
        Status::KVError("Cannot reopen key-value store; Key-value store was "
                        "not opened in read mode"));

  if (!is_open())
    return LOG_STATUS(Status::KVError(
        "Cannot reopen key-value store; Key-value store is not open"));

  return array_->reopen();
}

Status KV::reopen(uint64_t timestamp) {
  std::unique_lock<std::mutex> lck(mtx_);

  QueryType query_type;
  RETURN_NOT_OK(array_->get_query_type(&query_type));
  if (query_type != QueryType::READ)
    return LOG_STATUS(
        Status::KVError("Cannot reopen key-value store; Key-value store was "
                        "not opened in read mode"));

  if (!is_open())
    return LOG_STATUS(Status::KVError(
        "Cannot reopen key-value store; Key-value store is not open"));

  return array_->reopen(timestamp);
}

uint64_t KV::timestamp() const {
  return array_->timestamp();
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

  RETURN_NOT_OK(buff_coords.write(&(key.hash_.first), sizeof(key.hash_.first)));
  RETURN_NOT_OK(
      buff_coords.write(&(key.hash_.second), sizeof(key.hash_.second)));
  uint64_t offset = buff_keys.size();
  RETURN_NOT_OK(buff_key_offsets.write(&offset, sizeof(offset)));
  auto key_type_c = static_cast<char>(key.key_type_);
  RETURN_NOT_OK(buff_keys.write(&key_type_c, sizeof(key_type_c)));
  RETURN_NOT_OK(buff_keys.write(key.key_, key.key_size_));

  return Status::Ok();
}

Status KV::add_value(const std::string& attribute, const KVItem::Value& value) {
  assert(array_->is_open());
  auto schema = array_->array_schema();

  if (schema->var_size(attribute)) {
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

void KV::clear() {
  attributes_.clear();
  attribute_types_.clear();

  clear_items();
  clear_read_buffers();
  clear_write_buffers();
}

Status KV::populate_write_buffers() {
  assert(array_->is_open());
  auto schema = array_->array_schema();

  // Reset buffers
  for (const auto& buff_it : write_buffers_) {
    buff_it.second.first->reset_size();
    if (buff_it.second.second != nullptr)
      buff_it.second.second->reset_size();
  }

  // Create buffers if this is the first time to populate the write buffers
  if (write_buffers_.empty()) {
    for (const auto& attr : attributes_) {
      auto var = schema->var_size(attr);
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
      if (attr == constants::coords || attr == constants::key_attr_name)
        continue;
      auto value = (item.second)->value(attr);
      assert(value != nullptr);
      RETURN_NOT_OK(add_value(attr, *value));
    }
  }

  return Status::Ok();
}

void KV::prepare_attributes_and_read_buffer_sizes() {
  // Load all attributes
  auto schema = array_->array_schema();
  auto schema_attributes = schema->attributes();
  attributes_.push_back(constants::coords);
  for (const auto& attr : schema_attributes)
    attributes_.push_back(attr->name());

  // Prepare attribute types and read buffer sizes
  for (const auto& attr : attributes_) {
    attribute_types_.push_back(schema->type(attr));
    read_buffer_sizes_[attr] = std::pair<uint64_t, uint64_t>(0, 0);
  }
}

Status KV::read_item(const KVItem::Hash& hash, bool* found) {
  // Prepare subarray
  uint64_t subarray[4];
  subarray[0] = hash.first;
  subarray[1] = hash.first;
  subarray[2] = hash.second;
  subarray[3] = hash.second;

  // Compute max buffer sizes
  RETURN_NOT_OK(array_->compute_max_buffer_sizes(
      subarray, attributes_, &read_buffer_sizes_));

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
  assert(array_->is_open());
  auto schema = array_->array_schema();

  for (const auto& attr : attributes_) {
    auto alloced_size_1 = read_buffer_alloced_sizes_[attr].first;
    auto alloced_size_2 = read_buffer_alloced_sizes_[attr].second;
    auto var = schema->var_size(attr);
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
  assert(array_->is_open());
  auto schema = array_->array_schema();

  for (const auto& attr : attributes_) {
    if (!schema->var_size(attr)) {
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
  assert(array_->is_open());
  auto schema = array_->array_schema();

  for (const auto& attr : attributes_) {
    if (!schema->var_size(attr)) {
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
  auto query = new Query(storage_manager_, array_);
  RETURN_NOT_OK_ELSE(set_read_query_buffers(query), delete query);
  RETURN_NOT_OK_ELSE(query->set_subarray(subarray), delete query);
  RETURN_NOT_OK_ELSE(query->submit(), delete query);
  delete query;

  return Status::Ok();
}

Status KV::submit_write_query() {
  auto query = new Query(storage_manager_, array_);
  RETURN_NOT_OK_ELSE(set_write_query_buffers(query), delete query);
  RETURN_NOT_OK_ELSE(query->submit(), delete query);
  delete query;

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
