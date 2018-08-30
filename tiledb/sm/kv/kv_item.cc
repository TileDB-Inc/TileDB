/**
 * @file   kv_item.cc
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
 * This file implements class KVItem.
 */

#include "tiledb/sm/kv/kv_item.h"
#include "md5/md5.h"
#include "tiledb/sm/misc/logger.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

KVItem::KVItem() {
  key_.key_ = nullptr;
}

KVItem::~KVItem() {
  clear();
}

/* ********************************* */
/*             OPERATORS             */
/* ********************************* */

KVItem& KVItem::operator=(const KVItem& kv_item) {
  clear();
  copy_key(kv_item.key_);
  for (const auto& v : kv_item.values_)
    copy_value(*(v.second));
  copy_hash(kv_item.hash());

  return *this;
}

/* ********************************* */
/*                API                */
/* ********************************* */

void KVItem::clear() {
  std::free(key_.key_);
  key_.key_ = nullptr;

  for (auto& v : values_) {
    std::free((v.second)->value_);
    delete v.second;
  }
  values_.clear();
}

Status KVItem::good(
    const std::vector<std::string>& attributes,
    const std::vector<Datatype>& types) const {
  assert(attributes.size() == types.size());

  if (key_.key_ == nullptr)
    return LOG_STATUS(Status::KVItemError("Invalid item; The key is null"));

  auto attribute_num = attributes.size();
  for (unsigned i = 0; i < attribute_num; ++i) {
    // Skip the special attributes
    if (attributes[i] == constants::coords ||
        attributes[i] == constants::key_attr_name ||
        attributes[i] == constants::key_type_attr_name)
      continue;

    auto it = values_.find(attributes[i]);
    if (it == values_.end())
      return LOG_STATUS(Status::KVItemError(
          std::string("Invalid item; ") + "Missing value on attribute " +
          attributes[i]));
    if (it->second->value_ == nullptr)
      return LOG_STATUS(Status::KVItemError(
          std::string("Invalid item; Value on attribute ") + attributes[i] +
          " is null"));
    if (it->second->value_type_ != types[i])
      return LOG_STATUS(Status::KVItemError(
          std::string("Invalid item; Type mismatch on attribute ") +
          attributes[i] + ", " + datatype_str(it->second->value_type_) +
          " != " + datatype_str(types[i])));
  }

  return Status::Ok();
}

const KVItem::Hash& KVItem::hash() const {
  return key_.hash_;
}

const KVItem::Key* KVItem::key() const {
  return &key_;
}

const KVItem::Value* KVItem::value(const std::string& attribute) const {
  auto it = values_.find(attribute);
  if (it == values_.end())
    return nullptr;

  return it->second;
}

Status KVItem::set_key(const void* key, Datatype key_type, uint64_t key_size) {
  auto hash = compute_hash(key, key_type, key_size);
  return set_key(key, key_type, key_size, hash);
}

Status KVItem::set_key(
    const void* key, Datatype key_type, uint64_t key_size, const Hash& hash) {
  if (key == nullptr || key_size == 0)
    return LOG_STATUS(
        Status::KVItemError("Cannot add key; Key cannot be empty"));

  std::free(key_.key_);
  key_.key_ = std::malloc(key_size);
  if (key_.key_ == nullptr)
    return LOG_STATUS(
        Status::KVItemError("Cannot set key; Failed to allocate memory"));
  std::memcpy(key_.key_, key, key_size);
  key_.key_type_ = key_type;
  key_.key_size_ = key_size;
  key_.hash_.first = hash.first;
  key_.hash_.second = hash.second;

  return Status::Ok();
}

Status KVItem::set_value(
    const std::string& attribute,
    const void* value,
    Datatype value_type,
    uint64_t value_size) {
  // Sanity checks
  if (value == nullptr || value_size == 0)
    return LOG_STATUS(
        Status::KVItemError("Cannot add value; Value cannot be empty"));
  if (attribute.empty())
    return LOG_STATUS(Status::KVItemError(
        "Cannot add value; Attribute name cannot be empty"));
  if (value_size % datatype_size(value_type))
    return LOG_STATUS(Status::KVItemError(
        "Cannot add value; Value size is not a multiple of the datatype size"));

  // Delete previous value buffer
  auto it = values_.find(attribute);
  if (it != values_.end()) {
    std::free(it->second->value_);
    delete it->second;
    values_.erase(it);
  }

  // Set new value
  auto value_obj = new Value();
  value_obj->attribute_ = attribute;
  value_obj->value_ = std::malloc(value_size);
  if (value_obj->value_ == nullptr) {
    delete value_obj;
    return LOG_STATUS(
        Status::KVItemError("Cannot set value; Failed to allocate memory"));
  }
  std::memcpy(value_obj->value_, value, value_size);
  value_obj->value_size_ = value_size;
  value_obj->value_type_ = value_type;
  values_[attribute] = value_obj;

  return Status::Ok();
}

/* ********************************* */
/*         STATIC FUNCTIONS          */
/* ********************************* */

KVItem::Hash KVItem::compute_hash(
    const void* key, Datatype key_type, uint64_t key_size) {
  // Case of empty key
  if (key == nullptr)
    return Hash();

  Hash hash;
  md5::MD5_CTX md5_ctx;
  uint64_t coord_size = sizeof(md5_ctx.digest) / 2;
  assert(coord_size == sizeof(uint64_t));
  auto key_type_c = static_cast<char>(key_type);
  md5::MD5Init(&md5_ctx);
  md5::MD5Update(&md5_ctx, (unsigned char*)&key_type_c, sizeof(char));
  md5::MD5Update(&md5_ctx, (unsigned char*)&key_size, sizeof(uint64_t));
  md5::MD5Update(&md5_ctx, (unsigned char*)key, (unsigned int)key_size);
  md5::MD5Final(&md5_ctx);
  std::memcpy(&(hash.first), md5_ctx.digest, coord_size);
  std::memcpy(&(hash.second), md5_ctx.digest + coord_size, coord_size);

  return hash;
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

void KVItem::copy_hash(const Hash& hash) {
  key_.hash_.first = hash.first;
  key_.hash_.second = hash.second;
}

void KVItem::copy_key(const Key& key) {
  if (key.key_ != nullptr && key.key_size_ != 0) {
    key_.key_ = std::malloc(key.key_size_);
    if (key_.key_ != nullptr)
      std::memcpy(key_.key_, key.key_, key.key_size_);
    else
      key_.key_ = nullptr;
  } else {
    key_.key_ = nullptr;
  }
  key_.key_size_ = key.key_size_;
  key_.key_type_ = key.key_type_;
}

void KVItem::copy_value(const Value& value) {
  auto new_value = new Value();
  new_value->attribute_ = value.attribute_;
  if (value.value_ != nullptr && value.value_size_ != 0) {
    new_value->value_ = std::malloc(value.value_size_);
    if (new_value->value_ != nullptr)
      std::memcpy(new_value->value_, value.value_, value.value_size_);
    else
      new_value->value_ = nullptr;
  } else {
    new_value->value_ = nullptr;
  }
  new_value->value_size_ = value.value_size_;
  new_value->value_type_ = value.value_type_;
  values_[new_value->attribute_] = new_value;
}

}  // namespace sm
}  // namespace tiledb
