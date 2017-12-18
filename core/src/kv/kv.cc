/**
 * @file   kv.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "kv.h"
#include "logger.h"
#include "md5/md5.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

KV::KV(
    const std::vector<std::string>& attributes,
    const std::vector<tiledb::Datatype>& types,
    const std::vector<unsigned int>& nitems)
    : attributes_(attributes)
    , nitems_(nitems)
    , types_(types) {
  array_attributes_ = nullptr;
  array_attribute_num_ = 0;
  array_buffers_ = nullptr;
  array_buffer_sizes_ = nullptr;
  array_buffer_num_ = 0;
  buffer_alloc_size_ = constants::kv_buffer_size;
  key_num_ = 0;
  attribute_num_ = (unsigned int)attributes_.size();
  buff_value_offsets_.resize(attribute_num_);
  buff_values_.resize(attribute_num_);
  for (unsigned int i = 0; i < attribute_num_; ++i) {
    value_sizes_.emplace_back(
        (nitems[i] == constants::var_num) ?
            constants::var_size :
            nitems[i] * datatype_size(types[i]));
    value_num_.emplace_back(0);
  }
}

KV::~KV() {
  if (array_attributes_ != nullptr)
    std::free(array_attributes_);
  if (array_buffers_ != nullptr)
    std::free(array_buffers_);
  if (array_buffer_sizes_ != nullptr)
    std::free(array_buffer_sizes_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status KV::add_key(const void* key, Datatype key_type, uint64_t key_size) {
  if (key == nullptr || key_size == 0)
    return LOG_STATUS(Status::KVError("Cannot add key; Key cannot be empty"));

  uint64_t offset = buff_keys_.size();
  RETURN_NOT_OK(buff_key_offsets_.write(&offset, sizeof(offset)));
  RETURN_NOT_OK(buff_keys_.write(key, key_size));
  auto key_type_c = static_cast<char>(key_type);
  RETURN_NOT_OK(buff_key_types_.write(&key_type_c, sizeof(key_type_c)));

  ++key_num_;

  return Status::Ok();
}

Status KV::add_value(unsigned int attribute_idx, const void* value) {
  if (attribute_idx >= attribute_num_)
    return LOG_STATUS(Status::KVError(
        "Cannot add fixed-sized value; Invalid attribute index"));

  if (value == nullptr)
    return LOG_STATUS(
        Status::KVError("Cannot add fixed-sized value; Value cannot be empty"));

  if (nitems_[attribute_idx] == constants::var_num)
    return LOG_STATUS(Status::KVError(
        "Cannot add fixed-sized value; Attribute is variable-sized"));

  RETURN_NOT_OK(
      buff_values_[attribute_idx].write(value, value_sizes_[attribute_idx]));

  ++value_num_[attribute_idx];

  return Status::Ok();
}

Status KV::add_value(
    unsigned int attribute_idx, const void* value, uint64_t value_size) {
  if (attribute_idx >= attribute_num_)
    return LOG_STATUS(Status::KVError(
        "Cannot add variable-sized value; Invalid attribute index"));

  if (value == nullptr || value_size == 0)
    return LOG_STATUS(Status::KVError(
        "Cannot add variable-sized value; Value cannot be empty"));

  if (nitems_[attribute_idx] != constants::var_num)
    return LOG_STATUS(Status::KVError(
        "Cannot add variable-sized value; Attribute is fixed-sized"));

  uint64_t offset = buff_values_[attribute_idx].size();
  RETURN_NOT_OK(
      buff_value_offsets_[attribute_idx].write(&offset, sizeof(offset)));
  RETURN_NOT_OK(buff_values_[attribute_idx].write(value, value_size));

  ++value_num_[attribute_idx];

  return Status::Ok();
}

Status KV::get_array_attributes(
    const char*** attributes,
    unsigned int* attribute_num,
    bool get_coords,
    bool get_key) {
  // Trivial case when the array attributes have been already computed
  if (array_attributes_ != nullptr) {
    *attributes = array_attributes_;
    *attribute_num = array_attribute_num_;
    return Status::Ok();
  }

  // Compute array attributes and attribute number
  array_attribute_num_ = attribute_num_ + (int)get_coords + 2 * (int)get_key;
  array_attributes_ =
      (const char**)std::malloc(array_attribute_num_ * sizeof(const char*));
  for (unsigned int i = 0; i < attribute_num_; ++i)
    array_attributes_[i] = attributes_[i].c_str();
  if (get_key) {
    array_attributes_[attribute_num_] = constants::key_attr_name;
    array_attributes_[attribute_num_ + 1] = constants::key_type_attr_name;
  }
  if (get_coords)
    array_attributes_[attribute_num_ + 2 * (int)get_key] = constants::coords;

  // Set array attributes and attribute number
  *attributes = array_attributes_;
  *attribute_num = array_attribute_num_;

  return Status::Ok();
}

Status KV::get_array_buffers(void*** buffers, uint64_t** buffer_sizes) {
  // Check buffers in the case of writes
  bool has_coords = this->has_coords();
  if (has_coords) {
    for (unsigned int i = 0; i < attribute_num_; ++i) {
      if (value_num_[i] != key_num_)
        return LOG_STATUS(Status::KVError(
            "Cannot get array buffers; Number of keys/values mismatch"));
    }
  }

  // Make sure that the array attributes have been retrieved first
  if (array_attributes_ == nullptr)
    return LOG_STATUS(Status::KVError(
        "Cannot get array buffers; Array attributes have not been calculated"));

  // Check if keys are included
  bool has_keys = this->has_keys();

  // Calculate number of buffers (2 buffers for the key and 1 for the key type)
  unsigned int buffer_num = 3 * (unsigned int)has_keys;
  for (unsigned int i = 0; i < attribute_num_; ++i)
    buffer_num += 1 + (unsigned int)(nitems_[i] != constants::var_num);
  buffer_num += (unsigned int)has_coords;

  // Allocate array buffers
  array_buffers_ = (void**)malloc(buffer_num * sizeof(void*));
  if (array_buffers_ == nullptr)
    return LOG_STATUS(
        Status::KVError("Cannot get array buffers; Failed to allocate memory "
                        "for array buffers"));
  array_buffer_sizes_ = (uint64_t*)malloc(buffer_num * sizeof(uint64_t));
  if (array_buffer_sizes_ == nullptr)
    return LOG_STATUS(
        Status::KVError("Cannot get array buffers; Failed to allocate memory "
                        "for array buffer sizes"));

  // Allocate buffers in case of reads (no coordinates specified)
  if (!has_coords)
    alloc_buffers(has_keys);

  // Set array buffers
  unsigned int b = 0;
  array_buffer_idx_.resize(attribute_num_);
  for (unsigned int i = 0; i < attribute_num_; ++i) {
    array_buffer_idx_[i] = b;
    if (nitems_[i] == constants::var_num) {
      array_buffers_[b] = buff_value_offsets_[i].data();
      array_buffer_sizes_[b++] = (has_coords) ?
                                     buff_value_offsets_[i].size() :
                                     buff_value_offsets_[i].alloced_size();
    }
    array_buffers_[b] = buff_values_[i].data();
    array_buffer_sizes_[b++] =
        (has_coords) ? buff_values_[i].size() : buff_values_[i].alloced_size();
  }
  if (has_keys) {
    array_buffers_[b] = buff_key_offsets_.data();
    array_buffer_sizes_[b++] = (has_coords) ? buff_key_offsets_.size() :
                                              buff_key_offsets_.alloced_size();
    array_buffers_[b] = buff_keys_.data();
    array_buffer_sizes_[b++] =
        (has_coords) ? buff_keys_.size() : buff_keys_.alloced_size();
    array_buffers_[b] = buff_key_types_.data();
    array_buffer_sizes_[b++] =
        (has_coords) ? buff_key_types_.size() : buff_key_types_.alloced_size();
  }
  if (has_coords) {
    RETURN_NOT_OK(compute_coords());
    array_buffers_[b] = buff_coords_.data();
    array_buffer_sizes_[b++] = buff_coords_.size();
  }
  array_buffer_num_ = b;

  // Retrieve buffers
  *buffers = array_buffers_;
  *buffer_sizes = array_buffer_sizes_;

  return Status::Ok();
}

Status KV::get_key(
    uint64_t idx, void** key, Datatype* key_type, uint64_t* key_size) const {
  // Sanity check
  if (!has_keys())
    return LOG_STATUS(
        Status::KVError("Cannot get key; Keys were not retrieved"));

  uint64_t num = key_num();
  uint64_t buff_keys_size = (has_coords()) ?
                                buff_keys_.size() :
                                array_buffer_sizes_[array_buffer_num_ - 2];

  // Sanity check
  if (idx >= num)
    return LOG_STATUS(
        Status::KVError("Cannot get key; Key index out of bounds"));

  uint64_t key_offset = ((uint64_t*)buff_key_offsets_.data())[idx];
  *key_size = (idx == num - 1) ?
                  buff_keys_size - key_offset :
                  ((uint64_t*)buff_key_offsets_.data())[idx + 1] - key_offset;
  *key = buff_keys_.value_ptr(((uint64_t*)buff_key_offsets_.data())[idx]);
  char key_type_c = (((char*)(buff_key_types_.data()))[idx]);
  *key_type = static_cast<Datatype>(key_type_c);

  return Status::Ok();
}

Status KV::get_value(
    uint64_t obj_idx, unsigned int attr_idx, void** value) const {
  // Get number of values
  uint64_t num = 0;
  RETURN_NOT_OK(value_num(attr_idx, &num));

  // Sanity check
  if (obj_idx >= num)
    return LOG_STATUS(
        Status::KVError("Cannot get value; Value index out of bounds"));

  // Get value
  uint64_t offset = obj_idx * value_sizes_[attr_idx];
  *value = buff_values_[attr_idx].value_ptr(offset);

  return Status::Ok();
}

Status KV::get_value(
    uint64_t obj_idx,
    unsigned int attr_idx,
    void** value,
    uint64_t* value_size) const {
  // Get number of values and size of the value buffer
  uint64_t num = 0;
  RETURN_NOT_OK(value_num(attr_idx, &num));

  uint64_t buff_values_size =
      (has_coords()) ? buff_values_[attr_idx].size() :
                       array_buffer_sizes_[array_buffer_idx_[attr_idx] + 1];

  // Sanity check
  if (obj_idx >= num)
    return LOG_STATUS(
        Status::KVError("Cannot get value; Value index out of bounds"));

  uint64_t offset = ((uint64_t*)buff_value_offsets_[attr_idx].data())[obj_idx];
  *value = buff_values_[attr_idx].value_ptr(offset);
  *value_size =
      (obj_idx == num - 1) ?
          buff_values_size - offset :
          ((uint64_t*)buff_value_offsets_[attr_idx].data())[obj_idx + 1] -
              offset;

  return Status::Ok();
}

uint64_t KV::key_num() const {
  if (has_coords())
    return key_num_;

  if (!has_keys())
    return 0;

  uint64_t key_num = array_buffer_sizes_[array_buffer_num_ - 3] /
                     constants::cell_var_offset_size;
  uint64_t key_types_num =
      array_buffer_sizes_[array_buffer_num_ - 1] / sizeof(char);

  return std::min(key_num, key_types_num);
}

void KV::set_buffer_alloc_size(uint64_t nbytes) {
  buffer_alloc_size_ = nbytes;
}

Status KV::value_num(unsigned int attribute_idx, uint64_t* num) const {
  if (attribute_idx >= attribute_num_)
    return LOG_STATUS(Status::KVError(
        "Cannot get number of values; Invalid attribute index"));

  if (has_coords())
    *num = value_num_[attribute_idx];
  else if (array_buffers_ == nullptr)
    *num = 0;
  else
    *num = array_buffer_sizes_[array_buffer_idx_[attribute_idx]] /
           ((nitems_[attribute_idx] == constants::var_num) ?
                constants::cell_var_offset_size :
                value_sizes_[attribute_idx]);

  return Status::Ok();
}

/* ********************************* */
/*         STATIC FUNCTIONS          */
/* ********************************* */

void KV::compute_subarray(
    const void* key, Datatype key_type, uint64_t key_size, uint64_t* subarray) {
  // Compute an MD5 digest for the <key_type | key_size | key> tuple
  md5::MD5_CTX md5_ctx;
  uint64_t coord_size = sizeof(md5_ctx.digest) / 2;
  assert(coord_size == sizeof(uint64_t));
  auto key_type_c = static_cast<char>(key_type);
  md5::MD5Init(&md5_ctx);
  md5::MD5Update(&md5_ctx, (unsigned char*)&key_type_c, sizeof(char));
  md5::MD5Update(&md5_ctx, (unsigned char*)&key_size, sizeof(uint64_t));
  md5::MD5Update(&md5_ctx, (unsigned char*)key, (unsigned int)key_size);
  md5::MD5Final(&md5_ctx);
  std::memcpy(subarray, md5_ctx.digest, coord_size);
  std::memcpy(&subarray[1], md5_ctx.digest, coord_size);
  std::memcpy(&subarray[2], md5_ctx.digest + coord_size, coord_size);
  std::memcpy(&subarray[3], md5_ctx.digest + coord_size, coord_size);
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

Status KV::alloc_buffers(bool has_keys) {
  for (unsigned int i = 0; i < attribute_num_; ++i) {
    if (nitems_[i] == constants::var_num)
      RETURN_NOT_OK(buff_value_offsets_[i].realloc(buffer_alloc_size_));
    RETURN_NOT_OK(buff_values_[i].realloc(buffer_alloc_size_));
  }
  if (has_keys) {
    RETURN_NOT_OK(buff_key_offsets_.realloc(buffer_alloc_size_));
    RETURN_NOT_OK(buff_keys_.realloc(buffer_alloc_size_));
    RETURN_NOT_OK(buff_key_types_.realloc(buffer_alloc_size_));
  }

  return Status::Ok();
}

Status KV::compute_coords() {
  // For easy reference
  auto keys = (uint64_t*)buff_key_offsets_.data();
  auto keys_var = (unsigned char*)buff_keys_.data();
  auto types = (unsigned char*)buff_key_types_.data();
  auto keys_var_size = buff_keys_.size();
  uint64_t coords[2];

  // Compute an MD5 digest per <key_type | key_size | key> tuple
  md5::MD5_CTX md5_ctx;
  uint64_t coords_size = sizeof(md5_ctx.digest);
  assert(coords_size == 2 * sizeof(uint64_t));
  uint64_t key_size;
  for (uint64_t i = 0; i < key_num_; ++i) {
    key_size = (i != key_num_ - 1) ? (keys[i + 1] - keys[i]) :
                                     (keys_var_size - keys[i]);
    md5::MD5Init(&md5_ctx);
    md5::MD5Update(&md5_ctx, &types[i], sizeof(char));
    md5::MD5Update(&md5_ctx, (unsigned char*)&key_size, sizeof(uint64_t));
    md5::MD5Update(&md5_ctx, &keys_var[keys[i]], (unsigned int)key_size);
    md5::MD5Final(&md5_ctx);
    std::memcpy(coords, md5_ctx.digest, coords_size);
    RETURN_NOT_OK(buff_coords_.write(coords, coords_size));
  }

  return Status::Ok();
}

bool KV::has_coords() const {
  if (array_attributes_ == nullptr)
    return false;
  return (array_attributes_[array_attribute_num_ - 1] == constants::coords);
}

bool KV::has_keys() const {
  if (array_attributes_ == nullptr || array_attribute_num_ <= attribute_num_)
    return false;
  return (array_attributes_[attribute_num_] == constants::key_attr_name);
}

}  // namespace tiledb
