/**
 * @file   metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements the Metadata class.
 */

#include "metadata.h"
#include <openssl/md5.h>
#include "logger.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

/*

Metadata::Metadata() {
array_ = nullptr;
}

Metadata::~Metadata() = default;
*/

/* ****************************** */
/*             API                */
/* ****************************** */

/*

Array* Metadata::array() const {
return array_;
}

 */

const MetadataSchema* Metadata::metadata_schema() const {
  return metadata_schema_;
}

/*
bool Metadata::overflow(int attribute_id) const {
return array_->overflow(attribute_id);
}

Status Metadata::read(const char* key, void** buffers, size_t* buffer_sizes) {
// Sanity checks
if (mode_ != TILEDB_METADATA_READ) {
return LOG_STATUS(
    Status::MetadataError("Cannot read from metadata; Invalid mode"));
}

// Compute subarray for the read
int subarray[8];
unsigned int coords[4];
MD5((const unsigned char*)key, strlen(key) + 1, (unsigned char*)coords);

for (int i = 0; i < 4; ++i) {
subarray[2 * i] = int(coords[i]);
subarray[2 * i + 1] = int(coords[i]);
}

// Re-init sub array
RETURN_NOT_OK(array_->reset_subarray(subarray));

// Read from array
RETURN_NOT_OK(array_->read(buffers, buffer_sizes));

return Status::Ok();
}

Status Metadata::consolidate(
Fragment*& new_fragment, std::vector<std::string>& old_fragment_names) {
// Consolidate
RETURN_NOT_OK(array_->consolidate(new_fragment, old_fragment_names));
return Status::Ok();
}

Status Metadata::finalize() {
Status st = array_->finalize();
delete array_;
array_ = nullptr;
return st;
}

Status Metadata::init(
StorageManager* storage_manager,
const ArraySchema* array_schema,
const std::vector<std::string>& fragment_names,
const std::vector<BookKeeping*>& book_keeping,
tiledb_metadata_mode_t mode,
const char** attributes,
int attribute_num,
const Configurator* config) {
// For easy reference
unsigned name_max_len = Configurator::name_max_len();

if (metadata_schema_ != nullptr)
delete metadata_schema_;

metadata_schema_ = new MetadataSchema(array_schema);

// Sanity check on mode
if (mode != TILEDB_METADATA_READ && mode != TILEDB_METADATA_WRITE) {
return LOG_STATUS(Status::MetadataError(
    "Cannot initialize metadata; Invalid metadata mode"));
}

// Set mode
mode_ = mode;
ArrayMode array_mode = (mode == TILEDB_METADATA_READ) ?
                         ArrayMode::READ :
                         ArrayMode::WRITE_UNSORTED;

// Set attributes
char** array_attributes;
int array_attribute_num;
if (attributes == nullptr) {
array_attribute_num = (mode == TILEDB_METADATA_WRITE) ?
                          array_schema->attribute_num() + 1 :
                          array_schema->attribute_num();
array_attributes = new char*[array_attribute_num];
for (int i = 0; i < array_attribute_num; ++i) {
  const char* attribute = array_schema->attribute(i).c_str();
  size_t attribute_len = strlen(attribute);
  array_attributes[i] = new char[attribute_len + 1];
  strcpy(array_attributes[i], attribute);
}
} else {
array_attribute_num =
    (mode == TILEDB_METADATA_WRITE) ? attribute_num + 1 : attribute_num;
array_attributes = new char*[array_attribute_num];
for (int i = 0; i < attribute_num; ++i) {
  size_t attribute_len = strlen(attributes[i]);
  // Check attribute name length
  if (attributes[i] == nullptr || attribute_len > name_max_len) {
    for (int attr = 0; attr < i; ++i)
      delete array_attributes[i];
    delete[] array_attributes;
    return LOG_STATUS(
        Status::MetadataError("Invalid attribute name length"));
  }
  array_attributes[i] = new char[attribute_len + 1];
  strcpy(array_attributes[i], attributes[i]);
}
if (mode == TILEDB_METADATA_WRITE) {
  size_t attribute_len = strlen(Configurator::coords());
  array_attributes[array_attribute_num] = new char[attribute_len + 1];
  strcpy(array_attributes[array_attribute_num], Configurator::coords());
}
}

// Initialize array
array_ = new Array();
Status st = array_->init(
  storage_manager,
  array_schema,
  fragment_names,
  book_keeping,
  array_mode,
  (const char**)array_attributes,
  array_attribute_num,
  nullptr,
  config);

// Clean up
for (int i = 0; i < array_attribute_num; ++i)
delete[] array_attributes[i];
delete[] array_attributes;

return st;
}

Status Metadata::reset_attributes(const char** attributes, int attribute_num) {
// For easy reference
const ArraySchema* array_schema = array_->array_schema();
unsigned name_max_len = Configurator::name_max_len();

// Set attributes
char** array_attributes;
int array_attribute_num;
if (attributes == nullptr) {
array_attribute_num = (mode_ == TILEDB_METADATA_WRITE) ?
                          array_schema->attribute_num() + 1 :
                          array_schema->attribute_num();
array_attributes = new char*[array_attribute_num];
for (int i = 0; i < array_attribute_num; ++i) {
  const char* attribute = array_schema->attribute(i).c_str();
  size_t attribute_len = strlen(attribute);
  array_attributes[i] = new char[attribute_len + 1];
  strcpy(array_attributes[i], attribute);
}
} else {
array_attribute_num =
    (mode_ == TILEDB_METADATA_WRITE) ? attribute_num + 1 : attribute_num;
array_attributes = new char*[array_attribute_num];
for (int i = 0; i < attribute_num; ++i) {
  size_t attribute_len = strlen(attributes[i]);
  // Check attribute name length
  if (attributes[i] == nullptr || attribute_len > name_max_len) {
    for (int attr = 0; attr < i; ++i)
      delete array_attributes[i];
    delete[] array_attributes;
    return LOG_STATUS(
        Status::MetadataError("Invalid attribute name length"));
  }
  array_attributes[i] = new char[attribute_len + 1];
  strcpy(array_attributes[i], attributes[i]);
}
if (mode_ == TILEDB_METADATA_WRITE) {
  size_t attribute_len = strlen(Configurator::coords());
  array_attributes[array_attribute_num] = new char[attribute_len + 1];
  strcpy(array_attributes[array_attribute_num], Configurator::coords());
}
}

// Reset attributes
Status st = array_->reset_attributes(
  (const char**)array_attributes, array_attribute_num);

// Clean up
for (int i = 0; i < array_attribute_num; ++i)
delete[] array_attributes[i];
delete[] array_attributes;

return st;
}

Status Metadata::write(
const char* keys,
size_t keys_size,
const void** buffers,
const size_t* buffer_sizes) {
// Sanity checks
if (mode_ != TILEDB_METADATA_WRITE) {
return LOG_STATUS(
    Status::MetadataError("Cannot write to metadata; Invalid mode"));
}
if (keys == nullptr) {
return LOG_STATUS(
    Status::MetadataError("Cannot write to metadata; No keys given"));
}

// Compute array coordinates
void* coords;
size_t coords_size;
compute_array_coords(keys, keys_size, coords, coords_size);

// Prepare array buffers
const void** array_buffers;
size_t* array_buffer_sizes;
prepare_array_buffers(
  coords,
  coords_size,
  buffers,
  buffer_sizes,
  array_buffers,
  array_buffer_sizes);

// Write the metadata
Status st = array_->write(array_buffers, array_buffer_sizes);

// Clean up
free(coords);
free(array_buffers);
free(array_buffer_sizes);

return st;
}

 */

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

/*

void Metadata::compute_array_coords(
const char* keys,
size_t keys_size,
void*& coords,
size_t& coords_size) const {
// Compute keys offsets
size_t* keys_offsets = (size_t*)malloc(10 * sizeof(size_t));
int64_t keys_num_allocated = 10;
int64_t keys_num = 0;
bool null_char_found = true;
for (size_t i = 0; i < keys_size; ++i) {
// In case the null character is found
if (null_char_found) {
  if (keys_num == keys_num_allocated) {
    keys_num_allocated *= 2;
    // TODO: this can leak if realloc fails
    keys_offsets =
        (size_t*)realloc(keys_offsets, keys_num_allocated * sizeof(size_t));
  }
  keys_offsets[keys_num] = i;
  ++keys_num;
  null_char_found = false;
}

// Null character found and proper flag is set
if (keys[i] == '\0')
  null_char_found = true;
}
assert(keys_num > 0);

// Compute coords
coords_size = keys_num * 4 * sizeof(int);
coords = malloc(coords_size);
size_t key_size;
const unsigned char* keys_c;
unsigned char* coords_c;
for (int64_t i = 0; i < keys_num; ++i) {
key_size = (i != keys_num - 1) ? keys_offsets[i + 1] - keys_offsets[i] :
                                 keys_size - keys_offsets[i];
keys_c = ((const unsigned char*)keys) + keys_offsets[i];
coords_c = ((unsigned char*)coords) + i * 4 * sizeof(int);
MD5(keys_c, key_size, coords_c);
}

// Clean up
free(keys_offsets);
}

void Metadata::prepare_array_buffers(
const void* coords,
size_t coords_size,
const void** buffers,
const size_t* buffer_sizes,
const void**& array_buffers,
size_t*& array_buffer_sizes) const {
// For easy reference
const ArraySchema* array_schema = array_->array_schema();
int attribute_num = array_schema->attribute_num();
const std::vector<int> attribute_ids = array_->attribute_ids();
int attribute_id_num = attribute_ids.size();

// Count number of variable-sized attributes
int var_attribute_num = 0;
for (int i = 0; i < attribute_id_num; ++i)
if (array_schema->var_size(attribute_ids[i]))
  ++var_attribute_num;

// Allocate space for the array buffers
array_buffers = (const void**)malloc(
  (attribute_id_num + var_attribute_num) * sizeof(const void*));
array_buffer_sizes =
  (size_t*)malloc((attribute_id_num + var_attribute_num) * sizeof(size_t));

// Set the array buffers
int buffer_i = 0;
int array_buffer_i = 0;
for (int i = 0; i < attribute_id_num; ++i) {
if (attribute_ids[i] == attribute_num) {  // Coordinates
  array_buffers[array_buffer_i] = coords;
  array_buffer_sizes[array_buffer_i] = coords_size;
  ++array_buffer_i;
} else {  // Any other attribute
  array_buffers[array_buffer_i] = buffers[buffer_i];
  array_buffer_sizes[array_buffer_i] = buffer_sizes[buffer_i];
  ++array_buffer_i;
  ++buffer_i;
  if (array_schema->var_size(attribute_ids[i])) {  // Variable-sized
    array_buffers[array_buffer_i] = buffers[buffer_i];
    array_buffer_sizes[array_buffer_i] = buffer_sizes[buffer_i];
    ++array_buffer_i;
    ++buffer_i;
  }
}
}
}

 */

};  // namespace tiledb
