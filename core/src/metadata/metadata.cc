/**
 * @file   metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
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
#include <cassert>
#include <cstring>
#include <openssl/md5.h>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_MT_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif



/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_mt_errmsg = "";





/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Metadata::Metadata() {
  array_ = NULL;
}

Metadata::~Metadata() {
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

Array* Metadata::array() const {
  return array_;
}

const ArraySchema* Metadata::array_schema() const {
  return array_->array_schema();
}

bool Metadata::overflow(int attribute_id) const {
  return array_->overflow(attribute_id);
}

int Metadata::read(const char* key, void** buffers, size_t* buffer_sizes) {
  // Sanity checks
  if(mode_ != TILEDB_METADATA_READ) {
    std::string errmsg = "Cannot read from metadata; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_mt_errmsg = TILEDB_MT_ERRMSG + errmsg; 
    return TILEDB_MT_ERR;
  }

  // Compute subarray for the read
  int subarray[8];
  unsigned int coords[4];
  MD5((const unsigned char*) key, strlen(key)+1, (unsigned char*) coords);

  for(int i=0; i<4; ++i) {
    subarray[2*i] = int(coords[i]);
    subarray[2*i+1] = int(coords[i]);
  } 

  // Re-init sub array
  if(array_->reset_subarray(subarray) != TILEDB_AR_OK) {
    tiledb_mt_errmsg = tiledb_ar_errmsg; 
    return TILEDB_MT_ERR;
  }

  // Read from array
  if(array_->read(buffers, buffer_sizes) != TILEDB_AR_OK) {
    tiledb_mt_errmsg = tiledb_ar_errmsg; 
    return TILEDB_MT_ERR;
  } 

  // Success
  return TILEDB_MT_OK;
}




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int Metadata::consolidate(
    Fragment*& new_fragment,
    std::vector<std::string>& old_fragment_names) {
  // Consolidate
  if(array_->consolidate(new_fragment, old_fragment_names) != TILEDB_AR_OK) {
    tiledb_mt_errmsg = tiledb_ar_errmsg; 
    return TILEDB_MT_ERR;
  }

  // Success
  return TILEDB_MT_OK;
}

int Metadata::finalize() {
  int rc = array_->finalize();

  delete array_;
  array_ = NULL;

  if(rc != TILEDB_AR_OK)  {
    tiledb_mt_errmsg = tiledb_ar_errmsg; 
    return TILEDB_MT_ERR; 
  }

  // Success
  return TILEDB_MT_OK; 
}

int Metadata::init(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping,
    int mode,
    const char** attributes,
    int attribute_num,
    const Config* config) {
  // Sanity check on mode
  if(mode != TILEDB_METADATA_READ &&
     mode != TILEDB_METADATA_WRITE) {
    std::string errmsg = "Cannot initialize metadata; Invalid metadata mode";
    PRINT_ERROR(errmsg);
    tiledb_mt_errmsg = TILEDB_MT_ERRMSG + errmsg; 
    return TILEDB_MT_ERR;
  }

  // Set mode
  mode_ = mode;
  int array_mode = (mode == TILEDB_METADATA_READ) 
                    ? TILEDB_ARRAY_READ : TILEDB_ARRAY_WRITE_UNSORTED;

  // Set attributes
  char** array_attributes;
  int array_attribute_num;
  if(attributes == NULL) {
    array_attribute_num = 
        (mode == TILEDB_METADATA_WRITE) ? array_schema->attribute_num() + 1
                                        : array_schema->attribute_num();
    array_attributes = new char*[array_attribute_num];
    for(int i=0; i<array_attribute_num; ++i) {
      const char* attribute = array_schema->attribute(i).c_str();
      size_t attribute_len = strlen(attribute);
      array_attributes[i] = new char[attribute_len+1];
      strcpy(array_attributes[i], attribute);
    } 
  } else {
    array_attribute_num = 
        (mode == TILEDB_METADATA_WRITE) ? attribute_num + 1
                                        : attribute_num;
    array_attributes = new char*[array_attribute_num];
    for(int i=0; i<attribute_num; ++i) {
      size_t attribute_len = strlen(attributes[i]);
      // Check attribute name length
      if(attributes[i] == NULL || attribute_len > TILEDB_NAME_MAX_LEN) {
        std::string errmsg = "Invalid attribute name length";
        PRINT_ERROR(errmsg);
        tiledb_mt_errmsg = TILEDB_MT_ERRMSG + errmsg; 
        return TILEDB_MT_ERR;
      }
      array_attributes[i] = new char[attribute_len+1];
      strcpy(array_attributes[i], attributes[i]);
    }
    if(mode == TILEDB_METADATA_WRITE) {
      size_t attribute_len = strlen(TILEDB_COORDS);
      array_attributes[array_attribute_num] = new char[attribute_len+1];
      strcpy(array_attributes[array_attribute_num], TILEDB_COORDS);
    }
  }

  // Initialize array
  array_ = new Array();
  int rc = array_->init(
              array_schema, 
              fragment_names,
              book_keeping,
              array_mode, 
              (const char**) array_attributes, 
              array_attribute_num, 
              NULL,
              config);

  // Clean up
  for(int i=0; i<array_attribute_num; ++i) 
    delete [] array_attributes[i];
  delete [] array_attributes;

  // Error
  if(rc != TILEDB_AR_OK) {
    tiledb_mt_errmsg = tiledb_ar_errmsg; 
    return TILEDB_MT_ERR;
  }

  // Success
  return TILEDB_MT_OK;
}

int Metadata::reset_attributes(
    const char** attributes,
    int attribute_num) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Set attributes
  char** array_attributes;
  int array_attribute_num;
  if(attributes == NULL) {
    array_attribute_num = 
        (mode_ == TILEDB_METADATA_WRITE) ? array_schema->attribute_num() + 1
                                        : array_schema->attribute_num();
    array_attributes = new char*[array_attribute_num];
    for(int i=0; i<array_attribute_num; ++i) {
      const char* attribute = array_schema->attribute(i).c_str();
      size_t attribute_len = strlen(attribute);
      array_attributes[i] = new char[attribute_len+1];
      strcpy(array_attributes[i], attribute);
    } 
  } else {
    array_attribute_num = 
        (mode_ == TILEDB_METADATA_WRITE) ? attribute_num + 1
                                        : attribute_num;
    array_attributes = new char*[array_attribute_num];
    for(int i=0; i<attribute_num; ++i) {
      size_t attribute_len = strlen(attributes[i]);
      // Check attribute name length
      if(attributes[i] == NULL || attribute_len > TILEDB_NAME_MAX_LEN) {
        std::string errmsg = "Invalid attribute name length";
        PRINT_ERROR(errmsg);
        tiledb_mt_errmsg = errmsg; 
        return TILEDB_MT_ERR;
      }
      array_attributes[i] = new char[attribute_len+1];
      strcpy(array_attributes[i], attributes[i]);
    }
    if(mode_ == TILEDB_METADATA_WRITE) {
      size_t attribute_len = strlen(TILEDB_COORDS);
      array_attributes[array_attribute_num] = new char[attribute_len+1];
      strcpy(array_attributes[array_attribute_num], TILEDB_COORDS);
    }
  }

  // Reset attributes
  int rc = array_->reset_attributes(
               (const char**) array_attributes, 
               array_attribute_num);

  // Clean up
  for(int i=0; i<array_attribute_num; ++i) 
    delete [] array_attributes[i];
  delete [] array_attributes;

  // Error
  if(rc != TILEDB_AR_OK) {
    tiledb_mt_errmsg = tiledb_ar_errmsg; 
    return TILEDB_MT_ERR;
  }

  // Success
  return TILEDB_MT_OK;
}

int Metadata::write(
    const char* keys,
    size_t keys_size,
    const void** buffers, 
    const size_t* buffer_sizes) {
  // Sanity checks
  if(mode_ != TILEDB_METADATA_WRITE) {
    std::string errmsg = "Cannot write to metadata; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_mt_errmsg = errmsg; 
    return TILEDB_MT_ERR;
  }
  if(keys == NULL) { 
    std::string errmsg = "Cannot write to metadata; No keys given";
    PRINT_ERROR(errmsg);
    tiledb_mt_errmsg = errmsg; 
    return TILEDB_MT_ERR;
  }

  // Compute array coordinates
  void* coords;
  size_t coords_size; 
  compute_array_coords(
      keys, 
      keys_size, 
      coords, 
      coords_size);

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
  int rc = array_->write(array_buffers, array_buffer_sizes);

  // Clean up
  free(coords);
  free(array_buffers);
  free(array_buffer_sizes);

  // Error
  if(rc != TILEDB_AR_OK) {
    tiledb_mt_errmsg = tiledb_ar_errmsg; 
    return TILEDB_MT_ERR;
  }
  
  // Success
  return TILEDB_MT_OK;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Metadata::compute_array_coords(
    const char* keys,
    size_t keys_size,
    void*& coords,
    size_t& coords_size) const {
  // Compute keys offsets
  size_t* keys_offsets = (size_t*) malloc(10*sizeof(size_t)); 
  int64_t keys_num_allocated = 10;
  int64_t keys_num = 0;
  bool null_char_found = true;
  for(size_t i=0; i<keys_size; ++i) {
    // In case the null character is found
    if(null_char_found) {
      if(keys_num == keys_num_allocated) {
        keys_num_allocated *= 2;
        keys_offsets = 
            (size_t*) realloc(keys_offsets, keys_num_allocated*sizeof(size_t));
      }
      keys_offsets[keys_num] = i;
      ++keys_num;
      null_char_found = false;
    }

    // Null character found and proper flag is set
    if(keys[i] == '\0')  
      null_char_found = true;
  }
  assert(keys_num > 0);

  // Compute coords
  coords_size = keys_num * 4 * sizeof(int); 
  coords = malloc(coords_size);
  size_t key_size;
  const unsigned char* keys_c;
  unsigned char* coords_c;
  for(int64_t i=0; i<keys_num; ++i) {
    key_size = (i != keys_num-1) ? keys_offsets[i+1] - keys_offsets[i] 
                                 : keys_size - keys_offsets[i];
    keys_c = ((const unsigned char*) keys) + keys_offsets[i];
    coords_c = ((unsigned char*) coords) + i*4*sizeof(int);
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
    size_t*& array_buffer_sizes) const{
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size();

  // Count number of variable-sized attributes
  int var_attribute_num = 0;
  for(int i=0; i<attribute_id_num; ++i) 
    if(array_schema->var_size(attribute_ids[i]))
      ++var_attribute_num;

  // Allocate space for the array buffers
  array_buffers = 
      (const void**) malloc(
           (attribute_id_num + var_attribute_num)*sizeof(const void*));
  array_buffer_sizes = 
      (size_t*) malloc(
           (attribute_id_num + var_attribute_num)*sizeof(size_t));

  // Set the array buffers
  int buffer_i = 0;
  int array_buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) {
    if(attribute_ids[i] == attribute_num) {   // Coordinates 
      array_buffers[array_buffer_i] = coords;
      array_buffer_sizes[array_buffer_i] = coords_size;
      ++array_buffer_i;
    } else {                                  // Any other attribute 
      array_buffers[array_buffer_i] = buffers[buffer_i];
      array_buffer_sizes[array_buffer_i] = buffer_sizes[buffer_i];
      ++array_buffer_i;
      ++buffer_i;
      if(array_schema->var_size(attribute_ids[i])) { // Variable-sized 
        array_buffers[array_buffer_i] = buffers[buffer_i];
        array_buffer_sizes[array_buffer_i] = buffer_sizes[buffer_i];
        ++array_buffer_i;
        ++buffer_i;
      }
    }
  }
}

