/**
 * @file   metadata.h
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
 * This file defines class Metadata. 
 */

#ifndef __METADATA_H__
#define __METADATA_H__

#include "array.h"
#include "storage_manager_config.h"

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_MT_OK           0
#define TILEDB_MT_ERR         -1
/**@}*/

/** Default error message. */
#define TILEDB_MT_ERRMSG std::string("[TileDB::Metadata] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_mt_errmsg;




/** Manages a TileDB metadata object. */
class Metadata {
 public:
  /* ********************************* */
  /*    CONSTRUCTORS & DESTRUCTORS     */
  /* ********************************* */
  
  /** Constructor. */
  Metadata();

  /** Destructor. */
  ~Metadata();




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the array that implements the metadata. */
  Array* array() const;

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /**
   * Checks if a read operation for a particular attribute resulted in a
   * buffer overflow.
   * 
   * @param attribute_id The id of the attribute for which the overflow is
   *     checked. This id corresponds to the position of the attribute name
   *     placed in the *attributes* input of init(), or reset_attributes(). If
   *     *attributes* was NULL in the above functions, then the attribute id
   *     corresponds to the order in which the attributes were defined in the
   *     array schema upon the array creation. Note that, in that case, the
   *     extra key attribute corresponds to the last extra attribute, i.e., its
   *     id is *attribute_num*. 
   * @return *true* for overflow and *false* otherwise.
   */
  bool overflow(int attribute_id) const;

  /**
   * Performs a read operation in a metadata object, which must be initialized
   * with mode TILEDB_METADATA_READ. The read is performed on a single key. 
   * 
   * @param key This is the query key, which must be a string.
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in init() or 
   *     reset_attributes(). The case of variable-sized attributes is special.
   *     Instead of providing a single buffer for such an attribute, **two**
   *     must be provided: the second will hold the variable-sized values,
   *     whereas the first holds the start offsets of each value in the second
   *     buffer.
   * @param buffer_sizes The sizes (in bytes) allocated by the user for the
   *     input buffers (there is a one-to-one correspondence). The function will
   *     attempt to write value corresponding to the key. If a buffer cannot
   *     hold the result, the function will still succeed, turning on an
   *     overflow flag which can be checked with function overflow(). 
   * @return TILEDB_MT_OK for success and TILEDB_MT_ERR for error.
   */
  int read(const char* key, void** buffers, size_t* buffer_sizes); 




  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */

  /**
   * Consolidates all fragments into a new single one, on a per-attribute basis.
   * Returns the new fragment (which has to be finalized outside this functions),
   * along with the names of the old (consolidated) fragments (which also have
   * to be deleted outside this function).
   *
   * @param new_fragment The new fragment to be returned.
   * @param old_fragment_names The names of the old fragments to be returned.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int consolidate(
      Fragment*& new_fragment, 
      std::vector<std::string>& old_fragment_names);

  /**
   * Finalizes the metadata, properly freeing up the memory space.
   *
   * @return TILEDB_MT_OK on success, and TILEDB_MT_ERR on error.
   */
  int finalize();
 
  /**
   * Initializes a TileDB metadata object.
   *
   * @param array_schema This essentially encapsulates the metadata schema.
   * @param fragment_names The names of the fragments of the array.
   * @param book_keeping The book-keeping structures of the fragments
   *     of the array.
   * @param mode The mode of the metadata. It must be one of the following:
   *    - TILEDB_METADATA_WRITE 
   *    - TILEDB_METADATA_READ 
   * @param attributes A subset of the metadata attributes the read/write will
   *     be constrained on. A NULL value indicates **all** attributes (including
   *     the key as an extra attribute in the end).
   * @param attribute_num The number of the input attributes. If *attributes* is
   *     NULL, then this should be set to 0.
   * @param config Congiguration parameters.
   * @return TILEDB_MT_OK on success, and TILEDB_MT_ERR on error.
   */
  int init(
      const ArraySchema* array_schema, 
      const std::vector<std::string>& fragment_names,
      const std::vector<BookKeeping*>& book_keeping,
      int mode,
      const char** attributes,
      int attribute_num,
      const StorageManagerConfig* config);

  /**
   * Resets the attributes used upon initialization of the metadata. 
   *
   * @param attributes The new attributes to focus on. If it is NULL, then
   *     all the attributes are used (including the key as an extra attribute
   *     in the end).
   * @param attribute_num The number of the attributes. If *attributes* is NULL,
   *     then this should be 0.
   * @return TILEDB_MT_OK on success, and TILEDB_MT_ERR on error.
   */
  int reset_attributes(const char** attributes, int attribute_num);

  /**
   * Performs a write operation in metadata object. The values are provided
   * in a set of buffers (one per attribute specified upon initialization).
   * Note that there must be a one-to-one correspondance between the 
   * values across the attribute buffers.
   *
   * The metadata must be initialized with mode TILEDB_METADATA_WRITE.
   * 
   * @param keys The buffer holding the metadata keys. These keys must be
   *     strings, serialized one after the other in the *keys* buffer.
   * @param keys_size The size (in bytes) of buffer *keys*.
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     init() or reset_attributes(). The case of variable-sized attributes is
   *     special. Instead of providing a single buffer for such an attribute,
   *     **two** must be provided: the second holds the variable-sized values,
   *     whereas the first holds the start offsets of each value in the second
   *     buffer.
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @return TILEDB_MT_OK for success and TILEDB_MT_ERR for error.
   */
  int write(
      const char* keys,
      size_t keys_size,
      const void** buffers, 
      const size_t* buffer_sizes); 




 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The underlying array that implements the metadata. */
  Array* array_;
  /** 
   * The metadata mode. It must be one of the following:
   *    - TILEDB_METADATA_WRITE 
   *    - TILEDB_METADATA_READ 
   */
  int mode_;




  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Computes the coordinates for each key (through the MD5 hash function),
   * which will be used when storing the metadata to the underlying array.
   *
   * @param keys The buffer holding the metadata keys. These keys must be
   *     strings, serialized one after the other in the *keys* buffer.
   * @param keys_size The size (in bytes) of buffer *keys*.
   * @param coords A buffer holding the computed coordinates for *keys*.
   * @param coords_size The size (in bytes) of the input *coords* buffer.
   * @return void
   */
  void compute_array_coords(
      const char* keys,
      size_t keys_size,
      void*& coords,
      size_t& coords_size) const;

  /**
   * Prepares the buffers that will be passed to the underlying array when 
   * writing metadata.
   *
   * @param coords A buffer holding the computed coordinates for the keys to be
   *     written.
   * @param coords_size The size (in bytes) of the input *coords* buffer.
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     init() or reset_attributes(). The case of variable-sized attributes is
   *     special. Instead of providing a single buffer for such an attribute,
   *     **two** must be provided: the second holds the variable-sized values,
   *     whereas the first holds the start offsets of each value in the second
   *     buffer.
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @param array_buffers These are the produced buffers that will be passed
   *     in the write() function.
   * @param array_buffer_sizes The sizes (in bytes) of the corresponding buffers
   *     in the *buffers* parameter.
   * @return void
   */
  void prepare_array_buffers(
      const void* coords,
      size_t coords_size,
      const void** buffers,
      const size_t* buffer_sizes,
      const void**& array_buffers,
      size_t*& array_buffer_sizes) const;
};

#endif
