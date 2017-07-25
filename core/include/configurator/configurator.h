/**
 * @file  configurator.h
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
 * This file defines class Configurator.
 */

#ifndef TILEDB_CONFIGURATOR_H
#define TILEDB_CONFIGURATOR_H

#include <cfloat>
#include <climits>
#include "io_method.h"
#ifdef HAVE_MPI
#include <mpi.h>
#endif
#include <string>

namespace tiledb {

/**
 * This class is responsible for the TileDB configuration parameters.
 */
class Configurator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Configurator();

  /**
   * Constructor that clones the values of the input configurator.
   *
   * @param config The configurator to clone into the new object.
   */
  Configurator(Configurator* config);

  /** Destructor. */
  ~Configurator();

  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */

#ifdef HAVE_MPI
  /**
   * Initializes the configuration parameters.
   *
   * @param mpi_comm The MPI communicator.
   * @param read_method The method for reading data from a file.
   *     It can be one of the following:
   *        - TILEDB_IO_METHOD_READ
   *          TileDB will use POSIX read.
   *        - TILEDB_IO_METHOD_MMAP
   *          TileDB will use mmap.
   *        - TILEDB_IO_METHOD_MPI
   *          TileDB will use MPI-IO read.
   * @param write_method The method for writing data to a file.
   *     It can be one of the following:
   *        - TILEDB_IO_METHOD_WRITE
   *          TileDB will use POSIX write.
   *        - TILEDB_IO_METHOD_MPI
   *          TileDB will use MPI-IO write.
   * @return void.
   */
  void init(MPI_Comm* mpi_comm, IOMethod read_method, IOMethod write_method);
#else
  /**
   * Initializes the configuration parameters.
   *
   * @param read_method The method for reading data from a file.
   *     It can be one of the following:
   *        - TILEDB_IO_METHOD_READ
   *          TileDB will use POSIX read.
   *        - TILEDB_IO_METHOD_MMAP
   *          TileDB will use mmap.
   *        - TILEDB_IO_METHOD_MPI
   *          TileDB will use MPI-IO read.
   * @param write_method The method for writing data to a file.
   *     It can be one of the following:
   *        - TILEDB_IO_METHOD_WRITE
   *          TileDB will use POSIX write.
   *        - TILEDB_IO_METHOD_MPI
   *          TileDB will use MPI-IO write.
   * @return void.
   */
  void init(IOMethod read_method, IOMethod write_method);
#endif

#ifdef HAVE_MPI
  /**
   * Sets the MPI communicator.
   *
   * @param mpi_comm The MPI communicator to be set.
   */
  void set_mpi_comm(MPI_Comm* mpi_comm);
#endif

  /**
   * Sets the read method.
   *
   * @param read_method The read method to be set.
   */
  void set_read_method(IOMethod read_method);

  /**
   * Sets the write method.
   *
   * @param write_method The write method to be set.
   */
  void set_write_method(IOMethod write_method);

  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the array filelock name. */
  static const char* array_filelock_name();

  /** Returns the array schema file name. */
  static const char* array_schema_filename();

  /** Returns the default tile capacity. */
  static uint64_t capacity();

  /** Returns the size of a variable cell offset. */
  static uint64_t cell_var_offset_size();

  /** Returns a special name reserved for the coordinates attribute. */
  static const char* coords();

  /** Returns the consolidation buffer size. */
  static uint64_t consolidation_buffer_size();

  /** Returns the special value for an empty int32. */
  static int empty_int32();

  /** Returns the special value for an empty int64. */
  static int64_t empty_int64();

  /** Returns the special value for an empty float32. */
  static float empty_float32();

  /** Returns the special value for an empty float64. */
  static double empty_float64();

  /** Returns the special value for an empty char. */
  static char empty_char();

  /** Returns the special value for an empty int8. */
  static int8_t empty_int8();

  /** Returns the special value for an empty uint8. */
  static uint8_t empty_uint8();

  /** Returns the special value for an empty int16. */
  static int16_t empty_int16();

  /** Returns the special value for an empty uint16. */
  static uint16_t empty_uint16();

  /** Returns the special value for an empty uint32. */
  static uint32_t empty_uint32();

  /** Returns the special value for an empty uint64. */
  static uint64_t empty_uint64();

  /** Returns the file suffix used in TileDB. */
  static const char* file_suffix();

  /** Returns the fragment file name. */
  static const char* fragment_filename();

  /** Returns the fragment metadata file name. */
  static const char* fragment_metadata_filename();

  /** Returns the group file name. */
  static const char* group_filename();

  /** Returns the GZIP suffix used in TileDB. */
  static const char* gzip_suffix();

  /** Returns the initial internal buffer size for the case of sparse arrays. */
  static uint64_t internal_buffer_size();

  /** Returns a special name reserved for the metadata key attribute. */
  static const char* key();

  /** Returns the name of the first key dimension (used in metadata). */
  static const char* key_dim1_name();

  /** Returns the name of the second key dimension (used in metadata). */
  static const char* key_dim2_name();

  /** Returns the name of the third key dimension (used in metadata). */
  static const char* key_dim3_name();

  /** Returns the name of the fourth key dimension (used in metadata). */
  static const char* key_dim4_name();

  /** Returns the maximum number of bytes written in a single I/O. */
  static uint64_t max_write_bytes();

  /** Returns the metadata schema file name. */
  static const char* metadata_schema_filename();

#ifdef HAVE_MPI
  /** Returns the MPI communicator. */
  MPI_Comm* mpi_comm() const;
#endif

  /** Returns the maximum name length. */
  static unsigned name_max_len();

  /** Returns the read method. */
  IOMethod read_method() const;

  /** Returns the size of the buffer that holds the sorted cells. */
  static uint64_t sorted_buffer_size();

  /** Returns the size of the buffer that holds the sorted variable cells. */
  static uint64_t sorted_buffer_var_size();

  /** Returns a special value indicating a variable number of elements. */
  static int var_num();

  /** Returns a special value indicating a variable size. */
  static uint64_t var_size();

  /** Returns the write method. */
  IOMethod write_method() const;

  /** Returns a string describing no compression. */
  static const char* no_compression_str();

  /** Returns a string describing GZIP. */
  static const char* gzip_str();

  /** Returns a string describing ZSTD. */
  static const char* zstd_str();

  /** Returns a string describing LZ4. */
  static const char* lz4_str();

  /** Returns a string describing BLOSC. */
  static const char* blosc_str();

  /** Returns a string describing BLOSC_LZ4. */
  static const char* blosc_lz4_str();

  /** Returns a string describing BLOSC_LZ4HC. */
  static const char* blosc_lz4hc_str();

  /** Returns a string describing BLOSC_SNAPPY. */
  static const char* blosc_snappy_str();

  /** Returns a string describing BLOSC_ZLIB. */
  static const char* blosc_zlib_str();

  /** Returns a string describing BLOSC_ZSTD. */
  static const char* blosc_zstd_str();

  /** Returns a string describing RLE. */
  static const char* rle_str();

  /** Returns a string describing BZIP2. */
  static const char* bzip2_str();

  /** Returns the string representation for type int32. */
  static const char* int32_str();

  /** Returns the string representation for type int64. */
  static const char* int64_str();

  /** Returns the string representation for type float32. */
  static const char* float32_str();

  /** Returns the string representation for type float64. */
  static const char* float64_str();

  /** Returns the string representation for type char. */
  static const char* char_str();

  /** Returns the string representation for type int8. */
  static const char* int8_str();

  /** Returns the string representation for type uint8. */
  static const char* uint8_str();

  /** Returns the string representation for type int16. */
  static const char* int16_str();

  /** Returns the string representation for type uint16. */
  static const char* uint16_str();

  /** Returns the string representation for type uint32. */
  static const char* uint32_str();

  /** Returns the string representation for type uint64. */
  static const char* uint64_str();

  /** Returns the string representation for the dense array type. */
  static const char* dense_str();

  /** Returns the string representation for the sparse array type. */
  static const char* sparse_str();

  /** Returns the string representation for the column-major layout. */
  static const char* col_major_str();

  /** Returns the string representation for the row-major layout. */
  static const char* row_major_str();

  /** Returns the string representation of null. */
  static const char* null_str();

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

#ifdef HAVE_MPI
  /** The MPI communicator. */
  MPI_Comm* mpi_comm_;
#endif

  /**
   * The method for reading data from a file.
   * It can be one of the following:
   *    - TILEDB_IO_METHOD_READ
   *      TileDB will use POSIX read.
   *    - TILEDB_IO_METHOD_MMAP
   *      TileDB will use mmap.
   *    - TILEDB_IO_METHOD_MPI
   *      TileDB will use MPI-IO read.
   */
  IOMethod read_method_;

  /**
   * The method for writing data to a file.
   * It can be one of the following:
   *    - TILEDB_IO_METHOD_WRITE
   *      TileDB will use POSIX write.
   *    - TILEDB_IO_METHOD_MPI
   *      TileDB will use MPI-IO write.
   */
  IOMethod write_method_;

  /* ********************************* */
  /*         PRIVATE CONSTANTS         */
  /* ********************************* */

  /**@{*/
  /** Special TileDB file name. */
  static const char* BOOKKEEPING_FILENAME;
  /**@}*/

  /** Size of the starting offset of a variable cell value. */
  static const uint64_t CELL_VAR_OFFSET_SIZE;

  /** Size of the buffer used during consolidation. */
  static const uint64_t CONSOLIDATION_BUFFER_SIZE;

  /**@{*/
  /** Special TileDB file name suffix. */
  static const char* FILE_SUFFIX;
  static const char* GZIP_SUFFIX;
  /**@}*/

  /** Initial internal buffer size for the case of sparse arrays. */
  static const uint64_t INTERNAL_BUFFER_SIZE;

  /** The maximum length for the names of TileDB objects. */
  static const unsigned NAME_MAX_LEN;

  /**@{*/
  /** Special key dimension name. */
  static const char* KEY_DIM1_NAME;
  static const char* KEY_DIM2_NAME;
  static const char* KEY_DIM3_NAME;
  static const char* KEY_DIM4_NAME;
  /**@}*/

  /** Default tile capacity. */
  static const uint64_t CAPACITY;

  /**@{*/
  /** Size of buffer used for sorting. */
  static const uint64_t SORTED_BUFFER_SIZE;
  static const uint64_t SORTED_BUFFER_VAR_SIZE;
  /**@}*/

  /**@{*/
  /** Special TileDB file name. */
  static const char* ARRAY_SCHEMA_FILENAME;
  static const char* METADATA_SCHEMA_FILENAME;
  static const char* ARRAY_FILELOCK_NAME;
  static const char* FRAGMENT_METADATA_FILENAME;
  static const char* FRAGMENT_FILENAME;
  static const char* GROUP_FILENAME;
  /**@}*/

  /**@{*/
  /** Special empty cell value. */
  static const int EMPTY_INT32;
  static const int64_t EMPTY_INT64;
  static const float EMPTY_FLOAT32;
  static const double EMPTY_FLOAT64;
  static const char EMPTY_CHAR;
  static const int8_t EMPTY_INT8;
  static const uint8_t EMPTY_UINT8;
  static const int16_t EMPTY_INT16;
  static const uint16_t EMPTY_UINT16;
  static const uint32_t EMPTY_UINT32;
  static const uint64_t EMPTY_UINT64;
  /**@}*/

  /** Maximum number of bytes written in a single I/O. */
  static const uint64_t MAX_WRITE_BYTES;

  /**@{*/
  /** Special value indicating a variable number or size. */
  static const unsigned int VAR_NUM;
  static const uint64_t VAR_SIZE;
  /**@}*/

  /**@{*/
  /** Special attribute name. */
  static const char* COORDS;
  static const char* KEY;
  /**@}*/

  /**@{*/
  /** String describing the compressor. */
  static const char* NO_COMPRESSION_STR;
  static const char* GZIP_STR;
  static const char* ZSTD_STR;
  static const char* LZ4_STR;
  static const char* BLOSC_STR;
  static const char* BLOSC_LZ4_STR;
  static const char* BLOSC_LZ4HC_STR;
  static const char* BLOSC_SNAPPY_STR;
  static const char* BLOSC_ZLIB_STR;
  static const char* BLOSC_ZSTD_STR;
  static const char* RLE_STR;
  static const char* BZIP2_STR;
  /**@}*/

  /**@{*/
  /** String describing the type. */
  static const char* INT32_STR;
  static const char* INT64_STR;
  static const char* FLOAT32_STR;
  static const char* FLOAT64_STR;
  static const char* CHAR_STR;
  static const char* INT8_STR;
  static const char* UINT8_STR;
  static const char* INT16_STR;
  static const char* UINT16_STR;
  static const char* UINT32_STR;
  static const char* UINT64_STR;
  /**@}*/

  /**@{*/
  /** String describing the array type. */
  static const char* DENSE_STR;
  static const char* SPARSE_STR;
  /**@}*/

  /**@{*/
  /** String describing the layout. */
  static const char* COL_MAJOR_STR;
  static const char* ROW_MAJOR_STR;
  /**@}*/

  /** String representation for null. */
  static const char* NULL_STR;
};

}  // namespace tiledb

#endif  // TILEDB_CONFIGURATOR_H
