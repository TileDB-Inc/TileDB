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

#ifndef __TILEDB_CONFIGURATOR_H__
#define __TILEDB_CONFIGURATOR_H__

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

  /** Returns the array schema file name. */
  static const char* array_schema_filename();

  /** Returns the bookkeeping file name. */
  static const char* bookkeeping_filename();

  /** Returns the default tile capacity. */
  static int capacity();

  /** Returns the size of a variable cell offset. */
  static uint64_t cell_var_offset_size();

  /** Returns a special name reserved for the coordinates attribute. */
  static const char* coords();

  /** Returns the consolidation buffer size. */
  static uint64_t consolidation_buffer_size();

  /** Returns the consolidation filelock name. */
  static const char* consolidation_filelock_name();

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
  static const int CAPACITY;

  /**@{*/
  /** Size of buffer used for sorting. */
  static const uint64_t SORTED_BUFFER_SIZE;
  static const uint64_t SORTED_BUFFER_VAR_SIZE;
  /**@}*/

  /**@{*/
  /** Special TileDB file name. */
  static const char* ARRAY_SCHEMA_FILENAME;
  static const char* METADATA_SCHEMA_FILENAME;
  static const char* CONSOLIDATION_FILELOCK_NAME;
  static const char* BOOK_KEEPING_FILENAME;
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
  static const int VAR_NUM;
  static const uint64_t VAR_SIZE;
  /**@}*/

  /**@{*/
  /** Special attribute name. */
  static const char* COORDS;
  static const char* KEY;
  /**@}*/
};

}  // namespace tiledb

#endif  // __TILEDB_CONFIGURATOR_H__
