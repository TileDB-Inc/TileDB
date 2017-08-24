/**
 * @file  config.h
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
 * This file defines class Config.
 */

#ifndef TILEDB_CONFIGURATOR_H
#define TILEDB_CONFIGURATOR_H

#include "io_method.h"
#ifdef HAVE_MPI
#include <mpi.h>
#endif

namespace tiledb {

/**
 * This class is responsible for the TileDB configuration parameters.
 */
class Config {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Config();

  /**
   * Constructor that clones the values of the input config object.
   *
   * @param config The config object to clone into the new object.
   */
  Config(const Config* config);

  /** Destructor. */
  ~Config();

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

#ifdef HAVE_MPI
  /** Returns the MPI communicator. */
  MPI_Comm* mpi_comm() const;
#endif

  /** Returns the read method. */
  IOMethod read_method() const;

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
};

}  // namespace tiledb

#endif  // TILEDB_CONFIGURATOR_H
