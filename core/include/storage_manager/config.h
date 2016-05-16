/**
 * @file   config.h
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
 * This file defines class Config.  
 */  

#ifndef __CONFIG_H__
#define __CONFIG_H__

#include <mpi.h>
#include <string>




/** This class is responsible for the TileDB configuration parameters. */
class Config {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Config();

  /** Destructor. */
  ~Config();



 
  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */

  /**
   * Initializes the configuration parameters.
   *
   * @param home The TileDB home directory.
   * @param mpi_comm The MPI communicator.
   * @param read_method The method for reading data from a file. 
   *     It can be one of the following: 
   *        - TILEDB_USE_READ
   *          TileDB will use POSIX read.
   *        - TILEDB_USE_MMAP
   *          TileDB will use mmap.
   *        - TILEDB_USE_MPIIO
   *          TileDB will use MPI-IO read. 
   * @param write_method The method for writing data to a file. 
   *     It can be one of the following: 
   *        - TILEDB_USE_WRITE
   *          TileDB will use POSIX write.
   *        - TILEDB_USE_MPI_IO
   *          TileDB will use MPI-IO write.
   * @return void. 
   */
  void init(
      const char* home,
      MPI_Comm* mpi_comm,
      int read_method,
      int write_methods); 

 
  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the TileDB home directory. */
  const std::string& home() const; 

  /** Returns the MPI communicator. */
  MPI_Comm* mpi_comm() const;

  /** Returns the read method. */
  int read_method() const;

  /** Returns the write method. */
  int write_method() const;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** TileDB home directory. */
  std::string home_;
  /** The MPI communicator. */
  MPI_Comm* mpi_comm_;
  /** 
   * The method for reading data from a file. 
   * It can be one of the following: 
   *    - TILEDB_USE_READ
   *      TileDB will use POSIX read.
   *    - TILEDB_USE_MMAP
   *      TileDB will use mmap.
   *    - TILEDB_USE_MPI_IO
   *      TileDB will use MPI-IO read. 
   */
  int read_method_;
  /** 
   * The method for writing data to a file. 
   * It can be one of the following: 
   *    - TILEDB_USE_WRITE
   *      TileDB will use POSIX write.
   *    - TILEDB_USE_MPI_IO
   *      TileDB will use MPI-IO write. 
   */
  int write_method_;
};

#endif
