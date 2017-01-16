/**
 * @file   storage_manager_config.cc
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
 * This file implements the StorageManagerConfig class.
 */



#include "storage_manager_config.h"
#include "tiledb_constants.h"




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManagerConfig::StorageManagerConfig() {
  // Default values
  home_ = "";
  read_method_ = TILEDB_IO_MMAP;
  write_method_ = TILEDB_IO_WRITE;
#ifdef HAVE_MPI
  mpi_comm_ = NULL;
#endif
}

StorageManagerConfig::~StorageManagerConfig() {
}




/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void StorageManagerConfig::init(
    const char* home,
#ifdef HAVE_MPI
    MPI_Comm* mpi_comm,
#endif
    int read_method,
    int write_method) {
  // Initialize home
  if(home == NULL)
    home_ = "";
  else
    home_ = home;

#ifdef HAVE_MPI
  // Initialize MPI communicator
  mpi_comm_ = mpi_comm;
#endif

  // Initialize read method
  read_method_ = read_method;
  if(read_method_ != TILEDB_IO_READ &&
     read_method_ != TILEDB_IO_MMAP &&
     read_method_ != TILEDB_IO_MPI)
    read_method_ = TILEDB_IO_MMAP;  // Use default 

  // Initialize write method
  write_method_ = write_method;
  if(write_method_ != TILEDB_IO_WRITE &&
     write_method_ != TILEDB_IO_MPI)
    write_method_ = TILEDB_IO_WRITE;  // Use default 
}




/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const std::string& StorageManagerConfig::home() const {
  return home_;
}

#ifdef HAVE_MPI
MPI_Comm* StorageManagerConfig::mpi_comm() const {
  return mpi_comm_;
}
#endif

int StorageManagerConfig::read_method() const {
  return read_method_;
}

int StorageManagerConfig::write_method() const {
  return write_method_;
}
