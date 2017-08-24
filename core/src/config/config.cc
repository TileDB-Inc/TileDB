/**
 * @file   config.cc
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
 * This file implements the Config class.
 */

#include "config.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Config::Config() {
  // Default values
  read_method_ = IOMethod::MMAP;
  write_method_ = IOMethod::WRITE;
#ifdef HAVE_MPI
  mpi_comm_ = nullptr;
#endif
}

Config::Config(const Config* config) {
  if (config == nullptr) {  // Default
#ifdef HAVE_MPI
    mpi_comm_ = nullptr;
#endif
    read_method_ = IOMethod::MMAP;
    write_method_ = IOMethod::WRITE;
  } else {  // Clone
#ifdef HAVE_MPI
    mpi_comm_ = config->mpi_comm();
#endif
    read_method_ = config->read_method();
    write_method_ = config->write_method();
  }
}

Config::~Config() = default;

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void Config::init(
#ifdef HAVE_MPI
    MPI_Comm* mpi_comm,
#endif
    IOMethod read_method,
    IOMethod write_method) {
#ifdef HAVE_MPI
  // Initialize MPI communicator
  mpi_comm_ = mpi_comm;
#endif

  // Initialize read method
  read_method_ = read_method;
  if (read_method_ != IOMethod::READ && read_method_ != IOMethod::MMAP &&
      read_method_ != IOMethod::MPI)
    read_method_ = IOMethod::MMAP;  // Use default

  // Initialize write method
  write_method_ = write_method;
  if (write_method_ != IOMethod::WRITE && write_method_ != IOMethod::MPI)
    write_method_ = IOMethod::WRITE;  // Use default
}

#ifdef HAVE_MPI
void Config::set_mpi_comm(MPI_Comm* mpi_comm) {
  mpi_comm_ = mpi_comm;
}
#endif

void Config::set_read_method(IOMethod read_method) {
  read_method_ = read_method;
}

void Config::set_write_method(IOMethod write_method) {
  write_method_ = write_method;
}

#ifdef HAVE_MPI
MPI_Comm* Config::mpi_comm() const {
  return mpi_comm_;
}
#endif

IOMethod Config::read_method() const {
  return read_method_;
}

IOMethod Config::write_method() const {
  return write_method_;
}

};  // namespace tiledb
