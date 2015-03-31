/**
 * @file   MPI_module.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines class MPIModule, which is implements the network
 * infrastructure via MPI.
 */

#ifndef MPI_MODULE_H
#define MPI_MODULE_H

#include <cstddef>

/* We do not use the MPI C++ bindings because:
 * (1) they cause linker problems. 
 * (2) they are not part of MPI-3. */
#define MPICH_SKIP_MPICXX
#define OMPI_SKIP_MPICXX
#ifdef __cplusplus
extern "C" {
#endif
#include <mpi.h>
#ifdef __cplusplus
} /* extern "C" */
#endif

class MPIModule {
// TODO: Jeff to implement this and check everything I have done here.

 public:
  /** MPI environment constructor. */
  MPIModule() { MPIModule::Initialize(NULL, NULL, MPI_COMM_WORLD); }
  MPIModule(MPI_Comm comm) { MPIModule::Initialize(NULL, NULL, comm); }
  MPIModule(int * argc, char *** argv) { MPIModule::Initialize(argc, argv, MPI_COMM_WORLD); }
  MPIModule(int * argc, char *** argv, MPI_Comm comm) { MPIModule::Initialize(argc, argv, comm); }
  
  /** MPI environment destructor. */
  ~MPIModule() { MPIModule::Finalize(); }

  /** Returns the MPI rank of the machine. */
  int rank() { int rank; MPI_Comm_rank(this->comm_, &rank); return rank; }
  /** Returns the MPI size. */
  int size() { int size; MPI_Comm_size(this->comm_, &size); return size; }

 private:
  // PRIVATE METHODS
  void Initialize(int * argc, char *** argv, MPI_Comm comm);
  void Finalize();

  // PRIVATE ATTRIBUTES
  /** The MPI communicator associated with this MPI env */
  MPI_Comm comm_;
  /** The MPI dynamic window that will be used for moving data RVMA-style. */
  MPI_Win win_;
};

#endif
