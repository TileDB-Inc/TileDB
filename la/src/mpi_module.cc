/**
 * @file   MPI_module.cc
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
 * This file implements class MPIModule.
 */

#include "mpi_module.h"

/* Determine if MPI initialized */
static inline int Init_thread(int * argc, char*** argv, int requested) {
  int provided;
  int rc = MPI_Init_thread(argc, argv, requested, &provided);
  if (rc!=MPI_SUCCESS)
    throw "MPI_Initialized";
  return provided;
}

/* Determine if MPI initialized */
static inline bool Is_init() {
  int is_mpi_init;
  int rc = MPI_Initialized(&is_mpi_init);
  if (rc!=MPI_SUCCESS)
    throw "MPI_Initialized";
  return is_mpi_init;
}

/* Determine thread support and if mutex is required */
static inline int Query_thread() {
  int mpi_thread_level;
  int rc = MPI_Query_thread(&mpi_thread_level);
  if (rc!=MPI_SUCCESS)
    throw "MPI_Query_thread";
  return mpi_thread_level;
}

/* Query for intercommunicator */
static inline bool Is_intercomm(MPI_Comm comm) {
  int is_intercomm;
  int rc = MPI_Comm_test_inter(comm, &is_intercomm);
  if (rc!=MPI_SUCCESS)
    throw "MPI_Comm_test_inter";
  return is_intercomm;
}

/** MPI environment constructor. */
void MPIModule::Initialize(int * argc, char *** argv, MPI_Comm comm) {

  int rc;

  if (Init_thread(argc, argv, MPI_THREAD_MULTIPLE) < MPI_THREAD_MULTIPLE)
    throw "Full MPI thread support required";

  if (!Is_intercomm(comm))
    throw  "Intercommunicators not supported";

  /* Duplicate the users communicator to avoid any cross-talk. */
  rc = MPI_Comm_dup(comm, &(this->comm_) );
  if (rc!=MPI_SUCCESS)
    throw "MPI_Comm_dup";

  rc = MPI_Win_create_dynamic(MPI_INFO_NULL, this->comm_, &(this->win_) );
  if (rc!=MPI_SUCCESS)
    throw "MPI_Win_create_dynamic";

  /* Enter "PGAS mode" */
  rc = MPI_Win_lock_all(MPI_MODE_NOCHECK, this->win_);
  if (rc!=MPI_SUCCESS)
    throw "MPI_Win_lock_all";
}

/** MPI environment destructor. */
void MPIModule::Finalize() {
  int rc;

  /* Exit "PGAS mode" */
  rc = MPI_Win_unlock_all(this->win_);
  if (rc!=MPI_SUCCESS)
    throw "MPI_Win_unlock_all";

  /* Free the window */
  MPI_Win_free( &(this->win_) );
  if (rc!=MPI_SUCCESS)
    throw "MPI_Win_free";

  /* Free the duplicated user comm */
  rc = MPI_Comm_free( &(this->comm_) );
  if (rc!=MPI_SUCCESS)
    throw "MPI_Comm_free";
}

