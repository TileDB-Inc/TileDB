/**
 * @file   mpi_handler.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the MPIHandler class.
 */

#include "mpi_handler.h"
#include <stdlib.h>

MPIHandler::MPIHandler(int* argc, char*** argv) {
  init(MPI_COMM_WORLD, argc, argv);
}

MPIHandler::MPIHandler(void) {
  init(MPI_COMM_WORLD, NULL, NULL);
}

MPIHandler::MPIHandler(int* argc, char*** argv, MPI_Comm comm) {
  init(comm, argc, argv);
}

MPIHandler::MPIHandler(MPI_Comm comm) {
  init(comm, NULL, NULL);
}

MPIHandler::~MPIHandler() {
  finalize();
}

void MPIHandler::finalize() {

  {
    int rc = MPI_Comm_free(&comm_);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Comm_free failed");
      MPI_Abort(comm_, rc);
    }
  }

  if (own_mpi_) {
    int rc = MPI_Finalize();
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Finalize failed");
      MPI_Abort(comm_, rc);
    }
  }
}

void MPIHandler::gather(void* send_data, int send_size, 
                        void*& rcv_data, int& rcv_size,
                        int root) const {
  // Receive size of data to be received from each process
  int* rcv_sizes = NULL;
  int* displs = NULL;

  if(comm_rank_ == root) {
    rcv_sizes = new int[comm_size_]; 
  }
 
  int rc = MPI_Gather(&send_size, 1, MPI_INT, 
                      rcv_sizes, 1, MPI_INT,
                      root, MPI_COMM_WORLD); 

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error gathering send sizes with MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
  }

  // Allocate receive data buffer and compute displacements 
  if(comm_rank_ == root) {
    displs = new int[comm_size_];
    rcv_size = 0;

    for(int i=0; i<comm_size_; ++i) {
      displs[i] = rcv_size;
      rcv_size += rcv_sizes[i];
    } 

    rcv_data = malloc(rcv_size);
  }
    
  // Receive the data from each process
  rc = MPI_Gatherv(send_data, send_size, MPI_CHAR, 
                   rcv_data, rcv_sizes, displs, MPI_CHAR,
                   root, MPI_COMM_WORLD); 

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error gathering data with MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
  }

  // Clean up
  if(comm_rank_ == root) {
    delete [] rcv_sizes;
    delete [] displs;
  }
}

void MPIHandler::init(MPI_Comm user_comm, int* argc, char*** argv) {

  int is_init;  
  {
    int rc = MPI_Initialized(&is_init);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Initialized failed");
      MPI_Abort(user_comm, rc);
    }
  }

  int thread_requested = MPI_THREAD_MULTIPLE;
  int thread_provided;
  if (is_init) {
    int rc = MPI_Query_thread(&thread_provided);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Init_thread failed");
      MPI_Abort(user_comm, rc);
    }
    if(thread_provided<thread_requested) {
      throw MPIHandlerException("MPI: insufficient thread level");
      MPI_Abort(user_comm, thread_provided);
    }
    own_mpi_ = 0;
  } else {
    int rc = MPI_Init_thread(argc, argv, thread_requested, 
                             &thread_provided);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Init_thread failed");
      MPI_Abort(user_comm, rc);
    }
    if(thread_provided<thread_requested) {
      throw MPIHandlerException("MPI: insufficient thread level");
      MPI_Abort(user_comm, thread_provided);
    }
    own_mpi_ = 1;
  }

  /* User cannot provide valid comm if MPI is not
   * initialized.  Ignore silly case of MPI_COMM_SELF. */
  if (!is_init && user_comm!=MPI_COMM_WORLD)
  {
    throw MPIHandlerException("Initialize MPI first!");
    MPI_Abort(user_comm, 1);
  }

  {
    int rc = MPI_Comm_dup(user_comm, &comm_);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Comm_dup failed");
      MPI_Abort(user_comm, rc);
    }
  }

  {
    int rc = MPI_Comm_size(comm_, &comm_size_);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Comm_size failed");
      MPI_Abort(user_comm, rc);
    }
  }

  {
    int rc = MPI_Comm_rank(comm_, &comm_rank_);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Comm_rank failed");
      MPI_Abort(user_comm, rc);
    }
  }

  return;
}


