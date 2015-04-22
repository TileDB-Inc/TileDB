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
  init(argc, argv);
}

MPIHandler::~MPIHandler() {
  finalize();
}

void MPIHandler::finalize() {
  int rc = MPI_Finalize();

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error finalizing MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
  }
}

void MPIHandler::gather(void* send_data, int send_size, 
                        void*& rcv_data, int& rcv_size,
                        int root) const {
  // Receive size of data to be received from each process
  int* rcv_sizes = NULL;
  int* displs = NULL;

  if(rank_ == root) {
    rcv_sizes = new int[proc_num_]; 
  }
 
  int rc = MPI_Gather(&send_size, 1, MPI_INT, 
                      rcv_sizes, 1, MPI_INT,
                      root, MPI_COMM_WORLD); 

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error gathering send sizes with MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
  }

  // Allocate receive data buffer and compute displacements 
  if(rank_ == root) {
    displs = new int[proc_num_];
    rcv_size = 0;

    for(int i=0; i<proc_num_; ++i) {
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
  if(rank_ == root) {
    delete [] rcv_sizes;
    delete [] displs;
  }
}

void MPIHandler::init(int* argc, char*** argv) {
  int rc = MPI_Init(argc, argv);

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error initializing MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
  }

  MPI_Comm_size(MPI_COMM_WORLD, &proc_num_);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
}


