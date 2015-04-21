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

MPIHandler::MPIHandler(int* argc, char*** argv) {
  init(argc, argv);
}

MPIHandler::~MPIHandler() {
  finalize();
}

void MPIHandler::bcast(void* data, int size, int root) const {
  int rc = MPI_Bcast(data, size, MPI_CHAR, root, MPI_COMM_WORLD); 

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error broadcasting with MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
  }
}

void MPIHandler::finalize() {
  int rc = MPI_Finalize();

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error finalizing MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
  }
}

void MPIHandler::gather(void* send_data, int send_size, 
                        void* rcv_data, int rcv_size, 
                        int root) const {
  int rc = MPI_Gather(send_data, send_size, MPI_CHAR, 
                      rcv_data, rcv_size, MPI_CHAR,
                      root, MPI_COMM_WORLD); 

  if(rc != MPI_SUCCESS) {
    throw MPIHandlerException("Error gathering with MPI.");
    MPI_Abort(MPI_COMM_WORLD, rc);
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


