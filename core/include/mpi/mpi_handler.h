/**
 * @file   mpi_handler.h
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
 * This file defines class MPIHandler. It also defines
 * MPIHandlerException, which is thrown by MPIHandler.
 */

#ifndef MPI_HANDLER_H
#define MPI_HANDLER_H

#include <string>
#include <mpi.h>

#include <cstdlib>

#include <iostream>

#include <atomic>
#include <chrono>
#include <functional>
#include <thread>

/* THIS IS NOT THE RIGHT WAY TO DO THIS IN C++11,
 * but since C++11 threads are designed to write
 * cute blog posts about but not actually do serious
 * work, we will do it the C way (i.e. globals). */

extern std::atomic<bool> comm_thread_active_;

/* This must be static for thread constructor to work.
 * Because it is static, it cannot access members. */
static void Poll(void);

/** This modules is responsible for the MPI communication across multiple
 *  processes.
 */
class MPIHandler {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  MPIHandler(int* argc, char*** argv);
  MPIHandler(void);
  MPIHandler(int* argc, char*** argv, MPI_Comm comm);
  MPIHandler(MPI_Comm comm);

  /** Destructor. */
  ~MPIHandler();

  // ACCESSORS
  /** Returns the process rank. */
  int rank() const { return comm_rank_; }
  /** Returns the number of processes. */
  int proc_num() const { return comm_size_; }

  // INITIALIZATION & FINALIZATION
  /** Finalize MPI. */
  void finalize();
  /** Initialize MPI. */
  void init(MPI_Comm comm, int* argc, char*** argv);

  // COMMUNICATION
  /**
   * The root process gathers data from all processes (including the root),
   * which send data.
   */
  void gather(void* send_data, int send_size,
              void*& rcv_data, int& rcv_size,
              int root) const;

  void Start(void) {
    comm_thread_active_ = true;
    comm_thread_ = std::thread(Poll);
  }

  void Stop(void) {
    comm_thread_active_ = false;
    comm_thread_.join();
  }

 private:
  /** TileDB is responsible for init/final of MPI */
  int own_mpi_;
  /** TileDB communicator */
  MPI_Comm comm_;
  /** Number of processes. */
  int comm_size_;
  /** Rank of this process. */
  int comm_rank_;
  /** TileDB RMA window */
  MPI_Win win_;

  std::thread comm_thread_;
};

/** This exception is thrown by MPIHandler. */
class MPIHandlerException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  MPIHandlerException(const std::string& msg)
      : msg_(msg) {}
  /** Empty destructor. */
  ~MPIHandlerException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }

 private:
  /** The exception message. */
  std::string msg_;
};

#endif
