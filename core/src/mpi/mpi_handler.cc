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
#include <cstdlib>

typedef enum
{
    MSG_INFO_TAG,
    MSG_FLUSH_TAG,
    //MSG_GET_INDEX_TAG,
    //MSG_PUT_INDEX_TAG,
    MSG_GET_RAW_TAG,
    MSG_PUT_RAW_TAG,
    MSG_LAST_TAG
}
msg_tag_e;

typedef enum
{
    MSG_FLUSH,
    //MSG_GET_INDEX,
    //MSG_PUT_INDEX,
    MSG_GET_RAW,
    MSG_PUT_RAW,
    MSG_CHT_EXIT,
    MSG_LAST
}
msg_type_e;

typedef struct
{
    msg_type_e   type;
    void *       address;
    int          count;
    MPI_Datatype dt;
}
msg_info_t;

void * Poll(void * ptr) {

  MPI_Comm comm = *(MPI_Comm*)ptr;

  int rank;
  MPI_Comm_rank(comm, &rank);

  while (1) {

    msg_info_t info;
    MPI_Status status;
    MPI_Recv(&info, sizeof(info), MPI_BYTE,
             MPI_ANY_SOURCE, MSG_INFO_TAG, comm,
             &status);
    int source = status.MPI_SOURCE;

    /* poll stuff here */
    switch (info.type)
    {
        case MSG_FLUSH:
            MPI_Send(NULL, 0, MPI_BYTE, source, MSG_FLUSH_TAG, comm);
            break;

        case MSG_GET_RAW:
            MPI_Send(info.address, info.count, info.dt, source, MSG_GET_RAW_TAG, comm);
            break;

        case MSG_PUT_RAW:
            MPI_Status rstatus;
            MPI_Recv(info.address, info.count, info.dt, source, MSG_PUT_RAW_TAG, comm, &rstatus);
            int rcount;
            MPI_Get_count(&rstatus, MPI_BYTE, &rcount);
            if (info.count != rcount) {
              throw MPIHandlerException("CHT PUT message underflow");
              MPI_Abort(comm, info.count-rcount);
            }
            break;

#if 0 /* This does not appear to be the right way to do this. */
        case MSG_GET_INDEX:
            /* lookup data */

            // Read local cells in the range
            int ad;
            void* range; /* This is a template argument... */
            std::vector<int> attribute_ids;
            void* local_cells;
            size_t local_cells_size;
            storage_manager->read_cells(ad, range, attribute_ids, local_cells, local_cells_size);

            MPI_Send(info.address, info.count, info.dt, source, MSG_GET_RAW_TAG, comm);
            break;

        case MSG_PUT_INDEX:
            /* lookup data */
            MPI_Status rstatus;
            MPI_Recv(info.address, info.count, info.dt, source, MSG_PUT_RAW_TAG, comm, &rstatus);
            int rcount;
            MPI_Get_count(&rstatus, MPI_BYTE, &rcount);
            if (info.count != rcount) {
              throw MPIHandlerException("CHT PUT message underflow");
              MPI_Abort(comm, info.count-rcount);
            }
            break;
#endif

        case MSG_CHT_EXIT:
            if (rank!=source) {
              throw MPIHandlerException("CHT received EXIT signal from a rank besides self");
              MPI_Abort(comm, source);
            }
            pthread_exit(NULL);
            break;

        default:
            throw MPIHandlerException("CHT received invalid MSG TAG");
            MPI_Abort(comm, info.type);
            break;
    }
  }
  return NULL;
}

void MPIHandler::Start(void) {
  int rc = pthread_create(&comm_thread_, NULL, &Poll, &comm_);
  if(rc != 0) {
    throw MPIHandlerException("pthread_create failed");
    MPI_Abort(comm_, rc);
  }
}

/** Tells the comm thread to exit. */
void MPIHandler::Stop(void) {
  int rank;
  MPI_Comm_rank(comm_, &rank);

  msg_info_t info;
  info.type = MSG_CHT_EXIT;

  /* Synchronous send will wait until the receive is matched,
   * so we know that the thread has received the exit command. */
  MPI_Ssend(&info, sizeof(msg_info_t), MPI_BYTE, rank, MSG_INFO_TAG, comm_);
  void * rv; /* unused; thread returns NULL */
  int rc = pthread_join(comm_thread_, &rv);
  if (rc!=0) {
    throw MPIHandlerException("pthread_join failed");
    MPI_Abort(comm_, rc);
  }
}

void MPIHandler::gather(void* send_data, size_t send_size,
                        void*& rcv_data, size_t& rcv_size,
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

void MPIHandler::Flush(int remote_proc) const
{
    /* Verify that C99 struct initialization is fully compliant with ISO C++... */
    msg_info_t info = { .type    = MSG_FLUSH,
                        .address = NULL,
                        .count   = 0,
                        .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(msg_info_t), MPI_BYTE, remote_proc, MSG_INFO_TAG, comm_);
    MPI_Recv(NULL, info.count, info.dt, remote_proc, MSG_FLUSH_TAG, comm_, MPI_STATUS_IGNORE);
}

void MPIHandler::Get_raw(void* output, void * remote_input, int size, int remote_proc) const
{
    /* Verify that C99 struct initialization is fully compliant with ISO C++... */
    msg_info_t info = { .type    = MSG_GET_RAW,
                        .address = remote_input,
                        .count   = size,
                        .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(msg_info_t), MPI_BYTE, remote_proc, MSG_INFO_TAG, comm_);

    MPI_Status status;
    MPI_Recv(output, info.count, info.dt, remote_proc, MSG_GET_RAW_TAG, comm_, &status);

    int rcount;
    MPI_Get_count(&status, MPI_BYTE, &rcount);
    if (info.count != rcount) {
      throw MPIHandlerException("Get_raw message underflow");
      MPI_Abort(comm_, info.count-rcount);
    }
}

void MPIHandler::Get_raw_many(int count, void* output[], void * remote_input[], int size[], int remote_proc[]) const
{
    for (int i=0; i<count; i++) {
        MPIHandler::Get_raw(output[i], remote_input[i], size[i], remote_proc[i]);
    }
}

void MPIHandler::Put_raw(void* input, void * remote_output, int size, int remote_proc) const
{
    /* Verify that C99 struct initialization is fully compliant with ISO C++... */
    msg_info_t info = { .type    = MSG_PUT_RAW,
                        .address = remote_output,
                        .count   = size,
                        .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(msg_info_t), MPI_BYTE, remote_proc, MSG_INFO_TAG, comm_);
    MPI_Send(input, info.count, info.dt, remote_proc, MSG_PUT_RAW_TAG, comm_);
}

void MPIHandler::Put_raw_many(int count, void* input[], void * remote_output[], int size[], int remote_proc[]) const
{
    for (int i=0; i<count; i++) {
        MPIHandler::Put_raw(input[i], remote_output[i], size[i], remote_proc[i]);
    }
}

#if 0
void MPIHandler::Put_index(void* input, void * remote_output, int size, int remote_proc) const
{
    /* TODO pack metadata for lookup... */
    msg_info_t info = { .type    = MSG_PUT_INDEX,
                        .address = remote_output,
                        .count   = size,
                        .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(msg_info_t), MPI_BYTE, remote_proc, MSG_INFO_TAG, comm_);
    MPI_Send(input, info.count, info.dt, remote_proc, MSG_PUT_INDEX_TAG, comm_);
}

void MPIHandler::Get_index(void* output, void * remote_input, int size, int remote_proc) const
{
    /* TODO pack metadata for lookup... */
    msg_info_t info = { .type    = MSG_GET_INDEX,
                        .address = remote_input,
                        .count   = size,
                        .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(msg_info_t), MPI_BYTE, remote_proc, MSG_INFO_TAG, comm_);

    MPI_Status status;
    MPI_Recv(output, info.count, info.dt, remote_proc, MSG_GET_INDEX_TAG, comm_, &status);

    int rcount;
    MPI_Get_count(&status, MPI_BYTE, &rcount);
    if (info.count != rcount) {
      throw MPIHandlerException("Get_index message underflow");
      MPI_Abort(comm_, info.count-rcount);
    }
}
#endif

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
    /* Query for intercommunicator (unlikely) */
    int is_intercomm;
    int rc = MPI_Comm_test_inter(user_comm, &is_intercomm);
    if (rc!=MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Comm_test_inter failed");
      MPI_Abort(user_comm, 1);
    }
    if (is_intercomm) {
      throw MPIHandlerException("Intercommunicators not okay");
      MPI_Abort(user_comm, 1);
    }
  }

  /* Duplicate the users communicator to avoid any cross-talk. */
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
      MPI_Abort(comm_, rc);
    }
  }

  {
    int rc = MPI_Comm_rank(comm_, &comm_rank_);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Comm_rank failed");
      MPI_Abort(comm_, rc);
    }
  }

  {
    /* Create the global window to be used for all comms */
    MPI_Info win_info = MPI_INFO_NULL;
    int rc = MPI_Info_create(&win_info);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Info_create failed");
      MPI_Abort(comm_, rc);
    }

    /* Do not order atomic put */
    rc = MPI_Info_set(win_info,"accumulate_ordering","");
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Info_set failed");
      MPI_Abort(comm_, rc);
    }
    /* Assume only REPLACE and NO_OP i.e. atomic put */
    rc = MPI_Info_set(win_info,"accumulate_ops","same_op_no_op");
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Info_set failed");
      MPI_Abort(comm_, rc);
    }
    rc = MPI_Win_create_dynamic(win_info, comm_, &win_ );
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Win_create_dynamic failed");
      MPI_Abort(comm_, rc);
    }
    rc = MPI_Info_free(&win_info);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Info_free failed");
      MPI_Abort(comm_, rc);
    }

    /* Enter "PGAS mode" */
    rc = MPI_Win_lock_all(MPI_MODE_NOCHECK, win_);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Win_lock_all failed");
      MPI_Abort(comm_, rc);
    }
  }

  Start();

  return;
}

void MPIHandler::finalize(void) {

  Stop();

  {
    /* Exit "PGAS mode" */
    int rc = MPI_Win_unlock_all(win_);
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Win_unlock_all failed");
      MPI_Abort(comm_, rc);
    }
  }

  {
    int rc = MPI_Win_free(&win_ );
    if(rc != MPI_SUCCESS) {
      throw MPIHandlerException("MPI_Win_free failed");
      MPI_Abort(comm_, rc);
    }
  }

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

