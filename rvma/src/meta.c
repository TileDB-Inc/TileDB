#include "internals.h"

void rvma_am_flush(int remote_proc)
{
    rvma_msg_info_t info = { .type    = RVMA_MSG_FLUSH,
                             .address = NULL,
                             .count   = 0,
                             .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(rvma_msg_info_t), MPI_BYTE, remote_proc, RVMA_MSG_INFO_TAG, (RVMA_GLOBAL_STATE.rvma_comm));
    MPI_Recv(NULL, info.count, info.dt, remote_proc, RVMA_MSG_FLUSH_TAG, (RVMA_GLOBAL_STATE.rvma_comm), MPI_STATUS_IGNORE);
}

void rvma_am_get_raw(void* output, void * remote_input, int size, int remote_proc)
{
    rvma_msg_info_t info = { .type    = RVMA_MSG_GET_RAW,
                             .address = remote_input,
                             .count   = size,
                             .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(rvma_msg_info_t), MPI_BYTE, remote_proc, RVMA_MSG_INFO_TAG, (RVMA_GLOBAL_STATE.rvma_comm));

    MPI_Status status;
    MPI_Recv(output, info.count, info.dt, remote_proc, RVMA_MSG_GET_RAW_TAG, (RVMA_GLOBAL_STATE.rvma_comm), &status);

    int rcount;
    MPI_Get_count(&status, MPI_BYTE, &rcount);
    if (info.count != rcount) {
      printf("Get_raw message underflow");
      MPI_Abort((RVMA_GLOBAL_STATE.rvma_comm), info.count-rcount);
    }
}

void rvma_am_put_raw(void* input, void * remote_output, int size, int remote_proc)
{
    rvma_msg_info_t info = { .type    = RVMA_MSG_PUT_RAW,
                             .address = remote_output,
                             .count   = size,
                             .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(rvma_msg_info_t), MPI_BYTE, remote_proc, RVMA_MSG_INFO_TAG, (RVMA_GLOBAL_STATE.rvma_comm));
    MPI_Send(input, info.count, info.dt, remote_proc, RVMA_MSG_PUT_RAW_TAG, (RVMA_GLOBAL_STATE.rvma_comm));
}

void rvma_am_put_index(void* input, void * remote_output, int size, int remote_proc)
{
    /* TODO pack metadata for lookup... */
    rvma_msg_info_t info = { .type    = RVMA_MSG_PUT_INDEX,
                             .address = remote_output,
                             .count   = size,
                             .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(rvma_msg_info_t), MPI_BYTE, remote_proc, RVMA_MSG_INFO_TAG, (RVMA_GLOBAL_STATE.rvma_comm));
    MPI_Send(input, info.count, info.dt, remote_proc, RVMA_MSG_PUT_INDEX_TAG, (RVMA_GLOBAL_STATE.rvma_comm));
}

void rvma_am_get_index(void* output, void * remote_input, int size, int remote_proc)
{
    /* TODO pack metadata for lookup... */
    rvma_msg_info_t info = { .type    = RVMA_MSG_GET_INDEX,
                             .address = remote_input,
                             .count   = size,
                             .dt      = MPI_BYTE };
    MPI_Send(&info, sizeof(rvma_msg_info_t), MPI_BYTE, remote_proc, RVMA_MSG_INFO_TAG, (RVMA_GLOBAL_STATE.rvma_comm));

    MPI_Status status;
    MPI_Recv(output, info.count, info.dt, remote_proc, RVMA_MSG_GET_INDEX_TAG, (RVMA_GLOBAL_STATE.rvma_comm), &status);

    int rcount;
    MPI_Get_count(&status, MPI_BYTE, &rcount);
    if (info.count != rcount) {
      printf("Get_index message underflow");
      MPI_Abort((RVMA_GLOBAL_STATE.rvma_comm), info.count-rcount);
    }
}
