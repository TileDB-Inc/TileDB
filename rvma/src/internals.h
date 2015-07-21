#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>

#include <mpi.h>

typedef struct {
    MPI_Comm        rvma_comm;
    MPI_Win         rvma_win;
    pthread_t       rvma_thread;
    bool            rvma_owns_mpi;
} rvma_global_state_t;

extern rvma_global_state_t RVMA_GLOBAL_STATE;

typedef enum
{
    RVMA_MSG_INFO_TAG,
    RVMA_MSG_FLUSH_TAG,
    RVMA_MSG_GET_INDEX_TAG,
    RVMA_MSG_PUT_INDEX_TAG,
    RVMA_MSG_GET_RAW_TAG,
    RVMA_MSG_PUT_RAW_TAG,
    RVMA_MSG_LAST_TAG
}
rvma_msg_tag_e;

typedef enum
{
    RVMA_MSG_FLUSH,
    RVMA_MSG_GET_INDEX,
    RVMA_MSG_PUT_INDEX,
    RVMA_MSG_GET_RAW,
    RVMA_MSG_PUT_RAW,
    RVMA_MSG_CHT_EXIT,
    RVMA_MSG_LAST
}
rvma_msg_type_e;

typedef struct
{
    rvma_msg_type_e  type;
    void *           address;
    int              count;
    MPI_Datatype     dt;
}
rvma_msg_info_t;

