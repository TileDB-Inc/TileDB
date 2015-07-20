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
