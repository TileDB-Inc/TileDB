#include <stdio.h>
#include <limits.h>

#include <mpi.h>

typedef struct {
    MPI_Comm        rvma_comm;
    MPI_Win         rvma_win;
} rvma_global_state_t;

extern rvma_global_state_t RVMA_GLOBAL_STATE;
