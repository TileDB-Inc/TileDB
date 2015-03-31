#include <cstdio>
#include <climits>

#ifdef __cplusplus
extern "C" {
#endif

/* We do not use nor do we want the C++ bindings... */
#define MPICH_SKIP_MPICXX
#define OMPI_SKIP_MPICXX
#include <mpi.h>

typedef struct {
    MPI_Comm        rvma_comm;
    MPI_Win         rvma_win;
} rvma_global_state_t;

extern rvma_global_state_t RVMA_GLOBAL_STATE;

#ifdef __cplusplus
} /* extern "C" */
#endif
