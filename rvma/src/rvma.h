#ifndef INCLUDE_RVMA_H
#define INCLUDE_RVMA_H

#ifdef __cplusplus
#include <cstddef>
extern "C" {
#else
#include <stddef.h>
#endif

#include <mpi.h>

int rvma_initialize(MPI_Comm comm);
int rvma_finalize(void);

int rvma_capture(void * data, size_t bytes);
int rvma_release(void * data);

int rvma_get(int proc, void * addr, size_t bytes, void * local);
int rvma_put(void * local, int proc, void * addr, size_t bytes);

int rvma_flush(int proc);

#ifdef __cplusplus
}
#endif

#endif // INCLUDE_RVMA_H
