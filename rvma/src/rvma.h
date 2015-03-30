#ifdef __cplusplus
extern "C" {
#endif

int rvma_initialize(MPI_Comm comm);
int rvma_finalize(void);

int rvma_capture(void * data, size_t bytes);
int rvma_release(void * data);

int rvma_get(int proc, void * addr, size_t bytes, void * local);
int rvma_put(void * local, int proc, void * addr, size_t bytes);

int rvma_flush(int proc);

int rvma_poll(void);

int rvma_is_same(void * ptr, int * is_same);

#ifdef __cplusplus
} /* extern "C" */
#endif
