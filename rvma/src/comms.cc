#include "internals.h"

#ifdef __cplusplus
extern "C" {
#endif

/********************************************/

static inline int rvmai_capture(void * data, size_t bytes)
{
    int rc;

    MPI_Aint size = (MPI_Aint)bytes;
    
    rc = MPI_Win_attach(RVMA_GLOBAL_STATE.rvma_win, data, size);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Win_attach failed\n");
        return 1;
    }

    return 0;
}

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_capture = prvma_capture
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_capture rvma_capture
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_capture as prvma_capture
#endif
/* -- end weak symbols block -- */

/** Something
  *
  * @return            Zero on success
  */

int rvma_capture(void * data, size_t bytes)
{
    {
        return rvmai_capture(data, bytes);
    }
}

static inline int rvmai_release(void * data)
{
    int rc;

    rc = MPI_Win_detach(RVMA_GLOBAL_STATE.rvma_win, data);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Win_detach failed\n");
        return 1;
    }

    return 0;
}

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_release = prvma_release
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_release rvma_release
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_release as prvma_release
#endif
/* -- end weak symbols block -- */

/** Something
  *
  * @return            Zero on success
  */

int rvma_release(void * data)
{
    {
        return rvmai_release(data);
    }
}

/********************************************/

static inline int rvmai_get(int proc, void * remote, size_t bytes, void * local)
{
    int rc;

    /* TODO Bring in the appropriate BigMPI business to allow this
     * routine to support more than INT_MAX bytes.*/

    if (bytes > (size_t)INT_MAX) {
        fprintf(stderr,"Transfers of more than %llu bytes are not supported.\n", (long long unsigned)INT_MAX);
        return 1;
    }

    MPI_Aint address;
    rc = MPI_Get_address(remote, &address);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Get_address failed\n");
        return 1;
    }

    int count = (int)bytes;
    rc = MPI_Get(local, count, MPI_BYTE,
                 proc,  address, count, MPI_BYTE,
                 RVMA_GLOBAL_STATE.rvma_win);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Get failed\n");
        return 1;
    }
    return 0;
}

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_get = prvma_get
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_get rvma_get
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_get as prvma_get
#endif
/* -- end weak symbols block -- */

/** Something
  *
  * @return            Zero on success
  */

int rvma_get(int proc, void * addr, size_t bytes, void * local)
{
    {
        return rvmai_get(proc, addr, bytes, local);
    }
}

static inline int rvmai_put(void * local, int proc, void * remote, size_t bytes)
{
    int rc;

    /* TODO Bring in the appropriate BigMPI business to allow this
     * routine to support more than INT_MAX bytes.*/

    if (bytes > (size_t)INT_MAX) {
        fprintf(stderr,"Transfers of more than %llu bytes are not supported.\n", (long long unsigned)INT_MAX);
        return 1;
    }

    MPI_Aint address;
    rc = MPI_Get_address(remote, &address);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Get_address failed\n");
        return 1;
    }

    int count = (int)bytes;
    rc = MPI_Put(local, count, MPI_BYTE,
                 proc,  address, count, MPI_BYTE,
                 RVMA_GLOBAL_STATE.rvma_win);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Put failed\n");
        return 1;
    }
    return 0;
}

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_put = prvma_put
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_put rvma_put
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_put as prvma_put
#endif
/* -- end weak symbols block -- */

/** Something
  *
  * @return            Zero on success
  */

int rvma_put(void * local, int proc, void * addr, size_t bytes)
{
    {
        return rvmai_put(local, proc, addr, bytes);
    }
}

/********************************************/

static inline int rvmai_flush(int proc)
{
    int rc;

    rc = MPI_Win_flush(proc, RVMA_GLOBAL_STATE.rvma_win);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Win_flush failed\n");
        return 1;
    }
    return 0;
}

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_flush = prvma_flush
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_flush rvma_flush
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_flush as prvma_flush
#endif
/* -- end weak symbols block -- */

/** Something
  *
  * @return            Zero on success
  */

int rvma_flush(int proc)
{
    {
        return rvmai_flush(proc);
    }
}

#ifdef __cplusplus
} /* extern "C" */
#endif
