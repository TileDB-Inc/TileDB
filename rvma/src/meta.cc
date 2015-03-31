#ifdef __cplusplus
extern "C" {
#endif

#include "internals.h"

int rvma_poll(void)
{
    int rc;
    MPI_Message message;
    MPI_Status  status;

    /* See if a message has been sent. */
    rc = MPI_Mprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, RVMA_GLOBAL_STATE.rvma_comm,
                    &message, &status);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Mprobe failed\n");
        return 1;
    }

    int source = status.MPI_SOURCE;
    int tag    = status.MPI_TAG;
    int statrc = status.MPI_ERROR;
    if (statrc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Mprobe status error\n");
        return 1;
    }

    /* Determine how many bytes are in this message. */
    int pcount; /* i.e. probe count */
    rc = MPI_Get_elements(&status, MPI_BYTE, &pcount);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Get_elements failed\n");
        return 1;
    }

    void * buffer = NULL;
    rc = MPI_Alloc_mem((MPI_Aint)pcount, MPI_INFO_NULL, &buffer);
    if (rc!=MPI_SUCCESS || buffer==NULL) {
        fprintf(stderr,"MPI_Alloc_mem failed\n");
        return 1;
    }

    rc = MPI_Mrecv(buffer, pcount, MPI_BYTE, &message, &status);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Mrecv failed\n");
        return 1;
    }

    int rcount; /* i.e. receive count */
    rc = MPI_Get_elements(&status, MPI_BYTE, &rcount);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Get_elements failed\n");
        return 1;
    }

    if (rcount!=pcount) {
        fprintf(stderr,"MPI_Mrecv status count less than MPI_Mprobe status count\n");
        return 1;
    }

    /* TODO process message */

    rc = MPI_Free_mem(buffer);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Free_mem failed\n");
        return 1;
    }

    return 0;
}

#ifdef __cplusplus
} /* extern "C" */
#endif
