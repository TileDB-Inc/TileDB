#ifdef __cplusplus
extern "C" {
#endif

#include "internals.h"

rvma_global_state_t RVMA_GLOBAL_STATE;

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_initialize = prvma_initialize
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_initialize rvma_initialize
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_initialize as prvma_initialize
#endif
/* -- end weak symbols block -- */

/** Initialize RVMA.  MPI must be initialized before this can be called.
  * It is invalid to make RVMA calls before initialization.
  * Collective on the group of the communicator provided.
  *
  * @return            Zero on success
  */

int rvma_initialize(MPI_Comm comm)
{
    int rc;

    /* RVMA requires MPI */
    int is_mpi_init;
    rc = MPI_Initialized(&is_mpi_init);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Initialized failed\n");
        return 1;
    }
    if (!is_mpi_init) {
        fprintf(stderr, "You must initialize MPI to use RVMA\n");
        return 1;
    } 

    /* Determine thread support and if mutex is required */
    int mpi_thread_level;
    rc = MPI_Query_thread(&mpi_thread_level);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Query_thread failed\n");
        return 1;
    }
    if (mpi_thread_level!=MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "You must initialize MPI with MULTIPLE\n");
        return 1;
    }

    /* Query for intercommunicator (unlikely) */
    int is_intercomm;
    rc = MPI_Comm_test_inter(comm, &is_intercomm);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Comm_test_inter failed\n");
        return 1;
    }
    if (is_intercomm) {
        fprintf(stderr, "You cannot initialize RVMA on an intercommunicator\n");
        return 1;
    } 

    /* Duplicate the users communicator to avoid any cross-talk. */
    rc = MPI_Comm_dup(comm, &(RVMA_GLOBAL_STATE.rvma_comm) );
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Comm_dup failed\n");
        return 1;
    }

    /* Create the global window to be used for all comms */
    MPI_Info win_info = MPI_INFO_NULL;
    rc = MPI_Info_create(&win_info);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Info_create failed\n");
        return 1;
    }
    /* Do not order atomic put */
    rc = MPI_Info_set(win_info,"accumulate_ordering","");
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Info_set failed\n");
        return 1;
    }
    /* Assume only REPLACE and NO_OP i.e. atomic put */
    rc = MPI_Info_set(win_info,"accumulate_ops","same_op_no_op");
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Info_set failed\n");
        return 1;
    }
    rc = MPI_Win_create_dynamic(win_info, RVMA_GLOBAL_STATE.rvma_comm,
                                &(RVMA_GLOBAL_STATE.rvma_win) );
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Win_create_dynamic failed\n");
        return 1;
    }
    rc = MPI_Info_free(&win_info);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Info_free failed\n");
        return 1;
    }

    /* Enter "PGAS mode" */
    rc = MPI_Win_lock_all(MPI_MODE_NOCHECK, RVMA_GLOBAL_STATE.rvma_win);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Win_lock_all failed\n");
        return 1;
    }

    return 0;
}

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_finalize = prvma_finalize
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_finalize rvma_finalize
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_finalize as prvma_finalize
#endif
/* -- end weak symbols block -- */

/** Finalize RVMA. 
  * It is invalid to make RVMA calls after finalization.
  * Collective on the group of the communicator provided.
  *
  * @return            Zero on success
  */

int rvma_finalize(void)
{
    int rc;

    /* Exit "PGAS mode" */
    rc = MPI_Win_unlock_all(RVMA_GLOBAL_STATE.rvma_win);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Win_unlock_all failed\n");
        return 1;
    }

    /* Free the window */
    MPI_Win_free( &(RVMA_GLOBAL_STATE.rvma_win) );
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Win_free failed\n");
        return 1;
    }

    /* Free the duplicated user comm */
    rc = MPI_Comm_free( &(RVMA_GLOBAL_STATE.rvma_comm) );
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Comm_free failed\n");
        return 1;
    }
    
    return 0;
}

#ifdef __cplusplus
} /* extern "C" */
#endif
