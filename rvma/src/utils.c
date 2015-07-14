#include "internals.h"

static inline int rvmai_is_same(void * ptr, int * is_same)
{
    int rc;

    MPI_Aint address;
    rc = MPI_Get_address(ptr, &address);

    MPI_Aint plusminus[2] = {address,-address};

    rc = MPI_Allreduce(MPI_IN_PLACE, plusminus, 2, MPI_AINT, MPI_MAX,
                       RVMA_GLOBAL_STATE.rvma_comm);
    if (rc!=MPI_SUCCESS) {
        fprintf(stderr,"MPI_Allreduce failed\n");
        return 1;
    }

    *is_same = ( (plusminus[0]==address) && (plusminus[1]==(-address))) ? 1 : 0;

    return 0;
}

/* -- begin weak symbols block -- */
#if defined(HAVE_PRAGMA_WEAK)
#  pragma weak rvma_is_same = prvma_is_same
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#  pragma _HP_SECONDARY_DEF prvma_is_same rvma_is_same
#elif defined(HAVE_PRAGMA_CRI_DUP)
#  pragma _CRI duplicate rvma_is_same as prvma_is_same
#endif
/* -- end weak symbols block -- */

/** Something
  *
  * @return            Zero on success
  */

int rvma_is_same(void * ptr, int * is_same)
{
    {
        return rvmai_is_same(ptr, is_same);
    }
}
