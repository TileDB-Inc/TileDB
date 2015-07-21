#include "rvma.h"

int main(int argc, char* argv[])
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    rvma_initialize(MPI_COMM_WORLD);

    rvma_finalize();
    MPI_Finalize();
    return 0;
}
