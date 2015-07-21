#include "rvma.h"

/* This will be C++ code until the C interface includes read_cells... */
#include "cell_iterator.h"
#include "loader.h"
#include "mpi_handler.h"
#include "query_processor.h"
#include "storage_manager.h"

int main(int argc, char* argv[])
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    rvma_initialize(MPI_COMM_WORLD);

    /* Create a storage manager module */
    StorageManager * storage_manager = new StorageManager("/tmp/", mpi_handler);

    rvma_finalize();
    MPI_Finalize();
    return 0;
}
