#ifndef TILEDB_FILESYSTEM_H
#define TILEDB_FILESYSTEM_H

#ifdef HAVE_MPI
#include <mpi.h>
#endif
#include <sys/types.h>
#include <string>
#include <vector>
#include "status.h"

namespace tiledb {

namespace filesystem {

/**
 * Creates a new directory.
 *
 * @param dir The name of the directory to be created.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
Status create_dir(const std::string& path);

/**
 * Returns the directory where the program is executed.
 *
 * @return The directory where the program is executed. If the program cannot
 *     retrieve the current working directory, the empty string is returned.
 */
std::string current_dir();

/**
 * Deletes a directory. Note that the directory must not contain other
 * directories, but it should only contain files.
 *
 * @param dirname The name of the directory to be deleted.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
Status delete_dir(const std::string& path);

/**
 * Returns the size of the input file.
 *
 * @param filename The name of the file whose size is to be retrieved.
 * @return The file size on success, and TILEDB_UT_ERR for error.
 */
Status file_size(const std::string& filename, off_t* path);

/** Returns the names of the directories inside the input directory.
 *
 * @param dir input directory path
 * @return a vector of string paths
 */
std::vector<std::string> get_dirs(const std::string& path);

/**
 * Checks if the input is an existing directory.
 *
 * @param dir The directory to be checked.
 * @return *true* if *dir* is an existing directory, and *false* otherwise.
 */
bool is_dir(const std::string& path);

/**
 * Checks if the input is an existing file.
 *
 * @param file The file to be checked.
 * @return tTrue* if *file* is an existing file, and *false* otherwise.
 */
bool is_file(const std::string& path);

/**
 * It takes as input an **absolute** path, and returns it in its canonicalized
 * form, after appropriately replacing "./" and "../" in the path.
 *
 * @param path The input path passed by reference, which will be modified
 *     by the function to hold the canonicalized absolute path. Note that the
 *     path must be absolute, otherwise the function fails. In case of error
 *     (e.g., if "../" are not properly used in *path*, or if *path* is not
 *     absolute), the function sets the empty string (i.e., "") to *path*.
 * @return void
 */
void purge_dots_from_path(std::string& path);

/**
 * Reads data from a file into a buffer.
 *
 * @param filename The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
Status read_from_file(
    const std::string& filename, off_t offset, void* buffer, size_t length);

// TODO: this should go away
/** Returns the names of the fragments inside the input directory. */
std::vector<std::string> get_fragment_dirs(const std::string& dir);

/**
 * Creates a special file to indicate that the input directory is a
 * TileDB fragment.
 *
 * @param dir The name of the fragment directory where the file is created.
 * @return Status
 */
Status create_fragment_file(const std::string& dir);

/**
 * Reads data from a file into a buffer, using memory map (mmap).
 *
 * @param filename The name of the file
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
Status read_from_file_with_mmap(
    const std::string& path, off_t offset, void* buffer, size_t length);

/**
 * Returns the absolute canonicalized directory path of the input directory.
 *
 * @param dir The input directory to be canonicalized.
 * @return The absolute canonicalized directory path of the input directory.
 */
std::string real_dir(const std::string& path);

/**
 * Syncs a file or directory. If the file/directory does not exist,
 * the function gracefully exits (i.e., it ignores the syncing).
 *
 * @param filename The name of the file.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
Status sync(const std::string& path);

/**
 * Writes the input buffer to a file.
 *
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
Status write_to_file(
    const std::string& path, const void* buffer, size_t buffer_size);

/**
 * Write the input buffer to a file, compressed with GZIP.
 *
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
Status write_to_file_cmp_gzip(
    const std::string& path, const void* buffer, size_t buffer_size);

#ifdef HAVE_MPI
/**
 * Reads data from a file into a buffer using MPI-IO.
 *
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int mpi_io_read_from_file(
    const MPI_Comm* mpi_comm,
    const std::string& path,
    off_t offset,
    void* buffer,
    size_t length);

/**
 * Syncs a file or directory using MPI-IO. If the file/directory does not exist,
 * the function gracefully exits (i.e., it ignores the syncing).
 *
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int mpi_io_sync(const MPI_Comm* mpi_comm, const char* path);

/**
 * Writes the input buffer to a file using MPI-IO.
 *
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int mpi_io_write_to_file(
    const MPI_Comm* mpi_comm,
    const char* path,
    const void* buffer,
    size_t buffer_size);
#endif

}  // namespace filesystem

}  // namespace tiledb

#endif  // TILEDB_FILESYSTEM_H
