#include "filesystem.h"
#include "configurator.h"
#include "logger.h"
#include "utils.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

namespace tiledb {

namespace filesystem {

Status create_dir(const std::string& path) {
  // Get real directory path
  std::string real_dir = filesystem::real_dir(path);

  // If the directory does not exist, create it
  if (filesystem::is_dir(real_dir)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory '") + real_dir +
        "'; Directory already exists"));
  }
  if (mkdir(real_dir.c_str(), S_IRWXU)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory '") + real_dir + "'; " +
        strerror(errno)));
  };
  return Status::Ok();
}

std::string current_dir() {
  std::string dir = "";
  char* path = getcwd(nullptr, 0);
  if (path != nullptr) {
    dir = path;
    free(path);
  }
  return dir;
}

Status delete_dir(const std::string& path) {
  // Get real path
  std::string dirname_real = filesystem::real_dir(path);

  // Delete the contents of the directory
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(dirname_real.c_str());

  if (dir == nullptr) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot open directory; ") + strerror(errno)));
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, ".."))
      continue;
    filename = dirname_real + "/" + next_file->d_name;
    if (remove(filename.c_str())) {
      return LOG_STATUS(Status::OSError(
          std::string("Cannot delete file; ") + strerror(errno)));
    }
  }

  // Close directory
  if (closedir(dir)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot close directory; ") + strerror(errno)));
  }

  // Remove directory
  if (rmdir(dirname_real.c_str())) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot delete directory; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status file_size(const std::string& path, off_t* size) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(
        Status::OSError("Cannot get file size; File opening error"));
  }

  struct stat st;
  fstat(fd, &st);
  *size = st.st_size;

  close(fd);
  return Status::Ok();
}

std::vector<std::string> get_dirs(const std::string& path) {
  std::vector<std::string> dirs;
  std::string new_dir;
  struct dirent* next_file;
  DIR* c_dir = opendir(path.c_str());

  if (c_dir == nullptr)
    return std::vector<std::string>();

  while ((next_file = readdir(c_dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !filesystem::is_dir(path + "/" + next_file->d_name))
      continue;
    new_dir = path + "/" + next_file->d_name;
    dirs.push_back(new_dir);
  }

  // Close array directory
  closedir(c_dir);

  // Return
  return dirs;
}

bool is_dir(const std::string& path) {
  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool is_file(const std::string& path) {
  struct stat st;
  return (stat(path.c_str(), &st) == 0) && !S_ISDIR(st.st_mode);
}

// TODO: path is getting modified here, don't pass by reference
void purge_dots_from_path(std::string& path) {
  // For easy reference
  size_t path_size = path.size();

  // Trivial case
  if (path_size == 0 || path == "/")
    return;

  // It expects an absolute path
  assert(path[0] == '/');

  // Tokenize
  const char* token_c_str = path.c_str() + 1;
  std::vector<std::string> tokens, final_tokens;
  std::string token;

  for (size_t i = 1; i < path_size; ++i) {
    if (path[i] == '/') {
      path[i] = '\0';
      token = token_c_str;
      if (token != "")
        tokens.push_back(token);
      token_c_str = path.c_str() + i + 1;
    }
  }
  token = token_c_str;
  if (token != "")
    tokens.push_back(token);

  // Purge dots
  int token_num = tokens.size();
  for (int i = 0; i < token_num; ++i) {
    if (tokens[i] == ".") {  // Skip single dots
      continue;
    } else if (tokens[i] == "..") {
      if (final_tokens.size() == 0) {
        // Invalid path
        path = "";
        return;
      } else {
        final_tokens.pop_back();
      }
    } else {
      final_tokens.push_back(tokens[i]);
    }
  }

  // Assemble final path
  path = "/";
  int final_token_num = final_tokens.size();
  for (int i = 0; i < final_token_num; ++i)
    path += ((i != 0) ? "/" : "") + final_tokens[i];
}

// TODO: this should be generic
std::vector<std::string> get_fragment_dirs(const std::string& path) {
  std::vector<std::string> dirs;
  std::string new_dir;
  struct dirent* next_file;
  DIR* c_dir = opendir(path.c_str());

  if (c_dir == nullptr)
    return std::vector<std::string>();

  while ((next_file = readdir(c_dir))) {
    new_dir = path + "/" + next_file->d_name;

    if (utils::is_fragment(new_dir))
      dirs.push_back(new_dir);
  }

  // Close array directory
  closedir(c_dir);

  // Return
  return dirs;
}

Status create_fragment_file(const std::string& path) {
  // Create the special fragment file
  std::string filename = path + "/" + Configurator::fragment_filename();
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1 || ::close(fd)) {
    return LOG_STATUS(Status::OSError(
        std::string("Failed to create fragment file; ") + strerror(errno)));
  }
  return Status::Ok();
};

Status read_from_file(
    const std::string& path, off_t offset, void* buffer, size_t length) {
  // Open file
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(
        Status::OSError("Cannot read from file; File opening error"));
  }
  // Read
  lseek(fd, offset, SEEK_SET);
  ssize_t bytes_read = ::read(fd, buffer, length);
  if (bytes_read != ssize_t(length)) {
    return LOG_STATUS(
        Status::IOError("Cannot read from file; File reading error"));
  }
  // Close file
  if (close(fd)) {
    return LOG_STATUS(
        Status::OSError("Cannot read from file; File closing error"));
  }
  return Status::Ok();
}

Status read_from_file_with_mmap(
    const std::string& path, off_t offset, void* buffer, size_t length) {
  // Calculate offset considering the page size
  size_t page_size = sysconf(_SC_PAGE_SIZE);
  off_t start_offset = (offset / page_size) * page_size;
  size_t extra_offset = offset - start_offset;
  size_t new_length = length + extra_offset;

  // Open file
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(
        Status::OSError("Cannot read from file; File opening error"));
  }

  // Map
  void* addr =
      mmap(nullptr, new_length, PROT_READ, MAP_SHARED, fd, start_offset);
  if (addr == MAP_FAILED) {
    return LOG_STATUS(
        Status::MMapError("Cannot read from file; Memory map error"));
  }

  // Give advice for sequential access
  if (madvise(addr, new_length, MADV_SEQUENTIAL)) {
    return LOG_STATUS(
        Status::MMapError("Cannot read from file; Memory advice error"));
  }

  // Copy bytes
  memcpy(buffer, static_cast<char*>(addr) + extra_offset, length);

  // Close file
  if (close(fd)) {
    return LOG_STATUS(
        Status::OSError("Cannot read from file; File closing error"));
  }

  // Unmap
  if (munmap(addr, new_length)) {
    return LOG_STATUS(
        Status::MMapError("Cannot read from file; Memory unmap error"));
  }

  return Status::Ok();
}

bool both_slashes(char a, char b) {
  return a == '/' && b == '/';
}

void adjacent_slashes_dedup(std::string& value) {
  value.erase(
      std::unique(value.begin(), value.end(), both_slashes), value.end());
}

std::string real_dir(const std::string& path) {
  // Initialize current, home and root
  std::string current = filesystem::current_dir();
  auto env_home_ptr = getenv("HOME");
  std::string home = env_home_ptr ? env_home_ptr : current;
  std::string root = "/";

  // Easy cases
  if (path == "" || path == "." || path == "./")
    return current;
  else if (path == "~")
    return home;
  else if (path == "/")
    return root;

  // Other cases
  std::string ret_dir;
  if (utils::starts_with(path, "/"))
    ret_dir = root + path;
  else if (utils::starts_with(path, "~/"))
    ret_dir = home + path.substr(1, path.size() - 1);
  else if (utils::starts_with(path, "./"))
    ret_dir = current + path.substr(1, path.size() - 1);
  else
    ret_dir = current + "/" + path;

  adjacent_slashes_dedup(ret_dir);
  purge_dots_from_path(ret_dir);

  return ret_dir;
}

Status sync(const std::string& path) {
  // Open file
  int fd = -1;
  if (filesystem::is_dir(path))  // DIRECTORY
    fd = open(path.c_str(), O_RDONLY, S_IRWXU);
  else if (filesystem::is_file(path))  // FILE
    fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
  else
    return Status::Ok();  // If file does not exist, exit

  // Handle error
  if (fd == -1) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot sync file '") + path + "'; File opening error"));
  }

  // Sync
  if (fsync(fd)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot sync file '") + path + "'; File syncing error"));
  }

  // Close file
  if (close(fd)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot sync file '") + path + "'; File closing error"));
  }

  // Success
  return Status::Ok();
}

Status write_to_file(
    const std::string& path, const void* buffer, size_t buffer_size) {
  // Open file
  int fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
  if (fd == -1) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot write to file '") + path +
        "'; File opening error"));
  }

  // Append data to the file in batches of Configurator::max_write_bytes()
  // bytes at a time
  ssize_t bytes_written;
  while (buffer_size > Configurator::max_write_bytes()) {
    bytes_written = ::write(fd, buffer, Configurator::max_write_bytes());
    if (bytes_written != Configurator::max_write_bytes()) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot write to file '") + path +
          "'; File writing error"));
    }
    buffer_size -= Configurator::max_write_bytes();
  }
  bytes_written = ::write(fd, buffer, buffer_size);
  if (bytes_written != ssize_t(buffer_size)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file '") + path +
        "'; File writing error"));
  }

  // Close file
  if (close(fd)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot write to file '") + path +
        "'; File closing error"));
  }

  // Success
  return Status::Ok();
}

Status read_from_gzipfile(
    const std::string& path,
    void* buffer,
    unsigned int size,
    int* decompressed_size) {
  // Open file
  gzFile fd = gzopen(path.c_str(), "r");
  if (fd == nullptr) {
    return LOG_STATUS(Status::OSError(
        std::string("Could not read file '") + path + "'; file open error"));
  }
  int nbytes = gzread(fd, buffer, size);
  if (nbytes < 0) {
    return LOG_STATUS(Status::GZipError(gzerror(fd, NULL)));
  }
  *decompressed_size = nbytes;
  if (gzclose(fd)) {
    return Status::OSError(std::string("Could not close file '") + path + "'");
  }
  return Status::Ok();
}

Status write_to_gzipfile(
    const std::string& path, const void* buffer, size_t buffer_size) {
  // Open file
  gzFile fd = gzopen(path.c_str(), "wb");
  if (fd == nullptr) {
    return LOG_STATUS(Status::OSError(
        std::string("Could not write to file '") + path +
        "'; File opening error"));
  }
  // Append data to the file in batches of Configurator::max_write_bytes()
  // bytes at a time
  ssize_t bytes_written;
  while (buffer_size > Configurator::max_write_bytes()) {
    bytes_written = gzwrite(fd, buffer, Configurator::max_write_bytes());
    if (bytes_written != Configurator::max_write_bytes()) {
      return LOG_STATUS(Status::GZipError(gzerror(fd, NULL)));
    }
    buffer_size -= Configurator::max_write_bytes();
  }
  bytes_written = gzwrite(fd, buffer, buffer_size);
  if (bytes_written != ssize_t(buffer_size)) {
    return LOG_STATUS(Status::GZipError(gzerror(fd, NULL)));
  }
  // Close file
  if (gzclose(fd)) {
    return LOG_STATUS(Status::OSError(
        std::string("Could not write to file '") + path +
        "'; File closing error"));
  }
  return Status::Ok();
}

#ifdef HAVE_MPI
Status mpi_io_read_from_file(
    const MPI_Comm* mpi_comm,
    const std::string& filename,
    off_t offset,
    void* buffer,
    size_t length) {
  // Sanity check
  if (mpi_comm == NULL) {
    return LOG_STATUS(
        Status::Error("Cannot read from file; Invalid MPI communicator"));
  }

  // Open file
  MPI_File fh;
  if (MPI_File_open(
          *mpi_comm,
          (char*)filename.c_str(),
          MPI_MODE_RDONLY,
          MPI_INFO_NULL,
          &fh)) {
    return LOG_STATUS(
        Status::Error("Cannot read from file; File opening error"));
  }

  // Read
  MPI_File_seek(fh, offset, MPI_SEEK_SET);
  MPI_Status mpi_status;
  if (MPI_File_read(fh, buffer, length, MPI_CHAR, &mpi_status)) {
    return LOG_STATUS(
        Status::IOError("Cannot read from file; File reading error"));
  }

  // Close file
  if (MPI_File_close(&fh)) {
    return LOG_STATUS(
        Status::OSError("Cannot read from file; File closing error"));
  }

  // Success
  return Status::Ok();
}

Status mpi_io_write_to_file(
    const MPI_Comm* mpi_comm,
    const char* filename,
    const void* buffer,
    size_t buffer_size) {
  // Open file
  MPI_File fh;
  if (MPI_File_open(
          *mpi_comm,
          (char*)filename,
          MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE |
              MPI_MODE_SEQUENTIAL,
          MPI_INFO_NULL,
          &fh)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot write to file '") + filename +
        "'; File opening error"));
  }

  // Append attribute data to the file in batches of
  // Configurator::max_write_bytes() bytes at a time
  MPI_Status mpi_status;
  while (buffer_size > Configurator::max_write_bytes()) {
    if (MPI_File_write(
            fh,
            (void*)buffer,
            Configurator::max_write_bytes(),
            MPI_CHAR,
            &mpi_status)) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot write to file '") + filename +
          "'; File writing error"));
    }
    buffer_size -= Configurator::max_write_bytes();
  }
  if (MPI_File_write(fh, (void*)buffer, buffer_size, MPI_CHAR, &mpi_status)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file '") + filename +
        "'; File writing error");
  }

  // Close file
  if (MPI_File_close(&fh)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot write to file '") + filename +
        "'; File closing error");
  }

  // Success
  return Status::Ok()
}

STATUS mpi_io_sync(const MPI_Comm* mpi_comm, const char* filename) {
  // Open file
  MPI_File fh;
  int rc;
  if (filesystem::is_dir(filename))  // DIRECTORY
    rc = MPI_File_open(
        *mpi_comm, (char*)filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
  else if (filesystem::is_file(filename))  // FILE
    rc = MPI_File_open(
        *mpi_comm,
        (char*)filename,
        MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE |
            MPI_MODE_SEQUENTIAL,
        MPI_INFO_NULL,
        &fh);
  else
    return Status::Ok();  // If file does not exist, exit

  // Handle error
  if (rc) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot open file '") + filename + "'; File opening error");
  }

  // Sync
  if (MPI_File_sync(fh)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot sync file '") + filename +
        "'; File syncing error"));
  }

  // Close file
  if (MPI_File_close(&fh)) {
    return LOG_STATUS(Status::OSError(
        std::string("Cannot sync file '") + filename + "'; File closing error")));
  }

  // Success
  return Status::Ok();
}
#endif

}  // namespace filesystem

}  // namespace tiledb