/**
 * @file   vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements the VFS class.
 */

#include "vfs.h"
#include "path_win.h"
#include "tiledb/common/assert.h"
#include "tiledb/common/log_duration_instrument.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/tile/tile.h"

#include <iostream>
#include <list>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <variant>

using namespace tiledb::common;
using namespace tiledb::sm::filesystem;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS(
    stats::Stats* const parent_stats,
    Logger* const logger,
    ThreadPool* const compute_tp,
    ThreadPool* const io_tp,
    const Config& config)
    : VFSBase(parent_stats)
    , Azure_within_VFS(io_tp, config)
    , GCS_within_VFS(io_tp, config)
    , S3_within_VFS(stats_, io_tp, config)
    , config_(config)
    , logger_(logger)
    , compute_tp_(compute_tp)
    , io_tp_(io_tp)
    , vfs_params_(VFSParameters(config)) {
  Status st;
  passert(compute_tp);
  passert(io_tp);

  // Construct the read-ahead cache.
  read_ahead_cache_ = tdb_unique_ptr<ReadAheadCache>(
      tdb_new(ReadAheadCache, vfs_params_.read_ahead_cache_size_));

  if constexpr (s3_enabled) {
    supported_fs_.insert(Filesystem::S3);
  }

#ifdef HAVE_AZURE
  supported_fs_.insert(Filesystem::AZURE);
#endif

#ifdef HAVE_GCS
  supported_fs_.insert(Filesystem::GCS);
#endif

  local_ = LocalFS(config_);

  supported_fs_.insert(Filesystem::MEMFS);
}

/* ********************************* */
/*                API                */
/* ********************************* */

const FilesystemBase& VFS::get_fs(const URI& uri) const {
  if (uri.is_file()) {
    return local_;
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3();
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs();
#else
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    return memfs_;
  }
  throw UnsupportedURI(uri.to_string());
}

FilesystemBase& VFS::get_fs(const URI uri) {
  return const_cast<FilesystemBase&>(
      static_cast<const VFS*>(this)->get_fs(uri));
}

std::string VFS::abs_path(std::string_view path) {
  // workaround for older clang (llvm 3.5) compilers (issue #828)
  std::string path_copy(path);
#ifdef _WIN32
  {
    std::string norm_sep_path =
        path_win::slashes_to_backslashes(std::string(path));
    if (path_win::is_win_path(norm_sep_path))
      return path_win::uri_from_path(Win::abs_path(norm_sep_path));
    else if (URI::is_file(path))
      return path_win::uri_from_path(
          Win::abs_path(path_win::path_from_uri(path)));
  }
#else
  if (URI::is_file(path))
    return Posix::abs_path(path);
#endif
  if (URI::is_s3(path))
    return path_copy;
  if (URI::is_azure(path))
    return path_copy;
  if (URI::is_gcs(path))
    return path_copy;
  if (URI::is_memfs(path))
    return path_copy;
  // Certainly starts with "<resource>://" other than "file://"
  return path_copy;
}

Config VFS::config() const {
  return config_;
}

void VFS::create_dir(const URI& uri) const {
  if (!uri.is_s3() && !uri.is_azure() && !uri.is_gcs()) {
    if (this->is_dir(uri))
      return;
  }
  get_fs(uri).create_dir(uri);
}

Status VFS::dir_size(const URI& dir_name, uint64_t* dir_size) const {
  // Sanity check
  if (!this->is_dir(dir_name)) {
    throw VFSException(
        "Cannot get directory size; Input '" + dir_name.to_string() +
        "' is not a directory");
  }

  // Get all files in the tree rooted at `dir_name` and add their sizes
  *dir_size = 0;
  std::list<URI> to_ls;
  to_ls.push_front(dir_name);
  do {
    auto uri = to_ls.front();
    to_ls.pop_front();

    for (const auto& child : ls_with_sizes(uri)) {
      if (!child.is_directory()) {
        *dir_size += child.file_size();
      } else {
        to_ls.push_back(URI(child.path().native()));
      }
    }
  } while (!to_ls.empty());

  return Status::Ok();
}

void VFS::touch(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "touch");
  get_fs(uri).touch(uri);
}

Status VFS::cancel_all_tasks() {
  cancelable_tasks_.cancel_all_tasks();
  return Status::Ok();
}

void VFS::create_bucket(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "create_bucket");
  get_fs(uri).create_bucket(uri);
}

void VFS::remove_bucket(const URI& uri) const {
  get_fs(uri).remove_bucket(uri);
}

void VFS::empty_bucket(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "empty_bucket");
  get_fs(uri).empty_bucket(uri);
}

bool VFS::is_empty_bucket(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "is_empty_bucket");
  return get_fs(uri).is_empty_bucket(uri);
}

void VFS::remove_dir(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "remove_dir");
  get_fs(uri).remove_dir(uri);
}

void VFS::remove_dir_if_empty(const URI& uri) const {
  if (uri.is_file()) {
    local_.remove_dir_if_empty(uri.to_path());
  }
  // Object stores do not have directories.
}

void VFS::remove_dirs(
    ThreadPool* compute_tp, const std::vector<URI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    if (this->is_dir(uris[i])) {
      remove_dir(uris[i]);
    }
    return Status::Ok();
  }));
}

void VFS::remove_file(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "remove_file");
  get_fs(uri).remove_file(uri);
}

void VFS::remove_files(
    ThreadPool* compute_tp, const std::vector<URI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    remove_file(uris[i]);
    return Status::Ok();
  }));
}

void VFS::remove_files(
    ThreadPool* compute_tp, const std::vector<TimestampedURI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    remove_file(uris[i].uri_);
    return Status::Ok();
  }));
}

uint64_t VFS::max_parallel_ops(const URI& uri) const {
  if (uri.is_s3()) {
    return config_.get<uint64_t>("vfs.s3.max_parallel_ops", Config::must_find);
  } else if (uri.is_azure()) {
    return config_.get<uint64_t>(
        "vfs.azure.max_parallel_ops", Config::must_find);
  } else if (uri.is_gcs()) {
    return config_.get<uint64_t>("vfs.gcs.max_parallel_ops", Config::must_find);
  } else {
    return 1;
  }
}

uint64_t VFS::file_size(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "file_size");
  stats_->add_counter("file_size_num", 1);
  return get_fs(uri).file_size(uri);
}

bool VFS::is_dir(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "is_dir");
  return get_fs(uri).is_dir(uri);
}

bool VFS::is_file(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "is_file");
  stats_->add_counter("is_object_num", 1);
  return get_fs(uri).is_file(uri);
}

bool VFS::is_bucket(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "is_bucket");
  return get_fs(uri).is_bucket(uri);
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  stats_->add_counter("ls_num", 1);

  for (auto& fs : ls_with_sizes(parent)) {
    uris->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::vector<directory_entry> VFS::ls_with_sizes(const URI& parent) const {
  auto instrument = make_log_duration_instrument(parent, "ls");
  // Noop if `parent` is not a directory, do not error out.
  // For S3, GCS and Azure, `ls` on a non-directory will just
  // return an empty `uris` vector.
  if (!(parent.is_s3() || parent.is_gcs() || parent.is_azure())) {
    if (!this->is_dir(parent)) {
      return {};
    }
  }

  std::vector<directory_entry> entries;
  auto& fs = get_fs(parent);
  entries = fs.ls_with_sizes(parent);

  parallel_sort(
      compute_tp_,
      entries.begin(),
      entries.end(),
      [](const directory_entry& l, const directory_entry& r) {
        return l.path().native() < r.path().native();
      });

  return entries;
}

LsObjects VFS::ls_filtered(
    const URI& parent, ResultFilter f, bool recursive) const {
  return get_fs(parent).ls_filtered(parent, std::move(f), recursive);
}

LsObjects VFS::ls_filtered_v2(
    const URI& parent, ResultFilterV2 f, bool recursive) const {
  return get_fs(parent).ls_filtered_v2(parent, std::move(f), recursive);
}

void VFS::move_file(const URI& old_uri, const URI& new_uri) const {
  auto& old_fs = get_fs(old_uri);
  auto& new_fs = get_fs(new_uri);
  if (&old_fs == &new_fs) {
    old_fs.move_file(old_uri, new_uri);
  } else {
    throw UnsupportedOperation("move_file");
  }
}

void VFS::move_dir(const URI& old_uri, const URI& new_uri) const {
  auto instrument = make_log_duration_instrument(old_uri, new_uri);
  auto& old_fs = get_fs(old_uri);
  auto& new_fs = get_fs(new_uri);
  if (&old_fs == &new_fs) {
    old_fs.move_dir(old_uri, new_uri);
  } else {
    throw UnsupportedOperation("move_dir");
  }
}

void VFS::chunked_buffer_io(const URI& src, const URI& dest) const {
  auto &src_fs{get_fs(src)}, &dest_fs{get_fs(dest)};

  // Deque which stores the buffers passed between the readers and writer.
  ProducerConsumerQueue<
      std::variant<tdb_shared_ptr<std::vector<char>>, std::exception_ptr>>
      buffer_queue;

  // By default, the buffer size is 10 MB, unless the filesize is smaller.
  uint64_t filesize = src_fs.file_size(src);
  uint64_t buffer_size = std::min(filesize, (uint64_t)10485760);  // 10 MB

  // The maximum number of buffers the reader may allocate.
  const int max_buffer_count = 10;

  // Atomic counter of the buffers allocated by the reader.
  // May not exceed `max_buffer_count`.
  std::atomic<int> buffer_count = 0;

  // Flag indicating an ongoing read. The reader will stop once set to `false`.
  std::atomic<bool> reading = true;

  // Reader
  ThreadPool::Task read_task = io_tp_->execute([&] {
    uint64_t offset = 0;
    while (reading) {
      tdb_unique_ptr<std::vector<char>> buffer(
          tdb_new(std::vector<char>, buffer_size));
      try {
        uint64_t bytes_read =
            src_fs.read(src, offset, buffer->data(), buffer->size());
        if (bytes_read > 0) {
          buffer->resize(bytes_read);
          buffer_queue.push(std::move(buffer));
          offset += bytes_read;
        }

        // Once the read is complete, drain the queue and exit the reader.
        // Note: drain() shuts down the queue without removing elements.
        // The write fiber will be notified and write the remaining chunks.
        if (offset >= filesize) {
          buffer_queue.drain();
          reading = false;
          break;
        }
      } catch (...) {
        // Enqueue caught-exceptions to be handled by the writer.
        buffer_queue.push(std::current_exception());
      }
      buffer_count++;

      io_tp_->wait_until([&]() { return buffer_count < max_buffer_count; });
    }
    return Status::Ok();
  });

  // Writer
  while (true) {
    // Allow the ProducerConsumerQueue to wait for an element to be enqueued.
    auto buffer_queue_element = buffer_queue.pop_back();
    if (!buffer_queue_element.has_value()) {
      // Stop writing once the queue is empty (reader finished reading file).
      break;
    }

    auto& buffer = buffer_queue_element.value();
    // Rethrow exceptions enqueued by read.
    if (std::holds_alternative<std::exception_ptr>(buffer)) {
      std::rethrow_exception(std::get<std::exception_ptr>(buffer));
    }

    auto& writebuf = std::get<0>(buffer);
    try {
      dest_fs.write(dest, writebuf->data(), writebuf->size());
      buffer_count--;
    } catch (...) {
      reading = false;  // Stop the reader.
      throw;
    }
  }

  dest_fs.flush(dest);
}

void VFS::copy_file(const URI& old_uri, const URI& new_uri) const {
  auto instrument = make_log_duration_instrument(old_uri, new_uri);
  auto& old_fs = get_fs(old_uri);
  auto& new_fs = get_fs(new_uri);
  if (&old_fs == &new_fs) {
    old_fs.copy_file(old_uri, new_uri);
  } else {
    chunked_buffer_io(old_uri, new_uri);
  }
}

void VFS::copy_dir(const URI& old_uri, const URI& new_uri) const {
  auto instrument = make_log_duration_instrument(old_uri, new_uri);
  auto& old_fs = get_fs(old_uri);
  auto& new_fs = get_fs(new_uri);
  if (&old_fs == &new_fs) {
    old_fs.copy_dir(old_uri, new_uri);
  } else {
    throw UnsupportedOperation("copy_dir");
  }
}

Status VFS::read_exactly(
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes) const {
  uint64_t length_read = 0;
  RETURN_NOT_OK(ok_if_not_throw(
      [&]() { length_read = read(uri, offset, buffer, nbytes); }));

  if (length_read < nbytes) {
    return Status_VFSError(
        "The read did not return the correct number of bytes. Expected: " +
        std::to_string(nbytes) + " Actual: " + std::to_string(length_read));
  }
  return Status::Ok();
}

uint64_t VFS::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  stats_->add_counter("read_byte_num", nbytes);

  // Ensure that each thread is responsible for at least min_parallel_size
  // bytes, and cap the number of parallel operations at the configured maximum
  // number.
  uint64_t num_ops = std::min(
      std::max(nbytes / vfs_params_.min_parallel_size_, uint64_t(1)),
      max_parallel_ops(uri));

  uint64_t length_read = 0;
  bool use_read_ahead = true;
  if (num_ops == 1) {
    throw_if_not_ok(
        read_impl(uri, offset, buffer, nbytes, use_read_ahead, &length_read));
  } else {
    // we don't want read-ahead when performing random access reads
    use_read_ahead = false;
    std::vector<ThreadPool::Task> results;
    uint64_t thread_read_nbytes = utils::math::ceil(nbytes, num_ops);
    for (uint64_t i = 0; i < num_ops; i++) {
      uint64_t begin = i * thread_read_nbytes,
               end = std::min((i + 1) * thread_read_nbytes - 1, nbytes - 1);
      uint64_t thread_nbytes = end - begin + 1;
      uint64_t thread_offset = offset + begin;
      auto thread_buffer = reinterpret_cast<char*>(buffer) + begin;
      auto task = const_cast<CancelableTasks*>(&cancelable_tasks_)
                      ->execute(
                          io_tp_,
                          [this,
                           uri,
                           thread_offset,
                           thread_buffer,
                           thread_nbytes,
                           use_read_ahead,
                           &thread_read_nbytes]() {
                            return read_impl(
                                uri,
                                thread_offset,
                                thread_buffer,
                                thread_nbytes,
                                use_read_ahead,
                                &thread_read_nbytes);
                          });
      length_read += thread_read_nbytes;
      results.push_back(std::move(task));
    }
    Status st = io_tp_->wait_all(results);
    if (!st.ok()) {
      throw VFSException(
          "VFS parallel read error '" + uri.to_string() + "'; " + st.message());
    }
  }
  return length_read;
}

Status VFS::read_impl(
    const URI& uri,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes,
    [[maybe_unused]] bool use_read_ahead,
    uint64_t* length_read) const {
  auto instrument = make_log_duration_instrument(uri, "read");
  stats_->add_counter("read_ops_num", 1);
  log_read(uri, offset, nbytes);
  auto& fs = get_fs(uri);

  // Do not read-ahead on local filesystems, or if disabled by the caller.
  if (!(fs.use_read_ahead_cache() && use_read_ahead)) {
    *length_read = fs.read(uri, offset, buffer, nbytes);
    return Status::Ok();
  }

  // Only perform a read-ahead if the requested read size
  // is smaller than the size of the buffers in the read-ahead
  // cache. This is because:
  // 1. The read-ahead is primarily beneficial for IO patterns
  //    that consist of numerous small reads.
  // 2. Large reads may evict cached buffers that would be useful
  //    to a future small read.
  // 3. It saves us a copy. We must make a copy of the buffer at
  //    some point (one for the user, one for the cache).
  if (nbytes >= vfs_params_.read_ahead_size_) {
    *length_read = fs.read(uri, offset, buffer, nbytes);
    return Status::Ok();
  }

  // Avoid a read if the requested buffer can be read from the
  // read cache. Note that we intentionally do not use a read
  // cache for local files because we rely on the operating
  // system's file system to cache readahead data in memory.
  bool success;
  RETURN_NOT_OK(read_ahead_cache_->read(uri, offset, buffer, nbytes, &success));
  if (success) {
    *length_read = nbytes;
    return Status::Ok();
  }

  // We will read directly into the read-ahead buffer and then copy
  // the subrange of this buffer back to the user to satisfy the
  // read request.
  Buffer ra_buffer;
  RETURN_NOT_OK(ra_buffer.realloc(vfs_params_.read_ahead_size_));

  // Calculate the exact number of bytes to populate `ra_buffer`
  // with `vfs_params_.read_ahead_size_` bytes.
  const uint64_t ra_nbytes = vfs_params_.read_ahead_size_ - nbytes;

  // Read into `ra_buffer`.
  *length_read = fs.read(uri, offset, ra_buffer.data(), nbytes + ra_nbytes);

  // Copy the requested read range back into the caller's output `buffer`.
  iassert(
      *length_read >= nbytes,
      "nbytes_read = {}, nbytes = {}",
      *length_read,
      nbytes);
  std::memcpy(buffer, ra_buffer.data(), nbytes);

  // Cache `ra_buffer` at `offset`.
  ra_buffer.set_size(*length_read);
  RETURN_NOT_OK(read_ahead_cache_->insert(uri, offset, std::move(ra_buffer)));

  return Status::Ok();
}

bool VFS::supports_uri(const URI& uri) const {
  return get_fs(uri).supports_uri(uri);
}

bool VFS::supports_fs(Filesystem fs) const {
  return (supported_fs_.find(fs) != supported_fs_.end());
}

bool VFS::supports_uri_scheme(const URI& uri) const {
  if (uri.is_s3()) {
    return supports_fs(Filesystem::S3);
  } else if (uri.is_azure()) {
    return supports_fs(Filesystem::AZURE);
  } else if (uri.is_gcs()) {
    return supports_fs(Filesystem::GCS);
  } else {
    return true;
  }
}

void VFS::sync(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "sync");
  get_fs(uri).sync(uri);
}

Status VFS::open_file(const URI& uri, VFSMode mode) {
  bool is_file = this->is_file(uri);
  switch (mode) {
    case VFSMode::VFS_READ:
      if (!is_file)
        throw VFSException(
            "Cannot open file '" + uri.to_string() + "'; File does not exist");
      break;
    case VFSMode::VFS_WRITE:
      if (is_file)
        remove_file(uri);
      break;
    case VFSMode::VFS_APPEND:
      if (uri.is_s3()) {
        if constexpr (s3_enabled) {
          throw VFSException(
              "Cannot open file '" + uri.to_string() +
              "'; S3 does not support append mode");
        } else {
          throw BuiltWithout("S3");
        }
      }
      if (uri.is_azure()) {
        if constexpr (azure_enabled) {
          throw VFSException(
              "Cannot open file '" + uri.to_string() +
              "'; Azure does not support append mode");
        } else {
          throw BuiltWithout("Azure");
        }
      }
      if (uri.is_gcs()) {
        if constexpr (gcs_enabled) {
          throw VFSException(
              "Cannot open file '" + uri.to_string() +
              "'; GCS does not support append mode");
        } else {
          throw BuiltWithout("GCS");
        }
      }
      break;
  }

  return Status::Ok();
}

void VFS::flush(const URI& uri, bool finalize) const {
  auto instrument = make_log_duration_instrument(uri, "flush");
  get_fs(uri).flush(uri, finalize);
}

Status VFS::close_file(const URI& uri) const {
  flush(uri);
  return Status::Ok();
}

void VFS::write(
    const URI& uri,
    const void* buffer,
    uint64_t buffer_size,
    bool remote_global_order_write) const {
  auto instrument = make_log_duration_instrument(uri, "write");
  stats_->add_counter("write_byte_num", buffer_size);
  stats_->add_counter("write_ops_num", 1);
  get_fs(uri).write(uri, buffer, buffer_size, remote_global_order_write);
}

std::pair<Status, std::optional<VFS::MultiPartUploadState>>
VFS::multipart_upload_state(const URI& uri) {
  if (uri.is_file()) {
    return {Status::Ok(), {}};
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    VFS::MultiPartUploadState state;
    auto s3_state = s3().multipart_upload_state(uri);
    if (!s3_state.has_value()) {
      return {Status::Ok(), nullopt};
    }
    state.upload_id = s3_state->upload_id;
    state.part_number = s3_state->part_number;
    state.status = s3_state->st;
    auto& completed_parts = s3_state->completed_parts;
    for (auto& entry : completed_parts) {
      state.completed_parts.emplace_back();
      state.completed_parts.back().e_tag = entry.second.GetETag();
      state.completed_parts.back().part_number = entry.second.GetPartNumber();
    }
    if (!s3_state->buffered_chunks.empty()) {
      state.buffered_chunks.emplace();
      for (auto& chunk : s3_state->buffered_chunks) {
        state.buffered_chunks->emplace_back(
            URI(chunk.uri).remove_trailing_slash().last_path_part(),
            chunk.size);
      }
    }

    return {Status::Ok(), state};
#else
    return {Status_VFSError("TileDB was built without S3 support"), nullopt};
#endif
  } else if (uri.is_azure()) {
    if constexpr (azure_enabled) {
      return {Status_VFSError("Not yet supported for Azure"), nullopt};
    } else {
      return {
          Status_VFSError("TileDB was built without Azure support"), nullopt};
    }
  } else if (uri.is_gcs()) {
    if constexpr (gcs_enabled) {
      return {Status_VFSError("Not yet supported for GCS"), nullopt};
    } else {
      return {Status_VFSError("TileDB was built without GCS support"), nullopt};
    }
  }

  return {
      Status_VFSError("Unsupported URI schemes: " + uri.to_string()), nullopt};
}

Status VFS::set_multipart_upload_state(
    const URI& uri, [[maybe_unused]] const MultiPartUploadState& state) {
  if (uri.is_file()) {
    return Status::Ok();
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    S3::MultiPartUploadState s3_state;
    s3_state.part_number = state.part_number;
    s3_state.upload_id = *state.upload_id;
    s3_state.st = state.status;
    for (auto& part : state.completed_parts) {
      auto rv = s3_state.completed_parts.try_emplace(part.part_number);
      rv.first->second.SetETag(part.e_tag->c_str());
      rv.first->second.SetPartNumber(part.part_number);
    }

    if (state.buffered_chunks.has_value()) {
      for (auto& chunk : *state.buffered_chunks) {
        // Chunk URI gets reconstructed from the serialized chunk name
        // and the real attribute uri
        s3_state.buffered_chunks.emplace_back(
            s3().generate_chunk_uri(uri, chunk.uri).to_string(), chunk.size);
      }
    }

    return s3().set_multipart_upload_state(uri.to_string(), s3_state);
#else
    throw BuiltWithout("S3");
#endif
  } else if (uri.is_azure()) {
    if constexpr (azure_enabled) {
      throw VFSException("Not yet supported for Azure");
    } else {
      throw BuiltWithout("Azure");
    }
  } else if (uri.is_gcs()) {
    if constexpr (gcs_enabled) {
      throw VFSException("Not yet supported for GCS");
    } else {
      throw BuiltWithout("GCS");
    }
  }

  throw UnsupportedURI(uri.to_string());
}

Status VFS::flush_multipart_file_buffer(const URI& uri) {
  auto instrument =
      make_log_duration_instrument(uri, "flush_multipart_file_buffer");
  if (uri.is_s3()) {
#ifdef HAVE_S3
    Buffer* buff = nullptr;
    throw_if_not_ok(s3().get_file_buffer(uri, &buff));
    s3().global_order_write(uri, buff->data(), buff->size());
    buff->reset_size();

#else
    throw BuiltWithout("S3");
#endif
  }

  return Status::Ok();
}

void VFS::log_read(const URI& uri, uint64_t offset, uint64_t nbytes) const {
  std::string read_to_log;
  switch (vfs_params_.read_logging_mode_) {
    case VFSParameters::ReadLoggingMode::DISABLED: {
      return;
    }
    case VFSParameters::ReadLoggingMode::FRAGMENTS: {
      auto fragment_name = uri.get_fragment_name();
      if (!fragment_name.has_value()) {
        return;
      }
      read_to_log = fragment_name.value().to_string();
      break;
    }
    case VFSParameters::ReadLoggingMode::FRAGMENT_FILES: {
      if (!uri.get_fragment_name().has_value()) {
        return;
      }
      read_to_log = uri.to_string();
      break;
    }
    case VFSParameters::ReadLoggingMode::ALL_FILES: {
      read_to_log = uri.to_string();
      break;
    }
    case VFSParameters::ReadLoggingMode::ALL_READS:
    case VFSParameters::ReadLoggingMode::ALL_READS_ALWAYS: {
      read_to_log = uri.to_string() + ":offset:" + std::to_string(offset) +
                    ":nbytes:" + std::to_string(nbytes);
      break;
    }
    default:
      return;
  }

  if (vfs_params_.read_logging_mode_ !=
      VFSParameters::ReadLoggingMode::ALL_READS_ALWAYS) {
    if (reads_logged_.find(read_to_log) != reads_logged_.end()) {
      return;
    }
    reads_logged_.insert(read_to_log);
  }

  LOG_INFO("VFS Read: " + read_to_log);
}

optional<LogDurationInstrument> VFS::make_log_duration_instrument(
    const URI& uri, const std::string_view operation_name) const {
  if (!vfs_params_.log_operations_ || logger_ == nullptr) {
    return nullopt;
  }
  // Construct LogDurationInstrument in-place to take advantage of RVO.
  return std::make_optional<LogDurationInstrument>(
      logger_, "{} on {}", operation_name, uri.to_string());
}

optional<LogDurationInstrument> VFS::make_log_duration_instrument(
    const URI& source,
    const URI& destination,
    const std::string_view operation_name) const {
  if (!vfs_params_.log_operations_ || logger_ == nullptr) {
    return nullopt;
  }
  // Construct LogDurationInstrument in-place to take advantage of RVO.
  return std::make_optional<LogDurationInstrument>(
      logger_,
      "{} from {} to {}",
      operation_name,
      source.to_string(),
      destination.to_string());
}

}  // namespace tiledb::sm
