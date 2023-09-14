Status VFS::read_ahead_impl(
    const std::function<Status(
        const URI&, off_t, void*, uint64_t, uint64_t, uint64_t*)>& read_fn,
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes,
    const bool use_read_ahead) {
  // Stores the total number of bytes read.
  uint64_t nbytes_read = 0;

  // Do not use the read-ahead cache if disabled by the caller.
  if (!use_read_ahead)
    return read_fn(uri, offset, buffer, nbytes, 0, &nbytes_read);

  // Only perform a read-ahead if the requested read size
  // is smaller than the size of the buffers in the read-ahead
  // cache. This is because:
  // 1. The read-ahead is primarily beneficial for IO patterns
  //    that consist of numerous small reads.
  // 2. Large reads may evict cached buffers that would be useful
  //    to a future small read.
  // 3. It saves us a copy. We must make a copy of the buffer at
  //    some point (one for the user, one for the cache).
  if (nbytes >= vfs_params_.read_ahead_size_)
    return read_fn(uri, offset, buffer, nbytes, 0, &nbytes_read);

  // Avoid a read if the requested buffer can be read from the
  // read cache. Note that we intentionally do not use a read
  // cache for local files because we rely on the operating
  // system's file system to cache readahead data in memory.
  // Additionally, we do not perform readahead with HDFS.
  bool success;
  RETURN_NOT_OK(read_ahead_cache_->read(uri, offset, buffer, nbytes, &success));
  if (success)
    return Status::Ok();

  // We will read directly into the read-ahead buffer and then copy
  // the subrange of this buffer back to the user to satisfy the
  // read request.
  Buffer ra_buffer;
  RETURN_NOT_OK(ra_buffer.realloc(vfs_params_.read_ahead_size_));

  // Calculate the exact number of bytes to populate `ra_buffer`
  // with `vfs_params_.read_ahead_size_` bytes.
  const uint64_t ra_nbytes = vfs_params_.read_ahead_size_ - nbytes;

  // Read into `ra_buffer`.
  RETURN_NOT_OK(
      read_fn(uri, offset, ra_buffer.data(), nbytes, ra_nbytes, &nbytes_read));

  // Copy the requested read range back into the caller's output `buffer`.
  assert(nbytes_read >= nbytes);
  std::memcpy(buffer, ra_buffer.data(), nbytes);

  // Cache `ra_buffer` at `offset`.
  ra_buffer.set_size(nbytes_read);
  RETURN_NOT_OK(read_ahead_cache_->insert(uri, offset, std::move(ra_buffer)));

  return Status::Ok();
}
