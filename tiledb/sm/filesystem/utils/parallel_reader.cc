
Status VFS::read(
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes,
    bool use_read_ahead) {
  create_fs(uri);
  stats_->add_counter("read_byte_num", nbytes);

  // Get config params
  uint64_t min_parallel_size = vfs_params_.min_parallel_size_;
  uint64_t max_ops = 0;
  RETURN_NOT_OK(max_parallel_ops(uri, &max_ops));

  // Ensure that each thread is responsible for at least min_parallel_size
  // bytes, and cap the number of parallel operations at the configured maximum
  // number.
  uint64_t num_ops =
      std::min(std::max(nbytes / min_parallel_size, uint64_t(1)), max_ops);

  if (num_ops == 1) {
    return read_impl(uri, offset, buffer, nbytes, use_read_ahead);
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
      auto task = cancelable_tasks_.execute(
          io_tp_,
          [this,
           uri,
           thread_offset,
           thread_buffer,
           thread_nbytes,
           use_read_ahead]() {
            return read_impl(
                uri,
                thread_offset,
                thread_buffer,
                thread_nbytes,
                use_read_ahead);
          });
      results.push_back(std::move(task));
    }
    Status st = io_tp_->wait_all(results);
    if (!st.ok()) {
      std::stringstream errmsg;
      errmsg << "VFS parallel read error '" << uri.to_string() << "'; "
             << st.message();
      return LOG_STATUS(Status_VFSError(errmsg.str()));
    }
    return st;
  }
}

Status VFS::read_impl(
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes,
    const bool use_read_ahead) {
  // fs_ is create in VFS::read
  stats_->add_counter("read_ops_num", 1);

  // We only check to use the read-ahead cache for cloud-storage
  // backends. No-op the `use_read_ahead` to prevent the unused
  // variable compiler warning.
  (void)use_read_ahead;

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.read(uri.to_path(), offset, buffer, nbytes);
#else
    return fs_->read(uri, offset, buffer, nbytes, false);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->read(uri, offset, buffer, nbytes);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    const auto read_fn = std::bind(
        &S3::read,
        &s3_,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    const auto read_fn = std::bind(
        &Azure::read,
        &azure_,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    const auto read_fn = std::bind(
        &GCS::read,
        &gcs_,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.read(uri.to_path(), offset, buffer, nbytes);
  }

  return LOG_STATUS(
      Status_VFSError("Unsupported URI schemes: " + uri.to_string()));
}
