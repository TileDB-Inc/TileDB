#include <tiledb.h>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static const uint64_t gb = 1024 * 1024 * 1024;
static const uint64_t tb = 1024 * gb;
static const uint64_t file_bytes = 3 * tb;
static const uint64_t block_bytes = 2 * gb;
static const uint64_t num_blocks = file_bytes / block_bytes;
static const uint64_t block_elts = block_bytes / sizeof(uint64_t);
static const std::string bucket_name("tiledb-s3-benchmarks");
static const std::string file_name("test-file");
static const std::string bucket_uri("s3://" + bucket_name);
static const std::string file_uri(bucket_uri + "/" + file_name);

static inline void check_error(int rc, std::string msg = "") {
  if (rc != TILEDB_OK) {
    throw std::runtime_error(msg);
  }
}

/** Returns the value of x/y (integer division) rounded up. */
template <typename T>
inline T ceil(T x, T y) {
  return x / y + (x % y != 0);
}

static void create_file(
    tiledb_ctx_t *ctx,
    tiledb_vfs_t *vfs,
    const std::string &bucket_uri,
    const std::string &file_uri) {
  static_assert(file_bytes % block_bytes == 0, "Block must divide file size");
  static_assert(
      block_bytes % sizeof(uint64_t) == 0, "Block must divide by elt bytes");
  static_assert(
      file_bytes % sizeof(uint64_t) == 0, "File must divide by elt bytes");

  int is_file = 0;
  check_error(tiledb_vfs_is_file(ctx, vfs, file_uri.c_str(), &is_file));
  if (is_file) {
    return;
  }

  std::cout << "Creating file " << file_uri << "..." << std::endl;

  int is_bucket = 0;
  check_error(tiledb_vfs_is_bucket(ctx, vfs, bucket_uri.c_str(), &is_bucket));
  if (!is_bucket) {
    check_error(
        tiledb_vfs_create_bucket(ctx, vfs, bucket_uri.c_str()),
        "create bucket");
  }

  uint64_t *block = new uint64_t[block_elts];
  for (uint64_t i = 0; i < num_blocks; i++) {
    for (uint64_t j = 0; j < block_elts; j++) {
      block[j] = i * block_elts + j;
    }
    // Append block to file.
    check_error(
        tiledb_vfs_write(ctx, vfs, file_uri.c_str(), block, block_bytes),
        "vfs write");
  }

  check_error(tiledb_vfs_is_file(ctx, vfs, file_uri.c_str(), &is_file));
  if (!is_file) {
    throw std::runtime_error("Expected file to exist");
  }

  std::cout << "Done." << std::endl;

  delete[] block;
}

static void run_queries(unsigned num_threads) {
  const uint64_t bytes_to_read = 1 * gb;
  std::vector<std::thread> workers;
  std::mutex barrier_mtx;
  std::condition_variable barrier;
  bool begin = false;

  std::cout << "Reading " << bytes_to_read << " bytes in " << num_threads
            << " concurrent operations." << std::endl;

  const uint64_t bytes_per_thread = ceil(bytes_to_read, (uint64_t)num_threads);

  // Start threads
  for (unsigned i = 0; i < num_threads; i++) {
    workers.emplace_back(
        [i, bytes_per_thread, bytes_to_read, &barrier_mtx, &barrier, &begin]() {
          const uint64_t t_off = i * bytes_per_thread;
          const uint64_t t_next_off =
              std::min(bytes_to_read, (i + 1) * bytes_per_thread);
          const uint64_t t_len = t_next_off - t_off;
          uint64_t *buffer = new uint64_t[t_len];
          tiledb_error_t *error;
          tiledb_config_t *config;
          tiledb_ctx_t *ctx;
          tiledb_vfs_t *vfs;
          check_error(tiledb_config_create(&config, &error), "config");
          check_error(tiledb_ctx_create(&ctx, config), "context");
          check_error(tiledb_vfs_create(ctx, &vfs, config), "vfs");

          {
            std::unique_lock<std::mutex> lck(barrier_mtx);
            barrier.wait(lck, [&begin] { return begin; });
          }

          check_error(
              tiledb_vfs_read(ctx, vfs, file_uri.c_str(), t_off, buffer, t_len),
              "vfs read");

          delete[] buffer;
          tiledb_vfs_free(ctx, vfs);
          tiledb_error_free(error);
          tiledb_ctx_free(ctx);
          tiledb_config_free(config);
        });
  }

  auto t0 = std::chrono::high_resolution_clock::now();

  {
    std::unique_lock<std::mutex> lck(barrier_mtx);
    begin = true;
    barrier.notify_all();
  }

  for (auto &t : workers) {
    t.join();
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_seconds = t1 - t0;
  std::cout << "End-to-end operation took " << elapsed_seconds.count()
            << " sec." << std::endl;
}

int main(int argc, char **argv) {
  unsigned num_threads = 1;
  if (argc > 1) {
    num_threads = std::atoi(argv[1]);
  }

  tiledb_error_t *error;
  tiledb_config_t *config;
  tiledb_ctx_t *ctx;
  tiledb_vfs_t *vfs;
  check_error(tiledb_config_create(&config, &error), "config");
  check_error(tiledb_ctx_create(&ctx, config), "context");
  check_error(tiledb_vfs_create(ctx, &vfs, config), "vfs");

  //   create_file(ctx, vfs, bucket_uri, file_uri);
  run_queries(num_threads);

  tiledb_vfs_free(ctx, vfs);
  tiledb_error_free(error);
  tiledb_ctx_free(ctx);
  tiledb_config_free(config);
  return 0;
}