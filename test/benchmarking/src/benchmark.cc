/**
 * @file   benchmark.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * Defines common code for the benchmark programs.
 */

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#ifdef __linux__
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "benchmark.h"

namespace {
void usage(const std::string& argv0) {
  std::cerr << "USAGE: " << argv0 << " [setup|run|teardown]" << std::endl
            << std::endl
            << "Runs a TileDB benchmark. Specify one of the following tasks:"
            << std::endl
            << "    setup : Performs benchmark setup and exits." << std::endl
            << "    run : Runs the benchmark." << std::endl
            << "    teardown : Performs benchmark cleanup and exits."
            << std::endl
            << "If no task is specified then setup, run, teardown are executed "
               "once, in that order."
            << std::endl;
}
}  // namespace

int BenchmarkBase::main(int argc, char** argv) {
  if (argc > 1) {
    std::string task(argv[1]);
    if (task == "setup") {
      setup_base();
    } else if (task == "run") {
      run_base();
    } else if (task == "teardown") {
      teardown_base();
    } else {
      usage(argv[0]);
      return 1;
    }
  } else {
    setup_base();
    run_base();
    teardown_base();
  }

  return 0;
}

void BenchmarkBase::teardown_base() {
  auto t0 = std::chrono::steady_clock::now();
  teardown();
  auto t1 = std::chrono::steady_clock::now();

  uint64_t ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  print_task("teardown", &ms, nullptr, 0);
}

void BenchmarkBase::setup_base() {
  teardown();

  auto t0 = std::chrono::steady_clock::now();
  setup();
  auto t1 = std::chrono::steady_clock::now();

  uint64_t ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  print_task("setup", &ms, nullptr, 0);
}

void BenchmarkBase::run_base() {
  pre_run();

  // Get a baseline memory sample before running the benchmark.
  const uint64_t baseline_mem_mb = sample_virt_mem_mb();

  bool stop_mem_sampling = false;
  std::vector<uint64_t> mem_samples_mb;
  std::thread mem_sampling_thread(
      mem_sampling_thread_func, &stop_mem_sampling, &mem_samples_mb);

  auto t0 = std::chrono::steady_clock::now();
  run();
  auto t1 = std::chrono::steady_clock::now();

  stop_mem_sampling = true;
  mem_sampling_thread.join();

  uint64_t ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  print_task("run", &ms, &mem_samples_mb, baseline_mem_mb);
}

void BenchmarkBase::print_task(
    const std::string& name,
    const uint64_t* const runtime_ms,
    const std::vector<uint64_t>* const mem_samples_mb,
    const uint64_t baseline_mem_mb) {
  std::cout << "{\n";
  std::cout << "  \"phase\": \"" << name << "\",\n";

  if (runtime_ms) {
    std::cout << "  \"runtime_ms\": " << *runtime_ms << ",\n";
  }

  if (mem_samples_mb) {
    uint64_t peak_mem_mb = 0;
    unsigned __int128 avg_mem_mb = 0;
    for (const auto& mem_sample_mb : *mem_samples_mb) {
      if (mem_sample_mb > peak_mem_mb) {
        peak_mem_mb = mem_sample_mb;
      }

      avg_mem_mb += mem_sample_mb;
    }
    avg_mem_mb = avg_mem_mb / mem_samples_mb->size();

    // Subtract the baseline from the memory statistics.
    peak_mem_mb =
        (peak_mem_mb > baseline_mem_mb) ? peak_mem_mb - baseline_mem_mb : 0;
    avg_mem_mb =
        (avg_mem_mb > baseline_mem_mb) ? avg_mem_mb - baseline_mem_mb : 0;

    // Subtract the size of the memory samples from the
    // memory statistics.
    const uint64_t sample_size =
        mem_samples_mb->size() * sizeof(uint64_t) / 1024;
    peak_mem_mb = (peak_mem_mb > sample_size) ? peak_mem_mb - sample_size : 0;
    avg_mem_mb = (avg_mem_mb > sample_size) ? avg_mem_mb - sample_size : 0;

    std::cout << "  \"baseline_mem_mb\": \"" << baseline_mem_mb << "\",\n";
    std::cout << "  \"peak_mem_mb\": \"" << peak_mem_mb << "\",\n";
    std::cout << "  \"avg_mem_mb\": \"" << static_cast<uint64_t>(avg_mem_mb)
              << "\"\n";
  }

  std::cout << "}\n";
}

void BenchmarkBase::mem_sampling_thread_func(
    bool* const stop_mem_sampling,
    std::vector<uint64_t>* const mem_samples_mb) {
  do {
    mem_samples_mb->emplace_back(sample_virt_mem_mb());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!*stop_mem_sampling);
}

uint64_t BenchmarkBase::sample_virt_mem_mb() {
#ifdef __linux__
  uint64_t vm_total_kb = 0;
  std::ifstream buffer("/proc/self/statm");
  buffer >> vm_total_kb;
  buffer.close();

  return vm_total_kb / 1024;
#else
  return 0;
#endif
}
