/**
 * @file   benchmark.h
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
 * Declares common code for the benchmark programs.
 */

#ifndef TILEDB_BENCHMARK_H
#define TILEDB_BENCHMARK_H

#include <cassert>
#include <string>
#include <vector>

/**
 * Base class for benchmarks.
 */
class BenchmarkBase {
 public:
  /**
   * Main method of the benchmark, which invokes setup, run, or teardown
   * depending on the arguments given.
   */
  int main(int argc, char** argv);

  /**
   * Benchmark setup (array creation, etc).
   */
  void setup_base();

  /** Benchmark cleanup (array removal, etc). */
  void teardown_base();

  /** Benchmark run method to be timed. */
  void run_base();

 protected:
  /** Implemented by subclass: the setup phase. */
  virtual void setup() = 0;

  /** Implemented by subclass: the cleanup phase. */
  virtual void teardown() = 0;

  /**
   * Implemented by subclass: anything that needs to happen in the same process
   * as 'run' but should be excluded from the benchmark run time, e.g.
   * query buffer allocation.
   */
  virtual void pre_run() = 0;

  /** Implemented by subclass: the run phase. */
  virtual void run() = 0;

 private:
  /**
   * Prints metrics for a given task.
   */
  void print_task(
      const std::string& name,
      const uint64_t* ms,
      const std::vector<uint64_t>* mem_samples_mb,
      uint64_t baseline_mem_mb);

  /**
   * Samples the current processes's used virtual memory every 50ms
   * and stores it in 'mem_samples_mb'.
   */
  static void mem_sampling_thread_func(
      bool* stop_mem_sampling, std::vector<uint64_t>* mem_samples_mb);

  /**
   * Samples the current processes's used virtual memory and returns it.
   */
  static uint64_t sample_virt_mem_mb();
};

#endif
