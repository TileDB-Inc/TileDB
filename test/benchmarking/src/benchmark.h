/**
 * @file   benchmark.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
  virtual void setup();

  /** Implemented by subclass: the cleanup phase. */
  virtual void teardown();

  /**
   * Implemented by subclass: anything that needs to happen in the same process
   * as 'run' but should be excluded from the benchmark run time, e.g.
   * query buffer allocation.
   */
  virtual void pre_run();

  /** Implemented by subclass: the run phase. */
  virtual void run();

 private:
  /**
   * Prints a time in milliseconds for a task name in JSON.
   */
  void print_task_ms_json(const std::string& name, uint64_t ms);
};

#endif