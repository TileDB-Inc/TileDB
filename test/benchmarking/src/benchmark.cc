/**
 * @file   benchmark.cc
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
 * Defines common code for the benchmark programs.
 */

#include <chrono>
#include <iostream>
#include <string>

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
  print_task_ms_json("teardown", ms);
}

void BenchmarkBase::setup_base() {
  teardown();

  auto t0 = std::chrono::steady_clock::now();
  setup();
  auto t1 = std::chrono::steady_clock::now();

  uint64_t ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  print_task_ms_json("setup", ms);
}

void BenchmarkBase::run_base() {
  pre_run();

  auto t0 = std::chrono::steady_clock::now();
  run();
  auto t1 = std::chrono::steady_clock::now();

  uint64_t ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  print_task_ms_json("run", ms);
}

void BenchmarkBase::teardown() {
}

void BenchmarkBase::setup() {
}

void BenchmarkBase::run() {
}

void BenchmarkBase::pre_run() {
}

void BenchmarkBase::print_task_ms_json(const std::string& name, uint64_t ms) {
  std::cout << "{ \"phase\": \"" << name << "\", \"ms\": " << ms << " }\n";
}