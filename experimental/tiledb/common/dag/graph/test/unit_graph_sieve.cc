/**
 * @file unit_graph_sieve.cpp
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Demo program sieve of Eratosthenes, function components for block
 * (and parallelizable) implementation.
 *
 * The block sieve algorithm begins by sequentially finding all the primes in
 * [2, sqrt(n)).  Using that initial set of primes, the algorithm finds primes
 * in each block of numbers delimited by
 *
 *       [sqrt(n) + p*block_size, sqrt(n) + (p+1)*block_size)
 *
 *  for p in [0, n/blocksize).
 *
 * This file provides a decomposition of that computation into the following
 * five tasks:
 *   input_body() generates p, a sequence of integers, starting at 0
 *   gen_range() creates a bitmap for indicating primality (or not)
 *   range_sieve() applies sieve, to block p, using initial set of
 *     sqrt(n) primes and records results in bitmap obtained from
 *     gen_range()
 *   sieve_to_primes_part() generates a list of prime numbers from the
 *     bitmap generated by range_sieve()
 *   output_body() saves the list of primes in a vector at location p+1.
 *     The original set of sqrt(n) primes is stored at loccation 0.
 *   A set of n/block_size parallel task chains is launched to carry
 *     out the computation.
 *
 * These functions take regular values as input parameters and return regular
 * values. They can be composed together to produce the sieve algorithm
 * described above.
 *
 * This program constructs primitive graphs for the sieve and executes that
 * graph with various configurations of schedulers and movers.
 */

#include <cassert>
#include <chrono>
#include <cmath>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

/**
 * Nullify the CHECK macro that might be spread throughout the code for
 * debugging/testing purposes.
 */

#include <catch2/internal/catch_clara.hpp>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/bountiful.h"
#include "experimental/tiledb/common/dag/execution/frugal.h"
#include "experimental/tiledb/common/dag/execution/throw_catch.h"
#include "experimental/tiledb/common/dag/graph/taskgraph.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

using namespace tiledb::common;
using namespace std::placeholders;

template <template <class> class Mover, class T>
using ConsumerNode = consumer_node<Mover, T>;

template <template <class> class Mover, class T>
using ProducerNode = producer_node<Mover, T>;

template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class>
    class SourceMover,
    class BlockOut>
using FunctionNode = function_node<SinkMover, BlockIn, SourceMover, BlockOut>;

/*
 * File-local variables for enabling debugging and tracing
 */
static bool debug = false;
static bool chart = false;

/*
 * Function to enable time based tracing of different portions of program
 * execution.
 */
template <class TimeStamp, class StartTime>
void stamp_time(
    const std::string& msg,
    size_t index,
    TimeStamp& timestamps,
    std::atomic<size_t>& time_index,
    StartTime start_time) {
  if (debug) {
    std::cout << "Thread " << index << std::endl;
  }

  if (chart) {
    timestamps[time_index++] = std::make_tuple(
        time_index.load(),
        index,
        msg,
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start_time)
            .count());
  }
}

/**
 * Some convenience type aliases
 */
template <class bool_t>
using part_info =
    std::tuple<size_t, size_t, size_t, std::shared_ptr<std::vector<bool_t>>>;
using prime_info = std::tuple<size_t, std::shared_ptr<std::vector<size_t>>>;

/**
 * Takes a vector of "bool" (which may be actual bool, or may be char) and
 * extracts the indicated prime numbers.
 */
template <class bool_t>
auto sieve_to_primes(std::vector<bool_t>& sieve) {
  std::vector<size_t> primes;

  for (size_t i = 2; i < sieve.size(); ++i) {
    if (sieve[i]) {
      primes.push_back(i);
    }
  }
  return primes;
}

/**
 * Takes a vector of bool which has a true value for any number that is a prime,
 * and converts to a vector of prime numbers.
 */
template <class bool_t>
auto sieve_to_primes(
    std::vector<bool_t>& sieve,
    std::vector<size_t>& base_primes,
    size_t sqrt_n) {
  std::vector<size_t> primes(base_primes);

  for (size_t i = sqrt_n; i < sieve.size(); ++i) {
    assert(i < sieve.size());
    if (sieve[i]) {
      primes.push_back(i);
    }
  }
  return primes;
}

/**
 * Purely sequential program for finding primes in the range 0 to n.  Returns a
 * vector of "bool" where each location whose index corresponds to a prime
 * number is true and all others are false.
 */
template <class bool_t>
auto sieve_seq(size_t n) {
  if (debug)
    std::cout << "** I am running too" << std::endl;

  std::vector<bool_t> sieve(n, true);

  sieve[0] = sieve[1] = true;

  size_t sqrt_n = static_cast<size_t>(std::ceil(std::sqrt(n)));

  for (size_t i = 2; i < sqrt_n; ++i) {
    assert(i < sieve.size());
    if (sieve[i]) {
      for (size_t j = i * i; j < n; j += i) {
        assert(j < sieve.size());
        sieve[j] = false;
      }
    }
  }

  return sieve;
}

/**
 * Class for generating a (thread safe) sequence of integers, starting at 0
 *
 * @return integer, value one greater than previously returned
 */
class input_body {
  inline static std::atomic<size_t> p{0};
  inline static size_t limit_;
  inline static size_t block_size_;

 public:
  input_body(size_t block_size, size_t limit) {
    limit_ = limit;
    block_size_ = block_size;
  }

  void reset() {
    p.store(0);
  }

  size_t operator()(std::stop_source& stop_source) {
    if (debug)
      std::cout << "input_body " + std::to_string(p) + " with limit " +
                       std::to_string(limit_) + "\n";

    if (p * block_size_ >= limit_) {
      stop_source.request_stop();
    }
    return p++;
  }
};

/**
 * Create a bitmap for storing sieve results
 * @tparam bool_t type of elements for bitmap
 * @param the block number to create bitmap for
 * @return tuple with block number, bitmap range, and bitmap
 */
template <class bool_t>
auto gen_range(size_t p, size_t block_size, size_t sqrt_n, size_t n) {
  if (debug)
    std::cout << "gen_range " << p << std::endl;

  size_t sieve_start = std::min(sqrt_n + p * block_size, n);
  size_t sieve_end = std::min(sieve_start + block_size, n);
  return std::make_tuple(
      p + 1,
      sieve_start,
      sieve_end,
      std::make_shared<std::vector<bool_t>>(sieve_end - sieve_start, true));
};

/**
 * Find primes in indicated range and record in bitmap
 * @param tuple with block number, bitmap range, and bitmap
 * @return tuple with block number, bitmap range, and bitmap
 */
template <class bool_t>
auto range_sieve(
    const part_info<bool_t>& in, const std::vector<size_t>& base_primes) {
  auto [p, sieve_start, sieve_end, local_sieve] = in;
  if (debug)
    std::cout << "range_sieve " << p << std::endl;

  for (size_t i = 0; i < base_primes.size(); ++i) {
    assert(i < base_primes.size());
    size_t pr = base_primes[i];

    size_t q = (pr + sieve_start - 1) / pr;
    q *= pr;

    for (size_t j = q - sieve_start; j < sieve_end - sieve_start; j += pr) {
      assert(j < local_sieve->size());
      (*local_sieve)[j] = false;
    }
  }

  return in;
};

/**
 * Create list of primes from bitmap
 * @param tuple with block number, bitmap range, and bitmap
 * @return tuple with block number and shared_ptr to vector of primes
 */
template <class bool_t>
auto sieve_to_primes_part(const part_info<bool_t>& in) {
  auto [p, sieve_start, sieve_end, local_sieve] = in;
  if (debug)
    std::cout << "sieve_to_primes_part " << p << std::endl;

  std::vector<size_t> primes;
  primes.reserve(local_sieve->size());
  for (size_t i = 0; i < local_sieve->size(); ++i) {
    assert(i < local_sieve->size());
    if ((*local_sieve)[i]) {
      primes.push_back(i + sieve_start);
    }
  }
  return std::make_tuple(p, std::make_shared<std::vector<size_t>>(primes));
};

/**
 * Store list of primes in vector
 * @param tuple with block number and shared_ptr to vector of primes
 */
auto output_body(
    const prime_info& in,
    std::vector<std::shared_ptr<std::vector<size_t>>>& prime_list) {
  auto [p, primes] = in;
  if (debug)
    std::cout << "output_body " << p << " / " << prime_list.size() << std::endl;
  assert(p < prime_list.size());
  prime_list[p] = primes;
};

/**
 * Some structures to simulate a task graph.  Here, a "task graph" is a set of
 * nodes and edges, submitted to a scheduler.
 *
 * @todo (IMPORTANT) Only run a subset of graphs at a time, rather than all of
 * them.
 *
 */

/**
 * Main sieve function
 *
 * @brief Generate primes from 2 to n using sieve of Eratosthenes.
 * @tparam bool_t the type to use for the "bitmap"
 * @param n upper bound of sieve
 * @param block_size how many primes to search for given a base set of primes
 */
template <class Scheduler, template <class> class Mover, class bool_t>
auto sieve_async_block(
    size_t n,
    size_t block_size,
    size_t width,
    bool reverse_order,
    bool grouped,
    [[maybe_unused]] bool use_futures,
    [[maybe_unused]] bool use_threadpool) {
  if (debug)
    std::cout << "== I am running" << std::endl;

  input_body gen{block_size, n};
  gen.reset();

  /*
   * Pseudo graph type. A structure to hold simple dag task graph nodes.
   */
  size_t sqrt_n = static_cast<size_t>(std::ceil(std::sqrt(n)));

  /* Generate base set of sqrt(n) primes to be used for subsequent sieving */
  auto first_sieve = sieve_seq<bool_t>(sqrt_n);
  std::vector<size_t> base_primes = sieve_to_primes(first_sieve);

  /* Store vector of list of primes (each list generated by separate task
   * chain)
   */
  std::vector<std::shared_ptr<std::vector<size_t>>> prime_list(
      n / block_size + 2);  // + n % block_size);

  if (debug)
    std::cout << "Prime list size " << prime_list.size() << std::endl;

  prime_list[0] = std::make_shared<std::vector<size_t>>(base_primes);

  size_t rounds = (n / block_size + 2) / width + 1;

  // auto sched = Scheduler(width);
  TaskGraph<DuffsScheduler<node>> graph(width);

  //  if (debug)
  //    sched.enable_debug();

  if (debug)
    std::cout << "n: " << n << " block_size:  " << block_size
              << " width: " << width << " rounds:  " << rounds << std::endl;

  // using time_t = std::chrono::time_point<std::chrono::high_resolution_clock>;

  std::vector<std::tuple<size_t, size_t, std::string, double>> timestamps(
      width * rounds * 20);
  std::atomic<size_t> time_index{0};
  [[maybe_unused]] auto start_time = std::chrono::high_resolution_clock::now();

  /*
   * Create the "graphs" by creating nodes, edges, and submitting to the
   * scheduler.
   */

  if (grouped == false && reverse_order == false) {
    for (size_t w = 0; w < width; ++w) {
      if (debug)
        std::cout << "w: " << w << std::endl;

      auto a = initial_node(graph, gen);
      auto b = transform_node(graph, [block_size, sqrt_n, n](size_t in) {
        return gen_range<bool_t>(in, block_size, sqrt_n, n);
      });
      auto c =
          transform_node(graph, [&base_primes](const part_info<bool_t>& in) {
            return range_sieve<bool_t>(in, base_primes);
          });
      auto d = transform_node(graph, sieve_to_primes_part<bool_t>);
      auto e = terminal_node(graph, [&prime_list](const prime_info& in) {
        output_body(in, prime_list);
      });
      make_edge(graph, a, b);
      make_edge(graph, b, c);
      make_edge(graph, c, d);
      make_edge(graph, d, e);
    }
    schedule(graph);
    sync_wait(graph);
  }
  if (grouped == false && reverse_order == true) {
    for (size_t w = 0; w < width; ++w) {
      if (debug)
        std::cout << "w: " << w << std::endl;

      auto e = terminal_node(graph, [&prime_list](const prime_info& in) {
        output_body(in, prime_list);
      });
      auto d = transform_node(graph, sieve_to_primes_part<bool_t>);
      auto c =
          transform_node(graph, [&base_primes](const part_info<bool_t>& in) {
            return range_sieve<bool_t>(in, base_primes);
          });
      auto b = transform_node(graph, [block_size, sqrt_n, n](size_t in) {
        return gen_range<bool_t>(in, block_size, sqrt_n, n);
      });
      auto a = initial_node(graph, gen);

      make_edge(graph, a, b);
      make_edge(graph, b, c);
      make_edge(graph, c, d);
      make_edge(graph, d, e);
    }
    schedule(graph);
    sync_wait(graph);
  }
  if (grouped == true && reverse_order == false) {
    // TBD
  }
  if (grouped == true && reverse_order == true) {
    // TBD
  }

  /*
   * Output tracing information from the runs.
   */
  if (chart) {
    std::for_each(
        timestamps.begin(), timestamps.begin() + time_index, [](auto&& a) {
          auto&& [idx, id, str, tm] = a;

          size_t col = 0;
          if (id < 5) {
            col = 2 * id;
          } else {
            col = 2 * (id % 5) + 1;
          }
          col = id;
          std::cout << idx << "\t" << id << "\t" << tm << "\t";
          for (size_t k = 0; k < col; ++k) {
            std::cout << "\t";
          }
          std::cout << str << std::endl;
        });
  }
  if (debug)
    std::cout << "Post sieve" << std::endl;

  return prime_list;
}

/*
 * Driver function for running different sieve function configurations.
 */
template <class F>
auto timer_2(
    F f,
    size_t max,
    size_t blocksize,
    size_t width,
    bool reverse_order,
    bool grouped,
    bool use_futures,
    bool use_threadpool) {
  auto start = std::chrono::high_resolution_clock::now();
  auto s =
      f(max,
        blocksize,
        width,
        reverse_order,
        grouped,
        use_futures,
        use_threadpool);
  auto stop = std::chrono::high_resolution_clock::now();

  size_t num = 0;
  for (auto& j : s) {
    if (j) {
      num += j->size();
    }
  }
  std::cout << "Found " + std::to_string(num) << " primes ";

  return stop - start;
}

using namespace Catch::Clara;

int main(int argc, char* argv[]) {
  size_t number = 100'000'000;
  size_t block_size = 100;
  size_t width = std::thread::hardware_concurrency();
  bool reverse_order = false;
  bool grouped = false;
  std::string scheduler = "bountiful";
  size_t stages = 2;
  size_t trips = 2;

  // This is unused until we migrate to catch2.
  bool durations = false;

  auto cli = Opt(block_size, "block_size")["-b"]["--block_size"]("Block size") |
             Opt(width, "width")["-w"]["--width"]("Width") |
             Opt(number, "number")["-n"]["--number"]("Number") |
             Opt(reverse_order)["-r"]["--reverse_order"]("Reverse order") |
             Opt(grouped)["-g"]["--grouped"]("Grouped") |
             Opt(scheduler, "scheduler")["-s"]["--scheduler"](
                 "Scheduler (bountiful, duffs, throw_catch, or frugal") |
             Opt(stages, "stages")["-t"]["--stages"]("Stages (2 or 3)") |
             Opt(trips, "trips")["-p"]["--trips"]("Trips") |
             Opt(durations, "durations")["-d"]["--durations"]("Durations");

  auto result = cli.parse(Args(argc, argv));
  if (!result) {
    std::cerr << "Error in command line: " << result.errorMessage()
              << std::endl;
    exit(1);
  }

  auto log = [=](std::chrono::duration<double> d) {
    std::cout << "using " + std::to_string(stages) + " stage " + scheduler +
                     " scheduler with " + std::to_string(block_size) +
                     " sized blocks" +
                     (reverse_order ? ", reverse order," :
                                      ", forward order, ") +
                     " and " + std::to_string(width) + " threads: " +
                     std::to_string(
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                             d)
                             .count()) +
                     " ms\n";
  };

  for (size_t i = 0; i < trips; ++i) {
    if (scheduler == "bountiful") {
      if (stages == 2) {
        [[maybe_unused]] auto x =
            sieve_async_block<BountifulScheduler<node>, BountifulMover2, char>;

        auto t1 = timer_2(
            sieve_async_block<BountifulScheduler<node>, BountifulMover2, char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      } else if (stages == 3) {
        auto t1 = timer_2(
            sieve_async_block<BountifulScheduler<node>, BountifulMover3, char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      }
    } else if (scheduler == "duffs") {
      if (stages == 2) {
        auto t1 = timer_2(
            sieve_async_block<DuffsScheduler<node>, DuffsMover2, char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      } else if (stages == 3) {
        auto t1 = timer_2(
            sieve_async_block<DuffsScheduler<node>, DuffsMover3, char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      }
    } else if (scheduler == "throw_catch") {
      if (stages == 2) {
        auto t1 = timer_2(
            sieve_async_block<
                ThrowCatchScheduler<node>,
                ThrowCatchMover2,
                char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      } else if (stages == 3) {
        auto t1 = timer_2(
            sieve_async_block<
                ThrowCatchScheduler<node>,
                ThrowCatchMover3,
                char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      }
    } else if (scheduler == "frugal") {
      std::cout << "The frugal scheduler will almost surely deadlock"
                << std::endl;
      if (stages == 2) {
        auto t1 = timer_2(
            sieve_async_block<FrugalScheduler<node>, FrugalMover2, char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      } else if (stages == 3) {
        auto t1 = timer_2(
            sieve_async_block<FrugalScheduler<node>, FrugalMover3, char>,
            number,
            block_size * 1024,
            width,
            reverse_order,
            grouped,
            true, /* use_futures */
            false);
        log(t1);
      }

    } else {
      std::cout << "Invalid scheduler: " << scheduler << std::endl;
      return 1;
    }
  }
}
