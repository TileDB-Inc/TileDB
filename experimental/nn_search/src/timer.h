/**
 * @file   timer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 */

#ifndef TILEDB_TIMER_HPP
#define TILEDB_TIMER_HPP

#include <chrono>
#include <iostream>

template <class D = std::chrono::microseconds>
class timer {
 private:
  typedef std::chrono::time_point<std::chrono::system_clock> time_t;

 public:
  explicit timer(const std::string& msg = "")
      : start_time(std::chrono::system_clock::now())
      , stop_time(start_time)
      , msg_(msg) {
  }

  time_t start() {
    return (start_time = std::chrono::system_clock::now());
  }
  time_t stop() {
    return (stop_time = std::chrono::system_clock::now());
  }
  double elapsed() const {
    return std::chrono::duration_cast<D>(stop_time - start_time).count();
  }
  double lap() {
    stop();
    return std::chrono::duration_cast<D>(stop_time - start_time).count();
  }

  std::string name() const {
    return msg_;
  }

 private:
  time_t start_time, stop_time;

 protected:
  std::string msg_;
};

using seconds_timer = timer<std::chrono::seconds>;
using ms_timer = timer<std::chrono::milliseconds>;
using us_timer = timer<std::chrono::microseconds>;

class empty_timer {
 public:
  empty_timer(const std::string& msg = "") {
  }
  ~empty_timer() {
  }
};

class life_timer : public empty_timer, public ms_timer {
 public:
  explicit life_timer(const std::string& msg = "")
      : ms_timer(msg) {
  }

  ~life_timer() {
    stop();
    if (ms_timer::msg_ != "") {
      std::cout << "# [ " + msg_ + " ]: ";
      std::cout << elapsed() << " ms" << std::endl;
    }
  }
};

std::ostream& operator<<(std::ostream& os, const seconds_timer& t) {
  std::string name = t.name();
  if (t.name() != "") {
    os << "# [ " + t.name() + " ]: ";
  }
  os << t.elapsed() << " sec";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ms_timer& t) {
  std::string name = t.name();
  if (t.name() != "") {
    os << "# [ " + t.name() + " ]: ";
  }

  os << t.elapsed() << " ms";
  return os;
}

std::ostream& operator<<(std::ostream& os, const us_timer& t) {
  std::string name = t.name();
  if (t.name() != "") {
    os << "# [ " + t.name() + " ]: ";
  }
  os << t.elapsed() << " us";
  return os;
}

#endif  // TILEDB_TIMER_HPP
