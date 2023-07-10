/**
 * @file flow_graph/test/test_general_factory.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * @section DESCRIPTION Tests for `graph_type.h`. These eventually might be
 * moved into their own directory.
 */

#include "tdb_catch.h"
#include "tiledb/flow_graph/general_factory.h"

using namespace tiledb;

class ProducedBase {
  int x_;

 public:
  ProducedBase(int x)
      : x_{x} {};
  int x() const {
    return x_;
  }
};

class Produced_0 : public ProducedBase {
 public:
  Produced_0()
      : ProducedBase{5} {};
  Produced_0(int x)
      : ProducedBase{x} {};
};

void prototype_0(){};
void prototype_1(int){};

template <class T>
class Produced_1 : public ProducedBase {
 public:
  Produced_1()
      : ProducedBase{T::x} {};
  Produced_1(int x)
      : ProducedBase{x} {};
};

struct ProducedInitializer {
  static constexpr int x{7};
};

TEST_CASE("ClassFactory, simple 0", "[general_factory]") {
  Produced_0 destination{0};
  CHECK(destination.x() == 0);
  ClassFactory<prototype_0> factory{ForClass<Produced_0>};
  factory.make(&destination);
  CHECK(destination.x() == 5);
}

TEST_CASE("ClassFactory, simple 1", "[general_factory]") {
  Produced_0 destination;
  ClassFactory<prototype_1> factory{ForClass<Produced_0>};
  factory.make(&destination, 3);
  CHECK(destination.x() == 3);
}

TEST_CASE("ClassTemplateFactory, simple 0", "[general_factory]") {
  Produced_1<ProducedInitializer> destination{0};
  CHECK(destination.x() == 0);
  ClassTemplateFactory<prototype_0, ProducedInitializer> factory{
      ForClassTemplate<Produced_1>};
  factory.make(&destination);
  CHECK(destination.x() == 7);
}

TEST_CASE("ClassTemplateFactory, simple 1", "[general_factory]") {
  Produced_1<ProducedInitializer> destination{0};
  CHECK(destination.x() == 0);
  ClassTemplateFactory<prototype_1, ProducedInitializer> factory{
      ForClassTemplate<Produced_1>};
  factory.make(&destination, 2);
  CHECK(destination.x() == 2);
}