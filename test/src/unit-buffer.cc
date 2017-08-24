#include <buffer.h>
#include <catch.hpp>

using namespace tiledb;

TEST_CASE("Default constructor with write void*", "[buffer]") {
  Status st;
  char data[3] = {1, 2, 3};
  Buffer* buff = new Buffer();
  CHECK(buff->size() == 0);
  st = buff->write(data, 3);
  REQUIRE(st.ok());
  CHECK(buff->size() == 3);
  buff->reset_offset();
  // read a single value
  char val = 0;
  st = buff->read(&val, 1);
  REQUIRE(st.ok());
  CHECK(val == 1);
  // read two values
  char readtwo[2] = {0, 0};
  st = buff->read(readtwo, 2);
  REQUIRE(st.ok());
  CHECK(readtwo[0] == 2);
  CHECK(readtwo[1] == 3);
  delete buff;
}

TEST_CASE("Pre-allocated size with write void*", "[buffer]") {
  Status st;
  char data[3] = {1, 2, 3};
  Buffer* buff = new Buffer(3);
  CHECK(buff->size() == 3);
  st = buff->write(data, 3);
  REQUIRE(st.ok());
  CHECK(buff->size() == 3);
  buff->reset_offset();
  // read a single value
  char val[1] = {0};
  st = buff->read(val, 1);
  REQUIRE(st.ok());
  CHECK(val[0] == 1);
  // read two values
  char readtwo[2] = {0, 0};
  st = buff->read(readtwo, 2);
  REQUIRE(st.ok());
  CHECK(readtwo[0] == 2);
  CHECK(readtwo[1] == 3);
  delete buff;
}
