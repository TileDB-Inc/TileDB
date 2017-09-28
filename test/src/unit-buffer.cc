#include <buffer.h>
#include <catch.hpp>

using namespace tiledb;

TEST_CASE("Buffer: Test default constructor with write void*", "[buffer]") {
  // Write a char array
  Status st;
  char data[3] = {1, 2, 3};
  auto buff = new Buffer();
  CHECK(buff->size() == 0);
  st = buff->write(data, sizeof(data));
  REQUIRE(st.ok());
  CHECK(buff->offset() == 3);
  CHECK(buff->size() == sizeof(data));
  CHECK(buff->alloced_size() == 3);
  buff->reset_offset();
  CHECK(buff->offset() == 0);

  // Read a single char value
  char val = 0;
  st = buff->read(&val, sizeof(char));
  REQUIRE(st.ok());
  CHECK(val == 1);
  CHECK(buff->offset() == 1);

  // Read two values
  char readtwo[2] = {0, 0};
  st = buff->read(readtwo, 2);
  REQUIRE(st.ok());
  CHECK(readtwo[0] == 2);
  CHECK(readtwo[1] == 3);
  CHECK(buff->offset() == 3);

  // Reallocate
  st = buff->realloc(10);
  REQUIRE(st.ok());
  CHECK(buff->size() == 3);
  CHECK(buff->alloced_size() == 10);
  CHECK(buff->offset() == 3);

  delete buff;
}
