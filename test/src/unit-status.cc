#include <status.h>
#include <catch.hpp>

using namespace tiledb;

TEST_CASE("Status: Test ok", "[status]") {
  Status st = Status::Ok();
  CHECK(st.ok());
  st = Status::Error("err msg");
  CHECK(!st.ok());
}

TEST_CASE("Status: Test code and message", "[status]") {
  Status ok = Status::Ok();
  CHECK(StatusCode::Ok == ok.code());

  Status err = Status::Error("err msg");
  CHECK(StatusCode::Error == err.code());
  CHECK_THAT(err.message(), Catch::Equals("err msg"));
}

TEST_CASE("Status: Test to_string", "[status]") {
  Status ok = Status::Ok();
  CHECK_THAT(ok.to_string(), Catch::Equals("Ok"));

  Status err = Status::Error("err msg");
  CHECK_THAT(err.to_string(), Catch::Equals("Error: err msg"));
}

TEST_CASE("Status: Test code_to_string", "[status]") {
  Status ok = Status::Ok();
  CHECK_THAT(ok.code_to_string(), Catch::Equals("Ok"));

  Status err = Status::Error("err message");
  CHECK_THAT(err.code_to_string(), Catch::Equals("Error"));
}

TEST_CASE("Status: Test posix_code", "[status]") {
  Status st = Status::Ok();
  // check that posix code is < 0 by default
  CHECK(st.posix_code() == -1);

  st = Status::Error("err msg");
  CHECK(st.posix_code() == -1);
}
