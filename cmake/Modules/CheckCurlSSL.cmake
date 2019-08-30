cmake_push_check_state()

include(CheckCSourceRuns)

set(CMAKE_REQUIRED_LIBRARIES ${CURL_APPEND_LIBS} ${OPENSSL_SSL_LIBRARY})
set(CMAKE_REQUIRED_INCLUDES ${CURL_INCLUDE_DIR} ${OPENSSL_INCLUDE_DIR})

# suppress warnings, this is a runtime check
set(CMAKE_REQUIRED_FLAGS -w)

if (NOT WIN32)
  list(APPEND CMAKE_REQUIRED_LIBRARIES -ldl -lpthread)
endif()

check_c_source_runs(
  "#include <stdlib.h>
  #include <curl/curl.h>
  int main() {
    curl_version_info_data* vers = curl_version_info(CURLVERSION_NOW);
    if (!(vers->features & CURL_VERSION_SSL)) {
      exit(1);
    }
    exit(0);
  }"
  TILEDB_CURL_HAS_SSL
)

if (NOT TILEDB_CURL_HAS_SSL)
  message(FATAL_ERROR "Target libcurl does not provide SSL -- HTTPS will be broken! "
                      "(override with TILEDB_CURL_HAS_SSL=1) "
                      "Note: output of this check will be in CMakeFiles/CMake[Output,Error].log")
endif()
cmake_pop_check_state()
