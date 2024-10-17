## cf https://stackoverflow.com/questions/1815688/how-to-use-ccache-with-cmake
## leading to https://invent.kde.org/kde/konsole/merge_requests/26/diffs
find_program(CCACHE REQUIRED NAMES "sccache" "ccache")

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" # GNU is GNU GCC
    OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  # without this compiler messages in `make` backend would be uncolored
  set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fdiagnostics-color=auto")
endif()
