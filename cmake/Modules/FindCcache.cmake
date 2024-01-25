## cf https://stackoverflow.com/questions/1815688/how-to-use-ccache-with-cmake
## leading to https://invent.kde.org/kde/konsole/merge_requests/26/diffs
find_program(CCACHE_FOUND NAMES "sccache" "ccache")
set(CCACHE_SUPPORT ON CACHE BOOL "Enable ccache support")
if (CCACHE_FOUND AND CCACHE_SUPPORT)
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" # GNU is GNU GCC
      OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # without this compiler messages in `make` backend would be uncolored
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fdiagnostics-color=auto")
  endif()
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
  message(STATUS "Found ccache: ${CCACHE_FOUND}")
else()
  message(FATAL_ERROR "Unable to find ccache")
endif()
