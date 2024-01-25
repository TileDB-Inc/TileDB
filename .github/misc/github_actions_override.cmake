if (WIN32)
  # This override causes CMake to ignore strawberry perl paths in the Github Actions images
  # due to incompatible, mingw-built libraries which are picked up by find_package
  # - upstream issue: https://github.com/actions/runner-images/issues/6627
  # - likely due to changes in CMake 3.25

  set(CMAKE_IGNORE_PREFIX_PATH "C:/Strawberry")
  set(CMAKE_IGNORE_PATH "C:/Strawberry/perl/bin;C:/Strawberry/c/lib")
endif()
