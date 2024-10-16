#
# CMake Toolchain file for crosscompiling on ARM.
#
# Target operating system name.
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

set(TARGET_CC $ENV{TARGET_CC})
set(TARGET_CXX $ENV{TARGET_CXX})

if (NOT TARGET_CC)
  set(TARGET_CC "aarch64-unknown-linux-gnu-gcc")
endif()

if (NOT TARGET_CXX)
  set(TARGET_CXX "aarch64-unknown-linux-gnu-g++")
endif()

# Name of C compiler.
set(CMAKE_C_COMPILER "${TARGET_CC}")
set(CMAKE_CXX_COMPILER "${TARGET_CXX}")
