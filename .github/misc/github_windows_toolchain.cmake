# This is an auxiliary file intended to be included by CMake via CMAKE_TOOLCHAIN_FILE
# The purpose is to force CMake to Ignore strawberry perl paths in the Github Actions images
# due to incompatible libraries which are now picked up by find_package
# (possibly due to changes in CMake 3.25)

set(CMAKE_IGNORE_PREFIX_PATH "C:/Strawberry")
set(CMAKE_IGNORE_PATH "C:/Strawberry/perl/bin;C:/Strawberry/c/lib")
