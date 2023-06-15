set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
  set(VCPKG_BUILD_TYPE "release")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  set(VCPKG_BUILD_TYPE "debug")
else()
  message(STATUS "vcpkg does not support '${CMAKE_BUILD_TYPE}' or unset; using 'release'")
  set(VCPKG_BUILD_TYPE "release")
endif()

set(X_VCPKG_APPLOCAL_DEPS_INSTALL ON)
