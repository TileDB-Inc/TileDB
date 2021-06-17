#tool chain cmake file for arm64

set(CMAKE_CROSSCOMPILING TRUE CACHE INTERNAL "")    # stop recursion
set(IOS_ARCH "arm64" CACHE INTERNAL "arch is arm64 for ios")

# Standard settings.
set(CMAKE_SYSTEM_NAME Darwin CACHE INTERNAL "") # iOS is unknown to cmake. if use Darwin, macOS sdk sysroot will be set
set(CMAKE_SYSTEM_PROCESSOR arm64 CACHE INTERNAL "")
#set(CMAKE_SYSTEM_VERSION ${IOS_SDK_VERSION})
set(UNIX TRUE CACHE INTERNAL "")
set(APPLE TRUE CACHE INTERNAL "")
#set(IOS TRUE CACHE INTERNAL "")
 
# Skip checks
set(CMAKE_CXX_COMPILER_ID_RUN TRUE CACHE INTERNAL "")
set(CMAKE_CXX_COMPILER_FORCED TRUE CACHE INTERNAL "")
set(CMAKE_CXX_COMPILER_WORKS TRUE CACHE INTERNAL "")
set(CMAKE_CXX_COMPILER_ID Clang CACHE INTERNAL "")

set(CMAKE_C_COMPILER_ID_RUN TRUE CACHE INTERNAL "")
set(CMAKE_C_COMPILER_FORCED TRUE CACHE INTERNAL "")
set(CMAKE_C_COMPILER_WORKS TRUE CACHE INTERNAL "")
set(CMAKE_C_COMPILER_ID Clang CACHE INTERNAL "")

set(USED_SDK_NAME "macosx")
# Get the SDK name
if(NOT XCODEBUILD_PLATFORM_VERSION)
  message(STATUS "Detecting version...")
  foreach(var_loop IN LISTS XCODE_SDK_LIST) 
    string(REGEX REPLACE "([a-z]+)[0-9\\.]+" "\\1" CURRENT_LOOP_PLATFORM "${var_loop}")
    if("${CURRENT_LOOP_PLATFORM}" STREQUAL "${USED_SDK_NAME}")
      string(REGEX REPLACE "[a-z]+([0-9\\.]+)" "\\1" XCODEBUILD_PLATFORM_VERSION "${var_loop}")
      break()
    endif()
  endforeach()
endif()

# SDK
set(FULL_SDK_NAME "${USED_SDK_NAME}${XCODEBUILD_PLATFORM_VERSION}")
message(STATUS "toolchain clang-arm64 FULL_SDK_NAME:${FULL_SDK_NAME}")

# Get sysroot
execute_process(COMMAND xcodebuild -version -sdk ${FULL_SDK_NAME} Path
OUTPUT_VARIABLE XARCH_SYSROOT
ERROR_QUIET
OUTPUT_STRIP_TRAILING_WHITESPACE)

if(NOT XARCH_SYSROOT)
  message(FATAL_ERROR "Impossible to get the full path for ${FULL_SDK_NAME}")
endif()
set(CMAKE_OSX_SYSROOT ${XARCH_SYSROOT} CACHE INTERNAL "")
message(STATUS "toolchain clang-arm64 CMAKE_OSX_SYSROOT:${CMAKE_OSX_SYSROOT}")

execute_process(COMMAND xcodebuild -version
  OUTPUT_VARIABLE XCODE_VERSION
  ERROR_QUIET
  OUTPUT_STRIP_TRAILING_WHITESPACE)
string(REGEX MATCH "Xcode [0-9\\.]+" XCODE_VERSION "${XCODE_VERSION}")
string(REGEX REPLACE "Xcode ([0-9\\.]+)" "\\1" XCODE_VERSION "${XCODE_VERSION}")
message(STATUS "toolchain clang-arm64 Building with Xcode version: ${XCODE_VERSION}")

  
set(CMAKE_C_COMPILER clang CACHE INTERNAL "")
set(CMAKE_CXX_COMPILER clang++ CACHE INTERNAL "")

# Set the C Compiler
#execute_process(COMMAND xcrun -sdk ${FULL_SDK_NAME} -find clang
#OUTPUT_VARIABLE CMAKE_C_COMPILER
#ERROR_QUIET
#OUTPUT_STRIP_TRAILING_WHITESPACE)

# Set the C++ Compiler
#execute_process(COMMAND xcrun -sdk ${FULL_SDK_NAME} -find clang++
#OUTPUT_VARIABLE CMAKE_CXX_COMPILER
#ERROR_QUIET
#OUTPUT_STRIP_TRAILING_WHITESPACE)

message(STATUS "toolchain clang-arm64 C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "toolchain clang-arm64 CXX Compiler: ${CMAKE_CXX_COMPILER}")

# XCODE_SDK_LIST
execute_process(COMMAND xcodebuild -showsdks
  OUTPUT_VARIABLE XCODE_SDK_LIST
  ERROR_QUIET
)
message(STATUS "toolchain clang-arm64 XCODE_SDK_LIST:${XCODE_SDK_LIST}")


# required as of cmake 2.8.10.
set(CMAKE_OSX_DEPLOYMENT_TARGET "" CACHE STRING "Must be empty for iOS builds." FORCE)
# Set the architectures for which to build. required by try_compile()
set(CMAKE_OSX_ARCHITECTURES ${IOS_ARCH} CACHE STRING "iOS architectures" FORCE)

set(XARCH_CFLAGS "${XARCH_CFLAGS} -arch ${IOS_ARCH}  -isysroot${XARCH_SYSROOT}")
set(XARCH_LFLAGS "${XARCH_LFLAGS} -arch ${IOS_ARCH}  -Wl,-syslibroot,${XARCH_SYSROOT}")


# Set the ranlib tool
execute_process(COMMAND xcrun -sdk ${FULL_SDK_NAME} -find ranlib
OUTPUT_VARIABLE CMAKE_RANLIB
ERROR_QUIET
OUTPUT_STRIP_TRAILING_WHITESPACE)

# Set the ar tool
execute_process(COMMAND xcrun -sdk ${FULL_SDK_NAME} -find ar
OUTPUT_VARIABLE CMAKE_AR
ERROR_QUIET
OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(COMMAND xcrun -sdk ${FULL_SDK_NAME} -find libtool
OUTPUT_VARIABLE IOS_LIBTOOL
ERROR_QUIET
OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Using libtool: ${IOS_LIBTOOL}")
# Configure libtool to be used instead of ar + ranlib to build static libraries.
# This is required on Xcode 7+, but should also work on previous versions of
# Xcode.
set(CMAKE_C_CREATE_STATIC_LIBRARY
"${IOS_LIBTOOL} -static -o <TARGET> <LINK_FLAGS> <OBJECTS> ")
set(CMAKE_CXX_CREATE_STATIC_LIBRARY
"${IOS_LIBTOOL} -static -o <TARGET> <LINK_FLAGS> <OBJECTS> ")

set(CMAKE_C_FLAGS "${XARCH_CFLAGS} -fPIC" CACHE INTERNAL "ios c compiler flags" FORCE) # -fobjc-arc
set(CMAKE_CXX_FLAGS "${XARCH_CFLAGS} -fPIC" CACHE INTERNAL "ios c compiler flags" FORCE) # -fobjc-arc
 
# Flags - Needs to force
set(CMAKE_OSX_SYSROOT ${CMAKE_OSX_SYSROOT} CACHE INTERNAL "")
set(CMAKE_OSX_ARCHITECTURES ${IOS_ARCH} CACHE INTERNAL "")
set(CMAKE_FIND_ROOT_PATH ${CMAKE_OSX_SYSROOT} CACHE INTERNAL "")

message(STATUS "toolchain ios IOS_ARCH: ${IOS_ARCH}")
message(STATUS "toolchain ios XARCH_FLAGS: ${XARCH_FLAGS}")
message(STATUS "toolchain iOS CMAKE_C_FLAGS:${CMAKE_C_FLAGS}")
message(STATUS "toolchain iOS CMAKE_CXX_FLAGS:${CMAKE_CXX_FLAGS}")