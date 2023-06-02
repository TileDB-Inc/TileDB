cmake_minimum_required(VERSION 3.7)

INCLUDE(CheckIncludeFile)

project(file)

option(FILE_TESTS "Enable file tests" OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if (BUILD_MAGIC_MACOS_UNIVERSAL)
  set(CMAKE_OSX_ARCHITECTURES "arm64;x86_64")
endif()

# Get library directory for multiarch linux distros
include(GNUInstallDirs)

function(cat IN_FILE OUT_FILE)
  file(READ ${IN_FILE} CONTENTS)
  file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")
set(targets)
find_package(pcre2 REQUIRED)

option(BUILD_SHARED_LIBS "build as shared library" ON)

set(LIBMAGIC_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/file/src")
set(LIBMAGIC_PATCHED_FILES "${CMAKE_CURRENT_BINARY_DIR}/patched")
set(LIBMAGIC_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/file/tests")
set(WIN_HEADERS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/win-headers")
set(PCRE2_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/pcre2/install_dir/lib")

set(FILE_VERSION 5.39)

set(WIN_COMPAT_SOURCES
  ${LIBMAGIC_SOURCE_DIR}/asctime_r.c
  ${LIBMAGIC_SOURCE_DIR}/asprintf.c
  ${LIBMAGIC_SOURCE_DIR}/ctime_r.c
  ${LIBMAGIC_SOURCE_DIR}/dprintf.c
  ${LIBMAGIC_SOURCE_DIR}/getline.c
  ${LIBMAGIC_SOURCE_DIR}/gmtime_r.c
  ${LIBMAGIC_SOURCE_DIR}/localtime_r.c
  ${LIBMAGIC_SOURCE_DIR}/pread.c
  ${LIBMAGIC_SOURCE_DIR}/strcasestr.c
#  ${LIBMAGIC_SOURCE_DIR}/strlcat.c
#  ${LIBMAGIC_SOURCE_DIR}/strlcpy.c
  ${LIBMAGIC_SOURCE_DIR}/vasprintf.c
#  ${LIBMAGIC_SOURCE_DIR}/fmtcheck.c
  ${WIN_HEADERS_DIR}/magic.def
)

if(WIN32)
  set(LIBMAGIC_SOURCE_FILES
    ${WIN_COMPAT_SOURCES}
  )
else()
  set(LIBMAGIC_SOURCE_FILES
  )
endif()

#set(LIBMAGIC_SOURCE_FILES
#  ${WIN_COMPAT_SOURCES}
list(APPEND LIBMAGIC_SOURCE_FILES
  ${LIBMAGIC_SOURCE_DIR}/apprentice.c
  ${LIBMAGIC_SOURCE_DIR}/apptype.c
  ${LIBMAGIC_SOURCE_DIR}/ascmagic.c
  ${LIBMAGIC_SOURCE_DIR}/buffer.c
  ${LIBMAGIC_SOURCE_DIR}/cdf.c
  ${LIBMAGIC_SOURCE_DIR}/cdf_time.c
  ${LIBMAGIC_SOURCE_DIR}/compress.c
  ${LIBMAGIC_SOURCE_DIR}/der.c
  ${LIBMAGIC_SOURCE_DIR}/encoding.c
  ${LIBMAGIC_SOURCE_DIR}/fmtcheck.c
  ${LIBMAGIC_SOURCE_DIR}/fsmagic.c
  ${LIBMAGIC_SOURCE_DIR}/funcs.c
  ${LIBMAGIC_SOURCE_DIR}/is_json.c
  ${LIBMAGIC_SOURCE_DIR}/is_tar.c
  ${CMAKE_CURRENT_BINARY_DIR}/magic.c
  ${LIBMAGIC_SOURCE_DIR}/print.c
  ${LIBMAGIC_SOURCE_DIR}/readcdf.c
  ${CMAKE_CURRENT_BINARY_DIR}/readelf.c
  ${LIBMAGIC_SOURCE_DIR}/softmagic.c
#  ${LIBMAGIC_SOURCE_DIR}/strlcat.c
#  ${LIBMAGIC_SOURCE_DIR}/strlcpy.c
  ${LIBMAGIC_SOURCE_DIR}/is_csv.c
)
if(NOT APPLE)
list(APPEND LIBMAGIC_SOURCE_FILES
  ${LIBMAGIC_SOURCE_DIR}/strlcat.c
  ${LIBMAGIC_SOURCE_DIR}/strlcpy.c
)
endif()

# replace the version in the magic.h.in and write it to magic.h
FILE(READ ${LIBMAGIC_SOURCE_DIR}/magic.h.in MAGIC_H_CONTENT)
STRING(REPLACE "." "" FILE_VERSION_WITHOUT_DOT "${FILE_VERSION}")
STRING(REPLACE "X.YY" ${FILE_VERSION_WITHOUT_DOT} MAGIC_H_CONTENT_NEW "${MAGIC_H_CONTENT}")
FILE(WRITE ${LIBMAGIC_SOURCE_DIR}/magic.h "${MAGIC_H_CONTENT_NEW}")

if(0)
# TBD: Is the following really a problem for us?
# rtools40/msys2 cdf_ctime from file/src/cdf.c may yield bogus output as input size is not correct
if(MSYS)
FILE(READ ${LIBMAGIC_SOURCE_DIR}/cdf.h CDF_H_CONTENT)
STRING(REPLACE "#ifdef WIN32" "#if defined(WIN32) && !defined(__MINGW32__)" CDF_H_CONTENT_NEW ${CDF_H_CONTENT})
FILE(WRITE ${LIBMAGIC_SOURCE_DIR}/cdf.h "${CDF_H_CONTENT_NEW}")
endif()
endif()

FILE(READ ${LIBMAGIC_SOURCE_DIR}/readelf.c READELF_C_CONTENT)
#if(WIN32)
if(MSVC)
STRING(CONCAT READELF_C_CONTENT_NEW "#include <dirent.h>\n" "${READELF_C_CONTENT}")
else()
set( READELF_C_CONTENT_NEW "${READELF_C_CONTENT}")
endif()
FILE(WRITE ${CMAKE_CURRENT_BINARY_DIR}/readelf.c "${READELF_C_CONTENT_NEW}")

FILE(READ ${LIBMAGIC_SOURCE_DIR}/magic.c MAGIC_C_CONTENT)
#if(WIN32)
if(MSVC)
STRING(CONCAT MAGIC_C_CONTENT_NEW "#include <dirent.h>\n" "${MAGIC_C_CONTENT}")
else()
set( MAGIC_C_CONTENT_NEW "${MAGIC_C_CONTENT}")
endif()
FILE(WRITE ${CMAKE_CURRENT_BINARY_DIR}/magic.c "${MAGIC_C_CONTENT_NEW}")

FILE(READ ${LIBMAGIC_SOURCE_DIR}/file.h FILE_H_CONTENT)
if(MSVC)
STRING(CONCAT FILE_H_CONTENT_NEW "#ifdef WIN32\n#include <unistd.h>\n#include <dirent.h>\n#undef S_IFLNK\n#undef S_IFSOCK\n#endif\n" "${FILE_H_CONTENT}")
else()
#set( FILE_H_CONTENT_NEW "${FILE_H_CONTENT}")
STRING(CONCAT FILE_H_CONTENT_NEW "#include <config.h>\n" "${FILE_H_CONTENT}")
endif()
FILE(WRITE ${LIBMAGIC_PATCHED_FILES}/file.h "${FILE_H_CONTENT_NEW}")

if (NOT WIN32)
#if ((NOT WIN32) OR MSYS)
# nasty, nasty...
# so, this windows port has it's own unistd.h for windows...
# and the path to include is searched -before- system includes...
# and we need that local search path because it has other items besides
# the unistd.h that are needed (at least 'config.h')...
# not having found any way to handle both ubuntu/mac, ask cmake
# (ubuntu, include </usr/include/unistd.h> worked, but not on mac)
# to find it and replace our local one with its contents
# or fail if not found.
  if(MSYS)
  find_path(unistd_INCLUDE_DIR
    NAMES unistd.h
#    PATH_SUFFIXES include
#    /usr
  )
  else()
  find_path(unistd_INCLUDE_DIR
    NAMES unistd.h
    PATH_SUFFIXES include
    /usr
  )
  endif()
  if (unistd_INCLUDE_DIR-NOTFOUND)
    message(FATAL_ERROR "Unable to find needed unistd.h!")
  endif()
  FILE(READ ${unistd_INCLUDE_DIR}/unistd.h UNISTD_H_CONTENT)
  FILE(WRITE ${WIN_HEADERS_DIR}/unistd.h "${UNISTD_H_CONTENT}")
endif()

#add_definitions(-DHAVE_CONFIG_H -DVERSION="${FILE_VERSION}" -DWIN32_LEAN_AND_MEAN -DWIN32 -DPCRE2_STATIC )
add_definitions(-DHAVE_CONFIG_H -DVERSION="${FILE_VERSION}" -DPCRE2_STATIC )
if(WIN32)
  add_definitions(-DWIN32_LEAN_AND_MEAN -DWIN32)
endif()

#add_library(libmagic SHARED ${LIBMAGIC_SOURCE_FILES})
#include_directories (${WIN_HEADERS_DIR} pcre2/src ${LIBMAGIC_SOURCE_DIR} dirent/include getopt)
#target_link_libraries(libmagic pcre2-posix shlwapi)

#if (NOT WIN32)
#  list(APPEND LIBMAGIC_SOURCE_FILES
#    /usr/include/unistd.h
#  )
#endif()

if(BUILD_SHARED_LIBS AND NOT libmagic_STATIC_LIB)
message(STATUS "Building libmagic SHARED")
  add_library(libmagic SHARED ${LIBMAGIC_SOURCE_FILES})
else()
message(STATUS "Building libmagic STATIC")
  add_library(libmagic STATIC ${LIBMAGIC_SOURCE_FILES})
endif()
set(targets ${targets} libmagic)

#include_directories (${WIN_HEADERS_DIR} ${PCRE2_INCLUDE_DIRS} ${LIBMAGIC_SOURCE_DIR} dirent/include getopt)
if(WIN32)
  include_directories (${WIN_HEADERS_DIR} dirent/include)
endif()
include_directories (${LIBMAGIC_SOURCE_DIR})
#if(WIN32)
if(MSVC)
  include_directories (getopt)
else()
  # for NOT WIN32 currently only expecting to need config.h from win-headers, if
  # anything else found/needed then we're prob. still not correct xplat...
# this 'standard' directory location seems to be ignored, not added directly by cmake?
# maybe gcc (clang?) sposed to search by default?
  # note: presence of /usr/include can lead to inclusion of system pcre2*.h headers
  # rather than the local ones we want found.  This is compensated for below by adding
  # the local directories -before- everything else.
#  include_directories (BEFORE SYSTEM /usr/include ) # local unistd.h hopefully found here
  include_directories (AFTER ${WIN_HEADERS_DIR})
endif()

include_directories(${LIBMAGIC_PATCHED_FILES})

#include_directories (pcre2/src file/src getopt)
#include_directories (BEFORE pcre2/src file/src)
include_directories (pcre2/src file/src)

#target_link_libraries(libmagic PRIVATE ${PCRE2_LIBRARIES} ${PCRE2_POSIX_LIBRARIES} shlwapi)
#target_link_libraries(libmagic PRIVATE pcre2-posix pcre2-8 shlwapi)
if(WIN32)
  target_link_libraries(libmagic pcre2-posix pcre2-8 shlwapi)
else()
  if(NOT APPLE)
#    target_link_libraries(libmagic pcre2-posix pcre2-8 bsd)
    target_link_libraries(libmagic pcre2-posix pcre2-8)
  else()
    # strlcat,strlcpy seem to be in headers (intrinsic) somewhere already
    # and apple complains about 'bsd' lib not found
    target_link_libraries(libmagic pcre2-posix pcre2-8)
  endif()
endif()

target_include_directories(libmagic
        PUBLIC
        "$<BUILD_INTERFACE:${LIBMAGIC_SOURCE_DIR}>"
        "$<INSTALL_INTERFACE:include>"
)

# copy /pcre2posix.h to regex.h so it can be used as posix regex libary if regex.h does not exist
if(1)
  # tiledb always building local pcre, so always perform this copy.
  message("copying ${CMAKE_CURRENT_SOURCE_DIR}/pcre2/src/pcre2posix.h to ${CMAKE_CURRENT_SOURCE_DIR}/file/src/regex.h")
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/pcre2/src/pcre2posix.h ${LIBMAGIC_PATCHED_FILES}/regex.h COPYONLY)
else()
  if (NOT PCRE2_INCLUDE_DIRS)
    GET_TARGET_PROPERTY(PCRE2_INCLUDE_DIRS pcre2-posix INCLUDE_DIRECTORIES)
  endif()
  CHECK_INCLUDE_FILE("regex.h" REGEX_HEADER_FOUND)
  if(NOT REGEX_HEADER_FOUND)
    foreach(DIR ${PCRE2_INCLUDE_DIRS})
      if(EXISTS ${DIR}/pcre2posix.h)
        configure_file(${DIR}/pcre2posix.h ${DIR}/regex.h COPYONLY)
        set(REGEX_HEADER_FOUND TRUE)
        break()
      endif()
    endforeach()
    if(NOT REGEX_HEADER_FOUND)
      message("copying ${CMAKE_CURRENT_SOURCE_DIR}/pcre2/src/pcre2posix.h to ${CMAKE_CURRENT_SOURCE_DIR}/file/src/regex.h")
      configure_file(${CMAKE_CURRENT_SOURCE_DIR}/pcre2/src/pcre2posix.h ${CMAKE_CURRENT_SOURCE_DIR}/file/src/regex.h COPYONLY)
    endif()
  endif()
endif()

# 'file' CLI
add_executable(file ${LIBMAGIC_SOURCE_DIR}/file.c)
set(targets ${targets} file)
target_link_libraries(file libmagic pcre2-posix)
if (WIN32)
  target_link_libraries(file shlwapi)
endif()

# Tests
if (FILE_TESTS)
  add_executable(file_test ${LIBMAGIC_TEST_DIR}/test.c)
  set(targets ${targets} file_test)
  if(WIN32)
  target_link_libraries(file_test libmagic pcre2-posix shlwapi)
  else()
  target_link_libraries(file_test libmagic pcre2-posix )
  endif()

endif()
#target_link_libraries(file libmagic pcre2-posix shlwapi)

# this tests all because of time-zone or crlf errors
set(DISABLED_TESTS
  gedcom
  fit-map-data
  regex-eol
  )

if (FILE_TESTS)
  enable_testing()

  file(GLOB TESTFILES "${LIBMAGIC_TEST_DIR}/*.testfile")
  foreach(TEST_FILE ${TESTFILES})
    # extract testname from path and generate command
    string(REGEX MATCH  "(.*)\.testfile" TESTPATH ${TEST_FILE})
    set(TESTPATH ${CMAKE_MATCH_1})
    string(REGEX MATCH  "([a-zA-Z0-9_]|-|\\.)+$" TESTNAME ${TESTPATH})
    if(NOT ${TESTNAME} IN_LIST DISABLED_TESTS)
        add_test(NAME ${TESTNAME} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/file_test ${TEST_FILE} ${TESTPATH}.result)
    endif()
  endforeach()

endif()

# Following is the compilation of the magic file
set(MAGIC_FRAGMENT_DIR file/magic/Magdir)
file(GLOB MAGIC_FRAGMENTS ${MAGIC_FRAGMENT_DIR}/*)

# Prepare a temporary file to "cat" to:
file(WRITE magic.in "")

# Call the "cat" function for each input file
foreach(MAGIC_FRAGMENT ${MAGIC_FRAGMENTS})
#  message(STATUS "magic frag - ${MAGIC_FRAGMENT}")
  cat(${MAGIC_FRAGMENT} magic.in)
endforeach()

# Include module with function 'configure_package_config_file'
include(CMakePackageConfigHelpers)

# Copy the temporary file to the final location
configure_file(magic.in magic COPYONLY)


set(FILE_COMMAND "file")
if (DEFINED ENV{FILE_COMMAND_OVERRIDE})
  set(FILE_COMMAND "$ENV{FILE_COMMAND_OVERRIDE}")
endif()

add_custom_command(OUTPUT magic.mgc
  COMMAND ${FILE_COMMAND} -C -m magic
  DEPENDS file
  COMMENT "Compiling magic file"
)

add_custom_target(magic_mgc ALL DEPENDS magic.mgc)

#install(TARGETS file file_test DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/magic.mgc DESTINATION ${CMAKE_INSTALL_BINDIR})

install(FILES ${LIBMAGIC_SOURCE_DIR}/magic.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS ${targets}
        EXPORT libmagic-target
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(EXPORT libmagic-target
        FILE libmagic-targets.cmake
        DESTINATION lib/cmake/libmagic
        )
file(WRITE
        ${CMAKE_BINARY_DIR}/libmagic-config.cmake
        "include(\${CMAKE_CURRENT_LIST_DIR}/libmagic-targets.cmake)\n")
install(FILES
        ${CMAKE_BINARY_DIR}/libmagic-config.cmake
        DESTINATION lib/cmake/libmagic)
