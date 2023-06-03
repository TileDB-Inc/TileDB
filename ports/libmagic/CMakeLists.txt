cmake_minimum_required(VERSION 3.7)

INCLUDE(CheckIncludeFile)

project(file)

option(FILE_TESTS "Enable file tests" OFF)

# Get library directory for multiarch linux distros
include(GNUInstallDirs)

function(cat IN_FILE OUT_FILE)
  file(READ ${IN_FILE} CONTENTS)
  file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")
set(targets)
find_package(PCRE2 CONFIG REQUIRED COMPONENTS 8BIT)
if(WIN32)
  find_package(unofficial-getopt-win32 CONFIG REQUIRED)
endif()

set(LIBMAGIC_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(LIBMAGIC_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/tests")

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
  # ${LIBMAGIC_SOURCE_DIR}/strlcat.c
  # ${LIBMAGIC_SOURCE_DIR}/strlcpy.c
  ${LIBMAGIC_SOURCE_DIR}/vasprintf.c
  ${LIBMAGIC_SOURCE_DIR}/fmtcheck.c
  ${LIBMAGIC_SOURCE_DIR}/magic.def
)

if(WIN32)
  set(LIBMAGIC_SOURCE_FILES ${WIN_COMPAT_SOURCES})
else()
  set(LIBMAGIC_SOURCE_FILES)
endif()

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
  ${LIBMAGIC_SOURCE_DIR}/magic.c
  ${LIBMAGIC_SOURCE_DIR}/print.c
  ${LIBMAGIC_SOURCE_DIR}/readcdf.c
  ${LIBMAGIC_SOURCE_DIR}/readelf.c
  ${LIBMAGIC_SOURCE_DIR}/softmagic.c
  # ${LIBMAGIC_SOURCE_DIR}/strlcat.c
  # ${LIBMAGIC_SOURCE_DIR}/strlcpy.c
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

add_compile_definitions(-DHAVE_CONFIG_H -DVERSION="${FILE_VERSION}")
if(WIN32)
  add_compile_definitions(-DWIN32_LEAN_AND_MEAN -DWIN32)
endif()

add_library(libmagic ${LIBMAGIC_SOURCE_FILES})
set(targets ${targets} libmagic)

if(MSVC)
  include_directories(getopt)
endif()

target_link_libraries(libmagic pcre2::pcre2-posix)
if(WIN32)
  target_link_libraries(libmagic shlwapi unofficial::getopt-win32::getopt)
endif()

target_include_directories(libmagic
  PUBLIC
    "$<BUILD_INTERFACE:${LIBMAGIC_SOURCE_DIR}>"
    "$<INSTALL_INTERFACE:include>"
)

# 'file' CLI
add_executable(file ${LIBMAGIC_SOURCE_DIR}/file.c)
set(targets ${targets} file)
target_link_libraries(file libmagic)
if (WIN32)
  target_link_libraries(file shlwapi)
endif()

# Following is the compilation of the magic file
set(MAGIC_FRAGMENT_DIR magic/Magdir)
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

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/magic.mgc DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/libmagic)

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
