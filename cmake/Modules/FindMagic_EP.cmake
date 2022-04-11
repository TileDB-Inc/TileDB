#
# FindMagic_EP.cmake
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Finds the Magic library, installing with an ExternalProject as necessary.
# This module defines:
#   - libmagic_INCLUDE_DIR, directory containing headers
#   - libmagic_LIBRARIES, the Magic library path
#   - libmagic_FOUND, whether Magic has been found
#   - The libmagic imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(LIBMAGIC_PATHS ${TILEDB_EP_INSTALL_PREFIX})

if(0)
# Try the builtin find module unless built w/ EP superbuild
if ((NOT TILEDB_FORCE_ALL_DEPS) AND (NOT TILEDB_LIBMAGIC_EP_BUILT) 
  AND (NOT MSYS)) # RTOOLS problematic, build 'ours' which is self-contained
  find_package(libmagic ${TILEDB_DEPS_NO_DEFAULT_PATH} QUIET)
elseif(TILEDB_LIBMAGIC_EP_BUILT)
  find_package(libmagic PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()
else()
if(TILEDB_LIBMAGIC_EP_BUILT)
  find_package(libmagic PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()
endif()

#if (1)
# Next try finding the superbuild external project
#if (NOT libmagic_FOUND)
if (TILEDB_LIBMAGIC_EP_BUILT)
#if ((NOT libmagic_FOUND) AND (NOT MSYS))
  find_path(libmagic_INCLUDE_DIR
    NAMES magic.h
    PATHS ${LIBMAGIC_PATHS}
    PATH_SUFFIXES include
    #${TILEDB_DEPS_NO_DEFAULT_PATH}
    ${NO_DEFAULT_PATH}
  )

  if (NOT libmagic_INCLUDE_DIR)
    find_path(libmagic_INCLUDE_DIR
      NAMES file/file.h
      PATHS ${LIBMAGIC_PATHS}
      PATH_SUFFIXES include
      #${TILEDB_DEPS_NO_DEFAULT_PATH}
      ${NO_DEFAULT_PATH}
    )
  endif()

  # Link statically if installed with the EP.
  find_library(libmagic_LIBRARIES
  # TBD: should 'magic' here be 'libmagic'???
    #magic
    libmagic
    PATHS ${LIBMAGIC_PATHS}
    PATH_SUFFIXES lib a
    #${TILEDB_DEPS_NO_DEFAULT_PATH}
    ${NO_DEFAULT_PATH}
  )

  message(STATUS "dlh - LIBMAGIC_PATHS is ${LIBMAGIC_PATHS}")
  
  find_library(libmagic_pcre2posix_LIBRARIES
    pcre2-posix
    PATHS ${LIBMAGIC_PATHS} ${TILEDB_EP_SOURCE_DIR}/ep_magic
    PATH_SUFFIXES lib a
    #${TILEDB_DEPS_NO_DEFAULT_PATH}
    ${NO_DEFAULT_PATH}
  )
  find_library(libmagic_pcre2_8_LIBRARIES
    pcre2-8
    PATHS ${LIBMAGIC_PATHS} ${TILEDB_EP_SOURCE_DIR}/ep_magic
    PATH_SUFFIXES lib a
    #${TILEDB_DEPS_NO_DEFAULT_PATH}
    ${NO_DEFAULT_PATH}
  )

  message(STATUS "dlh - LIBMAGIC_PATHS is ${LIBMAGIC_PATHS}")
  message(STATUS "dlh - libmagic_LIBRARIES is ${libmagic_LIBRARIES}")
  message(STATUS "dlh - libmagic_INCLUDE_DIR is ${libmagic_INCLUDE_DIR}")
  message(STATUS "dlh - libmagic_pcre2posix_LIBRARIES is ${libmagic_pcre2posix_LIBRARIES}")
  message(STATUS "dlh - libmagic_pcre2_8_LIBRARIES is ${libmagic_pcre2_8_LIBRARIES}")
  
  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(libmagic
    REQUIRED_VARS libmagic_LIBRARIES libmagic_INCLUDE_DIR
  )
endif()
#endif()

# If not found, add it as an external project
#if (NOT libmagic_FOUND)
if(NOT TILEDB_LIBMAGIC_EP_BUILT)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Magic as an external project")

    if (WIN32)
      set(CFLAGS_DEF "")
    else()
      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    endif()

    if (WIN32)
      # For windows we need to use file-windows from julian-r who has build a cmake version and patched for msvc++
      ExternalProject_Add(ep_magic
              PREFIX "externals"
              GIT_REPOSITORY "https://github.com/TileDB-Inc/file-windows.git"
              #GIT_REPOSITORY "d:/dev/tiledb/gh.tdb.file-windows.git"
              #GIT_REPOSITORY "d:/dev/tiledb/gh.tdb.file-windows-SM-D.git"
              #GIT_TAG "cmake-install-support"
              #GIT_TAG "dlh/cmake-install-support" # '/' no worky...
              #GIT_TAG "dlh-cmake-install-support"
              GIT_TAG "nplat-cmake-install-support"
              GIT_SUBMODULES_RECURSE TRUE
              UPDATE_COMMAND ""
              CMAKE_ARGS
                -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
                -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
                -Dlibmagic_STATIC_LIB=ON
              LOG_DOWNLOAD TRUE
              LOG_CONFIGURE TRUE
              LOG_BUILD TRUE
              LOG_INSTALL TRUE
              LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
              )
    else()
      set(findcmdstr  "bash -c find . -name '*.a' -exec ls {} \;")
      ExternalProject_Add(ep_magic
        PREFIX "externals"
        # URL "ftp://ftp.astron.com/pub/file/file-5.41.tar.gz"
        # URL_HASH SHA1=8d80d2d50f4000e087aa60ffae4f099c63376762
        # DOWNLOAD_NAME "file-5.41.tar.gz"
        GIT_REPOSITORY "https://github.com/TileDB-Inc/file-windows.git"
        #GIT_REPOSITORY "/mnt/d/dev/tiledb/gh.tdb.file-windows-SM-D.git"
        #GIT_TAG "cmake-install-support"
        #GIT_TAG "dlh-cmake-install-support"
        GIT_TAG "nplat-cmake-install-support"
        GIT_SUBMODULES_RECURSE TRUE
        UPDATE_COMMAND ""
        #CONFIGURE_COMMAND
        #      ${TILEDB_EP_BASE}/src/ep_magic/configure --prefix=${TILEDB_EP_INSTALL_PREFIX} CFLAGS=${CFLAGS_DEF} CXXFLAGS=${CXXFLAGS_DEF}
        #BUILD_IN_SOURCE TRUE
        BUILD_COMMAND $(MAKE) VERBOSE=1
#        LIST_SEPARATOR ^^
#        COMMAND bash -c "${findcmdstr}"
        #INSTALL_COMMAND $(MAKE) install
        CMAKE_ARGS
          --trace
          -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
          "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
          -Dlibmagic_STATIC_LIB=ON
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      )
      set(findcmdstr  "bash -c find . -name '*.a' -exec ls {} \;")
      set(findcmdstr  "find . -name '*.a' -exec ls {} \;")
      set(findcmdstr  "find . -name '*.a' -exec ls {} \\;")
      set(findargs " . -name '*.a' -exec ls {} \;")
      set(findargs " . -name '*.a' -exec ls {} \\;")
      set(SEMI ";")
      if(0)
      message("${findcmdstr}")
      string(REPLACE ";" "@" findcmdstr "${findcmdstr}")
      message("${findcmdstr}")
      message("${findargs}")
      message("${SEMI}")
      ExternalProject_Add_Step(ep_magic afterbuild_diags
        DEPENDEES build
        COMMAND "pwd"
        #LIST_SEPARATOR ??
        #LIST_SEPARATOR ^^
        LIST_SEPARATOR @
        #COMMAND bash -c "find . -name '*.a' -exec ls {} \\;"
        #COMMAND "${findcmdstr}"
        COMMAND bash -c "echo oneA"
        COMMAND bash -c "echo find . -name '*.a' -exec ls {} \\\;"
        COMMAND bash -c "echo find . -name '*.a' -exec ls {} \\@"
        COMMAND bash -c "echo oneB"
        #COMMAND bash -c echo find . -name '*.a' -exec ls {} ;
        #COMMAND bash -c "echo find . -name '*.a' -exec ls {} \${SEMI}"
        #COMMAND bash -c "echo find . -name '*.a' -exec ls {} \??"
        #COMMAND bash -c "echo find . -name '*.a' -exec ls {} \\??"
        #COMMAND bash -c "echo find . -name '*.a' -exec ls {} \\^^"
        LIST_SEPARATOR ^^
        COMMAND bash -c "echo find . -name '*.a' -exec ls {} \\;"
        COMMAND bash -c "echo find . -name '*.a' -exec ls {} \\^^"
        #COMMAND bash -c "echo find . -name '*.a' -exec ls {} \\;"
        COMMAND bash -c "echo twoA"
        COMMAND bash -c "${findcmdstr}"
        COMMAND bash -c "echo twoAA"
        #COMMAND bash -c "echo find ${findargs}"
        COMMAND bash -c "echo find ${findargs} \\${SEMI}"
        COMMAND bash -c "echo twoB"
        COMMAND bash -c \"echo find ${findargs}\"
        COMMAND bash -c "echo three"
        COMMAND bash -c \"find ${findargs}\"
        COMMAND bash -c "echo four"
        COMMAND "cat ./src/ep_magic-stamp/*.log"
      )
      endif()
    endif()
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_magic)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_LIBMAGIC_EP_BUILT=TRUE
    )

    set(TILEDB_LIBMAGIC_DIR "${TILEDB_EP_INSTALL_PREFIX}")

  else()
    message(FATAL_ERROR "Unable to find Magic")
  endif()
endif()

if (libmagic_FOUND AND NOT TARGET libmagic)
  message(STATUS "Found Magic, adding imported target: ${libmagic_LIBRARIES}")
  add_library(libmagic UNKNOWN IMPORTED)
  set_target_properties(libmagic PROPERTIES
    IMPORTED_LOCATION "${libmagic_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${libmagic_INCLUDE_DIR}"
  )
endif()

if (MSYS AND TILEDB_LIBMAGIC_EP_BUILT)
  target_link_libraries(libmagic  INTERFACE -lregex -ltre -lgettextpo -lgettextlib -lintl -liconv)
endif()

# If we built a static EP, install it if required.
if (TILEDB_LIBMAGIC_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(libmagic)
  # TBD: appropriate? needed? for APPLE? *nix, win finding these, APPLE not...
  install_target_libs(pcre2-posix)
  install_target_libs(pcre2-8)
endif()
