#
# FindMagic.cmake
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
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
# Completement FindMagic_EP.cmake and finds and uses external libmagic and header
# This module defines:
#   - MAGIC_INCLUDE_DIR, directory containing headers
#   - MAGIC_LIBRARIES, the Magic library path
#   - MAGIC_FOUND, whether Magic has been found
#   - libmagic_LibraryName used as a target in the main CMakeLists.txt
#   - libmagic_DICTIONARY, whether magic.mgc has been found

##debug messages to be removed when finalized
##message(STATUS "Start FindMagic.cmake")
if (NOT TILEDB_LIBMAGIC_EP_BUILT)
  ## As a MVP use pkg-config, can switch to standard library / header search
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(MAGIC IMPORTED_TARGET libmagic)
  if (MAGIC_FOUND)
    ## we need to set his to 'magic' as the system library is 'libmagic'
    ## the external project builds a library 'liblibmagic' and needs 'libmagic'
    ## NB not currently used in CMakeLists.txt
    set(libmagic_LibraryName "magic")
  endif()
  find_file(libmagic_DICTIONARY "magic.mgc" PATHS "/usr/lib/file")
endif()
##message(STATUS "End FindMagic.cmake")
