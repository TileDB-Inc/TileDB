#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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

add_executable(flat flat.cc query.h defs.h sift_db.h timer.h sift_array.h)

target_link_libraries(flat PUBLIC docopt)
target_link_libraries(flat PUBLIC flat_tbb)
target_link_libraries(flat PUBLIC flat_blas)
target_link_libraries(flat PUBLIC TileDB::tiledb_shared)

# add_executable(ingest ingest.cc sift_db.h timer.h)
# target_include_directories(ingest PUBLIC
#     /Users/lums/Contrib/dist/include
# )
# target_link_libraries(ingest PUBLIC docopt TileDB::tiledb_shared)


#
# Some debugging code
#
# Get the compile options for flat
# get_target_property(TargetCompileOptions flat COMPILE_OPTIONS)

# Print the compile options
# message("Compile options for flat:")
# foreach(Option ${TargetCompileOptions})
#     message("  ${Option}")
# endforeach()

# message (STATUS ${MKL_THREAD_LIB})
# message (STATUS ${INTERFACE_COMPILE_OPTIONS})
# message (STATUS ${INTERFACE_INCLUDE_DIRECTORIES})
