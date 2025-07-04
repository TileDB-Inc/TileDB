#
# tiledb/sm/rest/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2024-2025 TileDB, Inc.
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

include(common NO_POLICY_SCOPE)
include(object_library)

#
# Object library `rest_profile`
#
commence(object_library rest_profile)
  this_target_sources(rest_profile.cc)
  this_target_object_libraries(constants home_directory seedable_global_PRNG)
conclude(object_library)

#
# Object library `rest_client`
#
commence(object_library rest_client)
  this_target_sources(rest_client.cc)
  this_target_object_libraries(config rest_profile)
conclude(object_library)

#
# Object library `rest_client_remote`
#
# This object library does not link standalone at present. As long as the
# cyclic dependencies in `class RestClient` persist, that won't be possible.
#

add_test_subdirectory()
