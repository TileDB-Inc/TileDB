#
# tiledb/sm/crypto/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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

include(common NO_POLICY_SCOPE)
include(object_library)

#
# `crypto` object library
#
commence(object_library tiledb_crypto)
    this_target_sources(crypto.cc crypto_openssl.cc crypto_win32.cc encryption_key.cc)
    this_target_object_libraries(buffer config constants)
    if(WIN32)
        this_target_link_libraries(bcrypt)
    else()
        find_package(OpenSSL REQUIRED)
        # We cannot use this_target_link_libraries, because it links with PUBLIC
        # visibility, and we use OpenSSL only as an internal implementation detail.
        target_link_libraries(tiledb_crypto PRIVATE OpenSSL::Crypto)
    endif()
    # OpenSSL-3 deprecates MD5
    if(MSVC)
        set_source_files_properties(crypto_openssl.cc PROPERTIES COMPILE_OPTIONS "/wd4996")
    else()
        set_source_files_properties(crypto_openssl.cc PROPERTIES COMPILE_OPTIONS "-Wno-deprecated-declarations")
    endif()
conclude(object_library)

add_test_subdirectory()
