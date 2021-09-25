# ~~~
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ~~~

option(GOOGLE_CLOUD_CPP_ENABLE_WERROR
       "If set, compiles the library with -Werror and /WX (MSVC)." OFF)
mark_as_advanced(GOOGLE_CLOUD_CPP_ENABLE_WERROR)

# Find out what flags turn on all available warnings and turn those warnings
# into errors.
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wall GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WALL)
check_cxx_compiler_flag(-Wextra GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WEXTRA)
check_cxx_compiler_flag(-Wconversion
                        GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WCONVERSION)
check_cxx_compiler_flag(-Wno-sign-conversion
                        GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WNO_SIGN_CONVERSION)
check_cxx_compiler_flag(-Werror GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WERROR)

function (google_cloud_cpp_add_common_options target)
    if (MSVC)
        target_compile_options(${target} PRIVATE "/W3")
        if (GOOGLE_CLOUD_CPP_ENABLE_WERROR)
            target_compile_options(${target} PRIVATE "/WX")
        endif ()
        target_compile_options(${target} PRIVATE "/experimental:external")
        target_compile_options(${target} PRIVATE "/external:W0")
        target_compile_options(${target} PRIVATE "/external:anglebrackets")
        target_compile_options(${target} PRIVATE "/bigobj")
#        target_compile_options(${target} PRIVATE "/W1") # suppress all warnings
        return()
    endif ()
    if (GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WALL)
        target_compile_options(${target} PRIVATE "-Wall")
    endif ()
    if (GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WEXTRA)
        target_compile_options(${target} PRIVATE "-Wextra")
    endif ()
    if (GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WCONVERSION)
        target_compile_options(${target} PRIVATE "-Wconversion")
    endif ()
    if (GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WNO_SIGN_CONVERSION)
        target_compile_options(${target} PRIVATE "-Wno-sign-conversion")
    endif ()
    if (GOOGLE_CLOUD_CPP_COMPILER_SUPPORTS_WERROR
        AND GOOGLE_CLOUD_CPP_ENABLE_WERROR)
        target_compile_options(${target} PRIVATE "-Werror")
    endif ()
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU"
        AND "${CMAKE_CXX_COMPILER_VERSION}" VERSION_LESS 5.0)
        # With GCC 4.x this warning is too noisy to be useful.
        target_compile_options(${target}
                               PRIVATE "-Wno-missing-field-initializers")
    endif ()
endfunction ()
