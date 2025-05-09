#
# tiledb/sm/misc/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021-2024 TileDB, Inc.
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

find_package(LibMagic MODULE REQUIRED)

#
# `cancelable_tasks` object library
#
commence(object_library cancelable_tasks)
    this_target_sources(cancelable_tasks.cc)
    this_target_object_libraries(baseline thread_pool)
conclude(object_library)

#
# `constants` object library
#
commence(object_library constants)
    this_target_sources(constants.cc)
conclude(object_library)

############################################################
# provide actions/target for preparation of magic.mgc data for embedding/build

#
# `mgc_dict` object library
#
# The file is used as a header but we don't name it .h to avoid it being picked up by IDEs
set(MGC_GZIPPED_H_OUTPUT_FILE "${PROJECT_BINARY_DIR}/magic_mgc.zst.h_")

add_custom_command(
  OUTPUT "${MGC_GZIPPED_H_OUTPUT_FILE}"
  DEPENDS "${libmagic_DICTIONARY}" "${CMAKE_CURRENT_LIST_DIR}/generate_embedded_data_header.script.cmake"
  COMMAND ${CMAKE_COMMAND} "-DINPUT_FILE=${libmagic_DICTIONARY}" "-DOUTPUT_FILE=${MGC_GZIPPED_H_OUTPUT_FILE}" -P "${CMAKE_CURRENT_LIST_DIR}/generate_embedded_data_header.script.cmake"
)
add_custom_target(gen_mgc_unarch
  DEPENDS ${MGC_GZIPPED_H_OUTPUT_FILE}
)

commence(object_library mgc_dict)
    add_dependencies(mgc_dict gen_mgc_unarch)
    # change to `this_target_include_directories` when available
    target_include_directories(mgc_dict PRIVATE "${PROJECT_BINARY_DIR}")
    this_target_object_libraries(buffer compressors)
    this_target_link_libraries(libmagic)
    this_target_sources(mgc_dict.cc)
conclude(object_library)

#
# `math` object library
#
commence(object_library math)
    this_target_sources(tdb_math.cc)
conclude(object_library)

#
# `misc_types` object library
#
commence(object_library misc_types)
    this_target_sources(types.cc)
    this_target_object_libraries(range)
conclude(object_library)

#
# `parse_arguments` object library
#
commence(object_library parse_argument)
    this_target_sources(parse_argument.cc)
    this_target_object_libraries(baseline)
conclude(object_library)

#
# `rectangle` object library
#
commence(object_library rectangle)
    this_target_sources(rectangle.cc)
    this_target_object_libraries(baseline)
conclude(object_library)

#
# `time` object library
#
commence(object_library time)
    this_target_sources(tdb_time.cc)
conclude(object_library)

add_test_subdirectory()
