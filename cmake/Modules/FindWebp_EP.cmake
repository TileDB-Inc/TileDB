# FindWebp_EP.cmake
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
# Fetches and builds libwebp for use by tiledb.

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(WEBP_PATHS ${TILEDB_EP_INSTALL_PREFIX})

#########################
# from https://stackoverflow.com/a/56738858
## https://stackoverflow.com/questions/32183975/how-to-print-all-the-properties-of-a-target-in-cmake/56738858#56738858
## https://stackoverflow.com/a/56738858/3743145

## Get all properties that cmake supports
execute_process(COMMAND cmake --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)
## Convert command output into a CMake list
STRING(REGEX REPLACE ";" "\\\\;" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
STRING(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")

list(REMOVE_DUPLICATES CMAKE_PROPERTY_LIST)

function(print_target_properties tgt)
    if(NOT TARGET ${tgt})
      message("There is no target named '${tgt}'")
      return()
    endif()

    foreach (prop ${CMAKE_PROPERTY_LIST})
        string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" prop ${prop})
        get_target_property(propval ${tgt} ${prop})
        if (propval)
            message ("${tgt} ${prop} = ${propval}")
        endif()
    endforeach(prop)
endfunction(print_target_properties)
#########################

if (TILEDB_WEBP_EP_BUILT)

 if (1)
  find_package(WebP REQUIRED PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
  message(STATUS "WebP_FOUND is ${WebP_FOUND}")
  message(STATUS " webp_INCLUDE_DIR is ${wepb_INCLUDE_DIR}")
  message(STATUS " webp_LIBRARIES is ${wepb_LIBRARIES}")
  message(STATUS " WebP_INCLUDE_DIR is ${WebP_INCLUDE_DIR}")
  message(STATUS " WebP_LIBRARIES is ${WebP_LIBRARIES}")
  #print_target_properties(WebP)
  print_target_properties(WebP::webp)
  #find_package(webp PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
  #message(STATUS "webp_FOUND is ${webp_FOUND}")
  #print_target_properties(webp)
  #find_package(Webp_EP REQUIRED)
  #message(STATUS "Webp_EP_FOUND is ${Webp_EP_FOUND}")
  #print_target_properties(Webp_EP)
  #include(CMakePrintHelpers)
  #cmake_print_properties(
  #  TARGETS Webp_EP
  #  PROPERTIES LOCATION INCLUDE_DIRECTORIES INTERFACE_INCLUDE_DIRECTORIES LIBRARIES_DIRECTORIES
  #  )
 endif()

endif()

# if not yet built add it as an external project
if(NOT TILEDB_WEBP_EP_BUILT)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Webp as an external project")

    #if (WIN32)
    #  set(CFLAGS_DEF "")
    #else()
    #  set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
    #  set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    #endif()

    # that was modified by tiledb to also build with cmake for nix
    ExternalProject_Add(ep_webp
      PREFIX "externals"
      GIT_REPOSITORY "https://chromium.googlesource.com/webm/libwebp"
      #GIT_TAG "release-1.?.?" # after 'static' addition in some release
      # from branch 'main' history as the 'static' support added apr 12 2022
      # at implementation time is not yet in release branch/tag.
      GIT_TAG "a19a25bb03757d5bb14f8d9755ab39f06d0ae5ef" 
      GIT_SUBMODULES_RECURSE TRUE
      UPDATE_COMMAND ""
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        -DWEBP_LINK_STATIC=ON
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_webp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_WEBP_EP_BUILT=TRUE
    )

    set(TILEDB_WEBP_DIR "${TILEDB_EP_INSTALL_PREFIX}")

  else()
    message(FATAL_ERROR "Unable to find Webp")
  endif()
endif()

# Always building static EP, install it..
if (TILEDB_WEBP_EP_BUILT)
  #install_target_libs(webp imagedec imageenc imageioutil webpdecoder webpdmux webpmux)
  #install_target_libs($<TARGET_PROPERTY:WebP::webp,INTERFACE_LINK_LIBRARIES>)
  #install_target_libs(WebP::webp)
  install_target_libs(WebP::webpdecoder WebP::webp WebP::webpdemux WebP::webpmux)
endif()
