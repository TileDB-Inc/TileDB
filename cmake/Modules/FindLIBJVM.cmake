#
# FindLIBJVM.cmake

# The MIT License
#
# Copyright (c) 2017 TileDB, Inc.
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
# Adapted from KitWare's FindJNI.cmake file
# Distributed under the OSI-approved BSD 3-Clause License.
# See https://cmake.org/licensing for details.
#
# Finds and links the libjsig and libjvm shared libraries: This module defines:
#  - LIBJVM_INCLUDE_DIRS, directory containing the jni.h header
#  - LIBJVM_LIBRARIES the library path
# 

# Expand {libarch} occurences to java_libarch subdirectory(-ies) and set ${_var}
if(NOT LIBHDFS_FOUND)
  macro(java_append_library_directories _var)
      # Determine java arch-specific library subdir
      # Mostly based on openjdk/jdk/make/common/shared/Platform.gmk as of openjdk
      # 1.6.0_18 + icedtea patches. However, it would be much better to base the
      # guess on the first part of the GNU config.guess platform triplet.
      if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
        if(CMAKE_LIBRARY_ARCHITECTURE STREQUAL "x86_64-linux-gnux32")
          set(_java_libarch "x32" "amd64" "i386")
        else()
          set(_java_libarch "amd64" "i386")
        endif()
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^i.86$")
          set(_java_libarch "i386")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^alpha")
          set(_java_libarch "alpha")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
          # Subdir is "arm" for both big-endian (arm) and little-endian (armel).
          set(_java_libarch "arm" "aarch32")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^mips")
          # mips* machines are bi-endian mostly so processor does not tell
          # endianess of the underlying system.
          set(_java_libarch "${CMAKE_SYSTEM_PROCESSOR}" "mips" "mipsel" "mipseb" "mips64" "mips64el" "mipsn32" "mipsn32el")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64le")
          set(_java_libarch "ppc64" "ppc64le")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")
          set(_java_libarch "ppc64" "ppc")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)")
          set(_java_libarch "ppc" "ppc64")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^sparc")
          # Both flavours can run on the same processor
          set(_java_libarch "${CMAKE_SYSTEM_PROCESSOR}" "sparc" "sparcv9")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(parisc|hppa)")
          set(_java_libarch "parisc" "parisc64")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^s390")
          # s390 binaries can run on s390x machines
          set(_java_libarch "${CMAKE_SYSTEM_PROCESSOR}" "s390" "s390x")
      elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^sh")
          set(_java_libarch "sh")
      else()
          set(_java_libarch "${CMAKE_SYSTEM_PROCESSOR}")
      endif()
  
      # Append default list architectures if CMAKE_SYSTEM_PROCESSOR was empty or
      # system is non-Linux (where the code above has not been well tested)
      if(NOT _java_libarch OR NOT (CMAKE_SYSTEM_NAME MATCHES "Linux"))
          list(APPEND _java_libarch "i386" "amd64" "ppc")
      endif()
  
      # Sometimes ${CMAKE_SYSTEM_PROCESSOR} is added to the list to prefer
      # current value to a hardcoded list. Remove possible duplicates.
      list(REMOVE_DUPLICATES _java_libarch)
  
      foreach(_path ${ARGN})
          if(_path MATCHES "{libarch}")
              foreach(_libarch ${_java_libarch})
                  string(REPLACE "{libarch}" "${_libarch}" _newpath "${_path}")
                  if(EXISTS ${_newpath})
                      list(APPEND ${_var} "${_newpath}")
                  endif()
              endforeach()
          else()
              if(EXISTS ${_path})
                  list(APPEND ${_var} "${_path}")
              endif()
          endif()
      endforeach()
  endmacro()
  
  # Find Java
  set(_JAVA_HOME "")
  if(JAVA_HOME AND IS_DIRECTORY "${JAVA_HOME}")
    set(_JAVA_HOME "${JAVA_HOME}")
    set(_JAVA_HOME_EXPLICIT 1)
  else()
    set(_ENV_JAVA_HOME "")
    if(DEFINED ENV{JAVA_HOME})
      file(TO_CMAKE_PATH "$ENV{JAVA_HOME}" _ENV_JAVA_HOME)
    endif()
    if(_ENV_JAVA_HOME AND IS_DIRECTORY "${_ENV_JAVA_HOME}")
      set(_JAVA_HOME "${_ENV_JAVA_HOME}")
      set(_JAVA_HOME_EXPLICIT 1)
    else()
      set(_CMD_JAVA_HOME "")
      if(APPLE AND EXISTS /usr/libexec/java_home)
        execute_process(COMMAND /usr/libexec/java_home
          OUTPUT_VARIABLE _CMD_JAVA_HOME OUTPUT_STRIP_TRAILING_WHITESPACE)
      endif()
      if(_CMD_JAVA_HOME AND IS_DIRECTORY "${_CMD_JAVA_HOME}")
        set(_JAVA_HOME "${_CMD_JAVA_HOME}")
        set(_JAVA_HOME_EXPLICIT 0)
      endif()
      unset(_CMD_JAVA_HOME)
    endif()
    unset(_ENV_JAVA_HOME)
  endif()
  
  # Save CMAKE_FIND_FRAMEWORK
  if(DEFINED CMAKE_FIND_FRAMEWORK)
    set(_JNI_CMAKE_FIND_FRAMEWORK ${CMAKE_FIND_FRAMEWORK})
  else()
    unset(_JNI_CMAKE_FIND_FRAMEWORK)
  endif()
  
  if(_JAVA_HOME_EXPLICIT)
    set(CMAKE_FIND_FRAMEWORK NEVER)
  endif()
  
  set(JAVA_JSIG_LIBRARY_DIRECTORIES)
  if(_JAVA_HOME)
      JAVA_APPEND_LIBRARY_DIRECTORIES(JAVA_JSIG_LIBRARY_DIRECTORIES
      ${_JAVA_HOME}/jre/lib/{libarch}
      ${_JAVA_HOME}/jre/lib
      ${_JAVA_HOME}/lib/{libarch}
      ${_JAVA_HOME}/lib
      ${_JAVA_HOME}
      )
  endif()
  get_filename_component(java_install_version
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\JavaSoft\\Java Development Kit;CurrentVersion]" NAME)
  
  list(APPEND JAVA_JSIG_LIBRARY_DIRECTORIES
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\JavaSoft\\Java Development Kit\\1.4;JavaHome]/lib"
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\JavaSoft\\Java Development Kit\\1.3;JavaHome]/lib"
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\JavaSoft\\Java Development Kit\\${java_install_version};JavaHome]/lib"
    )
  JAVA_APPEND_LIBRARY_DIRECTORIES(JAVA_JSIG_LIBRARY_DIRECTORIES
    /usr/lib
    /usr/local/lib
    /usr/lib/jvm/java/lib
    /usr/lib/java/jre/lib/{libarch}
    /usr/lib/jvm/jre/lib/{libarch}
    /usr/local/lib/java/jre/lib/{libarch}
    /usr/local/share/java/jre/lib/{libarch}
    /usr/lib/j2sdk1.4-sun/jre/lib/{libarch}
    /usr/lib/j2sdk1.5-sun/jre/lib/{libarch}
    /opt/sun-jdk-1.5.0.04/jre/lib/{libarch}
    /usr/lib/jvm/java-6-sun/jre/lib/{libarch}
    /usr/lib/jvm/java-1.5.0-sun/jre/lib/{libarch}
    /usr/lib/jvm/java-6-sun-1.6.0.00/jre/lib/{libarch}       # can this one be removed according to #8821 ? Alex
    /usr/lib/jvm/java-6-openjdk/jre/lib/{libarch}
    /usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0/jre/lib/{libarch}        # fedora
    # Debian specific paths for default JVM
    /usr/lib/jvm/default-java/jre/lib/{libarch}
    /usr/lib/jvm/default-java/jre/lib
    /usr/lib/jvm/default-java/lib
    # Arch Linux specific paths for default JVM
    /usr/lib/jvm/default/jre/lib/{libarch}
    /usr/lib/jvm/default/lib/{libarch}
    # Ubuntu specific paths for default JVM
    /usr/lib/jvm/java-8-openjdk-{libarch}/jre/lib/{libarch}     # Ubuntu 15.10
    /usr/lib/jvm/java-7-openjdk-{libarch}/jre/lib/{libarch}     # Ubuntu 15.10
    /usr/lib/jvm/java-6-openjdk-{libarch}/jre/lib/{libarch}     # Ubuntu 15.10
    # OpenBSD specific paths for default JVM
    /usr/local/jdk-1.7.0/jre/lib/{libarch}
    /usr/local/jre-1.7.0/lib/{libarch}
    /usr/local/jdk-1.6.0/jre/lib/{libarch}
    /usr/local/jre-1.6.0/lib/{libarch}
    # SuSE specific paths for default JVM
    /usr/lib64/jvm/java/jre/lib/{libarch}
    /usr/lib64/jvm/jre/lib/{libarch}
    )
  
  set(JAVA_JVM_LIBRARY_DIRECTORIES)
  foreach(dir ${JAVA_JSIG_LIBRARY_DIRECTORIES})
    list(APPEND JAVA_JVM_LIBRARY_DIRECTORIES
      "${dir}"
      "${dir}/client"
      "${dir}/server"
      # IBM SDK, Java Technology Edition, specific paths
      "${dir}/j9vm"
      "${dir}/default"
      )
  endforeach()
  
  if(APPLE)
    if(CMAKE_FIND_FRAMEWORK STREQUAL "ONLY")
      set(_JNI_SEARCHES FRAMEWORK)
    elseif(CMAKE_FIND_FRAMEWORK STREQUAL "NEVER")
      set(_JNI_SEARCHES NORMAL)
    elseif(CMAKE_FIND_FRAMEWORK STREQUAL "LAST")
      set(_JNI_SEARCHES NORMAL FRAMEWORK)
    else()
      set(_JNI_SEARCHES FRAMEWORK NORMAL)
    endif()
    set(_JNI_FRAMEWORK_JVM NAMES JavaVM)
    set(_JNI_FRAMEWORK_JSIG "${_JNI_FRAMEWORK_JVM}")
  else()
    set(_JNI_SEARCHES NORMAL)
  endif()
  
  set(_JNI_NORMAL_JVM
    NAMES jvm
    PATHS ${JAVA_JVM_LIBRARY_DIRECTORIES}
    )
  
  set(_JNI_NORMAL_JSIG
    NAMES jsig
    PATHS ${JAVA_JSIG_LIBRARY_DIRECTORIES}
  )
  
  foreach(search ${_JNI_SEARCHES})
    find_library(JAVA_JVM_LIBRARY ${_JNI_${search}_JVM})
    message(STATUS "Found libjvm: ${JAVA_JVM_LIBRARY}")
    find_library(JAVA_JSIG_LIBRARY ${_JNI_${search}_JSIG})
    message(STATUS "Found libjsig: ${JAVA_JSIG_LIBRARY}")
    if(JAVA_JVM_LIBRARY)
      break()
    endif()
  endforeach()
  unset(_JNI_SEARCHES)
  unset(_JNI_FRAMEWORK_JVM)
  unset(_JNI_FRAMEWORK_JSIG)
  unset(_JNI_NORMAL_JVM)
  unset(_JNI_NORMAL_JSIG)
  
  # Find headers matching the library.
  if("${JAVA_JVM_LIBRARY};" MATCHES "(/JavaVM.framework|-framework JavaVM);")
    set(CMAKE_FIND_FRAMEWORK ONLY)
  else()
    set(CMAKE_FIND_FRAMEWORK NEVER)
  endif()
  
  # add in the include path
  find_path(JAVA_INCLUDE_PATH jni.h)
  
  # Restore CMAKE_FIND_FRAMEWORK
  if(DEFINED _JNI_CMAKE_FIND_FRAMEWORK)
    set(CMAKE_FIND_FRAMEWORK ${_JNI_CMAKE_FIND_FRAMEWORK})
    unset(_JNI_CMAKE_FIND_FRAMEWORK)
  else()
    unset(CMAKE_FIND_FRAMEWORK)
  endif()
  
  mark_as_advanced(JAVA_JVM_LIBRARY 
		   JAVA_JSIG_LIBRARY
	           JAVA_INCLUDE_PATH)
  
  set(LIBJVM_LIBRARIES ${JAVA_JSIG_LIBRARY} ${JAVA_JVM_LIBRARY})
  set(LIBJVM_INCLUDE_DIRS ${JAVA_INCLUDE_PATH})
  
  if(LIBJVM_INCLUDE_DIRS)
    message(STATUS "Found jni.h header file: ${JAVA_INCLUDE_PATH}")
  else()
    message(STATUS "jni.h header file not found")
  endif()
  
  if(LIBJVM_LIBRARIES)
    message(STATUS "Found libjsig library: ${JAVA_JSIG_LIBRARY}")
    message(STATUS "Found libjvm library: ${JAVA_JVM_LIBRARY}")
  else()
    message(STATUS "libjsig, libjvm shared libraries not found")
  endif()
  
  if(JAVA_JSIG_LIBRARY AND JAVA_JVM_LIBRARY)
    set(LIBJVM_FOUND TRUE)
  else()
    set(LIBJVM_FOUND FALSE)
  endif()
endif()

if(LIBJVM_FIND_REQUIRED AND NOT LIBJVM_FOUND)
  message(FATAL_ERROR "Could not find the JVM libraries.")
endif()
