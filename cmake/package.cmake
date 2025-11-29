# Packaging configuration
configure_file ("${PROJECT_SOURCE_DIR}/cmake/inputs/CustomCPackOptions.cmake.in"
        "${PROJECT_BINARY_DIR}/CustomCPackOptions.cmake"
        @ONLY)
set (CPACK_PROJECT_CONFIG_FILE
        "${PROJECT_BINARY_DIR}/CustomCPackOptions.cmake")

# Not all options can be set in CustomCPackOptions.cmake
if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(CPACK_SOURCE_GENERATOR "ZIP")
    set(CPACK_GENERATOR "ZIP")
else()
    set(CPACK_SOURCE_GENERATOR "TGZ")
    set(CPACK_GENERATOR "TGZ")
endif()

# Package file name variables can not be in config file as well

# Append NOAVX2 if needed
if(NOT ${COMPILER_SUPPORTS_AVX2})
    set(CPACK_NOAVX2 "-noavx2")
endif()

# Properly set system name and architecture
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(CPACK_SYSTEM_NAME "MacOS")
    if(CMAKE_OSX_ARCHITECTURES)
        set(CPACK_SYSTEM_PROCESSOR ${CMAKE_OSX_ARCHITECTURES})
    else()
        set(CPACK_SYSTEM_PROCESSOR ${CMAKE_SYSTEM_PROCESSOR})
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(CPACK_SYSTEM_NAME ${CMAKE_SYSTEM_NAME})
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
        set(CPACK_SYSTEM_PROCESSOR "x86_64")
    else()
        set(CPACK_SYSTEM_PROCESSOR ${CMAKE_SYSTEM_PROCESSOR})
    endif()
else()
    set(CPACK_SYSTEM_NAME "Linux")
    if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
      set(CPACK_SYSTEM_PROCESSOR "arm64")
    else()
      set(CPACK_SYSTEM_PROCESSOR ${CMAKE_SYSTEM_PROCESSOR})
    endif()
endif()

set(CPACK_SOURCE_IGNORE_FILES ".*\.git;.*build.*/.*")

set(CPACK_SOURCE_PACKAGE_FILE_NAME "${PROJECT_NAME}-source")
string(TOLOWER ${CPACK_SOURCE_PACKAGE_FILE_NAME} CPACK_SOURCE_PACKAGE_FILE_NAME)

set(CPACK_PACKAGE_FILE_NAME "${PROJECT_NAME}-${CPACK_SYSTEM_NAME}-${CPACK_SYSTEM_PROCESSOR}${CPACK_NOAVX2}")
string(TOLOWER ${CPACK_PACKAGE_FILE_NAME} CPACK_PACKAGE_FILE_NAME)

include(CPack)
