# Based on the manual Git approach found here:
# https://github.com/jlblancoc/vcpkg/blob/2952bcc6c596f0b6b880595c6728c711826cb0aa/ports/mrpt/portfile.cmake

set(TARGET_GIT_URL "https://github.com/TileDB-Inc/file-windows.git")

set(PACKAGE_CHECKOUT_DIR "src")
set(SOURCE_PATH ${CURRENT_BUILDTREES_DIR}/${PACKAGE_CHECKOUT_DIR})

if (NOT EXISTS "${CURRENT_BUILDTREES_DIR}/${PACKAGE_CHECKOUT_DIR}")
  vcpkg_execute_required_process(
    COMMAND ${GIT} clone ${TARGET_GIT_URL} ${PACKAGE_CHECKOUT_DIR}
    WORKING_DIRECTORY ${CURRENT_BUILDTREES_DIR}
    LOGNAME git-clone-${TARGET_TRIPLET}
  )
else()
  # Purge any local changes
  vcpkg_execute_required_process(
    COMMAND ${GIT} clean -ffxd
    WORKING_DIRECTORY ${CURRENT_BUILDTREES_DIR}/${PACKAGE_CHECKOUT_DIR}
    LOGNAME git-clean-1-${TARGET_TRIPLET}
  )

  # Also purge changes in submodules
  vcpkg_execute_required_process(
    COMMAND ${GIT} submodule foreach --recursive git clean -ffxd
    WORKING_DIRECTORY ${CURRENT_BUILDTREES_DIR}/${PACKAGE_CHECKOUT_DIR}
    LOGNAME git-clean-2-${TARGET_TRIPLET}
  )

  # And ensure that we are dealing with the right commit
  vcpkg_execute_required_process(
    COMMAND ${GIT} fetch origin ${TARGET_GIT_SHA}
    WORKING_DIRECTORY ${CURRENT_BUILDTREES_DIR}/${PACKAGE_CHECKOUT_DIR}
    LOGNAME git-clean-3-${TARGET_TRIPLET}
  )
endif()

vcpkg_execute_required_process(
  COMMAND ${GIT} checkout ${TARGET_GIT_SHA}
  WORKING_DIRECTORY ${CURRENT_BUILDTREES_DIR}/${PACKAGE_CHECKOUT_DIR}
  LOGNAME git-checkout-${TARGET_TRIPLET}
)

vcpkg_execute_required_process(
  COMMAND ${GIT} submodule update --init --recursive
  WORKING_DIRECTORY ${CURRENT_BUILDTREES_DIR}/${PACKAGE_CHECKOUT_DIR}
  LOGNAME git-submodule-${TARGET_TRIPLET}
)

if (WIN32)
  set(CFLAGS_DEF "")
else()
  set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
endif()

# CMake configure and build
# ---------------------------
vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    DISABLE_PARALLEL_CONFIGURE
    OPTIONS
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        -Dlibmagic_STATIC_LIB=ON
    OPTIONS_DEBUG
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        -Dlibmagic_STATIC_LIB=ON
)

vcpkg_install_cmake(ADD_BIN_TO_PATH)
vcpkg_copy_pdbs()
vcpkg_install_copyright(
  FILE_LIST
    ${SOURCE_PATH}/LICENSE
    ${SOURCE_PATH}/dirent/LICENSE
    ${SOURCE_PATH}/file/COPYING
    ${SOURCE_PATH}/pcre2/LICENCE
)
