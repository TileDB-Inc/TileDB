if(VCPKG_TARGET_IS_WINDOWS)
    set(PATCHES
        "0001-Use-pcre2.patch"
        "0003-Fix-WIN32-macro-checks.patch"
        "0004-Typedef-POSIX-types-on-Windows.patch"
        "0005-Include-dirent.h-for-S_ISREG-and-S_ISDIR.patch"
        "0006-Remove-Wrap-POSIX-headers.patch"
        "0007-Substitute-unistd-macros-for-MSVC.patch"
        "0008-Add-FILENO-defines.patch"
        "0009-No-fcntl-in-magic.c.patch"
        "0010-Properly-check-for-the-presence-of-bitmasks.patch"
        "0011-Remove-pipe-related-functions-in-funcs.c.patch"
        "0014-Define-POSIX-macros-if-missing.patch"
        "0015-MSYS2-Remove-ioctl-call.patch"
    )
endif()

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO file/file
    REF FILE5_40
    SHA512 d76bfe5326e1b40368e055c2e049a24b4ffdbd727371f4f3aa1dd3f53787d16b88550b3cc71ecf02151e2fb3e567eb2598e4707badab8c391eb71113c2dcc319
    HEAD_REF master
    PATCHES ${PATCHES}
)

file(COPY "${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt" DESTINATION "${SOURCE_PATH}")
file(COPY "${CMAKE_CURRENT_LIST_DIR}/magic.def" DESTINATION "${SOURCE_PATH}/src")
file(COPY "${CMAKE_CURRENT_LIST_DIR}/config.h" DESTINATION "${SOURCE_PATH}/src")
if(VCPKG_TARGET_IS_WINDOWS)
    file(COPY "${CMAKE_CURRENT_LIST_DIR}/unistd.h" DESTINATION "${SOURCE_PATH}/src")
endif()

vcpkg_cmake_configure(
    SOURCE_PATH ${SOURCE_PATH}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()

if(VCPKG_CROSSCOMPILING)
    vcpkg_add_to_path(PREPEND "${CURRENT_HOST_INSTALLED_DIR}/tools/libmagic/bin")
elseif(VCPKG_TARGET_IS_WINDOWS AND VCPKG_LIBRARY_LINKAGE STREQUAL dynamic)
    set(EXTRA_ARGS "ADD_BIN_TO_PATH")
endif()

vcpkg_install_make(${EXTRA_ARGS})
vcpkg_copy_tool_dependencies("${CURRENT_PACKAGES_DIR}/tools/${PORT}/bin")
vcpkg_copy_tool_dependencies("${CURRENT_PACKAGES_DIR}/tools/${PORT}/debug/bin")
vcpkg_fixup_pkgconfig()

if(VCPKG_LIBRARY_LINKAGE STREQUAL static)
    file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/bin" "${CURRENT_PACKAGES_DIR}/debug/bin")
endif()

if(VCPKG_TARGET_IS_WINDOWS)
    if(NOT VCPKG_BUILD_TYPE OR VCPKG_BUILD_TYPE STREQUAL "release")
        file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/tools/${PORT}/share/misc")
        file(COPY "${CURRENT_PACKAGES_DIR}/share/${PORT}/misc/magic.mgc" DESTINATION "${CURRENT_PACKAGES_DIR}/tools/${PORT}/share/misc")
    endif()
    if(NOT VCPKG_BUILD_TYPE OR VCPKG_BUILD_TYPE STREQUAL "debug")
        file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/tools/${PORT}/debug/share/misc")
        file(COPY "${CURRENT_PACKAGES_DIR}/share/${PORT}/misc/magic.mgc" DESTINATION "${CURRENT_PACKAGES_DIR}/tools/${PORT}/debug/share/misc")
    endif()
endif()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share/${PORT}/man5")

# Handle copyright
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")
