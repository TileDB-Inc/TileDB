vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO PCRE2Project/pcre2
    REF 0ef82f7eb78e9effd662239c6dac70c534a6d60b
    SHA512 e8274e98d183f174eb882ff69a4e74e9b03207c6511561622d28751bf9e3f020fb944e820fa50940e4941773aa497cf5e38a5b35e375f3cac6daa6d015f7d0e8
    HEAD_REF master
    PATCHES
        cmake-fix.patch
        pcre2-10.35_fix-uwp.patch
        no-static-suffix.patch
)

string(COMPARE EQUAL "${VCPKG_LIBRARY_LINKAGE}" "static" BUILD_STATIC)
string(COMPARE EQUAL "${VCPKG_LIBRARY_LINKAGE}" "dynamic" INSTALL_PDB)
string(COMPARE EQUAL "${VCPKG_CRT_LINKAGE}" "static" BUILD_STATIC_CRT)

vcpkg_check_features(
    OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        jit   PCRE2_SUPPORT_JIT
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        ${FEATURE_OPTIONS}
        -DBUILD_STATIC_LIBS=${BUILD_STATIC}
        -DPCRE2_STATIC_RUNTIME=${BUILD_STATIC_CRT}
        -DPCRE2_BUILD_PCRE2_8=ON
        -DPCRE2_BUILD_PCRE2_16=ON
        -DPCRE2_BUILD_PCRE2_32=ON
        -DPCRE2_SUPPORT_UNICODE=ON
        -DPCRE2_BUILD_TESTS=OFF
        -DPCRE2_BUILD_PCRE2GREP=OFF
        -DCMAKE_DISABLE_FIND_PACKAGE_BZip2=ON
        -DCMAKE_DISABLE_FIND_PACKAGE_ZLIB=ON
        -DCMAKE_DISABLE_FIND_PACKAGE_Readline=ON
        -DCMAKE_DISABLE_FIND_PACKAGE_Editline=ON
        -DINSTALL_MSVC_PDB=${INSTALL_PDB}
    )

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_fixup_pkgconfig()
vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/pcre2)

file(REMOVE_RECURSE
    "${CURRENT_PACKAGES_DIR}/man"
    "${CURRENT_PACKAGES_DIR}/share/doc"
    "${CURRENT_PACKAGES_DIR}/debug/include"
    "${CURRENT_PACKAGES_DIR}/debug/man"
    "${CURRENT_PACKAGES_DIR}/debug/share")

if(BUILD_STATIC)
    file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/bin" "${CURRENT_PACKAGES_DIR}/debug/bin")
elseif(VCPKG_TARGET_IS_WINDOWS)
    vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/bin/pcre2-config" "${CURRENT_PACKAGES_DIR}" "`dirname $0`/..")
    if(EXISTS "${CURRENT_PACKAGES_DIR}/debug/bin/pcre2-config")
        vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/debug/bin/pcre2-config" "${CURRENT_PACKAGES_DIR}" "`dirname $0`/../..")
    endif()
endif()

file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")
