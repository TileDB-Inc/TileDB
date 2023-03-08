
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO Azure/azure-storage-cpplite
    REF v0.3.0
    SHA512 14a476fb9dcff0f8fe2e659cf81ad29433befba723b6440d21f7f563307f116726a15325b53cdc05e827634c00f90a7a8be6d5d212a1e7e442860f7075206b70
    HEAD_REF master
    PATCHES
        001-CMakeLists.patch
        002-base64.cpp.patch
        003-misc.patch
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
    OPTIONS
        -DBUILD_TESTS=OFF
        -DBUILD_SAMPLES=OFF
        -DCURL_NO_CURL_CMAKE=ON
)

vcpkg_install_cmake()

file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/include)
vcpkg_install_copyright(FILE_LIST ${SOURCE_PATH}/LICENSE ${SOURCE_PATH}/NOTICE.txt)

vcpkg_copy_pdbs()
