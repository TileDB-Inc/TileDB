
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO Azure/azure-storage-cpplite
    REF v0.3.0
    SHA512 14a476fb9dcff0f8fe2e659cf81ad29433befba723b6440d21f7f563307f116726a15325b53cdc05e827634c00f90a7a8be6d5d212a1e7e442860f7075206b70
    HEAD_REF master
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}/Microsoft.WindowsAzure.Storage
    PREFER_NINJA
    OPTIONS
        -DBUILD_TESTS=OFF
        -DBUILD_SAMPLES=OFF
        -DCURL_NO_CURL_CMAKE=ON
    OPTIONS_RELEASE
        -DGETTEXT_LIB_DIR=${CURRENT_INSTALLED_DIR}/lib
    OPTIONS_DEBUG
        -DGETTEXT_LIB_DIR=${CURRENT_INSTALLED_DIR}/debug/lib
)

vcpkg_install_cmake()

file(INSTALL
    ${SOURCE_PATH}/LICENSE.txt
    DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
file(REMOVE_RECURSE
    ${CURRENT_PACKAGES_DIR}/debug/include)

vcpkg_copy_pdbs()
