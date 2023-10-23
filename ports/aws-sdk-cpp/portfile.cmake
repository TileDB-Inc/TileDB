vcpkg_buildpath_length_warning(37)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO aws/aws-sdk-cpp
    REF "${VERSION}"
    SHA512 b1a07939dd40f635fdc5b5947c3d679e6a2482b5017a3b26801639785fa1cb3e88414dd216fe64d3fb984d812ff3e8c4103e9b4355d531e533b78f1fa2a7cb01
    PATCHES
        patch-relocatable-rpath.patch
        fix-aws-root.patch
        lock-curl-http-and-tls-settings.patch
)

string(COMPARE EQUAL "${VCPKG_CRT_LINKAGE}" "dynamic" FORCE_SHARED_CRT)

set(EXTRA_ARGS)
if(VCPKG_TARGET_IS_OSX OR VCPKG_TARGET_IS_IOS)
    set(rpath "@loader_path")
elseif (VCPKG_TARGET_IS_ANDROID)
    set(EXTRA_ARGS "-DTARGET_ARCH=ANDROID"
            "-DGIT_EXECUTABLE=--invalid-git-executable--"
            "-DGIT_FOUND=TRUE"
            "-DNDK_DIR=$ENV{ANDROID_NDK_HOME}"
            "-DANDROID_BUILD_ZLIB=FALSE"
            "-DANDROID_BUILD_CURL=FALSE"
            "-DANDROID_BUILD_OPENSSL=FALSE"
            )
else()
    set(rpath "\$ORIGIN")
endif()

set(BUILD_ONLY core)
include(${CMAKE_CURRENT_LIST_DIR}/compute_build_only.cmake)
vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    DISABLE_PARALLEL_CONFIGURE
    OPTIONS
        ${EXTRA_ARGS}
        "-DENABLE_UNITY_BUILD=ON"
        "-DENABLE_TESTING=OFF"
        "-DFORCE_SHARED_CRT=${FORCE_SHARED_CRT}"
        "-DBUILD_ONLY=${BUILD_ONLY}"
        "-DBUILD_DEPS=OFF"
        "-DBUILD_SHARED_LIBS=OFF"
        "-DAWS_SDK_WARNINGS_ARE_ERRORS=OFF"
        "-DCMAKE_INSTALL_RPATH=${rpath}"
        "-DCMAKE_MODULE_PATH=${CURRENT_INSTALLED_DIR}/share/aws-c-common" # use extra cmake files
)
vcpkg_cmake_install()

foreach(TARGET IN LISTS BUILD_ONLY)
    vcpkg_cmake_config_fixup(PACKAGE_NAME "aws-cpp-sdk-${TARGET}" CONFIG_PATH "lib/cmake/aws-cpp-sdk-${TARGET}" DO_NOT_DELETE_PARENT_CONFIG_PATH)
endforeach()
vcpkg_cmake_config_fixup(PACKAGE_NAME "AWSSDK" CONFIG_PATH "lib/cmake/AWSSDK")

vcpkg_copy_pdbs()

file(GLOB_RECURSE AWS_TARGETS "${CURRENT_PACKAGES_DIR}/share/*/*-targets-*.cmake")
foreach(AWS_TARGET IN LISTS AWS_TARGETS)
    file(READ ${AWS_TARGET} _contents)
    string(REGEX REPLACE
        "bin\\/([A-Za-z0-9_.-]+\\.lib)"
        "lib/\\1"
        _contents "${_contents}")
    file(WRITE ${AWS_TARGET} "${_contents}")
endforeach()

file(GLOB AWS_CONFIGS "${CURRENT_PACKAGES_DIR}/share/*/aws-cpp-sdk-*-config.cmake")
list(FILTER AWS_CONFIGS EXCLUDE REGEX "aws-cpp-sdk-core-config\\.cmake\$")
foreach(AWS_CONFIG IN LISTS AWS_CONFIGS)
    file(READ "${AWS_CONFIG}" _contents)
    file(WRITE "${AWS_CONFIG}" "include(CMakeFindDependencyMacro)\nfind_dependency(aws-cpp-sdk-core)\n${_contents}")
endforeach()

file(REMOVE_RECURSE
    "${CURRENT_PACKAGES_DIR}/debug/include"
    "${CURRENT_PACKAGES_DIR}/debug/share"
    "${CURRENT_PACKAGES_DIR}/lib/pkgconfig"
    "${CURRENT_PACKAGES_DIR}/debug/lib/pkgconfig"
    "${CURRENT_PACKAGES_DIR}/nuget"
    "${CURRENT_PACKAGES_DIR}/debug/nuget"
)

if(VCPKG_LIBRARY_LINKAGE STREQUAL dynamic)
    file(GLOB LIB_FILES ${CURRENT_PACKAGES_DIR}/bin/*.lib)
    if(LIB_FILES)
        file(COPY ${LIB_FILES} DESTINATION ${CURRENT_PACKAGES_DIR}/lib)
        file(REMOVE ${LIB_FILES})
    endif()
    file(GLOB DEBUG_LIB_FILES ${CURRENT_PACKAGES_DIR}/debug/bin/*.lib)
    if(DEBUG_LIB_FILES)
        file(COPY ${DEBUG_LIB_FILES} DESTINATION ${CURRENT_PACKAGES_DIR}/debug/lib)
        file(REMOVE ${DEBUG_LIB_FILES})
    endif()

    file(APPEND "${CURRENT_PACKAGES_DIR}/include/aws/core/SDKConfig.h" "#ifndef USE_IMPORT_EXPORT\n#define USE_IMPORT_EXPORT\n#endif")
endif()

configure_file("${CURRENT_PORT_DIR}/usage" "${CURRENT_PACKAGES_DIR}/share/${PORT}/usage" @ONLY)

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
