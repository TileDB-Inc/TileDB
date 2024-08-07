set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES x86_64)
set(VCPKG_OSX_DEPLOYMENT_TARGET 11)

set(VCPKG_CXX_FLAGS "-g")
set(VCPKG_C_FLAGS "-g")

# Fix CMake 4.0 errors in ports supporting very old CMake verions.
set(VCPKG_CMAKE_CONFIGURE_OPTIONS "-DCMAKE_POLICY_VERSION_MINIMUM=3.5")

# Hide symbols in the AWS SDK. Fixes symbol collisions with other libraries
# like arrow (https://github.com/apache/arrow/issues/42154).
if("${PORT}" MATCHES "^aws-")
    set(VCPKG_CMAKE_CONFIGURE_OPTIONS "-DCMAKE_CXX_VISIBILITY_PRESET=hidden;-DCMAKE_C_VISIBILITY_PRESET=hidden")
endif()

# Hide symbols in spdlog. Fixes symbol collisions in downstream projects
# (https://github.com/gabime/spdlog/pull/3322 was rejected).
if("${PORT}" MATCHES "spdlog")
    list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS "-DCMAKE_CXX_VISIBILITY_PRESET=hidden;-DCMAKE_C_VISIBILITY_PRESET=hidden")
endif()
