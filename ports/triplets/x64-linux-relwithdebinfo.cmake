set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

set(VCPKG_CXX_FLAGS "-g")
set(VCPKG_C_FLAGS "-g")

# Hide symbols in the AWS SDK. Fixes symbol collisions with other libraries
# like arrow (https://github.com/apache/arrow/issues/42154).
if("${PORT}" MATCHES "^aws-")
    set(VCPKG_CMAKE_CONFIGURE_OPTIONS "-DCMAKE_CXX_VISIBILITY_PRESET=hidden;-DCMAKE_C_VISIBILITY_PRESET=hidden")
endif()
