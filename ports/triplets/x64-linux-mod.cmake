set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# For AWS SDK. Remove after update from 1.74
set(VCPKG_CXX_FLAGS "-Wno-error=nonnull -Wno-error=deprecated-declarations")
set(VCPKG_C_FLAGS "-Wno-error=nonnull -Wno-error=deprecated-declarations")
