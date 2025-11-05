set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(X_VCPKG_APPLOCAL_DEPS_INSTALL ON)

# bigobj is needed for capnp.
set(VCPKG_C_FLAGS "/fsanitize=address /bigobj")
set(VCPKG_CXX_FLAGS "/fsanitize=address /bigobj")

# Fix CMake 4.0 errors in ports supporting very old CMake verions.
set(VCPKG_CMAKE_CONFIGURE_OPTIONS "-DCMAKE_POLICY_VERSION_MINIMUM=3.5")
