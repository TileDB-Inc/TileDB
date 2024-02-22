set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(X_VCPKG_APPLOCAL_DEPS_INSTALL ON)

set(VCPKG_BUILD_TYPE release)

# Temporarily pinning to 14.38 until the main build tree can pick
# 14.39 with the Visual Studio generator.
set(VCPKG_PLATFORM_TOOLSET_VERSION "14.38")
