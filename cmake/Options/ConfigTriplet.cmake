set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_SOURCE_DIR}/ports/triplets/")
if (LINUX)
    # Set up triplet in order to override CXX_FLAGS
    set(VCPKG_TARGET_TRIPLET "x64-linux-mod")
elseif(WIN32)
    set(VCPKG_TARGET_TRIPLET "x64-windows-mod")
elseif(APPLE)
    if (CMAKE_OSX_ARCHITECTURES STREQUAL x86_64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64|amd64)|(^i.86$)")
        set(VCPKG_TARGET_TRIPLET "x64-macos-mod")
    elseif (CMAKE_OSX_ARCHITECTURES STREQUAL arm64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
        set(VCPKG_TARGET_TRIPLET "arm64-macos-mod")
    endif()

endif()
