set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_SOURCE_DIR}/ports/triplets/")
if (LINUX)
    # Set up triplet in order to override CXX_FLAGS
    set(VCPKG_TARGET_TRIPLET "x64-linux-mod")
elseif(WIN32)
    set(VCPKG_TARGET_TRIPLET "x64-windows-mod")
    set(VCPKG_LIBRARY_LINKAGE "static")
endif()
