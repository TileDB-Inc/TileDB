############################################################
# TileDB Vcpkg Customization
############################################################

# A set of TileDB specific tweaks for vcpkg due to various weirdness like
# flaky download mirrors that kill CI. As these are all targeted at CI, we
# only apply them when running in the context of GitHub Actions.

if (NOT DEFINED ENV{GITHUB_ACTIONS})
  message("Skipping GitHub Actions vpckg Tweaks")
  return()
endif()

if (NOT DEFINED ENV{VCPKG_DOWNLOADS})
  message(WARNING "The VCPKG_DOWNLAODS environment variable is not set.")
  message(WARNING "Flaky download mirrors may be an issue for this build.")
  return()
else()
  file(MAKE_DIRECTORY $ENV{VCPKG_DOWNLOADS})
endif()

include(tiledb-vcpkg-download-utils)

tiledb_vcpkg_download("2.17-deps" "nasm-2.16.01.zip")
tiledb_vcpkg_download("2.17-deps" "xz-5.4.1.tar.xz")

# TMP: Debugging
file(GLOB_RECURSE TILEDB_VCPKG_DOWNLOADS $ENV{VCPKG_DOWNLOADS}/*)

message("Show downloade files.")
foreach(path ${TILEDB_VCPKG_DOWNLOADS})
  message("Downloaded: ${path}")
endforeach()
message("Done showing downloads.")
