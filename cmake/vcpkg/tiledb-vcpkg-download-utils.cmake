############################################################
# TileDB Vcpkg Download Helpers
############################################################

function(tiledb_vcpkg_download tag_name file_name)
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/vcpkg/downloads/${file_name}")
    file(
      DOWNLOAD
        "https://github.com/TileDB-Inc/tiledb-deps-mirror/releases/download/${tag_name}/${file_name}"
        "$ENV{VCPKG_DOWNLOADS}/${file_name}"
      LOG result
    )
    message("Downloaded: ${file_name}")
  else()
    message("Previously downloaded: ${file_name}")
  endif()
endfunction()
