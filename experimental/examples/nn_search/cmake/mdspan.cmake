# -----------------------------------------------------------------------------
# Allow our executables to use mdspan.
# -----------------------------------------------------------------------------


message(STATUS "Fetching mdspan...")
include(FetchContent)

FetchContent_Declare(
        mdspan
        GIT_REPOSITORY https://github.com/kokkos/mdspan.git
        GIT_TAG stable)

FetchContent_MakeAvailable(mdspan)

